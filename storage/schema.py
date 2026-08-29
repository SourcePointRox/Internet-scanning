"""存储记录 Schema 版本化与数据血缘。

问题背景：动态 Schema 扁平化导致列无序膨胀、类型漂移；且记录落盘后无法
追溯"由哪个探针/引擎/版本产生"。本模块定义：

- ``SCHEMA_VERSION``：当前记录格式版本（writer 写入时盖章）；
- ``lineage(record, ...)``：为每条记录附加血缘块 ``_lineage``：
  来源模块、引擎/工具版本、采集时间、流水线 hop 链；
- ``migrate(record)``：读取侧把旧版本记录升级为当前版本（v1 -> v2：
  补 _lineage / schema_version 缺省值，规整 ts 类型）；
- ``typed_columns``：Compactor 不再全部 string 化 —— 对已知标量字段
  （ip/port/ts/rtt_ms/confidence 等）保留原生类型，嵌套字段 JSON 化，
  新字段以 string 追加（向后兼容的 Schema 演进）。
"""
from __future__ import annotations

import platform
import time
from typing import Any

SCHEMA_VERSION = 2

# v2 已知标量列的 pyarrow 目标类型（其余列一律 string / JSON-string）
TYPED_COLUMNS: dict[str, str] = {
    "ip": "string", "port": "int32", "proto": "string", "protocol": "string",
    "status": "string", "ttl": "int32", "ts": "int64", "family": "int8",
    "rtt_ms": "float64", "domain": "string", "reverse_dns": "string",
    "banner": "string", "error": "string", "engine": "string",
    "l7_probed": "bool", "schema_version": "int32",
}

_LINEAGE_KEYS = ("source", "engine", "tool_version", "collected_at", "hops")


def stamp(record: dict, *, source: str, engine: str | None = None,
          tool_version: str | None = None) -> dict:
    """写入侧盖章：为记录附加 schema 版本与血缘块（原地修改并返回）。

    hops 链：记录每经过一个流水线模块，追加该模块名 —— 完整还原数据路径。
    """
    lin = record.get("_lineage")
    if not isinstance(lin, dict):
        lin = {"source": source, "engine": engine, "tool_version": tool_version,
               "collected_at": int(time.time()), "hops": [source]}
        record["_lineage"] = lin
    else:
        hops = lin.setdefault("hops", [])
        if not hops or hops[-1] != source:
            hops.append(source)
    record["schema_version"] = SCHEMA_VERSION
    return record


def migrate(record: dict) -> dict:
    """读取侧升级：把任意历史版本记录规整为当前版本。"""
    ver = record.get("schema_version", 1)
    if ver >= SCHEMA_VERSION:
        return record
    # v1 -> v2：补血缘块与类型规整
    if not isinstance(record.get("_lineage"), dict):
        record["_lineage"] = {
            "source": record.get("engine") or "unknown",
            "engine": record.get("engine"),
            "tool_version": None,
            "collected_at": record.get("ts") or int(time.time()),
            "hops": [record.get("engine") or "unknown"],
        }
    try:
        record["ts"] = int(record.get("ts") or 0)
    except (TypeError, ValueError):
        record["ts"] = 0
    record["schema_version"] = SCHEMA_VERSION
    return record


def arrow_type(name: str):
    """列名 -> pyarrow 类型（已知列原生类型，未知列 string）。"""
    import pyarrow as pa
    return {
        "string": pa.string(), "int32": pa.int32(), "int64": pa.int64(),
        "int8": pa.int8(), "float64": pa.float64(), "bool": pa.bool_(),
    }[TYPED_COLUMNS.get(name, "string")]


def coerce_value(col: str, v: Any) -> Any:
    """按列目标类型规整单值：嵌套结构 JSON 化，标量按类型转换。"""
    import json
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, default=str)
    kind = TYPED_COLUMNS.get(col, "string")
    try:
        if kind in ("int32", "int64", "int8"):
            return int(v)
        if kind == "float64":
            return float(v)
        if kind == "bool":
            return bool(v)
    except (TypeError, ValueError):
        return None
    return str(v)


def runtime_tool_version(tool: str) -> str:
    """记录采集工具版本（血缘用）。"""
    if tool == "python":
        return f"python-{platform.python_version()}"
    return tool
