"""列式层压缩器：NDJSON.zst 分片 -> Parquet 分区表（DuckDB 可直接查询）。

- 表：hosts（L7 抓取记录）、l4_open（开放端口）、classification（站点分类）；
- 分区：table=<name>/date=YYYY-MM-DD/part-NNNN.parquet；
- zstd level 6 + 字典/游程编码，空间效率对标 Censys delta encoding 思路。

Schema 演进（v2）：
- 已知标量列保留原生类型（int/float/bool），嵌套字段 JSON 化，新字段以
  string 追加 —— 旧文件可读、新文件更准（union_by_name 兼容）；
- 读取侧经 storage.schema.migrate 升级历史记录；
- 已完成清单持久化到 data/meta/compacted.json，进程重启不再重复压缩。
"""
from __future__ import annotations

import io
import json
import logging
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from orchestrator.config import Config
from orchestrator.errors import ErrorReporter
from orchestrator.state import REGISTRY
from storage import schema

log = logging.getLogger("netatlas.compactor")
MODULE = "compactor"


def iter_ndjson_zst(path: Path):
    dctx = zstd.ZstdDecompressor()
    with open(path, "rb") as fh, dctx.stream_reader(fh) as reader:
        for line in io.TextIOWrapper(reader, encoding="utf-8", errors="replace"):
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


class Compactor:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.raw = cfg.abs_path("paths", "data_raw")
        self.out = cfg.abs_path("paths", "data_parquet")
        self.level = int(cfg.get("storage", "parquet_compression_level", default=6))
        self.interval = int(cfg.get("storage", "compact_interval_min", default=30)) * 60
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._done_path = cfg.abs_path("paths", "data_meta") / "compacted.json"
        self._done: set[str] = self._load_done()

    def _load_done(self) -> set[str]:
        try:
            if self._done_path.exists():
                return set(json.loads(self._done_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as e:
            ErrorReporter.get().report(MODULE, "压缩清单损坏，重新全量扫描",
                                       level="warning", exc=e)
        return set()

    def _save_done(self) -> None:
        try:
            self._done_path.parent.mkdir(parents=True, exist_ok=True)
            self._done_path.write_text(json.dumps(sorted(self._done)), encoding="utf-8")
        except OSError as e:
            ErrorReporter.get().report(MODULE, "压缩清单落盘失败", level="warning", exc=e)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="compactor")
        self._thread.start()
        REGISTRY.set_running(MODULE, True)

    def stop(self) -> None:
        self._stop.set()
        self._save_done()
        REGISTRY.set_running(MODULE, False)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.compact_all()
            except Exception as e:  # noqa: BLE001 —— 整轮失败上报，下轮重试
                ErrorReporter.get().report(MODULE, "compact 整轮失败，下个周期重试", exc=e)
                REGISTRY.set_extra(MODULE, last_error=str(e))
            self._stop.wait(self.interval)

    def compact_all(self) -> int:
        """把所有未压缩的分片转 Parquet。返回新文件数。"""
        made = 0
        if not self.raw.exists():
            return 0
        for day_dir in sorted(self.raw.iterdir()):
            if not day_dir.is_dir():
                continue
            date = day_dir.name
            for stream_dir in day_dir.iterdir():
                if not stream_dir.is_dir():
                    continue
                table = stream_dir.name
                for shard in sorted(stream_dir.glob("part-*.jsonl.zst")):
                    key = f"{date}/{table}/{shard.name}"
                    if key in self._done:
                        continue
                    try:
                        if self._compact_shard(shard, table, date):
                            made += 1
                            REGISTRY.incr(MODULE, "parquet_files")
                        self._done.add(key)
                    except Exception as e:  # noqa: BLE001 —— 单分片失败跳过并上报
                        ErrorReporter.get().report(MODULE, f"分片压缩失败: {key}", exc=e)
                        REGISTRY.incr(MODULE, "compact_errors")
        if made:
            self._save_done()
        REGISTRY.set_extra(MODULE, last_run=time.strftime("%H:%M:%S"), parquet_dir=str(self.out))
        return made

    def _compact_shard(self, shard: Path, table: str, date: str) -> bool:
        """单分片 -> Parquet。返回是否产出了文件。原子写入（tmp+rename）。"""
        out_dir = self.out / f"table={table}" / f"date={date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / (shard.stem.replace(".jsonl", "") + ".parquet")
        rows = [schema.migrate(r) for r in iter_ndjson_zst(shard)]
        if not rows:
            return False
        # 类型化 Schema：已知列原生类型 + 未知列 string（向后兼容追加）
        keys = sorted({k for r in rows for k in r})
        cols = {k: [schema.coerce_value(k, r.get(k)) for r in rows] for k in keys}
        arrays = [pa.array(cols[k], type=schema.arrow_type(k)) for k in keys]
        tbl = pa.table(arrays, names=keys)
        tmp_path = out_path.with_suffix(".parquet.tmp")
        pq.write_table(tbl, tmp_path, compression="zstd", compression_level=self.level)
        tmp_path.replace(out_path)
        return True


def _flatten(v):
    """兼容旧接口：复杂结构 JSON 序列化（新代码请用 schema.coerce_value）。"""
    return schema.coerce_value("_unknown", v)
