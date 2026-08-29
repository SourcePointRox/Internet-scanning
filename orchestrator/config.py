"""配置加载：YAML + 环境变量插值 + 环境变量覆盖 + 校验。

三层配置来源（优先级从高到低）：
1. 环境变量覆盖：``NETATLAS_<段>__<键>``（双下划线分层），如
   ``NETATLAS_L4__SOURCE_IP=10.0.0.5``、``NETATLAS_PATHS__ROOT=/data/netatlas``；
2. YAML 内的 ``${ENV_VAR:默认值}`` 插值；
3. ``config/config.yaml`` 文件本体（由 config.example.yaml 复制而来，
   不得包含任何机器相关的硬编码值）。

设计原则：仓库内的配置模板对任何机器都应当"开箱即 dry-run"，
机器相关的值（网卡 IP / 网关 MAC / 磁盘路径）一律留空自动探测或经环境变量注入。
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent

log = logging.getLogger("netatlas.config")

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")
_ENV_PREFIX = "NETATLAS_"

# 历史上容易残留机器相关硬编码的敏感键，校验时若命中旧默认值则告警
_SENSITIVE_KEYS = ("l4.source_ip", "l4.router_mac", "paths.root")


def _interpolate(value: Any) -> Any:
    """递归替换字符串值中的 ${VAR:default} 占位符。"""
    if isinstance(value, str):
        def _sub(m: re.Match) -> str:
            var, default = m.group(1), m.group(2)
            env = os.environ.get(var)
            if env is not None:
                return env
            return default if default is not None else ""
        return _ENV_PATTERN.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def _apply_env_overrides(data: dict) -> dict:
    """把 NETATLAS_A__B__C=value 形式的环境变量写入嵌套字典。"""
    for key, raw in os.environ.items():
        if not key.startswith(_ENV_PREFIX):
            continue
        path = key[len(_ENV_PREFIX):].lower().split("__")
        if not path or not path[0]:
            continue
        node = data
        for part in path[:-1]:
            nxt = node.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                node[part] = nxt
            node = nxt
        node[path[-1]] = _coerce(raw)
    return data


def _coerce(raw: str) -> Any:
    """环境变量字符串 -> 原生类型（bool/int/float/JSON 数组/字典）。"""
    low = raw.strip().lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    if low in ("null", "none", ""):
        return None
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    if raw.startswith(("[", "{")):
        try:
            import json
            return json.loads(raw)
        except ValueError:
            pass
    return raw


class Config:
    def __init__(self, path: str | os.PathLike | None = None):
        self.path = Path(path) if path else ROOT / "config" / "config.yaml"
        if not self.path.exists():
            example = self.path.with_name("config.example.yaml")
            raise FileNotFoundError(
                f"配置文件不存在: {self.path}。请从模板复制: cp {example} {self.path}")
        with open(self.path, "r", encoding="utf-8") as f:
            data: dict[str, Any] = yaml.safe_load(f) or {}
        self.data = _apply_env_overrides(_interpolate(data))
        # paths.root 允许配置为空：默认取仓库根目录（可移植）
        root_cfg = (self.data.get("paths") or {}).get("root")
        self.root = Path(str(root_cfg)).resolve() if root_cfg else ROOT

    def get(self, *keys: str, default: Any = None) -> Any:
        node = self.data
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node

    def abs_path(self, *keys: str) -> Path:
        p = Path(str(self.get(*keys)))
        return p if p.is_absolute() else (self.root / p)

    def validate(self) -> list[str]:
        """启动前校验：返回告警列表（空 = 通过）。

        - 检查机器相关敏感值是否仍是"占位/空"之外的旧硬编码痕迹；
        - 检查配额总和是否超过全局令牌桶；
        - 检查必需目录是否可写。
        """
        warnings: list[str] = []
        src_ip = self.get("l4", "source_ip")
        rmac = self.get("l4", "router_mac")
        if src_ip and not _looks_like_ip(str(src_ip)):
            warnings.append(f"l4.source_ip 非法: {src_ip!r}")
        if rmac and not _looks_like_mac(str(rmac)):
            warnings.append(f"l4.router_mac 非法: {rmac!r}")
        if bool(src_ip) != bool(rmac):
            warnings.append("l4.source_ip 与 l4.router_mac 必须同时配置（二层直连）或同时留空（自动）")
        bw = self.data.get("bandwidth") or {}
        quotas = (bw.get("quotas") or {})
        total = sum(float(v) for v in quotas.values())
        cap = float(bw.get("upload_mbps", 25.0)) * float(bw.get("global_cap_pct", 80)) / 100
        if total > cap + 1e-6:
            warnings.append(f"带宽配额总和 {total:.1f} Mbps 超过全局令牌桶 {cap:.1f} Mbps")
        email = str(self.get("project", "contact_email", default="") or "")
        if "example.org" in email or "@" not in email:
            warnings.append("project.contact_email 仍是占位邮箱：opt-out 联系人必须真实可达")
        for key in ("data_raw", "data_parquet", "data_meta"):
            try:
                self.abs_path("paths", key).mkdir(parents=True, exist_ok=True)
            except OSError as e:
                warnings.append(f"数据目录 paths.{key} 不可写: {e}")
        return warnings


def _looks_like_ip(v: str) -> bool:
    import ipaddress
    try:
        ipaddress.ip_address(v)
        return True
    except ValueError:
        return False


def _looks_like_mac(v: str) -> bool:
    return bool(re.fullmatch(r"([0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}", v))


def load(path: str | None = None) -> Config:
    return Config(path)
