"""配置加载：YAML + 路径解析。"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent


class Config:
    def __init__(self, path: str | os.PathLike | None = None):
        self.path = Path(path) if path else ROOT / "config" / "config.yaml"
        with open(self.path, "r", encoding="utf-8") as f:
            self.data: dict[str, Any] = yaml.safe_load(f)
        self.root = ROOT

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


def load(path: str | None = None) -> Config:
    return Config(path)
