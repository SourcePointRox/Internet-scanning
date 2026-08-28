"""模块注册与共享运行时状态（WebUI 与编排器之间的桥梁）。"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ModuleStatus:
    name: str
    running: bool = False
    started_at: float | None = None
    counters: dict[str, int] = field(default_factory=dict)
    last_error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class ModuleRegistry:
    """线程安全的模块生命周期与计数注册表。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._modules: dict[str, ModuleStatus] = {}
        self._hooks: dict[str, tuple[Callable, Callable]] = {}  # name -> (start, stop)

    def register(self, name: str, start: Callable, stop: Callable) -> None:
        with self._lock:
            self._modules.setdefault(name, ModuleStatus(name=name))
            self._hooks[name] = (start, stop)

    def start(self, name: str) -> bool:
        with self._lock:
            st = self._modules.get(name)
            hook = self._hooks.get(name)
            if not st or not hook or st.running:
                return False
            try:
                hook[0]()
                st.running = True
                st.started_at = time.time()
                st.last_error = None
                return True
            except Exception as e:  # noqa: BLE001
                st.last_error = str(e)
                return False

    def stop(self, name: str) -> bool:
        with self._lock:
            st = self._modules.get(name)
            hook = self._hooks.get(name)
            if not st or not hook or not st.running:
                return False
            try:
                hook[1]()
            finally:
                st.running = False
            return True

    def set_running(self, name: str, running: bool) -> None:
        with self._lock:
            self._modules.setdefault(name, ModuleStatus(name=name)).running = running

    def incr(self, name: str, key: str, n: int = 1) -> None:
        with self._lock:
            st = self._modules.setdefault(name, ModuleStatus(name=name))
            st.counters[key] = st.counters.get(key, 0) + n

    def set_extra(self, name: str, **kv: Any) -> None:
        with self._lock:
            st = self._modules.setdefault(name, ModuleStatus(name=name))
            st.extra.update(kv)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                name: {
                    "running": st.running,
                    "uptime_s": round(time.time() - st.started_at, 1) if st.started_at and st.running else 0,
                    "counters": dict(st.counters),
                    "last_error": st.last_error,
                    "extra": dict(st.extra),
                }
                for name, st in self._modules.items()
            }


REGISTRY = ModuleRegistry()
