"""运行时状态持久化：REGISTRY 快照 + 扫描进度状态机落盘与恢复。

解决"进程重启后所有扫描状态丢失"：
- ``StateStore``：每 interval_s 秒把 REGISTRY 快照原子写入 JSON 文件
  （tmp + os.replace，防写半截），启动时 restore 累计计数基线；
- ``ScanStateMachine``：L4 扫描进度持久化（目标、端口、已发探针数、
  masscan resume 文件路径、分片序号），支持 崩溃 -> 重启 -> 续扫。
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

log = logging.getLogger("netatlas.state.persist")


def _atomic_write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1, default=str),
                   encoding="utf-8")
    os.replace(tmp, path)


class StateStore:
    """REGISTRY 运行时状态的周期性落盘/恢复。"""

    def __init__(self, path: Path, interval_s: float = 15.0):
        self.path = Path(path)
        self.interval = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def save(self, snapshot: dict[str, Any]) -> None:
        try:
            _atomic_write_json(self.path, {"saved_at": time.time(), "modules": snapshot})
        except OSError as e:
            log.warning("状态落盘失败: %s", e)

    def load(self) -> dict[str, Any]:
        try:
            if self.path.exists():
                return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.warning("状态文件损坏，忽略恢复: %s", e)
        return {}

    def start(self, registry) -> None:
        """后台周期保存。registry: orchestrator.state.REGISTRY。"""
        self._stop.clear()

        def _loop():
            while not self._stop.wait(self.interval):
                self.save(registry.snapshot())

        self._thread = threading.Thread(target=_loop, daemon=True, name="state-persister")
        self._thread.start()

    def stop(self, registry=None) -> None:
        self._stop.set()
        if registry is not None:
            self.save(registry.snapshot())  # 退出前最后落盘一次


class ScanStateMachine:
    """L4 扫描进度状态机（持久化到 JSON，支持崩溃续扫）。

    状态迁移：IDLE -> RUNNING -> (PAUSED | COMPLETED | FAILED) -> RUNNING ...
    记录：targets / ports / 分片序号 / 已确认探针数 / masscan resume 文件 / 当前速率。
    """

    STATES = ("IDLE", "RUNNING", "PAUSED", "COMPLETED", "FAILED")

    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._state: dict[str, Any] = self._default()
        self._load()

    @staticmethod
    def _default() -> dict[str, Any]:
        return {"state": "IDLE", "targets": [], "ports": "", "segment": 0,
                "probes_sent": 0, "open_found": 0, "rate_pps": 0,
                "resume_file": None, "started_at": None, "updated_at": None,
                "last_error": None, "run_id": None}

    def _load(self) -> None:
        try:
            if self.path.exists():
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if data.get("state") in self.STATES:
                    self._state = {**self._default(), **data}
                    # 进程已死，RUNNING 状态一律降级为 PAUSED（可续扫）
                    if self._state["state"] == "RUNNING":
                        self._state["state"] = "PAUSED"
        except (OSError, json.JSONDecodeError) as e:
            log.warning("扫描状态文件损坏，重置: %s", e)

    def _flush(self) -> None:
        try:
            _atomic_write_json(self.path, self._state)
        except OSError as e:
            log.warning("扫描状态落盘失败: %s", e)

    # ---- 状态迁移 API（全部线程安全 + 立即落盘）----
    def begin_run(self, targets: list[str], ports: str, rate_pps: int,
                  resume_file: str | None, run_id: str) -> None:
        with self._lock:
            self._state.update(state="RUNNING", targets=list(targets), ports=ports,
                               rate_pps=rate_pps, resume_file=resume_file, run_id=run_id,
                               started_at=time.time(), updated_at=time.time(),
                               last_error=None)
            self._flush()

    def progress(self, *, probes_sent: int | None = None, open_found: int | None = None,
                 segment: int | None = None, rate_pps: int | None = None,
                 resume_file: str | None = None) -> None:
        with self._lock:
            if probes_sent is not None:
                self._state["probes_sent"] = probes_sent
            if open_found is not None:
                self._state["open_found"] = open_found
            if segment is not None:
                self._state["segment"] = segment
            if rate_pps is not None:
                self._state["rate_pps"] = rate_pps
            if resume_file is not None:
                self._state["resume_file"] = resume_file
            self._state["updated_at"] = time.time()
            self._flush()

    def transition(self, state: str, *, error: str | None = None) -> None:
        if state not in self.STATES:
            raise ValueError(f"非法状态: {state}")
        with self._lock:
            self._state["state"] = state
            self._state["updated_at"] = time.time()
            if error:
                self._state["last_error"] = str(error)[:500]
            self._flush()

    def resumable(self) -> bool:
        """是否存在可续扫的未完成扫描。"""
        with self._lock:
            return self._state["state"] in ("PAUSED", "FAILED") and bool(
                self._state.get("resume_file") or self._state.get("targets"))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)
