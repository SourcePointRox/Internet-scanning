"""网卡级真实吞吐监控（psutil.net_io_counters 采样）。

背景：masscan / zgrab2 是外部进程，其网络流量不经过 Python 令牌桶，
导致 WebUI 吞吐曲线恒为空。本模块直接采样系统网卡计数器，
得到全机真实上行/下行速率（Mbps），供 WebUI 曲线与配额参考。
"""
from __future__ import annotations

import threading
import time
from collections import deque

try:
    import psutil
except ImportError:  # 优雅降级：无 psutil 时曲线显示为 0
    psutil = None


class NicMonitor:
    def __init__(self, sample_interval: float = 1.0, window_s: int = 120):
        self.interval = sample_interval
        self.samples: deque[tuple[float, float, float]] = deque(maxlen=window_s)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last = None

    def start(self) -> None:
        if psutil is None:
            return
        self._stop.clear()
        self._last = psutil.net_io_counters()
        self._thread = threading.Thread(target=self._run, daemon=True, name="nic-monitor")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            time.sleep(self.interval)
            try:
                cur = psutil.net_io_counters()
                now = time.time()
                sent = cur.bytes_sent - self._last.bytes_sent
                recv = cur.bytes_recv - self._last.bytes_recv
                self._last = cur
                self.samples.append((now, max(sent, 0), max(recv, 0)))
            except Exception:  # noqa: BLE001
                continue

    def rates(self, window: float = 5.0) -> dict[str, float]:
        """最近 window 秒的平均上行/下行 Mbps。"""
        now = time.time()
        sent = recv = 0
        for t, s, r in self.samples:
            if t >= now - window:
                sent += s
                recv += r
        span = max(window, 1e-6)
        return {"up_mbps": round(sent * 8 / 1e6 / span, 3),
                "down_mbps": round(recv * 8 / 1e6 / span, 3)}
