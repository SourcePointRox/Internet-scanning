"""全局令牌桶带宽控制器。

设计要点（对应开发方案 §4.6）：
- 一个全局令牌桶约束总上行（默认 = 物理上行 * global_cap_pct）；
- 各消费者（l4/l7/dns）持独立子桶，配额来自 config.bandwidth.quotas；
- WebUI 可在运行时调整全局上限与各配额（下限 5%，上限 100%）。
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


class TokenBucket:
    """线程安全令牌桶，速率单位：字节/秒。"""

    def __init__(self, rate_bps: float, burst_factor: float = 1.5):
        self._lock = threading.Lock()
        self.rate = max(1.0, rate_bps)
        self.capacity = self.rate * burst_factor
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self.consumed_total = 0

    def set_rate(self, rate_bps: float) -> None:
        with self._lock:
            self.rate = max(1.0, rate_bps)
            self.capacity = self.rate * 1.5
            self.tokens = min(self.tokens, self.capacity)

    def consume(self, nbytes: int, block: bool = True, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= nbytes:
                    self.tokens -= nbytes
                    self.consumed_total += nbytes
                    return True
            if not block or time.monotonic() > deadline:
                return False
            time.sleep(min(0.01, nbytes / self.rate))

    def try_consume(self, nbytes: int) -> bool:
        return self.consume(nbytes, block=False)


@dataclass
class BandwidthController:
    """全局带宽分配与统计。"""

    upload_mbps: float = 25.0
    cap_pct: float = 80.0
    quotas_mbps: dict[str, float] = field(default_factory=lambda: {
        "l4_scan": 12.0, "l7_grab": 8.0, "dns_enrich": 2.0, "reserve": 3.0,
    })

    def __post_init__(self):
        self._lock = threading.RLock()  # 可重入：snapshot 内部会嵌套调用统计方法
        self.global_bucket = TokenBucket(self._global_bytes_per_s())
        self.buckets: dict[str, TokenBucket] = {
            name: TokenBucket(mbps * 1e6 / 8) for name, mbps in self.quotas_mbps.items()
        }
        self._window: list[tuple[float, dict[str, int]]] = []  # 滑动窗口吞吐样本

    def _global_bytes_per_s(self) -> float:
        return self.upload_mbps * 1e6 / 8 * (self.cap_pct / 100.0)

    # ---- 消费接口 ----
    def acquire(self, consumer: str, nbytes: int, block: bool = True) -> bool:
        bucket = self.buckets.get(consumer)
        if bucket and not bucket.consume(nbytes, block=block):
            return False
        ok = self.global_bucket.consume(nbytes, block=block)
        if ok:
            self._sample(consumer, nbytes)
        return ok

    # ---- 动态调整（WebUI） ----
    def set_upload_mbps(self, mbps: float) -> None:
        with self._lock:
            self.upload_mbps = max(0.5, min(mbps, 10000.0))
            self.global_bucket.set_rate(self._global_bytes_per_s())

    def set_cap_pct(self, pct: float) -> None:
        with self._lock:
            self.cap_pct = max(5.0, min(pct, 100.0))
            self.global_bucket.set_rate(self._global_bytes_per_s())

    def set_quota(self, consumer: str, mbps: float) -> None:
        with self._lock:
            self.quotas_mbps[consumer] = max(0.1, mbps)
            self.buckets.setdefault(consumer, TokenBucket(1)).set_rate(mbps * 1e6 / 8)

    # ---- 统计 ----
    def _sample(self, consumer: str, nbytes: int) -> None:
        now = time.time()
        with self._lock:
            self._window.append((now, {consumer: nbytes}))
            cutoff = now - 60
            while self._window and self._window[0][0] < cutoff:
                self._window.pop(0)

    def throughput_mbps(self, seconds: float = 5.0) -> dict[str, float]:
        now = time.time()
        totals: dict[str, int] = {}
        with self._lock:
            samples = [s for s in self._window if s[0] >= now - seconds]
        for _, parts in samples:
            for k, v in parts.items():
                totals[k] = totals.get(k, 0) + v
        span = max(seconds, 1e-6)
        return {k: v * 8 / 1e6 / span for k, v in totals.items()}

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "upload_mbps": self.upload_mbps,
                "cap_pct": self.cap_pct,
                "global_cap_mbps": round(self._global_bytes_per_s() * 8 / 1e6, 2),
                "quotas_mbps": dict(self.quotas_mbps),
                "throughput": self.throughput_mbps(),
                "consumed_mb_total": {k: round(b.consumed_total / 1e6, 1) for k, b in self.buckets.items()},
            }
