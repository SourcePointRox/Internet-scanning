"""动态速率控制器：AIMD（和式增 / 乘式减）反馈调速。

masscan 不支持热调速（--rate 只在启动时生效），本控制器按时间段把扫描
切成"调速分片"，片间用新速率 + --resume 重启 masscan，对外表现为准实时调速。

反馈信号：
- 丢包估计：open_found / probes_sent 的滑动比值骤降 + 网卡利用率逼近上限
  -> 判定拥塞，乘性降速；
- 无拥塞：每周期和式提速 up_step_pct，封顶 max_rate_pps。
"""
from __future__ import annotations

import logging
import threading
import time

log = logging.getLogger("netatlas.ratecontrol")


class RateController:
    """线程安全的 AIMD 速率控制器。"""

    def __init__(self, *, initial_pps: int, max_pps: int, min_pps: int = 100,
                 up_step_pct: float = 10.0, down_factor: float = 0.5,
                 loss_high_pct: float = 5.0, interval_s: float = 10.0):
        self._lock = threading.Lock()
        self.rate = max(min_pps, min(initial_pps, max_pps))
        self.max_pps, self.min_pps = max_pps, min_pps
        self.up_step_pct, self.down_factor = up_step_pct, down_factor
        self.loss_high_pct, self.interval_s = loss_high_pct, interval_s
        self._baseline: tuple[int, int] | None = None  # (probes, opens) 上次采样
        self.history: list[dict] = []                  # 调速决策历史（WebUI 展示）
        self.last_decision = "init"

    def set_rate(self, pps: int) -> int:
        """手动调速（WebUI）。返回实际生效值。"""
        with self._lock:
            self.rate = max(self.min_pps, min(int(pps), self.max_pps))
            self._record("manual", self.rate)
            return self.rate

    def decide(self, *, probes_sent: int, open_found: int,
               nic_util_pct: float | None = None) -> tuple[int, bool]:
        """AIMD 决策。返回 (新速率, 是否变化)。"""
        with self._lock:
            if self._baseline is None:
                # 首次调用仅建立采样基线，不调整速率
                self._baseline = (probes_sent, open_found)
                return self.rate, False
            congested = False
            d_probes = probes_sent - self._baseline[0]
            d_opens = open_found - self._baseline[1]
            if d_probes >= 1000:
                # 丢包率估计：完全无响应即视为限速/拥塞黑洞
                if d_opens == 0:
                    congested = True
            if nic_util_pct is not None and nic_util_pct >= 95.0:
                congested = True
            self._baseline = (probes_sent, open_found)
            old = self.rate
            if congested:
                self.rate = max(self.min_pps, int(self.rate * self.down_factor))
                self.last_decision = "multiplicative-decrease"
            else:
                self.rate = min(self.max_pps, int(self.rate * (1 + self.up_step_pct / 100)))
                self.last_decision = "additive-increase"
            if self.rate != old:
                self._record(self.last_decision, self.rate)
            return self.rate, self.rate != old

    def _record(self, reason: str, rate: int) -> None:
        self.history.append({"ts": time.time(), "reason": reason, "rate_pps": rate})
        del self.history[:-200]

    def snapshot(self) -> dict:
        with self._lock:
            return {"rate_pps": self.rate, "max_pps": self.max_pps,
                    "last_decision": self.last_decision, "history": self.history[-30:]}
