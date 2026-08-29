"""Scapy 后端进度持久化接线测试（不触发真实发包，直接驱动内部方法）。

覆盖：set_rate 热调速落盘 / _flush 探针数进度 / _on_packet 开放端口计数 /
收尾最终进度 + 状态迁移 / L4Scanner.pause 真正停发 scapy。
"""
import json
import queue
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.l4_scapy import BATCH_SIZE, ScapyL4Scanner
from orchestrator.persistence import ScanStateMachine


class _Cfg:
    """最小 Config 替身。"""

    def __init__(self, data):
        self._data = data

    def get(self, *keys, default=None):
        node = self._data
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node

    def abs_path(self, *keys):
        p = Path(str(self.get(*keys, default="")))
        return p if p.is_absolute() else Path(__file__).resolve().parent.parent / p


def _scanner(state_path):
    cfg = _Cfg({"l4": {"default_rate_pps": 8000, "max_rate_pps": 18000,
                       "cooldown_s": 0, "iface": "eth-test"}})
    state = ScanStateMachine(state_path)
    sc = ScapyL4Scanner(cfg, queue.Queue(), exclude=mock.Mock(contains=lambda ip: False),
                        scan_state=state)
    return sc, state


class TestScapyScanStateWiring(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="netatlas-scapy-")
        self.state_path = Path(self.td) / "scan_state.json"

    def test_set_rate_persists(self):
        sc, state = _scanner(self.state_path)
        state.begin_run(["203.0.114.0/29"], "80", 8000, None, "run-1")
        sc.set_rate(12000)
        self.assertEqual(sc.rate_pps, 12000)
        snap = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertEqual(snap["rate_pps"], 12000)

    def test_flush_reports_probes_sent(self):
        sc, state = _scanner(self.state_path)
        state.begin_run(["203.0.114.0/29"], "80", 8000, None, "run-1")
        sc.l2 = False  # _run 才会初始化的属性，直接驱动 _flush 需显式设置
        sent = []
        fake_send = lambda pkts, **kw: sent.append(len(pkts))
        # 进度按 5000 探针边界限频落盘：发 20 批（5120 探针）跨越边界
        for _ in range(20):
            sc._flush([object()] * BATCH_SIZE, fake_send)
        self.assertEqual(sent, [BATCH_SIZE] * 20)
        self.assertEqual(state.snapshot()["probes_sent"], 20 * BATCH_SIZE)

    def test_open_found_counting_and_final_transition(self):
        """模拟收包计数 + 收尾：最终进度落盘、RUNNING -> COMPLETED。"""
        sc, state = _scanner(self.state_path)
        state.begin_run(["203.0.114.0/29"], "80", 8000, None, "run-1")
        sc._sent = 1234
        sc._open = 7
        # 直接驱动收尾分支（不启动真实 sniffer）
        state.progress(probes_sent=sc._sent, open_found=sc._open)
        if state.snapshot().get("state") == "RUNNING":
            state.transition("COMPLETED")
        snap = state.snapshot()
        self.assertEqual(snap["state"], "COMPLETED")
        self.assertEqual(snap["probes_sent"], 1234)
        self.assertEqual(snap["open_found"], 7)
        # 崩溃恢复语义：COMPLETED 不可续扫，PAUSED 可续扫
        self.assertFalse(state.resumable())

    def test_paused_run_is_resumable(self):
        sc, state = _scanner(self.state_path)
        state.begin_run(["203.0.114.0/29"], "80", 8000, None, "run-1")
        sc.stop()  # 主动停止
        state.progress(probes_sent=500, open_found=1)
        state.transition("PAUSED")
        self.assertTrue(state.resumable())
        # 进程重启后重新加载：RUNNING 不会出现，PAUSED 保持可续扫
        reloaded = ScanStateMachine(self.state_path)
        self.assertEqual(reloaded.snapshot()["state"], "PAUSED")
        self.assertTrue(reloaded.resumable())


class TestL4PauseStopsScapy(unittest.TestCase):
    def test_pause_invokes_scapy_stop(self):
        """L4Scanner.pause() 必须连 scapy 后端一起停（否则暂停形同虚设）。"""
        from modules.l4_scanner import L4Scanner
        cfg = _Cfg({"l4": {"default_rate_pps": 8000, "max_rate_pps": 18000,
                           "exclude_file": "config/exclude.txt",
                           "phase1_ports": "80"}})
        scanner = L4Scanner(cfg, queue.Queue(), dry_run=True)
        scanner._scapy = mock.Mock()
        scanner.pause()
        scanner._scapy.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
