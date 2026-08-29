"""L4Scanner 集成测试：假 masscan 进程驱动真实子进程管理路径。

覆盖：JSON 流解析 → 队列、断点续扫状态机、崩溃自动重启 + --resume、
暂停/续扫控制、dry-run 模拟、scapy 端口表达式解析、目标遍历洗牌。
"""
import json
import os
import queue
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.l4_scanner import ExcludeList, L4Scanner
from modules.l4_scapy import iter_targets, parse_ports
from orchestrator.config import load
from orchestrator.persistence import ScanStateMachine
from orchestrator.ratecontrol import RateController

FAKE = str(Path(__file__).resolve().parent / "fake_masscan.py")
TESTS_DIR = Path(__file__).resolve().parent


def make_cfg(tmp: Path) -> object:
    cfg = load()
    cfg.data["paths"]["data_meta"] = str(tmp / "meta")
    cfg.data["paths"]["data_raw"] = str(tmp / "raw")
    cfg.data["l4"]["resume"]["state_file"] = str(tmp / "scan_state.json")
    cfg.data["l4"]["resume"]["segment_minutes"] = 0.05   # 3s 分片（须大于子进程冷启动 ~1.5s）
    return cfg


class L4TestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.argv_log = self.tmp / "argv.log"
        self.env_patch = mock.patch.dict(os.environ, {
            "FAKE_MASSCAN_ARGV_LOG": str(self.argv_log),
        })
        self.env_patch.start()
        self.q: queue.Queue[dict] = queue.Queue()

    def tearDown(self):
        self.env_patch.stop()

    def make_scanner(self, mode: str, **kw):
        os.environ["FAKE_MASSCAN_MODE"] = mode
        cfg = make_cfg(self.tmp)
        scan_state = ScanStateMachine(self.tmp / "scan_state.json")
        rc = RateController(initial_pps=8000, max_pps=18000)
        scanner = L4Scanner(cfg, self.q, dry_run=True,  # dry_run 跳过 __init__ 后端探测
                            scan_state=scan_state, rate_controller=rc, **kw)
        # 注入假 masscan：实例级补丁，确保 start() 重新探测时仍然生效
        scanner.dry_run = False
        scanner.masscan = sys.executable
        scanner._find_masscan = lambda _bd: sys.executable
        orig = scanner._build_cmd
        scanner._build_cmd = lambda t, p, r: [sys.executable, FAKE] + orig(t, p, r)[1:]
        return scanner, scan_state

    def read_argv_log(self) -> list[list[str]]:
        if not self.argv_log.exists():
            return []
        return [json.loads(l) for l in self.argv_log.read_text(encoding="utf-8").splitlines()]

    def drain(self, timeout=5.0) -> list[dict]:
        out = []
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                out.append(self.q.get(timeout=0.2))
            except queue.Empty:
                if out:
                    break
        return out


class TestMasscanIntegration(L4TestBase):
    def test_quick_scan_records_flow_and_complete(self):
        scanner, sm = self.make_scanner("quick")
        scanner.start(targets=["203.0.114.0/29"], ports="80,443")
        records = self.drain()
        for _ in range(40):  # 等待状态机落定
            if sm.snapshot()["state"] == "COMPLETED":
                break
            time.sleep(0.1)
        scanner.stop()
        self.assertGreaterEqual(len(records), 5)
        self.assertEqual(records[0]["ip"], "203.0.114.1")
        self.assertEqual(records[0]["port"], 80)
        self.assertEqual(records[0]["engine"], "masscan")
        self.assertEqual(sm.snapshot()["state"], "COMPLETED")
        # masscan 命令行合规性：必须携带排除列表与速率
        argv = self.read_argv_log()[0]
        self.assertIn("--excludefile", argv)
        self.assertIn("--rate", argv)
        self.assertIn("-p80,443", argv)

    def test_crash_triggers_restart_then_failed(self):
        scanner, sm = self.make_scanner("crash")
        # 加速崩溃重启退避（生产 2/4/8/16/30s → 测试 0.05s）
        real_sleep = time.sleep
        with mock.patch("modules.l4_scanner.time.sleep", lambda s: real_sleep(min(s, 0.05))):
            scanner.start(targets=["203.0.114.0/29"], ports="80")
            deadline = time.time() + 30
            while time.time() < deadline:
                if sm.snapshot()["state"] == "FAILED":
                    break
                time.sleep(0.3)
            scanner.stop()
        state = sm.snapshot()
        self.assertEqual(state["state"], "FAILED")
        runs = self.read_argv_log()
        self.assertGreaterEqual(len(runs), 2, "崩溃后必须自动重启")

    def test_resume_after_crash_uses_resume_file(self):
        """resume-crash 模式：首次崩溃（留下 paused.conf），重启必须带 --resume 并成功。"""
        scanner, sm = self.make_scanner("resume-crash")
        scanner.start(targets=["203.0.114.0/29"], ports="80")
        deadline = time.time() + 30
        while time.time() < deadline:
            if sm.snapshot()["state"] in ("COMPLETED", "FAILED"):
                break
            time.sleep(0.3)
        scanner.stop()
        self.assertEqual(sm.snapshot()["state"], "COMPLETED")
        runs = self.read_argv_log()
        self.assertGreaterEqual(len(runs), 2)
        self.assertFalse(any("--resume" in r for r in runs[:1]), "首跑不应带 --resume")
        self.assertTrue(any("--resume" in r for r in runs[1:]), "崩溃重启必须携带 --resume")

    def test_pause_resume_cycle(self):
        scanner, sm = self.make_scanner("hang")
        scanner.start(targets=["203.0.114.0/29"], ports="80")
        time.sleep(1.0)
        scanner.pause()
        for _ in range(30):
            if sm.snapshot()["state"] == "PAUSED":
                break
            time.sleep(0.1)
        self.assertEqual(sm.snapshot()["state"], "PAUSED")
        scanner.resume()
        time.sleep(1.0)
        scanner.stop()
        self.assertGreaterEqual(len(self.read_argv_log()), 2, "续扫必须重启进程")

    def test_set_rate_applied_on_segment_restart(self):
        scanner, sm = self.make_scanner("hang")
        scanner.start(targets=["203.0.114.0/29"], ports="80")
        time.sleep(1.0)
        scanner.set_rate(2000)
        time.sleep(8.0)  # 跨过一个 3s 分片边界（含优雅终止等待 ~2s）
        scanner.stop()
        runs = self.read_argv_log()
        rates = [r[r.index("--rate") + 1] for r in runs if "--rate" in r]
        self.assertIn("2000", rates, f"分片重启后必须使用新速率，实际: {rates}")


class TestDryRun(L4TestBase):
    def test_dry_run_simulation(self):
        cfg = make_cfg(self.tmp)
        sm = ScanStateMachine(self.tmp / "scan_state.json")
        scanner = L4Scanner(cfg, self.q, dry_run=True, scan_state=sm)
        scanner.start(targets=["203.0.114.0/29"])
        records = self.drain(timeout=3.0)
        scanner.stop()
        self.assertTrue(all(r.get("simulated") for r in records))
        self.assertEqual({r["port"] for r in records}, {80, 443, 22, 8080, 21})


class TestScapyHelpers(unittest.TestCase):
    def test_parse_ports(self):
        self.assertEqual(parse_ports("21-23,80,443,80"), [21, 22, 23, 80, 443])
        self.assertEqual(parse_ports(""), [])

    def test_iter_targets_ipv4(self):
        ips = list(iter_targets(["192.0.2.0/30"]))
        self.assertEqual(sorted(ips), ["192.0.2.1", "192.0.2.2"])

    def test_iter_targets_skips_ipv6(self):
        self.assertEqual(list(iter_targets(["2001:db8::/126"])), [])

    def test_exclude_list_invalid_line(self):
        tmp = Path(tempfile.mkdtemp()) / "exclude.txt"
        tmp.write_text("10.0.0.0/8\nnot-a-cidr\n# comment\n", encoding="utf-8")
        ex = ExcludeList(tmp)
        self.assertEqual(len(ex.networks), 1)  # 非法条目被忽略


if __name__ == "__main__":
    unittest.main()
