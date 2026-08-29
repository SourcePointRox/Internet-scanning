"""状态持久化与断点续扫状态机测试（含崩溃恢复场景）。"""
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.persistence import ScanStateMachine, StateStore
from orchestrator.state import ModuleRegistry


class TestStateStore(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def test_save_load_roundtrip(self):
        store = StateStore(self.tmp / "state.json", interval_s=999)
        reg = ModuleRegistry()
        reg.incr("l4_scanner", "open_ports", 42)
        store.save(reg.snapshot())
        loaded = store.load()
        self.assertEqual(loaded["modules"]["l4_scanner"]["counters"]["open_ports"], 42)

    def test_corrupt_file_ignored(self):
        p = self.tmp / "state.json"
        p.write_text("{not json", encoding="utf-8")
        self.assertEqual(StateStore(p).load(), {})

    def test_atomic_write_no_partial(self):
        """写入使用 tmp+replace：状态文件永远是完整 JSON。"""
        store = StateStore(self.tmp / "s.json", interval_s=999)
        for i in range(20):
            store.save({"modules": {"m": {"counters": {"n": i}}}})
            json.loads((self.tmp / "s.json").read_text(encoding="utf-8"))  # 不抛即完整

    def test_registry_restore_counters(self):
        reg = ModuleRegistry()
        reg.restore({"l4_scanner": {"counters": {"open_ports": 100}, "extra": {}}})
        reg.incr("l4_scanner", "open_ports", 5)
        snap = reg.snapshot()
        self.assertEqual(snap["l4_scanner"]["counters"]["open_ports"], 105)
        self.assertTrue(snap["l4_scanner"]["extra"]["restored_from_previous_run"])


class TestScanStateMachine(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.path = self.tmp / "scan_state.json"

    def test_lifecycle(self):
        sm = ScanStateMachine(self.path)
        sm.begin_run(["203.0.113.0/29"], "80,443", 8000, None, "run-1")
        sm.progress(probes_sent=1000, open_found=5, segment=2)
        sm.transition("COMPLETED")
        snap = sm.snapshot()
        self.assertEqual(snap["state"], "COMPLETED")
        self.assertEqual(snap["probes_sent"], 1000)
        self.assertEqual(snap["segment"], 2)
        self.assertFalse(sm.resumable())

    def test_crash_recovery_running_becomes_paused(self):
        """模拟进程崩溃：RUNNING 状态落盘后进程死亡，重启实例必须降级为 PAUSED 且可续扫。"""
        sm1 = ScanStateMachine(self.path)
        sm1.begin_run(["198.51.100.0/29"], "0-65535", 5000, "paused.conf", "run-x")
        sm1.progress(probes_sent=999, segment=3)
        del sm1  # 模拟进程被杀（无 stop 调用）

        sm2 = ScanStateMachine(self.path)  # 新进程加载
        snap = sm2.snapshot()
        self.assertEqual(snap["state"], "PAUSED", "崩溃时的 RUNNING 必须降级为 PAUSED")
        self.assertEqual(snap["probes_sent"], 999)
        self.assertEqual(snap["resume_file"], "paused.conf")
        self.assertTrue(sm2.resumable())

    def test_failed_is_resumable(self):
        sm = ScanStateMachine(self.path)
        sm.begin_run(["192.0.2.0/29"], "80", 1000, None, "run-y")
        sm.transition("FAILED", error="masscan crashed")
        self.assertTrue(sm.resumable())
        self.assertIn("masscan", sm.snapshot()["last_error"])

    def test_invalid_state_rejected(self):
        sm = ScanStateMachine(self.path)
        with self.assertRaises(ValueError):
            sm.transition("EXPLODED")

    def test_concurrent_progress_threadsafe(self):
        import threading
        sm = ScanStateMachine(self.path)
        sm.begin_run(["10.0.0.0/8"], "80", 100, None, "run-z")
        def bump(n):
            for i in range(n):
                sm.progress(probes_sent=i)
        threads = [threading.Thread(target=bump, args=(50,)) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        json.loads(self.path.read_text(encoding="utf-8"))  # 文件始终完整


if __name__ == "__main__":
    unittest.main()
