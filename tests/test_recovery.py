"""异常恢复测试：进程崩溃 / 磁盘满 / 网络中断 / 队列背压。

模拟真实故障场景，验证系统行为符合设计：不丢已确认数据、状态可恢复、
错误被上报而非吞掉。
"""
import json
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.config import load
from orchestrator.concurrency import AsyncConsumer, SupervisedProcess, ThreadPoolConsumer
from orchestrator.errors import ErrorReporter
from orchestrator.persistence import ScanStateMachine, StateStore
from orchestrator.state import ModuleRegistry
from storage.writer import ShardWriter, StorageWriter
from storage.compactor import iter_ndjson_zst


class TestProcessCrashRecovery(unittest.TestCase):
    """崩溃恢复：独立进程写状态后被 kill，新进程恢复续扫。"""

    def test_scan_state_survives_sigkill(self):
        tmp = Path(tempfile.mkdtemp())
        state_file = tmp / "scan.json"
        # 子进程：begin_run + progress 后立即 _exit(1)（模拟断电/被 kill）
        code = f"""
import sys; sys.path.insert(0, {str(Path(__file__).resolve().parent.parent)!r})
from orchestrator.persistence import ScanStateMachine
sm = ScanStateMachine({str(state_file)!r})
sm.begin_run(['203.0.113.0/29'], '80', 8000, 'paused.conf', 'run-crash')
sm.progress(probes_sent=777, segment=4)
import os; os._exit(1)
"""
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True)
        self.assertNotEqual(proc.returncode, 0)
        sm = ScanStateMachine(state_file)
        snap = sm.snapshot()
        self.assertEqual(snap["state"], "PAUSED")
        self.assertEqual(snap["probes_sent"], 777)
        self.assertEqual(snap["segment"], 4)
        self.assertTrue(sm.resumable())

    def test_registry_counters_survive_restart(self):
        tmp = Path(tempfile.mkdtemp())
        store = StateStore(tmp / "rt.json", interval_s=999)
        reg1 = ModuleRegistry()
        reg1.incr("l4_scanner", "open_ports", 1234)
        store.save(reg1.snapshot())
        reg2 = ModuleRegistry()  # 新进程
        reg2.restore(store.load()["modules"])
        self.assertEqual(reg2.snapshot()["l4_scanner"]["counters"]["open_ports"], 1234)


class TestDiskFullRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def test_writer_recovers_after_disk_freed(self):
        """磁盘满 → 暂停；空间恢复 → 自动续写，数据不丢。"""
        cfg = load()
        cfg.data["paths"]["data_raw"] = str(self.tmp / "raw")
        cfg.data["paths"]["data_meta"] = str(self.tmp / "meta")
        q: queue.Queue[dict] = queue.Queue()
        writer = StorageWriter(cfg, {"hosts": q})

        class FakeUsage:
            free = 0  # 从"满"开始

        usage = FakeUsage()
        with mock.patch("storage.writer.shutil.disk_usage", return_value=usage):
            writer.start()
            q.put({"ip": "203.0.113.1", "port": 80})
            time.sleep(0.4)
            self.assertFalse(list((self.tmp / "raw").rglob("part-*")), "磁盘满时不得写入")
            usage.free = 10 ** 10  # 模拟空间释放
            time.sleep(6.0)  # 等待水位轮询（5s 周期）
            writer.stop()
        rows = list(iter_ndjson_zst(next((self.tmp / "raw").rglob("part-*.jsonl.zst"))))
        self.assertEqual(len(rows), 1, "磁盘恢复后缓冲记录必须落盘")

    def test_write_failure_reported_not_swallowed(self):
        reporter = ErrorReporter()
        ErrorReporter._instance = reporter
        # 分片目录不可创建（非法路径字符）：write -> _roll 必须抛 OSError
        w = ShardWriter(self.tmp / "bad<>dir", "hosts", shard_mb=1)
        with self.assertRaises(OSError):
            w.write({"x": 1})
        # 刷盘失败不得抛出，但必须上报（旧实现为 except:pass 静默吞掉）
        w2 = ShardWriter(self.tmp, "s2", shard_mb=1)
        w2.write({"x": 1})
        w2._fh.close()  # 背着 zstd writer 关掉底层文件句柄
        w2.flush_if_due(0.0)
        self.assertTrue(any("刷盘失败" in e["message"] for e in reporter.recent(10)))


class TestNetworkInterruptionRecovery(unittest.TestCase):
    def test_async_consumer_survives_handler_failures(self):
        """网络中断（handler 连续异常）：消费者不死、错误全量上报、恢复后继续处理。"""
        reporter = ErrorReporter()
        ErrorReporter._instance = reporter
        in_q, out_q = queue.Queue(), queue.Queue()
        state = {"fail": True}

        async def flaky_handler(rec):
            if state["fail"] and rec["i"] < 5:
                raise ConnectionError("network unreachable")
            return {**rec, "ok": True}

        consumer = AsyncConsumer("test", in_q, flaky_handler, concurrency=8,
                                 out_q=out_q, module="test")
        consumer.start()
        try:
            for i in range(5):
                in_q.put({"i": i})
            time.sleep(1.5)
            self.assertGreaterEqual(len(reporter.recent(20, module="test")), 5,
                                    "中断期异常必须全部上报")
            state["fail"] = False
            for i in range(5, 10):
                in_q.put({"i": i})
            got = []
            deadline = time.time() + 5
            while len(got) < 5 and time.time() < deadline:
                try:
                    got.append(out_q.get(timeout=0.5))
                except queue.Empty:
                    pass
            self.assertEqual(len(got), 5, "网络恢复后必须继续处理新任务")
        finally:
            consumer.stop()

    def test_thread_consumer_isolates_failures(self):
        reporter = ErrorReporter()
        ErrorReporter._instance = reporter
        in_q, out_q = queue.Queue(), queue.Queue()

        def handler(rec):
            if rec.get("bad"):
                raise ValueError("poison record")
            return rec

        c = ThreadPoolConsumer("t", in_q, handler, workers=2, out_q=out_q, module="t")
        c.start()
        try:
            in_q.put({"bad": True})
            in_q.put({"good": 1})
            self.assertEqual(out_q.get(timeout=5)["good"], 1)
            time.sleep(0.3)
            self.assertTrue(any("poison" in str(e.get("exception")) or "处理失败" in e["message"]
                                for e in reporter.recent(10, module="t")))
        finally:
            c.stop()


class TestSupervisedProcessRecovery(unittest.TestCase):
    def test_crash_restart_with_backoff(self):
        reporter = ErrorReporter()
        ErrorReporter._instance = reporter
        sp = SupervisedProcess("t", lambda: [sys.executable, "-c", "import sys; sys.exit(1)"],
                               module="t", max_restarts=2)
        self.assertTrue(sp.ensure_alive())     # 第 1 次重启
        sp.proc.wait(timeout=15)               # 等待子进程实际退出（启动有延迟）
        self.assertTrue(sp.ensure_alive())     # 第 2 次重启
        sp.proc.wait(timeout=15)
        self.assertFalse(sp.ensure_alive())    # 次数耗尽 → 放弃 + 上报
        self.assertTrue(any("重启次数耗尽" in e["message"] for e in reporter.recent(10)))
        sp.terminate()

    def test_terminate_clean(self):
        sp = SupervisedProcess("t", lambda: [sys.executable, "-c",
                                             "import time; time.sleep(60)"], module="t")
        sp.launch()
        self.assertTrue(sp.ensure_alive())
        t0 = time.time()
        sp.terminate(timeout=3)
        self.assertLess(time.time() - t0, 5)
        self.assertFalse(sp.ensure_alive())


class TestBackpressureIntegrity(unittest.TestCase):
    def test_full_queue_blocks_not_loses(self):
        """有界队列打满：put 阻塞而非丢数据（背压语义）。"""
        q: queue.Queue[dict] = queue.Queue(maxsize=10)
        for i in range(10):
            q.put({"i": i})
        put_done = threading.Event()
        t = threading.Thread(target=lambda: (q.put({"i": 10}), put_done.set()), daemon=True)
        t.start()
        time.sleep(0.3)
        self.assertFalse(put_done.is_set(), "队列满时 put 必须阻塞（背压）")
        q.get()
        self.assertTrue(put_done.wait(2), "消费后生产者必须解除阻塞")
        self.assertEqual(q.qsize(), 10)


if __name__ == "__main__":
    unittest.main()
