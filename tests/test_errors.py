"""错误处理基础设施测试：重试 / 错误上报 / 降级记录。"""
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.errors import ErrorReporter, retry


class TestRetry(unittest.TestCase):
    def setUp(self):
        # 每个用例使用独立 reporter，避免单例状态串扰
        self.reporter = ErrorReporter()
        ErrorReporter._instance = self.reporter

    def test_success_first_try(self):
        calls = []

        @retry(max_attempts=3, backoff_s=0.01, module="test")
        def ok():
            calls.append(1)
            return "done"

        self.assertEqual(ok(), "done")
        self.assertEqual(len(calls), 1)

    def test_transient_retry_then_success(self):
        calls = []

        @retry(max_attempts=3, backoff_s=0.01, module="test")
        def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise TimeoutError("simulated timeout")
            return "recovered"

        self.assertEqual(flaky(), "recovered")
        self.assertEqual(len(calls), 3)

    def test_giveup_reports_and_raises(self):
        @retry(max_attempts=2, backoff_s=0.01, module="test")
        def always_fail():
            raise ConnectionError("network down")

        with self.assertRaises(ConnectionError):
            always_fail()
        events = self.reporter.recent(10, module="test")
        self.assertTrue(any("重试 2 次后失败" in e["message"] for e in events))

    def test_non_transient_not_retried(self):
        calls = []

        @retry(max_attempts=3, backoff_s=0.01, module="test")
        def fatal():
            calls.append(1)
            raise ValueError("programming error")

        with self.assertRaises(ValueError):
            fatal()
        self.assertEqual(len(calls), 1)  # 非瞬时错误不重试

    def test_on_giveup_none_degrades(self):
        @retry(max_attempts=2, backoff_s=0.01, module="test", on_giveup="none")
        def fail_soft():
            raise TimeoutError()

        self.assertIsNone(fail_soft())
        events = self.reporter.recent(10, module="test")
        self.assertTrue(any("降级" in e["message"] for e in events))


class TestErrorReporter(unittest.TestCase):
    def setUp(self):
        self.reporter = ErrorReporter()
        ErrorReporter._instance = self.reporter

    def test_report_structure(self):
        e = self.reporter.report("mod1", "something broke", exc=RuntimeError("x"), target="1.2.3.4")
        self.assertEqual(e["module"], "mod1")
        self.assertIn("RuntimeError", e["exception"])
        self.assertEqual(e["context"]["target"], "1.2.3.4")
        self.assertEqual(self.reporter.counts()["mod1"], 1)

    def test_hook_invoked_and_isolated(self):
        seen = []
        self.reporter.add_hook(lambda ev: seen.append(ev))
        self.reporter.add_hook(lambda ev: 1 / 0)  # 钩子异常不得影响主流程
        self.reporter.report("m", "msg")
        self.assertEqual(len(seen), 1)

    def test_degrade_is_warning(self):
        e = self.reporter.degrade("m", "geoip", "mmdb missing")
        self.assertEqual(e["level"], "warning")
        self.assertIn("geoip", e["message"])

    def test_ring_buffer_bounded(self):
        rep = ErrorReporter(maxlen=10)
        for i in range(50):
            rep.report("m", f"e{i}")
        self.assertEqual(len(rep.recent(100)), 10)


if __name__ == "__main__":
    unittest.main()
