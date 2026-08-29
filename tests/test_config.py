"""配置系统测试：环境变量插值 / 覆盖 / 校验 / 可移植性。"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.config import Config, load


class TestConfigInterpolation(unittest.TestCase):
    def _write(self, text: str) -> str:
        f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8")
        f.write(text)
        f.close()
        return f.name

    def test_env_placeholder(self):
        os.environ["NETATLAS_TEST_MAIL"] = "real@example.com"
        try:
            path = self._write('project:\n  contact_email: "${NETATLAS_TEST_MAIL:x@example.org}"\n')
            cfg = Config(path)
            self.assertEqual(cfg.get("project", "contact_email"), "real@example.com")
        finally:
            del os.environ["NETATLAS_TEST_MAIL"]

    def test_placeholder_default(self):
        os.environ.pop("NETATLAS_MISSING_VAR", None)
        path = self._write('project:\n  contact_email: "${NETATLAS_MISSING_VAR:fallback@example.org}"\n')
        cfg = Config(path)
        self.assertEqual(cfg.get("project", "contact_email"), "fallback@example.org")

    def test_env_override_nested(self):
        os.environ["NETATLAS_L4__DEFAULT_RATE_PPS"] = "12345"
        os.environ["NETATLAS_L4__IPV6_ENABLED"] = "false"
        try:
            cfg = load()
            self.assertEqual(cfg.get("l4", "default_rate_pps"), 12345)   # 类型转换 int
            self.assertIs(cfg.get("l4", "ipv6_enabled"), False)          # 类型转换 bool
        finally:
            del os.environ["NETATLAS_L4__DEFAULT_RATE_PPS"]
            del os.environ["NETATLAS_L4__IPV6_ENABLED"]

    def test_no_hardcoded_machine_values(self):
        """仓库内配置不得残留机器相关硬编码（盘符路径/内网 IP/MAC）。"""
        cfg = load()
        root = str(cfg.get("paths", "root", default="") or "")
        self.assertNotRegex(root, r"^[A-Za-z]:[/\\]", "paths.root 不得硬编码盘符")
        self.assertIn(cfg.get("l4", "source_ip") or "", ("", None))
        self.assertIn(cfg.get("l4", "router_mac") or "", ("", None))
        # root 为空时必须可移植地回退到仓库根
        self.assertTrue(cfg.root.exists())

    def test_validate_quota_sum(self):
        path = self._write(
            "bandwidth:\n  upload_mbps: 10\n  global_cap_pct: 50\n"
            "  quotas:\n    a: 6.0\n    b: 6.0\n"
            "project:\n  contact_email: 'real@example.com'\n"
            "l4:\n  source_ip: ''\n  router_mac: ''\n")
        cfg = Config(path)
        warnings = cfg.validate()
        self.assertTrue(any("配额总和" in w for w in warnings))

    def test_validate_l2_pairing(self):
        path = self._write(
            "l4:\n  source_ip: '10.0.0.5'\n  router_mac: ''\n"
            "project:\n  contact_email: 'real@example.com'\n"
            "bandwidth:\n  upload_mbps: 25\n  global_cap_pct: 80\n  quotas: {}\n")
        cfg = Config(path)
        warnings = cfg.validate()
        self.assertTrue(any("同时配置" in w for w in warnings))

    def test_missing_config_raises(self):
        with self.assertRaises(FileNotFoundError):
            Config("no/such/config.yaml")


if __name__ == "__main__":
    unittest.main()
