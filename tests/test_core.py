"""核心组件单元测试：带宽控制器 / 排除列表 / 分类引擎 / 存储目录。"""
import json
import queue
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.bandwidth import BandwidthController, TokenBucket
from orchestrator.config import load
from modules.l4_scanner import ExcludeList
from modules.classifier import Classifier
from storage.catalog import Catalog


class TestTokenBucket(unittest.TestCase):
    def test_rate_limit(self):
        bucket = TokenBucket(rate_bps=1000, burst_factor=1.0)  # 容量=1000B
        self.assertTrue(bucket.try_consume(1000))              # 耗尽令牌
        t0 = time.monotonic()
        self.assertTrue(bucket.consume(500, timeout=3.0))      # 需等待 ~0.5s 补充
        self.assertGreater(time.monotonic() - t0, 0.2)

    def test_bandwidth_controller(self):
        bw = BandwidthController(upload_mbps=25.0, cap_pct=80.0)
        self.assertTrue(bw.acquire("l4_scan", 1000))
        snap = bw.snapshot()
        self.assertEqual(snap["global_cap_mbps"], 20.0)
        bw.set_upload_mbps(50.0)
        self.assertEqual(bw.snapshot()["global_cap_mbps"], 40.0)
        bw.set_quota("l7_grab", 4.0)
        self.assertEqual(bw.quotas_mbps["l7_grab"], 4.0)


class TestExcludeList(unittest.TestCase):
    def test_exclusions(self):
        ex = ExcludeList(Path("config/exclude.txt"))
        for ip in ["10.1.2.3", "192.168.1.1", "127.0.0.1", "224.0.0.1", "172.16.5.5"]:
            self.assertTrue(ex.contains(ip), f"{ip} 应被排除")
        self.assertFalse(ex.contains("8.8.8.8"))
        self.assertTrue(ex.contains("not-an-ip"))  # 非法输入安全默认排除


class TestClassifier(unittest.TestCase):
    def _make(self):
        cfg = load()
        tmp = tempfile.mkdtemp()
        cfg.data["paths"]["data_meta"] = tmp
        cat = Catalog(cfg)
        return Classifier(cfg, queue.Queue(), cat)

    def test_open_directory(self):
        clf = self._make()
        rec = {"ip": "203.0.113.5", "port": 80, "protocol": "http",
               "http": {"status": "HTTP/1.1 200 OK", "headers": {"server": "nginx"},
                        "title": "Index of /Gaia"},
               "body_sample": "Parent Directory  gdr3/  2023-01-01"}
        path, conf, signals = clf.classify(rec)
        self.assertEqual(path[-1], "Open Directory (autoindex)")
        self.assertGreaterEqual(conf, 0.9)
        self.assertIn("open-directory", signals)
        # 完整层级必须保留（至少 3 级）
        self.assertGreaterEqual(len(path), 3)

    def test_scientific_mirror(self):
        clf = self._make()
        rec = {"ip": "1.2.3.4", "port": 80, "domain": "cdn.gea.esac.esa.int",
               "protocol": "http",
               "http": {"status": "HTTP/1.1 200 OK", "headers": {}, "title": "Gaia Data Release 3"},
               "body_sample": ""}
        path, conf, _ = clf.classify(rec)
        self.assertEqual(path[0], "Science")

    def test_unknown_below_threshold(self):
        clf = self._make()
        rec = {"ip": "5.6.7.8", "port": 80, "protocol": "http",
               "http": {"status": "HTTP/1.1 200 OK", "headers": {}, "title": "hello"},
               "body_sample": "nothing interesting"}
        path, conf, _ = clf.classify(rec)
        self.assertEqual(path[-1], "Unclassified")


class TestCatalog(unittest.TestCase):
    def test_classification_roundtrip(self):
        cfg = load()
        tmp = tempfile.mkdtemp()
        cfg.data["paths"]["data_meta"] = tmp
        cat = Catalog(cfg)
        cat.upsert_classification("example.org", "1.2.3.4", 443,
                                  ["Technology & Computing", "Internet", "Data Distribution & File Storage",
                                   "CDN Edge Node"], 0.9, ["cdn-edge"])
        stats = cat.classification_stats()
        self.assertEqual(stats[0][0], "CDN Edge Node")
        self.assertEqual(stats[0][1], 1)


if __name__ == "__main__":
    unittest.main()
