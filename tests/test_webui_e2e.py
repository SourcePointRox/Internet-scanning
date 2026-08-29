"""WebUI API 端到端测试：真实 FastAPI app + httpx 客户端 + WebSocket。

覆盖全部 REST 端点与 WS 推送帧结构；编排器用轻量替身（不启动真实流水线），
但带宽控制器 / 状态机 / Catalog 均为真实组件。
"""
import queue
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient

from orchestrator.bandwidth import BandwidthController
from orchestrator.config import load
from orchestrator.persistence import ScanStateMachine
from orchestrator.ratecontrol import RateController
from orchestrator.state import REGISTRY
from storage.catalog import Catalog
from webui.backend.app import create_app, find_free_port


class _Nic:
    def rates(self):
        return {"up_mbps": 1.5, "down_mbps": 3.2}


class _L7:
    engine = "python"


class _L4:
    def __init__(self):
        self.rate_pps = 8000
        self.paused = False
        self.resumed = False

    def set_rate(self, pps):
        self.rate_pps = max(100, min(pps, 18000))

    def pause(self):
        self.paused = True

    def resume(self):
        self.resumed = True


class _FakeOrch:
    def __init__(self, tmp: Path):
        self.cfg = load()
        self.cfg.data["paths"]["data_meta"] = str(tmp / "meta")
        self.cfg.data["paths"]["data_raw"] = str(tmp / "raw")
        self.cfg.data["paths"]["data_parquet"] = str(tmp / "parquet")
        self.bandwidth = BandwidthController(upload_mbps=25.0, cap_pct=80.0)
        self.catalog = Catalog(self.cfg)
        self.nic = _Nic()
        self.l4 = _L4()
        self.l7 = _L7()
        self.dry_run = True
        self.node_id = "test-node-1"
        self.l4_q, self.l7_q = queue.Queue(), queue.Queue()
        self.enrich_q, self.class_q = queue.Queue(), queue.Queue()
        self.scan_state = ScanStateMachine(tmp / "scan.json")
        self.rate_controller = RateController(initial_pps=8000, max_pps=18000)
        self._started = self._stopped = False

    def scan_progress(self):
        return {"scan_state": self.scan_state.snapshot(),
                "rate": self.rate_controller.snapshot(),
                "shard": {"node_id": self.node_id, "total": 1, "index": 0,
                          "coordinator": "LocalShardCoordinator"}}

    def start_pipeline(self):
        self._started = True

    def stop_pipeline(self):
        self._stopped = True


class TestWebUIE2E(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        cls.orch = _FakeOrch(cls.tmp)
        cls.client = TestClient(create_app(cls.orch))

    def test_index_served(self):
        r = self.client.get("/")
        self.assertEqual(r.status_code, 200)
        self.assertIn("NetAtlas", r.text)

    def test_status_payload_structure(self):
        r = self.client.get("/api/status")
        self.assertEqual(r.status_code, 200)
        d = r.json()
        for key in ("modules", "bandwidth", "nic", "queues", "scan", "storage",
                    "dry_run", "engine_l7"):
            self.assertIn(key, d, f"status 缺少字段 {key}")
        self.assertEqual(d["nic"]["up_mbps"], 1.5)
        self.assertTrue(d["dry_run"])

    def test_bandwidth_update(self):
        r = self.client.post("/api/bandwidth", json={"upload_mbps": 50, "cap_pct": 60})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["global_cap_mbps"], 30.0)
        r = self.client.post("/api/bandwidth", json={"quotas": {"l4_scan": 6.0}})
        self.assertEqual(r.json()["quotas_mbps"]["l4_scan"], 6.0)

    def test_scan_rate_control(self):
        r = self.client.post("/api/scan/rate", json={"rate_pps": 5000})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["rate_pps"], 5000)
        r = self.client.post("/api/scan/rate", json={"rate_pps": 50})  # 低于下限 → 422
        self.assertEqual(r.status_code, 422)

    def test_scan_pause_resume(self):
        r = self.client.post("/api/scan/pause")
        self.assertTrue(r.json()["paused"])
        self.assertTrue(self.orch.l4.paused)
        r = self.client.post("/api/scan/resume")
        self.assertTrue(r.json()["resumed"])
        self.assertTrue(self.orch.l4.resumed)

    def test_scan_progress(self):
        r = self.client.get("/api/scan/progress")
        d = r.json()
        self.assertIn("scan_state", d)
        self.assertEqual(d["shard"]["coordinator"], "LocalShardCoordinator")

    def test_module_control_and_conflict(self):
        REGISTRY.register("dummy", lambda: None, lambda: None)
        r = self.client.post("/api/modules/dummy/start")
        self.assertEqual(r.status_code, 200)
        r2 = self.client.post("/api/modules/dummy/start")  # 重复启动 → 409
        self.assertEqual(r2.status_code, 409)
        r3 = self.client.post("/api/modules/dummy/stop")
        self.assertEqual(r3.status_code, 200)

    def test_pipeline_control(self):
        self.assertTrue(self.client.post("/api/pipeline/start").json()["ok"])
        self.assertTrue(self.orch._started)
        self.assertTrue(self.client.post("/api/pipeline/stop").json()["ok"])
        self.assertTrue(self.orch._stopped)

    def test_query_guard(self):
        r = self.client.get("/api/query", params={"sql": "DROP TABLE hosts"})
        self.assertEqual(r.status_code, 400)
        self.assertIn("SELECT", r.json()["error"])

    def test_query_empty_result(self):
        r = self.client.get("/api/query", params={"sql": "SELECT 1 AS one"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["rows"], [[1]])

    def test_errors_endpoint(self):
        from orchestrator.errors import ErrorReporter
        ErrorReporter.get().report("test-mod", "e2e 错误注入", exc=RuntimeError("boom"))
        r = self.client.get("/api/errors?n=10")
        d = r.json()
        self.assertIn("in_memory", d)
        self.assertTrue(any(e["module"] == "test-mod" for e in d["in_memory"]))
        self.assertIn("test-mod", d["counts"])

    def test_feed_endpoint(self):
        from orchestrator import livefeed
        livefeed.push({"ip": "203.0.113.1", "port": 443, "category": "Test"})
        r = self.client.get("/api/feed?n=10")
        self.assertTrue(any(i["ip"] == "203.0.113.1" for i in r.json()["items"]))

    def test_classification_stats(self):
        self.orch.catalog.upsert_classification("h1", "1.2.3.4", 80, ["A", "B"], 0.9, ["s"])
        r = self.client.get("/api/classification/stats")
        self.assertEqual(r.json()["top_categories"][0][0], "B")

    def test_websocket_push(self):
        with self.client.websocket_connect("/ws") as ws:
            frame = ws.receive_json()
        for key in ("bandwidth", "nic", "modules", "queues", "feed", "scan"):
            self.assertIn(key, frame, f"WS 帧缺少 {key}")

    def test_find_free_port(self):
        port = find_free_port("127.0.0.1", 48000, 48010)
        self.assertTrue(48000 <= port <= 48010)
        with self.assertRaises(RuntimeError):
            find_free_port("256.256.256.256", 1, 2)  # 不可绑定地址


if __name__ == "__main__":
    unittest.main()
