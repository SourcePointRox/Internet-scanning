"""端到端全链路冒烟测试：真实装配 Orchestrator（dry-run），验证数据落盘。

链路：l4(dry-run 模拟) -> l7(python 引擎) -> enrich -> classifier(真实规则库)
      -> storage_writer(NDJSON.zst) -> 落盘校验（schema 盖章 / 分类 / 血缘）。

与单元测试的区别：不 mock 任何模块，走 orchestrator.main.Orchestrator 真实装配，
覆盖"配置加载 -> 启动 -> 背压队列 -> 停机排空 -> 状态落盘"的完整生命周期。
所有数据目录指向 tempfile.mkdtemp()，绝不写仓库目录。
"""
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.main import Orchestrator
from orchestrator.state import REGISTRY

REPO_ROOT = Path(__file__).resolve().parent.parent


def _write_smoke_config(td: str) -> Path:
    """生成冒烟专用配置：数据目录全部指向临时目录，机器相关路径保持仓库相对。"""
    cfg = {
        "project": {"name": "NetAtlas-smoke",
                    "contact_email": "smoke@example.com",
                    "user_agent": "NetAtlas-smoke/1.0"},
        "paths": {"root": "", "bin": "bin",
                  "data_raw": f"{td}/raw", "data_parquet": f"{td}/parquet",
                  "data_meta": f"{td}/meta", "data_geoip": f"{td}/geoip",
                  "seeds": f"{td}/seeds", "logs": f"{td}/logs"},
        "bandwidth": {"upload_mbps": 25.0, "global_cap_pct": 80,
                      "quotas": {"l4_scan": 10.0, "l7_grab": 6.0,
                                 "dns_enrich": 2.0, "reserve": 2.0}},
        "l4": {"scanner": "dry-run", "phase1_ports": "80,443,22",
               "default_rate_pps": 1000, "max_rate_pps": 2000,
               "exclude_file": "config/exclude.txt", "ipv6_enabled": False,
               "cooldown_s": 0.1,
               "rate_adapt": {"enabled": False, "interval_s": 5.0},
               "resume": {"enabled": True,
                          "state_file": f"{td}/meta/scan_state.json",
                          "segment_minutes": 10}},
        "ipv6_tga": {"algorithm": "6tree", "seed_sources": [],
                     "budget_per_round": 1000, "alias_probe_count": 1,
                     "feedback_epsilon": 0.1},
        "l7": {"engine": "python", "protocols": ["http", "https"],
               "ics_protocols": [], "body_limit_bytes": 4096,
               "connect_timeout_ms": 500, "read_timeout_ms": 500,
               "concurrency": 8, "zgrab2_senders": 1,
               "retry": {"max_attempts": 1, "backoff_ms": 10}},
        "enrichment": {"geoip_enabled": False, "asn_enabled": False,
                       "reverse_dns": False, "rtt_measure": False, "threads": 2,
                       "dns": {"engine": "threads", "concurrency": 4,
                               "timeout_ms": 300, "nameservers": []}},
        "classification": {"taxonomy_file": "classification/taxonomy.json",
                           "rules_dir": "classification/rules",
                           "min_confidence": 0.55},
        "storage": {"raw_shard_mb": 1, "flush_interval_s": 0.5,
                    "raw_retention_days": 1, "parquet_compression": "zstd",
                    "parquet_compression_level": 1, "compact_interval_min": 60,
                    "schema_version": 2, "disk_min_free_mb": 1},
        "webui": {"host": "127.0.0.1", "port_range": [18000, 18100],
                  "ws_push_interval_s": 0.5},
        "persistence": {"enabled": True,
                        "file": f"{td}/meta/runtime_state.json", "interval_s": 1},
        "distributed": {"enabled": False, "coordinator": "local",
                        "node_id": "smoke-node", "shard_total": 1, "shard_index": 0},
        "modules_autostart": [],
    }
    path = Path(td) / "smoke_config.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    return path


def _read_shard_records(shard: Path, limit: int = 50) -> list[dict]:
    import zstandard
    records = []
    with open(shard, "rb") as fh, zstandard.ZstdDecompressor().stream_reader(fh) as r:
        for line in r.read().decode("utf-8").splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))
                if len(records) >= limit:
                    break
    return records


class TestSmokePipeline(unittest.TestCase):
    def test_dry_run_end_to_end(self):
        td = tempfile.mkdtemp(prefix="netatlas-smoke-")
        cfg_path = _write_smoke_config(td)
        orch = Orchestrator(str(cfg_path), dry_run=True,
                            targets=["203.0.114.0/30"])

        def counters(module):
            return REGISTRY.snapshot().get(module, {}).get("counters", {})

        base_l4 = counters("l4_scanner").get("open_ports", 0)
        base_classified = counters("classifier").get("classified", 0)

        orch.start_pipeline()
        try:
            # 等待记录流过整条链路（dry-run 每秒 5 条伪发现）
            deadline = time.time() + 30
            while time.time() < deadline:
                if counters("classifier").get("classified", 0) - base_classified >= 3:
                    break
                time.sleep(0.5)
            else:
                self.fail("30s 内无记录到达分类器，流水线不通")
            # 等写入器至少落一批
            deadline = time.time() + 10
            while time.time() < deadline:
                if counters("storage_writer").get("hosts_records", 0) >= 1:
                    break
                time.sleep(0.3)
        finally:
            orch.stop_pipeline()  # 停机应排空各队列并 flush 落盘

        # ---- 1) L4 dry-run 确实产出了伪发现 ----
        self.assertGreater(counters("l4_scanner").get("open_ports", 0) - base_l4, 0,
                           "L4 dry-run 未产出任何伪发现")

        # ---- 2) 原始层落盘：NDJSON.zst 分片存在且非空 ----
        raw_dir = Path(td) / "raw"
        shards = list(raw_dir.rglob("part-*.jsonl.zst"))
        self.assertTrue(shards, f"原始层无分片落盘: {raw_dir}")
        records = _read_shard_records(shards[0])
        self.assertTrue(records, "分片存在但读不出记录")

        # ---- 3) 记录完整性：schema 盖章 + 血缘 + 分类结果 ----
        rec = records[0]
        self.assertEqual(rec.get("schema_version"), 2, "缺少 schema v2 盖章")
        self.assertIn("_lineage", rec, "缺少数据血缘 _lineage")
        self.assertIn("source", rec["_lineage"])
        self.assertIn("classification", rec, "缺少分类结果")
        self.assertIn("category_path", rec["classification"])
        self.assertEqual(rec.get("engine"), "dry-run")

        # ---- 4) 状态持久化：扫描状态机 + REGISTRY 快照均落盘 ----
        scan_state = json.loads(
            (Path(td) / "meta" / "scan_state.json").read_text(encoding="utf-8"))
        self.assertEqual(scan_state["state"], "PAUSED", "停机后扫描状态应为 PAUSED")
        self.assertEqual(scan_state["targets"], ["203.0.114.0/30"])
        runtime_state = json.loads(
            (Path(td) / "meta" / "runtime_state.json").read_text(encoding="utf-8"))
        self.assertIn("modules", runtime_state)
        self.assertIn("l4_scanner", runtime_state["modules"])

        # ---- 5) 元数据目录：Catalog SQLite 已建 ----
        self.assertTrue(any((Path(td) / "meta").glob("*.db"))
                        or any((Path(td) / "meta").glob("*.sqlite*")),
                        "Catalog 元数据库未创建")


if __name__ == "__main__":
    unittest.main()
