"""存储管线测试：ShardWriter → Compactor → Catalog 全链路 + Schema 演进。

覆盖：分片滚动、zstd 读写、Parquet 类型化列、DuckDB 视图查询、
schema v1→v2 迁移、血缘盖章、压缩清单持久化、磁盘水位保护。
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

import pyarrow.parquet as pq

from orchestrator.config import load
from storage import schema
from storage.catalog import Catalog
from storage.compactor import Compactor, iter_ndjson_zst
from storage.writer import ShardWriter, StorageWriter


def make_cfg(tmp: Path):
    cfg = load()
    cfg.data["paths"]["data_raw"] = str(tmp / "raw")
    cfg.data["paths"]["data_parquet"] = str(tmp / "parquet")
    cfg.data["paths"]["data_meta"] = str(tmp / "meta")
    cfg.data["storage"]["raw_shard_mb"] = 1
    cfg.data["storage"]["flush_interval_s"] = 0.1
    return cfg


class TestShardWriter(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def test_write_read_roundtrip(self):
        w = ShardWriter(self.tmp, "hosts", shard_mb=1)
        for i in range(10):
            w.write({"ip": f"203.0.113.{i}", "port": 443, "ts": 1700000000 + i})
        w.close()
        shards = list(self.tmp.rglob("part-*.jsonl.zst"))
        self.assertEqual(len(shards), 1)
        rows = list(iter_ndjson_zst(shards[0]))
        self.assertEqual(len(rows), 10)
        self.assertEqual(rows[3]["ip"], "203.0.113.3")

    def test_roll_on_size(self):
        w = ShardWriter(self.tmp, "hosts", shard_mb=1)
        big = "x" * 700_000
        for _ in range(4):  # ~2.8MB > 1MB 阈值 → 触发滚动
            w.write({"blob": big})
        w.close()
        shards = list(self.tmp.rglob("part-*.jsonl.zst"))
        self.assertGreaterEqual(len(shards), 2, "超过 shard_mb 必须滚动新分片")

    def test_unicode_and_nested(self):
        w = ShardWriter(self.tmp, "hosts", shard_mb=1)
        w.write({"title": "中文标题 🌐", "tls": {"cert": {"cn": "example.org"}}})
        w.close()
        row = next(iter_ndjson_zst(next(self.tmp.rglob("part-*.jsonl.zst"))))
        self.assertEqual(row["title"], "中文标题 🌐")
        self.assertEqual(row["tls"]["cert"]["cn"], "example.org")


class TestStorageWriterService(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.cfg = make_cfg(self.tmp)

    def test_end_to_end_queue_to_shard(self):
        q: queue.Queue[dict] = queue.Queue()
        writer = StorageWriter(self.cfg, {"hosts": q})
        writer.start()
        for i in range(5):
            q.put({"ip": f"198.51.100.{i}", "port": 80})
        deadline = time.time() + 5
        while time.time() < deadline and not list((self.tmp / "raw").rglob("part-*.jsonl.zst")):
            time.sleep(0.1)
        writer.stop()
        rows = list(iter_ndjson_zst(next((self.tmp / "raw").rglob("part-*.jsonl.zst"))))
        self.assertEqual(len(rows), 5)
        # 血缘盖章：写入侧必须附加 schema_version 与 _lineage
        self.assertEqual(rows[0]["schema_version"], schema.SCHEMA_VERSION)
        self.assertIn("storage_writer", rows[0]["_lineage"]["hops"])

    def test_disk_full_blocks_writes(self):
        """磁盘水位低于阈值：写入暂停 + 告警上报（模拟 shutil.disk_usage）。"""
        q: queue.Queue[dict] = queue.Queue()
        writer = StorageWriter(self.cfg, {"hosts": q})
        fake_usage = mock.Mock(free=0)  # 0 字节剩余
        with mock.patch("storage.writer.shutil.disk_usage", return_value=fake_usage):
            writer.start()
            q.put({"ip": "192.0.2.1", "port": 80})
            time.sleep(0.5)
            writer.stop()
        self.assertFalse(list((self.tmp / "raw").rglob("part-*.jsonl.zst")),
                         "磁盘满时不应产生分片")


class TestCompactorPipeline(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.cfg = make_cfg(self.tmp)

    def _seed_raw(self, rows, stream="hosts"):
        w = ShardWriter(self.tmp / "raw", stream, shard_mb=1)
        for r in rows:
            w.write(r)
        w.close()

    def test_compact_to_parquet_typed_columns(self):
        self._seed_raw([
            {"ip": "203.0.113.1", "port": 443, "ts": 1700000000, "rtt_ms": 12.5,
             "http": {"status": "200"}, "l7_probed": True},
            {"ip": "203.0.113.2", "port": 80, "ts": 1700000001, "new_field": "出现的"},
        ])
        comp = Compactor(self.cfg)
        made = comp.compact_all()
        self.assertEqual(made, 1)
        pfiles = list((self.tmp / "parquet").rglob("*.parquet"))
        self.assertEqual(len(pfiles), 1)
        tbl = pq.read_table(pfiles[0])
        self.assertEqual(tbl.num_rows, 2)
        # 类型化：port/ts/rtt_ms 保留原生类型（不再一律 string）
        types = {f.name: str(f.type) for f in tbl.schema}
        self.assertEqual(types["port"], "int32")
        self.assertEqual(types["ts"], "int64")
        self.assertEqual(types["rtt_ms"], "double")
        self.assertEqual(types["l7_probed"], "bool")
        # 嵌套 dict JSON 化、新字段 string 追加（Schema 演进兼容）
        d = tbl.to_pylist()
        self.assertEqual(json.loads(d[0]["http"])["status"], "200")
        self.assertEqual(d[1]["new_field"], "出现的")

    def test_done_list_persisted(self):
        self._seed_raw([{"ip": "203.0.113.9", "port": 22}])
        comp1 = Compactor(self.cfg)
        self.assertEqual(comp1.compact_all(), 1)
        comp1.stop()
        comp2 = Compactor(self.cfg)  # 新实例（模拟重启）——清单已落盘，不重复压缩
        self.assertEqual(comp2.compact_all(), 0)

    def test_migrate_v1_records(self):
        """v1 旧记录（无 schema_version/_lineage）压缩时必须自动升级。"""
        self._seed_raw([{"ip": "203.0.113.3", "port": 80, "ts": "1700000002"}])
        comp = Compactor(self.cfg)
        comp.compact_all()
        tbl = pq.read_table(next((self.tmp / "parquet").rglob("*.parquet")))
        row = tbl.to_pylist()[0]
        self.assertEqual(row["schema_version"], schema.SCHEMA_VERSION)
        lin = json.loads(row["_lineage"])
        self.assertIn("source", lin)
        self.assertEqual(row["ts"], 1700000002)  # 字符串 ts 规整为 int

    def test_corrupt_shard_isolated(self):
        """损坏分片：上报错误但不中断其他分片。"""
        self._seed_raw([{"ip": "203.0.113.4", "port": 80}])
        bad_dir = self.tmp / "raw" / "2099-01-01" / "hosts"
        bad_dir.mkdir(parents=True)
        (bad_dir / "part-0000.jsonl.zst").write_bytes(b"not zstd data")
        comp = Compactor(self.cfg)
        made = comp.compact_all()
        self.assertEqual(made, 1)  # 正常分片不受影响


class TestCatalogDuckDB(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.cfg = make_cfg(self.tmp)

    def test_duckdb_view_query(self):
        w = ShardWriter(self.tmp / "raw", "hosts", shard_mb=1)
        for i in range(3):
            w.write({"ip": f"203.0.113.{i}", "port": 443, "schema_version": 2,
                     "_lineage": {"source": "test", "hops": ["test"]}})
        w.close()
        Compactor(self.cfg).compact_all()
        cat = Catalog(self.cfg)
        con = cat.duck()
        rows = con.execute("SELECT ip, port FROM hosts ORDER BY ip").fetchall()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][1], 443)

    def test_error_events_persisted(self):
        cat = Catalog(self.cfg)
        cat.log_error({"ts": 1700000000, "module": "l4", "level": "error",
                       "message": "masscan died", "exception": "RuntimeError: x",
                       "context": {"pid": 1234}})
        errors = cat.recent_errors(10)
        self.assertEqual(errors[0]["module"], "l4")
        self.assertIn("masscan", errors[0]["message"])


class TestSchemaMigration(unittest.TestCase):
    def test_stamp_adds_lineage_and_hops(self):
        rec = {"ip": "1.2.3.4"}
        schema.stamp(rec, source="l7_grabber", engine="zgrab2")
        schema.stamp(rec, source="enricher")
        self.assertEqual(rec["schema_version"], 2)
        self.assertEqual(rec["_lineage"]["hops"], ["l7_grabber", "enricher"])
        self.assertEqual(rec["_lineage"]["engine"], "zgrab2")

    def test_stamp_no_duplicate_hop(self):
        rec = {}
        schema.stamp(rec, source="a")
        schema.stamp(rec, source="a")  # 同模块连续盖章不重复
        self.assertEqual(rec["_lineage"]["hops"], ["a"])

    def test_migrate_idempotent(self):
        rec = {"ip": "1.2.3.4", "ts": "bad"}
        once = schema.migrate(dict(rec))
        twice = schema.migrate(dict(once))
        self.assertEqual(once, twice)
        self.assertEqual(once["ts"], 0)  # 非法 ts 安全归零

    def test_coerce_value_types(self):
        self.assertEqual(schema.coerce_value("port", "443"), 443)
        self.assertIsNone(schema.coerce_value("port", "not-a-port"))
        self.assertEqual(schema.coerce_value("rtt_ms", "12.5"), 12.5)
        self.assertEqual(schema.coerce_value("l7_probed", 1), True)
        self.assertEqual(schema.coerce_value("unknown_col", {"a": 1}), '{"a": 1}')


if __name__ == "__main__":
    unittest.main()
