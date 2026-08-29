"""性能基准测试：吞吐 / 延迟 / 内存。

基准断言使用宽松阈值（防回归，非绝对性能承诺），CI 波动容忍 5 倍余量。
结果打印到 stdout 供趋势对比（python -m unittest tests.test_benchmarks -v）。
"""
import json
import queue
import sys
import tempfile
import time
import tracemalloc
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.classifier import Classifier
from orchestrator.bandwidth import BandwidthController
from orchestrator.config import load
from storage import schema
from storage.catalog import Catalog
from storage.writer import ShardWriter


def bench(fn, n, warmup=0):
    for _ in range(warmup):
        fn(0)
    t0 = time.perf_counter()
    for i in range(n):
        fn(i)
    return time.perf_counter() - t0


class TestThroughputBenchmarks(unittest.TestCase):
    def test_shard_writer_throughput(self):
        """原始层写入吞吐：基准 ~10k 记录 < 3s（zstd 流式压缩）。"""
        tmp = Path(tempfile.mkdtemp())
        w = ShardWriter(tmp, "bench", shard_mb=256)
        rec = {"ip": "203.0.113.1", "port": 443, "protocol": "https",
               "banner": "x" * 200, "ts": 1700000000}
        n = 10_000
        elapsed = bench(lambda i: w.write({**rec, "seq": i}), n, warmup=100)
        w.close()
        rate = n / elapsed
        print(f"\n[bench] ShardWriter: {rate:,.0f} rec/s ({n} recs in {elapsed:.2f}s)")
        self.assertGreater(rate, 3_000, "写入吞吐回归")

    def test_json_serialization_throughput(self):
        """血缘盖章 + JSON 序列化开销：> 50k rec/s。"""
        rec = {"ip": "203.0.113.1", "port": 443, "http": {"status": "200", "headers": {}}}
        def work(i):
            r = dict(rec)
            schema.stamp(r, source="bench")
            json.dumps(r)
        elapsed = bench(work, 20_000)
        rate = 20_000 / elapsed
        print(f"\n[bench] stamp+json: {rate:,.0f} rec/s")
        self.assertGreater(rate, 30_000)

    def test_token_bucket_throughput(self):
        """令牌桶非阻塞消费：> 500k ops/s（限速路径不得成为瓶颈）。"""
        bw = BandwidthController(upload_mbps=10000, cap_pct=100)
        elapsed = bench(lambda i: bw.acquire("l4_scan", 64, block=False), 100_000)
        rate = 100_000 / elapsed
        print(f"\n[bench] TokenBucket.acquire: {rate:,.0f} ops/s")
        self.assertGreater(rate, 200_000)

    def test_classifier_throughput(self):
        """分类引擎：> 5k rec/s（规则正则匹配路径）。"""
        tmp = Path(tempfile.mkdtemp())
        cfg = load()
        cfg.data["paths"]["data_meta"] = str(tmp)
        clf = Classifier(cfg, queue.Queue(), Catalog(cfg))
        rec = {"ip": "203.0.113.5", "port": 80, "protocol": "http",
               "http": {"status": "HTTP/1.1 200 OK", "headers": {"server": "nginx"},
                        "title": "Index of /data"},
               "body_sample": "Parent Directory file.tar.gz"}
        elapsed = bench(lambda i: clf.classify(rec), 5_000, warmup=50)
        rate = 5_000 / elapsed
        print(f"\n[bench] Classifier: {rate:,.0f} rec/s")
        self.assertGreater(rate, 2_000)


class TestLatencyBenchmarks(unittest.TestCase):
    def test_queue_handoff_latency(self):
        """队列交接 p99 延迟 < 5ms（背压路径的基础开销）。"""
        q: queue.Queue[dict] = queue.Queue(maxsize=10_000)
        lat = []
        for i in range(1000):
            t0 = time.perf_counter()
            q.put({"i": i})
            q.get()
            lat.append((time.perf_counter() - t0) * 1000)
        lat.sort()
        p50, p99 = lat[len(lat) // 2], lat[int(len(lat) * 0.99)]
        print(f"\n[bench] queue handoff: p50={p50:.3f}ms p99={p99:.3f}ms")
        self.assertLess(p99, 5.0)


class TestMemoryBenchmarks(unittest.TestCase):
    def test_shard_writer_memory_bounded(self):
        """写入 50k 记录的峰值内存增量 < 50MB（流式，不得堆积）。"""
        tmp = Path(tempfile.mkdtemp())
        w = ShardWriter(tmp, "bench", shard_mb=256)
        rec = {"ip": "203.0.113.1", "port": 443, "banner": "y" * 500}
        tracemalloc.start()
        before, _ = tracemalloc.get_traced_memory()
        for i in range(50_000):
            w.write({**rec, "seq": i})
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        w.close()
        growth_mb = (peak - before) / 1e6
        print(f"\n[bench] ShardWriter 50k recs peak growth: {growth_mb:.1f} MB")
        self.assertLess(growth_mb, 50.0, "内存增量回归（疑似缓冲堆积）")

    def test_feed_buffer_bounded(self):
        from orchestrator import livefeed
        tracemalloc.start()
        for i in range(5000):
            livefeed.push({"ip": f"203.0.113.{i % 255}", "port": 80})
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertLess(peak / 1e6, 20.0, "livefeed 环形缓冲必须有界")
        self.assertLessEqual(len(livefeed.latest(1000)), 500)


if __name__ == "__main__":
    unittest.main()
