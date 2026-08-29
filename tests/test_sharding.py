"""分布式分片与速率控制测试。"""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.ratecontrol import RateController
from orchestrator.sharding import HashedSharder, LocalShardCoordinator


class TestHashedSharder(unittest.TestCase):
    def test_single_node_passthrough(self):
        s = HashedSharder(1, 0)
        self.assertEqual(list(s.filter_cidr("203.0.113.0/24")), ["203.0.113.0/24"])

    def test_partition_covers_all_blocks(self):
        """两个节点对同一 CIDR 的划分必须互补且无交集。"""
        s0, s1 = HashedSharder(2, 0), HashedSharder(2, 1)
        b0 = set(s0.filter_cidr("10.0.0.0/16"))
        b1 = set(s1.filter_cidr("10.0.0.0/16"))
        self.assertFalse(b0 & b1, "分片交集必须为空")
        self.assertEqual(len(b0 | b1), 256, "两分片必须覆盖全部 /24 块")

    def test_deterministic_across_instances(self):
        """无状态哈希：任何节点独立计算得到相同划分（多机一致性的根基）。"""
        a = HashedSharder(4, 2, seed="netatlas")
        b = HashedSharder(4, 2, seed="netatlas")
        self.assertEqual(list(a.filter_cidr("192.0.2.0/22")), list(b.filter_cidr("192.0.2.0/22")))

    def test_invalid_index(self):
        with self.assertRaises(ValueError):
            HashedSharder(2, 2)

    def test_ipv6_blocks(self):
        s = HashedSharder(3, 1)
        blocks = list(s.filter_cidr("2001:db8::/46"))
        self.assertTrue(all("/48" in b for b in blocks))


class TestLocalCoordinator(unittest.TestCase):
    def test_register_heartbeat_progress(self):
        c = LocalShardCoordinator()
        c.register_node("node-1", {"shard_index": 0})
        c.heartbeat("node-1", {"probes_sent": 100, "open_found": 3})
        gp = c.global_progress()
        self.assertEqual(gp["nodes"], 1)
        self.assertEqual(gp["probes_sent"], 100)
        self.assertEqual(c.my_targets(["0.0.0.0/0"], 1, 0), ["0.0.0.0/0"])


class TestRateController(unittest.TestCase):
    def test_additive_increase(self):
        rc = RateController(initial_pps=1000, max_pps=10000, up_step_pct=10)
        rc.decide(probes_sent=0, open_found=0)          # 建立基线
        rate, changed = rc.decide(probes_sent=5000, open_found=50)
        self.assertTrue(changed)
        self.assertEqual(rate, 1100)                    # +10%

    def test_multiplicative_decrease_on_blackhole(self):
        rc = RateController(initial_pps=8000, max_pps=18000, down_factor=0.5)
        rc.decide(probes_sent=0, open_found=0)
        rate, _ = rc.decide(probes_sent=20000, open_found=0)   # 完全无响应 = 拥塞
        self.assertEqual(rate, 4000)
        self.assertEqual(rc.last_decision, "multiplicative-decrease")

    def test_bounds(self):
        rc = RateController(initial_pps=100, max_pps=500, min_pps=100)
        rc.decide(probes_sent=0, open_found=0)
        for _ in range(20):
            rc.decide(probes_sent=0, open_found=0)      # 连降不破下限
        self.assertGreaterEqual(rc.rate, 100)
        self.assertEqual(rc.set_rate(99999), 500)       # 手动调速封顶
        self.assertEqual(rc.set_rate(1), 100)           # 手动调速保底

    def test_manual_set_recorded(self):
        rc = RateController(initial_pps=1000, max_pps=5000)
        rc.set_rate(2000)
        self.assertEqual(rc.snapshot()["history"][-1]["reason"], "manual")


if __name__ == "__main__":
    unittest.main()
