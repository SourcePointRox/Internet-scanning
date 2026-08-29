"""IPv6 TGA 算法单元测试。"""
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.ipv6_tga import (EntropyIP, SixTree, addr_to_nybbles, alias_probes,
                              nybbles_to_addr, pattern_to_str)


class TestAddrUtils(unittest.TestCase):
    def test_roundtrip(self):
        import ipaddress
        addr = "2001:db8:85a3::8a2e:370:7334"
        # 往返必须指向同一地址（输出为 exploded 规范形式）
        self.assertEqual(ipaddress.IPv6Address(nybbles_to_addr(addr_to_nybbles(addr))),
                         ipaddress.IPv6Address(addr))
        self.assertEqual(nybbles_to_addr(addr_to_nybbles(addr)),
                         "2001:0db8:85a3:0000:0000:8a2e:0370:7334")

    def test_nybble_count(self):
        self.assertEqual(len(addr_to_nybbles("::1")), 32)


class TestSixTree(unittest.TestCase):
    SEEDS = [f"2001:db8::{i:x}" for i in range(64)] + \
            [f"2001:db8:1::{i:x}" for i in range(64)]

    def test_build_and_generate(self):
        tree = SixTree(min_seeds=8)
        tree.build(self.SEEDS)
        self.assertGreater(len(tree.leaves), 0)
        targets = tree.generate(200, rng=random.Random(42))
        self.assertEqual(len(targets), 200)
        # 生成的目标必须位于种子前缀覆盖的空间内（exploded 形式前 8 半字节 = 2001:0db8）
        for t in targets:
            self.assertTrue(t.startswith("2001:0db8:"))

    def test_feedback(self):
        tree = SixTree(min_seeds=8)
        tree.build(self.SEEDS)
        tree.adjust_budget({0: 0.5})
        self.assertAlmostEqual(tree.leaf_reward[0], 0.15, places=5)


class TestEntropyIP(unittest.TestCase):
    def test_fit_generate(self):
        seeds = [f"2001:db8:abcd::{i:04x}" for i in range(32)]
        eip = EntropyIP(n_segments=8)
        eip.fit(seeds)
        targets = eip.generate(50, rng=random.Random(1))
        self.assertEqual(len(targets), 50)
        for t in targets:
            self.assertIn(":", t)


class TestAliasProbes(unittest.TestCase):
    def test_probe_in_prefix(self):
        import ipaddress
        probes = alias_probes("2001:db8::/96", count=3, rng=random.Random(7))
        self.assertEqual(len(probes), 3)
        net = ipaddress.IPv6Network("2001:db8::/96")
        for p in probes:
            self.assertIn(ipaddress.IPv6Address(p), net)


if __name__ == "__main__":
    unittest.main()
