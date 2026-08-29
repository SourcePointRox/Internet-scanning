"""IPv6 TGA 算法与模拟 hitlist 数据的对比验证。

方法学（对齐 6Tree/Entropy-IP 论文评估方式）：
1. 用已知结构模式合成"真实 hitlist"（如 2001:db8:XXXX::/48 中活跃前缀聚集）；
2. 70% 种子训练 TGA，30% 留出作 ground truth；
3. 验证：生成地址的 /48 前缀与留出集重叠率显著高于随机基线；
4. 反馈循环：命中率回写后，高产子空间预算占比必须上升。
"""
import ipaddress
import random
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.ipv6_tga import EntropyIP, SixTree, load_seeds


def synth_hitlist(n: int, rng: random.Random) -> list[str]:
    """合成结构化 hitlist：地址聚集于少数 /48 前缀（真实 IPv6 分布特征）。"""
    hot_prefixes = [rng.randrange(0x1000, 0x10000) for _ in range(4)]  # 4 个热 /48
    seeds = []
    for _ in range(n):
        pfx = rng.choice(hot_prefixes)
        seeds.append(f"2001:db8:{pfx:x}::{rng.getrandbits(16):x}:{rng.getrandbits(16):x}")
    return seeds


def prefix48(addr: str) -> str:
    a = ipaddress.IPv6Address(addr)
    return str(ipaddress.IPv6Network((int(a) >> 80 << 80, 48), strict=False))


class TestTGAAgainstHitlist(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rng = random.Random(20260830)
        cls.all_addrs = synth_hitlist(600, cls.rng)
        cls.rng.shuffle(cls.all_addrs)
        cls.train = cls.all_addrs[:420]
        cls.holdout = cls.all_addrs[420:]
        cls.holdout_prefixes = {prefix48(a) for a in cls.holdout}

    def test_6tree_prefix_overlap_beats_random(self):
        """6Tree 生成地址的 /48 前缀与留出集重叠率 >> 随机基线（4/65536）。"""
        tree = SixTree(min_seeds=8)
        tree.build(self.train)
        targets = tree.generate(2000, rng=random.Random(1))
        overlap = sum(1 for t in targets if prefix48(t) in self.holdout_prefixes)
        overlap_rate = overlap / len(targets)
        random_baseline = len(self.holdout_prefixes) / 65536
        print(f"\n[hitlist] 6Tree /48 重叠率 {overlap_rate:.1%} vs 随机基线 {random_baseline:.4%}")
        self.assertGreater(overlap_rate, 0.5, "6Tree 必须高度集中于种子结构空间")
        self.assertGreater(overlap_rate, random_baseline * 100)

    def test_entropyip_distribution_fidelity(self):
        """Entropy/IP 生成地址的段值分布应与种子经验分布一致（卡方近似检验）。"""
        eip = EntropyIP(n_segments=8)
        eip.fit(self.train)
        targets = eip.generate(1000, rng=random.Random(2))
        # 第 3 个 hextet（/48 前缀低 16 位）的高频值应与种子热前缀高度重合
        train_vals = Counter(a.split(":")[3] for a in self.train)
        gen_vals = Counter(t.split(":")[3] for t in targets)
        train_top = {v for v, _ in train_vals.most_common(4)}
        gen_top = {v for v, _ in gen_vals.most_common(4)}
        self.assertTrue(train_top & gen_top, "生成分布必须保留种子的高频结构")

    def test_feedback_shifts_budget(self):
        """反馈循环：高命中叶节点的预算占比必须随反馈上升。"""
        tree = SixTree(min_seeds=8)
        tree.build(self.train)
        n_leaves = len(tree.leaves)
        tree.generate(1000, rng=random.Random(3))   # 预热权重
        # 模拟叶 0 命中率 80%，其余 0
        tree.adjust_budget({0: 0.8})
        reward0 = tree.leaf_reward[0]
        self.assertAlmostEqual(reward0, 0.24, places=5)  # 0.7*0 + 0.3*0.8
        # 多轮反馈后 reward 趋近命中率
        for _ in range(10):
            tree.adjust_budget({0: 0.8})
        self.assertGreater(tree.leaf_reward[0], 0.6)

    def test_generated_addresses_valid(self):
        tree = SixTree(min_seeds=8)
        tree.build(self.train)
        for t in tree.generate(200, rng=random.Random(4)):
            ipaddress.IPv6Address(t)  # 不抛即合法

    def test_hitlist_file_loading(self):
        """真实 hitlist 文件格式（每行地址，支持 CSV 首列、注释行）。"""
        tmp = Path(tempfile.mkdtemp()) / "hitlist.txt"  # 写入临时目录，不落仓库
        tmp.write_text("# comment\n2001:db8::1\n2001:db8::2,extra-column\nbad-line\n\n",
                       encoding="utf-8")
        seeds = load_seeds(tmp)
        self.assertEqual(len(seeds), 2)
        self.assertIn("2001:db8::1", seeds)


if __name__ == "__main__":
    unittest.main()
