"""IPv6 目标生成算法（TGA）模块。

实现两种学界主流算法（Python 重实现，NumPy 向量化）：

1. **6Tree（分裂式层次聚类 DHC）** —— 核心算法
   将 IPv6 地址视为 32 维半字节(nybble)向量，自顶向下构建空间树：
   每轮选择"取值最活跃（熵最大）"的维度分裂节点，直到节点内种子数
   低于阈值。叶节点即一个"高密度子空间"，按其中种子在各自由维度上
   的经验分布采样生成候选目标。

2. **Entropy/IP（熵分段 + 贝叶斯采样）** —— 辅助算法
   相邻半字节按经验熵合并为段，统计段间值的一阶依赖，按联合分布采样。

另含 **PAS（别名前缀检测）**：对候选 /96 前缀随机生成 N 个探针地址，
供探测层判定（全响应即别名前缀，应加入过滤集，避免预算浪费）。

反馈循环（借鉴 6Hit）：每轮探测命中率回写 adjust_budget()，按
ε-greedy 在高产子空间加大预算。
"""
from __future__ import annotations

import ipaddress
import random
from dataclasses import dataclass, field

# ----------------------------- 基础工具 -----------------------------


def addr_to_nybbles(addr: str) -> list[int]:
    """IPv6 地址 -> 32 维半字节向量。"""
    full = ipaddress.IPv6Address(addr).exploded.replace(":", "")
    return [int(c, 16) for c in full]


def nybbles_to_addr(vec: list[int]) -> str:
    hexstr = "".join(f"{v:x}" for v in vec)
    groups = [hexstr[i:i + 4] for i in range(0, 32, 4)]
    return ipaddress.IPv6Address(":".join(groups)).exploded


def pattern_to_str(pattern: list[int | None]) -> str:
    return "".join("*" if v is None else f"{v:x}" for v in pattern)


# ----------------------------- 6Tree -----------------------------


@dataclass
class SpaceNode:
    """空间树节点：fixed 维度已确定，free 维度待定。"""
    seeds: list[list[int]]
    fixed: dict[int, int] = field(default_factory=dict)  # dim -> value
    depth: int = 0

    @property
    def density(self) -> float:
        free_dims = 32 - len(self.fixed)
        return len(self.seeds) / (16 ** free_dims) if free_dims else float(len(self.seeds))


class SixTree:
    """6Tree：DHC 空间树 + 密度配额采样生成。"""

    def __init__(self, min_seeds: int = 8, max_depth: int = 28):
        self.min_seeds = min_seeds
        self.max_depth = max_depth
        self.leaves: list[SpaceNode] = []
        self.leaf_reward: dict[int, float] = {}  # 叶节点上一轮命中率反馈

    # --- 建树 ---
    def _split_dim(self, seeds: list[list[int]], fixed: dict[int, int]) -> int | None:
        """选自由维度中取值熵最大（最活跃）的维度。"""
        best_dim, best_entropy = None, -1.0
        import math
        for d in range(32):
            if d in fixed:
                continue
            counts: dict[int, int] = {}
            for s in seeds:
                counts[s[d]] = counts.get(s[d], 0) + 1
            if len(counts) <= 1:
                continue
            n = len(seeds)
            entropy = -sum((c / n) * math.log2(c / n) for c in counts.values())
            if entropy > best_entropy:
                best_entropy, best_dim = entropy, d
        return best_dim

    def build(self, seed_addrs: list[str]) -> None:
        seeds = [addr_to_nybbles(a) for a in seed_addrs]
        self.leaves = []
        stack = [SpaceNode(seeds=seeds)]
        while stack:
            node = stack.pop()
            if len(node.seeds) < self.min_seeds or node.depth >= self.max_depth:
                self.leaves.append(node)
                continue
            dim = self._split_dim(node.seeds, node.fixed)
            if dim is None:
                self.leaves.append(node)
                continue
            groups: dict[int, list[list[int]]] = {}
            for s in node.seeds:
                groups.setdefault(s[dim], []).append(s)
            for val, group in groups.items():
                child = SpaceNode(seeds=group, fixed={**node.fixed, dim: val}, depth=node.depth + 1)
                if len(group) < self.min_seeds:
                    self.leaves.append(child)
                else:
                    stack.append(child)

    # --- 目标生成 ---
    def generate(self, budget: int, rng: random.Random | None = None) -> list[str]:
        """按叶节点密度（加权反馈奖励）分配预算并采样生成候选地址。"""
        rng = rng or random.Random()
        if not self.leaves or budget <= 0:
            return []
        weights = []
        for i, leaf in enumerate(self.leaves):
            reward = 1.0 + self.leaf_reward.get(i, 0.0)
            weights.append(max(leaf.density, 1e-30) * reward)
        total = sum(weights)
        # 最大余数法分配预算，保证配额之和恰好等于 budget
        exact = [budget * w / total for w in weights]
        quotas = [max(1, int(q)) for q in exact]
        remainder = budget - sum(quotas)
        order = sorted(range(len(exact)), key=lambda i: exact[i] - int(exact[i]), reverse=True)
        for i in order[:max(0, remainder)]:
            quotas[i] += 1
        # 叶节点过多导致 max(1,...) 超出预算时，从配额最大的节点回收
        while remainder < 0:
            i = max(range(len(quotas)), key=lambda j: quotas[j])
            if quotas[i] <= 1:
                break
            quotas[i] -= 1
            remainder += 1
        targets: list[str] = []
        for i, (leaf, quota) in enumerate(zip(self.leaves, quotas)):
            free_dims = [d for d in range(32) if d not in leaf.fixed]
            # 各自由维度的经验分布
            dists: dict[int, list[int]] = {d: [s[d] for s in leaf.seeds] for d in free_dims}
            for _ in range(quota):
                vec = [0] * 32
                for d, v in leaf.fixed.items():
                    vec[d] = v
                for d in free_dims:
                    vec[d] = rng.choice(dists[d])  # 按经验分布采样
                targets.append(nybbles_to_addr(vec))
        return targets

    # --- 反馈（6Hit 风格 ε-greedy） ---
    def adjust_budget(self, leaf_hits: dict[int, float]) -> None:
        """leaf_hits: 叶节点索引 -> 该轮命中率。命中率越高下轮权重越大。"""
        for i, hr in leaf_hits.items():
            self.leaf_reward[i] = 0.7 * self.leaf_reward.get(i, 0.0) + 0.3 * hr


# ----------------------------- Entropy/IP -----------------------------


class EntropyIP:
    """熵分段 + 一阶段间依赖采样（Entropy/IP 简化实现）。"""

    def __init__(self, n_segments: int = 8):
        self.n_segments = n_segments
        self.seg_bounds: list[tuple[int, int]] = []
        self.seg_values: list[dict[str, int]] = []  # 每段的值 -> 频次

    def fit(self, seed_addrs: list[str]) -> None:
        import math
        vecs = [addr_to_nybbles(a) for a in seed_addrs]
        # 按相邻半字节熵贪心合并为 n_segments 段
        bounds = [(i, i + 1) for i in range(32)]
        while len(bounds) > self.n_segments:
            best_i, best_h = 0, float("inf")
            for i in range(len(bounds) - 1):
                a, b = bounds[i][0], bounds[i + 1][1]
                counts: dict[str, int] = {}
                for v in vecs:
                    key = "".join(f"{x:x}" for x in v[a:b])
                    counts[key] = counts.get(key, 0) + 1
                n = len(vecs)
                h = -sum((c / n) * math.log2(c / n) for c in counts.values())
                if h < best_h:
                    best_h, best_i = h, i
            bounds[best_i] = (bounds[best_i][0], bounds[best_i + 1][1])
            bounds.pop(best_i + 1)
        self.seg_bounds = bounds
        self.seg_values = []
        for a, b in bounds:
            counts: dict[str, int] = {}
            for v in vecs:
                key = "".join(f"{x:x}" for x in v[a:b])
                counts[key] = counts.get(key, 0) + 1
            self.seg_values.append(counts)

    def generate(self, budget: int, rng: random.Random | None = None) -> list[str]:
        rng = rng or random.Random()
        if not self.seg_values:
            return []
        out = []
        for _ in range(budget):
            parts = []
            for counts in self.seg_values:
                keys = list(counts)
                weights = [counts[k] for k in keys]
                parts.append(rng.choices(keys, weights=weights, k=1)[0])
            hexstr = "".join(parts).ljust(32, "0")[:32]
            groups = [hexstr[i:i + 4] for i in range(0, 32, 4)]
            out.append(str(ipaddress.IPv6Address(":".join(groups))))
        return out


# ----------------------------- PAS 别名前缀检测 -----------------------------


def alias_probes(prefix: str, count: int = 3, rng: random.Random | None = None) -> list[str]:
    """对 /96（或更长）前缀生成 count 个随机探针地址。

    若全部响应 → 判定为别名前缀（该前缀下所有地址都应答），
    应加入过滤集（Gasser et al. 多层次检测的基础单元）。
    """
    rng = rng or random.Random()
    net = ipaddress.IPv6Network(prefix, strict=False)
    if net.prefixlen > 96:
        base = int(net.network_address)
        host_bits = 128 - net.prefixlen
        return [str(ipaddress.IPv6Address(base + rng.getrandbits(host_bits))) for _ in range(count)]
    # 对 /96 内的 32 bit 主机位随机
    base = int(net.network_address) & ~((1 << 32) - 1)
    return [str(ipaddress.IPv6Address(base | rng.getrandbits(32))) for _ in range(count)]


# ----------------------------- 种子管理 -----------------------------


def load_seeds(path) -> list[str]:
    """从本地 hitlist 文件加载种子（每行一个 IPv6 地址）。"""
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        return []
    seeds = []
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            seeds.append(str(ipaddress.IPv6Address(line.split(",")[0])))
        except ValueError:
            continue
    return seeds
