"""IPv6 TGA 真实 hitlist 离线评估脚本。

用法：
  # 1) 获取真实种子（ipv6hitlist.github.io，需自行遵守其使用条款）
  python scripts/eval_ipv6_hitlist.py --download https://ipv6hitlist.github.io/<file>.txt.gz

  # 2) 离线评估：90% 种子训练 TGA，10% 留出验证，对比随机基线
  python scripts/eval_ipv6_hitlist.py --hitlist data/seeds/hitlist.txt --budget 100000

指标（与 tests/test_ipv6_hitlist.py 的合成数据评估口径一致）：
  - exact_hit_rate : 生成地址精确命中留出集的比例
  - p48_overlap    : 生成地址的 /48 前缀与留出集 /48 集合的重叠率
  - random_baseline: 同预算纯随机生成的 /48 重叠率（应接近 0）

退出码：0 = 评估完成；2 = 输入不足（种子太少/文件缺失）。
"""
from __future__ import annotations

import argparse
import gzip
import ipaddress
import json
import random
import shutil
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from modules.ipv6_tga import EntropyIP, SixTree, load_seeds  # noqa: E402

UA = {"User-Agent": "NetAtlas-eval/1.0 (academic measurement)"}


def download(url: str, dest_dir: Path) -> Path:
    """下载 hitlist（支持 .gz），落盘到 dest_dir 并返回解压后的文件路径。"""
    dest_dir.mkdir(parents=True, exist_ok=True)
    name = url.rsplit("/", 1)[-1] or "hitlist.txt"
    raw = dest_dir / name
    print(f"[download] {url} -> {raw}")
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=120) as r, open(raw, "wb") as f:
        shutil.copyfileobj(r, f)
    if name.endswith(".gz"):
        out = raw.with_suffix("")
        with gzip.open(raw, "rb") as fi, open(out, "wb") as fo:
            shutil.copyfileobj(fi, fo)
        raw.unlink()
        print(f"[download] 已解压 -> {out}")
        return out
    return raw


def p48_set(addrs: list[str]) -> set[str]:
    out = set()
    for a in addrs:
        try:
            net = ipaddress.IPv6Network(f"{a}/48", strict=False)
            out.add(str(net.network_address))
        except ValueError:
            continue
    return out


def evaluate(algo: str, train: list[str], holdout: list[str], budget: int,
             seed: int) -> dict:
    rng = random.Random(seed)
    if algo == "6tree":
        model = SixTree(min_seeds=8)
        model.build(train)
        generated = model.generate(budget, rng)
    elif algo == "entropyip":
        model = EntropyIP()
        model.fit(train)
        generated = model.generate(budget, rng)
    else:
        raise ValueError(f"未知算法: {algo}")

    hold_exact = set(holdout)
    hold_p48 = p48_set(holdout)
    gen_unique = list(dict.fromkeys(generated))
    exact_hits = sum(1 for a in gen_unique if a in hold_exact)
    p48_hits = sum(1 for a in gen_unique
                   if str(ipaddress.IPv6Network(f"{a}/48", strict=False).network_address)
                   in hold_p48)

    # 随机基线：同预算纯随机地址
    rand_gen = [str(ipaddress.IPv6Address(rng.getrandbits(128))) for _ in range(len(gen_unique))]
    rand_p48_hits = sum(
        1 for a in rand_gen
        if str(ipaddress.IPv6Network(f"{a}/48", strict=False).network_address) in hold_p48)

    n = max(1, len(gen_unique))
    base_rate = rand_p48_hits / n
    return {
        "algo": algo,
        "train_seeds": len(train),
        "holdout_seeds": len(holdout),
        "generated": len(gen_unique),
        "exact_hit_rate": round(exact_hits / n, 6),
        "p48_overlap": round(p48_hits / n, 6),
        "random_baseline_p48": round(base_rate, 6),
        # 随机基线为 0 时提升倍数无意义，记 None（展示为 ∞）
        "p48_lift_vs_random": round((p48_hits / n) / base_rate, 1) if base_rate > 0 else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="IPv6 TGA 真实 hitlist 离线评估")
    ap.add_argument("--hitlist", help="本地 hitlist 文件（每行一个 IPv6，支持 CSV 首列）")
    ap.add_argument("--download", metavar="URL",
                    help="先从 URL 下载 hitlist 到 data/seeds（支持 .gz）再评估")
    ap.add_argument("--algo", choices=["6tree", "entropyip", "both"], default="both")
    ap.add_argument("--budget", type=int, default=100_000, help="生成候选数")
    ap.add_argument("--holdout", type=float, default=0.1, help="留出验证比例")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--json", dest="json_out", help="评估结果另存为 JSON 文件")
    args = ap.parse_args()

    hitlist = args.hitlist
    if args.download:
        hitlist = str(download(args.download, ROOT / "data" / "seeds"))
    if not hitlist:
        ap.error("需要 --hitlist <文件> 或 --download <URL>")

    seeds = load_seeds(hitlist)
    if len(seeds) < 100:
        print(f"[评估] 种子不足（{len(seeds)} < 100），无法可靠评估", file=sys.stderr)
        sys.exit(2)

    rng = random.Random(args.seed)
    rng.shuffle(seeds)
    cut = max(1, int(len(seeds) * (1 - args.holdout)))
    train, holdout = seeds[:cut], seeds[cut:]
    print(f"[评估] 种子 {len(seeds)}：训练 {len(train)} / 留出 {len(holdout)}，"
          f"预算 {args.budget}")

    algos = ["6tree", "entropyip"] if args.algo == "both" else [args.algo]
    results = [evaluate(a, train, holdout, args.budget, args.seed) for a in algos]
    for r in results:
        lift = f"{r['p48_lift_vs_random']}x" if r["p48_lift_vs_random"] is not None else "∞"
        print(f"[{r['algo']:9s}] exact={r['exact_hit_rate']:.4%}  "
          f"/48重叠={r['p48_overlap']:.2%}  随机基线={r['random_baseline_p48']:.4%}  "
          f"提升={lift}")
    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"[评估] 结果已写入 {args.json_out}")


if __name__ == "__main__":
    main()
