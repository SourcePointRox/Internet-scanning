"""NetAtlas 一键启动脚本（跨平台）。

用法：
  python scripts/start.py                 # 生产模式（需要 masscan + Npcap）
  python scripts/start.py --dry-run       # 模拟模式（流水线联调，不发包）
  python scripts/start.py --targets 127.0.0.0/30 --dry-run

启动前自检：依赖包 / masscan 二进制 / Npcap / 排除列表 / GeoIP 库。
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

REQUIRED_PKGS = ["yaml", "fastapi", "uvicorn", "zstandard", "pyarrow", "duckdb"]


def check() -> list[str]:
    warnings = []
    for pkg in REQUIRED_PKGS:
        try:
            __import__(pkg)
        except ImportError:
            warnings.append(f"缺少 Python 依赖: {pkg}（pip install {' '.join(REQUIRED_PKGS)}）")
            break
    bin_dir = ROOT / "bin"
    masscan = (bin_dir / "masscan.exe").exists() or shutil.which("masscan")
    if not masscan:
        warnings.append("未找到 masscan（bin/masscan.exe）。将自动以 dry-run 模拟模式运行。")
    if not (ROOT / "config" / "exclude.txt").exists():
        warnings.append("缺少 config/exclude.txt —— 合规硬约束，禁止扫描！")
    if not (ROOT / "data" / "geoip" / "GeoLite2-City.mmdb").exists():
        warnings.append("未找到 GeoLite2-City.mmdb（可选）：地理/ASN 富化将降级。")
    return warnings


def main() -> None:
    args = sys.argv[1:]
    warnings = check()
    for w in warnings:
        print(f"[自检] {w}")
    if any("exclude.txt" in w for w in warnings):
        print("[自检] 严重问题，终止启动。")
        sys.exit(2)
    masscan_ok = not any("masscan" in w for w in warnings)
    if not masscan_ok and "--dry-run" not in args:
        args.append("--dry-run")
    cmd = [sys.executable, "-m", "orchestrator.main", *args]
    print(f"[启动] {' '.join(cmd)}")
    sys.exit(subprocess.call(cmd, cwd=str(ROOT)))


if __name__ == "__main__":
    main()
