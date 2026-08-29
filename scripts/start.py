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


def real_scan_ready() -> tuple[bool, str]:
    """是否能进行真实发包扫描（masscan 或 scapy+Npcap 任一可用）。"""
    bin_dir = ROOT / "bin"
    if (bin_dir / "masscan.exe").exists() or shutil.which("masscan"):
        return True, "masscan"
    npcap = (Path("C:/Windows/System32/Npcap").is_dir()
             or Path("C:/Windows/System32/Npcap/wpcap.dll").exists())
    try:
        import scapy  # noqa: F401
        scapy_ok = True
    except ImportError:
        scapy_ok = False
    if npcap and scapy_ok:
        return True, "scapy/Npcap"
    return False, "无可用 L4 后端（需 masscan 或 Npcap+scapy）"


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
        warnings.append("未找到 masscan（bin/masscan.exe）：官方无 Windows 预编译包，"
                        "需 MinGW 自行编译。将自动改用 scapy/Npcap 后端。")
    npcap = (Path("C:/Windows/System32/Npcap").is_dir()
             or Path("C:/Windows/System32/Npcap/wpcap.dll").exists())
    if not npcap:
        warnings.append("未检测到 Npcap：请先安装 deps/npcap-1.88.exe，否则只能以 dry-run 模拟运行。")
    try:
        import scapy  # noqa: F401
    except ImportError:
        warnings.append("scapy 未安装（pip install scapy）：无法使用 Npcap 原生扫描后端。")
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
    ready, backend = real_scan_ready()
    print(f"[自检] L4 发包后端: {backend}")
    if not ready and "--dry-run" not in args:
        args.append("--dry-run")
    cmd = [sys.executable, "-m", "orchestrator.main", *args]
    print(f"[启动] {' '.join(cmd)}")
    sys.exit(subprocess.call(cmd, cwd=str(ROOT)))


if __name__ == "__main__":
    main()
