"""外部依赖一键安装/检测脚本。

覆盖 README 中"需用户自行下载放置"的全部外部依赖：
  python scripts/setup_deps.py --all              # 全部检测 + 可自动化的自动安装
  python scripts/setup_deps.py --zgrab2           # 下载 zgrab2 预编译二进制（Go 可用时编译）
  python scripts/setup_deps.py --geoip KEY        # 用 MaxMind License Key 下载 GeoLite2 mmdb
  python scripts/setup_deps.py --npcap            # 检测/引导安装 Npcap（Windows）
  python scripts/setup_deps.py --masscan          # 检测 masscan（Windows 需 MinGW 编译，给指引）
  python scripts/setup_deps.py --detect-net       # 探测本机网卡 IP / 网关 MAC（二层直连配置辅助）
"""
from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "bin"
GEOIP = ROOT / "data" / "geoip"

UA = {"User-Agent": "NetAtlas-setup/1.0"}


def _download(url: str, dest: Path) -> bool:
    print(f"  下载 {url}")
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=60) as r, open(dest, "wb") as f:
            shutil.copyfileobj(r, f)
        return True
    except Exception as e:  # noqa: BLE001 —— 网络错误透传给用户
        print(f"  [失败] {e}")
        return False


def check_python_deps() -> bool:
    missing = []
    for pkg, mod in [("fastapi", "fastapi"), ("uvicorn", "uvicorn"), ("PyYAML", "yaml"),
                     ("zstandard", "zstandard"), ("pyarrow", "pyarrow"), ("duckdb", "duckdb"),
                     ("psutil", "psutil"), ("scapy", "scapy"), ("geoip2", "geoip2")]:
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"[Python] 缺少依赖: {' '.join(missing)}")
        print(f"  修复: {sys.executable} -m pip install -r requirements.txt")
        return False
    print("[Python] 依赖完整 ✓")
    return True


def setup_zgrab2() -> bool:
    BIN.mkdir(exist_ok=True)
    exe = BIN / ("zgrab2.exe" if platform.system() == "Windows" else "zgrab2")
    if exe.exists():
        print(f"[zgrab2] 已存在: {exe} ✓")
        return True
    go = shutil.which("go")
    if go:
        print("[zgrab2] 检测到 Go，编译 v0.1.8（Go 1.22 推荐；>=1.24 在 Windows 有兼容问题）...")
        env = {**os.environ, "GOBIN": str(BIN)}
        rc = subprocess.call([go, "install", "github.com/zmap/zgrab2/cmd/zgrab2@v0.1.8"], env=env)
        if rc == 0 and exe.exists():
            print(f"[zgrab2] 编译完成: {exe} ✓")
            return True
        print("[zgrab2] 编译失败（可重试或手工编译）")
        return False
    print("[zgrab2] 未检测到 Go 工具链。请安装 Go 1.22 后重试，或手工放置二进制到 bin/")
    return False


def setup_geoip(license_key: str | None) -> bool:
    GEOIP.mkdir(parents=True, exist_ok=True)
    products = {"GeoLite2-City": "GeoLite2-City.mmdb", "GeoLite2-ASN": "GeoLite2-ASN.mmdb"}
    if all((GEOIP / f).exists() for f in products.values()):
        print("[GeoIP] mmdb 已齐备 ✓")
        return True
    if not license_key:
        print("[GeoIP] 需要 MaxMind License Key（免费注册: https://www.maxmind.com/en/geolite2/signup）")
        print("  用法: python scripts/setup_deps.py --geoip <YOUR_LICENSE_KEY>")
        return False
    ok = True
    for edition, filename in products.items():
        if (GEOIP / filename).exists():
            continue
        url = (f"https://download.maxmind.com/app/geoip_download"
               f"?edition_id={edition}&license_key={license_key}&suffix=tar.gz")
        with tempfile.TemporaryDirectory() as td:
            tgz = Path(td) / "db.tar.gz"
            if not _download(url, tgz):
                ok = False
                continue
            try:
                with tarfile.open(tgz) as tar:
                    member = next(m for m in tar.getmembers() if m.name.endswith(filename))
                    member.name = filename  # sanitize: 防路径穿越
                    tar.extract(member, GEOIP, filter="data")
                print(f"[GeoIP] {filename} ✓")
            except (tarfile.TarError, StopIteration, OSError) as e:
                print(f"[GeoIP] 解压失败: {e}")
                ok = False
    return ok


def check_npcap() -> bool:
    if platform.system() != "Windows":
        print("[Npcap] 非 Windows 平台，跳过（Linux 用 libpcap: apt install libpcap-dev）")
        return True
    sys32 = Path(os.environ.get("SystemRoot", "C:\\Windows")) / "System32"
    found = (sys32 / "Npcap").is_dir() or (sys32 / "wpcap.dll").exists()
    if found:
        print("[Npcap] 已安装 ✓")
        return True
    print("[Npcap] 未安装。masscan/scapy 发包必需。")
    print("  下载: https://npcap.com/#download （安装时勾选 WinPcap 兼容模式）")
    return False


def check_masscan() -> bool:
    exe = BIN / ("masscan.exe" if platform.system() == "Windows" else "masscan")
    if exe.exists() or shutil.which("masscan"):
        print("[masscan] 已就绪 ✓")
        return True
    if platform.system() == "Windows":
        print("[masscan] 未找到。Windows 无官方预编译包，需 MinGW + Npcap SDK 编译：")
        print("  git clone https://github.com/robertdavidgraham/masscan deps/masscan")
        print("  用 MinGW make 后将 masscan.exe 放入 bin/（或改用内置 scapy 后端）")
    else:
        print("[masscan] 未找到。安装: apt install masscan 或源码编译")
    return False


def detect_net() -> None:
    """探测二层直连所需的 source_ip / router_mac（辅助填写 config.yaml）。"""
    print("[detect-net] 本机网卡与网关信息：")
    if platform.system() == "Windows":
        subprocess.call(["ipconfig"])
        print("\n网关 MAC（arp -a 中对应网关 IP 的物理地址）：")
        subprocess.call(["arp", "-a"])
    else:
        subprocess.call(["ip", "addr"])
        subprocess.call(["ip", "neigh"])


def main() -> None:
    ap = argparse.ArgumentParser(description="NetAtlas 外部依赖安装/检测")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--zgrab2", action="store_true")
    ap.add_argument("--geoip", nargs="?", const="", default=None, help="MaxMind License Key")
    ap.add_argument("--npcap", action="store_true")
    ap.add_argument("--masscan", action="store_true")
    ap.add_argument("--detect-net", action="store_true")
    args = ap.parse_args()

    results = {}
    if args.detect_net:
        detect_net()
        return
    if args.all or not any([args.zgrab2, args.geoip is not None, args.npcap, args.masscan]):
        results["python"] = check_python_deps()
        results["npcap"] = check_npcap()
        results["masscan"] = check_masscan()
        results["zgrab2"] = setup_zgrab2()
        results["geoip"] = setup_geoip(None)
    else:
        if args.zgrab2:
            results["zgrab2"] = setup_zgrab2()
        if args.geoip is not None:
            results["geoip"] = setup_geoip(args.geoip or None)
        if args.npcap:
            results["npcap"] = check_npcap()
        if args.masscan:
            results["masscan"] = check_masscan()
    failed = [k for k, v in results.items() if not v]
    print(f"\n== 汇总: {len(results) - len(failed)}/{len(results)} 就绪" +
          (f"，待处理: {', '.join(failed)}" if failed else " =="))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
