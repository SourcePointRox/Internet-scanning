"""L3/L4 无状态扫描模块：masscan 子进程封装（两阶段发现）。

阶段一：top 端口全网发现；阶段二：存活主机全端口。
- masscan 为外部 C 程序（Windows 需 Npcap），本模块负责：
  命令行组装 / 排除列表校验 / 速率下发 / JSON 流解析 / 断点续扫（--resume）。
- 无 masscan 二进制时自动进入 dry-run 模式（不发包，仅记录意图），
  便于在无权限环境下调试整条流水线。
"""
from __future__ import annotations

import ipaddress
import json
import logging
import queue
import shutil
import subprocess
import threading
import time
from pathlib import Path

from orchestrator.config import Config
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.l4")
MODULE = "l4_scanner"


class ExcludeList:
    """加载并校验排除网段（config/exclude.txt）。"""

    def __init__(self, path: Path):
        self.networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.split("#", 1)[0].strip()
                if line:
                    try:
                        self.networks.append(ipaddress.ip_network(line, strict=False))
                    except ValueError:
                        log.warning("排除列表忽略非法条目: %s", line)

    def contains(self, ip: str) -> bool:
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return True  # 非法 IP 一律视为排除
        return any(addr in n for n in self.networks)


class L4Scanner:
    def __init__(self, cfg: Config, out_queue: "queue.Queue[dict]", dry_run: bool = False,
                 bandwidth=None):
        self.cfg = cfg
        self.out = out_queue
        self.dry_run = dry_run
        self.bandwidth = bandwidth
        self._proc: subprocess.Popen | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.rate_pps = int(cfg.get("l4", "default_rate_pps", default=8000))
        bin_dir = cfg.abs_path("paths", "bin")
        self.masscan = self._find_masscan(bin_dir)
        self.exclude = ExcludeList(cfg.abs_path("l4", "exclude_file"))
        # 后端：masscan（首选）/ scapy（Npcap 原生）/ dry-run（模拟）
        self.backend = self._select_backend()
        self._scapy = None

    def _select_backend(self) -> str:
        if self.dry_run:
            return "dry-run"
        pref = str(self.cfg.get("l4", "scanner", default="auto")).lower()
        if pref == "masscan" or (pref == "auto" and self.masscan):
            return "masscan"
        if pref in ("scapy", "auto"):
            from modules.l4_scapy import ScapyL4Scanner
            ok, reason = ScapyL4Scanner.available()
            if ok:
                return "scapy"
            log.warning("Scapy 后端不可用（%s）", reason)
            REGISTRY.set_extra(MODULE, scapy_error=reason)
        return "dry-run"

    @staticmethod
    def _find_masscan(bin_dir: Path) -> str | None:
        for cand in (bin_dir / "masscan.exe", bin_dir / "masscan"):
            if cand.exists():
                return str(cand)
        return shutil.which("masscan")

    # ---------- 生命周期 ----------
    def start(self, targets: list[str] | None = None, ports: str | None = None,
              resume: str | None = None) -> None:
        targets = targets or ["0.0.0.0/0"]
        ports = ports or self.cfg.get("l4", "phase1_ports")
        self._stop.clear()
        # 每次启动重新探测后端，便于用户中途放入 masscan.exe 后热切换
        self.masscan = self._find_masscan(self.cfg.abs_path("paths", "bin"))
        if self.backend == "dry-run" and self.masscan and not self.dry_run:
            log.info("检测到 masscan（%s），后端升级为 masscan", self.masscan)
        self.backend = self._select_backend()
        REGISTRY.set_extra(MODULE, backend=self.backend)
        if self.backend == "scapy":
            from modules.l4_scapy import ScapyL4Scanner
            self._scapy = ScapyL4Scanner(self.cfg, self.out, exclude=self.exclude,
                                         bandwidth=self.bandwidth)
            self._scapy.start(targets=targets, ports=ports, rate_pps=self.rate_pps)
            REGISTRY.set_running(MODULE, True)
            return
        self._thread = threading.Thread(
            target=self._run, args=(targets, ports, resume),
            daemon=True, name="l4-scanner",
        )
        self._thread.start()
        REGISTRY.set_running(MODULE, True)

    def stop(self) -> None:
        self._stop.set()
        if self._scapy is not None:
            self._scapy.stop()
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        REGISTRY.set_running(MODULE, False)

    def set_rate(self, pps: int) -> None:
        """运行中调速：masscan 不支持热调速，记录并在下次启动/续扫生效。"""
        self.rate_pps = max(100, min(pps, int(self.cfg.get("l4", "max_rate_pps", default=18000))))
        if self._scapy is not None:
            self._scapy.set_rate(self.rate_pps)
        REGISTRY.set_extra(MODULE, rate_pps=self.rate_pps)

    # ---------- 主循环 ----------
    def _run(self, targets: list[str], ports: str, resume: str | None) -> None:
        if not self.masscan or self.dry_run:
            log.warning("masscan 不可用或 dry-run：进入模拟模式（不发包）")
            self._simulate(targets)
            return
        cmd = [
            self.masscan, *targets, f"-p{ports}",
            "--rate", str(self.rate_pps),
            "--excludefile", str(self.cfg.abs_path("l4", "exclude_file")),
            "--retries", str(self.cfg.get("l4", "retries", default=1)),
            "-oJ", "-",                      # JSON 流输出到 stdout
            "--output-status", "open",
        ]
        # 直连物理网卡（绕过会伪造响应的 VPN 隧道）
        src_ip = self.cfg.get("l4", "source_ip")
        router_mac = self.cfg.get("l4", "router_mac")
        if src_ip:
            cmd += ["--source-ip", str(src_ip)]
        if router_mac:
            cmd += ["--router-mac", str(router_mac)]
        if resume:
            cmd += ["--resume", resume]
        log.info("启动 masscan: %s", " ".join(cmd[:8]) + " ...")
        try:
            self._proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1, errors="replace",
            )
            assert self._proc.stdout is not None
            for line in self._proc.stdout:
                if self._stop.is_set():
                    break
                self._handle_line(line)
            self._proc.wait()
        except Exception as e:  # noqa: BLE001
            log.error("masscan 运行异常: %s", e)
            REGISTRY.set_extra(MODULE, last_error=str(e))
        finally:
            REGISTRY.set_running(MODULE, False)

    def _handle_line(self, line: str) -> None:
        line = line.strip()
        if not (line.startswith("{") and line.endswith(("}", "},"))):
            if "rate:" in line or "found:" in line:
                REGISTRY.set_extra(MODULE, masscan_status=line[:200])
            return
        try:
            rec = json.loads(line.rstrip(","))
            ip = rec.get("ip")
            if not ip or self.exclude.contains(ip):
                REGISTRY.incr(MODULE, "excluded")
                return
            for port in rec.get("ports", []):
                self.out.put({
                    "ip": ip, "port": port.get("port"),
                    "proto": port.get("proto", "tcp"),
                    "status": port.get("status", "open"),
                    "ttl": rec.get("ttl"),
                    "ts": rec.get("timestamp", int(time.time())),
                    "family": 6 if ":" in ip else 4,
                })
                REGISTRY.incr(MODULE, "open_ports")
        except (json.JSONDecodeError, AttributeError):
            pass

    def _simulate(self, targets: list[str]) -> None:
        """dry-run：用回环地址制造少量伪发现，供流水线联调。"""
        REGISTRY.set_extra(MODULE, mode="dry-run")
        sim_ports = [80, 443, 22, 8080, 21]
        while not self._stop.is_set():
            for p in sim_ports:
                if self._stop.is_set():
                    return
                self.out.put({"ip": "127.0.0.1", "port": p, "proto": "tcp",
                              "status": "open", "ttl": 128, "ts": int(time.time()), "family": 4,
                              "simulated": True})
                REGISTRY.incr(MODULE, "open_ports")
            time.sleep(1.0)
