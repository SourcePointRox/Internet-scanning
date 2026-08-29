"""Scapy/Npcap 原生 L4 无状态 SYN 扫描后端。

用途：当主机未安装 masscan（官方不提供 Windows 预编译包，需 MinGW 自行编译）时，
使用 Scapy 通过 Npcap 直接从网卡发送/接收以太网帧，实现真正的无状态 SYN 扫描。

原理：
  发送端：对每个 (ip, port) 构造 IP/TCP SYN 包，固定源端口便于 BPF 过滤；
  接收端：AsyncSniffer 在独立线程抓 SYN-ACK（BPF: tcp[13] & 0x12 == 0x12），
          按 (src_ip, src_port) 还原目标；
  带宽：每包约 54-60 字节，发送前经全局令牌桶 acquire('l4_scan', size) 限流。

要求：Windows 需安装 Npcap（https://npcap.com）；Linux/macOS 可直接运行。
"""
from __future__ import annotations

import ipaddress
import logging
import queue
import random
import threading
import time

from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.l4.scapy")
MODULE = "l4_scanner"

SYN_PKT_SIZE = 60  # 以太网帧开销后的估算字节数（含前导/间隙保守估计）


class ScapyL4Scanner:
    """无状态 SYN 扫描器（Scapy + Npcap）。"""

    def __init__(self, cfg, out_queue: "queue.Queue[dict]", bandwidth=None,
                 source_port: int = 61000, exclude=None):
        self.cfg = cfg
        self.out = out_queue
        self.bw = bandwidth
        self.source_port = source_port
        self.exclude = exclude
        self._stop = threading.Event()
        self._sniffer = None
        self._thread: threading.Thread | None = None
        self.sent = 0

    # ---------- 依赖检查 ----------
    @staticmethod
    def available() -> tuple[bool, str]:
        """检查是否具备真实发包能力。

        Windows 下必须满足两点，否则 Scapy 会退回原生 raw socket，
        而 Windows 禁止通过 raw socket 发送 TCP 包（会报
        'TCP data cannot be sent over raw socket'）：
          1) Npcap 的 wpcap.dll 可用（驱动已安装）；
          2) Scapy 的 L2 套接字（npcap 后端）可用。
        """
        try:
            from scapy.config import conf
        except ImportError:
            return False, "scapy 未安装（pip install scapy）"
        # 1) wpcap.dll 是否可用
        import os
        sysroot = os.environ.get("SystemRoot", "C:\\Windows")
        dll_paths = [os.path.join(sysroot, "System32", "wpcap.dll"),
                     os.path.join(sysroot, "System32", "Npcap", "wpcap.dll")]
        if not any(os.path.exists(p) for p in dll_paths):
            return False, ("未找到 wpcap.dll —— Npcap 未安装。"
                           "请运行 deps/npcap-1.88.exe 完成安装后重启本模块")
        # 2) Scapy L2(npcap) 套接字是否可用
        l2 = conf.L2socket
        if l2 is None or (isinstance(l2, type) and l2.__name__ == "_NotAvailableSocket"):
            return False, "Scapy 未能加载 Npcap 后端（wpcap.dll 加载失败）"
        try:
            from scapy.all import get_if_list
            if not get_if_list():
                return False, "未检测到网络接口"
        except Exception as e:  # noqa: BLE001
            return False, f"接口枚举失败: {e}"
        return True, "ok"

    # ---------- 生命周期 ----------
    def start(self, targets: list[str] | None = None, ports: str | None = None,
              rate_pps: int = 8000) -> None:
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, args=(targets or ["0.0.0.0/0"], ports, rate_pps),
            daemon=True, name="l4-scapy")
        self._thread.start()
        REGISTRY.set_running(MODULE, True)

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._sniffer is not None:
                self._sniffer.stop()
        except Exception:  # noqa: BLE001
            pass
        REGISTRY.set_running(MODULE, False)

    # ---------- 主流程 ----------
    def _run(self, targets: list[str], ports: str | None, rate_pps: int) -> None:
        from scapy.all import IP, TCP, AsyncSniffer, send
        # Windows 下必须走 Npcap 的 L3 套接字：
        # 默认的 L3WinSocket 使用 Winsock 原始套接字，禁止发送 TCP 包，
        # 会抛 "TCP data cannot be sent over raw socket"。
        try:
            from scapy.config import conf
            from scapy.arch.libpcap import L3pcapSocket
            conf.L3socket = L3pcapSocket
            log.info("已启用 Npcap L3 套接字后端（可发送 TCP 探测包）")
        except Exception as e:  # noqa: BLE001
            log.warning("无法启用 Npcap L3 套接字（%s），退回默认后端", e)

        port_list = self._parse_ports(ports or self.cfg.get("l4", "phase1_ports"))
        ok, reason = self.available()
        if not ok:
            log.error("Scapy 后端不可用: %s", reason)
            REGISTRY.set_extra(MODULE, scapy_error=reason)
            REGISTRY.set_running(MODULE, False)
            return

        bpf = f"tcp[13] & 0x12 = 0x12 and dst port {self.source_port}"
        try:
            self._sniffer = AsyncSniffer(filter=bpf, prn=self._on_packet, store=False)
            self._sniffer.start()
        except Exception as e:  # noqa: BLE001
            log.error("抓包线程启动失败（Npcap 是否已安装？）: %s", e)
            REGISTRY.set_extra(MODULE, scapy_error=str(e))
            REGISTRY.set_running(MODULE, False)
            return

        REGISTRY.set_extra(MODULE, engine="scapy", rate_pps=rate_pps)
        interval = 1.0 / max(1, rate_pps)
        try:
            for ip in self._iter_targets(targets):
                if self._stop.is_set():
                    break
                for port in port_list:
                    if self._stop.is_set():
                        break
                    if self.bw:
                        self.bw.acquire("l4_scan", SYN_PKT_SIZE)
                    pkt = IP(dst=ip) / TCP(dport=port, sport=self.source_port,
                                           seq=random.randint(0, 2**32 - 1), flags="S")
                    try:
                        send(pkt, verbose=False)
                        self.sent += 1
                    except Exception:  # noqa: BLE001
                        REGISTRY.incr(MODULE, "send_errors")
                    if interval >= 1e-4:
                        time.sleep(interval)
                REGISTRY.set_extra(MODULE, sent=self.sent)
        finally:
            # 等待最后一批响应，再关闭抓包
            time.sleep(3)
            try:
                self._sniffer.stop()
            except Exception:  # noqa: BLE001
                pass
            REGISTRY.set_running(MODULE, False)

    def _on_packet(self, pkt) -> None:
        """处理 SYN-ACK 响应（在抓包线程中执行，需轻量）。"""
        try:
            from scapy.layers.inet import IP as IPLayer, TCP as TCPLayer
            if IPLayer not in pkt or TCPLayer not in pkt:
                return
            if pkt[TCPLayer].flags != "SA":
                return
            ip = pkt[IPLayer].src
            port = int(pkt[TCPLayer].sport)
            if self.exclude and self.exclude.contains(ip):
                REGISTRY.incr(MODULE, "excluded")
                return
            self.out.put({"ip": ip, "port": port, "proto": "tcp", "status": "open",
                          "ttl": pkt[IPLayer].ttl, "ts": int(time.time()),
                          "family": 6 if ":" in ip else 4, "engine": "scapy"})
            REGISTRY.incr(MODULE, "open_ports")
        except Exception:  # noqa: BLE001
            pass

    # ---------- 工具 ----------
    @staticmethod
    def _parse_ports(spec: str) -> list[int]:
        ports: list[int] = []
        for part in str(spec).split(","):
            part = part.strip()
            if not part:
                continue
            if "-" in part:
                a, b = part.split("-", 1)
                ports.extend(range(int(a), int(b) + 1))
            else:
                ports.append(int(part))
        return ports

    @staticmethod
    def _iter_targets(targets: list[str]):
        """按 CIDR 迭代主机地址（跳过网络号/广播号，支持 IPv4/IPv6）。"""
        for t in targets:
            try:
                net = ipaddress.ip_network(t, strict=False)
            except ValueError:
                continue
            if net.prefixlen in (net.max_prefixlen, net.max_prefixlen - 1):
                yield str(net.network_address)
                continue
            for host in net.hosts():
                yield str(host)
