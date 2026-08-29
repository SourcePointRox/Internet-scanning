"""Scapy/Npcap 无状态 SYN 扫描后端（masscan 缺失时的内置引擎）。

设计要点：
- **无状态校验**：把目标端口编码进 TCP 序号（seq = port），收到 SYN-ACK 时
  用 ack-1 还原端口，无需为在途探针维护连接状态表（masscan 同款思路）；
- **速率受控**：发包前经过全局令牌桶（l4_scan 配额），每包按 74 字节计费；
- **顺序随机化**：以 /24 块为单位洗牌后遍历，降低对同一网段的瞬时冲击；
- **合规**：排除列表过滤、仅发 SYN 探针、不做任何 exploit 行为。

Windows 依赖 Npcap（项目部署要求）。
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

SYN_PKT_BYTES = 74      # 以太网帧 + IP 头 + TCP 头（SYN，无选项）
SPORT_BASE = 41000
BATCH_SIZE = 256


def parse_ports(spec: str) -> list[int]:
    """解析端口表达式，如 "21-23,80,443,8000-8002"。"""
    ports: list[int] = []
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            ports.extend(range(int(lo), int(hi) + 1))
        else:
            ports.append(int(part))
    return sorted(set(ports))


def iter_targets(cidrs, shuffle_prefix: int = 24, rng: random.Random | None = None):
    """按 /24 块洗牌后遍历目标 IP。"""
    rng = rng or random.Random()
    for cidr in cidrs:
        net = ipaddress.ip_network(cidr, strict=False)
        if net.version != 4:
            continue
        prefix = max(net.prefixlen, min(shuffle_prefix, 32))
        blocks = list(net.subnets(new_prefix=prefix))
        rng.shuffle(blocks)
        for block in blocks:
            for host in block.hosts():
                yield str(host)


class ScapyL4Scanner:
    def __init__(self, cfg, out_queue: "queue.Queue[dict]", exclude, bandwidth=None,
                 iface: str | None = None):
        self.cfg = cfg
        self.out = out_queue
        self.exclude = exclude
        self.bw = bandwidth
        self.iface = iface or cfg.get("l4", "iface", default=None)
        self.rate_pps = int(cfg.get("l4", "default_rate_pps", default=8000))
        # 扫描结束后的冷却期，用于接收晚到的响应（masscan 的 --cooldown-time 同理）
        self.cooldown_s = float(cfg.get("l4", "cooldown_s", default=10.0))
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._sent = 0

    # ---------- 可用性自检 ----------
    @staticmethod
    def available() -> tuple[bool, str]:
        try:
            from scapy.all import AsyncSniffer, conf  # noqa: F401
        except ImportError as e:
            return False, f"scapy 未安装 ({e})"
        try:
            ifaces = [i for i in __import__("scapy.all", fromlist=["IFACES"]).IFACES.values()]
            if not ifaces:
                return False, "未检测到网络接口（Npcap 是否安装？）"
        except Exception as e:  # noqa: BLE001
            return False, f"接口枚举失败: {e}"
        return True, "ok"

    # ---------- 生命周期 ----------
    def start(self, targets: list[str] | None = None, ports: str | None = None,
              rate_pps: int | None = None) -> None:
        if any(t.is_alive() for t in self._threads):
            return
        self._stop.clear()
        if rate_pps:
            self.rate_pps = rate_pps
        ports_list = parse_ports(ports or self.cfg.get("l4", "phase1_ports"))
        t = threading.Thread(target=self._run, args=(targets or ["0.0.0.0/0"], ports_list),
                             daemon=True, name="l4-scapy")
        t.start()
        self._threads = [t]

    def stop(self) -> None:
        self._stop.set()

    def set_rate(self, pps: int) -> None:
        self.rate_pps = max(10, min(pps, int(self.cfg.get("l4", "max_rate_pps", default=18000))))
        REGISTRY.set_extra(MODULE, rate_pps=self.rate_pps)

    # ---------- 主流程 ----------
    @staticmethod
    def _find_iface_by_ip(ip: str):
        """按本机 IP 反查网卡名（用于二层直连）。"""
        try:
            from scapy.all import IFACES
            for i in IFACES.values():
                if i.ip == ip:
                    return i.name
        except Exception:  # noqa: BLE001
            pass
        return None

    def _run(self, targets: list[str], ports: list[int]) -> None:
        from scapy.all import AsyncSniffer, IP, TCP, conf, send
        # L2 直连模式：配置了 source_ip + router_mac 时绕过 VPN 隧道
        # （经 VPN 的 L3 探测会得到伪造的 SYN-ACK，扫描结果不可用）
        self.src_ip = self.cfg.get("l4", "source_ip") or None
        self.router_mac = self.cfg.get("l4", "router_mac") or None
        self.l2 = bool(self.src_ip and self.router_mac)
        if self.l2:
            iface_by_ip = self._find_iface_by_ip(self.src_ip)
            if iface_by_ip and not self.iface:
                self.iface = iface_by_ip
        elif not self.iface:
            # 未显式指定网卡时，自动选用通往公网的默认路由接口
            try:
                self.iface = conf.route.route("0.0.0.0")[0]
            except Exception:  # noqa: BLE001
                self.iface = None
        if self.iface:
            conf.iface = self.iface
        log.info("Scapy 无状态扫描启动：iface=%s ports=%d targets=%s",
                 conf.iface, len(ports), ",".join(map(str, targets))[:80])
        REGISTRY.set_extra(MODULE, engine="scapy", iface=str(conf.iface), rate_pps=self.rate_pps)

        bpf = "tcp[13] & 0x12 == 0x12"   # 仅收 SYN-ACK
        if self.src_ip:
            bpf += f" and dst host {self.src_ip}"
        sniffer = AsyncSniffer(
            iface=conf.iface,
            filter=bpf,
            prn=self._on_packet, store=False)
        sniffer.start()
        time.sleep(0.5)
        try:
            for port in ports:                    # 外层端口、内层地址（masscan 同构）
                if self._stop.is_set():
                    break
                self._scan_port(targets, port, IP, TCP, send)
        except Exception as e:  # noqa: BLE001
            log.exception("Scapy 扫描异常: %s", e)
            REGISTRY.set_extra(MODULE, last_error=str(e)[:200])
        finally:
            # 冷却：等待在途响应回包（用户主动停止则立即退出）
            if not self._stop.is_set() and self.cooldown_s > 0:
                deadline = time.monotonic() + self.cooldown_s
                while time.monotonic() < deadline and not self._stop.is_set():
                    time.sleep(0.2)
            try:
                sniffer.stop()
            except Exception:  # noqa: BLE001
                pass
            REGISTRY.set_running(MODULE, False)
            log.info("Scapy 扫描停止，累计发包 %d", self._sent)

    def _scan_port(self, targets: list[str], port: int, IP, TCP, send) -> None:
        rng = random.Random()
        batch: list = []
        last_tick = time.monotonic()
        for ip in iter_targets(targets, rng=rng):
            if self._stop.is_set():
                break
            if self.exclude.contains(ip):
                REGISTRY.incr(MODULE, "excluded")
                continue
            l3 = IP(dst=ip, id=port & 0xFFFF)
            if self.l2:                            # 二层直连：指定源 IP，绕过 VPN
                l3.src = self.src_ip
            pkt = l3 / TCP(dport=port, sport=rng.randrange(SPORT_BASE, 61000),
                           flags="S", seq=port)    # 无状态校验：seq = 目标端口
            if self.l2:
                from scapy.all import Ether
                pkt = Ether(dst=self.router_mac) / pkt
            batch.append(pkt)
            if len(batch) >= BATCH_SIZE:
                self._flush(batch, send)
                batch = []
                expected = BATCH_SIZE / max(self.rate_pps, 1)
                now = time.monotonic()
                if now - last_tick < expected:
                    time.sleep(expected - (now - last_tick))
                last_tick = time.monotonic()
        if batch:
            self._flush(batch, send)

    def _flush(self, batch: list, send) -> None:
        if self.bw:
            self.bw.acquire("l4_scan", SYN_PKT_BYTES * len(batch))
        try:
            if self.l2:                            # 二层直连用 sendp
                from scapy.all import sendp
                sendp(batch, iface=self.iface, verbose=0)
            else:
                send(batch, verbose=0)
        except Exception as e:  # noqa: BLE001
            log.warning("发包失败(%d 包): %s", len(batch), e)
            REGISTRY.incr(MODULE, "send_errors")
            return
        self._sent += len(batch)
        REGISTRY.incr(MODULE, "packets_sent", len(batch))

    def _on_packet(self, pkt) -> None:
        """SYN-ACK 响应处理：由 ack-1 还原被探测端口。"""
        try:
            # 兼容 scapy 各版本：优先 scapy.layers.inet（2.5+），回退 scapy.all
            try:
                from scapy.layers.inet import IP as SIP, TCP as STCP
            except ImportError:  # pragma: no cover
                from scapy.all import IP as SIP, TCP as STCP
            if SIP not in pkt or STCP not in pkt:
                return
            tcp, ip = pkt[STCP], pkt[SIP]
            if tcp.flags != "SA":
                return
            port = int(tcp.ack) - 1
            if not 0 <= port <= 65535:
                return
            src = ip.src
            if self.exclude.contains(src):
                REGISTRY.incr(MODULE, "excluded")
                return
            self.out.put({
                "ip": src, "port": port, "proto": "tcp", "status": "open",
                "ttl": ip.ttl, "ts": int(time.time()),
                "family": 6 if ":" in src else 4,
            })
            REGISTRY.incr(MODULE, "open_ports")
        except Exception:  # noqa: BLE001
            pass
