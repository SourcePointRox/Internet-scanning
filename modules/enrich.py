"""主机富化模块：BGP/ASN、地理定位、正反向 DNS、时延。

- GeoIP/ASN：MaxMind GeoLite2 mmdb（需用户按许可下载放入 data/geoip/），
  缺失时优雅降级（字段留空，流程不断）；
- BGP 路由：pyasn 离线 RIB 快照（可选 data/geoip/iptoasn.dat）；
- DNS：asyncio + socket 批量正/反向解析，可控并发；
- RTT：优先使用 L7 握手记录的 connect RTT。
"""
from __future__ import annotations

import asyncio
import logging
import queue
import socket
import threading
import time
from pathlib import Path

from orchestrator.config import Config
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.enrich")
MODULE = "enricher"


class GeoResolver:
    """MaxMind GeoLite2 查询（可选依赖，缺失即降级）。"""

    def __init__(self, geoip_dir: Path):
        self.city = self.asn = None
        try:
            import geoip2.database  # type: ignore
            city_db = geoip_dir / "GeoLite2-City.mmdb"
            asn_db = geoip_dir / "GeoLite2-ASN.mmdb"
            if city_db.exists():
                self.city = geoip2.database.Reader(str(city_db))
            if asn_db.exists():
                self.asn = geoip2.database.Reader(str(asn_db))
        except ImportError:
            log.warning("geoip2 未安装，地理/ASN 富化降级")

    def lookup(self, ip: str) -> dict:
        out: dict = {}
        if self.city:
            try:
                r = self.city.city(ip)
                out["geo"] = {"country": r.country.iso_code, "city": r.city.name,
                              "lat": r.location.latitude, "lon": r.location.longitude}
            except Exception:  # noqa: BLE001
                pass
        if self.asn:
            try:
                r = self.asn.asn(ip)
                out["asn"] = {"number": r.autonomous_system_number,
                              "org": r.autonomous_system_organization}
            except Exception:  # noqa: BLE001
                pass
        return out


class Enricher:
    """并行消费者：从 L7 输出取记录，附加富化字段后送入下游队列。"""

    def __init__(self, cfg: Config, in_queue: "queue.Queue[dict]",
                 out_queue: "queue.Queue[dict]", bandwidth=None):
        self.cfg = cfg
        self.in_q = in_queue
        self.out_q = out_queue
        self.bw = bandwidth
        self.geo = GeoResolver(cfg.abs_path("paths", "data_geoip"))
        self.reverse_dns = bool(cfg.get("enrichment", "reverse_dns", default=True))
        # rDNS 为阻塞式系统调用（每条最长数秒），单线程消费会成为整条流水线的瓶颈；
        # queue.Queue 线程安全，直接用消费线程池水平扩展（默认 16，可配）
        self.threads_n = int(cfg.get("enrichment", "threads", default=16))
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        if self.reverse_dns:
            # gethostbyaddr 不支持超时参数，只能设进程级默认超时（一次性设置，
            # 避免每次调用重复修改全局状态）
            socket.setdefaulttimeout(3.0)

    def start(self) -> None:
        self._stop.clear()
        self._threads = [
            threading.Thread(target=self._run, daemon=True, name=f"enricher-{i}")
            for i in range(self.threads_n)
        ]
        for t in self._threads:
            t.start()
        REGISTRY.set_running(MODULE, True)
        REGISTRY.set_extra(MODULE, threads=self.threads_n)

    def stop(self) -> None:
        self._stop.set()
        REGISTRY.set_running(MODULE, False)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                rec = self.in_q.get(timeout=0.5)
            except queue.Empty:
                continue
            ip = rec.get("ip", "")
            if ip:
                rec.update(self.geo.lookup(ip))
                if self.reverse_dns and not rec.get("domain"):
                    rdns = self._rdns(ip)
                    if rdns:
                        rec["reverse_dns"] = rdns
                        REGISTRY.incr(MODULE, "rdns_ok")
            self.out_q.put(rec)
            REGISTRY.incr(MODULE, "enriched")

    @staticmethod
    def _rdns(ip: str) -> str | None:
        try:
            return socket.gethostbyaddr(ip)[0]
        except Exception:  # noqa: BLE001
            return None
