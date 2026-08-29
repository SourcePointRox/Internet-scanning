"""主机富化模块：BGP/ASN、地理定位、正反向 DNS、时延。

- GeoIP/ASN：MaxMind GeoLite2 mmdb（缺失时优雅降级，字段留空、流程不断）；
- DNS：默认走内置纯 Python 异步解析器（modules/dns_async.AsyncDNSResolver，
  单事件循环数百在途查询，彻底绕开 GIL 与线程池瓶颈）；
  enrichment.dns.engine=threads 时回退线程池模式（兼容旧行为）；
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
from orchestrator.concurrency import AsyncConsumer, ThreadPoolConsumer
from orchestrator.errors import ErrorReporter
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.enrich")
MODULE = "enricher"


class GeoResolver:
    """MaxMind GeoLite2 查询（可选依赖，缺失即降级）。"""

    def __init__(self, geoip_dir: Path):
        self.city = self.asn = None
        try:
            import geoip2.database  # type: ignore
        except ImportError:
            ErrorReporter.get().degrade(MODULE, "geoip", "geoip2 未安装，地理/ASN 富化关闭")
            return
        city_db, asn_db = geoip_dir / "GeoLite2-City.mmdb", geoip_dir / "GeoLite2-ASN.mmdb"
        try:
            if city_db.exists():
                self.city = geoip2.database.Reader(str(city_db))
            if asn_db.exists():
                self.asn = geoip2.database.Reader(str(asn_db))
            if not (self.city or self.asn):
                ErrorReporter.get().degrade(MODULE, "geoip",
                                            f"mmdb 文件缺失于 {geoip_dir}，地理/ASN 富化关闭")
        except Exception as e:  # noqa: BLE001 —— mmdb 损坏：降级而非崩溃
            ErrorReporter.get().report(MODULE, "GeoIP 库加载失败，地理富化降级", exc=e)
            self.city = self.asn = None

    def lookup(self, ip: str) -> dict:
        out: dict = {}
        if self.city:
            try:
                r = self.city.city(ip)
                out["geo"] = {"country": r.country.iso_code, "city": r.city.name,
                              "lat": r.location.latitude, "lon": r.location.longitude}
            except Exception as e:  # noqa: BLE001 —— 未收录地址属正常情况，计数不告警
                if type(e).__name__ != "AddressNotFoundError":
                    REGISTRY.incr(MODULE, "geoip_errors")
        if self.asn:
            try:
                r = self.asn.asn(ip)
                out["asn"] = {"number": r.autonomous_system_number,
                              "org": r.autonomous_system_organization}
            except Exception as e:  # noqa: BLE001
                if type(e).__name__ != "AddressNotFoundError":
                    REGISTRY.incr(MODULE, "geoip_errors")
        return out


class Enricher:
    """富化消费者：异步 DNS 引擎（默认）或线程池（兜底）。"""

    def __init__(self, cfg: Config, in_queue: "queue.Queue[dict]",
                 out_queue: "queue.Queue[dict]", bandwidth=None):
        self.cfg = cfg
        self.in_q = in_queue
        self.out_q = out_queue
        self.bw = bandwidth
        self.geo = GeoResolver(cfg.abs_path("paths", "data_geoip"))
        self.reverse_dns = bool(cfg.get("enrichment", "reverse_dns", default=True))
        self.threads_n = int(cfg.get("enrichment", "threads", default=16))
        self.dns_engine = str(cfg.get("enrichment", "dns", "engine", default="auto")).lower()
        self._consumer = None

    # ---------- 记录处理（两种引擎共用） ----------
    def _enrich_base(self, rec: dict) -> dict:
        ip = rec.get("ip", "")
        if ip:
            rec.update(self.geo.lookup(ip))
        return rec

    def _count(self, rec: dict) -> dict:
        REGISTRY.incr(MODULE, "enriched")
        return rec

    # ---------- 异步引擎 ----------
    async def _handle_async(self, rec: dict) -> dict:
        rec = self._enrich_base(rec)
        ip = rec.get("ip", "")
        if self.reverse_dns and ip and not rec.get("domain"):
            assert self._resolver is not None
            rdns = await self._resolver.reverse(ip)
            if rdns:
                rec["reverse_dns"] = rdns
                REGISTRY.incr(MODULE, "rdns_ok")
            else:
                REGISTRY.incr(MODULE, "rdns_miss")
        return self._count(rec)

    # ---------- 线程池兜底 ----------
    def _handle_threaded(self, rec: dict) -> dict:
        rec = self._enrich_base(rec)
        ip = rec.get("ip", "")
        if self.reverse_dns and ip and not rec.get("domain"):
            try:
                rdns = socket.gethostbyaddr(ip)[0]
                rec["reverse_dns"] = rdns
                REGISTRY.incr(MODULE, "rdns_ok")
            except (socket.herror, socket.gaierror, socket.timeout, OSError):
                REGISTRY.incr(MODULE, "rdns_miss")
        return self._count(rec)

    # ---------- 生命周期 ----------
    def start(self) -> None:
        use_async = self.dns_engine in ("auto", "async")
        if use_async:
            from modules.dns_async import AsyncDNSResolver
            dns_cfg = self.cfg.get("enrichment", "dns", default={}) or {}
            self._resolver = AsyncDNSResolver(
                nameservers=list(dns_cfg.get("nameservers") or []) or None,
                timeout_s=float(dns_cfg.get("timeout_ms", 2500)) / 1000,
                concurrency=int(dns_cfg.get("concurrency", 256)))
            self._consumer = AsyncConsumer(
                "enricher", self.in_q, self._handle_async,
                concurrency=int(dns_cfg.get("concurrency", 256)),
                out_q=self.out_q, module=MODULE)
            engine = "async-dns"
        else:
            socket.setdefaulttimeout(3.0)  # 线程池模式：进程级 DNS 超时（一次性）
            self._resolver = None
            self._consumer = ThreadPoolConsumer(
                "enricher", self.in_q, self._handle_threaded,
                workers=self.threads_n, out_q=self.out_q, module=MODULE)
            engine = f"threads-{self.threads_n}"
        self._consumer.start()
        REGISTRY.set_running(MODULE, True)
        REGISTRY.set_extra(MODULE, dns_engine=engine)

    def stop(self) -> None:
        if self._consumer:
            self._consumer.stop()
        REGISTRY.set_running(MODULE, False)
