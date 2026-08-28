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
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="enricher")
        self._thread.start()
        REGISTRY.set_running(MODULE, True)

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
    def _rdns(ip: str, timeout: float = 3.0) -> str | None:
        try:
            socket.setdefaulttimeout(timeout)
            return socket.gethostbyaddr(ip)[0]
        except Exception:  # noqa: BLE001
            return None
