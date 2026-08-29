"""NetAtlas 主编排器：装配流水线、模块生命周期、WebUI 托管。

流水线：
  l4_scanner → l4_q → l7_grabber → l7_q → enricher → enrich_q → classifier → class_q → storage_writer
                                                         (IPv6 TGA 作为 l4 的目标源，由调度器按需注入)
"""
from __future__ import annotations

import argparse
import logging
import queue
import signal
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.bandwidth import BandwidthController
from orchestrator.config import load
from orchestrator.state import REGISTRY
from modules.l4_scanner import L4Scanner
from modules.l7_grabber import L7Grabber
from modules.enrich import Enricher
from modules.classifier import Classifier
from storage.writer import StorageWriter
from storage.compactor import Compactor
from storage.catalog import Catalog

log = logging.getLogger("netatlas")


class Orchestrator:
    def __init__(self, config_path: str | None = None, dry_run: bool = False,
                 targets: list[str] | None = None):
        self.cfg = load(config_path)
        self.dry_run = dry_run
        self.targets = targets

        # ---- 基础设施 ----
        bw_cfg = self.cfg.get("bandwidth", default={}) or {}
        self.bandwidth = BandwidthController(
            upload_mbps=float(bw_cfg.get("upload_mbps", 25.0)),
            cap_pct=float(bw_cfg.get("global_cap_pct", 80.0)),
            quotas_mbps=dict(bw_cfg.get("quotas", {})) or None or {
                "l4_scan": 12.0, "l7_grab": 8.0, "dns_enrich": 2.0, "reserve": 3.0})
        self.catalog = Catalog(self.cfg)

        # ---- 队列（有界，天然背压） ----
        self.l4_q: queue.Queue[dict] = queue.Queue(maxsize=100_000)
        self.l7_q: queue.Queue[dict] = queue.Queue(maxsize=50_000)
        self.enrich_q: queue.Queue[dict] = queue.Queue(maxsize=50_000)
        self.class_q: queue.Queue[dict] = queue.Queue(maxsize=50_000)

        # ---- 模块 ----
        self.l4 = L4Scanner(self.cfg, self.l4_q, dry_run=dry_run, bandwidth=self.bandwidth)
        self.l7 = L7Grabber(self.cfg, self.l4_q, self.l7_q, bandwidth=self.bandwidth)
        self.enricher = Enricher(self.cfg, self.l7_q, self.enrich_q, bandwidth=self.bandwidth)
        self.classifier = Classifier(self.cfg, self.enrich_q, self.catalog, out_queue=self.class_q)
        self.writer = StorageWriter(self.cfg, {"hosts": self.class_q})
        self.compactor = Compactor(self.cfg)

        # ---- 注册启停钩子（WebUI 控制） ----
        REGISTRY.register("l4_scanner", lambda: self.l4.start(targets=self.targets), self.l4.stop)
        REGISTRY.register("l7_grabber", self.l7.start, self.l7.stop)
        REGISTRY.register("enricher", self.enricher.start, self.enricher.stop)
        REGISTRY.register("classifier", self.classifier.start, self.classifier.stop)
        REGISTRY.register("storage_writer", self.writer.start, self.writer.stop)
        REGISTRY.register("compactor", self.compactor.start, self.compactor.stop)

    def start_pipeline(self) -> None:
        # 下游先行，避免队列堆积丢数据
        for name in ["storage_writer", "compactor", "classifier", "enricher", "l7_grabber", "l4_scanner"]:
            ok = REGISTRY.start(name)
            log.info("模块 %-16s %s", name, "已启动" if ok else "启动失败")

    def stop_pipeline(self) -> None:
        for name in ["l4_scanner", "l7_grabber", "enricher", "classifier", "compactor", "storage_writer"]:
            REGISTRY.stop(name)


def setup_logging(root: Path) -> None:
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(log_dir / "netatlas.log", encoding="utf-8")])


def main() -> None:
    parser = argparse.ArgumentParser(description="NetAtlas 互联网扫描平台编排器")
    parser.add_argument("--config", default=None)
    parser.add_argument("--dry-run", action="store_true", help="不发包，模拟流水线")
    parser.add_argument("--targets", nargs="*", default=None, help="限定扫描范围（CIDR）")
    parser.add_argument("--no-webui", action="store_true")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    setup_logging(root)
    orch = Orchestrator(args.config, dry_run=args.dry_run, targets=args.targets)

    def shutdown(*_):
        log.info("收到退出信号，停止流水线...")
        orch.stop_pipeline()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    orch.start_pipeline()

    if not args.no_webui:
        from webui.backend.app import run_webui
        run_webui(orch)  # 阻塞
    else:
        log.info("流水线运行中（无 WebUI）。Ctrl+C 退出。")
        threading.Event().wait()


if __name__ == "__main__":
    main()
