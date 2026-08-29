"""NetAtlas 主编排器：装配流水线、模块生命周期、WebUI 托管。

流水线：
  l4_scanner → l4_q → l7_grabber → l7_q → enricher → enrich_q → classifier → class_q → storage_writer
                                                         (IPv6 TGA 作为 l4 的目标源，由调度器按需注入)

装配的基础设施：
- 状态持久化（StateStore）：REGISTRY 快照周期落盘，重启恢复累计计数；
- 扫描状态机（ScanStateMachine）：L4 断点续扫；
- AIMD 调速（RateController）：分片式准实时调速；
- 错误总线（ErrorReporter）：全模块结构化错误上报 + SQLite 持久化；
- 分布式分片（ShardCoordinator/HashedSharder）：默认单机，预留多机接口。
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
from orchestrator.errors import ErrorReporter
from orchestrator.nicmon import NicMonitor
from orchestrator.persistence import ScanStateMachine, StateStore
from orchestrator.ratecontrol import RateController
from orchestrator.sharding import HashedSharder, build_coordinator, stable_node_id
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
        for w in self.cfg.validate():
            log.warning("[配置校验] %s", w)
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

        # 错误总线：内存环形缓冲 + SQLite 持久化双写
        self.errors = ErrorReporter.get()
        self.errors.add_hook(self.catalog.log_error)

        # 状态持久化：周期落盘 + 启动恢复
        persist_cfg = self.cfg.get("persistence", default={}) or {}
        self.state_store: StateStore | None = None
        if persist_cfg.get("enabled", True):
            self.state_store = StateStore(
                self.cfg.abs_path("persistence", "file")
                if self.cfg.get("persistence", "file")
                else self.cfg.abs_path("paths", "data_meta") / "runtime_state.json",
                interval_s=float(persist_cfg.get("interval_s", 15)))
            prev = self.state_store.load()
            if prev.get("modules"):
                REGISTRY.restore(prev["modules"])
                log.info("已恢复上次会话的模块累计计数（saved_at=%s）", prev.get("saved_at"))

        # 扫描状态机（L4 断点续扫）
        resume_cfg = self.cfg.get("l4", "resume", default={}) or {}
        self.scan_state: ScanStateMachine | None = None
        if resume_cfg.get("enabled", True):
            state_file = Path(str(self.cfg.get("l4", "resume", "state_file",
                                               default="data/meta/scan_state.json")))
            if not state_file.is_absolute():
                state_file = self.cfg.root / state_file
            self.scan_state = ScanStateMachine(state_file)

        # AIMD 动态调速
        adapt_cfg = self.cfg.get("l4", "rate_adapt", default={}) or {}
        self.rate_controller = RateController(
            initial_pps=int(self.cfg.get("l4", "default_rate_pps", default=8000)),
            max_pps=int(self.cfg.get("l4", "max_rate_pps", default=18000)),
            up_step_pct=float(adapt_cfg.get("up_step_pct", 10.0)),
            down_factor=float(adapt_cfg.get("down_factor", 0.5)),
            loss_high_pct=float(adapt_cfg.get("loss_high_pct", 5.0)),
            interval_s=float(adapt_cfg.get("interval_s", 10.0)))

        # 分布式分片（默认单机）
        dist_cfg = self.cfg.get("distributed", default={}) or {}
        self.coordinator = build_coordinator(self.cfg)
        self.node_id = dist_cfg.get("node_id") or stable_node_id()
        self.sharder = HashedSharder(int(dist_cfg.get("shard_total", 1)),
                                     int(dist_cfg.get("shard_index", 0)))
        self.coordinator.register_node(self.node_id, {
            "shard_total": self.sharder.total, "shard_index": self.sharder.index,
            "dry_run": dry_run})

        # ---- 网卡级真实吞吐监控（外部进程流量不经过令牌桶，曲线数据源）----
        self.nic = NicMonitor()
        self.nic.start()

        # ---- 队列（有界，天然背压） ----
        self.l4_q: queue.Queue[dict] = queue.Queue(maxsize=100_000)
        self.l7_q: queue.Queue[dict] = queue.Queue(maxsize=50_000)
        self.enrich_q: queue.Queue[dict] = queue.Queue(maxsize=50_000)
        self.class_q: queue.Queue[dict] = queue.Queue(maxsize=50_000)

        # ---- 模块 ----
        self.l4 = L4Scanner(self.cfg, self.l4_q, dry_run=dry_run, bandwidth=self.bandwidth,
                            scan_state=self.scan_state, rate_controller=self.rate_controller,
                            sharder=self.sharder if self.sharder.total > 1 else None)
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
        if self.state_store:
            self.state_store.start(REGISTRY)
        # 下游先行，避免队列堆积丢数据
        for name in ["storage_writer", "compactor", "classifier", "enricher", "l7_grabber", "l4_scanner"]:
            ok = REGISTRY.start(name)
            log.info("模块 %-16s %s", name, "已启动" if ok else "启动失败")

    def stop_pipeline(self) -> None:
        for name in ["l4_scanner", "l7_grabber", "enricher", "classifier", "compactor", "storage_writer"]:
            REGISTRY.stop(name)
        if self.state_store:
            self.state_store.stop(REGISTRY)
        self.coordinator.heartbeat(self.node_id,
                                   self.scan_state.snapshot() if self.scan_state else {})

    # ---- WebUI 管控扩展 ----
    def scan_progress(self) -> dict:
        """扫描进度 + 调速状态（WebUI 展示）。"""
        return {
            "scan_state": self.scan_state.snapshot() if self.scan_state else None,
            "rate": self.rate_controller.snapshot(),
            "shard": {"node_id": self.node_id, "total": self.sharder.total,
                      "index": self.sharder.index,
                      "coordinator": type(self.coordinator).__name__},
        }


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
