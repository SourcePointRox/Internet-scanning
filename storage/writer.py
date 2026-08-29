"""原始层写入器：zstd 压缩 NDJSON 滚动分片。

借鉴 Censys 写入侧"最小处理、落盘优先"原则：
- 记录到达即追加到当日分片，不做富化（富化由并行消费者完成）；
- 分片按 data/raw/YYYY-MM-DD/<stream>/part-NNNN.jsonl.zst 组织；
- 达到 raw_shard_mb 或跨天自动滚动；zstd 流式压缩（约 10:1）。
"""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import zstandard as zstd

from orchestrator.config import Config
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.storage")
MODULE = "storage_writer"


class ShardWriter:
    """单 stream 的滚动分片写入器。"""

    def __init__(self, base_dir: Path, stream: str, shard_mb: int = 256, level: int = 3):
        self.base_dir, self.stream, self.shard_bytes = base_dir, stream, shard_mb * 1024 * 1024
        self.cctx = zstd.ZstdCompressor(level=level)
        self._fh = None
        self._writer = None
        self._cur_path: Path | None = None
        self._cur_size = 0
        self._cur_day = ""
        self._seq = 0
        self._last_flush = time.monotonic()

    def flush_if_due(self, interval_s: float) -> None:
        """按时间刷盘：保证数据在 interval 秒内可读，避免进程异常退出丢数据。"""
        if self._writer and time.monotonic() - self._last_flush >= interval_s:
            try:
                self._writer.flush()      # 结束当前 zstd 帧（文件可增量读取）
                self._fh.flush()
            except Exception:  # noqa: BLE001
                pass
            self._last_flush = time.monotonic()

    def _roll(self) -> None:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._fh and day == self._cur_day and self._cur_size < self.shard_bytes:
            return
        self.close()
        self._cur_day = day
        out_dir = self.base_dir / day / self.stream
        out_dir.mkdir(parents=True, exist_ok=True)
        self._seq = len(list(out_dir.glob("part-*.jsonl.zst")))
        self._cur_path = out_dir / f"part-{self._seq:04d}.jsonl.zst"
        self._fh = open(self._cur_path, "wb")
        self._writer = self.cctx.stream_writer(self._fh)
        self._cur_size = 0
        REGISTRY.set_extra(MODULE, **{f"{self.stream}_shard": str(self._cur_path)})

    def write(self, record: dict) -> None:
        self._roll()
        line = (json.dumps(record, ensure_ascii=False, default=str) + "\n").encode("utf-8")
        self._writer.write(line)
        self._cur_size += len(line)
        REGISTRY.incr(MODULE, f"{self.stream}_records")
        REGISTRY.incr(MODULE, "bytes_written", len(line))

    def close(self) -> None:
        if self._writer:
            self._writer.close()
            self._writer = None
        if self._fh:
            self._fh.close()
            self._fh = None


class StorageWriter:
    """消费各输出队列并写入原始层。"""

    def __init__(self, cfg: Config, queues: dict[str, "queue.Queue[dict]"]):
        self.cfg = cfg
        self.queues = queues
        self.base = cfg.abs_path("paths", "data_raw")
        self.shard_mb = int(cfg.get("storage", "raw_shard_mb", default=256))
        self.flush_interval = float(cfg.get("storage", "flush_interval_s", default=5.0))
        self.writers: dict[str, ShardWriter] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="storage-writer")
        self._thread.start()
        REGISTRY.set_running(MODULE, True)

    def stop(self) -> None:
        self._stop.set()
        for w in self.writers.values():
            w.close()
        REGISTRY.set_running(MODULE, False)

    def _run(self) -> None:
        while not self._stop.is_set():
            idle = True
            for stream, q in self.queues.items():
                try:
                    rec = q.get(timeout=0.2)
                except queue.Empty:
                    continue
                idle = False
                w = self.writers.get(stream)
                if w is None:
                    w = self.writers[stream] = ShardWriter(self.base, stream, self.shard_mb)
                w.write(rec)
                w.flush_if_due(self.flush_interval)
            if idle:
                # 空闲时立即刷盘，保证已采集数据可读
                for w in self.writers.values():
                    w.flush_if_due(0.0)
                time.sleep(0.05)
