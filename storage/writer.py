"""原始层写入器：zstd 压缩 NDJSON 滚动分片。

借鉴 Censys 写入侧"最小处理、落盘优先"原则：
- 记录到达即追加到当日分片，不做富化（富化由并行消费者完成）；
- 分片按 data/raw/YYYY-MM-DD/<stream>/part-NNNN.jsonl.zst 组织；
- 达到 raw_shard_mb 或跨天自动滚动；zstd 流式压缩（约 10:1）。

可靠性强化：
- 磁盘水位保护：剩余空间低于 storage.disk_min_free_mb 时暂停写入并
  上报 CRITICAL 告警（不再静默丢数据），恢复后自动续写；
- 写入失败按指数退避重试，最终失败上报 ErrorReporter；
- 每条记录经 storage.schema.stamp 盖章（schema 版本 + 血缘）。
"""
from __future__ import annotations

import json
import logging
import queue
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import zstandard as zstd

from orchestrator.config import Config
from orchestrator.errors import ErrorReporter
from orchestrator.state import REGISTRY
from storage import schema

log = logging.getLogger("netatlas.storage")
MODULE = "storage_writer"


class DiskFullError(OSError):
    """磁盘剩余空间低于安全水位。"""


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
            except (OSError, ValueError) as e:
                ErrorReporter.get().report(MODULE, f"分片 {self.stream} 刷盘失败", exc=e)
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
            try:
                self._writer.close()
            except (OSError, ValueError) as e:
                ErrorReporter.get().report(MODULE, f"分片 {self.stream} 关闭失败",
                                           level="warning", exc=e)
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
        self.disk_min_free = int(cfg.get("storage", "disk_min_free_mb", default=512)) * 1024 * 1024
        self.writers: dict[str, ShardWriter] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._disk_blocked = False  # 磁盘水位保护状态

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="storage-writer")
        self._thread.start()
        REGISTRY.set_running(MODULE, True)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)  # 排空缓冲再关闭
        for w in self.writers.values():
            w.close()
        REGISTRY.set_running(MODULE, False)

    def _disk_ok(self) -> bool:
        """磁盘水位检查：低于阈值暂停写入并告警（每轮只告警一次状态翻转）。"""
        try:
            free = shutil.disk_usage(self.base).free
        except OSError as e:
            ErrorReporter.get().report(MODULE, "磁盘用量查询失败", exc=e)
            return True  # 查询失败不阻塞写入
        if free < self.disk_min_free:
            if not self._disk_blocked:
                self._disk_blocked = True
                ErrorReporter.get().report(
                    MODULE, f"磁盘剩余 {free // 1024 // 1024} MB 低于安全水位，写入暂停")
                REGISTRY.set_extra(MODULE, disk_blocked=True)
            return False
        if self._disk_blocked:
            self._disk_blocked = False
            ErrorReporter.get().report(MODULE, "磁盘水位恢复，写入继续", level="warning")
            REGISTRY.set_extra(MODULE, disk_blocked=False)
        return True

    def _write_with_retry(self, w: ShardWriter, rec: dict, attempts: int = 3) -> bool:
        delay = 0.2
        for i in range(attempts):
            try:
                w.write(rec)
                return True
            except (OSError, zstd.ZstdError) as e:
                if i == attempts - 1:
                    ErrorReporter.get().report(MODULE, "记录写入失败（重试耗尽）", exc=e,
                                               stream=w.stream)
                    REGISTRY.incr(MODULE, "write_errors")
                    return False
                time.sleep(delay)
                delay *= 2
        return False

    def _run(self) -> None:
        reporter = ErrorReporter.get()
        while not self._stop.is_set():
            if not self._disk_ok():
                time.sleep(5.0)  # 磁盘满：停写轮询，上游队列背压自然生效
                continue
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
                if isinstance(rec, dict):
                    schema.stamp(rec, source=MODULE)
                self._write_with_retry(w, rec)
                w.flush_if_due(self.flush_interval)
            if idle:
                for w in self.writers.values():
                    w.flush_if_due(0.0)
                time.sleep(0.05)
        # 退出前排空队列，尽量减少停机丢数据
        drained = 0
        for stream, q in self.queues.items():
            w = self.writers.get(stream)
            if w is None:
                continue
            while True:
                try:
                    rec = q.get_nowait()
                except queue.Empty:
                    break
                if isinstance(rec, dict):
                    schema.stamp(rec, source=MODULE)
                if self._write_with_retry(w, rec, attempts=1):
                    drained += 1
        if drained:
            reporter.report(MODULE, f"停机前排空 {drained} 条缓冲记录", level="warning")
