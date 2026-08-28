"""列式层压缩器：NDJSON.zst 分片 -> Parquet 分区表（DuckDB 可直接查询）。

- 表：hosts（L7 抓取记录）、l4_open（开放端口）、classification（站点分类）；
- 分区：table=<name>/date=YYYY-MM-DD/part-NNNN.parquet；
- zstd level 6 + 字典/游程编码，空间效率对标 Censys delta encoding 思路。
- 已在 DuckDB 注册的视图见 catalog.py。
"""
from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from orchestrator.config import Config
from orchestrator.state import REGISTRY

log = logging.getLogger("netatlas.compactor")
MODULE = "compactor"


def iter_ndjson_zst(path: Path):
    dctx = zstd.ZstdDecompressor()
    with open(path, "rb") as fh, dctx.stream_reader(fh) as reader:
        import io
        for line in io.TextIOWrapper(reader, encoding="utf-8", errors="replace"):
            line = line.strip()
            if line:
                import json
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


class Compactor:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.raw = cfg.abs_path("paths", "data_raw")
        self.out = cfg.abs_path("paths", "data_parquet")
        self.level = int(cfg.get("storage", "parquet_compression_level", default=6))
        self.interval = int(cfg.get("storage", "compact_interval_min", default=30)) * 60
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._done: set[str] = set()

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="compactor")
        self._thread.start()
        REGISTRY.set_running(MODULE, True)

    def stop(self) -> None:
        self._stop.set()
        REGISTRY.set_running(MODULE, False)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.compact_all()
            except Exception as e:  # noqa: BLE001
                log.error("compact 失败: %s", e)
                REGISTRY.set_extra(MODULE, last_error=str(e))
            self._stop.wait(self.interval)

    def compact_all(self) -> int:
        """把所有未压缩的分片转 Parquet。返回新文件数。"""
        made = 0
        if not self.raw.exists():
            return 0
        for day_dir in sorted(self.raw.iterdir()):
            if not day_dir.is_dir():
                continue
            date = day_dir.name
            for stream_dir in day_dir.iterdir():
                if not stream_dir.is_dir():
                    continue
                table = stream_dir.name
                shards = sorted(stream_dir.glob("part-*.jsonl.zst"))
                for shard in shards:
                    key = f"{date}/{table}/{shard.name}"
                    if key in self._done:
                        continue
                    out_dir = self.out / f"table={table}" / f"date={date}"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_path = out_dir / (shard.stem.replace(".jsonl", "") + ".parquet")
                    rows = list(iter_ndjson_zst(shard))
                    if not rows:
                        self._done.add(key)
                        continue
                    # 动态 schema：取全集键，值统一为 string 以避免异构冲突（JSON 语义保留）
                    keys = sorted({k for r in rows for k in r})
                    cols = {k: [_flatten(r.get(k)) for r in rows] for k in keys}
                    tbl = pa.table(cols)
                    pq.write_table(tbl, out_path, compression="zstd",
                                   compression_level=self.level)
                    self._done.add(key)
                    made += 1
                    REGISTRY.incr(MODULE, "parquet_files")
        REGISTRY.set_extra(MODULE, last_run=time.strftime("%H:%M:%S"), parquet_dir=str(self.out))
        return made


def _flatten(v):
    """Parquet 列统一 string 化，复杂结构 JSON 序列化。"""
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        import json
        return json.dumps(v, ensure_ascii=False, default=str)
    return str(v)
