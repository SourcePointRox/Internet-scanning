"""查询目录层：DuckDB 视图 + SQLite 元数据库。

- DuckDB：对 data/parquet 下的分区表注册视图，供 WebUI / 分析脚本即席查询；
- SQLite：任务状态、模块统计快照、站点分类索引、错误事件（小数据高频读写）。
  WAL 模式 + busy_timeout，多线程/多连接安全。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path

from orchestrator.config import Config

log = logging.getLogger("netatlas.catalog")

SCHEMA = """
CREATE TABLE IF NOT EXISTS site_classification (
    host TEXT PRIMARY KEY,
    ip TEXT,
    port INTEGER,
    category_path TEXT,        -- JSON 数组：完整分类层级
    leaf_category TEXT,        -- 最小子类
    confidence REAL,
    signals TEXT,              -- 命中信号（JSON）
    updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_leaf ON site_classification(leaf_category);
CREATE TABLE IF NOT EXISTS module_stats (
    ts INTEGER, module TEXT, key TEXT, value INTEGER,
    PRIMARY KEY (ts, module, key)
);
CREATE TABLE IF NOT EXISTS optout_log (
    ts INTEGER, cidr TEXT, requester TEXT, note TEXT
);
CREATE TABLE IF NOT EXISTS error_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER, module TEXT, level TEXT, message TEXT, exception TEXT, context TEXT
);
CREATE INDEX IF NOT EXISTS idx_err_ts ON error_events(ts);
"""


class Catalog:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        meta_dir = cfg.abs_path("paths", "data_meta")
        meta_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = meta_dir / "netatlas.db"
        self._local = threading.local()
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    def connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self.db_path), timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    # ---------- 分类结果 ----------
    def upsert_classification(self, host: str, ip: str, port: int,
                              category_path: list[str], confidence: float,
                              signals: list[str]) -> None:
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO site_classification
                   (host, ip, port, category_path, leaf_category, confidence, signals, updated_at)
                   VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(host) DO UPDATE SET
                     ip=excluded.ip, port=excluded.port,
                     category_path=excluded.category_path,
                     leaf_category=excluded.leaf_category,
                     confidence=excluded.confidence,
                     signals=excluded.signals, updated_at=excluded.updated_at""",
                (host, ip, port, json.dumps(category_path, ensure_ascii=False),
                 category_path[-1] if category_path else "Unknown",
                 confidence, json.dumps(signals, ensure_ascii=False), int(time.time())))

    def classification_stats(self) -> list[tuple[str, int]]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT leaf_category, COUNT(*) FROM site_classification "
                "GROUP BY leaf_category ORDER BY 2 DESC LIMIT 50").fetchall()

    # ---------- 错误事件持久化（ErrorReporter 钩子写入） ----------
    def log_error(self, event: dict) -> None:
        try:
            with self.connect() as conn:
                conn.execute(
                    "INSERT INTO error_events (ts, module, level, message, exception, context)"
                    " VALUES (?,?,?,?,?,?)",
                    (event.get("ts", int(time.time())), event.get("module"),
                     event.get("level"), event.get("message"),
                     event.get("exception"), json.dumps(event.get("context") or {},
                                                        ensure_ascii=False)))
        except sqlite3.Error as e:
            log.warning("错误事件落库失败: %s", e)

    def recent_errors(self, n: int = 50) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT ts, module, level, message, exception FROM error_events "
                "ORDER BY seq DESC LIMIT ?", (n,)).fetchall()
        return [{"ts": r[0], "module": r[1], "level": r[2], "message": r[3],
                 "exception": r[4]} for r in rows]

    # ---------- DuckDB 即席查询 ----------
    def duck(self):
        """惰性创建 DuckDB 连接并注册 Parquet 视图。"""
        import duckdb
        con = duckdb.connect()
        pq_root = self.cfg.abs_path("paths", "data_parquet")
        if pq_root.exists():
            for tbl_dir in pq_root.glob("table=*"):
                tbl = tbl_dir.name.split("=", 1)[1]
                if not tbl.isidentifier():
                    log.warning("跳过非法表名目录: %s", tbl_dir.name)
                    continue
                pattern = str(tbl_dir / "date=*" / "*.parquet").replace("\\", "/")
                try:
                    con.execute(
                        f'CREATE OR REPLACE VIEW "{tbl}" AS '
                        f"SELECT * FROM read_parquet('{pattern}', "
                        f"hive_partitioning=true, union_by_name=true)")
                except Exception as e:  # noqa: BLE001 —— 空目录/类型冲突时该表降级为不可用
                    log.warning("Parquet 视图 %s 注册失败（该表暂不可查）: %s", tbl, e)
        return con

    def storage_overview(self) -> dict:
        """各存储层路径与体量（WebUI 展示用）。"""
        def dir_info(p: Path) -> dict:
            total = sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.exists() else 0
            files = sum(1 for f in p.rglob("*") if f.is_file()) if p.exists() else 0
            return {"path": str(p), "size_mb": round(total / 1e6, 2), "files": files,
                    "exists": p.exists()}
        return {
            "raw_layer": dir_info(self.cfg.abs_path("paths", "data_raw")),
            "parquet_layer": dir_info(self.cfg.abs_path("paths", "data_parquet")),
            "meta_layer": dir_info(self.cfg.abs_path("paths", "data_meta")),
            "seeds": dir_info(self.cfg.abs_path("paths", "seeds")),
            "logs": dir_info(self.cfg.abs_path("paths", "logs")),
        }
