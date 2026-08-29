"""查询目录层：DuckDB 视图 + SQLite 元数据库。

- DuckDB：对 data/parquet 下的分区表注册视图，供 WebUI / 分析脚本即席查询；
- SQLite：任务状态、模块统计快照、站点分类索引（小数据高频读写）。
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from orchestrator.config import Config

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
            self._local.conn = conn
        return conn

    # ---------- 分类结果 ----------
    def upsert_classification(self, host: str, ip: str, port: int,
                              category_path: list[str], confidence: float,
                              signals: list[str]) -> None:
        import json, time
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

    # ---------- DuckDB 即席查询 ----------
    def duck(self):
        """惰性创建 DuckDB 连接并注册 Parquet 视图。"""
        import duckdb
        con = duckdb.connect()
        pq_root = self.cfg.abs_path("paths", "data_parquet")
        if pq_root.exists():
            for tbl_dir in pq_root.glob("table=*"):
                tbl = tbl_dir.name.split("=", 1)[1]
                pattern = str(tbl_dir / "date=*" / "*.parquet").replace("\\", "/")
                try:
                    con.execute(
                        f"CREATE OR REPLACE VIEW {tbl} AS "
                        f"SELECT * FROM read_parquet('{pattern}', "
                        f"hive_partitioning=true, union_by_name=true)")
                except Exception:  # noqa: BLE001
                    pass
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
