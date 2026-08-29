"""网站分类引擎：规则信号评分 → 多级分类（保留完整层级路径）。

- 规则来自 classification/rules/*.json（Wappalyzer 风格）；
- 每条规则命中后累加 weight，取最高分规则；低于 min_confidence → Unknown；
- 重点关注文件存储/数据分发类目（open-directory、scientific-data-mirror、
  ftp-repository、cdn-edge），单独写入 site_classification 库（catalog.py）；
- 与抓取并行：作为消费者线程从 L7 输出队列取记录实时分类。
"""
from __future__ import annotations

import json
import logging
import queue
import re
import threading
import time
from pathlib import Path

from orchestrator.config import Config
from orchestrator.state import REGISTRY
from orchestrator import livefeed
from storage.catalog import Catalog

log = logging.getLogger("netatlas.classifier")
MODULE = "classifier"


def _dig(record: dict, dotted: str):
    node = record
    for part in dotted.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            return None
    return node


class Rule:
    def __init__(self, raw: dict):
        self.id = raw["id"]
        self.category = raw["category"]
        self.weight = float(raw.get("weight", 0.5))
        self.match = raw.get("match", "any")
        self.signals = []
        for s in raw.get("signals", []):
            sig = dict(s)
            if "regex" in sig:
                sig["_re"] = re.compile(sig["regex"])
            if "regex_key" in sig:
                sig["_re_key"] = re.compile(sig["regex_key"])
            self.signals.append(sig)

    def hit(self, record: dict) -> bool:
        results = []
        for s in self.signals:
            val = _dig(record, s.get("field", ""))
            ok = False
            if "_re_key" in s and isinstance(val, dict):
                ok = any(s["_re_key"].search(k) for k in val)
            elif val is not None:
                if "_re" in s:
                    ok = bool(s["_re"].search(str(val)))
                elif "eq" in s:
                    ok = str(val) == str(s["eq"])
                elif "eq_int" in s:
                    ok = val == s["eq_int"]
                elif "eq_int_any" in s:
                    ok = val in s["eq_int_any"]
            results.append(ok)
        return all(results) if self.match == "all" else any(results)


class Classifier:
    def __init__(self, cfg: Config, in_queue: "queue.Queue[dict]", catalog: Catalog,
                 out_queue: "queue.Queue[dict] | None" = None):
        self.cfg = cfg
        self.in_q = in_queue
        self.out_q = out_queue  # 分类后的记录继续流向存储（可选）
        self.catalog = catalog
        self.min_conf = float(cfg.get("classification", "min_confidence", default=0.55))
        self.rules = self._load_rules(cfg.abs_path("classification", "rules_dir"))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @staticmethod
    def _load_rules(rules_dir: Path) -> list[Rule]:
        rules = []
        if rules_dir.exists():
            for f in sorted(rules_dir.glob("*.json")):
                for raw in json.loads(f.read_text(encoding="utf-8")):
                    rules.append(Rule(raw))
        log.info("加载分类规则 %d 条", len(rules))
        return rules

    def classify(self, record: dict) -> tuple[list[str], float, list[str]]:
        # 抓取失败（连接拒绝/超时）的记录不进入分类，避免将错误页误判为服务
        if record.get("error"):
            return (["Unknown", "Unclassified"], 0.0, ["error-skip"])
        best: tuple[list[str], float, list[str]] = (["Unknown", "Unclassified"], 0.0, [])
        for rule in self.rules:
            if rule.hit(record):
                score = min(rule.weight, 1.0)
                if score > best[1]:
                    best = (rule.category, score, [rule.id])
        if best[1] < self.min_conf:  # 低于置信阈值的弱信号不落地具体类目
            return (["Unknown", "Unclassified"], best[1], best[2])
        return best

    # ---------- 消费者循环 ----------
    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="classifier")
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
            try:
                path, conf, signals = self.classify(rec)
                host = rec.get("domain") or rec.get("ip", "")
                self.catalog.upsert_classification(
                    host=host, ip=rec.get("ip", ""), port=int(rec.get("port") or 0),
                    category_path=path, confidence=conf, signals=signals)
                rec["classification"] = {"category_path": path, "confidence": conf,
                                         "signals": signals}
                if self.out_q is not None:
                    self.out_q.put(rec)
                self._push_feed(rec, path, conf)
                REGISTRY.incr(MODULE, "classified")
                REGISTRY.incr(MODULE, f"cat::{path[-1]}")
            except Exception as e:  # noqa: BLE001 —— 单条分类失败计数上报，不拖垮消费循环
                REGISTRY.incr(MODULE, "errors")
                from orchestrator.errors import ErrorReporter
                ErrorReporter.get().report(MODULE, "记录分类失败", level="warning", exc=e)

    @staticmethod
    def _push_feed(rec: dict, path: list[str], conf: float) -> None:
        """把分类结果摘要推入实时信息流（WebUI 滚动展示）。"""
        http = rec.get("http") or {}
        title = http.get("title")
        if not title:
            m = re.search(r"<title[^>]*>(.*?)</title>", rec.get("body_sample", "") or "",
                          re.I | re.S)
            title = m.group(1).strip()[:120] if m else None
        if not title:
            headers = http.get("headers") or {}
            server = headers.get("server") if isinstance(headers, dict) else None
            if isinstance(server, list):
                server = "; ".join(str(s) for s in server[:2])
            title = server
        livefeed.push({
            "ip": rec.get("ip"), "port": rec.get("port"),
            "protocol": rec.get("protocol"),
            "domain": rec.get("domain") or rec.get("reverse_dns"),
            "category": path[-1],
            "category_path": " > ".join(path),
            "confidence": round(conf, 2),
            "title": (title or "")[:120] or None,
            "rtt_ms": rec.get("rtt_ms"),
        })
