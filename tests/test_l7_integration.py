"""L7Grabber 集成测试：Python asyncio 引擎对本地真实 HTTP/banner 服务抓取。

覆盖：HTTP 状态/头部/标题提取、banner 抓取、RTT 测量、瞬时错误重试、
未知端口直通、zgrab2 输出归一化（http/tls/banner 三种模块）。
"""
import asyncio
import json
import queue
import sys
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from modules.l7_grabber import L7Grabber, PORT_PROTOCOL
from orchestrator.config import load


class _HTTPHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"<html><head><title>NetAtlas Test Server</title></head><body>hi</body></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _BannerServer(threading.Thread):
    """TCP banner 桩：连接即推送 SSH 风格 banner。"""

    def __init__(self):
        super().__init__(daemon=True)
        import socket
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]
        self._stop = threading.Event()
        self.start()

    def run(self):
        self.sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            try:
                conn.sendall(b"SSH-2.0-OpenSSH_9.6 NetAtlas-Test\r\n")
                time.sleep(0.05)
            except OSError:
                pass
            conn.close()

    def close(self):
        self._stop.set()
        self.sock.close()


class TestL7PythonEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _HTTPHandler)
        cls.http_port = cls.httpd.server_address[1]
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()
        cls.banner = _BannerServer()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.banner.close()

    def _make_grabber(self):
        cfg = load()
        cfg.data["l7"]["retry"]["max_attempts"] = 2
        in_q, out_q = queue.Queue(), queue.Queue()
        g = L7Grabber(cfg, in_q, out_q)
        g.engine = "python"  # 强制 Python 引擎（测试环境与 zgrab2 无关）
        return g, in_q, out_q

    def _grab(self, hit: dict) -> dict:
        g, in_q, out_q = self._make_grabber()
        g.start()
        in_q.put(hit)
        try:
            return out_q.get(timeout=15)
        finally:
            g.stop()

    def test_http_grab_full_fields(self):
        rec = self._grab({"ip": "127.0.0.1", "port": self.http_port, "proto": "tcp"})
        # 测试端口未在 PORT_PROTOCOL 映射中 → banner 模式；改注册为 http 再测
        PORT_PROTOCOL[self.http_port] = "http"
        try:
            rec = self._grab({"ip": "127.0.0.1", "port": self.http_port, "proto": "tcp"})
        finally:
            del PORT_PROTOCOL[self.http_port]
        self.assertEqual(rec["protocol"], "http")
        self.assertIn("200", rec["http"]["status"])
        self.assertEqual(rec["http"]["title"], "NetAtlas Test Server")
        self.assertGreater(rec["rtt_ms"], 0)
        self.assertIn("NetAtlas", rec["body_sample"])
        self.assertIn("ts", rec)

    def test_banner_grab(self):
        PORT_PROTOCOL[self.banner.port] = "banner"
        try:
            rec = self._grab({"ip": "127.0.0.1", "port": self.banner.port, "proto": "tcp"})
        finally:
            del PORT_PROTOCOL[self.banner.port]
        self.assertIn("SSH-2.0", rec["banner"])
        self.assertGreaterEqual(rec["rtt_ms"], 0)

    def test_connection_refused_returns_error_record(self):
        """连接拒绝：错误必须作为数据记录返回（含重试计数），不得丢失目标。"""
        rec = self._grab({"ip": "127.0.0.1", "port": 1, "proto": "tcp"})
        self.assertIn("error", rec)
        self.assertEqual(rec["port"], 1)

    def test_concurrent_grabs(self):
        g, in_q, out_q = self._make_grabber()
        PORT_PROTOCOL[self.http_port] = "http"
        g.start()
        try:
            for i in range(20):
                in_q.put({"ip": "127.0.0.1", "port": self.http_port, "proto": "tcp"})
            got = 0
            deadline = time.time() + 20
            while got < 20 and time.time() < deadline:
                try:
                    out_q.get(timeout=1)
                    got += 1
                except queue.Empty:
                    pass
            self.assertEqual(got, 20, "20 并发抓取必须全部返回")
        finally:
            del PORT_PROTOCOL[self.http_port]
            g.stop()


class TestZgrabNormalize(unittest.TestCase):
    def test_normalize_http(self):
        rec = {"ip": "203.0.113.7",
               "data": {"http": {"status": "success", "result": {
                   "response": {"status_line": "200 OK", "headers": {"server": ["nginx"]},
                                "body": "<html>x</html>"}}}}}
        out = L7Grabber._normalize_zgrab(rec, 80, "http")
        self.assertEqual(out["http"]["status"], "200 OK")
        self.assertEqual(out["http"]["body_len"], len("<html>x</html>"))
        self.assertEqual(out["engine"], "zgrab2")
        self.assertNotIn("error", out)

    def test_normalize_tls_chain(self):
        rec = {"ip": "203.0.113.8",
               "data": {"tls": {"status": "success", "result": {
                   "handshake_log": {"server_certificates": {"chain": [{
                       "parsed": {
                           "subject_dn": "CN=example.org",
                           "subject": {"common_name": "example.org"},
                           "issuer": {"common_name": "Test CA"},
                           "issuer_dn": "CN=Test CA",
                           "validity": {"start": "2026-01-01", "end": "2027-01-01"},
                           "extensions": {"subject_alt_name": {"dns_names": ["example.org"]}},
                           "subject_key_info": {"key_algorithm": {"name": "RSA"}},
                       }}]}}}}}}
        out = L7Grabber._normalize_zgrab(rec, 443, "https")
        summary = out["tls"]["cert_summary"][0]
        self.assertEqual(summary["common_name"], "example.org")
        self.assertEqual(summary["issuer_cn"], "Test CA")
        self.assertEqual(summary["key_algorithm"], "RSA")

    def test_normalize_failure_status(self):
        rec = {"ip": "203.0.113.9",
               "data": {"ssh": {"status": "connection-timeout", "result": {}, "error": "timeout"}}}
        out = L7Grabber._normalize_zgrab(rec, 22, "ssh")
        self.assertIn("error", out)
        self.assertEqual(out["zgrab_status"], "connection-timeout")


if __name__ == "__main__":
    unittest.main()
