"""测试用假 masscan：模拟外部扫描进程的命令行协议。

行为由环境变量控制：
  FAKE_MASSCAN_MODE     quick | hang | crash | resume-crash
  FAKE_MASSCAN_ARGV_LOG 每次启动把 argv 追加写入该文件（断言用）
  FAKE_MASSCAN_RECORDS  输出的开放端口 JSON 条数（默认 5）

quick        : 输出 N 条记录 + 状态行，退出 0（自然完成）
hang         : 持续输出记录，直到收到中断（写 paused.conf 后退出 0）
crash        : 输出 1 条记录后退出码 1（模拟崩溃）
resume-crash : 带 --resume 启动时正常完成；否则崩溃（验证续扫路径）
"""
import json
import os
import signal
import sys
import time

MODE = os.environ.get("FAKE_MASSCAN_MODE", "quick")
ARGV_LOG = os.environ.get("FAKE_MASSCAN_ARGV_LOG")
N = int(os.environ.get("FAKE_MASSCAN_RECORDS", "5"))

RESUME = None
if "--resume" in sys.argv:
    RESUME = sys.argv[sys.argv.index("--resume") + 1]


def log_argv():
    if ARGV_LOG:
        with open(ARGV_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(sys.argv[1:]) + "\n")


def emit_records(n):
    for i in range(n):
        # 注意：不得使用 TEST-NET 段（在项目排除列表中），用未保留的 203.0.114.x
        rec = {"ip": f"203.0.114.{i + 1}", "timestamp": int(time.time()), "ttl": 54,
               "ports": [{"port": 80 + i, "proto": "tcp", "status": "open",
                          "reason": "syn-ack", "ttl": 54}]}
        print(json.dumps(rec) + ",", flush=True)
    print(f"rate:  8.00-kpps, {min(99, n * 10)}% done, found: {n}", flush=True)


def write_paused():
    try:
        with open("paused.conf", "w", encoding="utf-8") as f:
            f.write("resume = 1\nrange = 203.0.114.0/24\n")
    except OSError:
        pass


def main():
    log_argv()
    if MODE == "crash":
        emit_records(1)
        sys.exit(1)
    if MODE == "resume-crash":
        if RESUME:
            emit_records(N)
            sys.exit(0)
        write_paused()
        sys.exit(1)
    if MODE == "hang":
        def _sig(*_):
            write_paused()
            sys.exit(0)
        for sig_name in ("SIGINT", "SIGBREAK", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is not None:
                signal.signal(sig, _sig)
        i = 0
        while True:
            emit_records(1)
            i += 1
            time.sleep(0.05)
        return
    # quick
    emit_records(N)
    sys.exit(0)


if __name__ == "__main__":
    main()
