# masscan 行为契约（兼容性检查清单）

本文档把 `tests/fake_masscan.py`（测试替身）所模拟的 masscan 行为固化为**契约清单**。
`modules/l4_scanner.py` 依赖下列每一项行为；升级/更换 masscan 版本、或改用其他
发包器（如 ZMap 包装）时，请逐条验证。任一条不满足都会导致调速、续扫或数据
完整性退化。

> 验证方式：对真实 masscan 逐项执行"验证方法"列的命令/操作，比对预期行为。
> 自动化回归：`python -m unittest tests.test_l4_integration -v`（用替身跑全套）。

## 1. 命令行接口（l4_scanner 实际组装的 argv）

```
masscan <targets...> -p<ports> --rate <pps> --excludefile <file> --retries <n> \
        -oJ - --output-status open [--source-ip <ip> --router-mac <mac>] [--resume <file>]
```

| # | 契约 | 验证方法 |
|---|------|---------|
| 1.1 | `-oJ -` 把 JSON 记录流写到 **stdout**（非 stderr、非文件） | 小范围扫描，确认 stdout 逐行产出 JSON |
| 1.2 | `--output-status open` 只输出 open 记录 | 扫描已知 closed 端口，确认无记录 |
| 1.3 | `--excludefile` 的网段**绝不**出现在输出中 | 排除列表放入一个本机可控网段，扫描后 grep 输出 |
| 1.4 | `--source-ip`/`--router-mac` 必须**成对**出现才生效（二层直连） | 只给一个时 masscan 应报错或忽略，不得静默乱发 |
| 1.5 | `--resume <paused.conf>` 从中断点继续扫描 | 中断一次扫描后再 `--resume`，确认不重复已扫区段 |

## 2. stdout 记录格式（解析器 `L4Scanner._on_line` 的假设）

| # | 契约 | 说明 |
|---|------|------|
| 2.1 | 每条记录是**单行 JSON**，行尾可能带逗号（JSON 数组成员风格），解析端须 `rstrip(",")` | 已容忍 |
| 2.2 | 记录结构：`{"ip": str, "timestamp": int, "ttl": int, "ports": [{"port": int, "proto": "tcp", "status": "open", "reason": "syn-ack", "ttl": int}]}` | 缺 `ip` 或 `ports` 的行被安全跳过 |
| 2.3 | 状态行**混在同一 stdout**：`rate:  8.00-kpps, 12.34% done, found: 5` | 解析端用正则识别，不得当作 JSON 报错 |
| 2.4 | 状态行含 `found: N` 时必含 `N% done` 或可由正则分别匹配 | 用于 WebUI 进度百分比 |
| 2.5 | 非 JSON 行（启动 banner、adapter 信息等）可以任意出现 | 解析端计数 `parse_errors`，不中断 |

## 3. 中断与续扫（断点续扫的根基）

| # | 契约 | 说明 |
|---|------|------|
| 3.1 | 收到 SIGINT / CTRL_BREAK 时，masscan 在 **cwd** 写 `paused.conf` 后退出 0 | l4_scanner 以 `cwd=data/meta` 启动，resume 文件路径即 `data/meta/paused.conf` |
| 3.2 | **Windows 无控制台环境下 CTRL_BREAK 无法投递**（已知平台限制）：等待 `grace_s` 后只能 `terminate()` 强杀，此时**退出码非 0 且无 paused.conf** | 代码以 `_terminated_by_us` 标记区分"主动终止"与"真实崩溃"，**任何改动 `_terminate_proc` 必须保留该语义**，否则段边界强杀会被误判为崩溃触发退避重启 |
| 3.3 | 强杀（无 paused.conf）后，下次启动**不带** `--resume`（resume 文件不存在则忽略该参数） | `_build_cmd` 只在文件存在时追加 `--resume` |
| 3.4 | 崩溃（退出码非 0 且非主动终止）→ 看门狗指数退避重启，上限 5 次 | `SupervisedProcess` 语义 |

## 4. 分片式调速（准实时 AIMD 的载体）

| # | 契约 | 说明 |
|---|------|------|
| 4.1 | masscan **不支持运行中调速**——`--rate` 只在启动时生效 | 因此主循环按 `l4.resume.segment_minutes` 切段，片间以新速率重启 |
| 4.2 | 子进程冷启动耗时约 **1–1.5s** | 测试中 `segment_minutes` 不得 < 约 0.05（3s），否则子进程尚未产出就被段边界终止 |
| 4.3 | 段边界终止 = 主动终止（契约 3.2 的 `_terminated_by_us` 路径） | 不得触发看门狗退避 |

## 5. 数据完整性（真实 Bug 的教训，回归必查）

| # | 契约 | 背景 |
|---|------|------|
| 5.1 | 进程退出时，读取线程必须把管道中**已缓冲的尾部记录排空**（等待 sentinel）再退出 | 曾导致"开放端口记录全部丢失"（退出竞态） |
| 5.2 | stdout 读取按行流式进行，不得等进程结束才一次性读 | 大扫描会把管道撑满导致子进程阻塞 |

## 附：替身的四种模式（tests/fake_masscan.py）

| 模式 | 行为 | 覆盖的契约 |
|------|------|-----------|
| `quick` | 输出 N 条记录 + 状态行，退出 0 | 1.1 / 2.x |
| `hang` | 持续输出直到收到中断信号，写 paused.conf 退出 0 | 3.1 |
| `crash` | 输出 1 条后退出码 1 | 3.4 看门狗 |
| `resume-crash` | 带 `--resume` 正常完成，否则崩溃 | 1.5 / 3.3 |
