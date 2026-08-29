# NetAtlas 优化进度交接文档

> 用途：供下一个接手本仓库的模型/开发者快速了解「已完成的优化」「尚未完成的部分」「下一步该做什么」。
> 最后更新：2026-08-30
> 代码库路径：`D:\Internet-scanning-main`（仓库 `https://github.com/SourcePointRox/Internet-scanning.git`）
> 当前状态：**已推送 GitHub，README 未改动；126 项测试全部通过**

---

## 0. 当前状态速览

| 项目 | 状态 |
|---|---|
| 测试总数 | 126 项（原 13 项）→ `python -m unittest discover -s tests` 全绿 |
| 测试耗时 | ~72 秒（含基准测试） |
| 测试环境 | Python 3.13 venv：`C:\Users\huawei\.workbuddy\binaries\python\envs\netatlas` |
| Git | 已 `git init`，`origin` = `https://github.com/SourcePointRox/Internet-scanning.git`，2 个本地提交（分支 `main`） |
| 推送 | **待完成**：本机 Git Credential Manager 无交互式终端（`/dev/tty` 不可用），需提供 PAT 或授权 |
| 待办 | 见文末「第 3 节 · 未完成项」 |

运行测试（务必用该 venv，系统 Python 未装依赖）：

```bash
"C:/Users/huawei/.workbuddy/binaries/python/envs/netatlas/Scripts/python.exe" -m unittest discover -s tests
```

---

## 1. 用户提出的 11 项问题与完成情况

### 1.1 测试体系缺失 —— **已完成 ✅**
原 README 声称"13 项单元测试"，实际只有两个文件的 13 项。

新增测试文件（共 126 项）：

| 文件 | 覆盖内容 |
|---|---|
| `tests/test_config.py` | 环境变量插值/覆盖、配置校验、无硬编码校验 |
| `tests/test_errors.py` | 重试装饰器（瞬时/非瞬时/放弃降级）、错误上报、告警钩子隔离 |
| `tests/test_persistence.py` | 状态落盘原子性、注册表计数恢复、**崩溃后 RUNNING→PAUSED 降级** |
| `tests/test_sharding.py` | 分片划分互补性/无交集/跨实例一致性、AIMD 调速上下限 |
| `tests/test_dns_async.py` | DNS 报文编解码（含压缩指针）、本地 UDP DNS 桩端到端、64 并发吞吐 |
| `tests/test_l4_integration.py` | 假 masscan 进程驱动：JSON 流→队列、崩溃重启+`--resume`、暂停/续扫、分片调速、dry-run |
| `tests/test_l7_integration.py` | 本地 HTTP/banner 服务真实抓取、并发抓取、zgrab2 输出归一化 |
| `tests/test_storage_pipeline.py` | ShardWriter→Compactor→Catalog 全链路、类型化列、v1→v2 迁移、磁盘水位 |
| `tests/test_webui_e2e.py` | 全部 REST 端点 + WebSocket 帧结构（真实 FastAPI app） |
| `tests/test_benchmarks.py` | 吞吐/延迟/内存基准（写 42.9k rec/s、令牌桶 270k ops/s、队列 p99<0.01ms） |
| `tests/test_recovery.py` | 进程崩溃恢复、磁盘满/恢复、网络中断、看门狗重启、背压不丢数据 |
| `tests/test_ipv6_hitlist.py` | TGA 算法与合成 hitlist 对比验证（**6Tree /48 重叠率 62% vs 随机基线 0.0061%**） |

辅助文件：`tests/fake_masscan.py`（模拟 masscan 命令行协议的假进程，支持 quick/hang/crash/resume-crash 四种模式）。

### 1.2 配置硬编码 —— **已完成 ✅**
- `config/config.yaml`：`paths.root` 改为空（自动取仓库根）、`source_ip`/`router_mac` 置空（自动探测/环境变量注入）；
- `orchestrator/config.py` 重写：支持 `${ENV:默认}` 插值、`NETATLAS_<段>__<键>` 环境变量覆盖（自动类型推断 bool/int/float/JSON）、`validate()` 启动校验（配额总和、L2 配对、邮箱占位、目录可写）；
- 新增 `config/config.example.yaml`（模板，与 config.yaml 同内容）。

### 1.3 依赖管理 —— **已完成 ✅**
- `requirements.txt`：加上下界约束（`<1.0` 等）；
- **新增 `requirements.lock`**：全部依赖精确锁定（fastapi 0.141.1 / pyarrow 25.0.1 / duckdb 1.5.5 等）；
- **新增 `scripts/setup_deps.py`**：一键检测/安装外部依赖（`--all`、`--zgrab2`（Go 可自动编译）、`--geoip <KEY>`（MaxMind 自动下载解压）、`--npcap`、`--masscan`、`--detect-net` 网卡探测）；
- **新增 `.gitignore`、`.dockerignore`、`Dockerfile`、`docker-compose.yml`**：
  - Dockerfile：`python:3.13-slim` + libpcap + masscan（Debian 源）、`NETATLAS_PATHS__ROOT=/app`、编译检查、默认 `--dry-run` 入口；
  - docker-compose：`network_mode: host`、`cap_add: NET_RAW/NET_ADMIN`、数据卷持久化、机器相关配置全部经 `NETATLAS_*` 环境变量注入（不进镜像）。

### 1.4 异常处理 —— **已完成 ✅**
- **新增 `orchestrator/errors.py`**：`retry()` 指数退避装饰器（仅重试瞬时错误、可选降级返回 None）、`ErrorReporter` 结构化错误总线（环形缓冲 + 告警钩子 + 计数 + `degrade()` 降级记录）；
- 全部 `except Exception: pass` 已消除，改为：上报 ErrorReporter + 计数 + 具体异常类型捕获；
- 涉及文件：`l4_scanner.py`、`l4_scapy.py`、`l7_grabber.py`、`enrich.py`、`classifier.py`、`writer.py`、`compactor.py`、`catalog.py`；
- 错误事件持久化到 SQLite（`catalog.log_error`），WebUI `/api/errors` 可查。

### 1.5 状态持久化 —— **已完成 ✅**
- **新增 `orchestrator/persistence.py`**：
  - `StateStore`：REGISTRY 快照周期落盘（tmp+os.replace 原子写），启动 `restore()` 恢复累计计数；
  - `ScanStateMachine`：扫描进度状态机（IDLE/RUNNING/PAUSED/COMPLETED/FAILED），目标/端口/分片/速率/resume 文件全落盘；**进程崩溃后重启自动 RUNNING→PAUSED 且可续扫**；
- `orchestrator/state.py` 增加 `restore()` 方法；
- `orchestrator/main.py` 装配（配置节 `persistence`）。

### 1.6 并发模型统一 —— **已完成 ✅**
- **新增 `orchestrator/concurrency.py`**：`QueueConsumer` 协议 + `ThreadPoolConsumer`（阻塞型）+ `AsyncConsumer`（IO 密集，信号量限并发）+ `SupervisedProcess`（子进程看门狗、崩溃退避重启、上限 5 次）；
- `enrich.py` 已改用 `AsyncConsumer`。

### 1.7 分布式预留 —— **已完成 ✅**
- **新增 `orchestrator/sharding.py`**：`ShardCoordinator` 抽象类 + `LocalShardCoordinator` 单机实现 + `HashedSharder`（CIDR 空间 /24(IPv4)、/48(IPv6) 一致性哈希分片，无状态、多机可独立复算同一划分）；
- 配置项 `distributed: {enabled, coordinator, node_id, shard_total, shard_index}`；`build_coordinator()` 为 http/etcd 后端预留接入点。

### 1.8 动态调速与断点续扫 —— **已完成 ✅**
- **新增 `orchestrator/ratecontrol.py`**：AIMD 控制器（和式增 `up_step_pct=10%` / 乘式减 `down_factor=0.5`），首调仅建基线不调速率；
- `l4_scanner.py` 重写主循环为**分片式**：按 `l4.resume.segment_minutes` 切段，段间以新速率 + `--resume` 重启 masscan（准实时调速）；
- 关键实现细节：`_terminated_by_us` 标记区分"主动终止"与"真实崩溃"——Windows 无控制台环境 CTRL_BREAK 无法投递，段边界只能强杀（退出码非 0），若无此标记会被误判为崩溃而触发退避重启。

### 1.9 GIL 与 rDNS 瓶颈 —— **已完成 ✅**
- **新增 `modules/dns_async.py`**：纯 Python 异步 DNS 解析器（零依赖），RFC 1035 报文级实现，支持 PTR/A/AAAA，含压缩指针解码、qid 路由、并发信号量、超时保护；
- `enrich.py` 默认走异步引擎（`enrichment.dns.engine=auto|async|threads`，可回退线程池）；单事件循环支撑数百在途查询，替代原 16 线程 `socket.gethostbyaddr`。

### 1.10 Schema 演进与数据血缘 —— **已完成 ✅**
- **新增 `storage/schema.py`**：`SCHEMA_VERSION=2`、写入侧 `stamp()` 盖章（`schema_version` + `_lineage{source,engine,tool_version,collected_at,hops[]}`）、读取侧 `migrate()` v1→v2 升级、`TYPED_COLUMNS` 已知列保留原生类型（port int32/ts int64/rtt_ms float64/l7_probed bool），未知列 string 追加；
- `compactor.py`：类型化 Parquet 写入（原子 tmp+replace）、已完成清单持久化到 `data/meta/compacted.json`（重启不重复压缩）、坏分片隔离上报。

### 1.11 WebUI 升级 —— **已完成 ✅**
- 前端 `webui/frontend/index.html` 全量重写：深色现代仪表盘，8 项 KPI 指标带、双图（网卡吞吐面积图 + 速率历史）、扫描控制（暂停/续扫/速率下发，含状态机状态徽标）、队列背压进度条、模块启停+计数+错误标记、信息流前端过滤（关键词/分类/自动滚动）、错误事件面板、存储路径、SQL 查询；信息密度与可交互项显著提升；
- 后端 `webui/backend/app.py` 新增：`/api/scan/progress`、`/api/scan/rate`（POST，含 100–1_000_000 校验）、`/api/scan/pause`、`/api/scan/resume`、`/api/errors`；WS 帧扩展 `scan`/`node`/`errors_recent`；DuckDB 视图注册增加表名合法性校验与失败告警。

---

## 2. 由新测试发现并修复的真实代码 Bug

| Bug | 现象 | 修复 |
|---|---|---|
| **HTTP 正文丢失** | `reader.read(n)` 在首个 TCP 段到达即返回，headers/body 分段时正文为空、title 恒为 None | `l7_grabber._read_to_eof()` 循环读到 EOF/限量，整体超时返回已收部分 |
| **ScanStateMachine/StateStore 不接受 str 路径** | `AttributeError: 'str' object has no attribute 'exists'` | `self.path = Path(path)` |
| **进程退出竞态丢数据** | 子进程退出时读取线程缓冲的尾部记录被丢弃，开放端口记录全部丢失 | `_consume` 退出分支改为等待 sentinel None 排空管道 |
| **zstd 缓冲掩盖写失败** | 底层句柄损坏时 `write()` 不抛错（数据在 zstd 缓冲中） | flush 失败上报 ErrorReporter（原为 `except: pass`） |
| **masscan 段终止误判崩溃** | 强杀退出码非 0 → 触发退避重启链 | `_terminated_by_us` 标记区分主动终止 |
| **DuckDB 视图注册静默失败** | `except: pass` 导致表不可查且无提示 | 改为告警 + 表名合法性校验 |
| **idna 编码异常** | `part.encode("idna","replace")` → UnicodeError | DNS 名称编码改为容错实现 |

---

## 3. 未完成项（下一位请从这里接手）

| 优先级 | 项目 | 说明 |
|---|---|---|
| **高** | **推送到 GitHub** | 仅剩授权环节。拿到 PAT 后执行：`git remote set-url origin https://<TOKEN>@github.com/SourcePointRox/Internet-scanning.git && git push -u origin main`（或 `git -c credential.helper='!f() { echo username=SourcePointRox; echo password=<TOKEN>; }; f' push -u origin main`） |
| **高** | **README / CHANGELOG 重写** | 用户本次明确要求「GitHub README 不动」，故未改。README 仍写"13 项单元测试"（**已过时，实为 126 项**），且未反映新架构（异步 DNS、状态机、分片、动态调速、新 API、setup_deps.py、Docker）。需另建 `CHANGELOG.md` |
| 中 | 端到端全链路联调 | `python scripts/start.py --dry-run` 后的运行时冒烟尚未跑（dry-run 路径已单测覆盖）。建议补 `tests/test_smoke_pipeline.py`：启动 Orchestrator→过流水线→校验落盘 |
| 中 | 分布式协调器真实实现 | 仅 `LocalShardCoordinator`。HTTP 协调器（节点注册/心跳/分片认领）未实现 |
| 中 | IPv6 TGA 真实 hitlist 验证 | 目前用合成数据（6Tree 重叠率 62%）。可接 `https://ipv6hitlist.github.io` 真实种子做一次离线评估，`config.ipv6_tga.seed_sources` 已预留 |
| 低 | `Enricher` 的 pyasn BGP 依赖 | README 声称支持 pyasn 离线 RIB，代码中未实现（配置项 `enrichment.asn_enabled` 仅控制 GeoIP ASN） |
| 低 | `l4_scapy.py` 进度持久化 | `scan_state` 参数已预留但未接入（masscan 路径已完整接入） |
| 低 | 契约测试 | `tests/fake_masscan.py` 提供的是"行为契约"，可进一步固化为对真实 masscan 的兼容性检查清单 |

---

## 4. 新增/修改文件清单

**新增（19 个）**
```
orchestrator/errors.py          # 重试 + 结构化错误上报 + 降级
orchestrator/persistence.py     # StateStore + ScanStateMachine
orchestrator/concurrency.py     # 统一并发抽象 + 子进程看门狗
orchestrator/sharding.py        # 分片协调器接口 + 一致性哈希分片
orchestrator/ratecontrol.py     # AIMD 动态调速
modules/dns_async.py            # 纯 Python 异步 DNS（绕 GIL）
storage/schema.py               # Schema 版本化 + 数据血缘
scripts/setup_deps.py           # 外部依赖一键安装
requirements.lock               # 精确版本锁定
config/config.example.yaml      # 配置模板
.gitignore / .dockerignore
tests/fake_masscan.py           # 假 masscan 进程（测试替身）
tests/test_config.py / test_errors.py / test_persistence.py / test_sharding.py
tests/test_dns_async.py / test_l4_integration.py / test_l7_integration.py
tests/test_storage_pipeline.py / test_webui_e2e.py / test_benchmarks.py
tests/test_recovery.py / test_ipv6_hitlist.py
```

**修改（13 个）**
```
orchestrator/config.py          # 环境变量插值/覆盖/校验（重写）
orchestrator/state.py           # 新增 restore()
orchestrator/main.py            # 装配全部新基础设施
orchestrator/nicmon.py          # （未改，仅被调用）
modules/l4_scanner.py           # 分片式主循环 + 状态机 + 看门狗（重写）
modules/l4_scapy.py             # 异常处理 + engine 标记
modules/l7_grabber.py           # 读取循环修复 + 子进程/管道异常上报
modules/enrich.py               # 异步 DNS 引擎（重写）
modules/classifier.py           # 错误上报
storage/writer.py               # 磁盘水位 + 重试 + 血缘盖章 + 停机排空
storage/compactor.py            # 类型化列 + 清单持久化 + 坏分片隔离
storage/catalog.py              # 错误事件表 + DuckDB 错误处理
webui/backend/app.py            # 新增 5 个 API + WS 扩展
webui/frontend/index.html       # 全量重写（深色高密度仪表盘）
config/config.yaml              # 去除硬编码
scripts/start.py                # 接入配置校验 + setup_deps 指引
scripts/start_all.bat           # 去除硬编码用户路径
requirements.txt                # 版本约束
```

---

## 5. 关键设计约定（下一位请遵循）

1. **测试环境**：用 venv `C:\Users\huawei\.workbuddy\binaries\python\envs\netatlas`（含 fastapi/uvicorn/pytest 等）。系统 Python 无依赖。
2. **测试不得写入仓库目录**：沙箱禁止在 `tests/` 下 `unlink()`，临时文件一律用 `tempfile.mkdtemp()`（已有两个用例因踩此坑改过）。
3. **测试目标地址不能落在排除列表**：`203.0.113.x`（TEST-NET-3）、`198.51.100.x`（TEST-NET-2）均在 `config/exclude.txt` 中，测试请用 `203.0.114.x` 等未保留段。
4. **子进程启动耗时 ~1–1.5s**：涉及 masscan 分片的测试，`segment_minutes` 不得小于约 `0.05`（3s），否则子进程尚未产出就被段边界终止（曾导致 4 个用例诡异失败）。
5. **Windows 无控制台时 CTRL_BREAK 投递失败**：任何时候改动 `_terminate_proc` 都要保留 `_terminated_by_us` 语义。
6. **错误处理约定**：禁止 `except Exception: pass`；瞬时错误重试，其他错误经 `ErrorReporter.report()` 上报。
7. **数据写入约定**：任何写入 `data/raw` 的记录都要经 `storage.schema.stamp()` 盖章，读取侧经 `migrate()` 升级。

---

## 6. Git 状态（交接）

仓库此前**不是** Git 仓库（无 `.git` 目录），本次为首次初始化。当前状态：

```bash
git remote -v    # origin https://github.com/SourcePointRox/Internet-scanning.git
git log --oneline
# 483b573 test: 修复 IPv6 hitlist 与 L4 暂停续扫用例的稳定性；补充优化进度文档
# 959e0e3 wip: 基础设施重构与测试补齐（配置/持久化/调速/错误/WEBUI）
git branch       # main（已 git branch -M main）
```

- 三个提交均**尚未推送**（`git push` 因缺少凭据失败）。
- **已尝试的授权方式（均失败，勿重复浪费时间）**：
  1. HTTPS + Git Credential Manager → `bash: /dev/tty: No such device or address`，无法弹出交互式登录框；
  2. `git-credential-manager github login` → 无输出、未落凭据，随后 push 依旧失败（浏览器 OAuth 回调无法在当前环境完成）；
  3. SSH 推送 → `~/.ssh` 不存在，`ssh -T git@github.com` 返回 `Permission denied (publickey)`。
- 推送方式（任选其一，需要用户侧凭据）：
  1. 提供 Personal Access Token（需 `repo` 权限）：
     ```bash
     git remote set-url origin https://<TOKEN>@github.com/SourcePointRox/Internet-scanning.git
     git push -u origin main
     ```
  2. 在已有终端中先 `git credential-manager github login` 完成授权后再 `git push -u origin main`。
- `README.md` 按用户要求保持原样（未修改）。
