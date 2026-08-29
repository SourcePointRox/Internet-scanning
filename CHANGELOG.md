# 更新日志（CHANGELOG）

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 约定。

---

## [2.0.0] — 2026-08-30「基础设施重构与可靠性加固」

本次为项目有史以来最大规模重构：从"能跑的原型"升级为"可断点续扫、可观测、
可分布式扩展、测试覆盖完整"的生产级测量系统。测试从 13 项扩充至 **147 项**，
重构过程中由新测试发现并修复了 7 个真实代码 Bug。

### 新增（Added）

#### 测试体系（13 → 147 项）
- 新增 14 个测试文件，覆盖配置、错误处理、持久化、分片、异步 DNS、L4/L7 集成、
  存储全链路、WebUI 端到端、基准、故障恢复、IPv6 hitlist、端到端冒烟等；
- 新增 `tests/fake_masscan.py`：模拟 masscan 命令行协议的测试替身
  （quick/hang/crash/resume-crash 四种模式）；
- 新增 `tests/test_smoke_pipeline.py`：端到端冒烟——真实装配 Orchestrator（dry-run），
  验证记录流过全部模块并落盘（含 schema 盖章与血缘校验）；
- 新增 `tests/test_http_coordinator.py`：HTTP 协调器全协议测试（真实本地服务）；
- 新增 `tests/test_scapy_state.py` / `tests/test_enrich_bgp.py`：scapy 状态机接线
  与 pyasn BGP 富化测试。

#### 可靠性与状态管理
- **新增 `orchestrator/errors.py`**：`retry()` 指数退避装饰器（仅重试瞬时错误、
  可选降级）、`ErrorReporter` 结构化错误总线（环形缓冲 + 告警钩子 + 计数 +
  `degrade()` 降级记录）；错误事件持久化到 SQLite，WebUI `/api/errors` 可查；
- **新增 `orchestrator/persistence.py`**：
  - `StateStore`：REGISTRY 快照周期落盘（tmp+os.replace 原子写），启动恢复累计计数；
  - `ScanStateMachine`：扫描进度状态机（IDLE/RUNNING/PAUSED/COMPLETED/FAILED），
    目标/端口/分片/速率/resume 文件全落盘；**进程崩溃重启后自动 RUNNING→PAUSED
    且可续扫**；
- **新增 `orchestrator/ratecontrol.py`**：AIMD 动态调速控制器
  （和式增 +10% / 乘式减 ×0.5），首调仅建基线不调速率；
- `modules/l4_scanner.py` 重写主循环为**分片式**：按时间段切段，片间以新速率 +
  `--resume` 重启 masscan（准实时调速）；进程看门狗崩溃退避重启（上限 5 次）；
- `storage/writer.py`：磁盘水位保护（低于阈值暂停并告警）、停机排空。

#### 并发与 DNS
- **新增 `orchestrator/concurrency.py`**：`QueueConsumer` 协议 +
  `ThreadPoolConsumer`（阻塞型）+ `AsyncConsumer`（IO 密集，信号量限并发）+
  `SupervisedProcess`（子进程看门狗）；
- **新增 `modules/dns_async.py`**：纯 Python 异步 DNS 解析器（零依赖），
  RFC 1035 报文级实现（PTR/A/AAAA、压缩指针解码、qid 路由、并发信号量、
  超时保护）；单事件循环支撑数百在途查询，彻底绕开 GIL 与线程池瓶颈；
  `enrich.py` 默认走异步引擎（`enrichment.dns.engine=auto|async|threads` 可回退）。

#### 分布式
- **新增 `orchestrator/sharding.py`**：
  - `HashedSharder`：CIDR 空间 /24（IPv4）、/48（IPv6）一致性哈希分片，
    无状态、多机独立复算同一划分；
  - **HTTP 协调器完整实现**（本轮补全）：`CoordinatorServer` 参考服务端
    （节点注册/心跳/**租约式分片认领**，租约到期自动释放，宕机节点分片可接管）+
    `HttpShardCoordinator` 客户端（周期心跳续租、协调器不可达时降级回落配置分片，
    绝不扩量扫描）；`python -m orchestrator.sharding --serve` 独立运行。

#### 存储与数据血缘
- **新增 `storage/schema.py`**：`SCHEMA_VERSION=2`、写入侧 `stamp()` 盖章
  （`schema_version` + `_lineage{source,engine,tool_version,collected_at,hops[]}`）、
  读取侧 `migrate()` v1→v2 升级、`TYPED_COLUMNS` 类型化列（port int32 /
  ts int64 / rtt_ms float64 / l7_probed bool），未知列 string 追加；
- `storage/compactor.py`：类型化 Parquet 写入（原子 tmp+replace）、已完成清单
  持久化（重启不重复压缩）、坏分片隔离上报。

#### 富化
- **pyasn 离线 BGP RIB 支持**（本轮补全）：`BgpResolver` 查询 origin ASN 与
  BGP 前缀（`bgp` 字段，与 GeoIP ASN 互补），可选依赖缺失时优雅降级；
  `scripts/setup_deps.py --pyasn` 一键安装并下载转换最新 RIB；
- GeoIP 的 `geoip_enabled` / `asn_enabled` 开关本轮真正接线
  （此前配置项存在但不生效）。

#### 配置与依赖
- `orchestrator/config.py` 重写：`${ENV:默认}` 插值、`NETATLAS_<段>__<键>`
  环境变量覆盖（自动类型推断）、`validate()` 启动校验（配额总和、L2 配对、
  邮箱占位、目录可写）；
- 新增 `config/config.example.yaml` 配置模板；
- 新增 `requirements.lock` 精确版本锁定；`requirements.txt` 加上下界约束；
- **新增 `scripts/setup_deps.py`**：一键检测/安装外部依赖
  （zgrab2 编译 / GeoIP 下载 / pyasn RIB / Npcap / masscan / 网卡探测）；
- **新增 `Dockerfile` / `docker-compose.yml` / `.gitignore` / `.dockerignore`**：
  python:3.13-slim + libpcap + masscan，host 网络 + NET_RAW，
  机器相关配置全部经 `NETATLAS_*` 环境变量注入；
- **新增 `scripts/eval_ipv6_hitlist.py`**（本轮补全）：IPv6 TGA 真实 hitlist
  离线评估（训练/留出划分，exact 命中率 + /48 重叠率 + 随机基线对比，
  支持 .gz 下载）；
- **新增 `docs/MASSCAN_CONTRACT.md`**（本轮补全）：masscan 行为契约兼容性
  检查清单（命令行/输出格式/中断续扫/分片调速/数据完整性 5 节 17 条）。

#### WebUI
- 前端全量重写：深色现代仪表盘，8 项 KPI 指标带、网卡吞吐面积图 + 速率历史、
  扫描控制（暂停/续扫/速率下发，含状态机徽标）、队列背压进度条、模块启停、
  信息流过滤（关键词/分类/自动滚动）、错误事件面板、只读 SQL 查询；
- 后端新增：`/api/scan/progress`、`/api/scan/rate`（100–1,000,000 校验）、
  `/api/scan/pause`、`/api/scan/resume`、`/api/errors`；
  WS 帧扩展 `scan`/`node`/`errors_recent`。

### 变更（Changed）

- 全部 `except Exception: pass` 消除，改为：上报 ErrorReporter + 计数 +
  具体异常类型捕获（涉及 8 个模块文件）；
- `config/config.yaml` 去除全部机器相关硬编码：`paths.root` 置空（自动取仓库根）、
  `source_ip`/`router_mac` 置空（自动探测/环境变量注入）；
- 带宽默认配额修正为 10+6+2+2=20 Mbps（此前 12+8+2+3=25 超过全局令牌桶
  25×80%=20，启动校验必告警）；
- `modules/enrich.py` 重写为统一并发抽象（AsyncConsumer）；
- `orchestrator/main.py` 装配全部新基础设施（持久化/状态机/调速/分片/错误总线）；
- `scripts/start.py` 接入配置校验与 setup_deps 指引；
  `scripts/start_all.bat` 去除硬编码用户路径；
- `modules/l4_scapy.py`：接入扫描状态机（本轮补全：热调速落盘、开放端口计数、
  收尾最终进度 + 状态迁移）；`L4Scanner.pause()` 现在会真正停发 scapy 后端；
- `requirements.lock` 补上 httpx 等测试依赖（此前缺失导致新环境
  `fastapi.testclient` 导入失败）。

### 修复（Fixed）——由新测试发现的真实 Bug

| Bug | 现象 | 修复 |
|---|---|---|
| HTTP 正文丢失 | `reader.read(n)` 在首个 TCP 段到达即返回，headers/body 分段时正文为空、title 恒为 None | `l7_grabber._read_to_eof()` 循环读到 EOF/限量，整体超时返回已收部分 |
| ScanStateMachine/StateStore 不接受 str 路径 | `AttributeError: 'str' object has no attribute 'exists'` | 构造时统一 `Path(path)` |
| 进程退出竞态丢数据 | 子进程退出时读取线程缓冲的尾部记录被丢弃，开放端口记录全部丢失 | `_consume` 退出分支改为等待 sentinel 排空管道 |
| zstd 缓冲掩盖写失败 | 底层句柄损坏时 `write()` 不抛错（数据在 zstd 缓冲中） | flush 失败上报 ErrorReporter |
| masscan 段终止误判崩溃 | 强杀退出码非 0 → 触发退避重启链 | `_terminated_by_us` 标记区分主动终止（Windows 无控制台 CTRL_BREAK 无法投递的规避） |
| DuckDB 视图注册静默失败 | `except: pass` 导致表不可查且无提示 | 改为告警 + 表名合法性校验 |
| idna 编码异常 | `part.encode("idna","replace")` → UnicodeError | DNS 名称编码改为容错实现 |
| HTTP 协调器分片认领串任务（本轮） | 认领表未按 `shard_total` 命名空间隔离，不同分片规模的任务互相挤占序号 | 认领键改为 `{shard_total}:{index}` 复合键（由 `test_http_coordinator` 发现） |

---

## [1.0.0] — 初始版本

- IPv4 两阶段无状态扫描（masscan 封装）、IPv6 TGA（6Tree/EntropyIP）；
- L7 抓取（ZGrab2 + Python 兜底）、GeoIP/rDNS 富化、IAB 分类；
- 三层存储（NDJSON.zst → Parquet → DuckDB/SQLite）；
- WebUI 基础版（吞吐曲线、模块启停、带宽滑块、SQL 查询）；
- 13 项单元测试。

[2.0.0]: https://github.com/SourcePointRox/Internet-scanning/compare/v1.0.0...main
