# NetAtlas — 全球互联网扫描与资产测绘平台

面向**学术研究**的生产级互联网测量系统：IPv4 全网两阶段无状态扫描、IPv6 TGA 智能目标生成、
L7 协议握手抓取（banner / TLS 证书 / HTTP 指纹）、主机富化（BGP/ASN/GeoIP/RTT/DNS）、
网站多级分类（精确到最小子类）、列式压缩存储，以及带实时管控能力的 WebUI。

> ⚠️ 运行前请务必阅读 [docs/RESPONSIBLE_SCANNING.md](docs/RESPONSIBLE_SCANNING.md)。
> 扫描公网须遵守排除列表、速率约束与 opt-out 机制。架构设计与调研依据见 [docs/开发方案.MD](docs/开发方案.MD)。
> 版本变更历史见 [CHANGELOG.md](CHANGELOG.md)。

---

## 功能总览

| 层 | 能力 |
|---|---|
| L3/L4 发现 | masscan（C）两阶段扫描：top 端口全网发现 → 存活主机全端口；**分片式准实时调速、断点续扫状态机、进程看门狗**、强制排除列表；无 masscan 时自动切换内置 scapy/Npcap 引擎 |
| IPv6 | 6Tree（DHC 空间树）+ Entropy/IP 目标生成，PAS 别名前缀检测，ε-greedy 探测预算反馈，hitlist 种子导入（`scripts/eval_ipv6_hitlist.py` 可离线评估真实 hitlist） |
| L7 采集 | ZGrab2（Go，33+ 协议）主引擎 + Python asyncio 兜底引擎（HTTP/TLS/banner/RTT），读限量保护，瞬时错误自动重试 |
| 富化 | MaxMind GeoLite2 地理/ASN（独立开关）、**pyasn 离线 BGP RIB**（origin ASN + 前缀）、**纯 Python 异步 DNS 解析器**（绕 GIL，单事件循环数百在途查询）、TCP connect RTT |
| 分类 | IAB Content Taxonomy 3.0 主干 + 测绘扩展类目；规则信号评分；**重点识别文件存储/CDN/科研数据分发站点**，保留完整分类层级路径并单独成库 |
| 存储 | 三层：zstd NDJSON 原始层（滚动分片）→ Parquet 列式层（zstd level 6，date/port 分区，**schema 版本化 + 数据血缘盖章**）→ DuckDB 查询目录 + SQLite 元数据（含错误事件表） |
| WebUI | 深色高密度仪表盘：8 项 KPI、网卡吞吐面积图、速率历史、**扫描暂停/续扫/调速（含状态机徽标）**、队列背压、模块启停、信息流过滤、错误事件面板、只读 SQL 查询、WebSocket 推送；启动前自动探测空闲端口 |
| 可靠性 | **状态持久化**（REGISTRY 快照周期落盘 + 崩溃后 RUNNING→PAUSED 降级续扫）、**结构化错误总线**（环形缓冲 + SQLite 持久化 + 告警钩子）、AIMD 动态调速、磁盘水位保护 |
| 分布式 | 一致性哈希分片（/24、/48 无状态复算）+ **HTTP 协调器**（节点注册/心跳/租约式分片认领，参考实现零依赖） |
| 带宽 | 全局令牌桶（默认 25 Mbps × 80%）+ 每消费者子配额，WebUI 动态可调 |

## 技术要点

- **语言选型**：发包/抓包栈 = C（masscan 外部进程）；协议握手 = Go（ZGrab2）；调度/数据处理 = Python 3.13（asyncio + DuckDB/pyarrow，Parquet 天然兼容后续 Spark 迁移）；
- **流式管道**：`目标生成 → L4 SYN → 存活队列 → L7 握手 → NDJSON.zst → [富化+分类并行消费者] → Parquet`，全程有界队列背压，不堆内存；
- **统一并发抽象**（`orchestrator/concurrency.py`）：阻塞型 `ThreadPoolConsumer` / IO 密集 `AsyncConsumer` / 子进程看门狗 `SupervisedProcess`（崩溃退避重启，上限 5 次）；
- **动态调速**：masscan 不支持热调速 —— 按时间段分片，片间以新速率 `--resume` 重启；AIMD 控制器（和式增 +10% / 乘式减 ×0.5）按丢包率反馈；
- **存储空间效率**：zstd 压缩 NDJSON + Parquet 字典/游程编码，DuckDB 进程内 OLAP 零服务开销；
- **schema 演进**：写入侧 `stamp()` 盖章（`schema_version` + `_lineage`），读取侧 `migrate()` 自动升级 v1→v2，类型化列保留原生类型。

## 部署要求

- Windows 10/11（主开发平台）或 Linux（Docker）；
- Python 3.13+；依赖：`pip install -r requirements.txt`（精确复现用 `requirements.lock`）。

### 一键安装外部依赖

```bash
python scripts/setup_deps.py --all          # 全部检测 + 可自动化的自动安装
python scripts/setup_deps.py --geoip KEY    # MaxMind GeoLite2（需免费 License Key）
python scripts/setup_deps.py --zgrab2       # Go 可用时自动编译 zgrab2
python scripts/setup_deps.py --pyasn        # pyasn + 最新 BGP RIB 下载转换
python scripts/setup_deps.py --npcap        # Npcap 检测/引导（Windows 发包基础）
python scripts/setup_deps.py --detect-net   # 网卡 IP / 网关 MAC 探测（二层直连配置辅助）
```

### L4 发包后端（三选一，启动时自动探测）

| 后端 | 依赖 | 说明 |
|---|---|---|
| **masscan** | `bin/masscan.exe` + Npcap | 性能最佳。官方无 Windows 预编译包，需 MinGW + Npcap SDK 自行编译 |
| **scapy**（默认可用） | Npcap + `pip install scapy` | 经 Npcap 直接发包，绕过 Windows 禁止 raw socket 的限制；支持真正热调速 |
| **dry-run** | 无 | 模拟模式，用于流水线联调 |

> **Npcap 是 Windows 下发包/抓包的基础**（masscan 与 scapy 都依赖它）：
> 以管理员权限安装 Npcap（勾选 WinPcap 兼容模式），再启动服务即可真实扫描。

### L7 抓取引擎

- **ZGrab2（Go，默认）**：支持 HTTP/TLS/SSH/FTP/SMTP/Telnet/MySQL/Redis/MongoDB/Postgres/MSSQL/NTP 及工控协议 Modbus/BACnet/DNP3/Fox/Siemens。系统按端口自动派生 zgrab2 常驻进程，把握手结果归一化（含 TLS 证书链摘要：CN/颁发者/有效期/密钥算法/SAN）。重新编译：`go install github.com/zmap/zgrab2/cmd/zgrab2@v0.1.8`（**Go 1.22 可编译**；Go ≥1.24 在 Windows 有兼容问题）。
- **Python asyncio 引擎**：无 zgrab2 时自动降级，支持 HTTP/HTTPS/TLS/banner 与 RTT 测量。

## 快速开始

```bash
# 模拟模式（不发包，验证整条流水线与 WebUI）
python scripts/start.py --dry-run

# 生产模式（需要 masscan 或 scapy + Npcap，建议先限定范围验证）
python scripts/start.py --targets 203.0.114.0/29

# Windows 一键启动
scripts\start_all.bat

# Docker（网络 host 模式 + NET_RAW，机器相关配置全部经环境变量注入）
docker compose up --build
```

启动后控制台打印 WebUI 地址（自动选取 8000–9000 区间空闲端口）。

### 配置

- 所有配置均可被环境变量覆盖：`NETATLAS_<段>__<键>`（双下划线分层），如
  `NETATLAS_L4__SOURCE_IP=10.0.0.5`、`NETATLAS_BANDWIDTH__UPLOAD_MBPS=50`；
- YAML 内支持 `${ENV_VAR:默认值}` 插值；
- `config/config.yaml` 由 `config/config.example.yaml` 复制而来，**不含任何机器相关硬编码**
  （网卡 IP/网关 MAC/磁盘路径一律留空自动探测或环境变量注入）；
- 启动时自动校验：配额总和、L2 配对、邮箱占位、目录可写性；
- **必填**：把 `project.contact_email` 替换为真实邮箱（opt-out 通道），可用
  `NETATLAS_CONTACT_EMAIL` 环境变量注入。

## 测试

```bash
python -m unittest discover -s tests     # 147 项，约 60 秒（含基准测试）
```

| 测试文件 | 覆盖内容 |
|---|---|
| `test_config.py` | 环境变量插值/覆盖、配置校验、无硬编码校验 |
| `test_errors.py` | 重试装饰器、错误上报、告警钩子隔离 |
| `test_persistence.py` | 状态落盘原子性、计数恢复、崩溃后 RUNNING→PAUSED 降级 |
| `test_sharding.py` / `test_http_coordinator.py` | 分片互补性/一致性、AIMD 调速、**HTTP 协调器全协议**（注册/心跳/租约认领/降级） |
| `test_dns_async.py` | DNS 报文编解码（含压缩指针）、本地 UDP 桩端到端、64 并发吞吐 |
| `test_l4_integration.py` | 假 masscan 驱动：JSON 流→队列、崩溃重启+`--resume`、暂停/续扫、分片调速 |
| `test_l7_integration.py` | 本地 HTTP/banner 服务真实抓取、并发、zgrab2 归一化 |
| `test_storage_pipeline.py` | ShardWriter→Compactor→Catalog 全链路、类型化列、v1→v2 迁移、磁盘水位 |
| `test_webui_e2e.py` | 全部 REST 端点 + WebSocket 帧结构（真实 FastAPI app） |
| `test_recovery.py` | 崩溃恢复、磁盘满/恢复、网络中断、看门狗、背压不丢数据 |
| `test_ipv6_hitlist.py` | TGA 与合成 hitlist 对比（6Tree /48 重叠率 62% vs 随机基线 0.006%） |
| `test_smoke_pipeline.py` | **端到端冒烟**：真实装配 Orchestrator → 过流水线 → 校验落盘与血缘盖章 |
| `test_scapy_state.py` / `test_enrich_bgp.py` | scapy 进度持久化接线、pyasn BGP 富化、GeoIP 开关 |
| `test_benchmarks.py` | 吞吐/延迟/内存基准（写入 70k+ rec/s、令牌桶 800k ops/s） |

测试约定：临时文件一律 `tempfile.mkdtemp()`；目标地址避开排除列表中的
`203.0.113.x`/`198.51.100.x`（用 `203.0.114.x`）；masscan 行为契约见
[docs/MASSCAN_CONTRACT.md](docs/MASSCAN_CONTRACT.md)。

## 分布式部署（多机分片）

```bash
# 协调节点：启动协调器（参考实现，零依赖，可替换 etcd/Redis 后端）
python -m orchestrator.sharding --serve --port 8765

# 各扫描节点：config.yaml 或环境变量
#   NETATLAS_DISTRIBUTED__ENABLED=true
#   NETATLAS_DISTRIBUTED__COORDINATOR=http
#   NETATLAS_DISTRIBUTED__COORDINATOR_URL=http://<协调节点>:8765
#   NETATLAS_DISTRIBUTED__SHARD_TOTAL=4
```

节点注册后认领**带租约的分片**（默认 45s，心跳续租，节点宕机分片自动释放）；
目标过滤由无状态一致性哈希（/24、/48 块）在各节点独立复算，互不重叠、完整覆盖。

## 目录结构

```
Internet-scanning/
├── docs/               # 开发方案 / 负责任扫描守则 / MASSCAN_CONTRACT（行为契约）
├── config/             # config.yaml（本地实例）/ config.example.yaml（模板）/ 排除列表 / zgrab2 配置
├── orchestrator/       # main(装配) / config / state / persistence(状态机) / concurrency
│                       # sharding(分片+HTTP协调器) / ratecontrol(AIMD) / errors(错误总线)
│                       # bandwidth(令牌桶) / nicmon(网卡监控) / livefeed
├── modules/            # l4_scanner(masscan) / l4_scapy / ipv6_tga / l7_grabber
│                       # enrich(异步DNS+pyasn) / classifier / dns_async(纯Python异步DNS)
├── classification/     # IAB 分类法 taxonomy.json + 规则库
├── storage/            # writer(NDJSON.zst) / compactor(Parquet) / catalog(DuckDB+SQLite) / schema(版本+血缘)
├── webui/              # FastAPI 后端（17 REST + WS）+ 深色仪表盘前端
├── scripts/            # start.py / setup_deps.py / eval_ipv6_hitlist.py / start_all.bat
├── tests/              # 147 项测试（unittest）+ fake_masscan（测试替身）
├── Dockerfile / docker-compose.yml
└── data/               # raw / parquet / meta / seeds / geoip / pyasn（运行时生成，不入库）
```

## WebUI API 速览

| 端点 | 说明 |
|---|---|
| `GET /api/status` `/api/feed` `/api/errors` | 模块状态 / 实时信息流 / 错误事件（内存+SQLite） |
| `GET /api/scan/progress` | 扫描状态机 + 调速器 + 分片信息 |
| `POST /api/scan/rate` `/api/scan/pause` `/api/scan/resume` | 调速（100–1,000,000 pps）/ 暂停 / 续扫 |
| `GET/POST /api/bandwidth` | 全局带宽配额查询/调整 |
| `POST /api/modules/{name}/start|stop` `/api/pipeline/start|stop` | 模块级/整线启停 |
| `GET /api/classification/stats` `/api/query?sql=...` | 分类统计 / 只读 SQL（DuckDB） |
| `WS /ws` | 实时推送（status/feed/scan/node/errors_recent 帧） |

## 更新日志

完整逐条记录见 [CHANGELOG.md](CHANGELOG.md)。以下是主要版本变更摘要。

### [2.0.0] — 2026-08-30「基础设施重构与可靠性加固」

测试从 13 项扩充至 **147 项**，重构过程中由新测试发现并修复 9 个真实代码 Bug。

**新增**

- **测试体系（13 → 147 项）**：14 个测试文件，覆盖配置 / 错误处理 / 持久化 / 分片 /
  异步 DNS / L4-L7 集成 / 存储全链路 / WebUI 端到端 / 故障恢复 / 基准 / 端到端冒烟；
  `tests/fake_masscan.py` 测试替身（quick/hang/crash/resume-crash 四模式）；
- **可靠性与状态管理**：`orchestrator/errors.py`（重试装饰器 + `ErrorReporter` 错误总线，
  SQLite 持久化）、`persistence.py`（`StateStore` 快照落盘 + `ScanStateMachine` 断点续扫，
  崩溃后 RUNNING→PAUSED 降级）、`ratecontrol.py`（AIMD 动态调速）；L4 主循环改为分片式
  （片间以新速率 `--resume` 重启 masscan）+ 进程看门狗；
- **并发与 DNS**：`concurrency.py`（统一并发抽象 + 子进程看门狗）、
  `modules/dns_async.py`（纯 Python 异步 DNS，绕 GIL，RFC 1035 报文级实现）；
- **分布式**：`sharding.py` —— `HashedSharder` 一致性哈希分片（/24、/48 无状态复算）+
  **HTTP 协调器**（`CoordinatorServer` 租约式分片认领 + `HttpShardCoordinator` 客户端，
  心跳续租、宕机自动释放、不可达降级）；
- **存储与血缘**：`storage/schema.py`（Schema v2 + `_lineage` 血缘盖章 + v1→v2 迁移 +
  类型化列）；compactor 类型化 Parquet + 清单持久化；
- **富化**：pyasn 离线 BGP RIB（`bgp` 字段）；异步 DNS 引擎默认启用；
- **配置与依赖**：环境变量插值/覆盖/启动校验、`config.example.yaml`、
  `requirements.lock` 精确锁定、`scripts/setup_deps.py` 一键安装外部依赖、
  `scripts/eval_ipv6_hitlist.py` 离线评估、`Dockerfile`/`docker-compose.yml`；
- **WebUI**：深色高密度仪表盘全量重写 + 5 个新 API（progress/rate/pause/resume/errors）。

**变更**

- 消除全部 `except Exception: pass`（改为 ErrorReporter 上报 + 计数 + 具体异常捕获）；
- `config/config.yaml` 去除机器相关硬编码；带宽默认配额修正为 20 Mbps；
- `enrich.py` 改用统一并发抽象；`main.py` 装配全部新基础设施。

**修复（由新测试发现的真实 Bug）**

| Bug | 修复 |
|---|---|
| HTTP 正文丢失（read 早返回） | `_read_to_eof()` 循环读到 EOF/限量 |
| 状态机不接受 str 路径 | 构造时统一 `Path()` |
| 进程退出竞态丢记录 | 等待 sentinel 排空管道 |
| zstd 缓冲掩盖写失败 | flush 失败上报 |
| masscan 段终止误判崩溃 | `_terminated_by_us` 标记区分主动终止 |
| DuckDB 视图注册静默失败 | 告警 + 表名校验 |
| idna 编码异常 | DNS 名称容错编码 |
| HTTP 协调器分片认领串任务 | 认领键按 `{shard_total}:{index}` 命名空间隔离 |

### [1.0.0] — 初始版本

IPv4 两阶段无状态扫描、IPv6 TGA（6Tree/EntropyIP）、L7 抓取（ZGrab2 + Python 兜底）、
GeoIP/rDNS 富化、IAB 分类、三层存储（NDJSON.zst → Parquet → DuckDB/SQLite）、
WebUI 基础版、13 项单元测试。

---

## 合规与安全

- `config/exclude.txt` 强制加载：IANA 保留段、RFC1918、组播、opt-out 网段；
- 所有探测携带研究标识 UA 与联系邮箱；
- 上行默认限速 25 Mbps × 80%，WebUI 可下调，不得超过物理带宽；
- Web 正文抓取默认截断 64KB，防止磁盘膨胀；
- 磁盘剩余低于 `storage.disk_min_free_mb` 时写入器自动暂停并告警。

## License

仅用于学术研究与授权网络测量。使用者须自行承担合规责任。
