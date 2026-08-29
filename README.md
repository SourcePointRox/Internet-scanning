# NetAtlas — 全球互联网扫描与资产测绘平台

面向**学术研究**的生产级互联网测量系统：IPv4 全网两阶段无状态扫描、IPv6 TGA 智能目标生成、
L7 协议握手抓取（banner / TLS 证书 / HTTP 指纹）、主机富化（BGP/ASN/GeoIP/RTT/DNS）、
网站多级分类（精确到最小子类）、列式压缩存储，以及带实时管控能力的 WebUI。

> ⚠️ 运行前请务必阅读 [docs/RESPONSIBLE_SCANNING.md](docs/RESPONSIBLE_SCANNING.md)。
> 扫描公网须遵守排除列表、速率约束与 opt-out 机制。架构设计与调研依据见 [docs/开发方案.MD](docs/开发方案.MD)。

---

## 功能总览

| 层 | 能力 |
|---|---|
| L3/L4 发现 | masscan（C）两阶段扫描：top 端口全网发现 → 存活主机全端口；运行时调速、断点续扫、强制排除列表 |
| IPv6 | 6Tree（DHC 空间树）+ Entropy/IP 目标生成，PAS 别名前缀检测，ε-greedy 探测预算反馈，hitlist 种子导入 |
| L7 采集 | ZGrab2（Go，33+ 协议）主引擎 + Python asyncio 兜底引擎（HTTP/TLS/banner/RTT），读限量保护 |
| 富化 | MaxMind GeoLite2 地理/ASN、pyasn BGP、正反向 DNS、TCP connect RTT |
| 分类 | IAB Content Taxonomy 3.0 主干 + 测绘扩展类目；规则信号评分；**重点识别文件存储/CDN/科研数据分发站点**（如 ESA Gaia CDN），保留完整分类层级路径并单独成库 |
| 存储 | 三层：zstd NDJSON 原始层（滚动分片）→ Parquet 列式层（zstd level 6，date/port 分区）→ DuckDB 查询目录 + SQLite 元数据 |
| WebUI | 实时吞吐曲线、模块级启停、全局上行带宽与配额滑块、队列背压监控、各存储层**实时本地路径**标注、只读 SQL 查询、WebSocket 推送；**启动前自动探测空闲端口** |
| 带宽 | 全局令牌桶（默认 25 Mbps × 80%）+ 每消费者子配额，WebUI 动态可调 |

## 技术要点

- **语言选型**：发包/抓包栈 = C（masscan 外部进程）；协议握手 = Go（ZGrab2）；调度/脚本/数据处理 = Python 3.13（asyncio + DuckDB/pyarrow，单机规模下替代 Scala/Spark，Parquet 天然兼容后续 Spark 迁移）；
- **流式管道**：`目标生成 → L4 SYN → 存活队列 → L7 握手 → NDJSON.zst → [富化+分类并行消费者] → Parquet`，全程有界队列背压，不堆内存；
- **IPv6 不可穷举**：种子驱动 TGA（6Tree/EntropyIP）+ 别名前缀过滤 + 命中率反馈；
- **存储空间效率**：借鉴 Censys CQRS/增量编码思想，本地采用 zstd 压缩 NDJSON + Parquet 字典/游程编码，DuckDB 进程内 OLAP 零服务开销。

## 部署要求

- Windows 10/11（存储与服务均按 Windows 路径开发）；
- Python 3.13+；依赖：`pip install -r requirements.txt`（含 `scapy`，用于 L4 原生扫描）；

### L4 发包后端（三选一，启动时自动探测）

| 后端 | 依赖 | 说明 |
|---|---|---|
| **masscan** | `bin/masscan.exe` + Npcap | 性能最佳。官方**无 Windows 预编译包**，需 MinGW + Npcap SDK 自行编译（源码已备于 `deps/`） |
| **scapy**（默认可用） | Npcap + `pip install scapy` | 经 Npcap 的 `L3pcapSocket` 直接发包，绕过 Windows「禁止 raw socket 发送 TCP」的限制 |
| **dry-run** | 无 | 模拟模式，用于流水线联调 |

> **Npcap 是 Windows 下发包/抓包的基础**（masscan 与 scapy 都依赖它）：
> 以管理员权限运行 `deps/npcap-1.88.exe` 完成驱动安装，
> 用 `python scripts/check_npcap.py` 验证，再重启服务即可开始真实扫描。

### L7 抓取引擎

- **ZGrab2（Go，默认）**：已从官方源码编译为 `bin/zgrab2.exe`，支持 HTTP/TLS/SSH/FTP/SMTP/Telnet/MySQL/Redis/MongoDB/Postgres/MSSQL/NTP，以及工控协议 Modbus/BACnet/DNP3/Fox/Siemens。
  重新编译：`go install github.com/zmap/zgrab2/cmd/zgrab2@v0.1.8`（**Go 1.22 可编译**；Go ≥1.24 会因上游 zcrypto 在 Windows 的兼容性问题失败）。
  系统按端口自动派生 zgrab2 常驻进程，并把握手结果归一化（含 TLS 证书链摘要：CN/颁发者/有效期/密钥算法/SAN）。
- **Python asyncio 引擎**：无 zgrab2 时自动降级，支持 HTTP/HTTPS/TLS/banner 与 RTT 测量。

### 其他可选依赖

- GeoLite2 mmdb 放入 `data/geoip/`（MaxMind 许可需自行注册下载），缺失时地理/ASN 富化优雅降级；
- 编辑 `config/config.yaml`：替换 `project.contact_email` 为你的真实邮箱（opt-out 通道）。

## 快速开始

```bash
# 模拟模式（不发包，验证整条流水线与 WebUI）
python scripts/start.py --dry-run

# 生产模式（需要 masscan + Npcap，建议先限定范围验证）
python scripts/start.py --targets 203.0.113.0/29

# Windows 一键启动
scripts\start_all.bat
```

启动后控制台会打印 WebUI 地址（自动选取 8000–9000 区间空闲端口），浏览器打开即可管控。

## 目录结构

```
Internet-scanning/
├── docs/               # 开发方案.MD（保留）/ 负责任扫描守则
├── config/             # 全局配置 / 扫描排除列表 / zgrab2 多协议配置
├── orchestrator/       # 编排器 / 带宽令牌桶 / 模块注册表
├── modules/            # l4_scanner / ipv6_tga / l7_grabber / enrich / classifier
├── classification/     # IAB 分类法 taxonomy.json + 规则库
├── storage/            # writer(NDJSON.zst) / compactor(Parquet) / catalog(DuckDB+SQLite)
├── webui/              # FastAPI 后端 + 原生前端单页
├── scripts/            # start.py / start_all.bat 一键启动
├── tests/              # 单元测试（13 项，python -m unittest discover -s tests）
├── bin/                # masscan.exe / zgrab2.exe（自行放置）
└── data/               # raw / parquet / meta / seeds / geoip（运行时生成）
```

## 合规与安全

- `config/exclude.txt` 强制加载：IANA 保留段、RFC1918、组播、opt-out 网段；
- 所有探测携带研究标识 UA 与联系邮箱；
- 上行默认限速 25 Mbps × 80%，WebUI 可下调，不得超过物理带宽；
- Web 正文抓取默认截断 64KB，防止磁盘膨胀。

## License

仅用于学术研究与授权网络测量。使用者须自行承担合规责任。
