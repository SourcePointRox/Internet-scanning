# 负责任扫描守则（Responsible Scanning Policy）

本项目面向**学术研究**目的的互联网测绘。运行本系统即表示你同意以下约束：

## 必须遵守

1. **排除列表强制生效**：`config/exclude.txt` 包含 IANA 保留段、RFC1918 私有地址、组播/广播段、以及所有 opt-out 请求网段。任何扫描任务启动前必须通过排除列表校验，**不得绕过**。
2. **速率保守**：默认上行占用不超过用户物理带宽的 80%，默认配额 25 Mbps 以下。扫描不是 DDoS——对被扫网段的冲击必须小于其正常背景噪声。
3. **身份可识别**：HTTP 探测的 User-Agent、TLS ClientHello 元数据中携带项目标识与联系邮箱，使被扫方能够联系到你并提出 opt-out。
4. **opt-out 即时生效**：收到任何网络维护者的退出请求后，24 小时内将其网段加入 `config/exclude.txt` 并重启调度器。
5. **法律自查**：在你所在司法辖区确认互联网测量的合法性；如需，提前与 ISP / CERT 协调。
6. **数据最小化**：只抓取协议握手与公开 banner，Web 正文默认截断 64KB；不尝试任何认证、漏洞利用或凭据测试。

## 被扫方如何退出

如果你是网络维护者，在日志中发现来自本项目的探测并希望退出，请联系配置文件中
`project.contact_email` 所列邮箱，提供你的网段（CIDR），我们将立即加入排除列表。

## 参考

- ZMap Project Best Practices: https://zmap.io/documentation
- Rapid7 Sonar 扫描实践
- Censys 扫描政策
