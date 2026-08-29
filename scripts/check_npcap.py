"""Npcap 安装状态检查脚本（避免命令行转义问题）。"""
import os

PF = "C:" + os.sep + "Program Files" + os.sep + "Npcap"
SYS_NPCAP = os.path.join(os.environ.get("SystemRoot", "C:\\Windows"), "System32", "Npcap")
SYS32 = os.path.join(os.environ.get("SystemRoot", "C:\\Windows"), "System32")

print("== Npcap 安装状态 ==")
print("Program Files\\Npcap 存在:", os.path.isdir(PF))
if os.path.isdir(PF):
    print("  内容:", sorted(os.listdir(PF))[:12])
print("System32\\Npcap 存在:", os.path.isdir(SYS_NPCAP))
if os.path.isdir(SYS_NPCAP):
    print("  内容:", sorted(os.listdir(SYS_NPCAP))[:12])

candidates = [
    os.path.join(SYS32, "wpcap.dll"),
    os.path.join(SYS32, "Packet.dll"),
    os.path.join(SYS_NPCAP, "wpcap.dll"),
    os.path.join(PF, "wpcap.dll"),
]
print("== 关键 DLL ==")
for p in candidates:
    print(f"  {p}: {os.path.exists(p)}")

print("== Scapy 后端 ==")
try:
    from scapy.config import conf
    print("  L2socket:", conf.L2socket)
    print("  L3socket:", conf.L3socket)
    from scapy.all import get_if_list
    print("  可见接口数:", len(get_if_list()))
except Exception as e:  # noqa: BLE001
    print("  scapy 检查失败:", e)
