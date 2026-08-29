@echo off
REM NetAtlas 一键启动（Windows）
REM 默认 dry-run 模拟模式；移除 --dry-run 进入生产扫描（需 masscan 或 Npcap+scapy）
cd /d "%~dp0\.."
REM 优先使用项目虚拟环境 .venv，其次系统 PATH 中的 python
set PYTHON=.venv\Scripts\python.exe
if not exist "%PYTHON%" set PYTHON=python
"%PYTHON%" scripts\start.py --dry-run %*
pause
