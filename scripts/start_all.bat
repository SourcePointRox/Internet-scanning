@echo off
REM NetAtlas 一键启动（Windows）
REM 默认 dry-run 模拟模式；移除 --dry-run 进入生产扫描（需 bin\masscan.exe + Npcap）
cd /d "%~dp0\.."
set PYTHON=C:\Users\wjk13\.workbuddy\binaries\python\envs\netatlas\Scripts\python.exe
if not exist "%PYTHON%" set PYTHON=python
"%PYTHON%" scripts\start.py --dry-run %*
pause
