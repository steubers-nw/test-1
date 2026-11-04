@echo off
chcp 65001 >nul
cd /d "%~dp0"
title vetta_proxy

echo 启动 vetta_proxy ...
echo.

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" vetta_proxy.py %*
) else (
    python vetta_proxy.py %*
)

echo.
echo [vetta_proxy] 已退出
pause
