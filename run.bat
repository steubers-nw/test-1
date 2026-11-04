@echo off
REM ============================================================
REM  vybe_proxy launcher (Windows)
REM ============================================================
chcp 65001 >nul
setlocal enabledelayedexpansion

REM 切到脚本所在目录（支持从任意路径调用）
cd /d "%~dp0"

REM ---------- 检查 config.ini ----------
if not exist "config.ini" (
    echo [ERROR] config.ini not found in %CD%
    echo         copy config.ini.example to config.ini and fill in your tokens.
    exit /b 1
)

REM ---------- 检查 Python ----------
where python >nul 2>nul
if errorlevel 1 (
    where py >nul 2>nul
    if errorlevel 1 (
        echo [ERROR] python not found in PATH
        echo         install Python 3.9+ from https://www.python.org/
        exit /b 1
    )
    set "PY=py -3"
) else (
    set "PY=python"
)

REM ---------- 语法自检（尽早发现错误） ----------
%PY% -m py_compile vybe_proxy.py
if errorlevel 1 (
    echo [ERROR] vybe_proxy.py has syntax errors, see above.
    exit /b 1
)

REM ---------- 启动 ----------
echo [INFO] starting vybe-proxy ...
%PY% vybe_proxy.py

REM 非 0 退出时保留窗口便于查看错误
if errorlevel 1 (
    echo.
    echo [ERROR] vybe_proxy exited with code %errorlevel%
    pause
)
endlocal
