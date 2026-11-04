#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f "config.ini" ]; then
  echo "config.ini not found in $(pwd)" >&2
  exit 1
fi

# 检查必需的 Python
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found" >&2
  exit 1
fi

# 可选：语法检查，尽早发现错误
python3 -m py_compile vybe_proxy.py

exec python3 vybe_proxy.py
