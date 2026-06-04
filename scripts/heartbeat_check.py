#!/usr/bin/env python3
"""心跳检测：检查三服务存活，仅异常时输出"""
import subprocess, sys

services = {
    'data_collector': ('data_collector', '数据收集器'),
    'futures_trader': ('futures_trader', '合约交易引擎'),
}

alive = True
for key, (pname, label) in services.items():
    r = subprocess.run(
        ['pgrep', '-f', pname],
        capture_output=True, timeout=5
    )
    if r.returncode != 0:
        print(f"🚨 服务异常: {label} ({pname}) 不在运行")
        alive = False

# Redis check
r = subprocess.run(
    ['redis-cli', 'ping'],
    capture_output=True, timeout=5
)
if 'PONG' not in (r.stdout or b'').decode():
    print(f"🚨 服务异常: Redis 不在运行或无响应")
    alive = False

# 全部正常 → 静默
if alive:
    sys.exit(0)
