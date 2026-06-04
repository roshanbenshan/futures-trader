#!/usr/bin/env python3
"""双fork完全脱离父进程启动daemon，带锁检查"""
import os, sys

LOCK_FILE = '/tmp/futures_trader.lock'
PID_FILE = '/tmp/futures_trader.pid'
LOG_FILE = '/tmp/futures_trader_daemon.log'
SCRIPT_DIR = '/home/ubuntu/.hermes/profiles/fengshengshuiqi/scripts'
SCRIPT = os.path.join(SCRIPT_DIR, 'futures_trader.py')

# 检查锁
if os.path.exists(LOCK_FILE):
    with open(LOCK_FILE) as f:
        old_pid = int(f.read().strip())
    try:
        os.kill(old_pid, 0)
        print(f"❌ daemon已在运行(PID={old_pid})")
        sys.exit(0)
    except OSError:
        pass

def daemonize():
    pid = os.fork()
    if pid > 0:
        os._exit(0)  # 父进程退出
    os.setsid()       # 新session
    pid = os.fork()
    if pid > 0:
        os._exit(0)  # 子进程退出
    # 孙进程: 完全独立，脱离agent生命周期
    os.chdir(SCRIPT_DIR)
    # 重定向stdio到日志
    with open(LOG_FILE, 'a') as f:
        os.dup2(f.fileno(), sys.stdout.fileno())
        os.dup2(f.fileno(), sys.stderr.fileno())
    # 写PID
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    os.execvp('python3', ['python3', '-u', SCRIPT])

if __name__ == '__main__':
    daemonize()
