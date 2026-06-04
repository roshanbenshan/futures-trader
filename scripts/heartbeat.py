#!/usr/bin/env python3
"""服务心跳检测 — 检查核心服务存活状态，异常时推送Telegram告警"""
import subprocess, json, time, os, sys

# 检查systemd scope状态
def check_scope(name):
    try:
        r = subprocess.run(
            ['systemctl', '--user', 'show', '--property=ActiveState', name],
            capture_output=True, text=True, timeout=5
        )
        if r.returncode == 0 and 'ActiveState=active' in r.stdout:
            return True, None
        return False, r.stdout.strip() or 'scope不存在'
    except Exception as e:
        return False, str(e)

# 检查Redis
def check_redis():
    try:
        import redis as redis_mod
        r = redis_mod.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        if r.ping():
            return True, int(r.dbsize())
        return False, 'PING失败'
    except Exception as e:
        return False, str(e)

alerts = []

# 1. data_collector
ok, msg = check_scope('binance-data-collector.scope')
if not ok:
    alerts.append(f"🔴 data_collector 宕机: {msg}")
else:
    # 更新心跳时间戳
    try:
        import redis as redis_mod
        r = redis_mod.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        r.set('hb:data_collector', str(time.time()))
    except:
        pass

# 2. futures_trader
ok, msg = check_scope('binance-futures-trader.scope')
if not ok:
    alerts.append(f"🔴 futures_trader 宕机: {msg}")
else:
    try:
        import redis as redis_mod
        r = redis_mod.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        r.set('hb:futures_trader', str(time.time()))
    except:
        pass

# 3. Redis
ok, msg = check_redis()
if not ok:
    alerts.append(f"🔴 Redis 宕机: {msg}")
else:
    try:
        import redis as redis_mod
        r = redis_mod.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        r.set('hb:redis', str(time.time()))
    except:
        pass

# 4. 检查上一条心跳是否过期（检测cron本身是否活着）
try:
    import redis as redis_mod
    r = redis_mod.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    last_hb = r.get('hb:heartbeat_script')
    now = time.time()
    if last_hb:
        elapsed = now - float(last_hb)
        if elapsed > 180:  # 超过3分钟无心跳
            alerts.append(f"⚠️ 心跳脚本上次运行在{elapsed:.0f}秒前，cron可能异常")
    r.set('hb:heartbeat_script', str(now))
    # 清理过期心跳（>1小时的清理掉）
    for key in r.keys('hb:*'):
        val = r.get(key)
        if val and now - float(val) > 3600:
            r.delete(key)
except:
    pass

# 输出告警（空=无异常，静默）
if alerts:
    print(f"🚨 服务异常告警 ({time.strftime('%H:%M UTC')})")
    print(f"{'─'*36}")
    for a in alerts:
        print(f"  {a}")
    print(f"\n请检查: systemctl --user status <service>")
else:
    # 正常时仅写心跳，不输出任何内容（静默）
    pass
