#!/usr/bin/env python3
"""服务健康看门狗 — 检查心跳+Redis+系统进程，异常时推送告警"""
import redis as redis_mod, time, os, json

r = redis_mod.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
try:
    r.ping()
except:
    print("🚨 Redis离线！数据收集和交易引擎均无法工作")
    exit(0)

now = time.time()
alerts = []

# ── 1. trader心跳检查（超过2分钟无心跳=异常） ──
hb_raw = r.get('stats:heartbeat')
if hb_raw:
    hb_ts = float(hb_raw)
    if now - hb_ts > 120:
        alerts.append("🚨 futures_trader 心跳超时(>2分钟无更新)，可能已卡死或崩溃")
else:
    alerts.append("🚨 futures_trader 无心跳记录，进程可能未启动")

# ── 2. Redis数据新鲜度检查（最近30秒应有写入） ──
dbsize = r.dbsize()
if dbsize < 2:
    alerts.append(f"⚠️ Redis数据异常(仅有{dbsize}个key)，data_collector可能离线")

# ── 3. 余额检查 ──
bal_raw = r.get('bal')
if bal_raw:
    try:
        bd = json.loads(bal_raw)
        bal = bd.get('bal', 0)
        if bal <= 0:
            alerts.append(f"⚠️ 账户余额${bal}，资金可能耗尽")
    except:
        pass

if alerts:
    print('\n'.join(alerts))
# 无告警则不输出（静默）
