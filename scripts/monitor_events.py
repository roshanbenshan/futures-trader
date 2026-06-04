#!/usr/bin/env python3
"""盘中监测：从Redis读取事件并推送Telegram (cron每分钟执行)"""

import json, os, sys, time

try:
    import redis as redis_mod
except:
    print("⚠️ redis模块未安装", file=sys.stderr)
    sys.exit(0)

REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

def redis_get(key):
    try:
        r = redis_mod.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        return r.get(key)
    except:
        return None

def redis_set(key, val, expire=86400):
    try:
        r = redis_mod.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.set(key, val, ex=expire)
    except:
        pass

def get_events():
    """从Redis读取最新事件"""
    try:
        r = redis_mod.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        events = r.lrange('events:monitor', 0, -1)
        return [json.loads(e) for e in events] if events else []
    except:
        return []

def format_open(e):
    d = e['d']
    side_cn = '做多' if d['side'] == 'LONG' else '做空'
    return (
        f"🟢 **开仓 {d['sym']} {side_cn}**\n"
        f"入场: ${d['entry']} x {d['qty']}\n"
        f"止损: ${d['sl']} (亏${d['sl_loss']})\n"
        f"止盈: ${d['tp']} (赚${d['tp_profit']})\n"
        f"策略: {d['reason']}"
    )

def format_close(e):
    d = e['d']
    icon = '✅' if d.get('pnl', 0) >= 0 else '❌'
    lines = [f"{icon} **平仓 {d.get('sym','?')}**"]
    pnl = d.get('pnl', 0)
    pnl_pct = d.get('pnl_pct')
    if pnl_pct is not None:
        lines.append(f"盈亏: ${pnl:+.2f} ({pnl_pct:+.1f}%)")
    else:
        lines.append(f"盈亏: ${pnl:+.2f}")
    if 'entry' in d and 'exit' in d:
        lines.append(f"入场: ${d['entry']} → 离场: ${d['exit']}")
    if 'hold_min' in d:
        lines.append(f"持仓: {d['hold_min']}分钟")
    if 'reason' in d:
        lines.append(f"原因: {d['reason']}")
    return '\n'.join(lines)

def format_alert(e):
    d = e['d']
    level = d.get('level', 'INFO')
    icon = '🔴' if level == 'CRITICAL' else ('🚨' if level == 'WARN_EXIT' else '⚠️')
    sym_str = f" {d['sym']}" if 'sym' in d else ""
    pnl_str = f" | PnL: ${d['pnl']:+.2f}" if 'pnl' in d else ""
    return f"{icon} **预警{sym_str}**{pnl_str}\n{d['msg']}"

def format_heartbeat(e):
    return None  # 不推送心跳

FORMATTERS = {
    'open': format_open,
    'close': format_close,
    'alert': format_alert,
    'heartbeat': format_heartbeat,
}

def main():
    # 获取最后处理的事件时间戳
    last_ts_key = 'monitor:last_event_ts'
    last_ts = float(redis_get(last_ts_key) or 0)
    
    events = get_events()
    if not events:
        # 无事件，可选输出保活信息
        sys.exit(0)
    
    new_events = []
    for ev in events:
        ts = ev.get('ts', 0)
        if ts > last_ts:
            new_events.append(ev)
    
    if not new_events:
        sys.exit(0)
    
    # 更新最后处理的时间戳
    max_ts = max(ev.get('ts', 0) for ev in new_events)
    redis_set(last_ts_key, str(max_ts))
    
    # 反向输出（Redis lpush是倒序）
    new_events.reverse()
    
    outputs = []
    for ev in new_events:
        fmt = FORMATTERS.get(ev.get('t', ''))
        if fmt:
            msg = fmt(ev)
            if msg:
                outputs.append(msg)
    
    if outputs:
        print('\n\n'.join(outputs))

if __name__ == '__main__':
    main()
