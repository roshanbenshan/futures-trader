#!/usr/bin/env python3
"""盘中监测推送脚本 — 从Redis读events:monitor推送到Telegram
搭配cron job每1分钟运行，no_agent=True模式直接输出msg到Telegram。
空输出=无事件=静默，不浪费消息。
"""
import redis, json, time, os, math

r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
try:
    r.ping()
except:
    exit(0)  # Redis不可用，静默

now = time.time()
last_ts_key = 'monitor:last_push_ts'
last_summary_key = 'monitor:last_summary_ts'

# 上次推送时间戳
last_ts = 0
raw_ts = r.get(last_ts_key)
if raw_ts:
    last_ts = float(raw_ts)

last_summary = 0
raw_sm = r.get(last_summary_key)
if raw_sm:
    last_summary = float(raw_sm)

# ── 读新事件（从Redis list读取全部，按ts过滤） ──
events = []
raw_events = r.lrange('events:monitor', 0, -1)
for raw in raw_events:
    try:
        ev = json.loads(raw)
        if ev['ts'] > last_ts:
            events.append(ev)
    except:
        pass

# 按时间排序（旧→新）
events.sort(key=lambda x: x.get('ts', 0))

lines = []

# ── 格式化事件 ──
for ev in events:
    t = ev.get('t', '')
    d = ev.get('d', {})
    sym = d.get('sym', '').upper()
    side = d.get('side', '')
    pnl = d.get('pnl', 0)
    reason = d.get('reason', '')
    level = d.get('level', '')
    msg = d.get('msg', '')

    if t == 'open':
        entry = d.get('entry', 0)
        qty = d.get('qty', 0)
        sl = d.get('sl', 0)
        tp = d.get('tp', 0)
        sl_loss = d.get('sl_loss', 0)
        tp_profit = d.get('tp_profit', 0)
        reason_text = d.get('reason', '')
        icon = '🔴' if side == 'SHORT' else '🟢'
        lines.append(f"{icon} {side} {sym} {qty} @ ${entry}")
        lines.append(f"  SL=${sl}(-${sl_loss}) TP=${tp}(+${tp_profit})")
        if reason_text:
            lines.append(f"  {reason_text}")
    elif t == 'close':
        pnl_str = f"${pnl:+.2f}" if isinstance(pnl, (int, float)) else f"${pnl}"
        reason_label = {
            'TP': '+6%止盈', 'SL': '-6%止损', 'TRAIL': '追踪触发',
            'TIME_STOP': '时间止损', '1H_REVERSAL': '1h逆势平仓',
            'WATERFALL': '瀑布保护', 'SWEEP': '15m横扫', 'CONS_BULL': '连续阳线'
        }.get(reason, reason)
        lines.append(f"✅ 平仓 {sym} {reason_label} 盈亏: {pnl_str}")
    elif t == 'alert':
        if level == 'WARN_EXIT':
            lines.append(f"🚨 {msg} {sym} PnL=${pnl}")
        elif level == 'WARN_REDUCE':
            lines.append(f"⚠️ 减仓 {sym} 平一半 PnL=${pnl}")
        elif level == 'INFO':
            lines.append(f"🔒 {sym} {msg} PnL=${pnl}")
        elif level == 'CRITICAL':
            lines.append(f"🚨 断路器! {d.get('msg','')}")
        else:
            lines.append(f"📌 {sym} {msg}")

# ── 持仓快报（每30分钟） ──
if now - last_summary > 1800:
    pos_keys = r.keys('pos:*')
    bal_raw = r.get('bal')
    balance = ''
    if bal_raw:
        try:
            bd = json.loads(bal_raw)
            balance = f"${bd.get('bal', '?')}"
        except:
            balance = '$?'
    
    pos_list = []
    for k in sorted(pos_keys or []):
        sym = k.replace('pos:', '').upper()
        val = r.get(k)
        if not val:
            continue
        try:
            p = json.loads(val)
            amt = float(p.get('positionAmt', 0))
            if abs(amt) <= 0:
                continue
            entry = float(p.get('entryPrice', 0))
            upnl = float(p.get('unRealizedProfit', 0))
            side = '做空' if amt < 0 else '做多'
            margin_used = entry * abs(amt) / 5  # 5x leverage
            pnl_pct = upnl / margin_used * 100 if margin_used > 0 else 0
            pos_list.append(f"  {sym} {side} {abs(amt):.2f} @ ${entry:.4f} ({pnl_pct:+.1f}%)")
        except:
            pos_list.append(f"  {sym} 数据异常")

    if pos_list:
        pos_count = len(pos_list)
        lines.append(f"\n📊 持仓快报 ({pos_count}仓) | {balance}")
        lines.extend(pos_list)
        lines.append("---")
    else:
        lines.append(f"\n📭 空仓 | {balance}")
        lines.append("---")
    
    r.set(last_summary_key, str(now))

# 更新推送时间戳
r.set(last_ts_key, str(now))

# 清空已处理的事件队列（防堆积）
r.delete('events:monitor')

# 输出（空=静默）
if lines:
    print('\n'.join(lines))
