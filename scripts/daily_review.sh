#!/usr/bin/env bash
set -e
cd /home/ubuntu/.hermes/profiles/fengshengshuiqi/scripts

python3 << 'PYEOF'
import json, os
from datetime import datetime, timedelta, timezone

DB = '/tmp/futures_trade_db.json'
if not os.path.exists(DB):
    print("📊 昨日无交易记录")
    exit(0)

with open(DB) as f:
    db = json.load(f)

trades = db.get('trades', [])
if not trades:
    print("📊 昨日无交易记录")
    exit(0)

# 北京时间日期
bjt = timezone(timedelta(hours=8))
now_bjt = datetime.now(bjt)
today_start = now_bjt.replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_start = today_start - timedelta(days=1)

# 筛选昨日交易
yesterday_trades = []
for t in trades:
    # 兼容两种时间字段名: exit_time(新) / ts(旧)
    ct = t.get('exit_time') or t.get('ts') or t.get('entry_time', '')
    if isinstance(ct, str):
        try:
            ct_dt = datetime.fromisoformat(ct)
            # 无时区标记视为UTC
            if ct_dt.tzinfo is None:
                ct_dt = ct_dt.replace(tzinfo=timezone.utc)
            ct_bjt = ct_dt.astimezone(bjt)
        except:
            continue
    elif isinstance(ct, (int, float)):
        ct_bjt = datetime.fromtimestamp(ct, tz=bjt)
    else:
        continue
    
    if yesterday_start <= ct_bjt < today_start:
        yesterday_trades.append(t)

if not yesterday_trades:
    print("📊 昨日无交易记录")
    exit(0)

# 统计
total = len(yesterday_trades)
wins = sum(1 for t in yesterday_trades if t.get('pnl', 0) > 0)
losses = sum(1 for t in yesterday_trades if t.get('pnl', 0) < 0)
total_pnl = sum(t.get('pnl', 0) for t in yesterday_trades)
win_rate = wins / total * 100 if total > 0 else 0

# 按币种汇总
coin_stats = {}
for t in yesterday_trades:
    sym = t.get('sym', '?')
    pnl = t.get('pnl', 0)
    if sym not in coin_stats:
        coin_stats[sym] = {'trades': 0, 'wins': 0, 'pnl': 0.0}
    coin_stats[sym]['trades'] += 1
    coin_stats[sym]['pnl'] += pnl
    if pnl > 0:
        coin_stats[sym]['wins'] += 1

# 找出最大盈亏
best = max(yesterday_trades, key=lambda t: t.get('pnl', 0))
worst = min(yesterday_trades, key=lambda t: t.get('pnl', 0))

# 按退出原因统计
reason_stats = {}
for t in yesterday_trades:
    r = t.get('exit_reason', '未知')
    pnl = t.get('pnl', 0)
    if r not in reason_stats:
        reason_stats[r] = {'count': 0, 'pnl': 0.0, 'wins': 0}
    reason_stats[r]['count'] += 1
    reason_stats[r]['pnl'] += pnl
    if pnl > 0:
        reason_stats[r]['wins'] += 1

print(f"📊 每日复盘 — {yesterday_start.strftime('%Y-%m-%d')}（北京时间）")
print(f"{'='*40}")
print(f"  交易次数: {total}")
print(f"  胜    率: {win_rate:.0f}% ({wins}胜/{losses}负)")
print(f"  总  盈  亏: {'$' if total_pnl>=0 else '-$'}{abs(total_pnl):.2f}")
print()

# 各币种表现
print(f"📈 币种表现:")
for sym, st in sorted(coin_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
    emoji = '🟢' if st['pnl'] > 0 else ('🔴' if st['pnl'] < 0 else '⚪')
    wr = st['wins']/st['trades']*100
    print(f"  {emoji} {sym:<8s}  {st['trades']}笔 {wr:.0f}%胜率  ${st['pnl']:+.2f}")
print()

# 最佳/最差单笔
print(f"🏆 最佳单笔: {best.get('sym','?')} {best.get('side','?')} ${best.get('pnl',0):+.2f} ({best.get('pnl_pct',0):+.1f}%) | {best.get('exit_reason','')} | 持有{best.get('hold_min',0):.0f}min")
print(f"💀 最差单笔: {worst.get('sym','?')} {worst.get('side','?')} ${worst.get('pnl',0):+.2f} ({worst.get('pnl_pct',0):+.1f}%) | {worst.get('exit_reason','')} | 持有{worst.get('hold_min',0):.0f}min")
print()

# 退出原因分析
print(f"🔍 退出原因分析:")
for reason, st in sorted(reason_stats.items(), key=lambda x: x[1]['pnl']):
    wr = st['wins']/st['count']*100 if st['count']>0 else 0
    print(f"  {reason:<15s} {st['count']}笔 {wr:.0f}%胜率  ${st['pnl']:+.2f}")
print()

# 总结与建议
max_loss_coin = min(coin_stats.items(), key=lambda x: x[1]['pnl'])
max_win_coin = max(coin_stats.items(), key=lambda x: x[1]['pnl'])

lessons = []
if losses > wins:
    lessons.append(f"⚠️ 胜率仅{win_rate:.0f}%，需检查入场条件是否过松")
if total_pnl < 0:
    lessons.append(f"💰 总亏损${abs(total_pnl):.2f}，建议降低仓位或提高RR门槛")
if max_loss_coin[1]['pnl'] < -1:
    lessons.append(f"🎯 {max_loss_coin[0]}亏最多(${max_loss_coin[1]['pnl']:.2f})，考虑减少该币种交易")
if worst.get('pnl_pct', 0) < -5:
    lessons.append(f"🛡️ 最差单笔亏损{worst.get('pnl_pct',0):.1f}%，止损需收紧")

# 找出亏损最多的退出原因
worst_reason = min(reason_stats.items(), key=lambda x: x[1]['pnl'])
if worst_reason[1]['pnl'] < 0:
    lessons.append(f"🔧 {worst_reason[0]}造成最大亏损(${worst_reason[1]['pnl']:.2f})，需复盘该退出逻辑")

if lessons:
    print(f"💡 改进建议:")
    for l in lessons:
        print(f"  {l}")

PYEOF
