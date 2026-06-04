#!/usr/bin/env python3
"""盘后复盘脚本 — 每日UTC+0自动跑，输出完整复盘报告到Telegram"""
import json, os, sys
from datetime import datetime, timedelta, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRADE_LOG = os.path.join(SCRIPT_DIR, 'futures_trades.json')

# ── 北京时间日期计算 ──
bjt = timezone(timedelta(hours=8))
now_bjt = datetime.now(bjt)
today_start = now_bjt.replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_start = today_start - timedelta(days=1)

def read_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except:
        return default

# ── 读取Redis统计 + 余额快照 ──
p1_intercepts = 0
circuit_breakers = 0
bal_by_date = {}  # {yyyymmdd: bal}
bal_ordered = []  # [(date, bal), ...]
r = None
try:
    import redis as redis_mod
    r = redis_mod.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    if r.ping():
        p1_intercepts = int(r.get('stats:p1_intercepts') or 0)
        circuit_breakers = int(r.get('stats:circuit_breaker') or 0)
        for k in sorted(r.keys('bal_snapshot:*') or []):
            date_str = k.replace('bal_snapshot:', '')
            raw = r.get(k)
            if raw:
                d = json.loads(raw)
                bal_by_date[date_str] = float(d['bal'])
                bal_ordered.append((date_str, float(d['bal'])))
except:
    pass

# ── 读取交易记录 ──
trades = read_json(TRADE_LOG, [])
if not trades:
    print("📭 今日无交易记录")
    sys.exit(0)

# ── 筛选昨日交易（北京时间） ──
yesterday_trades = []
for t in trades:
    ct = t.get('ts', '')
    if isinstance(ct, str):
        try:
            ct_dt = datetime.fromisoformat(ct)
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
    print(f"📅 {yesterday_start.strftime('%Y-%m-%d')} 无交易记录")
    sys.exit(0)

# ════════════════════════════════════════
#  核心统计
# ════════════════════════════════════════

total = len(yesterday_trades)
wins = [t for t in yesterday_trades if t.get('pnl', 0) > 0]
losses = [t for t in yesterday_trades if t.get('pnl', 0) < 0]
win_count = len(wins)
loss_count = len(losses)
win_rate = win_count / total * 100 if total > 0 else 0
total_pnl = sum(t.get('pnl', 0) for t in yesterday_trades)

avg_win = sum(t.get('pnl', 0) for t in wins) / win_count if win_count > 0 else 0
avg_loss = abs(sum(t.get('pnl', 0) for t in losses)) / loss_count if loss_count > 0 else 0
rr_ratio = avg_win / avg_loss if avg_loss > 0 else 0
expectancy = win_rate / 100 * avg_win - (1 - win_rate / 100) * avg_loss

# ── 币种统计 ──
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

# ── 最佳/最差单笔 ──
best = max(yesterday_trades, key=lambda t: t.get('pnl', 0))
worst = min(yesterday_trades, key=lambda t: t.get('pnl', 0))

# ── 退出原因盈亏统计 ──
reason_pnl = {}
for t in yesterday_trades:
    r = t.get('reason', '未知')
    reason_pnl.setdefault(r, 0)
    reason_pnl[r] += t.get('pnl', 0)

# ════════════════════════════════════════
#  主动风控统计
# ════════════════════════════════════════
reason_labels = {
    'TP': '+6%止盈', 'SL': '-6%止损', 'TRAIL': '追踪触发',
    'TIME_STOP': '时间止损', '1H_REVERSAL': '1h逆势', 'REVERSAL': '反转预警',
    'WATERFALL': '瀑布保护', 'SWEEP': '15m横扫', 'CONS_BULL': '连续阳线',
    'REDUCE_WARN': '减仓预警', 'MARKET': '手动平仓'
}

active_controls = {}
for t in yesterday_trades:
    r = t.get('reason', '未知')
    label = reason_labels.get(r, r)
    if label not in active_controls:
        active_controls[label] = {'count': 0, 'pnl': 0.0}
    active_controls[label]['count'] += 1
    active_controls[label]['pnl'] += t.get('pnl', 0)

# ════════════════════════════════════════
#  输出报告
# ════════════════════════════════════════

date_str = yesterday_start.strftime('%Y-%m-%d')
btc_price = "—"
try:
    import urllib.request
    rr = urllib.request.urlopen('https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT', timeout=5)
    btc_data = json.loads(rr.read())
    btc_price = f"${float(btc_data['price']):,.0f}"
except:
    pass

lines = []
lines.append(f"📅 {date_str} 盘后复盘 | BTC {btc_price}")
lines.append(f"{'─'*42}")

# 余额变化
if bal_ordered:
    first_bal = bal_ordered[0][1]
    last_bal = bal_ordered[-1][1]
    chg = last_bal - first_bal
    chg_pct = chg / first_bal * 100 if first_bal > 0 else 0
    # 最大回撤
    peak = first_bal
    max_dd = 0
    for _, bal in bal_ordered:
        if bal > peak:
            peak = bal
        dd = (peak - bal) / peak * 100
        if dd > max_dd:
            max_dd = dd
    lines.append(f"💰 账户: ${first_bal:.2f} → ${last_bal:.2f} ({chg_pct:+.2f}%) | 最大回撤 {max_dd:.1f}%")
else:
    lines.append(f"💰 账户: — (需运行一天后显示资金曲线)")

lines.append(f"📊 交易 {total}笔 | 胜率 {win_rate:.0f}% ({win_count}胜/{loss_count}负)")
lines.append(f"   盈亏比 {rr_ratio:.2f}:1 | 期望值 ${expectancy:+.2f}/笔 | 总PnL ${total_pnl:+.2f}")
lines.append("")

# 规则触发
lines.append(f"🛠 规则触发:")
lines.append(f"  🔒 P1大周期拦截: {p1_intercepts}次")
if circuit_breakers > 0:
    lines.append(f"  🚨 断路器触发: {circuit_breakers}次")
if active_controls:
    lines.append(f"  🔄 主动风控退出分布:")
    for label, st in sorted(active_controls.items(), key=lambda x: x[1]['count'], reverse=True):
        lines.append(f"    {label:<12s} {st['count']}笔 ${st['pnl']:+.2f}")
lines.append("")

# 退出原因分析
if reason_pnl:
    lines.append(f"🔍 退出原因盈亏:")
    for reason, pnl in sorted(reason_pnl.items(), key=lambda x: x[1]):
        label = reason_labels.get(reason, reason)
        lines.append(f"  {label:<14s} ${pnl:+.2f}")
    lines.append("")

# 币种表现
if coin_stats:
    lines.append(f"📈 币种表现:")
    for sym, st in sorted(coin_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
        wr = st['wins']/st['trades']*100 if st['trades']>0 else 0
        emoji = '🟢' if st['pnl'] > 0 else ('🔴' if st['pnl'] < 0 else '⚪')
        lines.append(f"  {emoji} {sym.upper():<10s} {st['trades']}笔 {wr:.0f}%胜率 ${st['pnl']:+.2f}")
    lines.append("")

# 最佳/最差单笔
best_reason = reason_labels.get(best.get('reason',''), best.get('reason',''))
worst_reason = reason_labels.get(worst.get('reason',''), worst.get('reason',''))
lines.append(f"🏆 最佳单笔: {best.get('sym','?').upper()} ${best.get('pnl',0):+.2f} ({best.get('pnl_pct',0):+.1f}%) | {best_reason}")
lines.append(f"💀 最差单笔: {worst.get('sym','?').upper()} ${worst.get('pnl',0):+.2f} ({worst.get('pnl_pct',0):+.1f}%) | {worst_reason}")
lines.append("")

# 改进建议
lessons = []
if loss_count > win_count:
    lessons.append(f"⚠️ 胜率{win_rate:.0f}%低于50%，检查入场条件是否过松")
if total_pnl < 0:
    lessons.append(f"💰 总亏损${abs(total_pnl):.2f}，建议降低仓位或提高RR门槛")
if expectancy < 0:
    lessons.append(f"📉 期望值为负(${expectancy:.2f})，策略亏损建议暂停复盘")
if rr_ratio < 1.0 and win_count > 0:
    lessons.append(f"📐 盈亏比{rr_ratio:.2f} < 1，止盈给太早或止损过宽")
worst_r = min(reason_pnl.items(), key=lambda x: x[1])
if worst_r[1] < -0.5:
    lessons.append(f"🔧 {reason_labels.get(worst_r[0], worst_r[0])}造成最大亏损(${worst_r[1]:.2f})")

if lessons:
    lines.append(f"💡 改进建议:")
    for l in lessons:
        lines.append(f"  {l}")

print('\n'.join(lines))
