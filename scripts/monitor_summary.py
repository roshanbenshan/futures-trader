#!/usr/bin/env python3
"""持仓快报：每30分钟推送当前持仓状态"""

import json, os, sys, time, urllib.request

try:
    import redis as redis_mod
except:
    print("⚠️ redis模块未安装", file=sys.stderr)
    sys.exit(0)

REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

def get_redis():
    try:
        return redis_mod.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    except:
        return None

def main():
    r = get_redis()
    if not r:
        print("⚠️ Redis不可用")
        return
    
    # 读取余额
    bal_raw = r.get('bal')
    bal = 0
    if bal_raw:
        try:
            bal = json.loads(bal_raw).get('bal', 0)
        except:
            pass
    
    # 读取持仓
    pos_keys = r.keys('pos:*') or []
    positions = []
    for k in sorted(pos_keys):
        sym = k.replace('pos:', '')
        val = r.get(k)
        if val:
            try:
                p = json.loads(val)
                amt = float(p.get('positionAmt', 0))
                if abs(amt) <= 0:
                    continue
                side = '做多' if amt > 0 else '做空'
                entry = float(p.get('entryPrice', 0))
                upnl = float(p.get('unRealizedProfit', 0))
                margin_used = entry * abs(amt) / 5  # 5x杠杆
                pnl_pct = upnl / margin_used * 100 if margin_used > 0 else 0
                positions.append((sym, side, abs(amt), entry, upnl, pnl_pct, margin_used))
            except:
                continue
    
    if not positions and bal <= 0:
        print("📭 空仓 | 余额: $0.00")
        return
    
    # 构建消息
    lines = []
    if positions:
        lines.append(f"📊 **当前持仓 ({len(positions)})**")
        total_upnl = 0
        for sym, side, qty, entry, upnl, pnl_pct, margin in positions:
            icon = '🟢' if upnl >= 0 else '🔴'
            total_upnl += upnl
            lines.append(f"{icon} {sym.upper()} {side} {qty:.2f} @${entry:.4f}")
            lines.append(f"   PnL: ${upnl:+.2f} ({pnl_pct:+.1f}%) | 保证金: ${margin:.2f}")
        lines.append(f"")
        lines.append(f"总浮亏: ${total_upnl:+.2f}")
    else:
        lines.append("📭 **空仓**")
    
    lines.append(f"💰 余额: ${bal:.2f}")
    
    # 运行时间
    lines.append(f"⏱ 运行中")
    
    print('\n'.join(lines))

if __name__ == '__main__':
    main()
