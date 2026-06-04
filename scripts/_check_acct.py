import json, subprocess, sys, os, hmac, hashlib, time

API_KEY = os.environ.get('BINANCE_API_KEY', '')
API_SECRET = os.environ.get('BINANCE_API_SECRET', '')
PROXY = 'http://127.0.0.1:7890'

def sign(params, secret):
    query = '&'.join(f'{k}={v}' for k, v in sorted(params.items()))
    sig = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f'{query}&signature={sig}'

ts = int(time.time() * 1000)
url = f'https://fapi.binance.com/fapi/v2/account?{sign({"timestamp": ts}, API_SECRET)}'
r = subprocess.run(['curl', '-s', '--proxy', PROXY, url, '-H', f'X-MBX-APIKEY: {API_KEY}'], capture_output=True, text=True, timeout=15)
data = json.loads(r.stdout)

print(f'余额: ${float(data.get("totalWalletBalance",0)):.2f}')
avail = float(data.get("availableBalance",0))
print(f'可用: ${avail:.2f}')

positions = [p for p in data['positions'] if float(p.get('positionAmt', 0)) != 0]
for p in positions:
    sym = p['symbol']
    amt = float(p['positionAmt'])
    entry = float(p['entryPrice'])
    mark = float(p['markPrice'])
    upl = float(p['unRealizedProfit'])
    pnl_pct = (mark/entry - 1)*100
    side = 'LONG'
    if amt < 0:
        pnl_pct = (entry/mark - 1)*100
        side = 'SHORT'
    liq = float(p.get('liquidationPrice', 0))
    print(f'{sym:10s} {side:5s} {abs(amt):>8.1f}  入场${entry:<8.4f}  当前${mark:<8.4f}  {pnl_pct:+.2f}%  浮盈${upl:.4f}  强平${liq}')
    # calculate SL and TP
    if side == 'LONG':
        sl = entry * 0.988
        tp = entry * 1.012
    else:
        sl = entry * 1.012
        tp = entry * 0.988
    sl_dist = (sl/mark - 1)*100 if side=='LONG' else (sl/mark - 1)*100
    tp_dist = (tp/mark - 1)*100 if side=='LONG' else (tp/mark - 1)*100
    print(f'     ±6% SL=${sl:.4f} ({sl_dist:+.2f}%)  TP=${tp:.4f} ({tp_dist:+.2f}%)')

if not positions:
    print('无持仓')
