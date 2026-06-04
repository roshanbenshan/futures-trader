#!/usr/bin/env python3
"""
data_collector.py — 独立数据收集器 v1.1
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
功能: WS订阅币安K线/账户/行情/资金费率 → 写入Redis
特点: 永不参与交易，gateway重启不影响，data永不断层
       Redis作为Binance数据镜像，AI/daemon零HTTP查询
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import asyncio, json, time, os, hmac, hashlib, sys, signal
from datetime import datetime
from collections import deque
import websockets
import redis as redis_mod

# ══ 自动加载 .env ══
if not os.environ.get('BINANCE_API_KEY'):
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    os.environ[k.strip()] = v.strip().strip("'\"")
        print(f"  📄 已加载 .env: {env_path}")

API_KEY = os.environ.get('BINANCE_API_KEY', '')
API_SECRET = os.environ.get('BINANCE_API_SECRET', '')
WS_BASE = 'wss://fstream.binance.com/ws'
FAPI = 'https://fapi.binance.com'
MAX_SYMBOLS = 150

# ══ Redis ══
REDIS_CLIENT = None
REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
RK_KLINES = 'kl:'           # 最新15m K线JSON（最新一根，短过期）
RK_KLINES_15M = 'kl_15m:'   # 最近20根15m收盘价list（供扫评分使用）
RK_KLINES_1H = 'kl_1h:'     # 最近14根1h收盘价list
RK_TREND = 'trend_1h:'      # 预计算1h MA7/MA14方向
RK_POS = 'pos:'             # 持仓
RK_RP = 'rp:'               # 已实现盈亏（平仓时保存，24h有效）
RK_FUNDING = 'funding:'     # 资金费率
RK_MARK = 'mark:'           # 标记价格
RK_BOOK = 'book:'           # 买卖盘

def redis_init():
    global REDIS_CLIENT
    try:
        REDIS_CLIENT = redis_mod.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        REDIS_CLIENT.ping()
        print("  ✅ Redis已连接")
        return True
    except Exception as e:
        print(f"  ⚠️ Redis: {e}")
        REDIS_CLIENT = None
        return False

def redis_put(key, value, expire=3600):
    global REDIS_CLIENT
    if REDIS_CLIENT is None: return
    try: REDIS_CLIENT.set(key, value, ex=expire)
    except: pass

# ══ HTTP 工具 ══
def sign(params, secret):
    q = '&'.join(f'{k}={v}' for k, v in sorted(params.items()))
    sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
    return f'{q}&signature={sig}'

async def http_get(path, params=None):
    ts = int(time.time() * 1000)
    if params is None: params = {}
    params['timestamp'] = str(ts)
    q = sign(params, API_SECRET)
    sep = '&' if '?' in path else '?'
    proc = await asyncio.create_subprocess_exec(
        'curl', '-s', f'{FAPI}{path}{sep}{q}',
        '-H', f'X-MBX-APIKEY:{API_KEY}',
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
    return json.loads(stdout) if stdout else {}

async def http_post(path, params=None):
    ts = int(time.time() * 1000)
    if params is None: params = {}
    params['timestamp'] = str(ts)
    q = sign(params, API_SECRET)
    proc = await asyncio.create_subprocess_exec(
        'curl', '-s', '-X', 'POST', f'{FAPI}{path}?{q}',
        '-H', f'X-MBX-APIKEY:{API_KEY}',
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
    return json.loads(stdout) if stdout else {}

async def http_get_public(path):
    proc = await asyncio.create_subprocess_exec(
        'curl', '-s', f'{FAPI}{path}',
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
    return json.loads(stdout) if stdout else {}

async def fetch_balance_rest():
    try:
        acct = await http_get('/fapi/v2/account')
        return float(acct.get('totalWalletBalance', 0))
    except:
        return 0.0

# ═══════════════════════════════════════════
# WS 数据流
# ═══════════════════════════════════════════

async def user_data_stream():
    """账户WS → 持仓/余额/账户信息写入Redis"""
    while True:
        try:
            resp = await http_post('/fapi/v1/listenKey')
            lk = resp.get('listenKey', '')
            if not lk: await asyncio.sleep(5); continue
            async with websockets.connect(f'{WS_BASE}/{lk}') as ws:
                print("  ✅ 账户WS已连接")
                while True:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=60))
                    et = msg.get('e', '')
                    if et == 'ACCOUNT_UPDATE':
                        a = msg.get('a', {})
                        # ── 持仓 ──
                        for pos in a.get('P', []):
                            sym = pos.get('s', '').lower()
                            amt = float(pos.get('pa', 0))
                            if abs(amt) > 0:
                                redis_put(f'{RK_POS}{sym}', json.dumps({
                                    'positionAmt': pos.get('pa', '0'),
                                    'entryPrice': pos.get('ep', '0'),
                                    'unRealizedProfit': pos.get('up', '0'),
                                    'realizedPnl': pos.get('rp', '0'),
                                }), 3600)
                            else:
                                # 平仓：先保存realizedPnl再清空
                                rp = pos.get('rp', '0')
                                if float(rp) != 0:
                                    redis_put(f'{RK_RP}{sym}', json.dumps({
                                        'realizedPnl': rp,
                                        'ts': time.time(),
                                        'entryPrice': pos.get('ep', '0'),
                                    }), 86400)
                                redis_put(f'{RK_POS}{sym}', '', 1)
                        # ── 余额 ──
                        for bal in a.get('B', []):
                            if bal.get('a', '') == 'USDT':
                                redis_put('bal', json.dumps({
                                    'bal': round(float(bal.get('wb', 0)), 2),
                                    'cw': float(bal.get('cw', 0)),      # cross wallet
                                    'available': round(float(bal.get('wb', 0)) - float(bal.get('cw', 0)), 2)
                                }), 3600)
                        # ── 完整账户快照 ──
                        b_map = {b.get('a', '').upper(): b for b in a.get('B', [])}
                        usdt_bal = b_map.get('USDT', {})
                        redis_put('account:summary', json.dumps({
                            'walletBalance': float(usdt_bal.get('wb', 0)),
                            'crossWallet': float(usdt_bal.get('cw', 0)),
                            'availableBalance': round(float(usdt_bal.get('wb', 0)) - float(usdt_bal.get('cw', 0)), 2),
                            'crossUnPnl': float(usdt_bal.get('up', 0)),
                            'ts': time.time()
                        }), 3600)
                    elif et in ('listenKeyExpired', 'error'): break
        except asyncio.TimeoutError:
            try: await http_post('/fapi/v1/listenKey')
            except: pass
        except Exception as e:
            print(f"  ⚠️ 账户WS: {e}")
            await asyncio.sleep(2)

async def kline_stream():
    """15m K线WS → 写入Redis 最新K线+价格列表（供扫评分使用）"""
    retry_delay = 1
    while True:
        try:
            info = await http_get_public('/fapi/v1/exchangeInfo')
            syms = [s['symbol'].lower() for s in info.get('symbols', [])
                    if s['symbol'].endswith('USDT') and s['status'] == 'TRADING']
            try:
                tk = await http_get_public('/fapi/v1/ticker/24hr')
                vm = {t['symbol'].lower(): float(t.get('quoteVolume', 0))
                      for t in tk if isinstance(t, dict)}
                syms.sort(key=lambda s: vm.get(s, 0), reverse=True)
            except: pass
            scan = syms[:MAX_SYMBOLS]
            retry_delay = 1

            # REST预加载：写最新K线 + 15m价格列表
            need_preload = []
            for s in scan:
                try:
                    if REDIS_CLIENT and REDIS_CLIENT.exists(f'{RK_KLINES_15M}{s}'):
                        continue
                except: pass
                need_preload.append(s)
            if need_preload:
                print(f"  📡 预加载15m {len(need_preload)} 个币种...")
                sem = asyncio.Semaphore(10)
                async def load_one(sym):
                    async with sem:
                        try:
                            hist = await http_get_public(f'/fapi/v1/klines?symbol={sym.upper()}&interval=15m&limit=20')
                            if isinstance(hist, list) and len(hist) >= 10:
                                closes = [float(k[4]) for k in hist]
                                # 写最新K线JSON
                                last_bar = hist[-1]
                                cdl = {'t': last_bar[0], 'o': float(last_bar[1]), 'h': float(last_bar[2]),
                                       'l': float(last_bar[3]), 'c': float(last_bar[4]), 'final': True}
                                redis_put(f'{RK_KLINES}{sym}', json.dumps(cdl), 300)
                                # 写15m价格列表（供daemon扫评分零HTTP）
                                rkey15 = f'{RK_KLINES_15M}{sym}'
                                if REDIS_CLIENT:
                                    REDIS_CLIENT.delete(rkey15)
                                    for p in closes:
                                        REDIS_CLIENT.rpush(rkey15, str(p))
                                    REDIS_CLIENT.ltrim(rkey15, -30, -1)
                                    REDIS_CLIENT.expire(rkey15, 3600)
                        except: pass
                await asyncio.gather(*[load_one(s) for s in need_preload])
                print(f"  ✅ 15m预加载完成: {len(need_preload)} 个")

            # WS订阅
            streams = [f'{s}@kline_15m' for s in scan]
            for i in range(0, len(streams), 200):
                chunk = streams[i:i+200]
                sp = '/'.join(chunk)
                async with websockets.connect(f'{WS_BASE}/stream?streams={sp}') as ws:
                    print(f"  ✅ K线15m WS ({i//200+1})")
                    while True:
                        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=60))
                        k = msg.get('data', {}).get('k', {})
                        sym = k.get('s', '').lower()
                        if not sym: continue
                        close = float(k['c'])
                        cdl = {'t': k['t'], 'o': float(k['o']), 'h': float(k['h']),
                               'l': float(k['l']), 'c': close, 'final': k['x']}
                        redis_put(f'{RK_KLINES}{sym}', json.dumps(cdl), 300)
                        # 追加到15m价格列表（保持最多30个，扫描需要≥25根）
                        rkey15 = f'{RK_KLINES_15M}{sym}'
                        try:
                            if REDIS_CLIENT:
                                REDIS_CLIENT.rpush(rkey15, str(close))
                                REDIS_CLIENT.ltrim(rkey15, -30, -1)
                                REDIS_CLIENT.expire(rkey15, 3600)
                        except: pass
                await asyncio.sleep(0.5)
        except Exception as e:
            delay = min(retry_delay, 30)
            print(f"  ⚠️ K线15m WS: {e} 重试{delay}s后")
            await asyncio.sleep(delay)
            retry_delay = min(retry_delay * 2, 60)

async def kline_1h_stream():
    """1h K线WS → 150币种1h K线+预计算MA7/MA14方向写入Redis"""
    retry_delay = 1
    while True:
        try:
            info = await http_get_public('/fapi/v1/exchangeInfo')
            syms = [s['symbol'].lower() for s in info.get('symbols', [])
                    if s['symbol'].endswith('USDT') and s['status'] == 'TRADING']
            try:
                tk = await http_get_public('/fapi/v1/ticker/24hr')
                vm = {t['symbol'].lower(): float(t.get('quoteVolume', 0))
                      for t in tk if isinstance(t, dict)}
                syms.sort(key=lambda s: vm.get(s, 0), reverse=True)
            except: pass
            scan = syms[:MAX_SYMBOLS]
            retry_delay = 1

            # ── REST预加载1h K线 ──
            need_preload = []
            for s in scan:
                try:
                    if REDIS_CLIENT and REDIS_CLIENT.exists(f'{RK_TREND}{s}'):
                        continue
                except: pass
                need_preload.append(s)
            if need_preload:
                print(f"  📡 预加载1h {len(need_preload)} 个币种...")
                sem = asyncio.Semaphore(10)
                async def load_1h(sym):
                    async with sem:
                        try:
                            hist = await http_get_public(f'/fapi/v1/klines?symbol={sym.upper()}&interval=1h&limit=14')
                            if isinstance(hist, list) and len(hist) >= 10:
                                prices = [float(k[4]) for k in hist]
                                rkey = f'{RK_KLINES_1H}{sym}'
                                if REDIS_CLIENT:
                                    REDIS_CLIENT.delete(rkey)
                                    for p in prices:
                                        REDIS_CLIENT.rpush(rkey, str(p))
                                    REDIS_CLIENT.ltrim(rkey, -14, -1)
                                    REDIS_CLIENT.expire(rkey, 3600)
                                _update_1h_trend(sym, prices)
                        except: pass
                await asyncio.gather(*[load_1h(s) for s in need_preload])
                print(f"  ✅ 1h预加载完成: {len(need_preload)} 个")

            # ── WS订阅1h K线 ──
            streams = [f'{s}@kline_1h' for s in scan]
            for i in range(0, len(streams), 200):
                chunk = streams[i:i+200]
                sp = '/'.join(chunk)
                async with websockets.connect(f'{WS_BASE}/stream?streams={sp}') as ws:
                    print(f"  ✅ 1h K线WS ({i//200+1})")
                    while True:
                        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=60))
                        k = msg.get('data', {}).get('k', {})
                        sym = k.get('s', '').lower()
                        if not sym: continue
                        if not k.get('x', False):
                            continue
                        close = float(k['c'])
                        rkey = f'{RK_KLINES_1H}{sym}'
                        if REDIS_CLIENT:
                            REDIS_CLIENT.rpush(rkey, str(close))
                            REDIS_CLIENT.ltrim(rkey, -14, -1)
                            REDIS_CLIENT.expire(rkey, 3600)
                            prices = REDIS_CLIENT.lrange(rkey, 0, -1)
                            if prices and len(prices) >= 7:
                                _update_1h_trend(sym, [float(p) for p in prices])
                await asyncio.sleep(0.5)
        except Exception as e:
            delay = min(retry_delay, 30)
            print(f"  ⚠️ 1h K线WS: {e} 重试{delay}s后")
            await asyncio.sleep(delay)
            retry_delay = min(retry_delay * 2, 60)


async def mark_price_stream():
    """标记价格+资金费率WS → Redis（全币种单连接）
    
    !markPrice@arr 返回全部合约的标记价格和资金费率
    格式: {"e":"markPriceUpdate","E":...,"s":"BTCUSDT","p":"...","P":"...","r":"...","T":...}
    p=标记价, P=预测资金费率, r=当前资金费率, T=结算时间
    """
    retry_delay = 1
    while True:
        try:
            async with websockets.connect(f'{WS_BASE}/!markPrice@arr@1s') as ws:
                print("  ✅ 标记价+资金费率WS已连接")
                while True:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
                    # 批量数据
                    if isinstance(msg, list):
                        for item in msg:
                            sym = item.get('s', '').lower()
                            if not sym or not sym.endswith('usdt'):
                                continue
                            # 资金费率
                            cur_fr = float(item.get('r', 0))
                            pred_fr = float(item.get('P', 0))
                            next_time = item.get('T', 0)
                            # 标记价格
                            mark_p = float(item.get('p', 0))
                            index_p = float(item.get('i', 0))
                            
                            redis_put(f'{RK_FUNDING}{sym}', json.dumps({
                                'rate': cur_fr,           # 当前资金费率
                                'predicted': pred_fr,       # 预测资金费率
                                'next_time': next_time,     # 下次结算时间戳
                                'ts': time.time()
                            }), 3600)
                            redis_put(f'{RK_MARK}{sym}', json.dumps({
                                'markPrice': mark_p,
                                'indexPrice': index_p,
                                'ts': time.time()
                            }), 3600)
        except Exception as e:
            delay = min(retry_delay, 30)
            print(f"  ⚠️ 标记价WS: {e} 重试{delay}s后")
            await asyncio.sleep(delay)
            retry_delay = min(retry_delay * 2, 60)


async def book_ticker_stream():
    """全币种最优买卖价WS → Redis"""
    retry_delay = 1
    while True:
        try:
            async with websockets.connect(f'{WS_BASE}/!bookTicker') as ws:
                print("  ✅ 买卖盘WS已连接")
                while True:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=60))
                    sym = msg.get('s', '').lower()
                    if not sym: continue
                    redis_put(f'{RK_BOOK}{sym}', json.dumps({
                        'bid': float(msg.get('b', 0)),
                        'ask': float(msg.get('a', 0)),
                        'ts': time.time()
                    }), 300)  # 5min过期，实时更新
        except Exception as e:
            delay = min(retry_delay, 30)
            print(f"  ⚠️ 买卖盘WS: {e} 重试{delay}s后")
            await asyncio.sleep(delay)
            retry_delay = min(retry_delay * 2, 60)


def _update_1h_trend(sym, prices):
    """计算1h趋势方向并写入Redis trend_1h:{sym}"""
    if REDIS_CLIENT is None or len(prices) < 7:
        return
    ma7 = sum(prices[-7:]) / 7
    ma14 = sum(prices[-14:]) / 14 if len(prices) >= 14 else ma7
    diff_pct = abs(ma7 - ma14) / ma14 * 100 if ma14 > 0 else 0
    if diff_pct < 0.1:
        direction = 'flat'
    elif ma7 > ma14:
        direction = 'bull'
    else:
        direction = 'bear'
    REDIS_CLIENT.set(f'{RK_TREND}{sym}', json.dumps({
        'ma7': round(ma7, 8),
        'ma14': round(ma14, 8),
        'direction': direction,
        'ts': time.time()
    }), ex=3600)


# ═══════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════

async def main():
    print(f"🚀 数据收集器 v1.1 | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    LOG_FILE = '/tmp/data_collector.log'
    try:
        lf = open(LOG_FILE, 'a', buffering=1)
        sys.stdout = lf
        sys.stderr = lf
        print(f"\n=== {datetime.utcnow().strftime('%H:%M UTC')} 启动 ===")
    except: pass
    if not API_KEY or not API_SECRET: print("❌ API未设置"); sys.exit(1)

    redis_init()

    # 启动前REST写入一次完整数据（刚启动时WS还没数据）
    try:
        acct = await http_get('/fapi/v2/account')
        bal = float(acct.get('totalWalletBalance', 0))
        avail = float(acct.get('availableBalance', 0))
        redis_put('bal', json.dumps({
            'bal': round(bal, 2),
            'cw': float(acct.get('totalCrossWalletBalance', 0)),
            'available': round(avail, 2),
            'ts': time.time()
        }), 3600)
        redis_put('account:summary', json.dumps({
            'walletBalance': bal,
            'availableBalance': avail,
            'crossWallet': float(acct.get('totalCrossWalletBalance', 0)),
            'totalUnrealizedProfit': float(acct.get('totalUnrealizedProfit', 0)),
            'totalMaintMargin': float(acct.get('totalMaintMargin', 0)),
            'totalInitialMargin': float(acct.get('totalInitialMargin', 0)),
            'ts': time.time()
        }), 3600)
        for p in acct.get('positions', []):
            amt = float(p.get('positionAmt', 0))
            if abs(amt) > 0:
                redis_put(f'{RK_POS}{p["symbol"].lower()}', json.dumps({
                    'positionAmt': p['positionAmt'],
                    'entryPrice': p['entryPrice'],
                    'unRealizedProfit': p.get('unRealizedProfit', '0'),
                }), 3600)
        print(f"💰 ${bal:.2f} 可用${avail:.2f}")
    except: pass

    async def resilient_wrapper(name, coro_factory):
        retry = 1
        while True:
            try:
                await coro_factory()
            except asyncio.CancelledError:
                await asyncio.sleep(1)
                retry = min(retry * 2, 30)
                continue
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                short_tb = '\n'.join(tb.split('\n')[-6:])
                print(f"  💥 {name} 崩溃({e})\n    {short_tb}")
            retry = min(retry * 2, 30)
            await asyncio.sleep(retry)
            if retry > 5:
                print(f"  🔄 等待{retry}s后重启 {name}...")

    tasks = [
        asyncio.create_task(resilient_wrapper('账户WS', user_data_stream)),
        asyncio.create_task(resilient_wrapper('K线WS(15m)', kline_stream)),
        asyncio.create_task(resilient_wrapper('K线WS(1h)', kline_1h_stream)),
        asyncio.create_task(resilient_wrapper('标记价+资金费率', mark_price_stream)),
        asyncio.create_task(resilient_wrapper('买卖盘', book_ticker_stream)),
    ]

    def shutdown():
        print("\n🛑 数据收集器关闭...")
        for t in tasks: t.cancel()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: signal.signal(sig, lambda s, f: shutdown())
        except: pass

    await asyncio.gather(*tasks, return_exceptions=True)
    print("👋 已关闭")

if __name__ == '__main__':
    asyncio.run(main())
