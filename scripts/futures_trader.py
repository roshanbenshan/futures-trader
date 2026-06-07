#!/usr/bin/env python3
"""
futures_trader.py — 统一合约交易系统 v2.3
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
整合来源: daemon_ws.py, auto_trader.py, 
          bnb/eth/ondo/trx/zec_stop_loss_monitor.py,
          zec_entry_monitor/position_manager,
          setup_algos/setup_6pct_algos/place_algo_short/reopen_algo/fix_algo,
          market_scanner, monitor_signals, check_positions, tao_monitor
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import asyncio, json, time, os, hmac, hashlib, math, sys, signal
from datetime import datetime
from collections import deque
import websockets
import redis as redis_mod  # Redis 缓存层：daemon 写入 WS 实时数据，AI/CLI 毫秒级读取

# ═══════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════
# ══ 自动加载 .env（双fork后环境变量丢失的兜底） ══
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

# ══ Redis缓存连接（daemon写入WS数据，AI毫秒级读取） ══
REDIS_CLIENT = None
REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

def redis_put(key, value, expire=3600):
    global REDIS_CLIENT
    if REDIS_CLIENT is None: return
    try: REDIS_CLIENT.set(key, value, ex=expire)
    except: pass

def redis_get(key):
    global REDIS_CLIENT
    if REDIS_CLIENT is None: return None
    try: return REDIS_CLIENT.get(key)
    except: return None

def backfill_trades_to_redis():
    """启动时回填本地交易记录到Redis（供AI零HTTP查询历史）"""
    global REDIS_CLIENT
    if REDIS_CLIENT is None: return
    try:
        tf_path = os.path.join(SCRIPT_DIR, 'futures_trades.json')
        if os.path.exists(tf_path):
            with open(tf_path) as f:
                trades = json.load(f)
            if isinstance(trades, list) and len(trades) > 0:
                # 检查Redis是否已有数据（防重复回填）
                existing = REDIS_CLIENT.llen('trades:history')
                if existing == 0:
                    for t in trades:
                        sym = t.get('sym', '').lower()
                        t_json = json.dumps(t)
                        REDIS_CLIENT.lpush('trades:history', t_json)
                        if sym:
                            REDIS_CLIENT.set(f'{RK_TRADE}{sym}', t_json, ex=86400)
                    REDIS_CLIENT.ltrim('trades:history', 0, 199)
                    # 汇总当日PNL
                    today = datetime.utcnow().strftime('%Y%m%d')
                    today_ts = 0.0
                    today_pnl = 0.0
                    for t in trades:
                        if t.get('ts', '').startswith(today[:4] + '-' + today[4:6] + '-' + today[6:8]):
                            today_pnl += float(t.get('pnl', 0))
                        # 累计最近30天daily pnl
                        day = t.get('ts', '')[:10].replace('-', '')
                        if day:
                            REDIS_CLIENT.incrbyfloat(f'stats:pnl:{day}', float(t.get('pnl', 0)))
                            REDIS_CLIENT.expire(f'stats:pnl:{day}', 86400 * 7)
                    print(f"  🔄 回填 {len(trades)} 笔交易到Redis ✓ (今日${today_pnl:.2f})")
    except Exception as e:
        print(f"  ⚠️ 回填失败: {e}")

def clean_stale_open_trades():
    """清理trade_db.json中残留的历史开仓（只保留当前真实持仓）"""
    try:
        db_path = os.path.join(SCRIPT_DIR, 'trade_db.json')
        if not os.path.exists(db_path):
            return
        with open(db_path) as f:
            db = json.load(f)
        ots = db.get('open_trades', {})
        # 只保留与当前state['positions']匹配的开仓
        real_syms = set(state.get('positions', {}).keys())
        stale = [s for s in ots if s not in real_syms]
        if stale:
            for s in stale:
                del ots[s]
            db['open_trades'] = ots
            with open(db_path, 'w') as f:
                json.dump(db, f, indent=2)
            print(f"  🧹 清理trade_db.json: 移除{len(stale)}个残留开仓 ({', '.join(stale)})")
    except Exception as e:
        print(f"  ⚠️ 清理stale open_trades失败: {e}")

def redis_get_klines_15m(sym):
    """从Redis读取最近20根15m收盘价（data_collector持续写入），失败时返回None"""
    global REDIS_CLIENT
    if REDIS_CLIENT is None: return None
    try:
        key = f'{RK_KLINES_15M}{sym.lower()}'
        vals = REDIS_CLIENT.lrange(key, 0, -1)
        if vals and len(vals) >= 10:
            return [float(v) for v in vals]
    except: pass
    return None

def redis_get_funding(sym):
    """从Redis读取资金费率（data_collector的mark_price_stream写入），失败返回None"""
    global REDIS_CLIENT
    if REDIS_CLIENT is None: return None
    try:
        val = REDIS_CLIENT.get(f'{RK_FUNDING}{sym.lower()}')
        if val:
            return json.loads(val)
    except: pass
    return None

def redis_get_mark(sym):
    """从Redis读取标记价格"""
    global REDIS_CLIENT
    if REDIS_CLIENT is None: return None
    try:
        val = REDIS_CLIENT.get(f'{RK_MARK}{sym.lower()}')
        if val:
            return json.loads(val)
    except: pass
    return None

def push_event(event_type, data):
    """写入盘中监测事件到Redis
    event_type: 'open'|'close'|'alert'|'heartbeat'
    data: dict
    """
    try:
        ev = json.dumps({'t': event_type, 'ts': time.time(), 'd': data})
        if REDIS_CLIENT:
            REDIS_CLIENT.lpush('events:monitor', ev)
            REDIS_CLIENT.ltrim('events:monitor', 0, 49)
    except:
        pass

def redis_init():
    global REDIS_CLIENT
    try:
        REDIS_CLIENT = redis_mod.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        REDIS_CLIENT.ping()
        return True
    except Exception as e:
        print(f"  ⚠️ Redis: {e}（不影响交易）")
        REDIS_CLIENT = None
        return False

# Redis key 前缀
RK_KLINES = 'kl:'       # kl:{sym} → 最新K线JSON
RK_KLINES_15M = 'kl_15m:'  # kl_15m:{sym} → 最近20根15m收盘价list（data_collector写入）
RK_KLINES_1H = 'kl_1h:' # kl_1h:{sym} → 最近14根1h收盘价list
RK_TREND = 'trend_1h:'  # trend_1h:{sym} → 1h MA7/MA14方向JSON
RK_FUNDING = 'funding:' # funding:{sym} → 资金费率JSON
RK_MARK = 'mark:'       # mark:{sym} → 标记价格JSON
RK_TICKER = 'tk:'       # tk:{sym} → 24h行情JSON
RK_POS = 'pos:'         # pos:{sym} → 持仓JSON
RK_TRADE = 'trade:'     # trade:{sym} → 最近一笔平仓记录JSON
API_KEY = os.environ.get('BINANCE_API_KEY', '')
API_SECRET = os.environ.get('BINANCE_API_SECRET', '')
WS_BASE = 'wss://fstream.binance.com/ws'
FAPI = 'https://fapi.binance.com'

MIN_BALANCE = 8.0
RECOVERY_BALANCE = 10.0
MAX_SYMBOLS = 150
SCAN_INTERVAL = 60       # 15m K线策略，1分钟扫描一次
RR_MIN = 1.3
RR_RECOVERY = 2.5
LEVERAGE = 5
PCT_6 = 0.012
MAX_CONCURRENT_TRADES = 3   # 同向仓位上限
MAX_MARGIN_PCT_PER_POS = 1.0 / MAX_CONCURRENT_TRADES  # 每仓位占保证金比例(33%)

# ── 平仓原因枚举（复盘用，标准化tagging） ──
REASON_TP = 'TP_6PCT'           # ±6%止盈
REASON_SL = 'SL_6PCT'           # ±6%止损
REASON_WATERFALL = 'WATERFALL'  # 瀑布保护(急跌3%)
REASON_SWEEP = 'SWEEP'          # 15m横扫(大阳线)
REASON_CONS_BULL = 'CONS_BULL'  # 连续阳线
REASON_TRAIL = 'TRAIL_CALLBACK' # 回调跟踪止损

# ── 连败保护 ──
MAX_LOSS_STREAK = 3   # 连续亏损≥3笔自动暂停交易复盘

# ── 暂停恢复等待(30分钟) ──
PAUSE_COOLDOWN = 1800

# ── 方向级连败暂停(12小时) ──
DIR_PAUSE_COOLDOWN = 43200  # 12h，方向级暂停（如做空连败3笔只暂停做空）
DIR_MAX_LOSS_STREAK = 3     # 方向级连败阈值

# ── 9阶动态资金梯队矩阵 ──
TIER_MATRIX = [
    (15.0,   1, 1, 'FIXED', 0.50),     # 第1阶: 极限生存
    (35.0,   2, 1, 'RATIO', 0.025),    # 第2阶: 微光萌芽
    (70.0,   3, 2, 'RATIO', 0.030),    # 第3阶: 稳健回血
    (150.0,  4, 2, 'RATIO', 0.035),    # 第4阶: 初具规模
    (300.0,  5, 3, 'RATIO', 0.035),    # 第5阶: 资金合流
    (600.0,  6, 4, 'RATIO', 0.040),    # 第6阶: 中坚复利
    (1200.0, 7, 5, 'RATIO', 0.040),    # 第7阶: 多维穿梭
    (3000.0, 8, 6, 'RATIO', 0.030),    # 第8阶: 风险收敛
    (float('inf'), 9, 8, 'RATIO', 0.025), # 第9阶: 雄鹰自由
]

# ── 板块相关性映射（安全阀A）──
SECTOR_MAP = {
    'btc': '蓝筹', 'eth': '蓝筹', 'bnb': '蓝筹',
    'sol': '公链', 'avax': '公链', 'near': '公链', 'dot': '公链',
    'ada': '公链', 'xrp': '公链', 'atom': '公链', 'ftm': '公链',
    'link': '预言机',
    'uni': 'defi', 'aave': 'defi', 'cake': 'defi', 'sushi': 'defi', 'crv': 'defi',
    'doge': 'meme', 'shib': 'meme', 'pepe': 'meme', 'bonk': 'meme', 'wif': 'meme', 'pengu': 'meme',
    'op': 'L2', 'arb': 'L2', 'matic': 'L2', 'strk': 'L2',
    'sei': '新公链', 'sui': '新公链', 'apt': '新公链',
    'inj': 'infra', 'tia': 'infra', 'pendle': 'infra',
    'render': 'AI', 'tao': 'AI', 'fet': 'AI',
    'ondo': 'RWA',
    'xau': '商品', 'xag': '商品',
}

def get_tier_config(balance):
    """9阶动态资金风控矩阵 → (tier, max_positions, risk_amount)"""
    for limit, tier, max_pos, risk_type, risk_val in TIER_MATRIX:
        if balance < limit:
            risk_amt = risk_val if risk_type == 'FIXED' else balance * risk_val
            return tier, max_pos, risk_amt
    return 1, 1, 0.50

def get_sector(sym_lower):
    """获取币种所属板块（未知币种默认独立）"""
    base = sym_lower.replace('usdt', '').replace('1000', '')
    return SECTOR_MAP.get(base, '独立')

# ── 交易复盘 ──
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TRADE_LOG = os.path.join(SCRIPT_DIR, 'futures_trades.json')  # 交易记录文件（持久化）
# ── 合约数据库 ──
TRADE_DB = os.path.join(SCRIPT_DIR, 'trade_db.json')

# ── 技术分析参数 ──
MA_SHORT = 7          # 短期均线周期
MA_LONG = 14          # 长期均线周期
RSI_PERIOD = 14       # RSI周期
RSI_LONG_MIN = 35     # 做多RSI下限
RSI_LONG_MAX = 72     # 做多RSI上限
RSI_SHORT_MIN = 30    # 做空RSI下限（收紧，避开超卖区反弹追空）
RSI_SHORT_MAX = 45    # 做空RSI上限（收紧到45，避开反弹动量区间）
BB_PERIOD = 20        # 布林带周期
BB_STD = 2            # 布林带标准差倍数
ATR_PERIOD = 14       # ATR周期
ATR_MULT = 2.0        # ATR止损倍数（原1.5→2.0，防插针）
ATR_MIN_PCT = 0.003   # ATR止损下限(价格的%)
ATR_MAX_PCT = 0.03    # ATR止损上限(价格的%)
FIB_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786]  # 斐波那契级别
PIVOT_WINDOW = 3      # 枢轴点窗口

CRASH_PCT_24H = -15.0
BOUNCE_MIN_PCT = 5.0
CRASH_LONG_PCT = -10.0

SWEEP_BODY_MULT = 1.5
SWEEP_CHG_MIN = 0.5
CONSISTENT_BULL = 3

# ── 盘中主动风控参数 ──
BREAKEVEN_THRESHOLD = 3.0     # 浮盈≥3% → SL移到保本价
PARTIAL_TP_THRESHOLD = 5.0    # 浮盈≥5% → 平50%锁定利润
TRAIL_ACTIVATE = 8.0           # 浮盈≥8% → 启动追踪止损(回调2%)
TRAIL_CALLBACK = 0.02          # 追踪回调比例
MAX_HOLD_MINS = 240            # 最大持仓时间(4h)无进展→主动减仓
CIRCUIT_BREAKER_PCT = 8.0     # 全账户总回撤阈值：总浮亏≥余额8%时一键清仓
RSI_REVERSAL_WARN = 15.0       # RSI反向移动幅度预警值(原10过于敏感，RSI从29→40正常恢复)
VOL_SPIKE_RATIO = 2.0          # 成交量放大倍数预警

# ── 预警级别 ──
WARN_INFO = 0      # 仅打印
WARN_REDUCE = 1    # 减半仓
WARN_EXIT = 2      # 全平

# ── 断面评分权重 ──
SCORE_TREND_WT = 0.20      # 趋势强度 (原0.25→0.20)
SCORE_RSI_WT = 0.15        # RSI适中度 (原0.20→0.15)
SCORE_VOL_WT = 0.12        # 成交量 (原0.15→0.12)
SCORE_RR_WT = 0.20         # 盈亏比 (原0.25→0.20)
SCORE_ATR_WT = 0.12        # 波动率 (原0.15→0.12)
SCORE_FR_WT = 0.08         # 资金费率 (新增)
SCORE_1H_WT = 0.13         # 1h跨周期动量 (新增)

# ── 瀑布保护 ──
WF_DROP_PCT = 0.03         # 3%急跌触发
WF_BARS = 2                # 2根K线窗口
WF_COOLDOWN_SEC = 300      # 冷却5分钟

# ── ADX趋势过滤 ──
ADX_PERIOD = 14            # ADX周期
ADX_MIN = 20               # 最低ADX(低于20=震荡,跳过)。原25在低波动市场过于严格
ADX_STRONG = 30            # 强趋势阈值
ADX_MIN_SHORT = 30         # 做空最低ADX(需强趋势确认,避免弱趋势中追空)

# ── 工作时间（北京时间） ──   # 用户要求24小时交易，已取消限制
# WORK_HOURS_BJT_START = 8
# WORK_HOURS_BJT_END = 24
# ── 24小时全天候交易，持仓监控永不间断 ──

# ── 测试模式（阻止真实下单，打印模拟结果） ──
TEST_MODE = False  # 正式上线 — 真实交易

# ═══════════════════════════════════════════
# 状态
# ═══════════════════════════════════════════
state = {
    'listen_key': None,
    'positions': {},
    'account_balance': 0.0,
    'kline_data': {},
    'algo_orders': {},
    'last_trade_time': 0,
    'connected': False,
    'running': True,
    'crash_bounce': {},
    'lot_sizes': {},
    'waterfall': {},
    'cooldown': {},         # 平仓冷却: {sym: expiry_timestamp}
    'failed_symbols': {},   # 开仓失败冷却
    'exiting': set(),       # 平仓中锁: {sym} — 防双重下单
    'paused': False,        # 连败暂停
    'pause_reason': '',
    'pause_resume_at': 0,
}

# ── 冷却期持久化（防重启丢冷却） ──
COOLDOWN_FILE = '/tmp/futures_data/cooldown.json'
COOLDOWN_DEFAULT = 1800     # 默认冷却30分钟（防反复进场）
COOLDOWN_AFTER_FAIL = 600   # 开仓失败冷却10分钟
COOLDOWN_AFTER_CLOSE = 3600  # 平仓后冷却1小时（防反复进场）

def load_persistent_cooldown():
    """从磁盘加载冷却期状态（防重启丢失）"""
    try:
        with open(COOLDOWN_FILE) as f:
            data = json.load(f)
            now = time.time()
            # 清理过期的
            for k in list(data.get('cooldown', {})):
                if now >= data['cooldown'][k]:
                    del data['cooldown'][k]
            for k in list(data.get('failed', {})):
                if now >= data['failed'][k]:
                    del data['failed'][k]
            state['cooldown'] = data.get('cooldown', {})
            state['failed_symbols'] = data.get('failed', {})
            if data.get('cooldown') or data.get('failed'):
                print(f"  💾 恢复冷却期: {len(state['cooldown'])}个币种冷却中")
    except (FileNotFoundError, json.JSONDecodeError):
        pass

def save_persistent_cooldown():
    """保存冷却期到磁盘"""
    try:
        os.makedirs(os.path.dirname(COOLDOWN_FILE), exist_ok=True)
        with open(COOLDOWN_FILE, 'w') as f:
            json.dump({
                'cooldown': state.get('cooldown', {}),
                'failed': state.get('failed_symbols', {}),
            }, f)
    except:
        pass

def set_cooldown(sym, duration=COOLDOWN_AFTER_CLOSE):
    """统一设冷却期（内存+磁盘，避免遗漏持久化）"""
    state.setdefault('cooldown', {})[sym.lower()] = time.time() + duration
    save_persistent_cooldown()

def set_failed(sym, duration=COOLDOWN_AFTER_FAIL):
    """统一设失败冷却（内存+磁盘）"""
    state.setdefault('failed_symbols', {})[sym.lower()] = time.time() + duration
    save_persistent_cooldown()

# ═══════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════

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


async def http_delete(path, params=None):
    ts = int(time.time() * 1000)
    if params is None: params = {}
    params['timestamp'] = str(ts)
    q = sign(params, API_SECRET)
    proc = await asyncio.create_subprocess_exec(
        'curl', '-s', '-X', 'DELETE', f'{FAPI}{path}?{q}',
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


async def get_step_size(sym):
    """返回 (stepSize, marketMaxQty) — 含市价单最大量"""
    cached = state.get('lot_sizes', {}).get(sym)
    if cached and isinstance(cached, tuple):
        return cached
    try:
        step, market_max = 0.001, 999999
        info = await http_get_public(f'/fapi/v1/exchangeInfo?symbol={sym}')
        symbols = info.get('symbols', [])
        # exchangeInfo?symbol=xxx 可能返回错误的币种数据（已知bug）
        # 验证返回的symbol是否匹配
        if symbols and symbols[0].get('symbol', '').upper() == sym.upper():
            for f in symbols[0].get('filters', []):
                if f['filterType'] == 'LOT_SIZE':
                    step = float(f['stepSize'])
                if f['filterType'] == 'MARKET_LOT_SIZE':
                    market_max = float(f['maxQty'])
        else:
            # symbol不匹配，全量查
            all_info = await http_get_public('/fapi/v1/exchangeInfo')
            for s in all_info.get('symbols', []):
                if s.get('symbol', '').upper() == sym.upper():
                    for f in s.get('filters', []):
                        if f['filterType'] == 'LOT_SIZE':
                            step = float(f['stepSize'])
                        if f['filterType'] == 'MARKET_LOT_SIZE':
                            market_max = float(f['maxQty'])
                    break
        result = (step, market_max)
        state.setdefault('lot_sizes', {})[sym] = result
        return result
    except:
        pass
    return 0.001, 999999


async def get_price_tick(sym):
    """返回PRICE_FILTER.tickSize（条件单触发价精度）"""
    try:
        info = await http_get_public(f'/fapi/v1/exchangeInfo?symbol={sym}')
        symbols = info.get('symbols', [])
        # exchangeInfo?symbol=xxx 可能返回错误的币种数据（已知问题）
        # 验证返回的symbol是否匹配
        if symbols and symbols[0].get('symbol', '').upper() == sym.upper():
            for f in symbols[0].get('filters', []):
                if f['filterType'] == 'PRICE_FILTER':
                    return float(f['tickSize'])
        # symbol不匹配，重新全量查
        all_info = await http_get_public('/fapi/v1/exchangeInfo')
        for s in all_info.get('symbols', []):
            if s.get('symbol', '').upper() == sym.upper():
                for f in s.get('filters', []):
                    if f['filterType'] == 'PRICE_FILTER':
                        return float(f['tickSize'])
                break
    except:
        pass
    # 根据价格估算合理tick
    return 0.01  # 默认0.01


async def fetch_balance_rest():
    try:
        acct = await http_get('/fapi/v2/account')
        for a in acct.get('assets', []):
            if a['asset'] == 'USDT':
                bal = float(a['walletBalance'])
                state['account_balance'] = bal
                return bal
    except:
        pass
    return state.get('account_balance', 0.0)


# ── 交易数据库（记录每笔交易用于复盘） ──

def load_trade_db():
    try:
        with open(TRADE_DB) as f:
            return json.load(f)
    except:
        return {'trades': [], 'total_trades': 0, 'wins': 0, 'losses': 0,
                'total_pnl': 0.0, 'total_fees': 0.0,
                'losing_streak': 0, 'short_losing_streak': 0, 'long_losing_streak': 0}


def save_trade_db(db):
    with open(TRADE_DB, 'w') as f:
        json.dump(db, f, indent=2)


async def record_trade_open(sym, side, entry, qty, reason, factors=None):
    db = load_trade_db()
    rec = {
        'sym': sym, 'side': side, 'entry': entry, 'qty': qty,
        'margin': qty * entry / LEVERAGE,
        'entry_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'reason': reason
    }
    if factors:
        rec['factors'] = factors  # P4 策略标签
    db.setdefault('open_trades', {})[sym] = rec
    save_trade_db(db)
    # ── 开仓数据写入Redis ──
    try:
        if REDIS_CLIENT:
            redis_put(f'open:{sym.lower()}', json.dumps(rec), 86400)
    except:
        pass


async def record_trade_close(sym, exit_reason, exit_price=None):
    """平仓记录：计算盈亏，更新统计"""
    db = load_trade_db()
    ot = db.get('open_trades', {}).pop(sym, None)
    if not ot:
        return  # 没有开仓记录（旧仓位或手动开的）
    if exit_price is None:
        exit_price = ot['entry']  # 用入场价兜底
    # 安全校验：出场价<=0说明未真实成交
    if exit_price <= 0:
        print(f"  ⚠️ record_trade_close拒绝: {sym} exit_price={exit_price} <=0")
        return
    
    side = ot['side']
    qty = ot['qty']
    entry = ot['entry']
    margin = qty * entry / LEVERAGE
    
    if side == 'LONG':
        pnl = (exit_price - entry) * qty
    else:
        pnl = (entry - exit_price) * qty
    
    pnl_pct = pnl / margin * 100 if margin > 0 else 0
    hold_sec = (datetime.utcnow() - datetime.strptime(
        ot['entry_time'], '%Y-%m-%d %H:%M UTC')).total_seconds()
    
    trade = {
        'sym': sym, 'side': side, 'entry': entry, 'exit': exit_price,
        'qty': qty, 'margin': round(margin, 2),
        'pnl': round(pnl, 2), 'pnl_pct': round(pnl_pct, 2),
        'hold_min': round(hold_sec / 60, 1),
        'entry_time': ot['entry_time'],
        'exit_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'exit_reason': exit_reason,
        'entry_reason': ot.get('reason', ''),
        'factors': ot.get('factors', {})  # P4 策略标签
    }
    db.setdefault('trades', []).append(trade)
    db['total_trades'] = db.get('total_trades', 0) + 1
    if pnl > 0:
        db['wins'] = db.get('wins', 0) + 1
        db['losing_streak'] = 0  # 赢了重置连败计数
    else:
        db['losses'] = db.get('losses', 0) + 1
        db['losing_streak'] = db.get('losing_streak', 0) + 1
    db['total_pnl'] = round(db.get('total_pnl', 0) + pnl, 2)
    save_trade_db(db)
    
    # ── 同步写入Redis（与record_trade()保持一致） ──
    try:
        if REDIS_CLIENT:
            REDIS_CLIENT.set(f'{RK_TRADE}{sym.lower()}', json.dumps(trade), ex=86400)
            REDIS_CLIENT.lpush('trades:history', json.dumps(trade))
            REDIS_CLIENT.ltrim('trades:history', 0, 199)
            REDIS_CLIENT.delete(f'{RK_POS}{sym.lower()}')
            today_key = 'stats:pnl:' + datetime.utcnow().strftime('%Y%m%d')
            REDIS_CLIENT.incrbyfloat(today_key, round(pnl, 2))
            REDIS_CLIENT.expire(today_key, 86400 * 7)
    except:
        pass
    
    # ══ 亏损加冷却 ══
    if pnl < 0:
        loss_abs = abs(pnl)
        if loss_abs >= 0.50: cool = 21600       # 6小时
        elif loss_abs >= 0.10: cool = 10800      # 3小时
        else: cool = 3600                         # 1小时
        set_cooldown(sym.lower(), cool)
        print(f"  🧊 {sym} 冷却{cool//3600}小时（亏损${loss_abs:.2f}）")
    
    print(f"  📝 交易记录: {sym} {'✅' if pnl>0 else '❌'} ${pnl:+.2f} ({pnl_pct:+.1f}%) | {exit_reason}")
    
    # 推送平仓事件
    push_event('close', {
        'sym': sym, 'side': side, 'qty': round(qty, 2),
        'entry': round(entry, 4), 'exit': round(exit_price, 4),
        'pnl': round(pnl, 2), 'pnl_pct': round(pnl_pct, 1),
        'hold_min': round(hold_sec / 60, 1),
        'reason': exit_reason
    })
    
    # 连败保护：≥MAX_LOSS_STREAK笔亏损自动暂停
    streak = db.get('losing_streak', 0)
    if streak >= MAX_LOSS_STREAK:
        resume_at = time.time() + PAUSE_COOLDOWN
        state['paused'] = True
        state['pause_reason'] = f"连续{streak}笔亏损"
        state['pause_resume_at'] = resume_at
        # 输出复盘分析
        print(f"\n  🛑 连败保护触发: 连续{streak}笔亏损，暂停交易{PAUSE_COOLDOWN//60}分钟")
        print(f"  📋 最近{(db.get('total_trades',0))}笔交易回顾:")
        for t in db.get('trades', [])[-5:]:
            flag = '✅' if t['pnl'] > 0 else '❌'
            print(f"    {flag} {t['sym']} {t['side']} ${t['pnl']:+.2f}({t['pnl_pct']:+.1f}%) {t['exit_reason']}")
        print(f"  ⏳ {PAUSE_COOLDOWN//60}分钟后自动恢复\n")



async def check_btc_breaker():
    """大盘断路器：检查BTC 15m状态，返回 (long_allowed, short_allowed)"""
    try:
        dq = state['kline_data'].get('btcusdt')
        if dq is None or len(dq) < 20:
            # WS数据不够，从REST拉
            r = await http_get_public('/fapi/v1/klines?symbol=BTCUSDT&interval=15m&limit=25')
            if not isinstance(r, list) or len(r) < 20:
                return True, True
            closes = [float(k[4]) for k in r]
            highs = [float(k[2]) for k in r]
            lows = [float(k[3]) for k in r]
        else:
            klines = [k for k in dq if k.get('final', True)]
            closes = [k['c'] for k in klines]
            highs = [k['h'] for k in klines]
            lows = [k['l'] for k in klines]
        
        price = closes[-1]
        ma14 = calc_sma(closes, MA_LONG)
        rsi = calc_rsi(closes, RSI_PERIOD)
        adx, pdi, mdi = calc_adx(highs, lows, closes, ADX_PERIOD)
        
        # 基础条件
        long_ok = not (rsi < 35 or price < ma14)
        short_ok = not (rsi > 65 or price > ma14)
        
        # BTC极端超卖(<20)：不做空（反弹期追空必死）
        # BTC极端超买(>80)：不做多（回调期追多必死）
        if rsi < 20:
            short_ok = False
        if rsi > 80:
            long_ok = False
        
        # BTC无趋势时不硬锁，让个币自己判断ADX
        # BTC有强趋势时不得逆势操作
        if adx >= ADX_MIN:
            if pdi > mdi:  # BTC上升趋势 → 不做空
                short_ok = False
            elif mdi > pdi:  # BTC下降趋势 → 不做多
                long_ok = False
        
        # ⭐ 新增：检查1h BTC趋势（修复空单连败根因）
        # 15m ADX可能<25（短期回调），但1h ADX可能>25（强趋势）
        # 当1h趋势明确时，15m回调不可逆势做空/做多
        try:
            r1h = await http_get_public('/fapi/v1/klines?symbol=BTCUSDT&interval=1h&limit=30')
            if isinstance(r1h, list) and len(r1h) >= 25:
                c1h = [float(k[4]) for k in r1h]
                h1h = [float(k[2]) for k in r1h]
                l1h = [float(k[3]) for k in r1h]
                adx1h, pdi1h, mdi1h = calc_adx(h1h, l1h, c1h, ADX_PERIOD)
                if adx1h >= ADX_MIN:  # 1h有强趋势
                    if pdi1h > mdi1h:  # 1h上升趋势 → 不做空
                        if short_ok:
                            print(f"  🔒 BTC 1h强上升趋势(ADX={adx1h:.1f}) 禁止做空")
                        short_ok = False
                    elif mdi1h > pdi1h:  # 1h下降趋势 → 不做多
                        if long_ok:
                            print(f"  🔒 BTC 1h强下降趋势(ADX={adx1h:.1f}) 禁止做多")
                        long_ok = False
        except:
            pass
        
        if not long_ok and not short_ok:
            print(f"  🔒 BTC大盘锁定: ADX={adx} RSI={rsi:.0f} 价/MA14={price/ma14:.4f}")
        return long_ok, short_ok
    except:
        return True, True


# ═══════════════════════════════════════════
# 指标函数
# ═══════════════════════════════════════════

def calc_sma(values, period):
    if not values: return 0
    return sum(values[-period:]) / period if len(values) >= period else sum(values) / len(values)


def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return 50
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0: return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_bb(closes, period=20, std_mult=2):
    if len(closes) < period:
        middle = sum(closes) / len(closes) if closes else 0
        return middle, middle, middle, 0, 0.5
    middle = sum(closes[-period:]) / period
    s = math.sqrt(sum((v - middle)**2 for v in closes[-period:]) / period)
    upper = middle + std_mult * s
    lower = middle - std_mult * s
    bw = (upper - lower) / middle if middle else 0
    pb = (closes[-1] - lower) / (upper - lower) if upper != lower else 0.5
    return upper, middle, lower, bw, pb


def calc_atr(highs, lows, closes, period=14):
    if len(highs) < period + 1: return 0
    trs = []
    for i in range(1, len(highs)):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        trs.append(max(hl, hc, lc))
    if len(trs) < period: return sum(trs) / len(trs) if trs else 0
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return atr


def calc_cross_score(is_long, ma7, ma14, price, rsi, bb_pb, vol, rr, atr, adx=0, vol_norm=50000000,
                     funding_rate=0, trend_1h=0):
    """多因子断面评分: 返回 (composite_score, factor_dict)
    
    新增因子:
    - funding_rate: 资金费率(8h), 负=多头占优, 正=空头占优
    - trend_1h: 1h大周期趋势方向, 1=bull, -1=bear, 0=flat
    """
    # 1. 趋势强度 (0~1)
    if is_long:
        trend_score = min(max((ma7 - ma14) / price * 100, 0), 3) / 3
    else:
        trend_score = min(max((ma14 - ma7) / price * 100, 0), 3) / 3

    # 2. RSI适中度 (0~1): 强趋势(ADX≥30)放宽上限到80
    if is_long:
        rsi_max = 80 if adx >= 30 else 72
        rsi_score = 1 - abs(rsi - 52) / 40 if 35 <= rsi <= rsi_max else 0
    else:
        rsi_score = 1 - abs(rsi - 44) / 32 if 28 <= rsi <= 60 else 0
    rsi_score = max(0, min(1, rsi_score))

    # 3. 布林位置 (0~1): 不超轨道外
    bb_score = max(0, 1 - abs(bb_pb - 0.5)) if 0 <= bb_pb <= 1.5 else 0

    # 4. 成交量 (0~1)
    vol_score = min(vol / vol_norm, 1)

    # 5. 盈亏比 (0~1)
    rr_score = min(rr / 3.0, 1)

    # 6. 波动率 (0~1): 适中最好，剔除极端低波和极端高波
    atr_pct = atr / price if price > 0 else 0
    if atr_pct < 0.005:
        atr_score = atr_pct / 0.005  # 0~0.5% 线性上升
    elif atr_pct < 0.015:
        atr_score = 0.3 + 0.7 * (atr_pct - 0.005) / 0.01  # 0.5%~1.5% 斜坡升
    elif atr_pct < 0.025:
        atr_score = 1.0  # 1.5%~2.5% 黄金区域
    elif atr_pct < 0.04:
        atr_score = 1.0 - (atr_pct - 0.025) / 0.015  # 2.5%~4% 斜坡降
    else:
        atr_score = 0.0  # >4% 零分

    # 7. 1h跨周期动量 (0~1): 大周期与交易方向一致=加分
    # trend_1h: 1=bull, -1=bear, 0=flat
    # 做多时trend_1h=1 → 高分；做空时trend_1h=-1 → 高分
    if is_long:
        t1h_score = max(0, trend_1h)  # 1→1.0, 0/-1→0
    else:
        t1h_score = max(0, -trend_1h)  # -1→1.0, 0/1→0

    # 8. 资金费率 (0~1): FR绝对值越小越好，偏向有利方向更好
    # 做多：负FR(多头占优)=好，正FR(多头过热)=差
    # 做空：正FR(空头占优)=好，负FR(空头过热)=差
    fr_abs = abs(funding_rate)
    if is_long:
        if funding_rate <= 0:
            # 负或零费率：多头不拥挤，高分
            fr_score = 1 - min(fr_abs / 0.002, 1)
        else:
            # 正费率：多头过热，降分
            fr_score = max(0, 1 - funding_rate / 0.001 * 2)
    else:
        if funding_rate >= 0:
            # 正或零费率：空头不拥挤，高分
            fr_score = 1 - min(fr_abs / 0.002, 1)
        else:
            # 负费率：空头过热，降分
            fr_score = max(0, 1 + funding_rate / 0.001 * 2)
    fr_score = max(0, min(1, fr_score))

    score = (SCORE_TREND_WT * trend_score +
             SCORE_RSI_WT * rsi_score +
             SCORE_VOL_WT * vol_score +
             SCORE_RR_WT * rr_score +
             SCORE_ATR_WT * atr_score +
             SCORE_FR_WT * fr_score +
             SCORE_1H_WT * t1h_score)

    factors = {
        'trend': round(trend_score, 3),
        'rsi': round(rsi_score, 3),
        'vol': round(vol_score, 3),
        'rr': round(rr_score, 3),
        'atr': round(atr_score, 3),
        'bb': round(bb_score, 3),
        'fr': round(fr_score, 3),
        't1h': round(t1h_score, 3),
    }
    return round(score, 3), factors


def calc_liq_price(entry, qty, leverage=5, is_long=True):
    mm_rate = 0.005
    if is_long:
        return round(entry * (1 - 1/leverage + mm_rate) / (1 - mm_rate), 6)
    else:
        return round(entry * (1 + 1/leverage - mm_rate) / (1 + mm_rate), 6)


# ═══════════════════════════════════════════
# 交易复盘
# ═══════════════════════════════════════════

def load_trades():
    try:
        with open(TRADE_LOG) as f:
            return json.load(f)
    except:
        return []


def save_trades(trades):
    # 只保留最近200条
    tmp = TRADE_LOG + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(trades[-200:], f)
        f.flush()
        os.fsync(f.fileno())  # 强制磁盘写入
    os.replace(tmp, TRADE_LOG)  # 原子替换，崩溃不损坏原文件


# ── 策略分类（哪种信号成功率多少） ──

SETUP_TYPES = {
    'LONG_STD':     '做多-标准(MA7回踩+RSI适中)',
    'LONG_STRONG':  '做多-强趋势(ADX≥30扩RSI上界)',
    'LONG_WEAK':    '做多-弱趋势(ADX20-25)',
    'SHORT_STD':    '做空-标准(MA14反弹+RSI≤50)',
    'SHORT_STRONG': '做空-强趋势(ADX≥30)',
    'SHORT_WEAK':   '做空-弱趋势(ADX20-25)',
}

def categorize_setup(side, adx, rsi_14, ma7, ma14, price):
    """根据入场时技术指标判断属于哪种策略类型"""
    if adx >= 30:
        return f'{side}_STRONG'
    elif adx >= 20:
        return f'{side}_WEAK'
    else:
        return f'{side}_STD'

def show_setup_stats(setup=None, detail=False):
    """展示各策略类型的历史胜率"""
    trades = load_trades()
    if not trades:
        return "暂无交易记录"

    from collections import defaultdict
    stats = defaultdict(lambda: {'n': 0, 'wins': 0, 'pnl': 0.0, 'fees': 0.0})
    by_setup_sym = defaultdict(lambda: defaultdict(lambda: {'n': 0, 'wins': 0, 'pnl': 0.0}))

    for t in trades:
        s = t.get('setup', 'UNKNOWN')
        if setup and s != setup:
            continue
        stats[s]['n'] += 1
        if t['pnl'] > 0:
            stats[s]['wins'] += 1
        stats[s]['pnl'] += t['pnl']
        stats[s]['fees'] += t.get('fee', 0)
        if detail:
            sym = t.get('sym', '?')
            by_setup_sym[s][sym]['n'] += 1
            by_setup_sym[s][sym]['wins'] += 1 if t['pnl'] > 0 else 0
            by_setup_sym[s][sym]['pnl'] += t['pnl']

    lines = []
    if setup:
        s = stats.get(setup)
        if not s or s['n'] == 0:
            return f"策略 [{setup}] 无历史交易记录"
        wr = s['wins']/s['n']*100
        label = SETUP_TYPES.get(setup, setup)
        lines.append(f"📊 [{label}] 共{s['n']}笔 | 胜率{wr:.1f}% ({s['wins']}胜/{s['n']-s['wins']}负)")
        lines.append(f"   总PnL: ${s['pnl']:+.2f} | 手续费: ${s['fees']:.2f}")
        avg_w = s['pnl']/s['wins'] if s['wins'] > 0 else 0
        avg_l = s['pnl']/(s['n']-s['wins']) if s['n']-s['wins'] > 0 else 0
        lines.append(f"   平均盈利: ${avg_w:+.2f} | 平均亏损: ${avg_l:.2f}")
        if detail and by_setup_sym.get(setup):
            lines.append(f"   币种分布:")
            for sym in sorted(by_setup_sym[setup].keys()):
                sd = by_setup_sym[setup][sym]
                swr = sd['wins']/sd['n']*100 if sd['n'] else 0
                lines.append(f"     {sym:12s} {sd['n']}笔 胜率{swr:.0f}% PnL${sd['pnl']:+.2f}")
    else:
        lines.append("📊 各策略胜率统计:")
        for s in sorted(stats.keys()):
            sd = stats[s]
            wr = sd['wins']/sd['n']*100 if sd['n'] else 0
            label = SETUP_TYPES.get(s, s)
            lines.append(f"  {label:20s} {sd['n']:3d}笔 胜率{wr:5.1f}% 总PnL${sd['pnl']:+7.2f}")

    return '\n'.join(lines)

# ── 通用平仓执行器：市价平仓 + 用实际成交价算PnL ──
async def close_position_market(sym_upper, amt, reason_tag):
    """市价平仓并返回 (success, fill_price, actual_pnl, response)
    
    用订单返回的avgPrice算实际PnL，避免标记价估算偏差（滑点导致数据不准）
    """
    cs = 'SELL' if amt > 0 else 'BUY'
    cp = 'LONG' if amt > 0 else 'SHORT'
    d = await http_post('/fapi/v1/order', {
        'symbol': sym_upper, 'side': cs, 'positionSide': cp,
        'type': 'MARKET', 'quantity': str(abs(amt))
    })
    if 'orderId' not in d:
        return False, 0, 0, d
    # 用实际成交均价
    fill_price = float(d.get('avgPrice', 0))
    if fill_price <= 0:
        # cumQuote / cumQty 反推
        cum_qty = float(d.get('executedQty', 0))
        cum_quote = float(d.get('cumQuote', 0))
        if cum_qty <= 0:
            return False, 0, 0, d  # 实际未成交（另一实例已平或错误）
        fill_price = cum_quote / cum_qty
    return True, fill_price, 0, d


def record_trade(sym, side, entry, exit_price, qty, reason, setup='UNKNOWN'):
    """记录一笔平的交易（带连败计数+暂停逻辑）"""
    # 安全校验：出场价<=0说明未真实成交，拒绝记录
    if exit_price <= 0:
        print(f"  ⚠️ record_trade拒绝: {sym} exit_price={exit_price} <=0, 未真实成交")
        return
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    margin_used = entry * abs(qty) / LEVERAGE
    pnl = (exit_price - entry) * qty if side == 'LONG' else (entry - exit_price) * abs(qty)
    pnl_pct = pnl / margin_used * 100 if margin_used > 0 else 0
    # 如果setup未指定，从持仓state中读取（退出函数调用时保留策略类型）
    if setup == 'UNKNOWN':
        pos_info = state.get('positions', {}).get(sym.lower(), {})
        if isinstance(pos_info, dict):
            setup = pos_info.get('setup', 'UNKNOWN')
    # P4: 策略标签 — 读取开仓时存储的多因子评分
    factors = {}
    pos_info = state.get('positions', {}).get(sym.lower(), {})
    if isinstance(pos_info, dict):
        factors = pos_info.get('factors', {})
    trades = load_trades()
    trade = {
        'ts': now, 'sym': sym, 'side': side,
        'entry': entry, 'exit': exit_price, 'qty': abs(qty),
        'pnl': round(pnl, 2), 'pnl_pct': round(pnl_pct, 2),
        'margin': round(margin_used, 2), 'reason': reason,
        'setup': setup,
    }
    if factors:
        trade['factors'] = factors  # P4: 策略因子标签
    trades.append(trade)
    save_trades(trades)
    # ── 写入Redis（供AI查询实时交易数据） ──
    try:
        if REDIS_CLIENT:
            # 写入该币种最新平仓记录（覆盖）
            redis_put(f'{RK_TRADE}{sym.lower()}', json.dumps(trade), 86400)
            # 追加到历史列表（保留最近200条）
            REDIS_CLIENT.lpush('trades:history', json.dumps(trade))
            REDIS_CLIENT.ltrim('trades:history', 0, 199)
            # 清除对应持仓数据（已平仓）
            redis_put(f'{RK_POS}{sym.lower()}', '', 1)
            # 更新统计：今日PNL累计
            today_d = datetime.utcnow().strftime('%Y%m%d')
            today_key = 'stats:pnl:' + today_d
            REDIS_CLIENT.incrbyfloat(today_key, round(pnl, 2))
            REDIS_CLIENT.expire(today_key, 86400 * 7)
    except:
        pass
    print(f"  📝 记录: {sym} {side} | 盈亏${pnl:+.2f} ({pnl_pct:+.1f}%) | {reason}")
    
    # ══ 亏损加冷却：防止同一币种反复进场 ══
    if pnl < 0:
        # 亏损越大冷却越长：亏损<$0.10=1h, $0.10-$0.50=3h, >$0.50=6h
        loss_abs = abs(pnl)
        if loss_abs >= 0.50:
            cool = 21600  # 6小时
        elif loss_abs >= 0.10:
            cool = 10800  # 3小时
        else:
            cool = 3600   # 1小时
        set_cooldown(sym.lower(), cool)
        print(f"  🧊 {sym} 冷却{cool//3600}小时（亏损${loss_abs:.2f}）")
    
    # ══ 连败保护：更新losing_streak计数，≥MAX_LOSS_STREAK自动暂停 ══
    # 方向级连败追踪：做空连败3笔只暂停做空，做多同理
    db = load_trade_db()
    side_key = f'{side.lower()}_losing_streak'
    if pnl > 0:
        db['losing_streak'] = 0  # 赢了重置全局连败计数
        db[side_key] = 0         # 赢了重置方向级连败
    else:
        db['losing_streak'] = db.get('losing_streak', 0) + 1
        db[side_key] = db.get(side_key, 0) + 1
    save_trade_db(db)

    # 全局连败暂停（所有方向暂停）
    streak = db.get('losing_streak', 0)
    if streak >= MAX_LOSS_STREAK:
        resume_at = time.time() + PAUSE_COOLDOWN
        state['paused'] = True
        state['pause_reason'] = f"连续{streak}笔亏损"
        state['pause_resume_at'] = resume_at
        # 持久化暂停到trade_db（抗gateway重启）
        db['pause_resume_at'] = resume_at
        db['pause_reason'] = state['pause_reason']
        save_trade_db(db)
        print(f"\n  🛑 全局连败保护触发: 连续{streak}笔亏损，暂停交易{PAUSE_COOLDOWN//60}分钟")
        for t in trades[-5:]:
            flag = '✅' if t['pnl'] > 0 else '❌'
            print(f"    {flag} {t['sym']} {t['side']} ${t['pnl']:+.2f}({t['pnl_pct']:+.1f}%) {t.get('reason','?')}")
        print(f"  ⏳ {PAUSE_COOLDOWN//60}分钟后自动恢复\n")
    
    # 方向级连败暂停（只暂停该方向）
    dir_streak = db.get(side_key, 0)
    if dir_streak >= DIR_MAX_LOSS_STREAK:
        dir_pause_key = f'{side.lower()}_paused'
        dir_pause_until = time.time() + DIR_PAUSE_COOLDOWN
        state[dir_pause_key] = True
        state[f'{dir_pause_key}_until'] = dir_pause_until
        state[f'{dir_pause_key}_reason'] = f"做{'多' if side=='LONG' else '空'}连续{dir_streak}笔亏损"
        # 持久化（抗gateway重启）
        db[dir_pause_key] = True
        db[f'{dir_pause_key}_until'] = dir_pause_until
        db[f'{dir_pause_key}_reason'] = state[f'{dir_pause_key}_reason']
        save_trade_db(db)
        print(f"  🔒 方向级暂停: 做{'多' if side=='LONG' else '空'}连续{dir_streak}笔亏损，暂停{side}方向{DIR_PAUSE_COOLDOWN//3600}小时")


def calc_trade_stats():
    """计算交易统计"""
    trades = load_trades()
    if not trades:
        return "暂无交易记录"
    n = len(trades)
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] < 0]
    win_rate = len(wins) / n * 100 if n else 0
    total_pnl = sum(t['pnl'] for t in trades)
    avg_win = sum(t['pnl'] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t['pnl'] for t in losses) / len(losses) if losses else 0
    best = max(trades, key=lambda t: t['pnl'])
    worst = min(trades, key=lambda t: t['pnl'])
    
    # 各币种统计
    by_sym = {}
    for t in trades:
        by_sym.setdefault(t['sym'], {'trades': 0, 'wins': 0, 'pnl': 0, 'reasons': {}})
        by_sym[t['sym']]['trades'] += 1
        by_sym[t['sym']]['pnl'] += t['pnl']
        if t['pnl'] > 0: by_sym[t['sym']]['wins'] += 1
        r = t.get('reason', '?')
        by_sym[t['sym']]['reasons'][r] = by_sym[t['sym']]['reasons'].get(r, 0) + 1
    
    lines = [
        f"📊 交易统计 (共{n}笔)",
        f"  胜率: {win_rate:.1f}% ({len(wins)}胜/{len(losses)}负)",
        f"  总盈亏: ${total_pnl:+.2f}",
        f"  平均盈利: ${avg_win:+.2f} | 平均亏损: ${avg_loss:.2f}",
        f"  最佳: {best['sym']} ${best['pnl']:+.2f} ({best.get('reason','?')})",
        f"  最差: {worst['sym']} ${worst['pnl']:.2f} ({worst.get('reason','?')})",
    ]
    # 各币种详情
    for sym, s in sorted(by_sym.items(), key=lambda x: x[1]['pnl']):
        wr = s['wins'] / s['trades'] * 100
        lines.append(f"  {sym}: {s['trades']}笔 胜率{wr:.0f}% 盈亏${s['pnl']:+.2f}")
    return '\n'.join(lines)


# ═══════════════════════════════════════════
# 指标函数
# ═══════════════════════════════════════════

def calc_adx(highs, lows, closes, period=14):
    """计算ADX: 返回adx, +di, -di"""
    n = len(highs)
    if n < period + 1: return 0, 0, 0
    tr_list, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        tr_list.append(max(hl, hc, lc))
        up_move = highs[i] - highs[i-1]
        down_move = lows[i-1] - lows[i]
        p_dm = up_move if up_move > down_move and up_move > 0 else 0
        m_dm = down_move if down_move > up_move and down_move > 0 else 0
        plus_dm.append(p_dm)
        minus_dm.append(m_dm)
    if len(tr_list) < period: return 0, 0, 0
    # 初始SMA
    atr_ = sum(tr_list[:period]) / period
    pdi_ = sum(plus_dm[:period]) / period
    mdi_ = sum(minus_dm[:period]) / period
    # 遍历平滑
    dx_list = []
    for i in range(period, len(tr_list)):
        atr_ = (atr_ * (period - 1) + tr_list[i]) / period
        pdi_ = (pdi_ * (period - 1) + plus_dm[i]) / period
        mdi_ = (mdi_ * (period - 1) + minus_dm[i]) / period
        pdi_pct = pdi_ / atr_ * 100 if atr_ > 0 else 0
        mdi_pct = mdi_ / atr_ * 100 if atr_ > 0 else 0
        dx = abs(pdi_pct - mdi_pct) / (pdi_pct + mdi_pct) * 100 if (pdi_pct + mdi_pct) > 0 else 0
        dx_list.append(dx)
    # ADX = SMA of DX
    adx = sum(dx_list[:period]) / period
    return round(adx, 2), round(pdi_pct, 2), round(mdi_pct, 2)


def calc_fib_levels(highs, lows, price, is_long=True):
    n = len(highs)
    if n < 10: return []
    if is_long:
        # 做多：在最低价序列中找摆动低点 → 往后找摆动高点
        li = lows.index(min(lows))
        al = highs[li:]
        if not al: return []
        hi = li + al.index(max(al))
        L, H = lows[li], highs[hi]
    else:
        # 做空：在最高价序列中找摆动高点 → 往后找摆动低点
        hi = highs.index(max(highs))
        ah = lows[hi:]
        if not ah: return []
        li = hi + ah.index(min(ah))
        H, L = highs[hi], lows[li]
    if H - L == 0: return []
    d = H - L
    levels = []
    for f in FIB_LEVELS:
        lvl = (H - d * f) if is_long else (L + d * f)
        levels.append((lvl, f'{f*100:.1f}%'))
    levels.sort(key=lambda x: abs(x[0] - price))
    return levels


def find_pivot_levels(highs, lows, window=None):
    if window is None: window = PIVOT_WINDOW
    pivots = {'resistances': [], 'supports': []}
    for i in range(window, len(highs) - window):
        if all(highs[i] >= highs[j] for j in range(i-window, i+window+1) if j != i):
            pivots['resistances'].append(highs[i])
        if all(lows[i] <= lows[j] for j in range(i-window, i+window+1) if j != i):
            pivots['supports'].append(lows[i])
    return pivots


def find_best_fib_sr(price, fib_levels, pivots, bb_u, bb_l):
    fib_sup, fib_res = price * 0.98, price * 1.02
    sup_str, res_str = 'default', 'default'
    sup_q, res_q = 0, 0
    for lvl, name in fib_levels:
        if lvl < price and lvl > fib_sup:
            fib_sup, sup_str = lvl, f'Fib{name}'
        elif lvl > price and lvl < fib_res:
            fib_res, res_str = lvl, f'Fib{name}'
    for r in pivots.get('resistances', []):
        if price < r < fib_res:
            fib_res, res_str, res_q = r, 'Pivot', res_q + 1
    for s in pivots.get('supports', []):
        if price > s > fib_sup:
            fib_sup, sup_str, sup_q = s, 'Pivot', sup_q + 1
    if bb_l < price and bb_l > fib_sup: fib_sup, sup_str = bb_l, 'BB'
    if bb_u > price and bb_u < fib_res: fib_res, res_str = bb_u, 'BB'
    return fib_sup, fib_res, sup_str, res_str, sup_q, res_q


# ═══════════════════════════════════════════
# 条件单管理器
# ═══════════════════════════════════════════

async def save_algo_ids(sym, tp_id, sl_id):
    state['algo_orders'][sym] = {'tp': tp_id, 'sl': sl_id, 'ts': time.time()}


async def clean_orphan_algos(sym):
    if sym not in state['algo_orders']: return
    algo = state['algo_orders'][sym]
    if time.time() - algo.get('ts', 0) > 86400: return
    for aid in [algo.get('tp'), algo.get('sl')]:
        if aid:
            r = await http_delete('/fapi/v1/algoOrder', {'symbol': sym, 'algoId': str(aid)})
            if r.get('code') == '200' or 'algoId' in r:
                print(f"  🧹 残单清理: {sym} algoId={aid}")
    del state['algo_orders'][sym]


async def verify_sl_tp(bal):
    """检查所有持仓是否都有条件单保护（防裸仓），裸仓就报警并补挂"""
    if not state['positions']:
        return
    # 获取当前所有活动条件单
    try:
        r = await http_get('/fapi/v1/openAlgoOrders')
        if not isinstance(r, list):
            return
    except:
        return
    active_algos = {}
    for o in r:
        s = o.get('symbol', '').upper()
        active_algos.setdefault(s, [])
        active_algos[s].append(o)
    for sym_lower, p in list(state['positions'].items()):
        sym_upper = sym_lower.upper()
        amt = abs(float(p.get('positionAmt', 0)))
        if amt <= 0:
            continue
        pos_algos = active_algos.get(sym_upper, [])
        has_sl = any(o.get('type') == 'STOP_MARKET' for o in pos_algos)
        has_tp = any(o.get('type') == 'TAKE_PROFIT_MARKET' for o in pos_algos)
        if not has_sl or not has_tp:
            entry = float(p.get('entryPrice', 0))
            side = 'LONG' if float(p.get('positionAmt', 0)) > 0 else 'SHORT'
            if entry <= 0:
                continue
            print(f"  ⚠️ {sym_upper} 裸仓! SL={has_sl} TP={has_tp} 正在补挂条件单...")
            # 计算ATR宽止损（防插针）
            try:
                bars = await http_get_public(f'/fapi/v1/klines?symbol={sym_upper}&interval=15m&limit=20')
                if isinstance(bars, list) and len(bars) >= 14:
                    h = [float(b[2]) for b in bars]
                    l = [float(b[3]) for b in bars]
                    c = [float(b[4]) for b in bars]
                    atr_val = calc_atr(h, l, c)
                    atr_sl = max(atr_val * ATR_MULT, entry * ATR_MIN_PCT)
                    atr_sl = min(atr_sl, entry * ATR_MAX_PCT)
                    if side == 'LONG':
                        wider_sl = entry - atr_sl
                    else:
                        wider_sl = entry + atr_sl
                    print(f"    📐 ATR宽止损=${wider_sl:.4f} (±{atr_sl/entry*100:.1f}%)")
                else:
                    wider_sl = None
            except:
                wider_sl = None
            await place_tp_sl_orders(sym_upper, side, entry, amt, wider_sl=wider_sl)
            # 再次验证
            try:
                r2 = await http_get('/fapi/v1/openAlgoOrders')
                if isinstance(r2, list):
                    for o2 in r2:
                        if o2.get('symbol', '').upper() == sym_upper:
                            if o2.get('type') == 'STOP_MARKET':
                                has_sl = True
                            if o2.get('type') == 'TAKE_PROFIT_MARKET':
                                has_tp = True
            except:
                pass
            if has_sl and has_tp:
                print(f"    ✅ {sym_upper} 条件单修复完成")
            else:
                print(f"    ❌ {sym_upper} 条件单修复失败！持仓不受保护！")


async def place_tp_sl_orders(sym, side, fill_p, fill_q, wider_sl=None, wider_tp=None):
    """挂±6%止盈止损条件单（用tickSize/stepSize保证精度，失败单独重试）
    
    wider_sl/wider_tp: ATR动态止损止盈价（防插针保护）
    """
    tp_side = 'SELL' if side == 'LONG' else 'BUY'
    pos_side = 'LONG' if side == 'LONG' else 'SHORT'
    
    # 获取精度
    tick = await get_price_tick(sym)
    step, _ = await get_step_size(sym)
    
    # ── 计算止损价（取更宽保护） ──
    # ±6%固定止损
    if side == 'LONG':
        sl_pct = fill_p * (1 - PCT_6)
        tp_pct = fill_p * (1 + PCT_6)
    else:
        sl_pct = fill_p * (1 + PCT_6)
        tp_pct = fill_p * (1 - PCT_6)
    
    # ATR宽止损（防插针）：取两者中更保险的一个
    if wider_sl is not None:
        if side == 'LONG':
            sl_raw = min(sl_pct, wider_sl)  # 更低=更远=更安全
        else:
            sl_raw = max(sl_pct, wider_sl)  # 更高=更远=更安全
    else:
        sl_raw = sl_pct
    
    # 止盈用ATR更优值
    if wider_tp is not None:
        if side == 'LONG':
            tp_raw = max(tp_pct, wider_tp)
        else:
            tp_raw = min(tp_pct, wider_tp)
    else:
        tp_raw = tp_pct
    
    # 用tickSize截断
    if tick > 0:
        sl_6pct = math.floor(sl_raw / tick) * tick
        tp_6pct = math.floor(tp_raw / tick) * tick
    else:
        sl_6pct, tp_6pct = sl_raw, tp_raw
    
    # 数量按stepSize截断
    if step > 0:
        qty_str = f'{math.floor(fill_q / step) * step:.{max(0, -int(math.log10(step)))}f}'
    else:
        qty_str = f'{fill_q:.1f}'
    
    print(f"  📐 ±6%({side}): SL=${sl_6pct} TP=${tp_6pct} qty={qty_str}")
    
    # 格式化触发价
    price_precision = max(0, -int(math.log10(tick))) if tick > 0 and tick < 1 else 2
    sl_str = f'{sl_6pct:.{price_precision}f}'
    tp_str = f'{tp_6pct:.{price_precision}f}'
    
    async def place_one(order_type, trigger_str):
        """挂单个条件单，优先closePosition，失败则用quantity"""
        params = {
            'symbol': sym, 'side': tp_side, 'positionSide': pos_side,
            'type': order_type, 'algoType': 'CONDITIONAL',
            'triggerPrice': trigger_str, 'closePosition': 'true',
            'workingType': 'MARK_PRICE'
        }
        data = await http_post('/fapi/v1/algoOrder', params)
        if 'algoId' in data:
            return data['algoId']
        # closePosition失败，改用quantity
        params.pop('closePosition')
        params['quantity'] = qty_str
        data = await http_post('/fapi/v1/algoOrder', params)
        return data.get('algoId', None)
    
    sl_id, tp_id = None, None
    # 第一轮：同时试两个
    sl_id = await place_one('STOP_MARKET', sl_str)
    tp_id = await place_one('TAKE_PROFIT_MARKET', tp_str)
    
    if sl_id and tp_id:
        await save_algo_ids(sym, tp_id, sl_id)
        print(f"  SL:✅ TP:✅")
        return True
    
    # 第二轮：只重试失败的
    if not sl_id:
        print(f"  ⚠️止损失败, 重试...")
        sl_id = await place_one('STOP_MARKET', sl_str)
    if not tp_id:
        print(f"  ⚠️止盈失败, 重试...")
        tp_id = await place_one('TAKE_PROFIT_MARKET', tp_str)
    
    if sl_id and tp_id:
        await save_algo_ids(sym, tp_id, sl_id)
        print(f"  SL:✅ TP:✅")
        return True
    
    print(f"  SL:{'✅' if sl_id else '❌'} TP:{'✅' if tp_id else '❌'}")
    if sl_id: await save_algo_ids(sym, tp_id or '', sl_id)
    if not sl_id: print(f"  🚨 {sym}止损挂单失败! 裸奔!")
    if not tp_id: print(f"  🚨 {sym}止盈挂单失败! 裸奔!")
    return False


# ═══════════════════════════════════════════
# 动态跟踪止损
# ═══════════════════════════════════════════

async def update_trailing_stop(sym, pnl_pct, entry, amt, cur_price):
    """回调跟踪止损：浮盈≥15%激活 → 最高价回调2%平仓"""
    is_long = amt > 0
    trail_key = f'trail_{sym}'
    
    # 当前最高/最低价追踪
    trail_data = state.setdefault('trail_peak', {}).get(trail_key)
    if trail_data is None:
        state.setdefault('trail_peak', {})[trail_key] = {
            'peak': cur_price,
            'activated': pnl_pct >= 15.0
        }
        trail_data = state['trail_peak'][trail_key]
    
    # 更新峰值
    if is_long and cur_price > trail_data['peak']:
        trail_data['peak'] = cur_price
    elif not is_long and cur_price < trail_data['peak']:
        trail_data['peak'] = cur_price
    
    # 未激活：浮盈≥15%才启动跟踪
    if pnl_pct < 15.0:
        trail_data['activated'] = False
        return False
    
    if not trail_data.get('activated'):
        trail_data['activated'] = True
        print(f"  🔄 激活跟踪止损: 峰值${trail_data['peak']:.4f} 回调2%平仓")
    
    # 已激活：检查是否回调2%从峰值
    CALLBACK_RATE = 0.02  # 2%回调触发
    if is_long:
        callback_price = trail_data['peak'] * (1 - CALLBACK_RATE)
        if cur_price <= callback_price:
            state.setdefault('exiting', set()).add(sym)
            print(f"  🎯 回调2%触发: 峰值${trail_data['peak']:.4f} → 现价${cur_price:.4f}")
            cs = 'SELL'; cp = 'LONG'
            d = await http_post('/fapi/v1/order', {
                'symbol': f'{sym}', 'side': cs, 'positionSide': cp,
                'type': 'MARKET', 'quantity': str(abs(amt))
            })
            if 'orderId' in d:
                pnl = (cur_price - entry) * abs(amt)
                print(f"  ✅ 已平仓 利润: ${pnl:+.2f}")
            record_trade(sym, 'LONG' if is_long else 'SHORT', entry, cur_price, amt, REASON_TRAIL)
            await clean_orphan_algos(sym)
            return True
        else:
            # 更新条件单止损价为回调价
            await clean_orphan_algos(sym)
            tick = await get_price_tick(sym) if 'get_price_tick' in dir() else 0.001
            sl_price = math.floor(callback_price / tick) * tick if tick > 0 else round(callback_price, 4)
            sl_side = 'SELL'; ps = 'LONG'
            sl_data = await http_post('/fapi/v1/algoOrder', {
                'symbol': sym, 'side': sl_side, 'positionSide': ps,
                'type': 'STOP_MARKET', 'algoType': 'CONDITIONAL',
                'triggerPrice': f'{sl_price:.4f}', 'quantity': f'{abs(amt):.1f}',
                'workingType': 'MARK_PRICE'
            })
            if 'algoId' in sl_data:
                await save_algo_ids(sym, '', sl_data['algoId'])
                print(f"  🔼 跟踪上移: 回调止损${sl_price:.4f} (峰值${trail_data['peak']:.4f})")
            return False
    else:  # 做空
        callback_price = trail_data['peak'] * (1 + CALLBACK_RATE)
        if cur_price >= callback_price:
            state.setdefault('exiting', set()).add(sym)
            print(f"  🎯 回调2%触发: 峰值${trail_data['peak']:.4f} → 现价${cur_price:.4f}")
            cs = 'BUY'; cp = 'SHORT'
            d = await http_post('/fapi/v1/order', {
                'symbol': f'{sym}', 'side': cs, 'positionSide': cp,
                'type': 'MARKET', 'quantity': str(abs(amt))
            })
            if 'orderId' in d:
                pnl = (entry - cur_price) * abs(amt)
                print(f"  ✅ 已平仓 利润: ${pnl:+.2f}")
            record_trade(sym, 'SHORT', entry, cur_price, amt, REASON_TRAIL)
            await clean_orphan_algos(sym)
            return True
        else:
            await clean_orphan_algos(sym)
            tick = await get_price_tick(sym) if 'get_price_tick' in dir() else 0.001
            sl_price = math.ceil(callback_price / tick) * tick if tick > 0 else round(callback_price, 4)
            sl_side = 'BUY'; ps = 'SHORT'
            sl_data = await http_post('/fapi/v1/algoOrder', {
                'symbol': sym, 'side': sl_side, 'positionSide': ps,
                'type': 'STOP_MARKET', 'algoType': 'CONDITIONAL',
                'triggerPrice': f'{sl_price:.4f}', 'quantity': f'{abs(amt):.1f}',
                'workingType': 'MARK_PRICE'
            })
            if 'algoId' in sl_data:
                await save_algo_ids(sym, '', sl_data['algoId'])
                print(f"  🔼 跟踪上移: 回调止损${sl_price:.4f} (峰值${trail_data['peak']:.4f})")
            return False
    
    return False


# ═══════════════════════════════════════════
# 市场扫描
# ═══════════════════════════════════════════

async def update_crash_bounce_flags(sym, low_24h, high_24h, chg_24h):
    if chg_24h <= CRASH_PCT_24H:
        state['crash_bounce'][sym] = {'low_24h': low_24h, 'high_24h': high_24h,
                                       'drop_pct': chg_24h, 'flagged_at': time.time()}
    elif sym in state['crash_bounce']:
        cb = state['crash_bounce'][sym]
        bounce = (high_24h - cb['low_24h']) / cb['low_24h'] * 100
        if bounce >= BOUNCE_MIN_PCT:
            cb['bounce_pct'] = bounce


# ═══ 多周期共振 + 资金费率过滤 ═══

async def check_coin_1h_trend(sym_upper, side):
    """1H大周期方向过滤（P1）— Redis优先，REST fallback。

    核心逻辑：
      - 做空：仅当 1H MA7 < MA14 空头排列 才允许
      - 做多：仅当 1H MA7 > MA14 多头排列 才允许
      - MA7≈MA14（差<0.1%）：不限制，按现有逻辑放行

    性能优化：data_collector 已通过WS预计算 trend_1h:{sym}
    写入Redis，本函数优先读Redis（零网络延迟）。
    仅Redis缺失时REST fallback。
    
    返回 True=放行, False=拦截
    """
    try:
        # ── 从Redis读取预计算趋势（毫秒级）──
        if REDIS_CLIENT:
            cached = REDIS_CLIENT.get(f'trend_1h:{sym_upper.lower()}')
            if cached:
                d = json.loads(cached)
                direction = d.get('direction', 'flat')
                if direction == 'flat':
                    return True  # MA7≈MA14 交缠状态，不限制方向
                if side == 'LONG' and direction != 'bull':
                    REDIS_CLIENT.incr('stats:p1_intercepts')
                    return False
                if side == 'SHORT' and direction != 'bear':
                    REDIS_CLIENT.incr('stats:p1_intercepts')
                    return False
                return True  # 方向一致，放行

        # ── Redis缺失 → REST fallback（启动后首次加载） ──
        r = await http_get_public(f'/fapi/v1/klines?symbol={sym_upper}&interval=1h&limit=30')
        if not isinstance(r, list) or len(r) < 25:
            return True  # 数据不足则放行
        c = [float(k[4]) for k in r]
        ma7_1h = sum(c[-7:]) / 7
        ma14_1h = sum(c[-14:]) / 14
        diff_pct = abs(ma7_1h - ma14_1h) / ma14_1h * 100
        if diff_pct < 0.1:
            return True  # MA7≈MA14 交缠状态，不限制方向
        if side == 'LONG' and ma7_1h <= ma14_1h:
            if REDIS_CLIENT:
                REDIS_CLIENT.incr('stats:p1_intercepts')
            return False
        if side == 'SHORT' and ma7_1h >= ma14_1h:
            if REDIS_CLIENT:
                REDIS_CLIENT.incr('stats:p1_intercepts')
            return False
        return True
    except:
        return True  # 异常则放行，不影响交易


async def get_trend_1h_direction(sym_lower):
    """从Redis读取1h趋势方向: 1=bull(多头排列), -1=bear(空头排列), 0=flat(交缠)
    
    data_collector已通过WS预计算 trend_1h:{sym} 写入Redis，零网络延迟。
    """
    try:
        if REDIS_CLIENT:
            cached = REDIS_CLIENT.get(f'trend_1h:{sym_lower}')
            if cached:
                d = json.loads(cached)
                direction = d.get('direction', 'flat')
                if direction == 'bull':
                    return 1.0
                elif direction == 'bear':
                    return -1.0
    except:
        pass
    return 0.0


async def fetch_funding_rate_map():
    """批量获取所有币种资金费率+标记价，写Redis缓存 + 返回 {sym_lower: rate}"""
    try:
        r = await http_get_public('/fapi/v1/premiumIndex')
        if not isinstance(r, list):
            return {}
        result = {}
        for x in r:
            sym = x.get('symbol', '').lower()
            if not sym: continue
            rate = float(x.get('lastFundingRate', 0))
            result[sym] = rate
            # 写Redis缓存（兜底：data_collector WS可能断开）
            if REDIS_CLIENT:
                try:
                    REDIS_CLIENT.set(f'{RK_FUNDING}{sym}', json.dumps({
                        'rate': rate,
                        'predicted': float(x.get('interestRate', 0)),  # premiumIndex没有predicted，用interestRate
                        'markPrice': float(x.get('markPrice', 0)),
                        'next_time': int(x.get('nextFundingTime', 0)),
                        'ts': time.time()
                    }), ex=3600)
                    # 标记价也写一份
                    REDIS_CLIENT.set(f'{RK_MARK}{sym}', json.dumps({
                        'markPrice': float(x.get('markPrice', 0)),
                        'indexPrice': float(x.get('indexPrice', 0)),
                        'ts': time.time()
                    }), ex=3600)
                except:
                    pass
        return result
    except:
        return {}


async def scan_candidates(bal, recovery_mode):
    """扫描市场找最佳交易机会"""
    tier, max_pos, risk_amount = get_tier_config(bal)
    if recovery_mode:
        risk_amount *= 0.5  # 回血模式减半风险
        min_rr = RR_RECOVERY
        scan_symbols = [s for s in state['kline_data'] if s in ('btcusdt', 'ethusdt')]
    else:
        min_rr = RR_MIN
        scan_symbols = list(state['kline_data'].keys())
    
    try:
        tickers_resp = await http_get_public('/fapi/v1/ticker/24hr')
        tickers = {t['symbol'].lower(): t for t in tickers_resp if isinstance(t, dict)}
    except:
        tickers = {}
    
    # 批量获取资金费率（用于过热币种过滤）
    funding_rates = await fetch_funding_rate_map()
    
    # 大盘断路器
    long_ok, short_ok = await check_btc_breaker()
    if not long_ok and not short_ok:
        print("  🔒 大盘异常, 双向锁定（允许独立强势币绕过）")
    
    # 方向级连败暂停（做空连败只暂停做空，做多同理）
    now = time.time()
    if state.get('short_paused', False):
        if now >= state.get('short_paused_until', 0):
            state['short_paused'] = False
            print("  ✅ 做空方向暂停自动解除")
        else:
            short_ok = False
    if state.get('long_paused', False):
        if now >= state.get('long_paused_until', 0):
            state['long_paused'] = False
            print("  ✅ 做多方向暂停自动解除")
        else:
            long_ok = False
    if not long_ok and not short_ok:
        print("  🔒 方向级暂停: 双向封锁")
        return None, 0

    # ──按9阶梯队检查仓位上限 ──
    pos_count = len(state['positions'])
    if pos_count >= max_pos:
        print(f"  🔒 已达仓位上限({pos_count}/{max_pos}) T{tier}")
        return None, 0
    
    # 计算可用保证金（减去已有持仓占用的）
    used_margin = 0
    for pos_sym, pdata in state['positions'].items():
        pa = abs(float(pdata.get('positionAmt', 0)))
        ep = float(pdata.get('entryPrice', 0))
        used_margin += pa * ep / LEVERAGE
    avail_margin = max(0, bal - used_margin)
    margin_per_pos_limit = bal / max_pos  # 均分: 每仓 = 余额 ÷ 最大仓位数
    margin_for_trade = min(avail_margin, margin_per_pos_limit)
    if margin_for_trade * LEVERAGE < 10:  # 最低名义价值$10
        print(f"  🔒 Tier{tier} 保证金不足(上限${margin_per_pos_limit:.2f}, 可用${avail_margin:.2f})")
        return None, 0
    
    best_trade = None
    scanned_count = 0
    candidates_list = []  # 断面收集: (score, trade_tuple)
    
    for sym_lower in scan_symbols:
        dq = state['kline_data'].get(sym_lower)
        if dq is None or len(dq) < 25: continue
        klines = [k for k in dq if k.get('final', True)]
        if len(klines) < 25: continue
        # 跳过近期失败的币种（避免重复重试）
        if sym_lower in state.get('failed_symbols', {}):
            if time.time() < state['failed_symbols'][sym_lower]:
                continue
            del state['failed_symbols'][sym_lower]
        # 跳过冷却中的币种（避免重复买入刚平仓的）
        if sym_lower in state.get('cooldown', {}):
            if time.time() < state['cooldown'][sym_lower]:
                continue
            del state['cooldown'][sym_lower]
        # 跳过已有持仓的币种（不加仓不同步建仓）
        if sym_lower in state.get('positions', {}):
            continue
        # 防REST同步延迟：刚开的币180秒内不再开（防API缓存清仓）
        fresh = state.get('fresh_open', {})
        if sym_lower in fresh:
            if time.time() - fresh[sym_lower] < 180:
                continue
            del fresh[sym_lower]
        scanned_count += 1
        # 🩺 诊断：记录前5个币的过滤原因
        diag_idx = state.setdefault('_diag_idx', 0)
        if diag_idx < 5:
            state['_diag_idx'] = diag_idx + 1
            state.setdefault('_diag_coin', None)
            state['_diag_coin'] = sym_lower
        
        # ⚡ 防重复：按时间戳去重（WS修复兜底）
        
        # ⚡ 防重复：按时间戳去重（WS修复兜底）
        seen_ts = set()
        unique_klines = []
        for k in klines:
            if k['t'] not in seen_ts:
                seen_ts.add(k['t'])
                unique_klines.append(k)
        klines = unique_klines
        
        closes = [k['c'] for k in klines]
        highs = [k['h'] for k in klines]
        lows = [k['l'] for k in klines]
        
        ticker = tickers.get(sym_lower, {})
        price = float(ticker.get('lastPrice', closes[-1])) if ticker else closes[-1]
        
        # ⚡ 数据新鲜度校验：实时价格与K线收盘价偏差超过2%则跳过（WS数据可能滞后）
        last_close = closes[-1]
        if price > 0 and last_close > 0:
            deviation = abs(price - last_close) / last_close * 100
            if deviation > 2.0:
                continue  # WS数据与实时价偏差过大，等下一个扫描周期同步
        vol = float(ticker.get('quoteVolume', 0)) if ticker else 0
        if vol < 2000000:
            if sym_lower == state.get('_diag_coin'): print(f"🩺 {sym_lower} 挡: vol={vol:.0f}<2M")
            continue
        chg_24h = float(ticker.get('priceChangePercent', 0)) if ticker else 0
        low_24h = float(ticker.get('lowPrice', price)) if ticker else price
        high_24h = float(ticker.get('highPrice', price)) if ticker else price
        await update_crash_bounce_flags(sym_lower, low_24h, high_24h, chg_24h)
        
        ma7 = calc_sma(closes, MA_SHORT)
        ma14 = calc_sma(closes, MA_LONG)
        rsi_14 = calc_rsi(closes, RSI_PERIOD)
        bb_u, bb_m, bb_l, bb_bw, bb_pb = calc_bb(closes, BB_PERIOD, BB_STD)
        pivots = find_pivot_levels(highs, lows)
        atr = calc_atr(highs, lows, closes, ATR_PERIOD)
        adx, pdi, mdi = calc_adx(highs, lows, closes, ADX_PERIOD)
        if adx < ADX_MIN:
            if sym_lower == state.get('_diag_coin'): print(f"🩺 {sym_lower} 挡: ADX={adx:.1f}<{ADX_MIN}")
            continue  # ADX<25 = 震荡, 跳过
        # 🩺 诊断前5个通过ADX的币
        diag_cnt = state.setdefault('_adx_diag', 0)
        if diag_cnt < 5:
            state['_adx_diag'] = diag_cnt + 1
            print(f"🩺#{diag_cnt+1} {sym_lower}: ${price:.4f} MA7={ma7:.4f} MA14={ma14:.4f} RSI={rsi_14:.1f} ADX={adx:.1f} (+DI={pdi:.1f} -DI={mdi:.1f}) short_ok={short_ok} long_ok={long_ok}")
        # ADX方向校验：ADX≥20时 +DI>-DI 做多 / -DI>+DI 做空
        # 防止强下跌趋势中ADX飙升诱多（飞刀场）
        adx_long_ok = pdi > mdi   # +DI > -DI = 上升趋势确认
        adx_short_ok = mdi > pdi  # -DI > +DI = 下降趋势确认
        
        # ── 做多 ──
        do_long = False
        if not long_ok:
            # BTC断路器阻挡做多，但币自身极强势可绕过
            if adx >= ADX_STRONG and pdi > mdi and ma7 > ma14 and price > ma7:
                do_long = True
                if sym_lower == state.get('_diag_coin'):
                    print(f"🩺 {sym_lower} ✅ 绕过BTC断路器(自身ADX={adx:.1f}强势上升)")
            else:
                if sym_lower == state.get('_diag_coin'):
                    print(f"🩺 {sym_lower} 挡: long_ok=False(BTC断路器)")
        else:
            do_long = True
        
        if do_long and ma7 > ma14 and (
            (adx >= ADX_STRONG and pdi > mdi and price >= ma7 * 0.995 and price <= ma14 * 1.005)
            or
            (adx < ADX_STRONG and price > ma14 and price < ma7 * 1.03)
        ):
            if sym_lower == state.get('_diag_coin'): print(f"🩺 {sym_lower} ✅ 做多条件通过 MA7={ma7:.4f} MA14={ma14:.4f} price={price:.4f}")
            if not adx_long_ok:
                if sym_lower == state.get('_diag_coin'): print(f"🩺 {sym_lower} 挡: 做多ADX方向不符 pdi={pdi:.1f} mdi={mdi:.1f}")
                continue  # ADX方向不符(+DI需>-DI)
            # ── 动量过滤：价格在MA7下方时检查K线实体跌幅 ──
            # 最近5根阴线累计实体总跌幅超过ATR×0.5才拦截（防暴跌）
            # 避免阴线计数死锁：回踩=阴线，但阴线≠暴跌
            if price < ma7:
                red_bars = [k for k in klines[-5:] if k['c'] < k['o']]
                if red_bars:
                    total_decline = sum(k['o'] - k['c'] for k in red_bars)
                    if total_decline > atr * 0.5:
                        continue  # 累计跌幅过大（暴跌），不做多
            # 强趋势(ADX≥30)放宽RSI上限到80，防止强趋势中超买过早过滤
            rsi_max = 80 if adx >= ADX_STRONG else RSI_LONG_MAX
            if rsi_14 < RSI_LONG_MIN or rsi_14 > rsi_max: continue
            # ── 多周期共振：个币1h趋势检查 ──
            if not await check_coin_1h_trend(f'{sym_lower.upper()}USDT', 'LONG'):
                continue
            # ── 资金费率过滤：费率过高不追多 ──
            fr = funding_rates.get(sym_lower, 0)
            if fr > 0.0005:  # 费率>0.05% → 多头过热，不做多
                continue
            # 安全阀A: 同板块同方向最多1仓
            sector = get_sector(sym_lower)
            same_sector_long = 0
            for psym in state.get('positions', {}):
                if get_sector(psym) == sector and float(state['positions'][psym].get('positionAmt', 0)) > 0:
                    same_sector_long += 1
            if same_sector_long >= 1:
                continue
            fib_levels = calc_fib_levels(highs, lows, price, is_long=True)
            fib_sup, fib_res, _, _, _, _ = find_best_fib_sr(price, fib_levels, pivots, bb_u, bb_l)
            dist = ((price - fib_sup) / fib_sup) * 100
            if dist > 5: continue
            atr_sl = max(atr * ATR_MULT, price * ATR_MIN_PCT)
            atr_sl = min(atr_sl, price * ATR_MAX_PCT)
            sl_price = price - atr_sl
            sl_price = max(sl_price, fib_sup)
            # 最小止损距离：硬0.15%和ATR×0.5取较大值，防插针被扫
            min_sl_dist = max(price * 0.0015, atr * 0.5)
            if price - sl_price < min_sl_dist:
                continue
            # 止盈: 强趋势用ATR扩展, 否则用fib_res
            if adx >= ADX_STRONG:
                tp_price = min(price + atr * 3, price * 1.05)
            else:
                tp_price = min(fib_res, price * 1.05)
            risk = price - sl_price
            reward = tp_price - price
            if risk <= 0: continue
            rr = reward / risk
            # RR动态: 强趋势放宽, 弱趋势收紧
            local_min_rr = 0.8 if adx >= ADX_STRONG else (1.0 if adx >= ADX_MIN + 5 else min_rr)
            if rr < local_min_rr: continue
            risk_per_unit = risk
            max_qty_by_risk = risk_amount / risk_per_unit if risk_per_unit > 0 else 0
            max_qty_by_margin = (margin_for_trade * LEVERAGE) / price
            max_qty = min(max_qty_by_risk, max_qty_by_margin)
            step, market_max = await get_step_size(f'{sym_lower.upper()}USDT')
            max_qty = min(max_qty, market_max)
            qty = math.floor(max_qty / step) * step
            if qty <= 0 or qty * price < 10: continue
            if best_trade is None or rr > best_trade[7]:
                score, factors = calc_cross_score(True, ma7, ma14, price, rsi_14, bb_pb, vol, rr, atr, adx,
                                                   funding_rate=fr, trend_1h=await get_trend_1h_direction(sym_lower))
                candidates_list.append((score, 'LONG', sym_lower.upper(), price, qty, sl_price, tp_price, rr,
                                       f"MA多头+ATR{atr:.4f}+RR{rr:.1f}:1+Score{score:.2f}", factors))
        
        # ── 做空 ──
        if not short_ok:
            if sym_lower == state.get('_diag_coin'):
                if long_ok: print(f"🩺 {sym_lower} 挡: long_ok=True但长单条件不符 MA7={ma7:.4f} MA14={ma14:.4f} price={price:.4f}")
                else: print(f"🩺 {sym_lower} 挡: short_ok=False(BTC断路器), 双向封锁")
            pass  # 不做空
        elif adx < ADX_MIN_SHORT:
            # 做空需要更强趋势确认(ADX≥30)，弱趋势中做空被反弹扫损
            if sym_lower == state.get('_diag_coin'):
                print(f"🩺 {sym_lower} 挡: 做空ADX={adx:.1f}<{ADX_MIN_SHORT}")
            pass  # ADX不够，不做空
        elif ma7 < ma14 and price > ma7 * 0.995 and (
            # 强下降趋势(ADX≥30+方向一致)：允许在MA7~MA14区间做空（骑趋势）
            (adx >= ADX_STRONG and mdi > pdi and price <= ma14 * 1.005)
            or
            # 弱趋势：必须反弹到MA14阻力位确认衰竭
            (adx < ADX_STRONG and price >= ma14 * 0.998 and price <= ma14 * 1.01)
        ):
            if sym_lower == state.get('_diag_coin'): print(f"🩺 {sym_lower} ✅ 做空条件通过 MA7={ma7:.4f} MA14={ma14:.4f} price={price:.4f}")
            # 下降趋势 + 价格触及MA14阻力区（反弹衰竭确认）
            # 与做多回踩MA7对称：做多等回踩到MA7，做空等反弹到MA14
            if not adx_short_ok:
                if sym_lower == state.get('_diag_coin'): print(f"🩺 {sym_lower} 挡: 做空ADX方向不符 pdi={pdi:.1f} mdi={mdi:.1f}")
                continue  # ADX方向不符(-DI需>+DI)
            # ── 布林带下轨过滤：价格在下轨附近不做空（底部做空被弹死） ──
            if price <= bb_l * 1.01:  # 价格在BB下轨1%以内=超卖区，不做空
                continue
            # ── 动量过滤：检查每根已收盘15m阳线实体大小 ──
            # 最近5根中任何单根阳线实体 > ATR×0.3 即拦截（防大阳线反弹追空）
            # 只看已收盘K线(去除当前未成形Bar)，避免Tick抖动闪烁
            recent_bars = klines[-5:]
            bar_too_strong = False
            for kb in recent_bars:
                if kb['c'] > kb['o']:  # 阳线
                    entity = kb['c'] - kb['o']
                    if entity > atr * 0.3:
                        bar_too_strong = True
                        if sym_lower == state.get('_diag_coin'):
                            print(f"🩺 {sym_lower} 挡: 阳线实体{entity:.6f}>ATR×0.3={atr*0.3:.6f}, 反弹动能过强")
                        break
            if bar_too_strong:
                continue
            if rsi_14 > RSI_SHORT_MAX or rsi_14 < RSI_SHORT_MIN: continue
            # ── 多周期共振：个币1h趋势检查 ──
            if not await check_coin_1h_trend(f'{sym_lower.upper()}USDT', 'SHORT'):
                continue
            # ── 资金费率过滤：费率过负不追空（空头过热） ──
            fr = funding_rates.get(sym_lower, 0)
            if fr < -0.0005:  # 费率<-0.05% → 空头过热，不做空
                continue
            # 安全阀A: 同板块同方向最多1仓
            sector = get_sector(sym_lower)
            same_sector_short = 0
            for psym in state.get('positions', {}):
                if get_sector(psym) == sector and float(state['positions'][psym].get('positionAmt', 0)) < 0:
                    same_sector_short += 1
            if same_sector_short >= 1:
                continue
            fib_levels = calc_fib_levels(highs, lows, price, is_long=False)
            fib_sup, fib_res, _, _, _, _ = find_best_fib_sr(price, fib_levels, pivots, bb_u, bb_l)
            dist_to_res = ((fib_res - price) / price) * 100
            if dist_to_res > 5: continue
            atr_sl = max(atr * ATR_MULT, price * ATR_MIN_PCT)
            atr_sl = min(atr_sl, price * ATR_MAX_PCT)
            sl_price = price + atr_sl
            sl_price = min(sl_price, fib_res)
            # 最小止损距离：硬0.15%和ATR×0.5取较大值，防插针被扫
            min_sl_dist = max(price * 0.0015, atr * 0.5)
            if sl_price - price < min_sl_dist:
                continue
            # 止盈: 强趋势用ATR扩展, 否则用fib_sup
            if adx >= ADX_STRONG:
                tp_price = max(price - atr * 3, price * 0.95)
            else:
                tp_price = max(fib_sup, price * 0.95)
            if sym_lower in state.get('crash_bounce', {}):
                cb = state['crash_bounce'][sym_lower]
                if cb['low_24h'] < price:
                    tp_price = cb['low_24h']
            risk = sl_price - price
            reward = price - tp_price
            if risk <= 0: continue
            rr = reward / risk
            local_min_rr = 0.8 if adx >= ADX_STRONG else (1.0 if adx >= ADX_MIN + 5 else min_rr)
            if rr < local_min_rr: continue
            risk_per_unit = risk
            max_qty_by_risk = risk_amount / risk_per_unit if risk_per_unit > 0 else 0
            max_qty_by_margin = (margin_for_trade * LEVERAGE) / price
            max_qty = min(max_qty_by_risk, max_qty_by_margin)
            step, market_max = await get_step_size(f'{sym_lower.upper()}USDT')
            max_qty = min(max_qty, market_max)
            qty = math.floor(max_qty / step) * step
            if qty <= 0 or qty * price < 10: continue
            if best_trade is None or rr > best_trade[7]:
                score, factors = calc_cross_score(False, ma7, ma14, price, rsi_14, bb_pb, vol, rr, atr, adx,
                                                   funding_rate=fr, trend_1h=await get_trend_1h_direction(sym_lower))
                candidates_list.append((score, 'SHORT', sym_lower.upper(), price, qty, sl_price, tp_price, rr,
                                       f"MA空头+ATR{atr:.4f}+RR{rr:.1f}:1+Score{score:.2f}", factors))
    
    # 断面评分排序: 取最高分（最低0.65分才开仓，过滤弱信号）
    MIN_SCORE = 0.65
    if candidates_list:
        candidates_list.sort(key=lambda x: x[0], reverse=True)
        if candidates_list[0][0] < MIN_SCORE:
            if len(candidates_list) > 1:
                print(f"  📊 断面评分: 最高{candidates_list[0][2]} Score={candidates_list[0][0]:.2f} < {MIN_SCORE} 跳过")
            return None, scanned_count
        best = candidates_list[0]
        # 重组成原trade格式 (side, sym, price, qty, sl, tp, rr, rr, desc, factors)
        best_trade = (best[1], best[2], best[3], best[4], best[5], best[6], best[7], best[7], best[8], best[9])
        if len(candidates_list) > 1:
            print(f"  📊 断面评分: 共{len(candidates_list)}个候选, 最好{best[2]} Score={best[0]:.2f}")
    
    return best_trade, scanned_count


# ═══════════════════════════════════════════
# 开仓执行
# ═══════════════════════════════════════════

async def execute_trade(best_trade, bal):
    side, sym, price, qty, sl, tp, rr, _, reason, factors = best_trade
    
    # ── REST交叉验证：用REST数据二次确认趋势方向（防WS数据偏差） ──
    try:
        rest_bars = await http_get_public(f'/fapi/v1/klines?symbol={sym}&interval=15m&limit=20')
        if isinstance(rest_bars, list) and len(rest_bars) >= 14:
            rest_c = [float(b[4]) for b in rest_bars]
            rest_h = [float(b[2]) for b in rest_bars]
            rest_l = [float(b[3]) for b in rest_bars]
            rest_ma7 = sum(rest_c[-7:]) / 7
            rest_ma14 = sum(rest_c[-14:]) / 14
            rest_adx, rest_pdi, rest_mdi = calc_adx(rest_h, rest_l, rest_c, ADX_PERIOD)
            if side == 'LONG':
                if not (rest_ma7 > rest_ma14 and rest_pdi > rest_mdi):
                    print(f"  ❌ REST验证失败: {sym} 做多但REST MA7={rest_ma7:.4f}<MA14={rest_ma14:.4f}或+DI< -DI")
                    set_failed(sym)
                    return None
            else:  # SHORT
                if not (rest_ma7 < rest_ma14 and rest_mdi > rest_pdi):
                    print(f"  ❌ REST验证失败: {sym} 做空但REST MA7={rest_ma7:.4f}>MA14={rest_ma14:.4f}或-DI<+DI")
                    set_failed(sym)
                    return None
    except:
        pass  # REST验证可选，失败不阻塞开仓
    
    # 用实际stepSize校准精度 + MARKET_LOT_SIZE限额
    step, market_max = await get_step_size(sym)
    qty = min(qty, market_max)
    qty = math.floor(qty / step) * step
    qty_str = f'{qty:.{max(0, -int(math.log10(step)))}f}' if step < 1 else f'{int(qty)}'
    if float(qty_str) <= 0: return None
    
    # ⚡ 安全阀B: 开仓后剩余保证金 ≥ 20%余额（防止系统性插针强平）
    tier, _, _ = get_tier_config(bal)
    margin_for_this = float(qty_str) * price / LEVERAGE
    # 计算当前已占用保证金
    used_margin = sum(
        abs(float(pdata.get('positionAmt', 0))) * float(pdata.get('entryPrice', 0)) / LEVERAGE
        for pdata in state['positions'].values()
    )
    remaining_after = bal - used_margin - margin_for_this
    if remaining_after < bal * 0.20:
        max_qty_safe = math.floor(((bal * 0.80 - used_margin) * LEVERAGE / price) / step) * step
        if max_qty_safe <= 0:
            print(f"  🔒 安全阀B: 开仓后仅剩${remaining_after:.2f}<20%, 放弃")
            return None
        qty = max_qty_safe
        qty_str = f'{qty:.{max(0, -int(math.log10(step)))}f}' if step < 1 else f'{int(qty)}'
        if float(qty_str) <= 0: return None
        print(f"  🔒 安全阀B缩量至{qty_str}(留20%=${bal*0.20:.2f})")
    
    now = datetime.utcnow().strftime('%H:%M:%S')
    # ── 策略分类+胜率展示 ──
    if 'STRONG' in (reason or ''):
        setup_type = f'{side}_STRONG'
    elif 'Score' in (reason or ''):
        setup_type = f'{side}_WEAK'
    else:
        setup_type = f'{side}_STD'
    stats_text = show_setup_stats(setup_type)
    if '无历史' not in stats_text:
        first_line = stats_text.split('\n')[0]
        print(f"  📊 {first_line}")
    
    print(f"\n🎯 {now} | {sym} {'做多' if side=='LONG' else '做空'} {qty:.1f} @ ${price:.4f}")
    print(f"📋 {reason} | SL=${sl:.4f} TP=${tp:.4f}")
    
    # ══ 测试模式：打印模拟结果，不执行真实下单 ══
    if TEST_MODE:
        qty_float = float(qty_str)
        risk_amt = abs(price - sl) * qty_float / LEVERAGE
        print(f"  🧪 测试模式 | 模拟{side} {sym} {qty_str} @ ${price:.4f}")
        print(f"  🧪 SL=${sl:.4f} TP=${tp:.4f} 风险=${risk_amt:.2f}")
        return {'test_mode': True, 'sym': sym.lower(), 'side': side,
                'qty': qty_float, 'entry': price, 'sl': sl, 'tp': tp,
                'reason': reason}
    
    lev_data = await http_post('/fapi/v1/leverage', {'symbol': f'{sym}', 'leverage': str(LEVERAGE)})
    print(f"  ⚙️ 杠杆: {lev_data.get('leverage', '?')}x")
    
    order_side = 'BUY' if side == 'LONG' else 'SELL'
    pos_side = 'LONG' if side == 'LONG' else 'SHORT'
    data = await http_post('/fapi/v1/order', {
        'symbol': f'{sym}', 'side': order_side, 'positionSide': pos_side,
        'type': 'MARKET', 'quantity': qty_str
    })
    if 'orderId' not in data:
        print(f"❌ 开仓失败: {data.get('msg', data)}")
        print(f"  🔍 DEBUG: sym={sym} qty_orig={best_trade[3]} step={step} qty_calc={qty} qty_str='{qty_str}'")
        set_failed(sym)
        return None
    
    print(f"✅ 已提交 ID={data.get('orderId','?')}")
    # 防REST同步延迟：订单提交后立即标记，不让后续扫描再次通过同一币种
    state.setdefault('fresh_open', {})[sym.lower()] = time.time()
    await asyncio.sleep(2)
    
    acct = await http_get('/fapi/v2/account')
    fill_q, fill_p = 0, price
    for p in acct.get('positions', []):
        if p['symbol'] == f'{sym}' and abs(float(p.get('positionAmt', 0))) > 0:
            fill_q = abs(float(p['positionAmt']))
            fill_p = float(p['entryPrice'])
            break
    if fill_q <= 0:
        # 市价单可能瞬间成交但账户API还未更新，查订单状态确认
        order_status = await http_get(f'/fapi/v1/order', {'symbol': str(sym), 'orderId': data.get('orderId', 0)})
        if isinstance(order_status, dict) and order_status.get('status') == 'FILLED':
            fill_q = abs(float(order_status.get('executedQty', 0)))
            fill_p = float(order_status.get('avgPrice', price))
        if fill_q <= 0:
            print(f"❌ 开仓验证失败（订单已提交但未查到成交）")
            set_failed(sym)
            return None
    
    print(f"✅ 成交: ${fill_p:.4f} x {fill_q:.1f}")
    
    # 记录开仓策略类型+多因子评分到持仓状态（方便平仓时记录）
    state['positions'].setdefault(sym_lower := sym.lower(), {})
    state['positions'][sym_lower]['setup'] = setup_type
    state['positions'][sym_lower]['factors'] = factors  # P4 策略标签
    
    await place_tp_sl_orders(sym, side, fill_p, fill_q, wider_sl=sl, wider_tp=tp)
    state['last_trade_time'] = time.time()
    
    # 防REST同步延迟：开仓后立即本地标记，180秒内不再开同一币种
    state.setdefault('fresh_open', {})[sym_lower] = time.time()
    
    # 记录开仓
    await record_trade_open(sym, side, fill_p, fill_q, reason, factors)
    
    # 推送开仓事件
    sl_loss = abs(fill_p - sl) * fill_q / LEVERAGE
    tp_profit = abs(tp - fill_p) * fill_q / LEVERAGE
    push_event('open', {
        'sym': sym, 'side': side, 'qty': round(fill_q, 2),
        'entry': round(fill_p, 4), 'sl': round(sl, 4), 'tp': round(tp, 4),
        'sl_loss': round(sl_loss, 2), 'tp_profit': round(tp_profit, 2),
        'reason': reason[:80]
    })
    
    return {'sym': sym, 'side': side, 'qty': fill_q, 'entry': fill_p}


# ═══════════════════════════════════════════
# 持仓监控
# ═══════════════════════════════════════════

async def monitor_and_exit(sym, p, bal):
    """盘中主动监控系统
    分层防御：硬止损(条件单) → 主动风控(保本/止盈/追踪) → 反转预警(RSI/MA/量) → 趋势逆势(1h) → 异动(瀑布/横扫)
    每层都有预警级别：WARN_INFO仅打印 | WARN_REDUCE减半仓 | WARN_EXIT全平
    """
    amt = float(p.get('positionAmt', 0))
    entry = float(p.get('entryPrice', 0))
    upnl = float(p.get('unRealizedProfit', 0))
    if abs(amt) <= 0 or entry <= 0: return False
    
    if sym in state.get('exiting', set()):
        return False
    
    liq = calc_liq_price(entry, abs(amt), LEVERAGE, is_long=(amt > 0))
    margin_used = entry * abs(amt) / LEVERAGE
    pnl_pct = upnl / margin_used * 100 if margin_used > 0 else 0
    side = 'LONG' if amt > 0 else 'SHORT'
    is_short = amt < 0
    
    if amt > 0:
        exit_price_est = entry + upnl / amt
    else:
        exit_price_est = entry - upnl / abs(amt)
    
    print(f"\n📊 {sym} {'做空' if is_short else '做多'} {abs(amt):.2f} @ ${entry:.4f}")
    print(f"  PnL: {pnl_pct:+.1f}% | 保证金: ${margin_used:.2f} | 强平: ${liq:.4f}")
    
    # ── 获取K线数据(用于盘中监测) ──
    klist = []
    klines_ok = False
    if sym.lower() in state['kline_data']:
        klist = list(state['kline_data'][sym.lower()])
        klines_ok = len(klist) >= 10
    
    # ══════════════════════════════════════════════
    #   LAYER 1:  硬止损/止盈 (条件单保护,最高优先级)
    # ══════════════════════════════════════════════
    if pnl_pct >= 6.0:
        state.setdefault('exiting', set()).add(sym)
        print(f"  🎯 +6%止盈!")
        success, fill_p, _, _ = await close_position_market(sym.upper(), amt, REASON_TP)
        if success:
            actual_pnl = (fill_p - entry) * abs(amt) if amt > 0 else (entry - fill_p) * abs(amt)
            print(f"  ✅ 盈利${actual_pnl:.2f} (实际均价${fill_p:.4f})")
            push_event('close', {'sym': sym, 'side': side, 'pnl': round(actual_pnl,2), 'reason': 'TP'})
            record_trade(sym, side, entry, fill_p, amt, REASON_TP)
        await clean_orphan_algos(sym)
        return True
    
    if pnl_pct <= -6.0:
        state.setdefault('exiting', set()).add(sym)
        print(f"  🛑 -6%止损!")
        success, fill_p, _, _ = await close_position_market(sym.upper(), amt, REASON_SL)
        if success:
            actual_pnl = (fill_p - entry) * abs(amt) if amt > 0 else (entry - fill_p) * abs(amt)
            print(f"  ✅ 亏损${abs(actual_pnl):.2f} (实际均价${fill_p:.4f})")
            push_event('close', {'sym': sym, 'side': side, 'pnl': round(actual_pnl,2), 'reason': 'SL'})
            record_trade(sym, side, entry, fill_p, amt, REASON_SL)
            # ── 记录插针事件（供回补检测） ──
            state.setdefault('wick_events', {})[sym.lower()] = {
                'side': side, 'entry': entry, 'ts': time.time(),
                'exit_price': fill_p, 'sl_price': entry * (1 + PCT_6) if is_short else entry * (1 - PCT_6)
            }
        await clean_orphan_algos(sym)
        return True
    
    # ══════════════════════════════════════════════
    #   LAYER 2:  主动风控 (动态管理已有盈利)
    # ══════════════════════════════════════════════
    
    # 2a. 保本止损: 浮盈≥3% → 将条件单SL移到入场价
    if pnl_pct >= BREAKEVEN_THRESHOLD:
        be_key = f'breakeven_{sym}'
        if not state.get(be_key):
            state[be_key] = True
            print(f"  🔒 保本激活(PnL={pnl_pct:+.1f}%): 移动SL到入场价${entry:.4f}")
            # 取消旧SL
            await clean_orphan_algos(sym)
            # 挂新SL在入场价(+1 tick防滑点)
            tick = max(await get_price_tick(sym), 0.001)
            be_sl = entry + tick if is_short else entry - tick
            be_data = await http_post('/fapi/v1/algoOrder', {
                'symbol': sym.upper(), 'side': 'BUY' if is_short else 'SELL',
                'positionSide': 'SHORT' if is_short else 'LONG',
                'type': 'STOP_MARKET', 'algoType': 'CONDITIONAL',
                'triggerPrice': f'{be_sl:.4f}', 'closePosition': 'true',
                'workingType': 'MARK_PRICE'
            })
            if 'algoId' in be_data:
                await save_algo_ids(sym, '', be_data['algoId'])
                print(f"    ✅ 保本SL=${be_sl:.4f}")
                push_event('alert', {'level': 'INFO', 'sym': sym, 'msg': '保本激活', 'pnl': round(upnl,2)})
            else:
                print(f"    ❌ 保本SL失败: {be_data}")
    
    # 2b. 部分止盈: 浮盈≥5% → 平50%锁定利润
    if pnl_pct >= PARTIAL_TP_THRESHOLD:
        half_key = f'half_tp_{sym}'
        if not state.get(half_key):
            state[half_key] = True
            half_qty = abs(amt) / 2
            # 只平一半
            print(f"  🎯 部分止盈(PnL={pnl_pct:+.1f}%): 平{half_qty:.2f}锁利${upnl/2:.2f}")
            cs = 'SELL' if amt > 0 else 'BUY'; cp = 'LONG' if amt > 0 else 'SHORT'
            d = await http_post('/fapi/v1/order', {
                'symbol': sym.upper(), 'side': cs, 'positionSide': cp,
                'type': 'MARKET', 'quantity': f'{half_qty:.1f}'
            })
            if 'orderId' in d:
                print(f"    ✅ 已平仓一半, 锁定${upnl/2:.2f}")
                push_event('alert', {'level': 'INFO', 'sym': sym, 'msg': '部分止盈50%', 'pnl': round(upnl/2,2)})
            else:
                print(f"    ❌ 部分止盈失败: {d.get('msg','')}")
    
    # 2c. 追踪止损: 浮盈≥8% → 启动回调2%跟踪
    TRAIL_KEY = f'trail_{sym}'
    if pnl_pct >= TRAIL_ACTIVATE and klines_ok:
        cur_price = klist[-1]['c']
        trail_data = state.setdefault('trail_peak', {}).get(TRAIL_KEY)
        if trail_data is None:
            state.setdefault('trail_peak', {})[TRAIL_KEY] = {
                'peak': cur_price, 'activated': True
            }
            trail_data = state['trail_peak'][TRAIL_KEY]
            print(f"  🔄 追踪激活(PnL={pnl_pct:+.1f}%): 峰值${cur_price:.4f} 回调2%平仓")
        # 更新峰值(做空看最低价,做多看最高价)
        if is_short and cur_price < trail_data['peak']:
            trail_data['peak'] = cur_price
        elif not is_short and cur_price > trail_data['peak']:
            trail_data['peak'] = cur_price
        # 检查回调
        if is_short:
            callback_price = trail_data['peak'] * (1 + TRAIL_CALLBACK)
            if cur_price >= callback_price:
                print(f"  🎯 追踪触发: 低点${trail_data['peak']:.4f}→现价${cur_price:.4f} 回调{TRAIL_CALLBACK*100:.0f}%平仓")
                push_event('close', {'sym': sym, 'side': side, 'pnl': round(upnl,2), 'reason': 'TRAIL'})
                state.setdefault('exiting', set()).add(sym)
                d = await http_post('/fapi/v1/order', {
                    'symbol': sym.upper(), 'side': 'BUY', 'positionSide': 'SHORT',
                    'type': 'MARKET', 'quantity': str(abs(amt))
                })
                if 'orderId' in d: print(f"  ✅ 平仓 利润: ${upnl:+.2f}")
                record_trade(sym, side, entry, cur_price, amt, REASON_TRAIL)
                await clean_orphan_algos(sym)
                return True
        else:
            callback_price = trail_data['peak'] * (1 - TRAIL_CALLBACK)
            if cur_price <= callback_price:
                print(f"  🎯 追踪触发: 高点${trail_data['peak']:.4f}→现价${cur_price:.4f} 回调{TRAIL_CALLBACK*100:.0f}%平仓")
                push_event('close', {'sym': sym, 'side': side, 'pnl': round(upnl,2), 'reason': 'TRAIL'})
                state.setdefault('exiting', set()).add(sym)
                d = await http_post('/fapi/v1/order', {
                    'symbol': sym.upper(), 'side': 'SELL', 'positionSide': 'LONG',
                    'type': 'MARKET', 'quantity': str(abs(amt))
                })
                if 'orderId' in d: print(f"  ✅ 平仓 利润: ${upnl:+.2f}")
                record_trade(sym, side, entry, cur_price, amt, REASON_TRAIL)
                await clean_orphan_algos(sym)
                return True
    
    # 2d. 时间止损: 持仓超4h且无盈利 → 主动平仓
    pos_open_time = state['positions'].get(sym.lower(), {}).get('open_time', 0)
    if pos_open_time > 0:
        hold_mins = (time.time() - pos_open_time) / 60
        if hold_mins > MAX_HOLD_MINS and pnl_pct <= 1.0:
            print(f"  ⏰ 时间止损: 持仓{hold_mins:.0f}分钟无进展,主动平仓")
            state.setdefault('exiting', set()).add(sym)
            success, fill_p, _, _ = await close_position_market(sym.upper(), amt, 'TIME_STOP')
            if success:
                actual_pnl = (fill_p - entry) * abs(amt) if amt > 0 else (entry - fill_p) * abs(amt)
                print(f"  ✅ 时间止损平仓 盈亏: ${actual_pnl:.2f} (实际均价${fill_p:.4f})")
                push_event('close', {'sym': sym, 'side': side, 'pnl': round(actual_pnl,2), 'reason': 'TIME_STOP'})
                record_trade(sym, side, entry, fill_p, amt, 'TIME_STOP')
            await clean_orphan_algos(sym)
            return True
    
    # ══════════════════════════════════════════════
    #   LAYER 3:  反转预警系统 (K线数据驱动)
    # ══════════════════════════════════════════════
    if klines_ok:
        closes = [k['c'] for k in klist]
        highs = [k['h'] for k in klist]
        lows = [k['l'] for k in klist]
        vols = [k['v'] for k in klist] if 'v' in klist[-1] else [k.get('quote_vol', 0) for k in klist]
        
        cur_price = klist[-1]['c']
        ma7 = sum(closes[-7:])/7 if len(closes)>=7 else closes[-1]
        ma14 = sum(closes[-14:])/14 if len(closes)>=14 else closes[-1]
        rsi_val = calc_rsi(closes, 14)
        avg_vol = sum(vols[-10:])/10 if len(vols)>=10 else 1
        
        # ── 3a. RSI反向趋势检测 ──
        rsi_warn = WARN_INFO  # 默认无预警
        if len(closes) >= 20:
            rsi_old = calc_rsi(closes[:-8], 14) if len(closes)>=22 else rsi_val
            if rsi_old is not None and rsi_val is not None:
                rsi_shift = rsi_val - rsi_old
                if is_short and rsi_shift > RSI_REVERSAL_WARN:
                    rsi_warn = WARN_REDUCE  # 做空时RSI大幅上升=买方进场
                    if rsi_val > 55:
                        rsi_warn = WARN_EXIT  # RSI已到中性偏多→必须走
                elif not is_short and rsi_shift < -RSI_REVERSAL_WARN:
                    rsi_warn = WARN_REDUCE  # 做多时RSI大幅下降=卖方进场
                    if rsi_val < 45:
                        rsi_warn = WARN_EXIT
        
        # ── 3b. MA突破检测 ──
        ma_warn = WARN_INFO
        if is_short and cur_price > ma14 and ma14 > ma7:
            ma_warn = WARN_EXIT  # 做空时价格突破MA14且MA空头走弱→趋势反转
        elif not is_short and cur_price < ma14 and ma14 < ma7:
            ma_warn = WARN_EXIT
        
        # ── 3c. 成交量异常检测 ──
        vol_warn = WARN_INFO
        last_vol = vols[-1] if vols else 0
        if avg_vol > 0 and last_vol > avg_vol * VOL_SPIKE_RATIO:
            last_body = klist[-1]['c'] - klist[-1]['o']
            if is_short and last_body > 0:  # 放量阳线=做空危险
                print(f"  📊 放量{last_vol/avg_vol:.1f}x阳线! 买方进场信号")
                vol_warn = WARN_REDUCE
            elif not is_short and last_body < 0:  # 放量阴线=做多危险
                print(f"  📊 放量{last_vol/avg_vol:.1f}x阴线! 卖方进场信号")
                vol_warn = WARN_REDUCE
        
        # ── 3d. 计算综合预警分数并执行 ──
        max_warn = max(rsi_warn, ma_warn, vol_warn)
        
        # 打印诊断信息（每次循环输出，让用户看到监测在工作）
        warn_icons = {0: '🟢', 1: '🟡', 2: '🔴'}
        signals = []
        if rsi_warn > 0: signals.append(f"RSI{rsi_shift:+.0f}")
        if ma_warn > 0: signals.append("MA突破")
        if vol_warn > 0: signals.append("放量")
        sig_str = f" | {'+'.join(signals) if signals else '正常'}" if max_warn > 0 else ''
        print(f"  {warn_icons[max_warn]} 监测: RSI={rsi_val:.0f} MA7={ma7:.4f} MA14={ma14:.4f}{sig_str}")
        
        if max_warn >= WARN_EXIT:
            pnl_str = f'${upnl:+.2f}'
            print(f"  🚨 反转预警! 平仓!")
            push_event('alert', {'level': 'WARN_EXIT', 'sym': sym, 'msg': f'反转预警平仓', 'pnl': round(upnl, 2)})
            state.setdefault('exiting', set()).add(sym)
            success, fill_p, _, _ = await close_position_market(sym.upper(), amt, 'REVERSAL_WARN')
            if success:
                actual_pnl = (fill_p - entry) * abs(amt) if amt > 0 else (entry - fill_p) * abs(amt)
                print(f"  ✅ 平仓 盈亏: ${actual_pnl:+.2f} (实际均价${fill_p:.4f})")
                push_event('alert', {'level': 'WARN_EXIT', 'sym': sym, 'msg': f'反转预警平仓', 'pnl': round(actual_pnl, 2)})
                record_trade(sym, side, entry, fill_p, amt, 'REVERSAL_WARN')
            await clean_orphan_algos(sym)
            return True
        
        if max_warn >= WARN_REDUCE:
            half_qty = abs(amt) / 2
            # 按stepSize截断
            step, _ = await get_step_size(sym.upper())
            if step > 0:
                half_qty = math.floor(half_qty / step) * step
            if half_qty <= 0: half_qty = step or 0.1
            print(f"  ⚠️ 减仓预警! 平一半({half_qty:.4f})")
            cs = 'SELL' if amt > 0 else 'BUY'; cp = 'LONG' if amt > 0 else 'SHORT'
            d = await http_post('/fapi/v1/order', {
                'symbol': sym.upper(), 'side': cs, 'positionSide': cp,
                'type': 'MARKET', 'quantity': str(half_qty)
            })
            if 'orderId' in d:
                fill_price = float(d.get('avgPrice', exit_price_est))
                if fill_price <= 0:
                    cq = float(d.get('executedQty',0)); cqt = float(d.get('cumQuote',0))
                    fill_price = cqt/cq if cq > 0 else exit_price_est
                actual_pnl = (fill_price - entry) * half_qty if amt > 0 else (entry - fill_price) * half_qty
                print(f"  ✅ 减仓完成 剩余{abs(amt)/2:.2f} (实际均价${fill_price:.4f}, 盈亏${actual_pnl:.2f})")
                push_event('alert', {'level': 'WARN_REDUCE', 'sym': sym, 'msg': '减仓预警平一半', 'pnl': round(actual_pnl,2)})
                record_trade(sym, side, entry, fill_price, amt/2, 'REDUCE_WARN')
            else:
                print(f"  ❌ 减仓失败: {d.get('msg','')}")
    else:
        # 无K线数据, 简单打印
        print(f"  📡 等待K线数据...")
    
    # ══════════════════════════════════════════════
    #   LAYER 4: 1h趋势逆势检测 (持仓方向vs大趋势)
    # ══════════════════════════════════════════════
    try:
        sym_upper = sym.upper()
        r = await http_get_public(f'/fapi/v1/klines?symbol={sym_upper}&interval=1h&limit=30')
        if isinstance(r, list) and len(r) >= 25:
            c1h = [float(k[4]) for k in r]
            h1h = [float(k[2]) for k in r]
            l1h = [float(k[3]) for k in r]
            adx1h, pdi1h, mdi1h = calc_adx(h1h, l1h, c1h, ADX_PERIOD)
            if adx1h is not None and adx1h >= ADX_MIN:
                going_up = pdi1h > mdi1h
                if (going_up and is_short) or (not going_up and not is_short):
                    print(f"  🔄 1h逆势! ADX={adx1h:.1f} {'上升' if going_up else '下降'}中方向相反，平仓!")
                    state.setdefault('exiting', set()).add(sym)
                    success, fill_p, _, _ = await close_position_market(sym_upper, amt, '1H_REVERSAL')
                    if success:
                        actual_pnl = (fill_p - entry) * abs(amt) if amt > 0 else (entry - fill_p) * abs(amt)
                        print(f"  ✅ 平仓 盈亏: ${actual_pnl:+.2f} (实际均价${fill_p:.4f})")
                        push_event('close', {'sym': sym, 'side': side, 'pnl': round(actual_pnl,2), 'reason': '1H_REVERSAL'})
                        record_trade(sym, side, entry, fill_p, amt, 'REVERSAL')
                    await clean_orphan_algos(sym)
                    return True
    except:
        pass
    
    # ══════════════════════════════════════════════
    #   LAYER 5: 瀑布保护 (急涨/急跌)
    # ══════════════════════════════════════════════
    if klines_ok:
        wf_key = f"{sym}_{side}"
        wf_state = state['waterfall'].get(wf_key)
        if not (wf_state and time.time() - wf_state.get('triggered_at', 0) < WF_COOLDOWN_SEC):
            if len(klist) >= WF_BARS + 1:
                recent = klist[-WF_BARS:]
                is_waterfall = False
                if not is_short:  # 多头: 检测急跌
                    drops = [(recent[i]['o'] - recent[i]['c']) / recent[i]['o'] for i in range(len(recent))]
                    if all(d > 0 for d in drops) and sum(drops) > WF_DROP_PCT:
                        is_waterfall = True
                else:  # 空头: 检测急涨
                    rises = [(recent[i]['c'] - recent[i]['o']) / recent[i]['o'] for i in range(len(recent))]
                    if all(r > 0 for r in rises) and sum(rises) > WF_DROP_PCT:
                        is_waterfall = True
                if is_waterfall:
                    state.setdefault('exiting', set()).add(sym)
                    print(f"  🌊 瀑布保护! {sym} {'急跌' if not is_short else '急涨'} 清仓!")
                    success, fill_p, _, _ = await close_position_market(sym.upper(), amt, REASON_WATERFALL)
                    if success:
                        actual_pnl = (fill_p - entry) * abs(amt) if amt > 0 else (entry - fill_p) * abs(amt)
                        print(f"  ✅ 已清仓 亏损: ${abs(actual_pnl):.2f} (实际均价${fill_p:.4f})")
                        state['waterfall'][wf_key] = {'triggered_at': time.time()}
                        push_event('close', {'sym': sym, 'side': side, 'pnl': round(actual_pnl,2), 'reason': 'WATERFALL'})
                        record_trade(sym, side, entry, fill_p, amt, REASON_WATERFALL)
                    await clean_orphan_algos(sym)
                    return True
    
    # ══════════════════════════════════════════════
    #   LAYER 6: 15m横扫+连续反向K线
    # ══════════════════════════════════════════════
    if klines_ok and is_short:
        klist_local = list(state['kline_data'][sym.lower()])
        if len(klist_local) >= 4:
            last = klist_local[-1]
            chg = (last['c'] - last['o']) / entry * 100
            body = abs(last['c'] - last['o'])
            avg_body = sum(abs(k['c']-k['o']) for k in klist_local[-5:]) / 5
            if chg > SWEEP_CHG_MIN and body > avg_body * SWEEP_BODY_MULT:
                state.setdefault('exiting', set()).add(sym)
                print(f"  ⚡15m横扫! 平仓!")
                success, fill_p, _, _ = await close_position_market(sym.upper(), amt, REASON_SWEEP)
                if success:
                    actual_pnl = (fill_p - entry) * abs(amt) if amt > 0 else (entry - fill_p) * abs(amt)
                    print(f"  ✅ 平仓! 亏损: ${abs(actual_pnl):.2f} (实际均价${fill_p:.4f})")
                    push_event('close', {'sym': sym, 'side': side, 'pnl': round(actual_pnl,2), 'reason': 'SWEEP'})
                    record_trade(sym, side, entry, fill_p, amt, REASON_SWEEP)
                await clean_orphan_algos(sym)
                return True
            # 连续阳线
            bc = sum(1 for k in klist_local[-4:-1] if k['c'] > k['o'])
            if bc >= CONSISTENT_BULL:
                state.setdefault('exiting', set()).add(sym)
                print(f"  🔴 连续{bc}根阳线! 平仓!")
                success, fill_p, _, _ = await close_position_market(sym.upper(), amt, REASON_CONS_BULL)
                if success:
                    actual_pnl = (fill_p - entry) * abs(amt) if amt > 0 else (entry - fill_p) * abs(amt)
                    print(f"  ✅ 平仓! 亏损: ${abs(actual_pnl):.2f} (实际均价${fill_p:.4f})")
                    push_event('close', {'sym': sym, 'side': side, 'pnl': round(actual_pnl,2), 'reason': 'CONS_BULL'})
                    record_trade(sym, side, entry, fill_p, amt, REASON_CONS_BULL)
                await clean_orphan_algos(sym)
                return True
    
    return False


# ═══════════════════════════════════════════
# WebSocket流已被 data_collector.py 替代，独立运行永不挂
# futures_trader.py 只通过 REST 获取数据，不再直接订阅 WS
# 数据流：WS → Redis ← futures_trader（读Redis+REST）

# ═══════════════════════════════════════════
# REST 预加载
# ═══════════════════════════════════════════

async def rest_preload():
    """REST预加载150币种15m K线到state['kline_data']"""
    try:
        info = await http_get_public('/fapi/v1/exchangeInfo')
        syms = [s['symbol'].lower() for s in info.get('symbols', [])
                if s['symbol'].endswith('USDT') and s['status'] == 'TRADING']
        tk = await http_get_public('/fapi/v1/ticker/24hr')
        vm = {t['symbol'].lower(): float(t.get('quoteVolume', 0))
              for t in tk if isinstance(t, dict)}
        syms.sort(key=lambda s: vm.get(s, 0), reverse=True)
        scan = syms[:MAX_SYMBOLS]

        need_load = [s for s in scan
                     if not (s in state['kline_data'] and len(state['kline_data'][s]) >= 25)]
        # ── 先查Redis缓存（data_collector通过WS实时写入kl_15m:*，零HTTP） ──
        from_redis = 0
        for s in list(need_load):
            try:
                red_prices = redis_get_klines_15m(s)
                if red_prices and len(red_prices) >= 25:
                    dq = deque(maxlen=42)
                    for p in red_prices:
                        dq.append({'t': 0, 'o': p, 'h': p, 'l': p, 'c': p, 'final': True})
                    state['kline_data'][s] = dq
                    need_load.remove(s)
                    from_redis += 1
            except:
                pass
        if from_redis:
            print(f"  📡 从Redis加载 {from_redis} 个币种K线（零HTTP）")
        if not need_load:
            print(f"  ✅ 全部从Redis加载（{from_redis}个币种）")
            return
        print(f"  📡 HTTP预加载 {len(need_load)} 个币种历史K线（Redis不足的后备）...")
        load_count = 0
        sem = asyncio.Semaphore(10)
        async def load_one(sym):
            nonlocal load_count
            async with sem:
                try:
                    hist = await http_get_public(f'/fapi/v1/klines?symbol={sym.upper()}&interval=15m&limit=30')
                    if isinstance(hist, list) and len(hist) >= 25:
                        dq = deque(maxlen=42)
                        for bar in hist:
                            dq.append({'t': bar[0], 'o': float(bar[1]), 'h': float(bar[2]),
                                       'l': float(bar[3]), 'c': float(bar[4]), 'final': True})
                        state['kline_data'][sym] = dq
                        load_count += 1
                        # 写回Redis kl_15m:（供下次扫描零HTTP加载）
                        if REDIS_CLIENT:
                            try:
                                prices_15m = [float(bar[4]) for bar in hist[-20:]]
                                rkey15 = f'{RK_KLINES_15M}{sym}'
                                REDIS_CLIENT.delete(rkey15)
                                for p in prices_15m:
                                    REDIS_CLIENT.rpush(rkey15, str(p))
                                REDIS_CLIENT.expire(rkey15, 3600)
                            except:
                                pass
                except: pass
        await asyncio.gather(*[load_one(s) for s in need_load])
        print(f"  ✅ 预加载完成: {load_count}/{len(need_load)} 个")
    except Exception as e:
        print(f"  ⚠️ 预加载异常: {e}")


# ═══════════════════════════════════════════
# 插针回补检测
# ═══════════════════════════════════════════

async def check_wick_reentry(bal, recovery, max_pos):
    """检测被插针止损的币种是否已回到入场区 → 自动回补
    
    判定标准：
    1. 被SL打掉在30分钟内
    2. 当前价格已回到入场价±1%以内
    3. 趋势方向与原始方向一致（MA7/MA14排列正确）
    """
    we = state.get('wick_events', {})
    if not we:
        return
    
    now = time.time()
    expired = []
    
    for sym_lower, ev in list(we.items()):
        # 过期清理（超过30分钟不再等待）
        if now - ev['ts'] > 1800:
            expired.append(sym_lower)
            continue
        
        # 仓位已满跳过
        if len(state['positions']) >= max_pos:
            continue
        
        # 还在冷却期跳过
        if sym_lower in state.get('cooldown', {}):
            continue
        
        # 有持仓跳过
        if sym_lower in state['positions']:
            expired.append(sym_lower)
            continue
        
        # 获取最新K线判断是否回到入场区
        klist = list(state['kline_data'].get(sym_lower, []))
        if len(klist) < 14:
            continue
        
        closes = [k['c'] for k in klist[-14:]]
        highs = [k['h'] for k in klist[-14:]]
        lows = [k['l'] for k in klist[-14:]]
        cur_price = klist[-1]['c']
        entry = ev['entry']
        side = ev['side']
        
        # 判断价格是否回到入场区：当前价在入场价±1%以内
        price_dist = abs(cur_price - entry) / entry * 100
        if price_dist > 1.0:
            continue
        
        # 趋势验证：MA7/MA14排列与原始方向一致
        ma7 = sum(closes[-7:]) / 7
        ma14 = sum(closes[-14:]) / 14
        
        if side == 'SHORT':
            if not (ma7 < ma14 and cur_price >= ma14 * 0.998):  # 仍为空头排列
                expired.append(sym_lower)
                continue
        else:
            if not (ma7 > ma14 and cur_price <= ma7 * 1.002):  # 仍为多头排列
                expired.append(sym_lower)
                continue
        
        # ── 通过所有条件 → 执行回补 ──
        print(f"\n   🔄 插针回补 {sym_lower.upper()} {side} (原入场${entry:.4f}, 现价${cur_price:.4f})")
        
        # 用原入场价作为模拟开仓价，使用8成原仓位
        fake_sl = entry * (1 + PCT_6) if side == 'SHORT' else entry * (1 - PCT_6)
        fake_tp = entry * (1 - PCT_6 * 1.5) if side == 'SHORT' else entry * (1 + PCT_6 * 1.5)
        fake_best = (side, sym_lower.upper(), entry, 0, fake_sl, fake_tp, 1.5, 1.5, f'WICK_REENTRY_{side}', [])
        r = await execute_trade(fake_best, bal)
        if r:
            print(f"  ✅ 插针回补成功 {sym_lower.upper()} {side}")
            # 清除wick事件
            expired.append(sym_lower)
        else:
            print(f"  ⚠️ 插针回补失败 {sym_lower.upper()}, 下次再试")
    
    # 清理过期和已回补的事件
    for sym in expired:
        we.pop(sym, None)


# ═══════════════════════════════════════════
# 交易主循环
# ═══════════════════════════════════════════

def is_work_hours():
    """24小时全天候交易，不再限制工作时间"""
    return True

async def run_trade_logic():
    sync_ticks = 0
    reload_ticks = 0
    while state['running']:
        try:
            bal = await fetch_balance_rest()
            
            # 每15个周期(≈15分钟)刷新一次K线数据（无WS数据更新）
            reload_ticks += 1
            if reload_ticks >= 15:
                reload_ticks = 0
                asyncio.ensure_future(rest_preload())
            
            # ── REST持仓同步（每6个周期≈30秒，暂停期间也执行，避免state卡住） ──
            sync_ticks += 1
            if sync_ticks >= 6:
                sync_ticks = 0
                try:
                    acct = await http_get('/fapi/v2/account')
                    real_positions = set()
                    for p in acct.get('positions', []):
                        amt = abs(float(p.get('positionAmt', 0)))
                        if amt > 0:
                            sym = p['symbol'].lower()
                            real_positions.add(sym)
                            state['positions'][sym] = {
                                'positionAmt': p['positionAmt'],
                                'entryPrice': p['entryPrice'],
                                'unRealizedProfit': p.get('unRealizedProfit', '0'),
                            }
                    for sym in list(state['positions'].keys()):
                        if sym not in real_positions:
                            # 防API缓存延迟：刚开的币标记期内不清除(180秒)
                            fresh = state.get('fresh_open', {})
                            if sym in fresh and time.time() - fresh[sym] < 180:
                                print(f"  🔄 同步:保留 {sym}(fresh_open, {int(time.time()-fresh[sym])}秒前刚开)")
                                continue
                            print(f"  🔄 同步:清除 {sym}")
                            set_cooldown(sym, 900)
                            state.setdefault('exiting', set()).discard(sym)
                            del state['positions'][sym]
                            redis_put(f'{RK_POS}{sym}', '', 1)  # 写空=清除
                except:
                    pass
            # 同步后写入Redis（供AI查询）
            for sym, p in state['positions'].items():
                redis_put(f'{RK_POS}{sym}', json.dumps(p), 3600)
            redis_put('bal', json.dumps({'bal': round(bal, 2), 'ts': time.time()}), 3600)
            # 余额历史快照（每日1条，Reids保留7天，供资金曲线复盘）
            today_key = f'bal_snapshot:{datetime.utcnow().strftime("%Y%m%d")}'
            if REDIS_CLIENT and not REDIS_CLIENT.exists(today_key):
                REDIS_CLIENT.set(today_key, json.dumps({'bal': round(bal, 2), 'ts': time.time()}), 604800)

            # ════════════════════════════════════════════
            #  盘中主动安全监测
            # ════════════════════════════════════════════

            # 1. 全账户总回撤断路器：所有持仓总浮亏超阈值→一键清仓
            if bal > 0 and state['positions']:
                total_upnl = sum(float(p.get('unRealizedProfit', 0)) for p in state['positions'].values())
                drawdown_pct = abs(total_upnl) / bal * 100
                if total_upnl < 0 and drawdown_pct >= CIRCUIT_BREAKER_PCT:
                    print(f"  🚨 总回撤{drawdown_pct:.1f}%≥{CIRCUIT_BREAKER_PCT:.0f}% 触发断路器!")
                    if REDIS_CLIENT:
                        REDIS_CLIENT.incr('stats:circuit_breaker')
                    push_event('alert', {'level': 'CRITICAL', 'msg': f'总回撤{drawdown_pct:.1f}%, 一键清仓'})
                    for sym_lower in list(state['positions'].keys()):
                        p = state['positions'][sym_lower]
                        amt = abs(float(p.get('positionAmt', 0)))
                        if amt <= 0: continue
                        side = 'LONG' if float(p.get('positionAmt', 0)) > 0 else 'SHORT'
                        cs = 'SELL' if side == 'LONG' else 'BUY'
                        ps = 'LONG' if side == 'LONG' else 'SHORT'
                        print(f"    🔴 紧急平仓 {sym_lower} {side} {amt}")
                        d = await http_post('/fapi/v1/order', {
                            'symbol': sym_lower.upper(), 'side': cs, 'positionSide': ps,
                            'type': 'MARKET', 'quantity': str(amt)
                        })
                        if 'orderId' in d:
                            print(f"    ✅ 已平仓 {sym_lower}")
                            set_cooldown(sym_lower, 3600)
                            state.setdefault('exiting', set()).discard(sym_lower)
                            del state['positions'][sym_lower]
                        else:
                            print(f"    ❌ 平仓失败 {sym_lower}: {d.get('msg','')}")
                    print(f"  🔒 断路器已触发，进入暂停状态")
                    state['paused'] = True
                    state['pause_reason'] = f'断路器:回撤{drawdown_pct:.1f}%'
                    state['pause_resume_at'] = time.time() + 3600  # 暂停1小时
                    await asyncio.sleep(SCAN_INTERVAL)
                    continue

            # 2. 条件单安全校验：每30周期(≈30分钟)检查一次持仓是否有SL/TP保护
            verify_ticks = getattr(run_trade_logic, 'verify_ticks', 0) + 1
            run_trade_logic.verify_ticks = verify_ticks
            if verify_ticks >= 30 and state['positions']:
                run_trade_logic.verify_ticks = 0
                await verify_sl_tp(bal)
            
            # 心跳保活（异常告警用，每30秒更新）
            if REDIS_CLIENT:
                REDIS_CLIENT.set('stats:heartbeat', str(time.time()), 120)
            
            # 每30秒刷新资金费率+标记价缓存（放在心跳旁边，暂停期间也执行）
            cache_ticks = getattr(run_trade_logic, 'cache_ticks', 0) + 1
            run_trade_logic.cache_ticks = cache_ticks
            if cache_ticks >= 30:
                run_trade_logic.cache_ticks = 0
                asyncio.ensure_future(fetch_funding_rate_map())
            
            # 连败暂停检查（自动恢复）
            if state.get('paused'):
                now = time.time()
                resume_at = state.get('pause_resume_at', 0)
                if now >= resume_at:
                    # 冷却到，自动恢复
                    state['paused'] = False
                    state['pause_reason'] = ''
                    db = load_trade_db()
                    db['losing_streak'] = 0
                    db['pause_resume_at'] = 0
                    db['pause_reason'] = ''
                    save_trade_db(db)
                    print(f"  ▶️ 冷却结束，自动恢复交易")
                else:
                    remaining = int(resume_at - now)
                    print(f"  🛑 暂停中({remaining//60}分{remaining%60}秒后自恢复): {state.get('pause_reason','')}")
                    await asyncio.sleep(SCAN_INTERVAL * 5)
                    continue
            
            # ── 24小时全天候交易（已取消时间限制） ──
            if not is_work_hours():
                if state['positions']:
                    # 有持仓：维持监控，但跳过开新仓
                    for sym, p in list(state['positions'].items()):
                        if await monitor_and_exit(sym, p, bal):
                            set_cooldown(sym, 900)
                            state.setdefault('exiting', set()).discard(sym)
                            del state['positions'][sym]
                    print(f"🌙 非工作时间(北京{(datetime.utcnow().hour+8)%24}时)持仓{len(state['positions'])} | ${bal:.2f}")
                    await asyncio.sleep(SCAN_INTERVAL); continue
                else:
                    # 无持仓：非工作时间自动退出
                    print(f"🌙 非工作时间(北京{(datetime.utcnow().hour+8)%24}时)，持仓已清，结束战斗")
                    state['running'] = False
                    return
            
            # ── 9阶梯队检查仓位上限 ──
            tier, max_pos, risk_amount = get_tier_config(bal)
            if len(state['positions']) >= max_pos:
                for sym, p in list(state['positions'].items()):
                    if await monitor_and_exit(sym, p, bal):
                        set_cooldown(sym, 900)  # 15分钟冷却
                        state.setdefault('exiting', set()).discard(sym)
                        del state['positions'][sym]
                        continue  # 仓位关闭后继续循环，不return！
                print(f"\n📡 持仓{len(state['positions'])}/{max_pos} T{tier} | 余额: ${bal:.2f} | 单仓风险${risk_amount:.2f}")
                await asyncio.sleep(SCAN_INTERVAL); continue
            elif state['positions']:
                # 还有空位, 先监控已有持仓再扫新机会
                for sym, p in list(state['positions'].items()):
                    if await monitor_and_exit(sym, p, bal):
                        set_cooldown(sym, 900)  # 15分钟冷却
                        state.setdefault('exiting', set()).discard(sym)
                        del state['positions'][sym]
                        # 仓位关闭后继续扫新机会, 不return
                print(f"\n📡 持仓{len(state['positions'])}/{max_pos} T{tier} 有余位 | 余额: ${bal:.2f} | 单仓风险${risk_amount:.2f}")
            
            if bal < MIN_BALANCE:
                print(f"❌ 余额${bal:.2f}<${MIN_BALANCE:.0f}")
                await asyncio.sleep(SCAN_INTERVAL); continue
            
            recovery = bal < RECOVERY_BALANCE
            
            # ── 插针回补：检测被插针止损的币种是否已回到入场区 ──
            await check_wick_reentry(bal, recovery, max_pos)
            
            best, scanned = await scan_candidates(bal, recovery)
            if best is None:
                if scanned > 0: print(f"  扫描{scanned}个, 无机会")
                await asyncio.sleep(SCAN_INTERVAL); continue
            
            r = await execute_trade(best, bal)
            if r:
                sf = r['sym'].lower()
                state['positions'][sf] = {
                    'positionAmt': str(r['qty'] if r['side'] == 'LONG' else -r['qty']),
                    'entryPrice': str(r['entry']), 'unRealizedProfit': '0',
                    'open_time': time.time()
                }
        except Exception as e:
            import traceback
            print(f"  ⚠️ 循环异常: {e}")
            traceback.print_exc()
        # 持久化冷却期（每次循环结束保存，防止重启丢失）
        save_persistent_cooldown()
        await asyncio.sleep(SCAN_INTERVAL)


# ═══════════════════════════════════════════
# 手动命令
# ═══════════════════════════════════════════

async def cmd_positions():
    acct = await http_get('/fapi/v2/account')
    bal = 0.0
    for a in acct.get('assets', []):
        if a['asset'] == 'USDT': bal = float(a['walletBalance']); break
    print(f"💰 ${bal:.2f} | 每仓保证金上限: ${bal*MAX_MARGIN_PCT_PER_POS:.2f}")
    total_margin = 0.0
    for p in acct.get('positions', []):
        amt = float(p.get('positionAmt', 0))
        if abs(amt) > 0:
            entry = float(p.get('entryPrice', 0))
            margin = entry * abs(amt) / LEVERAGE
            total_margin += margin
            upnl = float(p.get('unRealizedProfit', 0))
            pnl_pct = upnl / margin * 100 if margin > 0 else 0
            print(f"  {p['symbol']} {'LONG' if amt>0 else 'SHORT'} {abs(amt):.4f} @${entry:.4f}")
            print(f"    保证金: ${margin:.2f} | PnL: {pnl_pct:+.1f}% (${upnl:+.2f})")
    print(f"  合计保证金: ${total_margin:.2f} / 上限 ${bal*MAX_MARGIN_PCT_PER_POS*MAX_CONCURRENT_TRADES:.2f}")
    algos = await http_get('/fapi/v1/openAlgoOrders')
    if isinstance(algos, list) and algos:
        print(f"条件单: {len(algos)}")
        for o in algos: print(f"  {o['symbol']} {o.get('type','?')} @${o.get('triggerPrice','?')}")


async def cmd_manual_short(sym, qty):
    print(f"手动做空 {sym} {qty}")
    await http_post('/fapi/v1/leverage', {'symbol': sym, 'leverage': str(LEVERAGE)})
    d = await http_post('/fapi/v1/order', {
        'symbol': sym, 'side': 'SELL', 'positionSide': 'SHORT',
        'type': 'MARKET', 'quantity': str(qty)
    })
    if 'orderId' not in d: print(f"❌ {d}"); return
    await asyncio.sleep(2)
    acct = await http_get('/fapi/v2/account')
    for p in acct.get('positions', []):
        if p['symbol'] == sym and abs(float(p['positionAmt'])) > 0:
            await place_tp_sl_orders(sym, 'SHORT', float(p['entryPrice']), abs(float(p['positionAmt'])))
            print("✅ 已挂条件单"); return
    print("❌ 未找到持仓")


async def cmd_manual_long(sym, qty):
    print(f"手动做多 {sym} {qty}")
    await http_post('/fapi/v1/leverage', {'symbol': sym, 'leverage': str(LEVERAGE)})
    d = await http_post('/fapi/v1/order', {
        'symbol': sym, 'side': 'BUY', 'positionSide': 'LONG',
        'type': 'MARKET', 'quantity': str(qty)
    })
    if 'orderId' not in d: print(f"❌ {d}"); return
    await asyncio.sleep(2)
    acct = await http_get('/fapi/v2/account')
    for p in acct.get('positions', []):
        if p['symbol'] == sym and abs(float(p['positionAmt'])) > 0:
            await place_tp_sl_orders(sym, 'LONG', float(p['entryPrice']), abs(float(p['positionAmt'])))
            print("✅ 已挂条件单"); return
    print("❌ 未找到持仓")


async def cmd_scan():
    bal = await fetch_balance_rest()
    best, scanned = await scan_candidates(bal, bal < RECOVERY_BALANCE)
    if best:
        _, sym, pr, qty, sl, tp, rr, _, reason = best
        print(f"🎯 {sym} {'做多' if best[0]=='LONG' else '做空'} ${pr:.4f} SL=${sl:.4f} TP=${tp:.4f} R/R={rr:.1f}")
    else:
        print("无机会")


async def cmd_stats():
    """交易复盘统计"""
    print(calc_trade_stats())


async def cmd_resume():
    """手动恢复被连败保护暂停的交易"""
    if not state.get('paused'):
        print("✅ 未被暂停，无需恢复")
        return
    state['paused'] = False
    state['pause_reason'] = ''
    db = load_trade_db()
    db['losing_streak'] = 0
    db['pause_resume_at'] = 0
    db['pause_reason'] = ''
    save_trade_db(db)
    print("▶️  已恢复交易，连败计数已重置")


async def cmd_report():
    """复盘报告：最近交易详情"""
    db = load_trade_db()
    trades = db.get('trades', [])
    ot = db.get('open_trades', {})
    print(f"📊 交易复盘 ({datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')})")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━")
    total = db.get('total_trades', 0)
    wins = db.get('wins', 0)
    losses = db.get('losses', 0)
    win_rate = wins/total*100 if total > 0 else 0
    streak = db.get('losing_streak', 0)
    print(f"总交易: {total} | 胜率: {win_rate:.1f}% | 总盈亏: ${db.get('total_pnl',0):+.2f}")
    print(f"连败: {streak} 笔 {'🛑 已暂停' if state.get('paused') else '✅ 正常'}")
    print(f"当前持仓: {len(ot)}")
    if trades:
        print(f"\n最近5笔交易:")
        for t in trades[-5:]:
            flag = '✅' if t['pnl'] > 0 else '❌'
            print(f"  {flag} {t['sym']} {t['side']} ${t['entry']:.4f}->${t['exit']:.4f} "
                  f"${t['pnl']:+.2f}({t['pnl_pct']:+.1f}%) {t['exit_reason']}")
    print(f"\n▶️ 恢复: resume")


def cmd_setup_stats():
    detail = '--detail' in sys.argv or '-d' in sys.argv
    setup = None
    for arg in sys.argv[2:]:
        if arg.upper().startswith(('LONG_', 'SHORT_')):
            setup = arg.upper()
            break
    print(show_setup_stats(setup, detail))


# ═══════════════════════════════════════════
# 主入口
# ═══════════════════════════════════════════

async def main_auto():
    print(f"🚀 合约交易系统 | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    # ── 日志重定向（双fork后stdout丢失，写文件追踪） ──
    LOG_FILE = '/tmp/futures_trader_daemon.log'
    try:
        lf = open(LOG_FILE, 'a', buffering=1)
        sys.stdout = lf
        sys.stderr = lf
        print(f"\n=== {datetime.utcnow().strftime('%H:%M UTC')} 启动 ===")
    except:
        pass
    # ── 加载冷却期（重启不丢） ──
    load_persistent_cooldown()
    # ── 启动互斥锁 ──
    LOCK_FILE = '/tmp/futures_trader.lock'
    try:
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE) as f:
                old_pid = int(f.read().strip())
            try:
                os.kill(old_pid, 0)  # 探活
                print(f"❌ 已有实例运行中(PID={old_pid})，拒绝重复启动")
                sys.exit(1)
            except OSError:
                pass  # 旧进程已死，继续
        with open(LOCK_FILE, 'w') as f:
            f.write(str(os.getpid()))
        import atexit
        def cleanup_lock():
            try:
                if os.path.exists(LOCK_FILE):
                    with open(LOCK_FILE) as f:
                        cur = f.read().strip()
                    if cur == str(os.getpid()):
                        os.unlink(LOCK_FILE)
            except:
                pass
        atexit.register(cleanup_lock)
    except Exception as e:
        print(f"⚠️ 锁文件异常: {e}")
    if not API_KEY or not API_SECRET: print("❌ API未设置"); sys.exit(1)
    # ── 初始化Redis（失败不影响交易，只影响缓存查询） ──
    if redis_init():
        print("  ✅ Redis已连接")
        # 启动时回填历史交易 + 清理stale数据
        backfill_trades_to_redis()
        clean_stale_open_trades()
        # 启动时预缓存资金费率+标记价（data_collector WS可能还没连上）
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            loop.create_task(fetch_funding_rate_map())
        except:
            pass
    bal = await fetch_balance_rest()
    print(f"💰 ${bal:.2f}")
    try:
        acct = await http_get('/fapi/v2/account')
        for p in acct.get('positions', []):
            amt = float(p.get('positionAmt', 0))
            if abs(amt) > 0:
                state['positions'][p['symbol'].lower()] = {
                    'positionAmt': p['positionAmt'], 'entryPrice': p['entryPrice'],
                    'unRealizedProfit': p.get('unRealizedProfit', '0')
                }
        if state['positions']: print(f"📋 持仓: {', '.join(s.upper() for s in state['positions'])}")
        # 写入完整账户信息到Redis
        if REDIS_CLIENT:
            REDIS_CLIENT.set('account:summary', json.dumps({
                'walletBalance': float(acct.get('totalWalletBalance', 0)),
                'availableBalance': float(acct.get('availableBalance', 0)),
                'crossWallet': float(acct.get('totalCrossWalletBalance', 0)),
                'totalUnrealizedProfit': float(acct.get('totalUnrealizedProfit', 0)),
                'totalMaintMargin': float(acct.get('totalMaintMargin', 0)),
                'totalInitialMargin': float(acct.get('totalInitialMargin', 0)),
                'ts': time.time()
            }), ex=3600)
    except: pass

    # ── 启动时恢复连败计数（跳过Binance收入历史，用本地trade_db） ──
    try:
        db_rec = load_trade_db()
        # 如果本地没有记录，默认0
        if 'losing_streak' not in db_rec:
            db_rec['losing_streak'] = 0
            save_trade_db(db_rec)
        streak = db_rec.get('losing_streak', 0)
        if streak >= 2:
            print(f"  📊 启动恢复: 检测到连续{streak}笔亏损")
    except Exception as e:
        print(f"  ⚠️ 连败恢复异常: {e}")
        streak = 0
    
    # ── 启动时检查连败暂停（测试模式跳过） ──
    if not TEST_MODE:
        db_rec = load_trade_db()
        rec_streak = db_rec.get('losing_streak', 0)
        saved_pause = db_rec.get('pause_resume_at', 0)
        now = time.time()
        if saved_pause > now:
            # 持久化的暂停还没到期 → 继续等
            state['paused'] = True
            state['pause_reason'] = db_rec.get('pause_reason', f'连续{rec_streak}笔亏损')
            state['pause_resume_at'] = saved_pause
            remain = int(saved_pause - now)
            print(f"  🛑 恢复暂停: {state['pause_reason']}(还剩{remain//60}分{remain%60}秒)")
        elif rec_streak >= MAX_LOSS_STREAK:
            resume_at = now + PAUSE_COOLDOWN
            state['paused'] = True
            state['pause_reason'] = f"启动恢复:连续{rec_streak}笔亏损"
            state['pause_resume_at'] = resume_at
            # 持久化暂停时间到trade_db（抗gateway重启）
            db_rec['pause_resume_at'] = resume_at
            db_rec['pause_reason'] = state['pause_reason']
            save_trade_db(db_rec)
            print(f"  🛑 启动恢复: 连续{rec_streak}笔亏损，暂停{PAUSE_COOLDOWN//60}分钟")
        else:
            # 连败数低于阈值，清除残留暂停状态
            if saved_pause:
                db_rec['pause_resume_at'] = 0
                db_rec['pause_reason'] = ''
                save_trade_db(db_rec)
    
    # ── 方向级连败暂停恢复 ──
    for dir_name in ('short', 'long'):
        dir_pause_key = f'{dir_name}_paused'
        dir_until_key = f'{dir_name}_paused_until'
        dir_reason_key = f'{dir_name}_paused_reason'
        saved_dir_pause = db_rec.get(dir_pause_key, False)
        saved_dir_until = db_rec.get(dir_until_key, 0)
        now = time.time()
        if saved_dir_pause and saved_dir_until > now:
            state[dir_pause_key] = True
            state[dir_until_key] = saved_dir_until
            state[dir_reason_key] = db_rec.get(dir_reason_key, '')
            remain = int(saved_dir_until - now)
            print(f"  🔒 恢复方向暂停: {dir_name.upper()} (还剩{remain//3600}h{(remain%3600)//60}m)")
        elif saved_dir_pause:
            # 暂停已过期，清除
            state[dir_pause_key] = False
            db_rec[dir_pause_key] = False
            db_rec[dir_until_key] = 0
            db_rec[dir_reason_key] = ''
            save_trade_db(db_rec)
    
    # 清理残留条件单（跳过有持仓的币种）
    held_symbols = set(s.upper() for s in state['positions'])
    try:
        r = await http_get('/fapi/v1/openAlgoOrders')
        if isinstance(r, list) and r:
            print(f"🧹 检查{len(r)}个条件单...")
            for o in r:
                aid, s = o.get('algoId', ''), o.get('symbol', '')
                if not s or not aid: continue
                if s in held_symbols:
                    print(f"  ⏭️ 跳过 {s}（有持仓）")
                    continue
                dr = await http_delete('/fapi/v1/algoOrder', {'symbol': s, 'algoId': str(aid)})
                if dr.get('code') == '200' or 'algoId' in dr:
                    print(f"  ✅ 清除 {s}")
    except: pass
    
    async def resilient_wrapper(name, coro_factory):
        """永不退出的task包装器：捕获一切异常，异常后自动重启"""
        retry = 1
        while state['running']:
            try:
                await coro_factory()
            except asyncio.CancelledError:
                # 被gather取消 → 不计入崩溃，直接重试
                if state['running']:
                    print(f"  🔄 {name} 被取消，重试中...")
                    await asyncio.sleep(1)
                    retry = min(retry * 2, 30)
                    continue
                break
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                # 截断长traceback避免日志爆炸
                short_tb = '\n'.join(tb.split('\n')[-6:])
                print(f"  💥 {name} 崩溃({e})\n    {short_tb}")
            retry = min(retry * 2, 30)  # 指数退避：1→2→4→...→30s上限
            await asyncio.sleep(retry)
            if retry > 5:
                print(f"  🔄 等待{retry}s后重启 {name}...")
        print(f"  ⏹️ {name} 已停止")
    
    # 启动前预加载K线数据
    await rest_preload()
    
    # 只运行交易逻辑，数据由data_collector独立提供
    tasks = [
        asyncio.create_task(resilient_wrapper('交易逻辑', run_trade_logic)),
    ]
    
    def shutdown():
        print("\n🛑 关闭中..."); state['running'] = False
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: signal.signal(sig, lambda s, f: shutdown())
        except: pass
    await asyncio.gather(*tasks, return_exceptions=True)
    print("👋 已关闭")


async def main():
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == 'positions': await cmd_positions()
        elif cmd == 'scan':
            print("  📡 从Redis读取K线数据扫描中...")
            redis_init()
            state['running'] = True
            await cmd_scan()
            state['running'] = False
        elif cmd == 'short' and len(sys.argv) >= 4:
            await cmd_manual_short(sys.argv[2].upper(), float(sys.argv[3]))
        elif cmd == 'long' and len(sys.argv) >= 4:
            await cmd_manual_long(sys.argv[2].upper(), float(sys.argv[3]))
        elif cmd == 'stats':
            await cmd_stats()
        elif cmd == 'resume':
            await cmd_resume()
        elif cmd == 'report':
            await cmd_report()
        elif cmd == 'setup_stats':
            cmd_setup_stats()
        elif cmd == 'pos':
            """从Redis读持仓（毫秒级，不调HTTP）"""
            redis_init()
            keys = REDIS_CLIENT.keys(f'{RK_POS}*') if REDIS_CLIENT else []
            if not keys:
                print("📭 空仓")
            for k in sorted(keys):
                sym = k.replace(RK_POS, '')
                val = REDIS_CLIENT.get(k)
                if val:
                    p = json.loads(val)
                    amt = float(p.get('positionAmt',0))
                    if abs(amt) > 0:
                        side = 'LONG' if amt > 0 else 'SHORT'
                        print(f"  {sym.upper()} {side} {abs(amt):.4f} @${float(p['entryPrice']):.4f}")
                    else:
                        print(f"  {sym.upper()} 已平")
                else:
                    print(f"  {sym.upper()} 无数据")
            print(f"💰 ${redis_get('bal') or '?'}")
        elif cmd == 'price' and len(sys.argv) >= 3:
            """从Redis读币种最新K线（毫秒级）"""
            sym = sys.argv[2].lower()
            redis_init()
            raw = redis_get(f'{RK_KLINES}{sym}')
            if raw:
                k = json.loads(raw)
                print(f"{sym.upper()} ${k['c']:.4f} | O={k['o']:.4f} H={k['h']:.4f} L={k['l']:.4f}")
            else:
                # 兜底：HTTP查询
                import subprocess as _sp
                _r = _sp.run(['curl', '-s', f'https://fapi.binance.com/fapi/v1/ticker/price?symbol={sys.argv[2].upper()}'], capture_output=True, text=True, timeout=10)
                try:
                    _d = json.loads(_r.stdout)
                    print(f"{sys.argv[2].upper()} ${_d.get('price','?')} (HTTP)")
                except:
                    print(f"{sys.argv[2].upper()} ? (HTTP失败: {_r.stdout[:100]})")
        else: print("用法: futures_trader.py [positions|scan|stats|resume|report|setup_stats|short SYM QTY|long SYM QTY|price SYM|pos]")
    else:
        await main_auto()

if __name__ == '__main__':
    asyncio.run(main())
