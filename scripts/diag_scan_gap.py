#!/usr/bin/env python3
"""诊断: daemon vs 汇报脚本的扫描差距"""
import subprocess, json, time, os, math
from datetime import datetime

api_key = os.environ['BINANCE_API_KEY']

def curl_get(url):
    r = subprocess.run(['curl','-s',url,'-H',f'X-MBX-APIKEY:{api_key}'],capture_output=True,text=True,timeout=10)
    return json.loads(r.stdout) if r.stdout else {}

def sma(v,p): return sum(v[-p:])/p if len(v)>=p else 0
def rsi(c,p=14):
    if len(c)<p+1: return 50
    g=[max(c[i]-c[i-1],0) for i in range(1,len(c))]
    l=[max(c[i-1]-c[i],0) for i in range(1,len(c))]
    ag=sum(g[:p])/p;al=sum(l[:p])/p
    for i in range(p,len(g)): ag=(ag*(p-1)+g[i])/p;al=(al*(p-1)+l[i])/p
    return 100-100/(1+ag/al) if al>0 else 100
def adx_func(h,l,c,p=14):
    if len(h)<p+1: return 0
    trs,pd2,md2=[],[],[]
    for i in range(1,len(h)):
        trs.append(max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])))
        um=h[i]-h[i-1];dm=l[i-1]-l[i]
        pd2.append(um if um>dm and um>0 else 0);md2.append(dm if dm>um and dm>0 else 0)
    if len(trs)<p: return 0
    a2=sum(trs[:p])/p;pi2=sum(pd2[:p])/p;mi2=sum(md2[:p])/p;dx=[]
    for i in range(p,len(trs)):
        a2=(a2*(p-1)+trs[i])/p;pi2=(pi2*(p-1)+pd2[i])/p;mi2=(mi2*(p-1)+md2[i])/p
        pp=pi2/a2*100 if a2>0 else 0;mm=mi2/a2*100 if a2>0 else 0
        dx.append(abs(pp-mm)/(pp+mm)*100 if pp+mm>0 else 0)
    return sum(dx[:p])/p if dx else 0

print(f"📊 诊断: daemon vs 汇报 扫描差距分析")
print(f"时间: {datetime.utcnow().strftime('%H:%M UTC')}")
print()

# 获取top20
tk=curl_get('https://fapi.binance.com/fapi/v1/ticker/24hr')
usdt=[x for x in tk if x.get('symbol','').endswith('USDT')]
usdt.sort(key=lambda x:float(x.get('quoteVolume',0)),reverse=True)
top20=[x['symbol'] for x in usdt[:20]]

print("═"*70)
print("【A】汇报脚本: REST 30根K线（每个币种完整历史）")
print("═"*70)

total_30=0
for sym in top20:
    ss=sym.replace('USDT','')
    k=curl_get(f'https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=15m&limit=30')
    if not isinstance(k,list) or len(k)<25: print(f"  {ss:6s} ❌ 数据不足");continue
    c=[float(v[4]) for v in k];h=[float(v[2]) for v in k];l=[float(v[3]) for v in k]
    p=c[-1];m7=sma(c,7);m14=sma(c,14);r=rsi(c);adx=adx_func(h,l,c)
    score=0
    reason=[]
    if adx<20: reason.append(f"ADX{adx:.0f}<20")
    if m7>m14:
        if not(35<=r<=72): reason.append(f"RSI{r:.0f}")
        elif not(p>m14 and p<m7*1.03): reason.append(f"价{p:.4f}离MA7={m7:.4f}")
        else: score=1
    elif m7<m14:
        if not(r<=60): reason.append(f"RSI{r:.0f}>60")
        elif not(p<m14 and p>m7*0.97): reason.append(f"价{p:.4f}离MA7")
        else: score=1
    else: reason.append("趋势不明")
    emoji='✅' if score else '❌'
    tag=' = '.join(reason) if reason else '通过'
    print(f"  {emoji} {ss:6s} ADX{adx:.0f} R{r:.0f} MA7{m7:.4f} MA14{m14:.4f} 价{p:.4f}  → {tag}")
    total_30+=score
print(f"  → 报告通过: {total_30}个")

print()
print("═"*70)
print("【B】模拟daemon WS缓冲区: 只用当前实时K线（无历史）")
print("═"*70)
print("  daemon的kline_data从WS累积, 重启后缓冲区为空")
print("  检查条件: len(kline_data[sym]) >= 25 且 filtered(completed) >= 25")
print()

# 只用5根K线（模拟刚重启后仅有的实时数据）
total_5=0
for sym in top20:
    ss=sym.replace('USDT','')
    k=curl_get(f'https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=15m&limit=5')
    if not isinstance(k,list) or len(k)<5: continue
    c=[float(v[4]) for v in k];h=[float(v[2]) for v in k];l=[float(v[3]) for v in k]
    p=c[-1]
    completed=[v for v in k if v[7]]  # 已完结的K线
    if len(c)<25:
        print(f"  ❌ {ss:6s} 仅{len(c)}根K线 <25 → 被len(klines)<25 过滤, 进不了scanner")
    total_5+=1

print(f"\n  → WS仅5根K线: 0个进入scanner（全部被缓冲区大小检查挡住）")

print()
print("═"*70)
print("【C】需要WS运行多久才能积累足够数据？")
print("═"*70)
# daemon的kline_data是deque(maxlen=42), 通过WebSocket实时追加
# 每次kline更新(不论final与否)都append一个cdl
# 15m间隔: 每小时4根completed klines
# 要25根completed → 25/4 = 6.25小时
print("  15m K线: 每小时产生4根completed klines")
print("  需要25根completed klines → 最少6.25小时连续运行")
print("  但如果daemon频繁重启/断开: 永远凑不够数据")
print()
# 检查daemon存活时间
lf='/tmp/futures_trader.lock'
if os.path.exists(lf):
    with open(lf) as f:
        try: pid=int(f.read().strip())
        except: pid=0
    try:
        os.kill(pid, 0)
        import subprocess as sp
        r=sp.run(['ps','-p',str(pid),'-o','etimes='],capture_output=True,text=True,timeout=5)
        uptime=r.stdout.strip()
        print(f"  当前daemon PID={pid} 运行时间: {uptime}秒")
    except:
        print(f"  ❌ 锁文件PID={pid} 但进程已死 (看门狗未修复)")
        print(f"  → daemon实际已死, 0数据积累")
else:
    print(f"  ❌ 无锁文件 → daemon未运行")

print()
print("═"*70)
print("【D】额外关卡: daemon的notional≥$10检查（汇报不检查）")
print("═"*70)
bal=25.45
for sym in top20[:10]:
    ss=sym.replace('USDT','')
    k=curl_get(f'https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=15m&limit=3')
    if not isinstance(k,list) or len(k)<3: continue
    p=float(k[-1][4])
    max_q=(bal*5)/p
    step=0.001 if p>100 else 0.0001 if p>1 else 0.00001 if p>0.01 else 0.000001
    qty=math.floor(max_q/step)*step
    notional=qty*p
    print(f"  {ss:6s} 价${p:<10.4f} 可买{qty:<10.4f}枚 = ${notional:<8.2f}  {'✅' if notional>=10 else '❌'} (需≥$10)")

print()
print("═"*70)
print("📋 结论: 3层互斥原因")
print("═"*70)
print("""
① 数据层互斥（主要）
   汇报: REST 30根K线 → 立即有完整历史
   daemon: WS实时流 → 重启后缓冲区空 → len<25全部过滤
   结果: daemon永远看不到汇报通过的币种

② 数据量层互斥（次要）
   即使daemon不重启, 要累积25根completed klines至少6小时
   期间任何WS断开/重连都会丢失积累（deque重新填）

③ 保证金检查层（隐蔽）
   汇报不检查notional≥$10, daemon检查
   若某币种凑够数据进了scanner, 也可能被min notional挡

修复方向: daemon启动时先用REST预加载历史K线
   避免纯WS冷启动导致的"有汇报无交易"问题
""")
