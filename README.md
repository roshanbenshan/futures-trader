# Futures Trader — Binance合约自动交易系统

## 系统架构

```
data_collector.py  ──── WS ────→ Redis ←──── futures_trader.py
（K线/账户/资金费率）          （缓存层）    （扫描/开仓/平仓）
                                      ↑
                                  你/AI（读Redis，零HTTP）
```

## 快速部署

```bash
# 1. 解压
tar xzf futures-trader.tar.gz
cd futures-trader

# 2. 一键部署
bash deploy/install.sh

# 3. 配置API密钥
nano .env    # 填入 BINANCE_API_KEY 和 BINANCE_API_SECRET

# 4. 启动
sudo systemctl start futures-trader
```

## 手动命令

```bash
cd scripts
python3 futures_trader.py positions   # 查看持仓（从Redis读取，毫秒级）
python3 futures_trader.py scan        # 手动扫描市场
python3 futures_trader.py stats       # 查看交易统计
python3 futures_trader.py resume      # 恢复暂停交易
python3 futures_trader.py report      # 生成交易报告
python3 futures_trader.py setup_stats # 各策略胜率统计
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `futures_trader.py` | 主交易引擎：扫描、开仓、平仓、风控 |
| `data_collector.py` | WebSocket数据收集器：K线、账户、资金费率 |
| `daemon_launcher.py` | 双fork守护进程启动器 |
| `daily_review.py` | 每日复盘报告 |
| `monitor_push.py` | 盘中监测推送 |
| `deploy/install.sh` | 一键部署脚本 |
| `deploy/.env.template` | 环境变量模板 |
| `.env` | 币安API密钥配置 |

## 依赖

- Python 3.9+
- Redis (数据缓存层)
- pip包: redis, websockets, requests

## 风控特性

- 多因子评分选币 (趋势/RSI/成交量/盈亏比/波动率/资金费率/跨周期)
- ±6%条件单止盈止损
- 瀑布保护(急跌3%自动平仓)
- 横扫保护(大阳线/大阴线)
- 回调跟踪止损
- 连败暂停保护
- 大盘断路器(基于BTC走势)
- 亏损加冷却(大亏6小时不碰同一币种)
- 信号最低分数门槛(0.65)
- Redis全量数据镜像
