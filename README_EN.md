# Futures Trader — Binance Perpetual Futures Auto-Trading System

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](LICENSE)
## [中文](README.md) | **English**

> 🚀 **Running Live on Mainnet | Multi-Factor Scoring for Coin Selection | WebSocket Real-Time Data | Multi-Layer Risk Control**
>
> 📖 **[Changelog →](CHANGELOG.md)**

An automated trading bot running live on Binance perpetual futures markets. This isn't a paper-trading strategy experiment — it's a **battle-tested system forged with real money in live markets**.

---

## 📈 Live Trading Results & Experience

### Core Stats (Since Deployment)

- **Win Rate**: ~43% (32 winning trades out of 74)
- **Net P&L**: +$1.85 (after fees)
- **Max Intraday Drawdown**: Tightly controlled, most losses <$0.50 per trade
- **Largest Single Win**: +$48.70

> 💡 **How does a 43% win rate make money?** Risk-reward ratio. The average winning trade is 3–5× the average losing trade. Don't chase high win rates — chase "small losses, big wins."

### Lessons We Learned the Hard Way (Real War Stories)

#### 🔴 Lesson 1: Chasing Pumps & Dumps Is the #1 Source of Losses
> "Better to miss the move than to chase the top or bottom."

First principle of the system: **Only go long on a pullback to support. Only go short on a bounce to resistance.** Before entering, ask yourself: Am I chasing? Am I panic-selling?

**Code implementation**: Candle body filter — if the current candle body exceeds ATR × 0.3, reject the entry (no chasing).

#### 🔴 Lesson 2: Fees Eat Half Your Profits
Total fees on 74 trades: ~$1.85 — nearly equal to total net profit. **With a small account, frequent trading is slow suicide.**

**Countermeasures**:
- Cooldown mechanism: after a loss on a coin, cooldown 1–6 hours depending on loss size
- Signal threshold: no entry if score < 0.65
- Base cooldown: don't touch the same coin for 1 hour after opening a position

#### 🔴 Lesson 3: Dual Instances Corrupt Your Data
> Two daemons running at once — one fills an order, the other also records it, data is completely garbage.

**Countermeasure**: PID lock on startup prevents duplicate instances; systemd manages single-instance operation.

#### 🔴 Lesson 4: Don't Trust Mark Price for P&L Calculations
> P&L estimated from markPrice can differ from the actual fill price. **Always use the order's average fill price (avgPrice) and commission as the source of truth.**

**Countermeasure**: The close-position function pulls the actual average fill price directly from Binance's order API, accounting for fees in the record.

#### 🔴 Lesson 5: Direction Locks Are Neither Casual to Add Nor Remove
Had a direction lock in place (forced reversal to longs after 6 consecutive shorts). A sloppy patch accidentally deleted it, causing ~$6 in losses. **Before touching code, write a checklist, verify the blast radius, then make the change.**

---

## 🏗 System Architecture

```
┌─────────────────────────────────────────────────────┐
│              Production Environment (Server)         │
│                                                      │
│  data_collector.py    futures_trader.py               │
│  (WS Data Collection)  (Trading Engine)               │
│       │                    │                          │
│       │   ┌──────────┐    │                          │
│       └──→│  Redis   │←───┘                          │
│           │ (Cache)   │                                │
│           └────┬─────┘                                │
│                │                                      │
│                ↓                                      │
│         Telegram Push (Live Monitor/Alerts/Daily Report)│
└─────────────────────────────────────────────────────┘
         ↑
   You/AI Assistant (read Redis, zero HTTP, millisecond response)
```

### Data Flow

1. **WebSocket Subscriptions**: 15m K-lines, account updates, and funding rates for 150+ coins → streamed in real-time into Redis
2. **Trading Engine**: Reads from Redis every 30–60 seconds → multi-factor scoring → opens positions when conditions are met
3. **Risk Control Layer**: ±6% OCO orders, waterfall protection, sweep protection, consecutive-loss pause → fully automated
4. **Human-AI Collaboration**: AI assistant reads data via Redis for decision support — never calls Binance API directly

> 🎯 **Why Redis as the middle layer?** Decouples the data collector from the trading engine. If WebSocket disconnects, Redis still holds cached data. AI reads Redis ~10× faster than HTTP. Most importantly — scanning cycles never get blocked by ad-hoc HTTP requests.

---

## ⚙️ Quick Deploy

```bash
# 1. Clone
git clone https://github.com/roshanbenshan/futures-trader.git
cd futures-trader

# 2. One-click install (dependencies + systemd service)
bash deploy/install.sh

# 3. Configure API keys
nano .env    # Fill in BINANCE_API_KEY and BINANCE_API_SECRET
             # API needs: Futures trading permission + Read permission + Internal Transfer enabled

# 4. Start
sudo systemctl start futures-trader
sudo systemctl status futures-trader    # Verify it's running
```

### Environment Requirements

- **Python 3.9+** — Core runtime
- **Redis** — Data cache layer (memory footprint <50MB)
- **pip packages**: `redis`, `websockets`, `requests`, `pandas`
- **OS**: Ubuntu/Debian (recommended; one-click install script handles config)

---

## 🎯 Core Strategy: Multi-Factor Scoring System

### Seven Dimensions for Coin Selection

| Factor | Weight | Description |
|--------|--------|-------------|
| Trend Direction (MA7/MA14) | 0.22 | Moving average crossover to determine long/short trend |
| RSI | 0.15 | Oversold/overbought zone detection |
| Volume Confirmation | 0.12 | Breakout on volume / pullback on shrinking volume |
| Risk-Reward Ratio (RR) | 0.15 | Expected profit / stop-loss distance ≥ 3:1 |
| Volatility (ATR) | 0.08 | Bollinger Band width confirms appropriate volatility |
| Funding Rate | 0.08 | Funding rate direction signal (mean-reversion indicator) |
| Cross-Timeframe Momentum (1h) | 0.13 | Higher-timeframe direction filter |
| Momentum Candle Body Filter | 0.07 | Rejects when bullish/bearish candle body exceeds ATR × 0.3 |

**Total score ≥ 0.65 to open a position**

### Three Iron Laws (Factor Design Principles)

1. **Dimensionless**: All factors normalized to [0,1] range, no dependence on absolute price
2. **Richness**: Must cover at least three dimensions — technicals, capital flow, and cross-timeframe
3. **No Future Leakage**: No future data introduced; all calculations based on completed historical candles

---

## 🛡 Risk Control System (Six Layers of Defense)

### Layer 1: Entry Filtering
- **BTC Circuit Breaker**: BTC ADX > 50 + RSI extreme → market anomaly, bidirectional lockout (only independently strong coins can bypass)
- **Direction Pause**: 3+ consecutive losses on one side → pause that direction for 12 hours
- **Cooldown Mechanism**: Loss <$0.10 → 1h cooldown / $0.10–$0.50 → 3h / >$0.50 → 6h

### Layer 2: Capital Management
- **Equal split of 33% of total balance**: 3 positions at 1/3 each
- **Safety Valve**: When position margin + open orders exceed 80% of total balance, force position size reduction
- **5× Leverage fixed**: No gambling on leverage

### Layer 3: OCO Order Protection (±6% Rule)
```
Long:  SL = entry × 0.988  (-1.2% = -6% margin)
       TP = entry × 1.012  (+1.2% = +6% margin)
Short: SL = entry × 1.012
       TP = entry × 0.988
Floor: SL = max(1.2%, ATR × 0.5)
```

### Layer 4: Live Monitoring
- **Waterfall Protection**: Price drops 3% rapidly → force close
- **Sweep Protection**: 15m large bullish/bearish candle with rapid reversal
- **Trailing Stop on Profit**: Automatic take-profit when profit retracement hits threshold
- **Reversal Alert**: RSI + MA indicator combo monitors trend reversal

### Layer 5: Data Integrity
- Orders with volume = 0 are rejected from recording
- Exit prices ≤ 0 are rejected as bad data
- Redis data cross-verified daily against Binance API

### Layer 6: System Level
- systemd auto-restart (back in 5 seconds after crash)
- Dual-instance detection (PID lock on startup)
- Single-instance guard (rejects duplicate launches)
- Log redirection to file (stdout lost after double-fork)

---

## 📊 Manual Commands

```bash
cd ~/futures-trader/scripts
python3 futures_trader.py scan        # Scan market manually (see current opportunities)
python3 futures_trader.py positions   # View positions (read from Redis, millisecond response)
python3 futures_trader.py stats       # View trading statistics
python3 futures_trader.py resume      # Resume paused trading
python3 futures_trader.py report      # Generate trading report
python3 futures_trader.py setup_stats # Per-strategy win rate stats
```

---

## 📡 Monitoring & Notifications

The system automatically pushes the following via Telegram:

- **Open/Close Notifications**: Entry price, quantity, score, stop-loss/take-profit levels
- **Position Snapshot (30 min)**: Floating P&L, current monitoring status
- **Risk Alerts**: Reversal alert, sweep, waterfall triggers
- **Daily Review (UTC 00:00)**: Win rate, net P&L, factor analysis

---

## 🧪 Test Mode

```bash
TEST_MODE=true python3 futures_trader.py
```

All K-lines, indicators, trends, and funding rates come from Binance WebSocket real-time push (real data). **It just doesn't place or pend orders.** Perfect for validating signal quality.

---

## 💡 Advice from Production

### If You're Going Live

1. **Start small**: $30–$50 is enough. Run through all the workflows first.
2. **Check signal quality first**: Run test mode for 3–5 days. See if the scores and direction judgments make sense.
3. **Don't tweak parameters out of desperation**: The biggest trap — after a loss, widen the stop-loss and increase position size. Stability comes first.
4. **Fees are enemy #1**: The smaller your capital, the higher fees cut into it. Trade less, trade smarter.
5. **3 consecutive days of no profit is a signal**: The market might not suit the strategy right now. It doesn't mean the strategy is broken.

### Common Issues

- **No opportunities found** → Check BTC circuit breaker status: `python3 futures_trader.py scan` and watch the logs
- **Frequent entries getting stopped out** → ATR might be too small causing tight stops; check if signal scores are too low
- **Should have opened but didn't** → Check cooldown period Redis key `cooldown:*`, check if capital quota is maxed out
- **Data looks wrong** → Check if data_collector is running, if Redis connection is healthy

---

## 📁 File Structure

```
futures-trader/
├── scripts/
│   ├── futures_trader.py      # Main trading engine (3200+ lines, core code)
│   ├── data_collector.py      # WebSocket data collector
│   ├── daemon_launcher.py     # Daemon process launcher
│   ├── daily_review.py        # Daily review report
│   ├── monitor_push.py        # Live monitoring push (Telegram)
│   ├── monitor_events.py      # Event handler
│   ├── heartbeat.py           # Heartbeat monitor
│   ├── health_watchdog.py     # Health watchdog
│   └── _check_acct.py         # Account balance check
├── deploy/
│   ├── install.sh             # One-click deploy script
│   ├── .env.template          # Environment variable template
│   ├── requirements.txt       # Python dependencies
│   └── futures-trader.service # systemd service config
├── .env                       # API keys (.gitignored)
├── .gitignore
└── README.md
```

---

## 🔄 Version

Current version: v1.0 — Production-stable release.

Already running live for several weeks, validated through a complete market cycle. Continuously iterating.

---

## 📄 License

[GNU General Public License v3 (GPL v3)](LICENSE)

Derivative works **must remain open source** — no closed-source commercialization. Protecting community sharing so no one can slap a price tag on open code.

---

## ⚠️ Disclaimer

**Cryptocurrency trading carries significant risk. This system is provided for educational and research purposes only.** Before using, fully understand:

- Markets may experience insufficient liquidity, wicks, exchange outages, and other uncontrollable risks
- No trading strategy can guarantee 100% profitability
- Only trade with money you can afford to lose

---

## ⭐ If You Find This Useful

Stars, Forks, and PRs are welcome. Real-world experience shared openly is the greatest value of open source.
