# 资产管理清单
## 最后更新: 2026-05-29 16:45 UTC

## 活动脚本（2个）
| 脚本 | 功能 | 状态 |
|------|------|------|
| `futures_trader.py` | 统一合约交易系统（875行） | ✅ 可用 |
| `spot_trader.py` | 统一现货交易系统（883行） | ✅ 可用 |

## 已封存脚本（19个）

### backup/futures/ — 合约交易
| 脚本 | 整合去向 | 封存原因 |
|------|---------|---------|
| `auto_trader.py` | futures_trader.py | 合并到核心引擎 |
| `daemon_ws.py` | futures_trader.py | 旧版，已升级为统一版 |
| `setup_algos.py` | futures_trader.py | 条件单管理器统一 |
| `setup_6pct_algos.py` | futures_trader.py | 同上 |
| `place_algo_short.py` | futures_trader.py | 手动命令集成 |
| `reopen_algo.py` | futures_trader.py | 条件单管理器统一 |
| `fix_algo.py` | futures_trader.py | 同上 |

### backup/monitors/ — 止损监控
| 脚本 | 整合去向 | 封存原因 |
|------|---------|---------|
| `bnb_stop_loss_monitor.py` | futures_trader.py | 统一止损管理器替代 |
| `eth_stop_loss_monitor.py` | futures_trader.py | 同上 |
| `ondo_stop_loss_monitor.py` | futures_trader.py | 同上 |
| `trx_stop_loss_monitor.py` | futures_trader.py | 同上 |
| `zec_stop_loss_monitor.py` | futures_trader.py | 同上 |
| `zec_entry_monitor.py` | futures_trader.py | 同上 |
| `zec_position_manager.py` | futures_trader.py | 同上 |
| `tao_monitor.py` | futures_trader.py | 同上 |
| `monitor_signals.py` | futures_trader.py | 诊断模块统一 |

### backup/spot/ — 现货交易
| 脚本 | 整合去向 | 封存原因 |
|------|---------|---------|
| `spcx_auto_buy.py` | spot_trader.py | 安全转账模块，需手动确认 |

### backup/tools/ — 工具类
| 脚本 | 整合去向 | 封存原因 |
|------|---------|---------|
| `check_positions.py` | futures_trader.py | 集成到positions命令 |
| `market_scanner.py` | futures_trader/spot | 集成到scan命令 |

## 系统服务（已清理）
| 服务 | 状态 | 操作 |
|------|------|------|
| `binance-trader.service` | ❌ 已删除 | 防自动重启 |
| `spot-trader.service` | ❌ 已删除 | 防自动重启 |

## 当前账户
- 合约: $0.00 USDT
- 现货: $0.00 USDT + 灰尘
- 无脚本运行，无自动交易
