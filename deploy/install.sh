#!/usr/bin/env bash
set -e

# ═══════════════════════════════════════════════════
# Futures Trader — 一键部署脚本
# ═══════════════════════════════════════════════════

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPTS_DIR="$PROJECT_DIR/scripts"
DEPLOY_DIR="$PROJECT_DIR/deploy"

echo -e "${CYAN}╔══════════════════════════════════════╗${NC}"
echo -e "${CYAN}║    Futures Trader 一键部署           ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════╝${NC}"
echo ""

# ── 1. 检查 Python ──
echo -e "${YELLOW}[1/6] 检查运行环境...${NC}"
if ! command -v python3 &>/dev/null; then
    echo -e "${RED}❌ 未找到 python3，请先安装 Python 3.9+${NC}"
    exit 1
fi
PY_VER=$(python3 --version 2>&1 | grep -oP '\d+\.\d+')
echo -e "  ✅ Python $PY_VER"
echo ""

# ── 2. 检查 Redis ──
echo -e "${YELLOW}[2/6] 检查 Redis...${NC}"
if command -v redis-cli &>/dev/null && redis-cli ping 2>/dev/null | grep -q PONG; then
    echo -e "  ✅ Redis 运行中"
else
    echo -e "  ⚠️  Redis 未运行，尝试启动..."
    if command -v systemctl &>/dev/null; then
        sudo systemctl start redis 2>/dev/null && echo -e "  ✅ Redis 已启动" || true
    fi
    if command -v redis-server &>/dev/null; then
        redis-server --daemonize yes 2>/dev/null && echo -e "  ✅ Redis 已启动(后台)" || true
    fi
    # 再次确认
    if redis-cli ping 2>/dev/null | grep -q PONG; then
        echo -e "  ✅ Redis 运行中"
    else
        echo -e "  ${YELLOW}⚠️  请手动安装 Redis: apt install redis-server${NC}"
    fi
fi
echo ""

# ── 3. 安装依赖 ──
echo -e "${YELLOW}[3/6] 安装 Python 依赖...${NC}"
pip3 install -q -r "$DEPLOY_DIR/requirements.txt" 2>&1 || pip install -q -r "$DEPLOY_DIR/requirements.txt" 2>&1 || {
    echo -e "  ${YELLOW}⚠️  pip安装失败，尝试逐项安装...${NC}"
    pip3 install redis websockets requests 2>/dev/null || pip install redis websockets requests 2>/dev/null
}
echo -e "  ✅ 依赖安装完成"
echo ""

# ── 4. 配置环境变量 ──
echo -e "${YELLOW}[4/6] 配置 API 密钥...${NC}"
if [ -f "$PROJECT_DIR/.env" ]; then
    echo -e "  ✅ 已存在 .env 文件"
else
    if [ -f "$DEPLOY_DIR/.env.template" ]; then
        cp "$DEPLOY_DIR/.env.template" "$PROJECT_DIR/.env"
    fi
    echo -e "  ${YELLOW}⚠️  请编辑 ${PROJECT_DIR}/.env 填入你的币安API密钥${NC}"
    echo -e "  ${YELLOW}    API需要: 合约交易权限 + 读取权限${NC}"
    echo ""
    echo -n "  是否现在编辑？(y/n): "
    read -r ans
    if [ "$ans" = "y" ]; then
        nano "$PROJECT_DIR/.env" 2>/dev/null || vi "$PROJECT_DIR/.env"
    fi
fi
echo ""

# ── 5. 安装 systemd 服务（可选） ──
echo -e "${YELLOW}[5/6] 安装系统服务...${NC}"
if command -v systemctl &>/dev/null; then
    sudo cp "$DEPLOY_DIR/futures-trader.service" /etc/systemd/system/
    sudo cp "$DEPLOY_DIR/binance-trader.service" /etc/systemd/system/ 2>/dev/null || true
    sudo systemctl daemon-reload
    echo -e "  ✅ systemd 服务已安装"
    echo -e "  ${GREEN}  启动: sudo systemctl start futures-trader${NC}"
    echo -e "  ${GREEN}  开机自启: sudo systemctl enable futures-trader${NC}"
    echo -e "  ${GREEN}  查看日志: journalctl -u futures-trader -f${NC}"
else
    echo -e "  ${YELLOW}  ⚠️  未检测到 systemd，跳过服务安装${NC}"
    echo -e "  ${YELLOW}  手动启动: cd $SCRIPTS_DIR && python3 futures_trader.py${NC}"
fi
echo ""

# ── 6. 启动 ──
echo -e "${YELLOW}[6/6] 启动交易系统...${NC}"
if [ -f "$PROJECT_DIR/.env" ]; then
    # 先检查API是否已配置
    if grep -q "your_a" "$PROJECT_DIR/.env" 2>/dev/null; then
        echo -e "  ${YELLOW}⚠️  .env 还是模板内容，请先填入真实API密钥再启动${NC}"
    else
        if command -v systemctl &>/dev/null; then
            sudo systemctl start futures-trader
            echo -e "  ✅ futures-trader 服务已启动"
        else
            cd "$SCRIPTS_DIR" && nohup python3 -u futures_trader.py > /tmp/futures_trader.log 2>&1 &
            echo -e "  ✅ 进程已后台启动 (PID: $!)"
        fi
        echo ""
        echo -e "  ${GREEN}  📊 查看持仓: python3 $SCRIPTS_DIR/futures_trader.py positions${NC}"
        echo -e "  ${GREEN}  📡 手动扫描: python3 $SCRIPTS_DIR/futures_trader.py scan${NC}"
        echo -e "  ${GREEN}  📋 交易统计: python3 $SCRIPTS_DIR/futures_trader.py stats${NC}"
    fi
else
    echo -e "  ${RED}⚠️  请先配置 .env 文件后再启动${NC}"
fi
echo ""
echo -e "${GREEN}✅ 部署完成！更多说明见 README.md${NC}"
