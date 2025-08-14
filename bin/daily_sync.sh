#!/bin/bash

# 每日数据同步脚本 - Shell版本（真实数据版）
# 使用方法：./daily_sync.sh
# 功能：从Presto数据库同步真实数据

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/daily_sync_real.py"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 检查Python脚本是否存在
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo -e "${RED}❌ 错误: 找不到同步脚本 $PYTHON_SCRIPT${NC}"
    exit 1
fi

# 记录开始时间
START_TIME=$(date)
echo -e "${BLUE}🚀 启动每日数据同步...${NC}"
echo -e "${BLUE}📅 开始时间: $START_TIME${NC}"

# 执行Python同步脚本
python3 "$PYTHON_SCRIPT"
EXIT_CODE=$?

# 记录结束时间
END_TIME=$(date)
echo ""
echo -e "${BLUE}⏰ 结束时间: $END_TIME${NC}"

# 根据退出码显示结果
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}🎉 每日同步成功完成！${NC}"
else
    echo -e "${RED}❌ 每日同步失败，请检查错误信息${NC}"
fi

# 显示日志位置
echo -e "${BLUE}📝 详细日志: $SCRIPT_DIR/daily_sync.log${NC}"

exit $EXIT_CODE