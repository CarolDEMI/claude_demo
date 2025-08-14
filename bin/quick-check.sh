#!/bin/bash
# 快速健康检查脚本

cd "$(dirname "$0")/.."  # 切换到项目根目录

echo "🔍 快速系统检查..."

# 1. 数据状态
echo "📊 数据状态:"
python3 main.py --status
echo ""

# 2. 系统健康
echo "🏥 系统健康:"
python3 main.py --health
echo ""

# 3. 磁盘空间
echo "💾 磁盘空间:"
df -h . | tail -1 | awk '{print "使用: " $5 " (" $3 "/" $2 ")"}'

# 4. 数据库大小
if [ -f "data/data.db" ]; then
    DB_SIZE=$(du -h data/data.db | cut -f1)
    echo "📁 数据库大小: $DB_SIZE"
else
    echo "⚠️ 数据库文件不存在"
fi

echo ""
echo "✅ 检查完成"