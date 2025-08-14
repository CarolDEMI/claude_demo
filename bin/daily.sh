#!/bin/bash
# 每日必用脚本：同步昨天数据 + 生成报告 + 自动打开

cd "$(dirname "$0")/.."  # 切换到项目根目录

echo "🚀 每日数据处理开始..."
echo "📅 处理昨天的数据和报告"

# 1. 先同步昨天的数据
echo "🔄 步骤 1/2: 同步昨天的数据..."
python3 main.py --sync yesterday

if [ $? -eq 0 ]; then
    echo "✅ 数据同步成功"
    
    # 2. 生成报告并自动打开
    echo "📊 步骤 2/2: 生成报告并打开..."
    python3 main.py --report yesterday --auto-open
    
    if [ $? -eq 0 ]; then
        echo "🎉 每日处理完成！报告已打开"
        
        # 3. 显示快速状态
        echo ""
        python3 main.py --status
    else
        echo "⚠️ 报告生成失败，但数据已同步"
    fi
else
    echo "❌ 数据同步失败，请检查网络或配置"
    echo "💡 建议手动运行: python3 main.py --health"
fi