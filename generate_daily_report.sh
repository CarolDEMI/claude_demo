#!/bin/bash
# 标准报告生成脚本

echo "📊 每日业务数据报告生成器"
echo "=========================="

# 检查参数
if [ $# -eq 0 ]; then
    echo "⚠️  使用方法: ./generate_daily_report.sh YYYY-MM-DD"
    echo "📅 示例: ./generate_daily_report.sh 2025-07-28"
    echo ""
    echo "🔍 正在查询最近可用的日期..."
    sqlite3 data/data.db "SELECT DISTINCT dt FROM cpz_qs_newuser_channel_i_d ORDER BY dt DESC LIMIT 10;" 2>/dev/null || echo "❌ 无法访问数据库"
    exit 1
fi

DATE=$1

# 验证日期格式
if ! [[ $DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "❌ 日期格式错误！请使用 YYYY-MM-DD 格式"
    exit 1
fi

echo "🚀 正在生成 $DATE 的报告..."
echo ""

# 先同步最近3天的数据，确保次留率等指标准确
echo "📥 同步最近数据以确保准确性..."
./sync_recent_data.sh 3
echo ""

# 使用标准报告生成器
python3 generate_standard_report.py $DATE

# 检查生成结果
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ 报告生成成功！"
    echo "📁 报告位置: ./output/reports/daily_reports/"
    echo ""
    echo "💡 提示: 报告已按照标准模板生成，包含两个模块："
    echo "   - 模块一：大盘指标 (MAIN KPI、用户质量、注册转化)"
    echo "   - 模块二：异常分析 (异常检测、渠道分析)"
else
    echo ""
    echo "❌ 报告生成失败，请检查："
    echo "   1. 数据库中是否有 $DATE 的数据"
    echo "   2. 数据库路径是否正确 (./data/data.db)"
    echo "   3. Python环境是否正常"
fi