#!/bin/bash
# 同步最近几天的数据，确保次留率等指标准确
# Usage: ./sync_recent_data.sh [days]

# 同步天数，默认3天
DAYS=${1:-3}

echo "📊 数据同步工具"
echo "=================="
echo "📅 准备同步最近 $DAYS 天的数据..."
echo ""

# 确保配置文件存在
if [ ! -f "src/presto_config.py" ]; then
    echo "📋 复制配置文件..."
    cp config/presto_config.py src/
fi

# 获取今天的日期
TODAY=$(date +%Y-%m-%d)

# 同步最近几天的数据
for i in $(seq 0 $((DAYS-1))); do
    # 计算日期
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        SYNC_DATE=$(date -v-${i}d +%Y-%m-%d)
    else
        # Linux
        SYNC_DATE=$(date -d "$TODAY -$i days" +%Y-%m-%d)
    fi
    
    echo "🔄 同步 $SYNC_DATE 的数据..."
    python3 src/presto_sync.py --date $SYNC_DATE
    
    if [ $? -eq 0 ]; then
        echo "✅ $SYNC_DATE 同步成功"
    else
        echo "❌ $SYNC_DATE 同步失败"
    fi
    echo ""
done

echo "🎉 数据同步完成！"
echo ""

# 同步账户数据（测试成本表）
echo "📊 同步账户数据..."
python3 src/presto_sync.py --sync-test-cost

if [ $? -eq 0 ]; then
    echo "✅ 账户数据同步成功"
else
    echo "❌ 账户数据同步失败"
fi
echo ""

# 显示最新的数据概况
echo "📊 数据概况："
python3 -c "
import sqlite3
import pandas as pd

conn = sqlite3.connect('./data/data.db')

# 用户数据概况
user_query = '''
SELECT 
    dt,
    COUNT(DISTINCT ad_channel) as channels,
    SUM(newuser) as total_users,
    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users,
    ROUND(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN is_returned_1_day ELSE 0 END) * 100.0 / 
          NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0), 2) as retention_rate
FROM cpz_qs_newuser_channel_i_d
WHERE dt >= date('now', '-$DAYS days')
GROUP BY dt
ORDER BY dt DESC
'''
user_df = pd.read_sql_query(user_query, conn)

# 素材数据概况
creative_query = '''
SELECT 
    dt,
    COUNT(DISTINCT ad_creative_id_str) as creative_count,
    SUM(total_good_verified) as creative_users,
    SUM(cash_cost) as creative_cost
FROM dws_ttx_market_media_reports_i_d
WHERE dt >= date('now', '-$DAYS days')
GROUP BY dt
ORDER BY dt DESC
'''
try:
    creative_df = pd.read_sql_query(creative_query, conn)
    has_creative_data = not creative_df.empty
except:
    has_creative_data = False

conn.close()

print('\n📈 用户数据:')
print('日期         渠道数  总用户数  优质用户  次留率')
print('-' * 50)
for _, row in user_df.iterrows():
    print(f\"{row['dt']}     {row['channels']:>2}  {row['total_users']:>8,}  {row['quality_users']:>8,}    {row['retention_rate']:>4}%\")

if has_creative_data:
    print('\n📊 素材数据:')
    print('日期         素材数  Good认证  消耗成本')
    print('-' * 40)
    for _, row in creative_df.iterrows():
        creative_cost = row['creative_cost'] if pd.notna(row['creative_cost']) else 0
        print(f\"{row['dt']}     {row['creative_count']:>3}  {row['creative_users']:>8,}  ¥{creative_cost:>8,.0f}\")
else:
    print('\n⚠️ 暂无素材数据')
"