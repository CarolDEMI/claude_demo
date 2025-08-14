#!/bin/bash
# 专用账户数据同步脚本
# Usage: ./sync_account_data.sh

echo "🏦 账户数据同步工具"
echo "=================="
echo ""

# 确保配置文件存在
if [ ! -f "src/presto_config.py" ]; then
    echo "📋 复制配置文件..."
    cp config/presto_config.py src/
fi

# 同步账户数据（测试成本表）
echo "📊 开始同步账户数据..."
echo "🔄 同步测试成本表（包含详细账户信息）..."

python3 src/presto_sync.py --sync-test-cost

if [ $? -eq 0 ]; then
    echo "✅ 账户数据同步成功"
    echo ""
    
    # 显示账户数据概况
    echo "📊 账户数据概况："
    python3 -c "
import sqlite3
import pandas as pd

conn = sqlite3.connect('./data/data.db')

# 账户数据概况
account_query = '''
SELECT 
    dt,
    COUNT(DISTINCT account) as account_count,
    COUNT(DISTINCT account_name) as account_name_count,
    SUM(total_good_verified) as total_users,
    SUM(cash_cost) as total_cost,
    ROUND(SUM(cash_cost) / SUM(total_good_verified), 2) as cpa
FROM dwd_ttx_market_cash_cost_i_d_test
WHERE dt >= date('now', '-7 days')
GROUP BY dt
ORDER BY dt DESC
LIMIT 7
'''

try:
    df = pd.read_sql_query(account_query, conn)
    if not df.empty:
        print('\n📈 最近7天账户数据:')
        print('日期         账户数  账户名数  Good用户    消耗成本      CPA')
        print('-' * 60)
        for _, row in df.iterrows():
            cost = row['total_cost'] if pd.notna(row['total_cost']) else 0
            cpa = row['cpa'] if pd.notna(row['cpa']) else 0
            print(f\"{row['dt']}     {row['account_count']:>3}     {row['account_name_count']:>4}   {row['total_users']:>8,}  ¥{cost:>8,.0f}   ¥{cpa:>5.2f}\")
    else:
        print('\n⚠️ 暂无账户数据')
        
    # 账户排行榜
    top_accounts_query = '''
    SELECT 
        account_name,
        SUM(total_good_verified) as total_users,
        SUM(cash_cost) as total_cost,
        ROUND(SUM(cash_cost) / SUM(total_good_verified), 2) as cpa
    FROM dwd_ttx_market_cash_cost_i_d_test
    WHERE dt >= date('now', '-7 days')
    GROUP BY account_name
    ORDER BY total_users DESC
    LIMIT 10
    '''
    
    top_df = pd.read_sql_query(top_accounts_query, conn)
    if not top_df.empty:
        print('\n🏆 Top 10 账户（按用户数排名）:')
        print('账户名                                      用户数    消耗成本      CPA')
        print('-' * 75)
        for i, row in top_df.iterrows():
            account_name = row['account_name'][:35] + ('...' if len(row['account_name']) > 35 else '')
            cost = row['total_cost'] if pd.notna(row['total_cost']) else 0
            cpa = row['cpa'] if pd.notna(row['cpa']) else 0
            print(f\"{account_name:<38} {row['total_users']:>8,}  ¥{cost:>8,.0f}   ¥{cpa:>5.2f}\")
    
except Exception as e:
    print(f'\n❌ 账户数据查询失败: {e}')
    
conn.close()
"
    
    echo ""
    echo "✅ 账户数据同步并统计完成！"
else
    echo "❌ 账户数据同步失败"
    echo "请检查："
    echo "1. Presto连接配置是否正确"
    echo "2. 网络连接是否正常"
    echo "3. 数据库权限是否足够"
fi

echo ""
echo "💡 提示: 账户数据包含详细的账户名称和成本信息"
echo "📊 可用于报告中的账户异常分析和账户排行"