#!/bin/bash
# 对比两天数据的脚本

cd "$(dirname "$0")/.."  # 切换到项目根目录

if [ $# -ne 2 ]; then
    echo "使用方法: $0 <日期1> <日期2>"
    echo "示例: $0 2025-07-27 2025-07-26"
    echo "示例: $0 yesterday last-week"
    exit 1
fi

DATE1=$1
DATE2=$2

echo "📊 对比 $DATE1 vs $DATE2 的数据"
echo "=" * 50

# 生成临时查询来对比数据
python3 -c "
import sqlite3
from datetime import datetime, timedelta

def parse_date(date_str):
    if date_str == 'yesterday':
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    elif date_str == 'today':
        return datetime.now().strftime('%Y-%m-%d')
    elif date_str == 'last-week':
        return (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    else:
        return date_str

date1 = parse_date('$DATE1')
date2 = parse_date('$DATE2')

try:
    conn = sqlite3.connect('./data/data.db')
    cursor = conn.cursor()
    
    # 获取两天的数据
    query = '''
    SELECT 
        dt,
        COUNT(*) as records,
        SUM(newuser) as total_users,
        SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users,
        SUM(zizhu_revenue_1_aftertax) as revenue,
        ROUND(SUM(zizhu_revenue_1_aftertax) / NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0), 2) as arpu
    FROM cpz_qs_newuser_channel_i_d 
    WHERE dt IN (?, ?)
    GROUP BY dt
    ORDER BY dt DESC
    '''
    
    cursor.execute(query, [date1, date2])
    results = cursor.fetchall()
    
    if len(results) == 2:
        data1, data2 = results[0], results[1]
        
        def calc_change(val1, val2):
            if val2 == 0:
                return 'N/A'
            change = ((val1 - val2) / val2) * 100
            return f'{change:+.1f}%'
        
        print(f'日期: {data1[0]} vs {data2[0]}')
        print(f'记录数: {data1[1]:,} vs {data2[1]:,} ({calc_change(data1[1], data2[1])})')
        print(f'总用户: {data1[2]:,} vs {data2[2]:,} ({calc_change(data1[2], data2[2])})')
        print(f'Good用户: {data1[3]:,} vs {data2[3]:,} ({calc_change(data1[3], data2[3])})')
        print(f'收入: ¥{data1[4]:,.2f} vs ¥{data2[4]:,.2f} ({calc_change(data1[4], data2[4])})')
        arpu1 = data1[5] if data1[5] else 0
        arpu2 = data2[5] if data2[5] else 0
        print(f'ARPU: ¥{arpu1:.2f} vs ¥{arpu2:.2f} ({calc_change(arpu1, arpu2)})')
        
    elif len(results) == 1:
        print(f'⚠️ 只找到一天的数据: {results[0][0]}')
    else:
        print(f'❌ 没有找到指定日期的数据')
        print(f'请确认日期格式正确: {date1}, {date2}')
    
    conn.close()
    
except Exception as e:
    print(f'❌ 对比失败: {e}')
"

echo ""
echo "💡 提示: 如需详细分析，请生成完整报告"
echo "python3 main.py --report $DATE1 --auto-open"