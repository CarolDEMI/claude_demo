"""
大盘核心指标格式化器
"""
from typing import Dict, Any
import sqlite3

class DashboardFormatter:
    def __init__(self):
        pass
    
    def format_core_metrics(self, date: str, db_path: str = "./data.db") -> str:
        """
        格式化大盘核心指标数据
        """
        # 执行查询
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 分别查询用户数据和成本数据，避免JOIN导致的重复
        user_query = """
        SELECT 
            SUM(newuser) as total_new_users,
            ROUND(SUM(is_returned_1_day) * 100.0 / SUM(newuser), 2) as retention_rate,
            ROUND(SUM(zizhu_revenue_1) / SUM(newuser), 2) as arpu,
            ROUND(SUM(CASE WHEN gender = 'female' THEN newuser ELSE 0 END) * 100.0 / SUM(newuser), 2) as female_ratio,
            ROUND(SUM(CASE WHEN tag = 'T_NonBlueCollar' THEN newuser ELSE 0 END) * 100.0 / SUM(newuser), 2) as white_collar_ratio,
            ROUND(SUM(CASE WHEN age_group IN ('20-', '20~23') THEN newuser ELSE 0 END) * 100.0 / SUM(newuser), 2) as young_ratio
        FROM cpz_qs_newuser_channel_i_d
        WHERE status = 'good' AND verification_status = 'verified'
            AND dt = ?
        """
        
        # 查询全量用户数据用于计算质量比率
        all_user_query = """
        SELECT 
            SUM(newuser) as all_users,
            SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
            SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as good_verified_users
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt = ?
        """
        
        cost_query = """
        SELECT SUM(cash_cost) as total_cost
        FROM dwd_ttx_market_cash_cost_i_d
        WHERE dt = ?
        """
        
        # 执行用户数据查询
        cursor.execute(user_query, (date,))
        user_result = cursor.fetchone()
        
        # 执行全量用户数据查询
        cursor.execute(all_user_query, (date,))
        all_user_result = cursor.fetchone()
        
        # 执行成本数据查询
        cursor.execute(cost_query, (date,))
        cost_result = cursor.fetchone()
        
        conn.close()
        
        if not user_result or user_result[0] is None:
            return f"❌ 未找到{date}的用户数据"
        
        if not all_user_result or all_user_result[0] is None:
            return f"❌ 未找到{date}的全量用户数据"
        
        if not cost_result or cost_result[0] is None:
            return f"❌ 未找到{date}的成本数据"
        
        # 解析结果
        total_users, retention_rate, arpu, female_ratio, white_collar_ratio, young_ratio = user_result
        all_users, good_users, good_verified_users = all_user_result
        total_cost = cost_result[0]
        cpa = round(total_cost / total_users, 2) if total_users > 0 else 0
        
        # 计算质量比率
        good_rate = round(good_users * 100.0 / all_users, 2) if all_users > 0 else 0
        verified_rate = round(good_verified_users * 100.0 / good_users, 2) if good_users > 0 else 0  # 认证率 = verified的good用户 / good用户数
        good_verified_rate = round(good_verified_users * 100.0 / all_users, 2) if all_users > 0 else 0
        
        # 格式化输出
        formatted_output = f"""## {date}大盘核心指标

### 📊 核心指标
- **带量（新用户数）**: {int(total_users):,} 人（全量：{int(all_users):,} 人）
- **消耗**: {total_cost:,.2f} 元
- **获客成本(CPA)**: {cpa} 元/人
- **次留**: {retention_rate}%
- **ARPU**: {arpu} 元/人
- **Good率**: {good_rate}%
- **认证率**: {verified_rate}%
- **Good且认证率**: {good_verified_rate}%

### 👥 用户质量比指标
- **女比**: {female_ratio}%
- **白领比**: {white_collar_ratio}%
- **年轻占比**: {young_ratio}%（24岁以下）

### 💡 关键洞察
- **用户规模**: 单日获客{int(total_users):,}人（质量用户），全量{int(all_users):,}人，投入{total_cost:,.0f}元
- **获客效率**: CPA为{cpa}元，{'效率较高' if cpa < 50 else '有优化空间'}
- **用户质量**: Good率{good_rate}%，认证率{verified_rate}%，{'质量筛选严格' if good_verified_rate < 60 else '质量筛选适中'}
- **用户画像**: {'女性用户占比适中' if 35 <= female_ratio <= 45 else '女性用户占比' + ('偏低' if female_ratio < 35 else '偏高')}，{'白领用户比例良好' if white_collar_ratio >= 40 else '白领用户比例待提升'}
- **留存表现**: {'留存率优秀' if retention_rate >= 60 else '留存率' + ('良好' if retention_rate >= 50 else '待改善')}
- **变现能力**: {'用户价值较高' if arpu >= 5 else '用户价值' + ('一般' if arpu >= 3 else '偏低')}"""

        return formatted_output
    
    def get_core_metrics_sql(self, date: str) -> str:
        """
        获取大盘核心指标的SQL语句
        """
        return f"""
-- {date}大盘核心指标查询
SELECT 
    '{date}' as date,
    SUM(a.newuser) as total_new_users,
    SUM(b.cash_cost) as total_cost,
    ROUND(SUM(b.cash_cost) / SUM(a.newuser), 2) as cpa,
    ROUND(SUM(a.is_returned_1_day) * 100.0 / SUM(a.newuser), 2) as retention_rate,
    ROUND(SUM(a.zizhu_revenue_1) / SUM(a.newuser), 2) as arpu,
    ROUND(SUM(CASE WHEN a.gender = 'female' THEN a.newuser ELSE 0 END) * 100.0 / SUM(a.newuser), 2) as female_ratio,
    ROUND(SUM(CASE WHEN a.tag = 'T_NonBlueCollar' THEN a.newuser ELSE 0 END) * 100.0 / SUM(a.newuser), 2) as white_collar_ratio,
    ROUND(SUM(CASE WHEN a.age_group IN ('20-', '20~23') THEN a.newuser ELSE 0 END) * 100.0 / SUM(a.newuser), 2) as young_ratio
FROM cpz_qs_newuser_channel_i_d a
LEFT JOIN dwd_ttx_market_cash_cost_i_d b 
    ON a.dt = b.dt AND a.ad_channel = b.channel
WHERE a.status = 'good' AND a.verification_status = 'verified'
    AND a.dt = '{date}';
"""

# 快捷函数
def show_dashboard(date: str) -> str:
    """显示指定日期的大盘核心指标"""
    formatter = DashboardFormatter()
    return formatter.format_core_metrics(date)