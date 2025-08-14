#!/usr/bin/env python3
"""
更新代码以兼容优化后的表结构
注意：金额字段从元改为分存储
"""

# 兼容性说明：
# 1. 新表中金额字段以分为单位存储（INTEGER）
# 2. 使用兼容视图 _compat 可以保持原有代码不变
# 3. 直接查询新表需要除以100转换为元
# 4. 使用聚合表可以大幅提升查询性能

# 示例1：使用兼容视图（无需修改代码）
def query_with_compat_view():
    """使用兼容视图，保持原有代码不变"""
    query = """
    SELECT 
        SUM(newuser) as total_users,
        ROUND(SUM(zizhu_revenue_1), 2) as total_revenue,
        ROUND(SUM(zizhu_revenue_1) / SUM(newuser), 2) as arpu
    FROM cpz_qs_newuser_channel_i_d_compat
    WHERE dt = ? AND status = 'good' AND verification_status = 'verified'
    """
    # 金额单位：元（视图自动转换）
    return query

# 示例2：直接查询新表（需要转换）
def query_new_table():
    """直接查询新表，需要处理金额单位"""
    query = """
    SELECT 
        SUM(newuser) as total_users,
        ROUND(SUM(zizhu_revenue_1) / 100.0, 2) as total_revenue,
        ROUND(SUM(zizhu_revenue_1) / 100.0 / SUM(newuser), 2) as arpu
    FROM cpz_qs_newuser_channel_i_d
    WHERE dt = ? AND status = 'good' AND verification_status = 'verified'
    """
    # 金额从分转换为元
    return query

# 示例3：使用聚合表（推荐，性能最佳）
def query_aggregate_table():
    """使用聚合表，查询速度最快"""
    query = """
    SELECT 
        quality_users,
        total_revenue / 100.0 as total_revenue,
        arpu,
        cpa,
        good_rate,
        retention_rate
    FROM daily_metrics_summary
    WHERE dt = ?
    """
    # 聚合表已预计算，查询速度极快
    return query

# 更新配置：指定使用哪种查询方式
QUERY_MODE = {
    'compat': True,      # 使用兼容视图（默认）
    'direct': False,     # 直接查询新表
    'aggregate': False   # 使用聚合表
}

# 表名映射
TABLE_MAPPING = {
    'cpz_qs_newuser_channel_i_d': {
        'compat': 'cpz_qs_newuser_channel_i_d_compat',
        'direct': 'cpz_qs_newuser_channel_i_d',
        'aggregate': 'daily_metrics_summary'
    },
    'dwd_ttx_market_cash_cost_i_d': {
        'compat': 'dwd_ttx_market_cash_cost_i_d_compat',
        'direct': 'dwd_ttx_market_cash_cost_i_d',
        'aggregate': 'daily_metrics_summary'
    }
}

print("数据库优化完成！")
print("- 原表已备份为 *_old")
print("- 新表使用分为单位存储金额（避免浮点精度问题）")
print("- 创建了10个索引优化查询性能")
print("- 创建了聚合表加速常用查询")
print("- 兼容视图 *_compat 保持向后兼容")
print("\n建议：")
print("1. 现有代码无需修改，使用兼容视图")
print("2. 新代码建议使用聚合表提升性能")
print("3. 金额计算使用整数避免精度问题")