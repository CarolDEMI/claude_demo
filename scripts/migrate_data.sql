-- 数据迁移脚本
-- 将现有数据迁移到优化后的表结构

-- ========================================
-- 1. 迁移新用户渠道数据
-- ========================================
INSERT INTO cpz_qs_newuser_channel_i_d_new (
    dt, ad_channel, agent, ad_account, subchannel,
    status, verification_status, os_type, gender, tag, age_group, dengji,
    newuser, is_returned_1_day, zizhu_revenue_1, zizhu_revenue_1_aftertax
)
SELECT 
    dt,
    ad_channel,
    COALESCE(agent, ''),
    COALESCE(ad_account, ''),
    COALESCE(subchannel, ''),
    COALESCE(status, 'default'),
    COALESCE(verification_status, ''),
    COALESCE(os_type, ''),
    COALESCE(gender, ''),
    COALESCE(tag, ''),
    COALESCE(age_group, ''),
    COALESCE(dengji, ''),
    newuser,
    CAST(is_returned_1_day AS INTEGER),  -- 转换为整数
    CAST(zizhu_revenue_1 * 100 AS INTEGER),  -- 元转分
    CAST(zizhu_revenue_1_aftertax * 100 AS INTEGER)  -- 元转分
FROM cpz_qs_newuser_channel_i_d;

-- ========================================
-- 2. 迁移成本数据
-- ========================================
INSERT INTO dwd_ttx_market_cash_cost_i_d_new (
    dt, channel, agent, account, ad_plan_id_str, cash_cost
)
SELECT 
    dt,
    channel,
    COALESCE(agent, ''),
    COALESCE(account, ''),
    COALESCE(ad_plan_id_str, ''),
    CAST(ROUND(cash_cost * 100) AS INTEGER)  -- 元转分，处理精度问题
FROM dwd_ttx_market_cash_cost_i_d
WHERE cash_cost >= 0;  -- 排除异常负值

-- ========================================
-- 3. 生成每日大盘聚合数据
-- ========================================
INSERT INTO daily_metrics_summary (
    dt, quality_users, all_users, good_users, verified_users,
    retained_users, total_revenue, total_cost,
    good_rate, retention_rate, arpu, cpa
)
SELECT 
    a.dt,
    SUM(CASE WHEN a.status = 'good' AND a.verification_status = 'verified' THEN a.newuser ELSE 0 END) as quality_users,
    SUM(a.newuser) as all_users,
    SUM(CASE WHEN a.status = 'good' THEN a.newuser ELSE 0 END) as good_users,
    SUM(CASE WHEN a.verification_status = 'verified' THEN a.newuser ELSE 0 END) as verified_users,
    SUM(a.is_returned_1_day) as retained_users,
    SUM(a.zizhu_revenue_1) as total_revenue,  -- 已经是分
    COALESCE(SUM(b.cash_cost), 0) as total_cost,  -- 已经是分
    ROUND(SUM(CASE WHEN a.status = 'good' THEN a.newuser ELSE 0 END) * 100.0 / NULLIF(SUM(a.newuser), 0), 2) as good_rate,
    ROUND(SUM(a.is_returned_1_day) * 100.0 / NULLIF(SUM(CASE WHEN a.status = 'good' AND a.verification_status = 'verified' THEN a.newuser ELSE 0 END), 0), 2) as retention_rate,
    ROUND(SUM(a.zizhu_revenue_1) / 100.0 / NULLIF(SUM(CASE WHEN a.status = 'good' AND a.verification_status = 'verified' THEN a.newuser ELSE 0 END), 0), 2) as arpu,
    ROUND(SUM(b.cash_cost) / 100.0 / NULLIF(SUM(CASE WHEN a.status = 'good' AND a.verification_status = 'verified' THEN a.newuser ELSE 0 END), 0), 2) as cpa
FROM cpz_qs_newuser_channel_i_d_new a
LEFT JOIN (
    SELECT dt, SUM(cash_cost) as cash_cost
    FROM dwd_ttx_market_cash_cost_i_d_new
    GROUP BY dt
) b ON a.dt = b.dt
GROUP BY a.dt;

-- ========================================
-- 4. 生成渠道日指标聚合数据
-- ========================================
INSERT INTO channel_daily_metrics (
    dt, ad_channel, quality_users, all_users,
    retained_users, revenue, cost,
    good_rate, retention_rate, arpu, cpa
)
SELECT 
    a.dt,
    a.ad_channel,
    SUM(CASE WHEN a.status = 'good' AND a.verification_status = 'verified' THEN a.newuser ELSE 0 END) as quality_users,
    SUM(a.newuser) as all_users,
    SUM(a.is_returned_1_day) as retained_users,
    SUM(a.zizhu_revenue_1) as revenue,
    COALESCE(SUM(b.cash_cost), 0) as cost,
    ROUND(SUM(CASE WHEN a.status = 'good' THEN a.newuser ELSE 0 END) * 100.0 / NULLIF(SUM(a.newuser), 0), 2) as good_rate,
    ROUND(SUM(a.is_returned_1_day) * 100.0 / NULLIF(SUM(CASE WHEN a.status = 'good' AND a.verification_status = 'verified' THEN a.newuser ELSE 0 END), 0), 2) as retention_rate,
    ROUND(SUM(a.zizhu_revenue_1) / 100.0 / NULLIF(SUM(CASE WHEN a.status = 'good' AND a.verification_status = 'verified' THEN a.newuser ELSE 0 END), 0), 2) as arpu,
    ROUND(SUM(b.cash_cost) / 100.0 / NULLIF(SUM(CASE WHEN a.status = 'good' AND a.verification_status = 'verified' THEN a.newuser ELSE 0 END), 0), 2) as cpa
FROM cpz_qs_newuser_channel_i_d_new a
LEFT JOIN (
    SELECT dt, channel, SUM(cash_cost) as cash_cost
    FROM dwd_ttx_market_cash_cost_i_d_new
    GROUP BY dt, channel
) b ON a.dt = b.dt AND a.ad_channel = b.channel
GROUP BY a.dt, a.ad_channel;

-- ========================================
-- 5. 重命名表以完成迁移
-- ========================================
-- 备份原表
ALTER TABLE cpz_qs_newuser_channel_i_d RENAME TO cpz_qs_newuser_channel_i_d_old;
ALTER TABLE dwd_ttx_market_cash_cost_i_d RENAME TO dwd_ttx_market_cash_cost_i_d_old;

-- 重命名新表
ALTER TABLE cpz_qs_newuser_channel_i_d_new RENAME TO cpz_qs_newuser_channel_i_d;
ALTER TABLE dwd_ttx_market_cash_cost_i_d_new RENAME TO dwd_ttx_market_cash_cost_i_d;

-- 创建兼容性视图（使用原表名+"_compat"）
DROP VIEW IF EXISTS cpz_qs_newuser_channel_i_d_compat;
CREATE VIEW cpz_qs_newuser_channel_i_d_compat AS
SELECT 
    dt,
    ad_channel,
    NULLIF(agent, '') as agent,
    NULLIF(ad_account, '') as ad_account,
    NULLIF(subchannel, '') as subchannel,
    NULLIF(status, 'default') as status,
    NULLIF(verification_status, '') as verification_status,
    NULLIF(os_type, '') as os_type,
    NULLIF(gender, '') as gender,
    NULLIF(tag, '') as tag,
    NULLIF(age_group, '') as age_group,
    NULLIF(dengji, '') as dengji,
    newuser,
    CAST(is_returned_1_day AS REAL) as is_returned_1_day,
    CAST(zizhu_revenue_1 AS REAL) / 100.0 as zizhu_revenue_1,
    CAST(zizhu_revenue_1_aftertax AS REAL) / 100.0 as zizhu_revenue_1_aftertax
FROM cpz_qs_newuser_channel_i_d;

DROP VIEW IF EXISTS dwd_ttx_market_cash_cost_i_d_compat;
CREATE VIEW dwd_ttx_market_cash_cost_i_d_compat AS
SELECT 
    dt,
    channel,
    NULLIF(agent, '') as agent,
    NULLIF(account, '') as account,
    NULLIF(ad_plan_id_str, '') as ad_plan_id_str,
    CAST(cash_cost AS REAL) / 100.0 as cash_cost
FROM dwd_ttx_market_cash_cost_i_d;