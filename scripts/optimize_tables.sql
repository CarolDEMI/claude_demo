-- 数据库表结构优化脚本
-- 创建时间: 2025-07-25

-- ========================================
-- 1. 创建优化后的新用户渠道分析表
-- ========================================
DROP TABLE IF EXISTS cpz_qs_newuser_channel_i_d_new;

CREATE TABLE cpz_qs_newuser_channel_i_d_new (
    -- 维度字段
    dt TEXT NOT NULL,                      -- 日期，格式: YYYY-MM-DD
    ad_channel TEXT NOT NULL,              -- 广告渠道
    agent TEXT DEFAULT '',                 -- 代理商，空字符串代替NULL
    ad_account TEXT DEFAULT '',            -- 广告账户
    subchannel TEXT DEFAULT '',            -- 子渠道
    status TEXT NOT NULL DEFAULT 'default', -- 用户状态
    verification_status TEXT DEFAULT '',   -- 认证状态
    os_type TEXT DEFAULT '',               -- 操作系统
    gender TEXT DEFAULT '',                -- 性别
    tag TEXT DEFAULT '',                   -- 用户标签
    age_group TEXT DEFAULT '',             -- 年龄段
    dengji TEXT DEFAULT '',                -- 城市等级
    
    -- 指标字段（使用INTEGER存储金额，单位：分）
    newuser INTEGER NOT NULL DEFAULT 0,            -- 新用户数
    is_returned_1_day INTEGER NOT NULL DEFAULT 0,  -- 次日留存用户数（改为整数）
    zizhu_revenue_1 INTEGER NOT NULL DEFAULT 0,    -- 首日收入（单位：分）
    zizhu_revenue_1_aftertax INTEGER NOT NULL DEFAULT 0, -- 首日收入税后（单位：分）
    
    -- 约束
    PRIMARY KEY (dt, ad_channel, agent, ad_account, subchannel, status, 
                 verification_status, os_type, gender, tag, age_group, dengji),
    
    -- 检查约束
    CHECK (dt LIKE '____-__-__'),         -- 日期格式检查
    CHECK (newuser >= 0),                  -- 新用户数非负
    CHECK (is_returned_1_day >= 0),        -- 留存用户数非负
    CHECK (is_returned_1_day <= newuser),  -- 留存用户数不大于新用户数
    CHECK (zizhu_revenue_1 >= 0),          -- 收入非负
    CHECK (zizhu_revenue_1_aftertax >= 0), -- 税后收入非负
    CHECK (zizhu_revenue_1_aftertax <= zizhu_revenue_1) -- 税后收入不大于税前
);

-- ========================================
-- 2. 创建优化后的市场推广成本表
-- ========================================
DROP TABLE IF EXISTS dwd_ttx_market_cash_cost_i_d_new;

CREATE TABLE dwd_ttx_market_cash_cost_i_d_new (
    -- 维度字段
    dt TEXT NOT NULL,                   -- 日期
    channel TEXT NOT NULL,              -- 渠道
    agent TEXT DEFAULT '',              -- 代理商
    account TEXT DEFAULT '',            -- 账户
    ad_plan_id_str TEXT DEFAULT '',     -- 广告计划ID
    
    -- 指标字段（使用INTEGER存储金额，单位：分）
    cash_cost INTEGER NOT NULL DEFAULT 0,  -- 现金成本（单位：分）
    
    -- 约束
    PRIMARY KEY (dt, channel, agent, account, ad_plan_id_str),
    
    -- 检查约束
    CHECK (dt LIKE '____-__-__'),      -- 日期格式检查
    CHECK (cash_cost >= 0)              -- 成本非负
);

-- ========================================
-- 3. 创建索引
-- ========================================

-- 新用户表索引
CREATE INDEX idx_newuser_dt ON cpz_qs_newuser_channel_i_d_new(dt);
CREATE INDEX idx_newuser_channel ON cpz_qs_newuser_channel_i_d_new(ad_channel);
CREATE INDEX idx_newuser_dt_channel ON cpz_qs_newuser_channel_i_d_new(dt, ad_channel);
CREATE INDEX idx_newuser_status ON cpz_qs_newuser_channel_i_d_new(status, verification_status);
CREATE INDEX idx_newuser_dt_status ON cpz_qs_newuser_channel_i_d_new(dt, status, verification_status);

-- 成本表索引
CREATE INDEX idx_cost_dt ON dwd_ttx_market_cash_cost_i_d_new(dt);
CREATE INDEX idx_cost_channel ON dwd_ttx_market_cash_cost_i_d_new(channel);
CREATE INDEX idx_cost_dt_channel ON dwd_ttx_market_cash_cost_i_d_new(dt, channel);

-- ========================================
-- 4. 创建视图以保持兼容性（金额转换回元）
-- ========================================

-- 创建兼容原表的视图
DROP VIEW IF EXISTS cpz_qs_newuser_channel_i_d_view;
CREATE VIEW cpz_qs_newuser_channel_i_d_view AS
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
FROM cpz_qs_newuser_channel_i_d_new;

DROP VIEW IF EXISTS dwd_ttx_market_cash_cost_i_d_view;
CREATE VIEW dwd_ttx_market_cash_cost_i_d_view AS
SELECT 
    dt,
    channel,
    NULLIF(agent, '') as agent,
    NULLIF(account, '') as account,
    NULLIF(ad_plan_id_str, '') as ad_plan_id_str,
    CAST(cash_cost AS REAL) / 100.0 as cash_cost
FROM dwd_ttx_market_cash_cost_i_d_new;

-- ========================================
-- 5. 创建聚合表加速查询
-- ========================================

-- 每日大盘指标聚合表
DROP TABLE IF EXISTS daily_metrics_summary;
CREATE TABLE daily_metrics_summary (
    dt TEXT PRIMARY KEY,
    quality_users INTEGER,              -- 质量用户数(good+verified)
    all_users INTEGER,                  -- 全部用户数
    good_users INTEGER,                 -- good用户数
    verified_users INTEGER,             -- verified用户数
    retained_users INTEGER,             -- 留存用户数
    total_revenue INTEGER,              -- 总收入（分）
    total_cost INTEGER,                 -- 总成本（分）
    good_rate REAL,                     -- Good率
    retention_rate REAL,                -- 留存率
    arpu REAL,                          -- ARPU（元）
    cpa REAL,                           -- CPA（元）
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- 渠道日指标聚合表
DROP TABLE IF EXISTS channel_daily_metrics;
CREATE TABLE channel_daily_metrics (
    dt TEXT,
    ad_channel TEXT,
    quality_users INTEGER,
    all_users INTEGER,
    retained_users INTEGER,
    revenue INTEGER,                    -- 收入（分）
    cost INTEGER,                       -- 成本（分）
    good_rate REAL,
    retention_rate REAL,
    arpu REAL,
    cpa REAL,
    PRIMARY KEY (dt, ad_channel)
);

CREATE INDEX idx_channel_metrics_dt ON channel_daily_metrics(dt);
CREATE INDEX idx_channel_metrics_channel ON channel_daily_metrics(ad_channel);