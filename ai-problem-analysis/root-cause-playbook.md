# 根因分析手册

## 根因分析方法论

### 分析思维框架
根因分析遵循**"数据→假设→验证→结论"**的科学方法：
1. **数据观察**: 通过数据识别异常模式
2. **假设生成**: 基于业务知识提出可能原因
3. **假设验证**: 用数据验证或排除假设
4. **根因确认**: 找到能解释所有现象的根本原因

## 常见问题根因分析路径

### 1. DAU下降根因分析

#### 1.1 新增用户下降导致的DAU下降

**典型表现**:
- 新增用户数显著下降
- 老用户留存率正常
- 回流用户数正常

**根因分析路径**:

**A. 渠道投放问题**
```sql
-- 验证查询：渠道投放预算和效果
SELECT 
    channel,
    dt,
    SUM(budget) as daily_budget,
    COUNT(DISTINCT user_id) as acquisitions,
    budget / NULLIF(acquisitions, 0) as cpa
FROM marketing_spend_table ms
LEFT JOIN user_acquisition_table ua ON ms.channel = ua.channel AND ms.dt = ua.dt
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
GROUP BY channel, dt
ORDER BY channel, dt;
```

**可能根因**:
- 投放预算削减
- 渠道质量下降（CPA上升但转化下降）
- 竞对加大投放力度，抢夺流量
- 平台政策调整（如iOS隐私政策）

**B. 产品获客能力下降**
```sql
-- 验证查询：获客漏斗转化率
SELECT 
    dt,
    app_downloads,
    registrations,
    activations,
    registrations * 100.0 / NULLIF(app_downloads, 0) as reg_rate,
    activations * 100.0 / NULLIF(registrations, 0) as activation_rate
FROM acquisition_funnel_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
ORDER BY dt;
```

**可能根因**:
- 新手引导体验变差
- 注册流程出现Bug
- 应用商店评分/评价下降
- 新版本兼容性问题

#### 1.2 用户留存下降导致的DAU下降

**典型表现**:
- 新增用户数正常
- 次日/7日留存率下降
- 老用户活跃度下降

**根因分析路径**:

**A. 产品体验问题**
```sql
-- 验证查询：用户体验指标
SELECT 
    dt,
    app_version,
    AVG(session_duration) as avg_session,
    AVG(crash_rate) as avg_crash_rate,
    AVG(load_time_ms) as avg_load_time,
    COUNT(DISTINCT CASE WHEN session_duration < 30 THEN user_id END) * 100.0 / 
        COUNT(DISTINCT user_id) as bounce_rate
FROM user_session_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
GROUP BY dt, app_version
ORDER BY dt, app_version;
```

**可能根因**:
- 新版本引入严重Bug
- 性能问题（启动慢、卡顿）
- UI/UX改版用户不适应
- 核心功能流程变更

**B. 内容/匹配质量问题**
```sql
-- 验证查询：匹配效果和用户互动
SELECT 
    dt,
    COUNT(DISTINCT user_id) as active_users,
    SUM(matches_count) / COUNT(DISTINCT user_id) as avg_matches_per_user,
    SUM(conversations_started) / COUNT(DISTINCT user_id) as avg_conversations,
    SUM(messages_sent) / COUNT(DISTINCT user_id) as avg_messages
FROM user_interaction_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
    AND is_test = '0'
GROUP BY dt
ORDER BY dt;
```

**可能根因**:
- 推荐算法效果变差
- 用户内容质量下降
- 虚假用户/僵尸用户增多
- 社区氛围恶化

#### 1.3 外部环境影响

**验证查询**:
```sql
-- 与竞品、行业数据对比
-- 需要结合外部数据源
SELECT 
    dt,
    our_dau,
    industry_avg_dau,
    our_dau / industry_avg_dau as relative_performance
FROM competitive_analysis_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 30);
```

**可能根因**:
- 行业整体下滑
- 政策监管影响
- 重大社会事件影响
- 季节性因素
- 竞品推出爆款功能

### 2. 收入下降根因分析

#### 2.1 付费用户数下降

**典型表现**:
- ARPU正常，付费用户数下降
- 新用户付费转化率下降

**根因分析路径**:

**A. 付费功能问题**
```sql
-- 验证查询：付费漏斗分析
SELECT 
    dt,
    COUNT(DISTINCT user_id) as total_users,
    COUNT(DISTINCT CASE WHEN viewed_vip_page = 1 THEN user_id END) as vip_page_views,
    COUNT(DISTINCT CASE WHEN started_payment = 1 THEN user_id END) as payment_starts,
    COUNT(DISTINCT CASE WHEN completed_payment = 1 THEN user_id END) as payment_completions,
    
    vip_page_views * 100.0 / total_users as vip_view_rate,
    payment_starts * 100.0 / NULLIF(vip_page_views, 0) as payment_start_rate,
    payment_completions * 100.0 / NULLIF(payment_starts, 0) as payment_success_rate
FROM payment_funnel_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
    AND is_test = '0'
GROUP BY dt
ORDER BY dt;
```

**可能根因**:
- 支付流程出现故障
- 付费价值感知下降
- 付费权益削减
- 价格调整不当

**B. 用户购买动机问题**
```sql
-- 验证查询：付费驱动因素分析
SELECT 
    payment_trigger,
    COUNT(DISTINCT user_id) as paying_users,
    SUM(payment_amount) as total_revenue,
    AVG(payment_amount) as avg_payment
FROM payment_behavior_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
    AND is_test = '0'
GROUP BY payment_trigger
ORDER BY total_revenue DESC;
```

**可能根因**:
- 核心付费场景减少
- 免费功能过于充分
- 付费必要性降低

#### 2.2 ARPU下降

**典型表现**:
- 付费用户数正常，单用户收入下降
- 用户付费频次或金额降低

**根因分析路径**:
```sql
-- 验证查询：ARPU组成分析
SELECT 
    dt,
    COUNT(DISTINCT user_id) as paying_users,
    AVG(see_revenue) as avg_see_revenue,
    AVG(vip_revenue) as avg_vip_revenue, 
    AVG(qs_revenue) as avg_qs_revenue,
    AVG(see_revenue + vip_revenue + qs_revenue) as avg_total_revenue,
    
    -- 各收入类型占比
    SUM(see_revenue) * 100.0 / SUM(see_revenue + vip_revenue + qs_revenue) as see_pct,
    SUM(vip_revenue) * 100.0 / SUM(see_revenue + vip_revenue + qs_revenue) as vip_pct,
    SUM(qs_revenue) * 100.0 / SUM(see_revenue + vip_revenue + qs_revenue) as qs_pct
FROM user_revenue_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
    AND is_test = '0'
    AND (see_revenue + vip_revenue + qs_revenue) > 0
GROUP BY dt
ORDER BY dt;
```

**可能根因**:
- 某类付费功能使用下降
- 用户付费习惯改变
- 替代免费功能增多
- 经济环境影响用户消费

### 3. 用户质量下降根因分析

#### 3.1 Good率下降

**根因分析路径**:
```sql
-- 验证查询：Good率下降原因分析
SELECT 
    dt,
    channel,
    COUNT(DISTINCT user_id) as total_new_users,
    COUNT(DISTINCT CASE WHEN status = 'good' THEN user_id END) as good_users,
    good_users * 100.0 / NULLIF(total_new_users, 0) as good_rate,
    
    -- 审核相关指标
    AVG(profile_completion_rate) as avg_profile_completion,
    AVG(content_review_score) as avg_content_score
FROM user_quality_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
    AND is_test = '0'  
    AND is_new_user = 1
GROUP BY dt, channel
ORDER BY dt, channel;
```

**可能根因**:
- 渠道用户质量变差
- 审核标准调整
- 虚假注册增多
- 获客策略改变导致用户结构变化

#### 3.2 认证率下降

**根因分析路径**:
```sql
-- 验证查询：认证流程分析
SELECT 
    dt,
    COUNT(DISTINCT user_id) as total_users,
    COUNT(DISTINCT CASE WHEN started_verification = 1 THEN user_id END) as started_verification,
    COUNT(DISTINCT CASE WHEN completed_verification = 1 THEN user_id END) as completed_verification,
    COUNT(DISTINCT CASE WHEN verification_status = 'verified' THEN user_id END) as verified_users,
    
    started_verification * 100.0 / total_users as verification_start_rate,
    completed_verification * 100.0 / NULLIF(started_verification, 0) as completion_rate,
    verified_users * 100.0 / NULLIF(completed_verification, 0) as approval_rate
FROM verification_process_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
    AND is_test = '0'
GROUP BY dt
ORDER BY dt;
```

**可能根因**:
- 认证流程复杂度提升
- 认证要求变严格
- 用户认证意愿下降
- 认证系统故障

### 4. 渠道效果异常根因分析

#### 4.1 某渠道CPA异常上升

**根因分析路径**:
```sql
-- 验证查询：渠道详细表现
SELECT 
    dt,
    channel,
    sub_channel,
    SUM(cost) as total_cost,
    COUNT(DISTINCT CASE WHEN status = 'good' AND verification_status = 'verified' 
          THEN user_id END) as quality_users,
    total_cost / NULLIF(quality_users, 0) as cpa,
    
    -- 转化漏斗
    impressions,
    clicks,  
    registrations,
    clicks * 100.0 / NULLIF(impressions, 0) as ctr,
    registrations * 100.0 / NULLIF(clicks, 0) as cvr
FROM channel_performance_table
WHERE dt >= DATE_SUB(CURRENT_DATE, 14)
    AND channel = '异常渠道名称'
GROUP BY dt, channel, sub_channel
ORDER BY dt, cpa DESC;
```

**可能根因**:
- 渠道流量质量下降
- 竞争加剧导致流量成本上升
- 创意素材效果衰减
- 投放策略不当
- 渠道政策调整

## 根因验证方法

### 1. 时间序列验证
通过时间维度分析确定因果关系：
- 问题开始时间点
- 是否与关键事件时间吻合
- 变化趋势是否符合预期影响

### 2. 对照组验证
设置对照组验证假设：
- 未受影响的用户群体表现
- 不同版本用户对比
- A/B测试结果对比

### 3. 相关性验证
分析相关变量的关联关系：
```sql
-- 相关性分析模板
WITH daily_metrics AS (
    SELECT 
        dt,
        主要指标,
        疑似影响因子1,
        疑似影响因子2,
        疑似影响因子3
    FROM analysis_table
    WHERE dt >= DATE_SUB(CURRENT_DATE, 30)
)
SELECT 
    CORR(主要指标, 疑似影响因子1) as correlation_1,
    CORR(主要指标, 疑似影响因子2) as correlation_2,
    CORR(主要指标, 疑似影响因子3) as correlation_3
FROM daily_metrics;
```

### 4. 业务逻辑验证
结合业务常识判断：
- 影响程度是否合理
- 影响方向是否正确
- 是否有其他更合理的解释

## 根因分析决策树

### DAU下降决策树
```
DAU下降 → 
├─ 数据问题？
│  ├─ 是 → 修复数据问题
│  └─ 否 → 继续分析
├─ 新增用户下降？
│  ├─ 是 → 渠道/获客问题
│  └─ 否 → 留存/回流问题
├─ 留存率下降？
│  ├─ 是 → 产品体验/内容问题  
│  └─ 否 → 回流用户问题
└─ 外部因素影响？
   ├─ 是 → 行业/季节性因素
   └─ 否 → 深入细分析
```

### 收入下降决策树
```
收入下降 →
├─ 付费用户数下降？
│  ├─ 是 → 付费转化问题
│  └─ 否 → ARPU下降问题
├─ 付费漏斗哪一环节？
│  ├─ 付费意愿 → 产品价值问题
│  ├─ 支付转化 → 支付流程问题
│  └─ 复购频次 → 付费体验问题
```

## 根因分析报告模板

### 问题描述
- **异常现象**: [具体指标和下降幅度]
- **影响时间**: [开始时间和持续时间]
- **影响范围**: [影响用户群体和规模]

### 分析过程
1. **假设列表**: [列出所有可能原因]
2. **验证方法**: [每个假设的验证方式]
3. **分析结果**: [验证结论和数据支撑]

### 根因结论
- **主要根因**: [经验证的主要原因]
- **次要因素**: [其他影响因素]
- **排除原因**: [已排除的假设]

### 影响评估
- **业务影响**: [对用户、收入等的具体影响]
- **持续时间**: [预计影响持续时间]
- **恢复预期**: [自然恢复或需干预]

### 解决方案
- **立即行动**: [紧急措施]
- **短期改进**: [1-2周内措施]
- **长期优化**: [长期预防措施]

---
*使用说明: 选择合适的分析路径，结合实际数据验证，得出可信的根因结论*
*最后更新: 2025-08-06*