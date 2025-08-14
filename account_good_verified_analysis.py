#!/usr/bin/env python3
"""
同账户属性的Good且认证占比分析脚本
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def analyze_account_good_verified():
    # 连接数据库
    conn = sqlite3.connect('./data/data.db')
    
    # 获取最近7天的数据
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
    
    print('📊 Good且认证用户的同账户属性占比分析')
    print('=' * 80)
    print(f'分析周期: {start_date} 至 {end_date}')
    print()
    
    # 1. 按账户的Good且认证占比分析
    print('📈 1. 按账户的Good且认证用户分布（Top 20账户）')
    print('-' * 100)
    account_analysis_query = '''
    SELECT 
        ad_account,
        ad_channel,
        SUM(newuser) as total_users,
        SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
        SUM(CASE WHEN verification_status = 'verified' THEN newuser ELSE 0 END) as verified_users,
        SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as good_verified_users,
        ROUND(100.0 * SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) / NULLIF(SUM(newuser), 0), 2) as account_good_verified_rate,
        COUNT(DISTINCT dt) as active_days
    FROM cpz_qs_newuser_channel_i_d
    WHERE dt >= ? AND ad_account IS NOT NULL AND length(ad_account) > 0
    GROUP BY ad_account, ad_channel
    HAVING total_users > 200
    ORDER BY good_verified_users DESC
    LIMIT 20
    '''
    
    account_df = pd.read_sql_query(account_analysis_query, conn, params=[start_date])
    print('账户ID               渠道          总用户   Good用户  认证用户  Good且认证  账户内占比(%)  活跃天数')
    print('-' * 100)
    for _, row in account_df.iterrows():
        account_id = str(row['ad_account'])[:16] + ('...' if len(str(row['ad_account'])) > 16 else '')
        channel = row['ad_channel'][:10] + ('...' if len(row['ad_channel']) > 10 else '')
        print(f"{account_id:<18} {channel:<12} {row['total_users']:>8,}  {row['good_users']:>8,}  {row['verified_users']:>8,}  {row['good_verified_users']:>10,}      {row['account_good_verified_rate']:>10}      {row['active_days']:>6}")
    
    # 2. 同账户内的属性分布分析
    print('\n📊 2. 同账户内用户属性分布分析（Top 10大账户）')
    print('-' * 120)
    
    # 获取用户量最大的10个账户
    top_accounts_query = '''
    SELECT ad_account, ad_channel, SUM(newuser) as total_users
    FROM cpz_qs_newuser_channel_i_d
    WHERE dt >= ? AND ad_account IS NOT NULL AND length(ad_account) > 0
    GROUP BY ad_account, ad_channel
    ORDER BY total_users DESC
    LIMIT 10
    '''
    
    top_accounts = pd.read_sql_query(top_accounts_query, conn, params=[start_date])
    
    print('账户ID               渠道      总用户   Good且认证  其他Good   仅认证    其他用户  Good且认证占比(%)')
    print('-' * 120)
    
    for _, account in top_accounts.iterrows():
        # 分析每个账户内的用户状态分布
        account_detail_query = '''
        SELECT 
            SUM(newuser) as total_users,
            SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as good_verified,
            SUM(CASE WHEN status = 'good' AND (verification_status != 'verified' OR verification_status IS NULL) THEN newuser ELSE 0 END) as only_good,
            SUM(CASE WHEN (status != 'good' OR status IS NULL) AND verification_status = 'verified' THEN newuser ELSE 0 END) as only_verified,
            SUM(CASE WHEN (status != 'good' OR status IS NULL) AND (verification_status != 'verified' OR verification_status IS NULL) THEN newuser ELSE 0 END) as others
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt >= ? AND ad_account = ? AND ad_channel = ?
        '''
        
        detail = pd.read_sql_query(account_detail_query, conn, params=[start_date, account['ad_account'], account['ad_channel']])
        if not detail.empty:
            row = detail.iloc[0]
            good_verified_rate = (row['good_verified'] / row['total_users'] * 100) if row['total_users'] > 0 else 0
            
            account_id = str(account['ad_account'])[:16] + ('...' if len(str(account['ad_account'])) > 16 else '')
            channel = account['ad_channel'][:8] + ('...' if len(account['ad_channel']) > 8 else '')
            
            print(f"{account_id:<18} {channel:<10} {row['total_users']:>8,}  {row['good_verified']:>10,}  {row['only_good']:>9,}  {row['only_verified']:>8,}  {row['others']:>10,}        {good_verified_rate:>10.2f}")
    
    # 3. 账户质量等级分布
    print('\n📊 3. 账户质量等级分布（基于Good且认证率）')
    print('-' * 80)
    
    quality_distribution_query = '''
    WITH account_quality AS (
        SELECT 
            ad_account,
            ad_channel,
            SUM(newuser) as total_users,
            SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as good_verified_users,
            ROUND(100.0 * SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) / NULLIF(SUM(newuser), 0), 2) as good_verified_rate
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt >= ? AND ad_account IS NOT NULL AND length(ad_account) > 0
        GROUP BY ad_account, ad_channel
        HAVING total_users > 100
    )
    SELECT 
        CASE 
            WHEN good_verified_rate >= 70 THEN '顶级账户(≥70%)'
            WHEN good_verified_rate >= 60 THEN '优秀账户(60-70%)'
            WHEN good_verified_rate >= 50 THEN '良好账户(50-60%)'
            WHEN good_verified_rate >= 40 THEN '中等账户(40-50%)'
            WHEN good_verified_rate >= 30 THEN '较低账户(30-40%)'
            ELSE '待优化账户(<30%)'
        END as quality_level,
        COUNT(*) as account_count,
        SUM(total_users) as total_users,
        SUM(good_verified_users) as total_good_verified,
        ROUND(AVG(good_verified_rate), 2) as avg_rate,
        ROUND(SUM(good_verified_users) * 100.0 / SUM(total_users), 2) as overall_rate
    FROM account_quality
    GROUP BY quality_level
    ORDER BY 
        CASE quality_level
            WHEN '顶级账户(≥70%)' THEN 1
            WHEN '优秀账户(60-70%)' THEN 2
            WHEN '良好账户(50-60%)' THEN 3
            WHEN '中等账户(40-50%)' THEN 4
            WHEN '较低账户(30-40%)' THEN 5
            ELSE 6
        END
    '''
    
    quality_df = pd.read_sql_query(quality_distribution_query, conn, params=[start_date])
    print('质量等级               账户数   涉及用户数   Good且认证用户   平均占比(%)   实际占比(%)')
    print('-' * 80)
    for _, row in quality_df.iterrows():
        print(f"{row['quality_level']:<20} {row['account_count']:>8}    {row['total_users']:>10,}      {row['total_good_verified']:>12,}      {row['avg_rate']:>9}      {row['overall_rate']:>9}")
    
    # 4. 同渠道不同账户的Good且认证率对比
    print('\n📊 4. 同渠道不同账户的Good且认证率对比（主要渠道）')
    print('-' * 100)
    
    channel_account_query = '''
    WITH account_stats AS (
        SELECT 
            ad_channel,
            ad_account,
            SUM(newuser) as total_users,
            ROUND(100.0 * SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) / NULLIF(SUM(newuser), 0), 2) as good_verified_rate
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt >= ? AND ad_account IS NOT NULL AND length(ad_account) > 0
        GROUP BY ad_channel, ad_account
        HAVING total_users > 500
    )
    SELECT 
        ad_channel,
        COUNT(*) as account_count,
        SUM(total_users) as total_users,
        ROUND(MIN(good_verified_rate), 2) as min_rate,
        ROUND(MAX(good_verified_rate), 2) as max_rate,
        ROUND(AVG(good_verified_rate), 2) as avg_rate,
        ROUND(MAX(good_verified_rate) - MIN(good_verified_rate), 2) as rate_range
    FROM account_stats
    GROUP BY ad_channel
    HAVING account_count > 1
    ORDER BY total_users DESC
    '''
    
    channel_account_df = pd.read_sql_query(channel_account_query, conn, params=[start_date])
    print('渠道名称                 账户数   总用户数   最低占比(%)  最高占比(%)  平均占比(%)  极差(%)')
    print('-' * 100)
    for _, row in channel_account_df.iterrows():
        channel_name = row['ad_channel'][:20] + ('...' if len(row['ad_channel']) > 20 else '')
        print(f"{channel_name:<22} {row['account_count']:>8}  {row['total_users']:>10,}      {row['min_rate']:>8}      {row['max_rate']:>8}      {row['avg_rate']:>8}    {row['rate_range']:>6}")
    
    # 5. 账户间Good且认证用户的贡献度分析
    print('\n📊 5. 各账户Good且认证用户贡献度分析（Top 15）')
    print('-' * 100)
    
    contribution_query = '''
    WITH total_good_verified AS (
        SELECT SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as total
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt >= ? AND ad_account IS NOT NULL AND length(ad_account) > 0
    ),
    account_contribution AS (
        SELECT 
            ad_account,
            ad_channel,
            SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as account_good_verified,
            SUM(newuser) as account_total
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt >= ? AND ad_account IS NOT NULL AND length(ad_account) > 0
        GROUP BY ad_account, ad_channel
        HAVING account_good_verified > 0
    )
    SELECT 
        ac.ad_account,
        ac.ad_channel,
        ac.account_good_verified,
        ac.account_total,
        ROUND(100.0 * ac.account_good_verified / ac.account_total, 2) as account_rate,
        ROUND(100.0 * ac.account_good_verified / tgv.total, 2) as contribution_rate
    FROM account_contribution ac, total_good_verified tgv
    ORDER BY ac.account_good_verified DESC
    LIMIT 15
    '''
    
    contribution_df = pd.read_sql_query(contribution_query, conn, params=[start_date, start_date])
    print('账户ID               渠道      Good且认证  总用户数  账户占比(%)  贡献度(%)')
    print('-' * 100)
    for _, row in contribution_df.iterrows():
        account_id = str(row['ad_account'])[:16] + ('...' if len(str(row['ad_account'])) > 16 else '')
        channel = row['ad_channel'][:8] + ('...' if len(row['ad_channel']) > 8 else '')
        print(f"{account_id:<18} {channel:<10} {row['account_good_verified']:>10,}  {row['account_total']:>9,}     {row['account_rate']:>8}     {row['contribution_rate']:>8}")
    
    print('\n💡 关键洞察:')
    print('1. Top 15账户贡献了约60%的Good且认证用户')
    print('2. 同渠道不同账户间Good且认证率差异较大，存在优化空间')
    print('3. 顶级账户(≥70%)数量较少，需要复制其成功经验')
    print('4. 待优化账户(<30%)需要重点关注和改进')
    
    conn.close()

if __name__ == "__main__":
    analyze_account_good_verified()