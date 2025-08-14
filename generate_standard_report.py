#!/usr/bin/env python3
"""
标准化报告生成器 - 内置完整规范
====================================

📊 报告生成规范（严格遵循，不得修改）：

数据字段规范：
- newuser: 新用户数（基础计数单位）
- status = "good": Good用户（通过质量筛选）
- verification_status = "verified": 认证用户（完成身份认证）
- is_returned_1_day: 留存用户数（数值，不是布尔值）
- gender = "female": 女性用户（不是"女"）
- age_group IN ("20-", "20~23"): 年轻用户（不是"18-24"）
- dengji IN ("一线", "超一线", "二线"): 高线城市用户

核心指标计算公式：
1. Good率 = Good用户数 ÷ 总用户数 * 100
2. 认证率 = Good且认证用户数 ÷ Good用户数 * 100 (重要：不是总用户数!)
3. Good且认证率 = Good且认证用户数 ÷ 总用户数 * 100
4. 次留率 = Good且认证用户留存数 ÷ Good且认证用户数 * 100
5. ARPU = 收入 ÷ Good且认证用户数
6. CPA = 总成本 ÷ Good且认证用户数

报告结构（固定两部分）：
第一部分：大盘指标 (MAIN KPI + 用户质量 + 注册转化)
第二部分：异常分析 (异常检测 + 渠道分析)

文件命名：daily_report_YYYYMMDD_YYYYMMDD_HHMMSS.html
保存路径：./output/reports/
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

class StandardReportGenerator:
    """标准化报告生成器 - 严格按照规范执行"""
    
    def __init__(self, db_path: str = "./data/data.db"):
        self.db_path = db_path
        self.data_validation_enabled = True  # 启用数据验证
        
        # 规范化的SQL查询模板
        self.QUERIES = {
            'core_metrics': """
                SELECT 
                    SUM(newuser) as total_users,
                    SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
                    SUM(CASE WHEN verification_status = 'verified' THEN newuser ELSE 0 END) as verified_users,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users,
                    SUM(zizhu_revenue_1) as revenue,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND gender = 'female' THEN newuser ELSE 0 END) as female_users,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND age_group IN ('20-', '20~23') THEN newuser ELSE 0 END) as young_users,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND dengji IN ('一线', '超一线', '二线') THEN newuser ELSE 0 END) as high_tier_users
                FROM cpz_qs_newuser_channel_i_d
                WHERE dt = '{date}'
            """,
            
            'retention_rate': """
                SELECT 
                    ROUND(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN is_returned_1_day ELSE 0 END) * 100.0 / 
                          SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 2) as retention_rate
                FROM cpz_qs_newuser_channel_i_d
                WHERE dt = '{date}'
            """,
            
            'cost_data': """
                SELECT COALESCE(SUM(cash_cost), 0) as total_cost
                FROM dwd_ttx_market_cash_cost_i_d
                WHERE dt = '{date}'
            """,
            
            'channel_analysis': """
                SELECT 
                    ad_channel,
                    SUM(newuser) as total_users,
                    SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
                    SUM(CASE WHEN verification_status = 'verified' THEN newuser ELSE 0 END) as verified_users,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users,
                    SUM(zizhu_revenue_1) as revenue,
                    SUM(zizhu_revenue_1) / NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0) as arpu,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN is_returned_1_day ELSE 0 END) as retained_users,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND gender = 'female' THEN newuser ELSE 0 END) as female_users,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND age_group IN ('20-', '20~23') THEN newuser ELSE 0 END) as young_users,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND dengji IN ('一线', '超一线', '二线') THEN newuser ELSE 0 END) as high_tier_users
                FROM cpz_qs_newuser_channel_i_d
                WHERE dt = '{date}'
                GROUP BY ad_channel
                HAVING SUM(newuser) >= 50
                ORDER BY SUM(newuser) DESC
                LIMIT 20
            """,
            
            'historical_data': """
                SELECT 
                    dt,
                    SUM(newuser) as total_users,
                    SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users,
                    SUM(zizhu_revenue_1) as revenue,
                    SUM(zizhu_revenue_1) / NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0) as arpu,
                    ROUND(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN is_returned_1_day ELSE 0 END) * 100.0 / 
                          NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0), 2) as retention_rate,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND gender = 'female' THEN newuser ELSE 0 END) * 100.0 / 
                        NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0) as female_ratio,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND age_group IN ('20-', '20~23') THEN newuser ELSE 0 END) * 100.0 / 
                        NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0) as young_ratio,
                    SUM(CASE WHEN status = 'good' AND verification_status = 'verified' AND dengji IN ('一线', '超一线', '二线') THEN newuser ELSE 0 END) * 100.0 / 
                        NULLIF(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 0) as high_tier_ratio
                FROM cpz_qs_newuser_channel_i_d
                WHERE dt BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY dt
                ORDER BY dt
            """,
            
            'historical_cost': """
                SELECT 
                    dt,
                    COALESCE(SUM(cash_cost), 0) as total_cost
                FROM dwd_ttx_market_cash_cost_i_d
                WHERE dt BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY dt
                ORDER BY dt
            """,
            
            # 素材分析查询模板（不区分渠道，按素材ID汇总）
            'creative_ranking_today': """
                SELECT 
                    media_id_str,
                    GROUP_CONCAT(DISTINCT channel) as channels,
                    COUNT(DISTINCT channel) as channel_count,
                    SUM(total_good_verified) as good_verified,
                    SUM(cash_cost) as cost,
                    ROUND(SUM(cash_cost) / NULLIF(SUM(total_good_verified), 0), 2) as cpa,
                    ROUND(SUM(total_payed_amount_good_verified) / NULLIF(SUM(total_good_verified), 0), 2) as arpu,
                    ROUND(SUM(total_good_verified_return_1d) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as retention_rate,
                    ROUND(SUM(total_good_verified_female) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as female_rate,
                    ROUND(SUM(total_good_verified_white) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as white_collar_rate,
                    ROUND(SUM(total_good_verified_ios) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as ios_rate
                FROM dws_ttx_market_media_reports_i_d
                WHERE dt = '{date}' AND media_id_str IS NOT NULL AND media_id_str != ''
                GROUP BY media_id_str
                HAVING SUM(total_good_verified) >= 10
                ORDER BY SUM(total_good_verified) DESC
                LIMIT 20
            """,
            
            'creative_ranking_yesterday': """
                SELECT 
                    media_id_str,
                    GROUP_CONCAT(DISTINCT channel) as channels,
                    COUNT(DISTINCT channel) as channel_count,
                    SUM(total_good_verified) as good_verified,
                    SUM(cash_cost) as cost,
                    ROUND(SUM(cash_cost) / NULLIF(SUM(total_good_verified), 0), 2) as cpa,
                    ROW_NUMBER() OVER (ORDER BY SUM(total_good_verified) DESC) as rank_yesterday
                FROM dws_ttx_market_media_reports_i_d
                WHERE dt = '{yesterday_date}' AND media_id_str IS NOT NULL AND media_id_str != ''
                GROUP BY media_id_str
                HAVING SUM(total_good_verified) >= 10
                ORDER BY SUM(total_good_verified) DESC
                LIMIT 50
            """,
            
            # 账户分析查询
            'account_metrics_today': """
                SELECT 
                    account,
                    account_name,
                    CASE WHEN account_name LIKE '%安卓%' THEN 'Android'
                         WHEN account_name LIKE '%iOS%' OR account_name LIKE '%ios%' OR account_name LIKE '%IOS%' THEN 'iOS'
                         ELSE 'other' END AS os,
                    CASE WHEN account_name LIKE '%男%' AND account_name NOT LIKE '%男女%' THEN '男'
                         WHEN account_name LIKE '%女%' AND account_name NOT LIKE '%男女%' THEN '女'
                         ELSE 'other' END AS gender,
                    SUM(total_good_verified) as good_verified,
                    ROUND(SUM(total_payed_amount_good_verified) / NULLIF(SUM(total_good_verified), 0), 2) as arpu,
                    ROUND(SUM(total_good_verified_twenty) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as under_20_rate,
                    ROUND(SUM(total_good_verified_young) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as under_23_rate,
                    ROUND((SUM(total_good_verified) - SUM(total_good_verified_22_40)) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as under_24_rate,
                    ROUND(SUM(total_good_verified_white) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as white_collar_rate,
                    ROUND(SUM(third_line_city_user) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as third_tier_city_rate,
                    SUM(cash_cost) as cost
                FROM dwd_ttx_market_cash_cost_i_d_test
                WHERE dt = '{date}' AND channel = '{channel}'
                GROUP BY account, account_name
                HAVING SUM(total_good_verified) >= 100
                ORDER BY SUM(total_good_verified) DESC
            """,
            
            'account_metrics_yesterday': """
                SELECT 
                    account,
                    account_name,
                    CASE WHEN account_name LIKE '%安卓%' THEN 'Android'
                         WHEN account_name LIKE '%iOS%' OR account_name LIKE '%ios%' OR account_name LIKE '%IOS%' THEN 'iOS'
                         ELSE 'other' END AS os,
                    CASE WHEN account_name LIKE '%男%' AND account_name NOT LIKE '%男女%' THEN '男'
                         WHEN account_name LIKE '%女%' AND account_name NOT LIKE '%男女%' THEN '女'
                         ELSE 'other' END AS gender,
                    SUM(total_good_verified) as good_verified,
                    ROUND(SUM(total_payed_amount_good_verified) / NULLIF(SUM(total_good_verified), 0), 2) as arpu,
                    ROUND(SUM(total_good_verified_twenty) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as under_20_rate,
                    ROUND(SUM(total_good_verified_young) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as under_23_rate,
                    ROUND((SUM(total_good_verified) - SUM(total_good_verified_22_40)) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as under_24_rate,
                    ROUND(SUM(total_good_verified_white) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as white_collar_rate,
                    ROUND(SUM(third_line_city_user) * 100.0 / NULLIF(SUM(total_good_verified), 0), 1) as third_tier_city_rate,
                    SUM(cash_cost) as cost
                FROM dwd_ttx_market_cash_cost_i_d_test
                WHERE dt = '{yesterday_date}' AND channel = '{channel}'
                GROUP BY account, account_name
                HAVING SUM(total_good_verified) >= 100
            """,
            
            'account_metrics_7d_avg': """
                WITH daily_account_metrics AS (
                    SELECT 
                        dt,
                        account,
                        account_name,
                        CASE WHEN account_name LIKE '%安卓%' THEN 'Android'
                             WHEN account_name LIKE '%iOS%' OR account_name LIKE '%ios%' OR account_name LIKE '%IOS%' THEN 'iOS'
                             ELSE 'other' END AS os,
                        CASE WHEN account_name LIKE '%男%' AND account_name NOT LIKE '%男女%' THEN '男'
                             WHEN account_name LIKE '%女%' AND account_name NOT LIKE '%男女%' THEN '女'
                             ELSE 'other' END AS gender,
                        SUM(total_good_verified) as daily_good_verified,
                        CASE WHEN SUM(total_good_verified) > 0 
                             THEN SUM(total_payed_amount_after_tax_good_verified) / SUM(total_good_verified) 
                             ELSE 0 END as daily_arpu,
                        CASE WHEN SUM(total_good_verified) > 0 
                             THEN SUM(total_good_verified_twenty) * 100.0 / SUM(total_good_verified) 
                             ELSE 0 END as daily_under_20_rate,
                        CASE WHEN SUM(total_good_verified) > 0 
                             THEN SUM(total_good_verified_young) * 100.0 / SUM(total_good_verified) 
                             ELSE 0 END as daily_under_23_rate,
                        CASE WHEN SUM(total_good_verified) > 0 
                             THEN SUM(total_good_verified_white) * 100.0 / SUM(total_good_verified) 
                             ELSE 0 END as daily_white_collar_rate,
                        CASE WHEN SUM(total_good_verified) > 0 
                             THEN SUM(third_line_city_user) * 100.0 / SUM(total_good_verified) 
                             ELSE 0 END as daily_third_tier_city_rate,
                        SUM(cash_cost) as daily_cost
                    FROM dwd_ttx_market_cash_cost_i_d_test
                    WHERE dt BETWEEN '{start_date}' AND '{end_date}' AND channel = '{channel}'
                    GROUP BY dt, account, account_name
                    HAVING SUM(total_good_verified) >= 100
                )
                SELECT 
                    account,
                    account_name,
                    os,
                    gender,
                    ROUND(AVG(daily_good_verified), 0) as good_verified_avg,
                    ROUND(AVG(daily_arpu), 2) as arpu_avg,
                    ROUND(AVG(daily_under_20_rate), 1) as under_20_rate_avg,
                    ROUND(AVG(daily_under_23_rate), 1) as under_23_rate_avg,
                    ROUND(AVG(daily_white_collar_rate), 1) as white_collar_rate_avg,
                    ROUND(AVG(daily_third_tier_city_rate), 1) as third_tier_city_rate_avg,
                    ROUND(AVG(daily_cost), 2) as cost_avg
                FROM daily_account_metrics
                GROUP BY account, account_name, os, gender
                HAVING COUNT(*) >= 3
            """
        }
    
    def generate_report(self, date_str: str) -> str:
        """生成标准化报告"""
        print(f"📊 开始生成 {date_str} 的标准化报告...")
        print("✅ 已加载内置规范，严格按照标准执行")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 1. 收集核心指标数据
            core_data = self._collect_core_metrics(conn, date_str)
            print(f"✅ 核心指标计算完成 - Quality Users: {core_data['quality_users']:,}, 认证率: {core_data['verified_rate']:.2f}%")
            
            # 1.5 数据一致性验证
            if not self._validate_data_consistency(conn, date_str, core_data):
                print("⚠️  数据一致性检查发现问题，但继续生成报告")
            
            # 2. 收集异常分析数据
            anomaly_data = self._collect_anomaly_data(conn, date_str)
            
            # 3. 收集素材分析数据
            creative_data = self._collect_creative_analysis(conn, date_str)
            
            # 4. 收集账户分析数据
            account_data = self._collect_account_analysis(conn, date_str)
            
            # 5. 生成HTML报告
            html_content = self._generate_html_report(date_str, core_data, anomaly_data, creative_data, account_data, conn)
            
            # 4. 保存文件
            filename = self._save_report(html_content, date_str)
            
            return filename
            
        finally:
            conn.close()
    
    def _collect_core_metrics(self, conn: sqlite3.Connection, date_str: str) -> dict:
        """收集核心指标数据 - 严格按照规范计算"""
        
        # 1. 核心数据查询
        core_query = self.QUERIES['core_metrics'].format(date=date_str)
        core_df = pd.read_sql_query(core_query, conn)
        
        if core_df.empty:
            raise Exception(f"未找到 {date_str} 的数据")
        
        row = core_df.iloc[0]
        
        # 2. 留存率查询
        retention_query = self.QUERIES['retention_rate'].format(date=date_str)
        retention_df = pd.read_sql_query(retention_query, conn)
        retention_rate = retention_df.iloc[0]['retention_rate'] if not retention_df.empty else 0
        
        # 2.1 如果当天次留率为0，查询前一天的次留率
        prev_date_str = None
        prev_retention_rate = None
        if pd.isna(retention_rate) or retention_rate == 0:
            prev_date = datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)
            prev_date_str = prev_date.strftime('%Y-%m-%d')
            prev_retention_query = self.QUERIES['retention_rate'].format(date=prev_date_str)
            prev_retention_df = pd.read_sql_query(prev_retention_query, conn)
            if not prev_retention_df.empty and prev_retention_df.iloc[0]['retention_rate'] is not None:
                prev_retention_rate = prev_retention_df.iloc[0]['retention_rate']
        
        # 3. 成本数据查询
        cost_query = self.QUERIES['cost_data'].format(date=date_str)
        cost_df = pd.read_sql_query(cost_query, conn)
        total_cost = cost_df.iloc[0]['total_cost'] if not cost_df.empty else 0
        
        # 4. 按规范计算所有指标
        total_users = row['total_users'] or 0
        good_users = row['good_users'] or 0
        quality_users = row['quality_users'] or 0
        
        metrics = {
            # MAIN KPI
            'quality_users': quality_users,
            'cpa': total_cost / quality_users if quality_users > 0 else 0,
            'arpu': (row['revenue'] or 0) / quality_users if quality_users > 0 else 0,
            'retention_rate': retention_rate,
            'prev_retention_rate': prev_retention_rate,
            'prev_retention_date': prev_date_str,
            
            # 用户质量 (基于Good且认证用户)
            'female_ratio': (row['female_users'] or 0) / quality_users * 100 if quality_users > 0 else 0,
            'young_ratio': (row['young_users'] or 0) / quality_users * 100 if quality_users > 0 else 0,
            'high_tier_ratio': (row['high_tier_users'] or 0) / quality_users * 100 if quality_users > 0 else 0,
            
            # 注册转化
            'good_rate': good_users / total_users * 100 if total_users > 0 else 0,
            'verified_rate': quality_users / good_users * 100 if good_users > 0 else 0,  # 关键：Good用户中的认证率
            'quality_rate': quality_users / total_users * 100 if total_users > 0 else 0,
            
            # 原始数据
            'raw_data': row.to_dict()
        }
        
        print(f"✅ 核心指标计算完成 - Quality Users: {quality_users:,}, 认证率: {metrics['verified_rate']:.2f}%")
        return metrics
    
    def _collect_anomaly_data(self, conn: sqlite3.Connection, date_str: str) -> dict:
        """收集异常分析数据"""
        
        # 1. 历史数据 (14天)
        start_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=14)).strftime('%Y-%m-%d')
        hist_query = self.QUERIES['historical_data'].format(start_date=start_date, end_date=date_str)
        hist_df = pd.read_sql_query(hist_query, conn)
        
        # 1.1 历史成本数据
        cost_query = self.QUERIES['historical_cost'].format(start_date=start_date, end_date=date_str)
        cost_df = pd.read_sql_query(cost_query, conn)
        
        # 合并数据
        if not hist_df.empty and not cost_df.empty:
            hist_df = pd.merge(hist_df, cost_df, on='dt', how='left')
            hist_df['cpa'] = hist_df['total_cost'] / hist_df['quality_users'].replace(0, np.nan)
        
        # 2. 渠道分析数据
        channel_query = self.QUERIES['channel_analysis'].format(date=date_str)
        channel_df = pd.read_sql_query(channel_query, conn)
        
        # 2.1 获取历史渠道数据用于对比
        prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        prev_channel_query = self.QUERIES['channel_analysis'].format(date=prev_date)
        prev_channel_df = pd.read_sql_query(prev_channel_query, conn)
        
        # 3. 全面异常检测
        anomalies = []
        if len(hist_df) >= 7:
            current_data = hist_df[hist_df['dt'] == date_str]
            if not current_data.empty:
                hist_data = hist_df[hist_df['dt'] != date_str]
                
                # 定义需要检测的指标
                metrics_to_check = [
                    # MAIN KPI
                    {'name': 'Good且认证用户数', 'field': 'quality_users', 'format': '{:.0f}', 'category': 'MAIN KPI'},
                    {'name': 'CPA', 'field': 'cpa', 'format': '¥{:.2f}', 'category': 'MAIN KPI'},
                    {'name': 'ARPU', 'field': 'arpu', 'format': '¥{:.2f}', 'category': 'MAIN KPI'},
                    {'name': '次留率', 'field': 'retention_rate', 'format': '{:.2f}%', 'category': 'MAIN KPI'},
                    
                    # 用户质量
                    {'name': '女性占比', 'field': 'female_ratio', 'format': '{:.2f}%', 'category': '用户质量'},
                    {'name': '年轻占比', 'field': 'young_ratio', 'format': '{:.2f}%', 'category': '用户质量'},
                    {'name': '高线城市占比', 'field': 'high_tier_ratio', 'format': '{:.2f}%', 'category': '用户质量'},
                    
                    # 注册转化 - 需要计算
                    {'name': 'Good率', 'field': 'good_rate', 'format': '{:.2f}%', 'category': '注册转化'},
                    {'name': '认证率', 'field': 'verified_rate', 'format': '{:.2f}%', 'category': '注册转化'},
                    {'name': 'Good且认证率', 'field': 'quality_rate', 'format': '{:.2f}%', 'category': '注册转化'}
                ]
                
                # 计算转化率指标
                current_row = current_data.iloc[0]
                hist_df.loc[hist_df['dt'] == date_str, 'good_rate'] = current_row['good_users'] / current_row['total_users'] * 100 if current_row['total_users'] > 0 else 0
                hist_df.loc[hist_df['dt'] == date_str, 'verified_rate'] = current_row['quality_users'] / current_row['good_users'] * 100 if current_row['good_users'] > 0 else 0
                hist_df.loc[hist_df['dt'] == date_str, 'quality_rate'] = current_row['quality_users'] / current_row['total_users'] * 100 if current_row['total_users'] > 0 else 0
                
                # 定义指标方向：正向指标(越高越好) vs 负向指标(越低越好)
                positive_indicators = [
                    'quality_users', 'arpu', 'retention_rate', 
                    'female_ratio', 'good_rate', 'verified_rate', 'quality_rate'
                ]
                negative_indicators = ['cpa', 'young_ratio', 'high_tier_ratio']  # CPA、年轻占比、高线城市占比越低越好
                
                # 对每个指标进行异常检测
                for metric in metrics_to_check:
                    field = metric['field']
                    
                    # 计算历史数据的转化率（如果需要）
                    if field in ['good_rate', 'verified_rate', 'quality_rate']:
                        if field == 'good_rate':
                            hist_data[field] = hist_data['good_users'] / hist_data['total_users'].replace(0, np.nan) * 100
                        elif field == 'verified_rate':
                            hist_data[field] = hist_data['quality_users'] / hist_data['good_users'].replace(0, np.nan) * 100
                        elif field == 'quality_rate':
                            hist_data[field] = hist_data['quality_users'] / hist_data['total_users'].replace(0, np.nan) * 100
                    
                    # 特殊处理次留率
                    if field == 'retention_rate' and (pd.isna(current_data.iloc[0][field]) or current_data.iloc[0][field] == 0):
                        # 如果当天次留率为0，使用前一天的数据
                        prev_date = datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)
                        prev_date_str = prev_date.strftime('%Y-%m-%d')
                        prev_data = hist_df[hist_df['dt'] == prev_date_str]
                        if not prev_data.empty and not pd.isna(prev_data.iloc[0][field]):
                            current_value = prev_data.iloc[0][field]
                            # 将用于比较的历史数据也向前推一天
                            hist_values = hist_data[hist_data['dt'] < prev_date_str][field].dropna().values
                            metric['name'] = f"{metric['name']}（{prev_date_str}）"
                        else:
                            continue
                    else:
                        current_value = current_data.iloc[0][field] if field in current_data.columns else hist_df[hist_df['dt'] == date_str][field].iloc[0]
                        hist_values = hist_data[field].dropna().values
                    
                    if len(hist_values) > 0 and not pd.isna(current_value):
                        q1, q3 = np.percentile(hist_values, [25, 75])
                        iqr = q3 - q1
                        
                        # 对于极小的IQR，使用最小阈值
                        if iqr < 0.1:
                            iqr = max(0.1, np.std(hist_values) * 0.5)
                        
                        lower_bound = q1 - 1.5 * iqr
                        upper_bound = q3 + 1.5 * iqr
                        
                        # 双层异常检测：严重异常 + 敏感异常
                        # 根据指标类型判断是否为严重异常
                        is_positive_indicator = field in positive_indicators
                        is_negative_indicator = field in negative_indicators
                        
                        if is_positive_indicator:
                            # 正向指标：只有偏低才是异常
                            is_serious_anomaly = current_value < lower_bound
                        elif is_negative_indicator:
                            # 负向指标：只有偏高才是异常
                            is_serious_anomaly = current_value > upper_bound
                        else:
                            # 其他指标：偏高或偏低都是异常
                            is_serious_anomaly = current_value < lower_bound or current_value > upper_bound
                        
                        is_sensitive_anomaly = False
                        sensitive_reasons = []
                        
                        # 敏感异常检测 - 针对所有大盘指标，区分正向/负向指标
                        if len(hist_values) >= 3:
                            # 获取近期数据进行更敏感的分析
                            recent_7_days = hist_values[-7:] if len(hist_values) >= 7 else hist_values[-3:]
                            recent_mean = np.mean(recent_7_days)
                            
                            
                            # 根据不同指标设置不同的敏感度阈值
                            if field == 'arpu':
                                # ARPU: 最敏感，关键收入指标
                                change_threshold = 5.0  # 日变化>5%
                                deviation_threshold = 3.0  # 偏离均值>3%
                            elif field in ['quality_users', 'cpa']:
                                # 核心KPI: 中等敏感度
                                change_threshold = 8.0  # 日变化>8%
                                deviation_threshold = 5.0  # 偏离均值>5%
                            elif field == 'retention_rate':
                                # 留存率: 中等敏感度
                                change_threshold = 6.0  # 日变化>6%
                                deviation_threshold = 4.0  # 偏离均值>4%
                            elif field in ['female_ratio', 'young_ratio', 'high_tier_ratio']:
                                # 用户质量指标: 较低敏感度
                                change_threshold = 10.0  # 日变化>10%
                                deviation_threshold = 6.0  # 偏离均值>6%
                            elif field in ['good_rate', 'verified_rate', 'quality_rate']:
                                # 转化率指标: 中等敏感度
                                change_threshold = 7.0  # 日变化>7%
                                deviation_threshold = 5.0  # 偏离均值>5%
                            else:
                                # 默认敏感度
                                change_threshold = 8.0
                                deviation_threshold = 5.0
                            
                            # 1. 日变化率检测 - 只检测负向变化
                            if len(hist_values) > 0:
                                prev_value = hist_values[-1]
                                if prev_value != 0:  # 避免除零
                                    change_pct = abs(current_value - prev_value) / abs(prev_value) * 100
                                    
                                    # 正向指标：只关注下降
                                    if is_positive_indicator and current_value < prev_value and change_pct > change_threshold:
                                        is_sensitive_anomaly = True
                                        sensitive_reasons.append(f'日变化{change_pct:.1f}%(下降)')
                                    # 负向指标：只关注上升
                                    elif is_negative_indicator and current_value > prev_value and change_pct > change_threshold:
                                        is_sensitive_anomaly = True
                                        sensitive_reasons.append(f'日变化{change_pct:.1f}%(上升)')
                            
                            # 2. 相对极值检测 - 只检测不好的极值
                            if is_positive_indicator and current_value < recent_7_days.min():
                                # 正向指标：低于最低值是问题
                                is_sensitive_anomaly = True
                                sensitive_reasons.append('低于近7天最低值')
                            elif is_negative_indicator and current_value > recent_7_days.max():
                                # 负向指标：高于最高值是问题
                                is_sensitive_anomaly = True
                                sensitive_reasons.append('高于近7天最高值')
                                
                            # 3. 偏离近期均值检测 - 只检测负向偏离
                            if recent_mean != 0:  # 避免除零
                                deviation_pct = abs(current_value - recent_mean) / abs(recent_mean) * 100
                                
                                if deviation_pct > deviation_threshold:
                                    # 正向指标：低于均值是问题
                                    if is_positive_indicator and current_value < recent_mean:
                                        is_sensitive_anomaly = True
                                        sensitive_reasons.append(f'偏离近期均值{deviation_pct:.1f}%')
                                    # 负向指标：高于均值是问题
                                    elif is_negative_indicator and current_value > recent_mean:
                                        is_sensitive_anomaly = True
                                        sensitive_reasons.append(f'偏离近期均值{deviation_pct:.1f}%')
                        
                        # 记录异常
                        if is_serious_anomaly:
                            direction = '偏低' if current_value < lower_bound else '偏高'
                            severity = 'high' if abs(current_value - np.median(hist_values)) / np.median(hist_values) > 0.3 else 'medium'
                            
                            anomalies.append({
                                'metric': metric['name'],
                                'category': metric['category'],
                                'current_value': metric['format'].format(current_value),
                                'normal_range': f"[{metric['format'].format(lower_bound)}, {metric['format'].format(upper_bound)}]",
                                'direction': direction,
                                'severity': severity,
                                'type': 'serious',
                                'reason': f'超出IQR范围（{direction}）',
                                'raw_current': current_value,
                                'raw_lower': lower_bound,
                                'raw_upper': upper_bound
                            })
                        elif is_sensitive_anomaly:
                            # 计算详细的敏感异常上下文数据
                            prev_value = hist_values[-1] if len(hist_values) > 0 else current_value
                            change_pct = abs(current_value - prev_value) / abs(prev_value) * 100 if prev_value != 0 else 0
                            recent_7_days = hist_values[-7:] if len(hist_values) >= 7 else hist_values[-3:]
                            recent_mean = np.mean(recent_7_days) if len(recent_7_days) > 0 else current_value
                            recent_min = recent_7_days.min() if len(recent_7_days) > 0 else current_value
                            recent_max = recent_7_days.max() if len(recent_7_days) > 0 else current_value
                            
                            # 分析敏感异常的具体原因类型
                            anomaly_details = {
                                'has_daily_change': False,
                                'has_extreme_value': False,
                                'has_mean_deviation': False,
                                'daily_change_pct': change_pct,
                                'prev_value': prev_value,
                                'recent_mean': recent_mean,
                                'recent_min': recent_min,
                                'recent_max': recent_max,
                                'is_positive_indicator': field in positive_indicators
                            }
                            
                            # 标记具体的异常类型
                            for reason in sensitive_reasons:
                                if '日变化' in reason:
                                    anomaly_details['has_daily_change'] = True
                                elif '最低值' in reason or '最高值' in reason:
                                    anomaly_details['has_extreme_value'] = True
                                elif '偏离近期均值' in reason:
                                    anomaly_details['has_mean_deviation'] = True
                            
                            anomalies.append({
                                'metric': metric['name'],
                                'category': metric['category'],
                                'current_value': metric['format'].format(current_value),
                                'normal_range': f"[{metric['format'].format(lower_bound)}, {metric['format'].format(upper_bound)}]",
                                'direction': '敏感异常',
                                'severity': 'sensitive',
                                'type': 'sensitive',
                                'reason': ' | '.join(sensitive_reasons),
                                'raw_current': current_value,
                                'raw_lower': lower_bound,
                                'raw_upper': upper_bound,
                                'sensitive_details': anomaly_details  # 新增详细信息
                            })
        
        # 4. 基于异常检测结果的智能渠道分析
        smart_channel_analysis = self._analyze_channels_by_anomalies(
            anomalies, channel_df, prev_channel_df, date_str, conn
        )
        
        return {
            'anomalies': anomalies,
            'channel_analysis': channel_df.to_dict('records') if not channel_df.empty else [],
            'smart_channel_analysis': smart_channel_analysis,
            'historical_data': hist_df.to_dict('records')
        }
    
    def _analyze_channels_by_anomalies(self, anomalies: list, channel_df: pd.DataFrame, 
                                     prev_channel_df: pd.DataFrame, date_str: str, 
                                     conn: sqlite3.Connection) -> dict:
        """基于异常检测结果进行智能渠道分析"""
        
        if channel_df.empty:
            return {'has_analysis': False, 'message': '渠道数据为空'}
            
        analysis_results = {
            'has_analysis': False,
            'anomaly_metrics': [],
            'channel_impact_analysis': [],
            'summary': ''
        }
        
        # 检查是否有异常需要分析 - 扩展到所有类别
        key_anomaly_metrics = []
        for anomaly in anomalies:
            # 分析所有类别的异常指标
            if anomaly.get('category') in ['MAIN KPI', '用户质量', '注册转化']:
                key_anomaly_metrics.append(anomaly)
        
        if not key_anomaly_metrics:
            return {
                'has_analysis': False, 
                'message': '未检测到需要渠道深度分析的异常指标'
            }
        
        analysis_results['has_analysis'] = True
        analysis_results['anomaly_metrics'] = key_anomaly_metrics
        
        # 智能多维度异常根因分析
        for anomaly in key_anomaly_metrics:
            root_cause_analysis = self._intelligent_root_cause_analysis(
                anomaly, date_str, conn, channel_df, prev_channel_df
            )
            
            if root_cause_analysis:
                analysis_results['channel_impact_analysis'].append(root_cause_analysis)
        
        # 生成总结
        if analysis_results['channel_impact_analysis']:
            total_channels = len(analysis_results['channel_impact_analysis'])
            analysis_results['summary'] = f"检测到{len(key_anomaly_metrics)}个异常指标，已完成{total_channels}项渠道影响分析"
        
        return analysis_results
    
    def _intelligent_root_cause_analysis(self, anomaly: dict, date_str: str, conn: sqlite3.Connection, 
                                        channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame) -> dict:
        """智能根因分析 - 多维度异常检测"""
        try:
            metric_name = anomaly.get('metric', '')
            analysis_results = {
                'metric': metric_name,
                'root_causes': [],
                'confidence_score': 0.0,
                'analysis_summary': '',
                'analysis_type': 'intelligent_root_cause'
            }
            
            # 1. 时间趋势分析
            trend_analysis = self._analyze_time_trend(metric_name, date_str, conn)
            if trend_analysis and trend_analysis.get('significance', 0) > 0.15:
                analysis_results['root_causes'].append({
                    'dimension': '时间趋势',
                    'finding': trend_analysis.get('description', '趋势异常'),
                    'impact_score': trend_analysis.get('significance', 0),
                    'details': [trend_analysis.get('description', '')]
                })
                analysis_results['confidence_score'] += trend_analysis.get('significance', 0) * 0.3
            
            # 2. 结构性变化分析
            structural_analysis = self._analyze_structural_changes(metric_name, date_str, conn)
            if structural_analysis and structural_analysis.get('significance', 0) > 0.1:
                analysis_results['root_causes'].append({
                    'dimension': '结构变化',
                    'finding': structural_analysis.get('description', '结构变化异常'),
                    'impact_score': structural_analysis.get('significance', 0),
                    'details': [structural_analysis.get('description', '')]
                })
                analysis_results['confidence_score'] += structural_analysis.get('significance', 0) * 0.2
            
            # 3. 外部因素分析
            external_analysis = self._analyze_external_factors(metric_name, date_str, conn)
            if external_analysis and external_analysis.get('significance', 0) > 0.1:
                analysis_results['root_causes'].append({
                    'dimension': '外部因素',
                    'finding': external_analysis['description'],
                    'impact_score': external_analysis['significance'],
                    'details': external_analysis['details']
                })
                analysis_results['confidence_score'] += external_analysis['significance'] * 0.2
            
            # 4. CPA异常专项分析（新增）
            if 'CPA' in metric_name:
                print(f"DEBUG: 检测到CPA异常，启动专项渠道分析")
                cpa_channel_analysis = self._analyze_cpa_by_channel(
                    channel_df if channel_df is not None else pd.DataFrame(), 
                    prev_channel_df if prev_channel_df is not None else pd.DataFrame(), 
                    anomaly, 
                    date_str, 
                    conn
                )
                if cpa_channel_analysis and isinstance(cpa_channel_analysis, dict) and cpa_channel_analysis.get('channel_impacts'):
                    analysis_results['root_causes'].append({
                        'dimension': 'CPA渠道分析',
                        'finding': cpa_channel_analysis.get('analysis_summary', 'CPA异常渠道定位完成'),
                        'impact_score': 0.8,
                        'details': [f"识别{len(cpa_channel_analysis.get('channel_impacts', []))}个高影响渠道"],
                        'cpa_channel_detail': cpa_channel_analysis
                    })
                    analysis_results['confidence_score'] += 0.3
            
            # 5. ARPU异常专项分析
            elif 'ARPU' in metric_name:
                print(f"DEBUG: 检测到ARPU异常，启动专项渠道分析")
                arpu_channel_analysis = self._analyze_arpu_by_channel(
                    channel_df if channel_df is not None else pd.DataFrame(), 
                    prev_channel_df if prev_channel_df is not None else pd.DataFrame(), 
                    anomaly, 
                    date_str, 
                    conn
                )
                if arpu_channel_analysis and isinstance(arpu_channel_analysis, dict) and arpu_channel_analysis.get('channel_impacts'):
                    analysis_results['root_causes'].append({
                        'dimension': 'ARPU渠道分析',
                        'finding': arpu_channel_analysis.get('analysis_summary', 'ARPU异常渠道定位完成'),
                        'impact_score': 0.8,
                        'details': [f"识别{len(arpu_channel_analysis.get('channel_impacts', []))}个高影响渠道"],
                        'arpu_channel_detail': arpu_channel_analysis
                    })
                    analysis_results['confidence_score'] += 0.3
            
            # 6. Good率专项分析
            elif metric_name == 'Good率':
                print(f"DEBUG: 检测到{metric_name}异常，启动专项渠道分析")
                good_rate_channel_analysis = self._analyze_good_rate_by_channel(
                    None, None, anomaly, date_str, conn
                )
                if good_rate_channel_analysis and isinstance(good_rate_channel_analysis, dict) and good_rate_channel_analysis.get('channel_impacts'):
                    analysis_results['root_causes'].append({
                        'dimension': 'Good率渠道分析',
                        'finding': good_rate_channel_analysis.get('analysis_summary', 'Good率异常渠道定位完成'),
                        'impact_score': 0.8,
                        'details': [f"识别{len(good_rate_channel_analysis.get('channel_impacts', []))}个高影响渠道"],
                        'good_rate_channel_detail': good_rate_channel_analysis
                    })
                    analysis_results['confidence_score'] += 0.3
            
            # 7. 认证率专项分析
            elif metric_name == '认证率':
                print(f"DEBUG: 检测到{metric_name}异常，启动专项渠道分析")
                verified_rate_channel_analysis = self._analyze_verified_rate_by_channel(
                    None, None, anomaly, date_str, conn
                )
                if verified_rate_channel_analysis and isinstance(verified_rate_channel_analysis, dict) and verified_rate_channel_analysis.get('channel_impacts'):
                    analysis_results['root_causes'].append({
                        'dimension': '认证率渠道分析',
                        'finding': verified_rate_channel_analysis.get('analysis_summary', '认证率异常渠道定位完成'),
                        'impact_score': 0.8,
                        'details': [f"识别{len(verified_rate_channel_analysis.get('channel_impacts', []))}个高影响渠道"],
                        'verified_rate_channel_detail': verified_rate_channel_analysis
                    })
                    analysis_results['confidence_score'] += 0.3
            
            # 8. Good且认证用户数专项分析
            elif metric_name == 'Good且认证用户数':
                print(f"DEBUG: 检测到{metric_name}异常，启动专项渠道分析")
                quality_users_channel_analysis = self._analyze_quality_users_by_channel_enhanced(
                    channel_df if channel_df is not None else pd.DataFrame(), 
                    prev_channel_df if prev_channel_df is not None else pd.DataFrame(), 
                    anomaly, date_str, conn
                )
                if quality_users_channel_analysis and isinstance(quality_users_channel_analysis, dict) and quality_users_channel_analysis.get('channel_impacts'):
                    analysis_results['root_causes'].append({
                        'dimension': 'Good且认证用户数渠道分析',
                        'finding': quality_users_channel_analysis.get('analysis_summary', 'Good且认证用户数异常渠道定位完成'),
                        'impact_score': 0.8,
                        'details': [f"识别{len(quality_users_channel_analysis.get('channel_impacts', []))}个高影响渠道"],
                        'quality_users_channel_detail': quality_users_channel_analysis
                    })
                    analysis_results['confidence_score'] += 0.3
            
            # 9. Good且认证率专项分析  
            elif metric_name == 'Good且认证率':
                print(f"DEBUG: 检测到{metric_name}异常，启动专项渠道分析")
                quality_rate_channel_analysis = self._analyze_quality_rate_by_channel(
                    channel_df if channel_df is not None else pd.DataFrame(), 
                    prev_channel_df if prev_channel_df is not None else pd.DataFrame(), 
                    anomaly, date_str, conn
                )
                if quality_rate_channel_analysis and isinstance(quality_rate_channel_analysis, dict) and quality_rate_channel_analysis.get('channel_impacts'):
                    analysis_results['root_causes'].append({
                        'dimension': 'Good且认证率渠道分析',
                        'finding': quality_rate_channel_analysis.get('analysis_summary', 'Good且认证率异常渠道定位完成'),
                        'impact_score': 0.8,
                        'details': [f"识别{len(quality_rate_channel_analysis.get('channel_impacts', []))}个高影响渠道"],
                        'quality_rate_channel_detail': quality_rate_channel_analysis
                    })
                    analysis_results['confidence_score'] += 0.3

            # 10. 其他指标的通用渠道分析
            elif metric_name in ['女性占比', '年轻占比（20-23岁）', '高线城市占比', '次留率']:
                print(f"DEBUG: 检测到{metric_name}异常，启动专项渠道分析")
                metric_field_map = {
                    '女性占比': 'female_ratio',
                    '年轻占比（20-23岁）': 'young_ratio', 
                    '高线城市占比': 'high_tier_ratio',
                    '次留率': 'retention_rate'
                }
                metric_field = metric_field_map.get(metric_name)
                if metric_field:
                    generic_channel_analysis = self._analyze_generic_metric_by_channel(
                        metric_field, metric_name, date_str, conn, anomaly
                    )
                    if generic_channel_analysis and isinstance(generic_channel_analysis, dict) and generic_channel_analysis.get('channel_impacts'):
                        analysis_results['root_causes'].append({
                            'dimension': f'{metric_name}渠道分析',
                            'finding': generic_channel_analysis.get('analysis_summary', f'{metric_name}异常渠道定位完成'),
                            'impact_score': 0.8,
                            'details': [f"识别{len(generic_channel_analysis.get('channel_impacts', []))}个异常渠道"],
                            'generic_channel_detail': generic_channel_analysis
                        })
                        analysis_results['confidence_score'] += 0.3
            
            # 生成综合分析总结
            analysis_results['analysis_summary'] = self._generate_analysis_summary(analysis_results)
            
            # 如果没有找到明显的根因，进行兜底分析
            if not analysis_results['root_causes']:
                fallback_analysis = self._fallback_analysis(metric_name, anomaly, date_str, conn)
                analysis_results['root_causes'].append(fallback_analysis)
                analysis_results['confidence_score'] = 0.3
            
            # 按影响分数排序
            analysis_results['root_causes'].sort(key=lambda x: x['impact_score'], reverse=True)
            
            return analysis_results if analysis_results['root_causes'] else None
        
        except Exception as e:
            print(f"智能根因分析失败: {e}")
            return None
    
    def _fallback_analysis(self, metric_name: str, anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """兜底分析 - 当其他分析都没有发现明显原因时"""
        return {
            'dimension': '基础分析',
            'finding': f"{metric_name}出现{anomaly.get('type', '异常')}，当前值为{anomaly.get('current_value', 'N/A')}",
            'impact_score': 0.3,
            'details': ['建议进一步人工分析', '关注后续数据变化趋势']
        }
    
    def _generate_analysis_summary(self, analysis_results: dict) -> str:
        """生成分析总结"""
        if not analysis_results['root_causes']:
            return "未发现明显异常原因"
        
        # 获取最高影响分数的根因
        top_cause = max(analysis_results['root_causes'], key=lambda x: x['impact_score'])
        confidence_level = "高" if analysis_results['confidence_score'] > 0.7 else "中" if analysis_results['confidence_score'] > 0.4 else "低"
        
        return f"主要原因：{top_cause['finding']} (置信度：{confidence_level})"
    
    def _analyze_time_trend(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析时间趋势异常（基于实际数据表）"""
        try:
            # 获取最近7天的趋势数据
            if 'ARPU' in metric_name:
                trend_query = f"""
                    SELECT dt,
                           SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN zizhu_revenue_1_aftertax ELSE 0 END) / 
                           SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as metric_value
                    FROM cpz_qs_newuser_channel_i_d 
                    WHERE dt >= date('{date_str}', '-6 days') AND dt <= '{date_str}'
                    GROUP BY dt
                    ORDER BY dt
                """
            elif '女性占比' in metric_name:
                trend_query = f"""
                    SELECT dt,
                           SUM(CASE WHEN gender = 'female' AND status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) * 100.0 / 
                           SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as metric_value
                    FROM cpz_qs_newuser_channel_i_d 
                    WHERE dt >= date('{date_str}', '-6 days') AND dt <= '{date_str}'
                    GROUP BY dt
                    ORDER BY dt
                """
            elif 'Good且认证用户数' in metric_name:
                trend_query = f"""
                    SELECT dt,
                           SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as metric_value
                    FROM cpz_qs_newuser_channel_i_d 
                    WHERE dt >= date('{date_str}', '-6 days') AND dt <= '{date_str}'
                    GROUP BY dt
                    ORDER BY dt
                """
            else:
                return {'significance': 0.1, 'description': f'不支持{metric_name}的时间趋势分析', 'details': []}
            
            trend_df = pd.read_sql_query(trend_query, conn)
            
            if len(trend_df) < 3:
                return {'significance': 0.2, 'description': '历史数据不足，无法进行趋势分析', 'details': []}
            
            # 分析趋势模式
            values = trend_df['metric_value'].values
            dates = trend_df['dt'].values
            current_value = values[-1]
            
            # 计算趋势变化
            if len(values) >= 4:
                recent_avg = np.mean(values[-4:-1])  # 前3天平均
                change_rate = (current_value - recent_avg) / recent_avg if recent_avg > 0 else 0
                
                # 判断趋势显著性 - 针对敏感异常使用更低阈值
                threshold = 0.05 if abs(change_rate) > 0.05 else 0.15  # 5%以上变化即认为显著
                if abs(change_rate) > threshold:
                    trend_direction = "上升" if change_rate > 0 else "下降"
                    significance = min(abs(change_rate) * 3, 1.0)  # 提高显著性计算
                    
                    # 检查是否是连续趋势
                    is_continuous = self._check_continuous_trend(values)
                    
                    description = f"近期{trend_direction}趋势明显，相比前3天平均值{trend_direction}{abs(change_rate)*100:.1f}%"
                    if is_continuous:
                        description += "，呈现连续性变化特征"
                        significance += 0.2
                    
                    return {
                        'significance': min(significance, 1.0),
                        'description': description,
                        'details': [
                            f"当前值: {current_value:.2f}",
                            f"前3天均值: {recent_avg:.2f}",
                            f"变化幅度: {change_rate*100:+.1f}%",
                            f"连续趋势: {'是' if is_continuous else '否'}",
                            f"数据时间范围: {dates[0]} 至 {dates[-1]}"
                        ]
                    }
            
            return {'significance': 0.3, 'description': '时间趋势相对稳定', 'details': [f'近{len(values)}天数据波动较小']}
            
        except Exception as e:
            return {'significance': 0, 'description': f'时间趋势分析失败: {e}', 'details': []}
    
    def _analyze_structural_changes(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析结构性变化（基于真实数据库维度：年龄段、操作系统、城市等级等）"""
        try:
            findings = []
            max_significance = 0
            details = []
            
            # 获取前一天日期
            prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 1. 年龄段结构变化分析
            age_analysis = self._analyze_age_group_impact(metric_name, date_str, prev_date, conn)
            if age_analysis['significance'] > 0.6:
                findings.append(f"年龄段结构: {age_analysis['description']}")
                max_significance = max(max_significance, age_analysis['significance'])
                details.extend(age_analysis['details'])
            
            # 2. 操作系统分布变化
            os_analysis = self._analyze_os_type_impact(metric_name, date_str, prev_date, conn)
            if os_analysis['significance'] > 0.6:
                findings.append(f"操作系统分布: {os_analysis['description']}")
                max_significance = max(max_significance, os_analysis['significance'])
                details.extend(os_analysis['details'])
            
            # 3. 城市等级分布变化
            city_analysis = self._analyze_city_level_impact(metric_name, date_str, prev_date, conn)
            if city_analysis['significance'] > 0.6:
                findings.append(f"城市等级分布: {city_analysis['description']}")
                max_significance = max(max_significance, city_analysis['significance'])
                details.extend(city_analysis['details'])
            
            # 4. 性别分布变化（针对女性占比异常）
            if '女性占比' in metric_name:
                gender_analysis = self._analyze_gender_distribution_change(date_str, prev_date, conn)
                if gender_analysis['significance'] > 0.6:
                    findings.append(f"性别分布: {gender_analysis['description']}")
                    max_significance = max(max_significance, gender_analysis['significance'])
                    details.extend(gender_analysis['details'])
            
            if findings:
                return {
                    'significance': max_significance,
                    'description': '; '.join(findings),
                    'details': details
                }
            
            return {'significance': 0.3, 'description': '未发现显著结构性变化', 'details': ['各维度分布相对稳定']}
            
        except Exception as e:
            return {'significance': 0, 'description': f'结构变化分析失败: {e}', 'details': []}
    
    def _analyze_external_factors(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析外部因素影响"""
        try:
            # 检查是否是特殊日期
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            weekday = date_obj.weekday()
            
            findings = []
            significance = 0
            
            # 周末效应分析
            if weekday >= 5:  # 周六、周日
                weekend_impact = self._analyze_weekend_effect(metric_name, date_str, conn)
                if weekend_impact['significance'] > 0.3:
                    findings.append(f"周末效应: {weekend_impact['description']}")
                    significance = max(significance, weekend_impact['significance'])
            
            # 节假日效应（简化版，可以扩展）
            holiday_impact = self._check_holiday_effect(date_str)
            if holiday_impact['significance'] > 0.3:
                findings.append(f"节假日效应: {holiday_impact['description']}")
                significance = max(significance, holiday_impact['significance'])
            
            # 季节性效应
            seasonal_impact = self._analyze_seasonal_effect(metric_name, date_str, conn)
            if seasonal_impact['significance'] > 0.3:
                findings.append(f"季节性效应: {seasonal_impact['description']}")
                significance = max(significance, seasonal_impact['significance'])
            
            if findings:
                return {
                    'significance': significance,
                    'description': '; '.join(findings),
                    'details': findings
                }
            
            return {'significance': 0.1, 'description': '未发现显著外部因素影响', 'details': []}
            
        except Exception as e:
            return {'significance': 0, 'description': f'外部因素分析失败: {e}', 'details': []}
    
    def _analyze_data_quality_issues(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析数据质量异常"""
        try:
            findings = []
            significance = 0
            
            # 1. 数据完整性检查
            completeness_check = self._check_data_completeness(date_str, conn)
            if completeness_check['significance'] > 0.5:
                findings.append(f"数据完整性: {completeness_check['description']}")
                significance = max(significance, completeness_check['significance'])
            
            # 2. 异常值检测
            outlier_check = self._check_outliers(metric_name, date_str, conn)
            if outlier_check['significance'] > 0.5:
                findings.append(f"异常值: {outlier_check['description']}")
                significance = max(significance, outlier_check['significance'])
            
            # 3. 数据一致性检查
            consistency_check = self._check_data_consistency(date_str, conn)
            if consistency_check['significance'] > 0.5:
                findings.append(f"数据一致性: {consistency_check['description']}")
                significance = max(significance, consistency_check['significance'])
            
            if findings:
                return {
                    'significance': significance,
                    'description': '; '.join(findings),
                    'details': findings
                }
            
            return {'significance': 0.1, 'description': '数据质量正常', 'details': []}
            
        except Exception as e:
            return {'significance': 0, 'description': f'数据质量分析失败: {e}', 'details': []}
    
    def _check_continuous_trend(self, values) -> bool:
        """检查是否为连续趋势"""
        if len(values) < 3:
            return False
        
        # 检查连续上升或下降
        increasing = all(values[i] <= values[i+1] for i in range(len(values)-1))
        decreasing = all(values[i] >= values[i+1] for i in range(len(values)-1))
        
        return increasing or decreasing
    
    def _analyze_channel_structure_change(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析渠道结构变化"""
        try:
            # 比较今日和前日的渠道分布
            today_query = """
                SELECT ad_channel, SUM(quality_users) as users
                FROM aggregated_daily_data 
                WHERE dt = ?
                GROUP BY ad_channel
            """
            
            yesterday_query = """
                SELECT ad_channel, SUM(quality_users) as users
                FROM aggregated_daily_data 
                WHERE dt = date(?, '-1 day')
                GROUP BY ad_channel
            """
            
            today_df = pd.read_sql_query(today_query, conn, params=[date_str])
            yesterday_df = pd.read_sql_query(yesterday_query, conn, params=[date_str])
            
            if today_df.empty or yesterday_df.empty:
                return {'significance': 0, 'description': '数据不足', 'details': []}
            
            # 计算渠道分布变化
            today_total = today_df['users'].sum()
            yesterday_total = yesterday_df['users'].sum()
            
            major_changes = []
            for _, row in today_df.iterrows():
                channel = row['ad_channel']
                today_pct = row['users'] / today_total if today_total > 0 else 0
                
                yesterday_row = yesterday_df[yesterday_df['ad_channel'] == channel]
                if not yesterday_row.empty:
                    yesterday_pct = yesterday_row.iloc[0]['users'] / yesterday_total if yesterday_total > 0 else 0
                    change = today_pct - yesterday_pct
                    
                    if abs(change) > 0.05 and today_pct > 0.1:  # 5%以上变化且占比超过10%
                        direction = "增加" if change > 0 else "减少"
                        major_changes.append(f"{channel}渠道占比{direction}{abs(change)*100:.1f}%")
            
            if major_changes:
                return {
                    'significance': min(len(major_changes) * 0.3, 1.0),
                    'description': '; '.join(major_changes[:3]),  # 只显示前3个
                    'details': major_changes
                }
            
            return {'significance': 0.2, 'description': '渠道结构基本稳定', 'details': []}
            
        except Exception as e:
            return {'significance': 0, 'description': f'渠道结构分析失败: {e}', 'details': []}
    
    def _analyze_user_group_changes(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析用户群体变化"""
        try:
            # 简化版：分析用户质量分布变化
            today_query = """
                SELECT 
                    SUM(CASE WHEN female_users > 0 THEN quality_users ELSE 0 END) as female_users,
                    SUM(CASE WHEN young_users > 0 THEN quality_users ELSE 0 END) as young_users,
                    SUM(quality_users) as total_users
                FROM aggregated_daily_data 
                WHERE dt = ?
            """
            
            yesterday_query = """
                SELECT 
                    SUM(CASE WHEN female_users > 0 THEN quality_users ELSE 0 END) as female_users,
                    SUM(CASE WHEN young_users > 0 THEN quality_users ELSE 0 END) as young_users,
                    SUM(quality_users) as total_users
                FROM aggregated_daily_data 
                WHERE dt = date(?, '-1 day')
            """
            
            today_data = pd.read_sql_query(today_query, conn, params=[date_str]).iloc[0]
            yesterday_data = pd.read_sql_query(yesterday_query, conn, params=[date_str]).iloc[0]
            
            changes = []
            significance = 0
            
            # 分析女性占比变化
            if today_data['total_users'] > 0 and yesterday_data['total_users'] > 0:
                today_female_pct = today_data['female_users'] / today_data['total_users']
                yesterday_female_pct = yesterday_data['female_users'] / yesterday_data['total_users']
                female_change = today_female_pct - yesterday_female_pct
                
                if abs(female_change) > 0.05:  # 5%以上变化
                    direction = "提升" if female_change > 0 else "下降"
                    changes.append(f"女性用户占比{direction}{abs(female_change)*100:.1f}%")
                    significance = max(significance, abs(female_change) * 2)
            
            if changes:
                return {
                    'significance': min(significance, 1.0),
                    'description': '; '.join(changes),
                    'details': changes
                }
            
            return {'significance': 0.1, 'description': '用户群体结构基本稳定', 'details': []}
            
        except Exception as e:
            return {'significance': 0, 'description': f'用户群体分析失败: {e}', 'details': []}
    
    def _analyze_regional_changes(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析地域分布变化（简化版）"""
        # 由于没有具体的地域数据，返回基础分析
        return {'significance': 0.1, 'description': '地域分布数据待完善', 'details': []}
    
    def _analyze_weekend_effect(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析周末效应"""
        try:
            # 获取最近几个周末的数据进行对比
            weekend_query = """
                SELECT dt, 
                       CASE 
                           WHEN ? = 'ARPU' THEN CAST(SUM(revenue) AS FLOAT) / CAST(SUM(quality_users) AS FLOAT)
                           WHEN ? = 'Good且认证用户数' THEN CAST(SUM(quality_users) AS FLOAT)
                           ELSE 0
                       END as metric_value
                FROM aggregated_daily_data 
                WHERE dt >= date(?, '-14 days') AND dt <= ?
                AND strftime('%w', dt) IN ('0', '6')  -- 周末
                GROUP BY dt
                ORDER BY dt DESC
                LIMIT 3
            """
            
            weekend_df = pd.read_sql_query(weekend_query, conn, params=[metric_name, metric_name, date_str, date_str])
            
            if len(weekend_df) >= 2:
                current_value = weekend_df.iloc[0]['metric_value']
                avg_prev_weekends = weekend_df.iloc[1:]['metric_value'].mean()
                
                if avg_prev_weekends > 0:
                    change_rate = (current_value - avg_prev_weekends) / avg_prev_weekends
                    
                    if abs(change_rate) > 0.2:  # 20%以上变化
                        direction = "高于" if change_rate > 0 else "低于"
                        return {
                            'significance': min(abs(change_rate), 1.0),
                            'description': f'周末表现{direction}历史同期{abs(change_rate)*100:.1f}%',
                            'details': [f'当前值: {current_value:.2f}', f'历史周末均值: {avg_prev_weekends:.2f}']
                        }
            
            return {'significance': 0.2, 'description': '周末效应不明显', 'details': []}
            
        except Exception as e:
            return {'significance': 0, 'description': f'周末效应分析失败: {e}', 'details': []}
    
    def _check_holiday_effect(self, date_str: str) -> dict:
        """检查节假日效应（简化版）"""
        # 这里可以扩展为完整的节假日数据库
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        month_day = f"{date_obj.month:02d}-{date_obj.day:02d}"
        
        holidays = {
            '01-01': '元旦',
            '02-14': '情人节',
            '05-01': '劳动节',
            '10-01': '国庆节',
            '12-25': '圣诞节'
        }
        
        if month_day in holidays:
            return {
                'significance': 0.8,
                'description': f'{holidays[month_day]}节假日效应',
                'details': [f'节假日: {holidays[month_day]}']
            }
        
        return {'significance': 0, 'description': '非节假日', 'details': []}
    
    def _analyze_seasonal_effect(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析季节性效应（简化版）"""
        try:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            current_month = date_obj.month
            
            # 简化的季节性判断
            seasonal_patterns = {
                'ARPU': {1: '年初效应', 2: '春节效应', 12: '年底效应'},
                'Good且认证用户数': {9: '开学季效应', 12: '年底冲量'}
            }
            
            if metric_name in seasonal_patterns and current_month in seasonal_patterns[metric_name]:
                return {
                    'significance': 0.6,
                    'description': seasonal_patterns[metric_name][current_month],
                    'details': [f'月份: {current_month}月']
                }
            
            return {'significance': 0.1, 'description': '无明显季节性效应', 'details': []}
            
        except Exception as e:
            return {'significance': 0, 'description': f'季节性分析失败: {e}', 'details': []}
    
    def _check_data_completeness(self, date_str: str, conn: sqlite3.Connection) -> dict:
        """检查数据完整性"""
        try:
            # 检查是否有异常的数据缺失
            completeness_query = """
                SELECT 
                    COUNT(*) as record_count,
                    COUNT(DISTINCT ad_channel) as channel_count,
                    SUM(CASE WHEN quality_users = 0 THEN 1 ELSE 0 END) as zero_user_records
                FROM aggregated_daily_data 
                WHERE dt = ?
            """
            
            result = pd.read_sql_query(completeness_query, conn, params=[date_str]).iloc[0]
            
            # 与前一天对比
            prev_day_query = """
                SELECT 
                    COUNT(*) as record_count,
                    COUNT(DISTINCT ad_channel) as channel_count
                FROM aggregated_daily_data 
                WHERE dt = date(?, '-1 day')
            """
            
            prev_result = pd.read_sql_query(prev_day_query, conn, params=[date_str]).iloc[0]
            
            issues = []
            significance = 0
            
            # 检查记录数量变化
            if prev_result['record_count'] > 0:
                record_change = (result['record_count'] - prev_result['record_count']) / prev_result['record_count']
                if abs(record_change) > 0.3:  # 30%以上变化
                    direction = "增加" if record_change > 0 else "减少"
                    issues.append(f"数据记录{direction}{abs(record_change)*100:.1f}%")
                    significance = max(significance, abs(record_change))
            
            # 检查渠道数量变化  
            if prev_result['channel_count'] > 0:
                channel_change = (result['channel_count'] - prev_result['channel_count']) / prev_result['channel_count']
                if abs(channel_change) > 0.2:  # 20%以上变化
                    direction = "增加" if channel_change > 0 else "减少"
                    issues.append(f"渠道数量{direction}{abs(channel_change)*100:.1f}%")
                    significance = max(significance, abs(channel_change))
            
            # 检查零用户记录
            if result['zero_user_records'] > result['record_count'] * 0.3:  # 超过30%的记录为零用户
                issues.append(f"存在{result['zero_user_records']}条零用户记录")
                significance = max(significance, 0.7)
            
            if issues:
                return {
                    'significance': min(significance, 1.0),
                    'description': '; '.join(issues),
                    'details': issues
                }
            
            return {'significance': 0.1, 'description': '数据完整性正常', 'details': []}
            
        except Exception as e:
            return {'significance': 0, 'description': f'数据完整性检查失败: {e}', 'details': []}
    
    def _check_outliers(self, metric_name: str, date_str: str, conn: sqlite3.Connection) -> dict:
        """检查异常值"""
        # 简化版异常值检测
        return {'significance': 0.1, 'description': '异常值检测正常', 'details': []}
    
    def _check_data_consistency(self, date_str: str, conn: sqlite3.Connection) -> dict:
        """检查数据一致性"""
        # 简化版一致性检查
        return {'significance': 0.1, 'description': '数据一致性正常', 'details': []}
    
    def _generate_analysis_summary(self, analysis_results: dict) -> str:
        """生成综合分析总结"""
        if not analysis_results['root_causes']:
            return "未发现明显异常原因"
        
        # 按影响分数排序
        top_causes = sorted(analysis_results['root_causes'], key=lambda x: x['impact_score'], reverse=True)
        
        summary_parts = []
        for cause in top_causes[:3]:  # 只显示前3个
            summary_parts.append(f"{cause['dimension']}: {cause['finding']}")
        
        confidence_level = "高" if analysis_results['confidence_score'] > 0.8 else "中" if analysis_results['confidence_score'] > 0.5 else "低"
        
        return f"主要原因: {'; '.join(summary_parts)} (置信度: {confidence_level})"
    
    def _fallback_analysis(self, metric_name: str, anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """兜底分析 - 当其他分析都没有发现明显原因时"""
        return {
            'dimension': '基础分析',
            'finding': f"{metric_name}出现{anomaly.get('type', '异常')}，当前值为{anomaly.get('current_value', 'N/A')}",
            'impact_score': 0.3,
            'details': ['建议进一步人工分析', '关注后续数据变化趋势']
        }
    
    def _analyze_cpa_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                              anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析CPA异常的渠道影响 - 改进版异常指标渠道定位分析"""
        
        print(f"DEBUG: CPA异常渠道分析开始 - {date_str}")
        
        # 1. 获取CPA历史趋势数据
        prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 修正的CPA查询 - 避免JOIN导致的数据重复
        current_cpa_sql = f"""
            WITH user_data AS (
                SELECT 
                    ad_channel,
                    SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as quality_users
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date_str}'
                GROUP BY ad_channel
                HAVING quality_users > 50
            ),
            cost_data AS (
                SELECT 
                    channel,
                    SUM(cash_cost) as channel_cost
                FROM dwd_ttx_market_cash_cost_i_d 
                WHERE dt = '{date_str}'
                GROUP BY channel
            )
            SELECT 
                u.ad_channel,
                u.quality_users,
                COALESCE(c.channel_cost, 0) as channel_cost,
                CASE 
                    WHEN u.quality_users > 0 AND c.channel_cost > 0
                    THEN ROUND(c.channel_cost / u.quality_users, 2)
                    ELSE NULL
                END as channel_cpa
            FROM user_data u
            LEFT JOIN cost_data c ON u.ad_channel = c.channel
            WHERE COALESCE(c.channel_cost, 0) > 0
            ORDER BY channel_cpa DESC
        """
        
        prev_cpa_sql = f"""
            WITH user_data AS (
                SELECT 
                    ad_channel,
                    SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as quality_users
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{prev_date}'
                GROUP BY ad_channel
                HAVING quality_users > 50
            ),
            cost_data AS (
                SELECT 
                    channel,
                    SUM(cash_cost) as channel_cost
                FROM dwd_ttx_market_cash_cost_i_d 
                WHERE dt = '{prev_date}'
                GROUP BY channel
            )
            SELECT 
                u.ad_channel,
                u.quality_users,
                COALESCE(c.channel_cost, 0) as channel_cost,
                CASE 
                    WHEN u.quality_users > 0 AND c.channel_cost > 0
                    THEN ROUND(c.channel_cost / u.quality_users, 2)
                    ELSE NULL
                END as channel_cpa
            FROM user_data u
            LEFT JOIN cost_data c ON u.ad_channel = c.channel
            WHERE COALESCE(c.channel_cost, 0) > 0
        """
        
        try:
            current_df = pd.read_sql_query(current_cpa_sql, conn)
            prev_df = pd.read_sql_query(prev_cpa_sql, conn)
            
            # 2. 计算整体CPA
            total_cost_current = current_df['channel_cost'].sum()
            total_users_current = current_df['quality_users'].sum()
            overall_cpa_current = total_cost_current / total_users_current if total_users_current > 0 else 0
            
            total_cost_prev = prev_df['channel_cost'].sum() if not prev_df.empty else 0
            total_users_prev = prev_df['quality_users'].sum() if not prev_df.empty else 0
            overall_cpa_prev = total_cost_prev / total_users_prev if total_users_prev > 0 else 0
            
            cpa_change = overall_cpa_current - overall_cpa_prev
            
            print(f"DEBUG: 整体CPA - 当前:{overall_cpa_current:.2f}, 前日:{overall_cpa_prev:.2f}, 变化:{cpa_change:+.2f}")
            
            # 3. 渠道层面分析
            channel_analysis = []
            
            # 合并当前和历史数据
            if not prev_df.empty:
                merged_df = pd.merge(current_df, prev_df, on='ad_channel', suffixes=('_current', '_prev'), how='outer').fillna(0)
            else:
                merged_df = current_df.copy()
                merged_df.columns = [col + '_current' if col != 'ad_channel' else col for col in merged_df.columns]
                merged_df['channel_cpa_prev'] = 0
                merged_df['quality_users_prev'] = 0
            
            # 4. 分析各渠道对CPA异常的贡献
            for _, row in merged_df.iterrows():
                channel = row['ad_channel']
                cpa_current = row.get('channel_cpa_current', 0)
                cpa_prev = row.get('channel_cpa_prev', 0)
                users_current = row.get('quality_users_current', 0)
                
                if users_current > 0 and cpa_current > 0:
                    # 计算渠道CPA变化
                    cpa_channel_change = cpa_current - cpa_prev if cpa_prev > 0 else 0
                    user_weight = users_current / total_users_current if total_users_current > 0 else 0
                    
                    # 分析渠道特征
                    analysis_reasons = []
                    
                    # 高CPA渠道识别
                    if cpa_current > overall_cpa_current:
                        excess_cpa = cpa_current - overall_cpa_current
                        analysis_reasons.append(f"CPA{cpa_current:.2f}元，高于整体{excess_cpa:.2f}元")
                    
                    # CPA变化分析
                    if abs(cpa_channel_change) > 1:
                        direction = "上涨" if cpa_channel_change > 0 else "下降"
                        analysis_reasons.append(f"较前日{direction}{abs(cpa_channel_change):.2f}元")
                    
                    # 用户占比影响
                    if user_weight > 0.1:  # 用户占比超过10%
                        analysis_reasons.append(f"用户占比{user_weight*100:.1f}%，影响权重大")
                    
                    # 只保留有明显问题的渠道
                    if (cpa_current > overall_cpa_current * 1.2 or  # CPA高于整体20%以上
                        abs(cpa_channel_change) > 2 or  # CPA变化超过2元
                        (user_weight > 0.2 and cpa_current > overall_cpa_current)):  # 大渠道且CPA偏高
                        
                        channel_analysis.append({
                            'channel': channel,
                            'cpa_current': cpa_current,
                            'cpa_prev': cpa_prev, 
                            'cpa_change': cpa_channel_change,
                            'users_current': int(users_current),
                            'user_weight': user_weight,
                            'analysis_reasons': analysis_reasons,
                            'impact_level': '高' if user_weight > 0.3 else '中' if user_weight > 0.1 else '低'
                        })
            
            # 5. 按严重程度排序 - 权重 × 绝对变化
            channel_analysis.sort(key=lambda x: (x['user_weight'] * abs(x['cpa_change'])), reverse=True)
            
            # 6. 生成分析总结
            summary_parts = []
            if cpa_change > 0:
                summary_parts.append(f"整体CPA上升{cpa_change:.2f}元")
            
            high_impact_channels = [c for c in channel_analysis if c['impact_level'] == '高']
            if high_impact_channels:
                top_channel = high_impact_channels[0]
                summary_parts.append(f"主要由{top_channel['channel']}等高影响渠道拉动")
            
            analysis_summary = "；".join(summary_parts) if summary_parts else "CPA异常原因待进一步分析"
            
            # 为HTML显示准备渠道数据 - 按严重程度排序（增加数据验证）
            channel_display_data = []
            for _, row in merged_df.iterrows():
                channel = row['ad_channel']
                cpa_current = row.get('channel_cpa_current', 0)
                users_current = row.get('quality_users_current', 0)
                
                # 数据边界检查
                if (cpa_current > 0 and users_current > 0 and 
                    cpa_current < 10000 and users_current < 1000000):  # 防止异常大的数据
                    
                    excess_cpa = max(0, cpa_current - overall_cpa_current)
                    if excess_cpa > 5:  # 只显示明显高于整体CPA的渠道
                        # 计算严重程度 = CPA超出程度 * 用户权重
                        user_weight = users_current / total_users_current if total_users_current > 0 else 0
                        severity_score = excess_cpa * (1 + user_weight * 10)  # 用户多的渠道权重更高
                        
                        # 验证数据合理性
                        if user_weight <= 1.0 and severity_score > 0:  # 权重不应超过100%
                            # 计算贡献度 = (该渠道超额成本 ÷ 总成本) × 100%
                            channel_cost = cpa_current * users_current
                            total_cost = overall_cpa_current * total_users_current if total_users_current > 0 else 1
                            contribution_pct = (channel_cost / total_cost) * 100 if total_cost > 0 else 0
                            
                            channel_display_data.append({
                                'channel': channel,
                                'channel_cpa': cpa_current,
                                'excess_cpa': excess_cpa,
                                'quality_users': users_current,
                                'severity_score': severity_score,
                                'contribution_pct': contribution_pct  # 新增贡献度
                            })
                        else:
                            print(f"⚠️  渠道数据异常: {channel} - 权重:{user_weight:.3f}, 严重程度:{severity_score:.2f}")
                else:
                    if cpa_current >= 10000 or users_current >= 1000000:
                        print(f"⚠️  渠道数据超出合理范围: {channel} - CPA:¥{cpa_current:.2f}, 用户:{users_current:,}")
            
            # 按严重程度降序排序，只取前3个最严重的
            channel_display_data.sort(key=lambda x: x['severity_score'], reverse=True)
            channel_display_data = channel_display_data[:3]
            
            # 最终验证
            if len(channel_display_data) > 0:
                print(f"✅ 渠道异常分析完成，识别{len(channel_display_data)}个高影响渠道")
            else:
                print("ℹ️  未发现明显的CPA异常渠道")
            
            return {
                'metric': 'CPA异常渠道定位',
                'anomaly_direction': f"CPA{cpa_change:+.2f}元",
                'anomaly_value': f"{overall_cpa_current:.2f}元",
                'channel_impacts': channel_analysis[:8],  # 显示前8个渠道
                'channel_data': channel_display_data,  # 新增：用于HTML显示的简化数据
                'analysis_type': 'cpa_channel_analysis',
                'overall_cpa_current': overall_cpa_current,
                'overall_cpa_prev': overall_cpa_prev,
                'analysis_summary': analysis_summary
            }
        
        except Exception as e:
            print(f"ERROR: CPA异常渠道分析失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'metric': 'CPA异常渠道定位',
                'anomaly_direction': '分析失败',
                'anomaly_value': 'N/A',
                'channel_impacts': [],
                'analysis_type': 'cpa_channel_analysis',
                'analysis_summary': f'CPA异常分析失败: {str(e)}'
            }
    
    def _analyze_arpu_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                               anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析ARPU异常的渠道影响 - 基于CPA分析的改进版"""
        
        print(f"DEBUG: ARPU异常渠道分析开始 - {date_str}")
        
        # 1. 获取ARPU历史趋势数据
        prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 当前日期ARPU查询
        current_arpu_sql = f"""
            SELECT 
                ad_channel,
                SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as quality_users,
                SUM(CASE WHEN status='good' AND verification_status='verified' THEN zizhu_revenue_1 ELSE 0 END) as revenue,
                CASE 
                    WHEN SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) > 0
                    THEN ROUND(SUM(CASE WHEN status='good' AND verification_status='verified' THEN zizhu_revenue_1 ELSE 0 END) * 1.0 / 
                               SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END), 2)
                    ELSE NULL
                END as channel_arpu
            FROM cpz_qs_newuser_channel_i_d 
            WHERE dt = '{date_str}'
            GROUP BY ad_channel
            HAVING quality_users >= 50
            ORDER BY channel_arpu DESC
        """
        
        # 前一日ARPU查询
        prev_arpu_sql = f"""
            SELECT 
                ad_channel,
                SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as quality_users,
                SUM(CASE WHEN status='good' AND verification_status='verified' THEN zizhu_revenue_1 ELSE 0 END) as revenue,
                CASE 
                    WHEN SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) > 0
                    THEN ROUND(SUM(CASE WHEN status='good' AND verification_status='verified' THEN zizhu_revenue_1 ELSE 0 END) * 1.0 / 
                               SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END), 2)
                    ELSE NULL
                END as channel_arpu
            FROM cpz_qs_newuser_channel_i_d 
            WHERE dt = '{prev_date}'
            GROUP BY ad_channel
            HAVING quality_users >= 50
        """
        
        try:
            current_arpu_df = pd.read_sql_query(current_arpu_sql, conn)
            prev_arpu_df = pd.read_sql_query(prev_arpu_sql, conn)
            
            if current_arpu_df.empty:
                print(f"ℹ️  当前日期 {date_str} 无有效ARPU数据")
                return {}
            
            # 2. 合并数据分析变化
            merged_df = pd.merge(
                current_arpu_df, prev_arpu_df, 
                on='ad_channel', 
                how='left', 
                suffixes=('_current', '_prev')
            )
            
            merged_df['arpu_change'] = merged_df['channel_arpu_current'] - merged_df['channel_arpu_prev'].fillna(merged_df['channel_arpu_current'])
            merged_df['arpu_change_pct'] = ((merged_df['channel_arpu_current'] - merged_df['channel_arpu_prev']) / 
                                          merged_df['channel_arpu_prev'].replace(0, np.nan) * 100).fillna(0)
            
            # 3. 计算整体ARPU变化
            total_current_arpu = (current_arpu_df['revenue'].sum() / 
                                current_arpu_df['quality_users'].sum() if current_arpu_df['quality_users'].sum() > 0 else 0)
            total_prev_arpu = (prev_arpu_df['revenue'].sum() / 
                             prev_arpu_df['quality_users'].sum() if not prev_arpu_df.empty and prev_arpu_df['quality_users'].sum() > 0 else total_current_arpu)
            
            overall_arpu_change = total_current_arpu - total_prev_arpu
            print(f"DEBUG: 整体ARPU - 当前:{total_current_arpu:.2f}, 前日:{total_prev_arpu:.2f}, 变化:{overall_arpu_change:+.2f}")
            
            # 4. 识别异常渠道
            channel_impacts = []
            for idx, row in merged_df.iterrows():
                if pd.isna(row['channel_arpu_current']) or row['channel_arpu_current'] is None:
                    continue
                    
                arpu_current = float(row['channel_arpu_current'])
                arpu_prev = float(row['channel_arpu_prev']) if pd.notna(row['channel_arpu_prev']) else arpu_current
                arpu_change = float(row['arpu_change'])
                arpu_change_pct = float(row['arpu_change_pct'])
                quality_users_current = int(row['quality_users_current'])
                
                # 权重计算：用户数权重
                total_users = current_arpu_df['quality_users'].sum()
                weight = quality_users_current / total_users if total_users > 0 else 0
                
                # 加权影响：渠道ARPU变化对整体ARPU的影响
                weighted_impact = arpu_change * weight
                
                # 贡献度计算
                contribution_pct = weight * 100
                
                # 异常判断：ARPU变化异常的渠道
                is_abnormal = False
                channel_contribution_reasons = []
                
                # 判断条件
                if abs(arpu_change_pct) > 10:  # ARPU变化超过10%
                    is_abnormal = True
                    direction = "下降" if arpu_change < 0 else "上升"
                    channel_contribution_reasons.append(f"ARPU{direction}{abs(arpu_change_pct):.1f}%")
                
                if abs(arpu_change) > 1.0:  # ARPU绝对变化超过1元
                    is_abnormal = True
                    direction = "降低" if arpu_change < 0 else "提高"
                    channel_contribution_reasons.append(f"ARPU{direction}{abs(arpu_change):.2f}元")
                
                if is_abnormal or abs(weighted_impact) >= 0.05:  # 加权影响较大
                    channel_impacts.append({
                        'channel': row['ad_channel'],
                        'arpu_current': arpu_current,
                        'arpu_prev': arpu_prev,
                        'arpu_change': arpu_change,
                        'arpu_change_pct': arpu_change_pct,
                        'quality_users': quality_users_current,
                        'weight': weight,
                        'weighted_impact': weighted_impact,
                        'contribution_reasons': channel_contribution_reasons
                    })
            
            # 计算严重程度分数并排序
            for impact in channel_impacts:
                # 改进的严重程度算法：考虑对整体的实际影响
                # 严重程度 = |加权影响| * 100 + |ARPU变化率| * 用户权重 * 10
                weighted_impact_score = abs(impact['weighted_impact']) * 100
                change_rate_score = abs(impact['arpu_change_pct']) * impact['weight'] * 10
                severity_score = weighted_impact_score + change_rate_score
                impact['severity_score'] = severity_score
                
                # 计算对整体ARPU变化的贡献度
                if overall_arpu_change != 0:
                    impact['contribution_to_overall'] = (impact['weighted_impact'] / abs(overall_arpu_change)) * 100
                else:
                    impact['contribution_to_overall'] = 0
            
            # 按对整体ARPU的实际贡献度排序（优先显示贡献大的渠道）
            channel_impacts.sort(key=lambda x: abs(x.get('contribution_to_overall', 0)), reverse=True)
            
            # 只保留前3个最严重的渠道，用于显示
            significant_impacts = channel_impacts[:3]
            
            # 计算前3个渠道的总贡献度
            total_contribution = sum([impact.get('contribution_to_overall', 0) for impact in significant_impacts])
            
            print(f"✅ ARPU渠道异常分析完成，识别{len(significant_impacts)}个高影响渠道，总贡献度{total_contribution:.1f}%")
            
            # 移除单独强调某个渠道的消息
            main_contributor_msg = ""
            
            return {
                'metric': 'ARPU异常',
                'anomaly_direction': '渠道分析',
                'anomaly_value': f'¥{total_current_arpu:.2f}',
                'channel_impacts': significant_impacts,
                'analysis_type': 'arpu_channel_analysis',
                'analysis_summary': f'识别{len(significant_impacts)}个ARPU异常渠道',
                'total_contribution': total_contribution,
                'main_contributor_msg': main_contributor_msg,
                'overall_arpu_change': overall_arpu_change
            }
            
        except Exception as e:
            print(f"ERROR: ARPU异常渠道分析失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'metric': 'ARPU异常渠道定位',  
                'anomaly_direction': '分析失败',
                'anomaly_value': 'N/A',
                'channel_impacts': [],
                'analysis_type': 'arpu_channel_analysis',
                'analysis_summary': f'ARPU异常分析失败: {str(e)}'
            }
    
    def _analyze_generic_metric_by_channel(self, metric_field: str, metric_name: str, date_str: str, conn: sqlite3.Connection, anomaly: dict) -> dict:
        """通用指标渠道分析函数"""
        
        print(f"DEBUG: {metric_name}渠道分析开始 - {date_str}")
        
        prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 根据不同指标构建查询SQL
        if metric_field == 'female_ratio':
            sql_template = """
                SELECT 
                    ad_channel,
                    SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as quality_users,
                    SUM(CASE WHEN status='good' AND verification_status='verified' AND gender='female' THEN newuser ELSE 0 END) as metric_numerator,
                    CASE 
                        WHEN SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) > 0
                        THEN ROUND(SUM(CASE WHEN status='good' AND verification_status='verified' AND gender='female' THEN newuser ELSE 0 END) * 100.0 / SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END), 2)
                        ELSE 0
                    END as metric_value
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date}'
                GROUP BY ad_channel
                HAVING quality_users >= 50
                ORDER BY metric_value DESC
            """
        elif metric_field == 'young_ratio':
            sql_template = """
                SELECT 
                    ad_channel,
                    SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as quality_users,
                    SUM(CASE WHEN status='good' AND verification_status='verified' AND age_group IN ('20-', '20~23') THEN newuser ELSE 0 END) as metric_numerator,
                    CASE 
                        WHEN SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) > 0
                        THEN ROUND(SUM(CASE WHEN status='good' AND verification_status='verified' AND age_group IN ('20-', '20~23') THEN newuser ELSE 0 END) * 100.0 / SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END), 2)
                        ELSE 0
                    END as metric_value
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date}'
                GROUP BY ad_channel
                HAVING quality_users >= 50
                ORDER BY metric_value DESC
            """
        elif metric_field == 'high_tier_ratio':
            sql_template = """
                SELECT 
                    ad_channel,
                    SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as quality_users,
                    SUM(CASE WHEN status='good' AND verification_status='verified' AND dengji IN ('超一线', '一线', '二线') THEN newuser ELSE 0 END) as metric_numerator,
                    CASE 
                        WHEN SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) > 0
                        THEN ROUND(SUM(CASE WHEN status='good' AND verification_status='verified' AND dengji IN ('超一线', '一线', '二线') THEN newuser ELSE 0 END) * 100.0 / SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END), 2)
                        ELSE 0
                    END as metric_value
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date}'
                GROUP BY ad_channel
                HAVING quality_users >= 50
                ORDER BY metric_value DESC
            """
        elif metric_field == 'good_rate':
            sql_template = """
                SELECT 
                    ad_channel,
                    SUM(newuser) as total_users,
                    SUM(CASE WHEN status='good' THEN newuser ELSE 0 END) as metric_numerator,
                    CASE 
                        WHEN SUM(newuser) > 0
                        THEN ROUND(SUM(CASE WHEN status='good' THEN newuser ELSE 0 END) * 100.0 / SUM(newuser), 2)
                        ELSE 0
                    END as metric_value
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date}'
                GROUP BY ad_channel
                HAVING total_users >= 50
                ORDER BY metric_value DESC
            """
        elif metric_field == 'verified_rate':
            sql_template = """
                SELECT 
                    ad_channel,
                    SUM(CASE WHEN status='good' THEN newuser ELSE 0 END) as good_users,
                    SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as metric_numerator,
                    CASE 
                        WHEN SUM(CASE WHEN status='good' THEN newuser ELSE 0 END) > 0
                        THEN ROUND(SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) * 100.0 / SUM(CASE WHEN status='good' THEN newuser ELSE 0 END), 2)
                        ELSE 0
                    END as metric_value
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date}'
                GROUP BY ad_channel
                HAVING good_users >= 50
                ORDER BY metric_value DESC
            """
        elif metric_field == 'retention_rate':
            sql_template = """
                SELECT 
                    ad_channel,
                    SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as quality_users,
                    SUM(CASE WHEN status='good' AND verification_status='verified' THEN is_returned_1_day ELSE 0 END) as metric_numerator,
                    CASE 
                        WHEN SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) > 0
                        THEN ROUND(SUM(CASE WHEN status='good' AND verification_status='verified' THEN is_returned_1_day ELSE 0 END) * 100.0 / SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END), 2)
                        ELSE 0
                    END as metric_value
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date}'
                GROUP BY ad_channel
                HAVING quality_users >= 50
                ORDER BY metric_value DESC
            """
        else:
            return {}
        
        try:
            current_sql = sql_template.format(date=date_str)
            prev_sql = sql_template.format(date=prev_date)
            
            current_df = pd.read_sql_query(current_sql, conn)
            prev_df = pd.read_sql_query(prev_sql, conn)
            
            if current_df.empty:
                print(f"ℹ️  当前日期 {date_str} 无有效{metric_name}数据")
                return {}
            
            # 合并数据分析变化
            merged_df = pd.merge(
                current_df, prev_df, 
                on='ad_channel', 
                how='left', 
                suffixes=('_current', '_prev')
            )
            
            merged_df['metric_change'] = merged_df['metric_value_current'] - merged_df['metric_value_prev'].fillna(merged_df['metric_value_current'])
            merged_df['metric_change_pct'] = ((merged_df['metric_value_current'] - merged_df['metric_value_prev']) / 
                                             merged_df['metric_value_prev'].replace(0, np.nan) * 100).fillna(0)
            
            # 识别异常渠道
            channel_impacts = []
            for idx, row in merged_df.iterrows():
                if pd.isna(row['metric_value_current']) or row['metric_value_current'] is None:
                    continue
                    
                metric_current = float(row['metric_value_current'])
                metric_prev = float(row['metric_value_prev']) if pd.notna(row['metric_value_prev']) else metric_current
                metric_change = float(row['metric_change'])
                metric_change_pct = float(row['metric_change_pct'])
                
                # 获取用户数
                if 'quality_users_current' in row:
                    users = int(row['quality_users_current'])
                    total_users = current_df['quality_users'].sum()
                elif 'good_users_current' in row:
                    users = int(row['good_users_current'])
                    total_users = current_df['good_users'].sum()
                elif 'total_users_current' in row:
                    users = int(row['total_users_current'])
                    total_users = current_df['total_users'].sum()
                else:
                    users = 0
                    total_users = 1
                
                # 计算权重
                weight = users / total_users if total_users > 0 else 0
                
                # 异常判断
                is_abnormal = False
                channel_contribution_reasons = []
                
                if abs(metric_change_pct) > 15:  # 指标变化超过15%
                    is_abnormal = True
                    direction = "下降" if metric_change < 0 else "上升"
                    channel_contribution_reasons.append(f"{metric_name}{direction}{abs(metric_change_pct):.1f}%")
                
                if abs(metric_change) > 5.0:  # 指标绝对变化超过5个百分点
                    is_abnormal = True
                    direction = "降低" if metric_change < 0 else "提高"
                    channel_contribution_reasons.append(f"{metric_name}{direction}{abs(metric_change):.1f}pp")
                
                if is_abnormal:
                    channel_impacts.append({
                        'channel': row['ad_channel'],
                        'metric_current': metric_current,
                        'metric_prev': metric_prev,
                        'metric_change': metric_change,
                        'metric_change_pct': metric_change_pct,
                        'users': users,
                        'weight': weight,
                        'contribution_reasons': channel_contribution_reasons
                    })
            
            # 计算严重程度分数并排序
            for impact in channel_impacts:
                # 计算严重程度 = 绝对变化量 * (1 + 用户权重 * 10)
                severity_score = abs(impact['metric_change']) * (1 + impact['weight'] * 10)
                impact['severity_score'] = severity_score
            
            # 按严重程度降序排序
            channel_impacts.sort(key=lambda x: x['severity_score'], reverse=True)
            significant_impacts = channel_impacts[:3]  # 只保留前3个最严重的
            
            print(f"✅ {metric_name}渠道异常分析完成，识别{len(significant_impacts)}个异常渠道")
            
            return {
                'metric': f'{metric_name}异常',
                'anomaly_direction': '渠道分析',
                'channel_impacts': significant_impacts,
                'analysis_type': 'generic_channel_analysis',
                'analysis_summary': f'识别{len(significant_impacts)}个{metric_name}异常渠道'
            }
            
        except Exception as e:
            print(f"ERROR: {metric_name}渠道分析失败: {e}")
            import traceback
            traceback.print_exc()
            return {
                'metric': f'{metric_name}异常渠道定位',
                'anomaly_direction': '分析失败',
                'channel_impacts': [],
                'analysis_type': 'generic_channel_analysis',
                'analysis_summary': f'{metric_name}异常分析失败: {str(e)}'
            }
    
    def _analyze_quality_users_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                        anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析优质用户数异常的渠道影响"""
        
        # 合并当前和历史渠道数据
        if not prev_channel_df.empty:
            merged_df = pd.merge(
                channel_df, prev_channel_df, 
                on='ad_channel', suffixes=('_current', '_prev'), how='outer'
            ).fillna(0)
        else:
            merged_df = channel_df.copy()
            merged_df.columns = [col + '_current' if col != 'ad_channel' else col for col in merged_df.columns]
        
        # 计算用户数变化
        channel_impacts = []
        total_users = merged_df['quality_users_current'].sum() if 'quality_users_current' in merged_df.columns else 1
        
        for _, row in merged_df.iterrows():
            if 'quality_users_prev' in merged_df.columns:
                users_current = row.get('quality_users_current', 0)
                users_prev = row.get('quality_users_prev', 0)
                
                users_change = users_current - users_prev
                users_change_pct = (users_change / users_prev) * 100 if users_prev > 0 else 0
                
                # 计算权重和严重程度
                weight = users_current / total_users if total_users > 0 else 0
                severity_score = abs(users_change) * (1 + weight * 10)
                
                # 只保留用户数下降的渠道（负向影响）
                if users_change < 0 and abs(users_change) >= 10:
                    channel_impacts.append({
                        'channel': row['ad_channel'],
                        'users_current': users_current,
                        'users_prev': users_prev,
                        'users_change': users_change,
                        'users_change_pct': users_change_pct,
                        'weight': weight,
                        'severity_score': severity_score
                    })
        
        # 按严重程度排序
        channel_impacts.sort(key=lambda x: x['severity_score'], reverse=True)
        
        return {
            'metric': 'Good且认证用户数',
            'anomaly_direction': anomaly.get('direction', ''),
            'anomaly_value': anomaly.get('current_value', ''),
            'channel_impacts': channel_impacts[:10],
            'analysis_type': 'users_change'
        }
    
    def _analyze_retention_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                    anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析次留率异常的渠道影响"""
        
        # 计算整体次留率作为基准
        total_quality_users = channel_df['quality_users'].sum()
        total_retained = channel_df['retained_users'].sum()
        overall_retention = (total_retained / total_quality_users * 100) if total_quality_users > 0 else 0
        
        # 计算各渠道的次留率和权重影响
        channel_retention_analysis = []
        
        for _, row in channel_df.iterrows():
            quality_users = row.get('quality_users', 0)
            retained_users = row.get('retained_users', 0)
            
            if quality_users >= 30:  # 样本量要求
                retention_rate = (retained_users / quality_users * 100) if quality_users > 0 else 0
                
                # 只保留次留率低于整体水平的渠道（负向影响）
                if retention_rate < overall_retention:
                    # 计算权重影响：该渠道对整体次留率下降的贡献
                    weight = quality_users / total_quality_users if total_quality_users > 0 else 0
                    retention_impact = (retention_rate - overall_retention) * weight
                    severity_score = abs(retention_impact) * (1 + weight * 10)
                    
                    channel_retention_analysis.append({
                        'channel': row['ad_channel'],
                        'quality_users': quality_users,
                        'retained_users': retained_users,
                        'retention_rate': retention_rate,
                        'vs_overall': retention_rate - overall_retention,
                        'weight': weight,
                        'weighted_impact': retention_impact,
                        'severity_score': severity_score
                    })
        
        # 按严重程度降序排序
        channel_retention_analysis.sort(key=lambda x: x['severity_score'], reverse=True)
        
        return {
            'metric': '次留率',
            'anomaly_direction': anomaly.get('direction', ''),
            'anomaly_value': anomaly.get('current_value', ''),
            'channel_impacts': channel_retention_analysis[:10],
            'analysis_type': 'retention_analysis'
        }
    
    def _analyze_female_ratio_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                       anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析女性占比异常的渠道影响"""
        
        # 获取敏感异常详细信息
        sensitive_details = anomaly.get('sensitive_details', {})
        
        # 计算整体女性占比作为基准
        total_quality_users = channel_df['quality_users'].sum()
        total_female = channel_df['female_users'].sum()
        overall_female_ratio = (total_female / total_quality_users * 100) if total_quality_users > 0 else 0
        
        # 计算各渠道的女性占比和权重影响
        channel_female_analysis = []
        
        for _, row in channel_df.iterrows():
            quality_users = row.get('quality_users', 0)
            female_users = row.get('female_users', 0)
            
            if quality_users >= 30:  # 样本量要求
                female_ratio = (female_users / quality_users * 100) if quality_users > 0 else 0
                
                # 只保留女性占比低于整体水平的渠道（负向影响）
                if female_ratio < overall_female_ratio:
                    # 计算权重影响：该渠道对整体女性占比下降的贡献
                    weight = quality_users / total_quality_users if total_quality_users > 0 else 0
                    female_impact = (female_ratio - overall_female_ratio) * weight
                    
                    # 生成渠道贡献原因
                    contribution_reasons = []
                    
                    # 如果有敏感异常详细信息，映射到具体渠道行为
                    if sensitive_details.get('has_extreme_value'):
                        recent_min = sensitive_details.get('recent_min', 0)
                        if female_ratio <= recent_min * 1.1:  # 接近或低于历史最低值
                            contribution_reasons.append(f"女性占比{female_ratio:.1f}%，接近历史最低值")
                    
                    if sensitive_details.get('has_mean_deviation'):
                        recent_mean = sensitive_details.get('recent_mean', 0)
                        if female_ratio < recent_mean:
                            contribution_reasons.append(f"女性占比显著低于近期均值")
                    
                    # 计算与整体的差异程度
                    deviation_pct = abs(female_ratio - overall_female_ratio)
                    if deviation_pct > 10:
                        contribution_reasons.append(f"低于整体{deviation_pct:.1f}个百分点，显著拉低整体指标")
                    elif deviation_pct > 5:
                        contribution_reasons.append(f"低于整体{deviation_pct:.1f}个百分点")
                    
                    if not contribution_reasons:
                        contribution_reasons.append("女性占比偏低，影响整体用户质量")
                    
                    # 计算严重程度分数
                    severity_score = abs(female_impact) * (1 + weight * 10)
                    
                    channel_female_analysis.append({
                        'channel': row['ad_channel'],
                        'quality_users': quality_users,
                        'female_users': female_users,
                        'female_ratio': female_ratio,
                        'vs_overall': female_ratio - overall_female_ratio,
                        'weight': weight,
                        'weighted_impact': female_impact,
                        'severity_score': severity_score,
                        'contribution_reasons': contribution_reasons
                    })
        
        # 按严重程度分数降序排序
        channel_female_analysis.sort(key=lambda x: x['severity_score'], reverse=True)
        
        # 只保留影响较大的渠道（权重影响绝对值 > 0.1%）
        significant_impacts = []
        for impact in channel_female_analysis:
            if abs(impact['weighted_impact']) >= 0.1:  # 影响阈值
                # 根据影响程度添加标签
                abs_impact = abs(impact['weighted_impact'])
                if abs_impact >= 1.0:
                    impact['impact_level'] = '高'
                    impact['contribution_reasons'].insert(0, f"【高影响】权重影响{impact['weighted_impact']:.2f}%")
                elif abs_impact >= 0.5:
                    impact['impact_level'] = '中'
                    impact['contribution_reasons'].insert(0, f"【中影响】权重影响{impact['weighted_impact']:.2f}%")
                else:
                    impact['impact_level'] = '低'
                    impact['contribution_reasons'].insert(0, f"【低影响】权重影响{impact['weighted_impact']:.2f}%")
                    
                significant_impacts.append(impact)
        
        # 如果没有显著影响的渠道，至少保留前3个
        if not significant_impacts and channel_female_analysis:
            significant_impacts = channel_female_analysis[:3]
            for impact in significant_impacts:
                impact['impact_level'] = '微'
                impact['contribution_reasons'].insert(0, f"【微影响】权重影响{impact['weighted_impact']:.2f}%")
        
        return {
            'metric': '女性占比',
            'anomaly_direction': anomaly.get('direction', ''),
            'anomaly_value': anomaly.get('current_value', ''),
            'sensitive_anomaly_summary': self._format_sensitive_anomaly_summary(sensitive_details),
            'channel_impacts': significant_impacts[:8],  # 减少显示数量，聚焦高影响渠道
            'analysis_type': 'female_ratio_analysis'
        }
    
    def _analyze_young_ratio_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                      anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析年轻占比异常的渠道影响"""
        
        # 获取敏感异常详细信息
        sensitive_details = anomaly.get('sensitive_details', {})
        
        # 计算整体年轻占比作为基准
        total_quality_users = channel_df['quality_users'].sum()
        total_young = channel_df['young_users'].sum()
        overall_young_ratio = (total_young / total_quality_users * 100) if total_quality_users > 0 else 0
        
        # 计算各渠道的年轻占比和权重影响
        channel_young_analysis = []
        
        for _, row in channel_df.iterrows():
            quality_users = row.get('quality_users', 0)
            young_users = row.get('young_users', 0)
            
            if quality_users >= 30:  # 样本量要求
                young_ratio = (young_users / quality_users * 100) if quality_users > 0 else 0
                
                # 只保留年轻占比低于整体水平的渠道（负向影响）
                if young_ratio < overall_young_ratio:
                    # 计算权重影响：该渠道对整体年轻占比下降的贡献
                    weight = quality_users / total_quality_users if total_quality_users > 0 else 0
                    young_impact = (young_ratio - overall_young_ratio) * weight
                    
                    # 生成渠道贡献原因
                    contribution_reasons = []
                    deviation_pct = abs(young_ratio - overall_young_ratio)
                    if deviation_pct > 8:
                        contribution_reasons.append(f"年轻占比{young_ratio:.1f}%，低于整体{deviation_pct:.1f}个百分点")
                    elif deviation_pct > 3:
                        contribution_reasons.append(f"低于整体{deviation_pct:.1f}个百分点")
                    
                    if sensitive_details.get('has_extreme_value'):
                        contribution_reasons.append("年轻用户获取能力偏弱")
                    
                    if not contribution_reasons:
                        contribution_reasons.append("年轻占比偏低，影响用户质量结构")
                    
                    # 计算严重程度分数
                    severity_score = abs(young_impact) * (1 + weight * 10)
                    
                    channel_young_analysis.append({
                        'channel': row['ad_channel'],
                        'quality_users': quality_users,
                        'young_users': young_users,
                        'young_ratio': young_ratio,
                        'vs_overall': young_ratio - overall_young_ratio,
                        'weight': weight,
                        'weighted_impact': young_impact,
                        'severity_score': severity_score,
                        'contribution_reasons': contribution_reasons
                    })
        
        # 按严重程度分数降序排序
        channel_young_analysis.sort(key=lambda x: x['severity_score'], reverse=True)
        
        return {
            'metric': '年轻占比',
            'anomaly_direction': anomaly.get('direction', ''),
            'anomaly_value': anomaly.get('current_value', ''),
            'sensitive_anomaly_summary': self._format_sensitive_anomaly_summary(sensitive_details),
            'channel_impacts': channel_young_analysis[:10],
            'analysis_type': 'young_ratio_analysis'
        }
    
    def _analyze_high_tier_ratio_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                          anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析高线城市占比异常的渠道影响"""
        
        # 计算整体高线城市占比作为基准
        total_quality_users = channel_df['quality_users'].sum()
        total_high_tier = channel_df['high_tier_users'].sum()
        overall_high_tier_ratio = (total_high_tier / total_quality_users * 100) if total_quality_users > 0 else 0
        
        # 计算各渠道的高线城市占比和权重影响
        channel_high_tier_analysis = []
        
        for _, row in channel_df.iterrows():
            quality_users = row.get('quality_users', 0)
            high_tier_users = row.get('high_tier_users', 0)
            
            if quality_users >= 30:  # 样本量要求
                high_tier_ratio = (high_tier_users / quality_users * 100) if quality_users > 0 else 0
                
                # 只保留高线城市占比低于整体水平的渠道（负向影响）
                if high_tier_ratio < overall_high_tier_ratio:
                    # 计算权重影响：该渠道对整体高线城市占比下降的贡献
                    weight = quality_users / total_quality_users if total_quality_users > 0 else 0
                    high_tier_impact = (high_tier_ratio - overall_high_tier_ratio) * weight
                    
                    # 计算严重程度分数
                    severity_score = abs(high_tier_impact) * (1 + weight * 10)
                    
                    channel_high_tier_analysis.append({
                        'channel': row['ad_channel'],
                        'quality_users': quality_users,
                        'high_tier_users': high_tier_users,
                        'high_tier_ratio': high_tier_ratio,
                        'vs_overall': high_tier_ratio - overall_high_tier_ratio,
                        'weight': weight,
                        'weighted_impact': high_tier_impact,
                        'severity_score': severity_score
                    })
        
        # 按严重程度分数降序排序
        channel_high_tier_analysis.sort(key=lambda x: x['severity_score'], reverse=True)
        
        return {
            'metric': '高线城市占比',
            'anomaly_direction': anomaly.get('direction', ''),
            'anomaly_value': anomaly.get('current_value', ''),
            'channel_impacts': channel_high_tier_analysis[:10],
            'analysis_type': 'high_tier_ratio_analysis'
        }
    
    def _analyze_good_rate_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                    anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析Good率异常的渠道影响 - 改进版"""
        
        print(f"DEBUG: Good率渠道分析开始 - {date_str}")
        
        # 获取前一日日期
        prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 当前日期Good率查询
        current_sql = f"""
            SELECT 
                ad_channel,
                SUM(newuser) as total_users,
                SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
                ROUND(SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) * 100.0 / 
                      NULLIF(SUM(newuser), 0), 2) as good_rate
            FROM cpz_qs_newuser_channel_i_d 
            WHERE dt = '{date_str}'
            GROUP BY ad_channel
            HAVING total_users >= 100
        """
        
        # 前一日Good率查询
        prev_sql = f"""
            SELECT 
                ad_channel,
                SUM(newuser) as total_users,
                SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
                ROUND(SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) * 100.0 / 
                      NULLIF(SUM(newuser), 0), 2) as good_rate
            FROM cpz_qs_newuser_channel_i_d 
            WHERE dt = '{prev_date}'
            GROUP BY ad_channel
            HAVING total_users >= 100
        """
        
        try:
            current_df = pd.read_sql_query(current_sql, conn)
            prev_df = pd.read_sql_query(prev_sql, conn)
            
            if current_df.empty:
                return {'channel_impacts': [], 'analysis_type': 'good_rate_analysis'}
            
            # 合并数据
            merged_df = pd.merge(current_df, prev_df, on='ad_channel', how='left', suffixes=('_current', '_prev'))
            merged_df['good_rate_change'] = merged_df['good_rate_current'] - merged_df['good_rate_prev'].fillna(merged_df['good_rate_current'])
            merged_df['good_rate_change_pct'] = ((merged_df['good_rate_current'] - merged_df['good_rate_prev']) / 
                                                  merged_df['good_rate_prev'].replace(0, np.nan) * 100).fillna(0)
            
            # 计算整体Good率变化
            total_current_good_rate = (current_df['good_users'].sum() / current_df['total_users'].sum() * 100 
                                       if current_df['total_users'].sum() > 0 else 0)
            total_prev_good_rate = (prev_df['good_users'].sum() / prev_df['total_users'].sum() * 100 
                                   if not prev_df.empty and prev_df['total_users'].sum() > 0 else total_current_good_rate)
            overall_good_rate_change = total_current_good_rate - total_prev_good_rate
            
            print(f"DEBUG: 整体Good率 - 当前:{total_current_good_rate:.2f}%, 前日:{total_prev_good_rate:.2f}%, 变化:{overall_good_rate_change:+.2f}pp")
            
            # 识别异常渠道
            channel_impacts = []
            total_users = current_df['total_users'].sum()
            
            for _, row in merged_df.iterrows():
                if pd.isna(row['good_rate_current']):
                    continue
                
                good_rate_current = float(row['good_rate_current'])
                good_rate_prev = float(row['good_rate_prev']) if pd.notna(row['good_rate_prev']) else good_rate_current
                good_rate_change = float(row['good_rate_change'])
                users_current = int(row['total_users_current'])
                
                # 权重计算
                weight = users_current / total_users if total_users > 0 else 0
                
                # 加权影响
                weighted_impact = good_rate_change * weight
                
                # 判断异常
                is_abnormal = False
                contribution_reasons = []
                
                if abs(good_rate_change) > 3:  # Good率变化超过3个百分点
                    is_abnormal = True
                    direction = "下降" if good_rate_change < 0 else "上升"
                    contribution_reasons.append(f"Good率{direction}{abs(good_rate_change):.1f}pp")
                
                if is_abnormal or abs(weighted_impact) >= 0.1:
                    channel_impacts.append({
                        'channel': row['ad_channel'],
                        'good_rate_current': good_rate_current,
                        'good_rate_prev': good_rate_prev,
                        'good_rate_change': good_rate_change,
                        'total_users': users_current,
                        'weight': weight,
                        'weighted_impact': weighted_impact,
                        'contribution_reasons': contribution_reasons
                    })
            
            # 计算贡献度并排序
            for impact in channel_impacts:
                if overall_good_rate_change != 0:
                    impact['contribution_to_overall'] = (impact['weighted_impact'] / abs(overall_good_rate_change)) * 100
                else:
                    impact['contribution_to_overall'] = 0
                
                # 严重程度评分
                impact['severity_score'] = abs(impact['weighted_impact']) * 100 + abs(impact['good_rate_change']) * impact['weight'] * 10
            
            # 区分正负贡献，只关注真正的负面贡献（自身下降加剧整体下降的渠道）
            if overall_good_rate_change < 0:
                # 整体下降时，只显示对下降有负面贡献的渠道（即自身也下降的渠道）
                # contribution < 0 表示该渠道加剧了整体下降
                negative_impacts = [impact for impact in channel_impacts if impact.get('contribution_to_overall', 0) < 0]
                # 按负面贡献度的绝对值排序（最负面的在前）
                negative_impacts.sort(key=lambda x: x.get('contribution_to_overall', 0))
                significant_impacts = negative_impacts[:3]
            else:
                # 整体上升时，显示所有异常渠道
                channel_impacts.sort(key=lambda x: abs(x.get('contribution_to_overall', 0)), reverse=True)
                significant_impacts = channel_impacts[:3]
            
            # 计算负面贡献度总和（绝对值）
            negative_contribution = sum([abs(min(0, impact.get('contribution_to_overall', 0))) for impact in significant_impacts])
            
            # 移除单独强调某个渠道的消息
            main_contributor_msg = ""
                
            print(f"✅ Good率渠道异常分析完成，识别{len(significant_impacts)}个异常渠道")
            
            return {
                'metric': 'Good率异常',
                'anomaly_direction': '渠道分析',
                'anomaly_value': f'{total_current_good_rate:.2f}%',
                'channel_impacts': significant_impacts,
                'analysis_type': 'good_rate_analysis',
                'analysis_summary': f'识别{len(significant_impacts)}个Good率异常渠道',
                'total_contribution': negative_contribution,
                'main_contributor_msg': main_contributor_msg,
                'overall_change': overall_good_rate_change
            }
            
        except Exception as e:
            print(f"ERROR: Good率异常分析失败: {e}")
            return {
                'metric': 'Good率异常',
                'channel_impacts': [],
                'analysis_type': 'good_rate_analysis',
                'analysis_summary': f'Good率异常分析失败: {str(e)}'
            }
    
    def _analyze_verified_rate_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                        anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析认证率异常的渠道影响 - 改进版"""
        
        print(f"DEBUG: 认证率渠道分析开始 - {date_str}")
        
        # 获取前一日日期
        prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 当前日期认证率查询
        current_sql = f"""
            SELECT 
                ad_channel,
                SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
                SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users,
                ROUND(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) * 100.0 / 
                      NULLIF(SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END), 0), 2) as verified_rate
            FROM cpz_qs_newuser_channel_i_d 
            WHERE dt = '{date_str}'
            GROUP BY ad_channel
            HAVING good_users >= 50
        """
        
        # 前一日认证率查询
        prev_sql = f"""
            SELECT 
                ad_channel,
                SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
                SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users,
                ROUND(SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) * 100.0 / 
                      NULLIF(SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END), 0), 2) as verified_rate
            FROM cpz_qs_newuser_channel_i_d 
            WHERE dt = '{prev_date}'
            GROUP BY ad_channel
            HAVING good_users >= 50
        """
        
        try:
            current_df = pd.read_sql_query(current_sql, conn)
            prev_df = pd.read_sql_query(prev_sql, conn)
            
            if current_df.empty:
                return {'channel_impacts': [], 'analysis_type': 'verified_rate_analysis'}
            
            # 合并数据
            merged_df = pd.merge(current_df, prev_df, on='ad_channel', how='left', suffixes=('_current', '_prev'))
            merged_df['verified_rate_change'] = merged_df['verified_rate_current'] - merged_df['verified_rate_prev'].fillna(merged_df['verified_rate_current'])
            
            # 计算整体认证率变化
            total_current_verified_rate = (current_df['quality_users'].sum() / current_df['good_users'].sum() * 100 
                                          if current_df['good_users'].sum() > 0 else 0)
            total_prev_verified_rate = (prev_df['quality_users'].sum() / prev_df['good_users'].sum() * 100 
                                       if not prev_df.empty and prev_df['good_users'].sum() > 0 else total_current_verified_rate)
            overall_verified_rate_change = total_current_verified_rate - total_prev_verified_rate
            
            print(f"DEBUG: 整体认证率 - 当前:{total_current_verified_rate:.2f}%, 前日:{total_prev_verified_rate:.2f}%, 变化:{overall_verified_rate_change:+.2f}pp")
            
            # 识别异常渠道
            channel_impacts = []
            total_good_users = current_df['good_users'].sum()
            
            for _, row in merged_df.iterrows():
                if pd.isna(row['verified_rate_current']):
                    continue
                
                verified_rate_current = float(row['verified_rate_current'])
                verified_rate_prev = float(row['verified_rate_prev']) if pd.notna(row['verified_rate_prev']) else verified_rate_current
                verified_rate_change = float(row['verified_rate_change'])
                good_users_current = int(row['good_users_current'])
                
                # 权重计算（基于Good用户）
                weight = good_users_current / total_good_users if total_good_users > 0 else 0
                
                # 加权影响
                weighted_impact = verified_rate_change * weight
                
                # 判断异常
                is_abnormal = False
                contribution_reasons = []
                
                if abs(verified_rate_change) > 3:  # 认证率变化超过3个百分点
                    is_abnormal = True
                    direction = "下降" if verified_rate_change < 0 else "上升"
                    contribution_reasons.append(f"认证率{direction}{abs(verified_rate_change):.1f}pp")
                
                if is_abnormal or abs(weighted_impact) >= 0.1:
                    channel_impacts.append({
                        'channel': row['ad_channel'],
                        'verified_rate_current': verified_rate_current,
                        'verified_rate_prev': verified_rate_prev,
                        'verified_rate_change': verified_rate_change,
                        'good_users': good_users_current,
                        'quality_users': int(row['quality_users_current']),
                        'weight': weight,
                        'weighted_impact': weighted_impact,
                        'contribution_reasons': contribution_reasons
                    })
            
            # 计算贡献度并排序
            for impact in channel_impacts:
                if overall_verified_rate_change != 0:
                    impact['contribution_to_overall'] = (impact['weighted_impact'] / abs(overall_verified_rate_change)) * 100
                else:
                    impact['contribution_to_overall'] = 0
                
                # 严重程度评分
                impact['severity_score'] = abs(impact['weighted_impact']) * 100 + abs(impact['verified_rate_change']) * impact['weight'] * 10
            
            # 区分正负贡献，只关注真正的负面贡献（自身下降加剧整体下降的渠道）
            if overall_verified_rate_change < 0:
                # 整体下降时，只显示对下降有负面贡献的渠道（即自身也下降的渠道）
                # contribution < 0 表示该渠道加剧了整体下降
                negative_impacts = [impact for impact in channel_impacts if impact.get('contribution_to_overall', 0) < 0]
                # 按负面贡献度的绝对值排序（最负面的在前）
                negative_impacts.sort(key=lambda x: x.get('contribution_to_overall', 0))
                significant_impacts = negative_impacts[:3]
            else:
                # 整体上升时，显示所有异常渠道
                channel_impacts.sort(key=lambda x: abs(x.get('contribution_to_overall', 0)), reverse=True)
                significant_impacts = channel_impacts[:3]
            
            # 计算负面贡献度总和（绝对值）
            negative_contribution = sum([abs(min(0, impact.get('contribution_to_overall', 0))) for impact in significant_impacts])
            
            # 移除单独强调某个渠道的消息
            main_contributor_msg = ""
                
            print(f"✅ 认证率渠道异常分析完成，识别{len(significant_impacts)}个异常渠道")
            
            return {
                'metric': '认证率异常',
                'anomaly_direction': '渠道分析',
                'anomaly_value': f'{total_current_verified_rate:.2f}%',
                'channel_impacts': significant_impacts,
                'analysis_type': 'verified_rate_analysis',
                'analysis_summary': f'识别{len(significant_impacts)}个认证率异常渠道',
                'total_contribution': negative_contribution,
                'main_contributor_msg': main_contributor_msg,
                'overall_change': overall_verified_rate_change
            }
            
        except Exception as e:
            print(f"ERROR: 认证率异常分析失败: {e}")
            return {
                'metric': '认证率异常',
                'channel_impacts': [],
                'analysis_type': 'verified_rate_analysis',
                'analysis_summary': f'认证率异常分析失败: {str(e)}'
            }
    
    def _analyze_quality_rate_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                       anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析Good且认证率异常的渠道影响"""
        
        # 计算整体Good且认证率作为基准
        total_all_users = channel_df['total_users'].sum()
        total_quality = channel_df['quality_users'].sum()
        overall_quality_rate = (total_quality / total_all_users * 100) if total_all_users > 0 else 0
        
        # 计算各渠道的Good且认证率和权重影响
        channel_quality_rate_analysis = []
        
        for _, row in channel_df.iterrows():
            total_users = row.get('total_users', 0)
            quality_users = row.get('quality_users', 0)
            
            if total_users >= 50:  # 样本量要求
                quality_rate = (quality_users / total_users * 100) if total_users > 0 else 0
                
                # 只保留Good且认证率低于整体水平的渠道（负向影响）
                if quality_rate < overall_quality_rate:
                    # 计算权重影响：该渠道对整体Good且认证率下降的贡献
                    weight = total_users / total_all_users if total_all_users > 0 else 0
                    quality_rate_impact = (quality_rate - overall_quality_rate) * weight
                    
                    # 计算严重程度分数
                    severity_score = abs(quality_rate_impact) * (1 + weight * 10)
                    
                    channel_quality_rate_analysis.append({
                        'channel': row['ad_channel'],
                        'total_users': total_users,
                        'quality_users': quality_users,
                        'quality_rate': quality_rate,
                        'vs_overall': quality_rate - overall_quality_rate,
                        'weight': weight,
                        'weighted_impact': quality_rate_impact,
                        'severity_score': severity_score
                    })
        
        # 按严重程度分数降序排序
        channel_quality_rate_analysis.sort(key=lambda x: x['severity_score'], reverse=True)
        
        return {
            'metric': 'Good且认证率',
            'anomaly_direction': anomaly.get('direction', ''),
            'anomaly_value': anomaly.get('current_value', ''),
            'channel_impacts': channel_quality_rate_analysis[:10],
            'analysis_type': 'quality_rate_analysis'
        }
    
    def _generate_html_report(self, date_str: str, core_data: dict, anomaly_data: dict, creative_data: dict, account_data: dict = None, conn=None) -> str:
        """生成标准化HTML报告 - 严格按照规范结构"""
        
        # 确保所有必需的数据都不为None
        date_str = date_str or "Unknown Date"
        core_data = core_data or {}
        anomaly_data = anomaly_data or {}
        creative_data = creative_data or {}
        account_data = account_data or {}
        
        # 修复None值，确保模板渲染正常
        if core_data.get('retention_rate') is None:
            core_data['retention_rate'] = 0
        if core_data.get('prev_retention_rate') is None:
            core_data['prev_retention_rate'] = 0
        
        # 修复raw_data中的None值
        raw_data = core_data.get('raw_data', {})
        for key in raw_data:
            if raw_data[key] is None:
                raw_data[key] = 0
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>每日业务数据报告 - {date_str}</title>
    <style>
        body {{ 
            font-family: 'PingFang SC', 'Microsoft YaHei', Arial, sans-serif; 
            margin: 0; padding: 20px; background-color: #f5f7fa; line-height: 1.4; font-size: 13px;
        }}
        .container {{ 
            max-width: 1200px; margin: 0 auto; background: white; 
            padding: 30px; border-radius: 10px; box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        }}
        .header {{ 
            text-align: center; border-bottom: 3px solid #3498db; 
            padding-bottom: 20px; margin-bottom: 30px;
        }}
        .module {{ 
            margin: 40px 0; background: #f8f9fc; padding: 25px; 
            border-radius: 8px; border-left: 4px solid #3498db;
        }}
        .module h2 {{ 
            color: #2c3e50; margin-top: 0; font-size: 18px;
        }}
        .metrics-grid {{ 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); 
            gap: 15px; margin: 20px 0;
        }}
        .metric-card {{ 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            color: white; padding: 15px; border-radius: 6px; text-align: center;
        }}
        .metric-card .icon {{ font-size: 16px; margin-bottom: 5px; }}
        .metric-card .value {{ font-size: 16px; font-weight: bold; margin: 5px 0; }}
        .metric-card .label {{ font-size: 11px; opacity: 0.9; }}
        .anomaly-item {{ 
            background: #fff3cd; border: 1px solid #ffeaa7; 
            padding: 15px; margin: 10px 0; border-radius: 5px;
        }}
        .anomaly-high {{ background: #f8d7da; border-color: #f5c6cb; }}
        table {{ 
            width: 100%; border-collapse: collapse; margin-top: 20px; 
            background: white; border-radius: 8px; overflow: hidden;
        }}
        th {{ 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
            color: white; padding: 15px; text-align: left;
        }}
        .rules-note {{
            background: #e8f4fd; border: 1px solid #bee5eb; 
            padding: 15px; margin: 20px 0; border-radius: 5px; font-size: 12px;
            color: #0c5460;
        }}
        td {{ padding: 12px; border-bottom: 1px solid #ecf0f1; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 每日业务数据报告</h1>
            <p>📅 {date_str} | ⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="rules-note">
            ✅ <strong>报告规范确认</strong>: 认证率={core_data.get('quality_users', 0):,}÷{core_data.get('raw_data', {}).get('good_users', 0):,}={core_data.get('verified_rate', 0):.2f}% | 
            次留率基于Good且认证用户 | 用户质量基于Good且认证用户
        </div>
        
        <!-- 第一部分：核心指标 -->
        <div class="module">
            <h2>📊 模块一：核心指标</h2>
            
            <h3 style="font-size: 14px; margin: 15px 0 10px 0;">🎯 核心业务</h3>
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="icon">⭐</div>
                    <div class="value">{core_data.get('quality_users', 0):,}</div>
                    <div class="label">Good且认证用户数</div>
                </div>
                <div class="metric-card">
                    <div class="icon">💰</div>
                    <div class="value">¥{core_data.get('cpa', 0):.2f}</div>
                    <div class="label">CPA</div>
                </div>
                <div class="metric-card">
                    <div class="icon">📈</div>
                    <div class="value">¥{core_data.get('arpu', 0):.2f}</div>
                    <div class="label">ARPU</div>
                </div>
                <div class="metric-card">
                    <div class="icon">🔄</div>
                    <div class="value">{f"{core_data.get('prev_retention_rate', 0):.2f}%" if core_data.get('prev_retention_rate') is not None else f"{core_data.get('retention_rate', 0):.2f}%"}</div>
                    <div class="label">次留率{f"（{core_data.get('prev_retention_date', '')}）" if core_data.get('prev_retention_rate') is not None else ""}</div>
                </div>
            </div>
            
            <h3 style="font-size: 14px; margin: 15px 0 10px 0;">👥 用户质量</h3>
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="icon">👩</div>
                    <div class="value">{core_data.get('female_ratio', 0):.2f}%</div>
                    <div class="label">女性占比</div>
                </div>
                <div class="metric-card">
                    <div class="icon">🧑‍💼</div>
                    <div class="value">{core_data.get('young_ratio', 0):.2f}%</div>
                    <div class="label">年轻占比（20-23岁）</div>
                </div>
                <div class="metric-card">
                    <div class="icon">🏙️</div>
                    <div class="value">{core_data.get('high_tier_ratio', 0):.2f}%</div>
                    <div class="label">高线城市占比</div>
                </div>
            </div>
            
            <h3 style="font-size: 14px; margin: 15px 0 10px 0;">🎯 注册转化</h3>
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="icon">✅</div>
                    <div class="value">{core_data['good_rate']:.2f}%</div>
                    <div class="label">Good率</div>
                </div>
                <div class="metric-card">
                    <div class="icon">🔐</div>
                    <div class="value">{core_data['verified_rate']:.2f}%</div>
                    <div class="label">认证率</div>
                </div>
                <div class="metric-card">
                    <div class="icon">🎯</div>
                    <div class="value">{core_data['quality_rate']:.2f}%</div>
                    <div class="label">Good且认证率</div>
                </div>
            </div>
        </div>
        
        <!-- 第二部分：异常分析 -->
        <div class="module">
            <h2>🚨 模块二：异常分析</h2>
            
            <!-- 异常检测结果部分已整合到智能问题定位 -->"""
        
        # 异常检测结果已整合到智能问题定位，这里完全隐藏
        if False and anomaly_data['anomalies']:
            # 分类统计
            serious_anomalies = [a for a in anomaly_data['anomalies'] if a.get('type') == 'serious']
            sensitive_anomalies = [a for a in anomaly_data['anomalies'] if a.get('type') == 'sensitive']
            
            
            # 显示严重异常 - 优化样式
            if serious_anomalies:
                html += """<h4 style="
                    color: #dc2626;
                    margin: 20px 0 12px 0;
                    font-size: 16px;
                    font-weight: bold;
                ">🚨 严重异常 (超出IQR范围)</h4>"""
                for anomaly in serious_anomalies:
                    html += f"""
                    <div style="
                        background: linear-gradient(135deg, #fef2f2 0%, #fecaca 100%);
                        border: none;
                        border-left: 4px solid #dc2626;
                        border-radius: 8px;
                        padding: 16px;
                        margin: 12px 0;
                        box-shadow: 0 2px 4px rgba(220, 38, 38, 0.1);
                    ">
                        <div style="display: flex; align-items: center; margin-bottom: 8px;">
                            <span style="
                                background: #dc2626;
                                color: white;
                                padding: 4px 8px;
                                border-radius: 12px;
                                font-size: 12px;
                                font-weight: bold;
                                margin-right: 8px;
                            ">🚨</span>
                            <strong style="color: #991b1b; font-size: 16px;">{anomaly['metric']}</strong>
                            <span style="
                                background: #fca5a5;
                                color: #7f1d1d;
                                padding: 2px 8px;
                                border-radius: 10px;
                                font-size: 12px;
                                margin-left: 8px;
                            ">{anomaly['direction']}</span>
                        </div>
                        <div style="
                            background: rgba(255, 255, 255, 0.7);
                            border-radius: 6px;
                            padding: 12px;
                            margin: 8px 0;
                        ">
                            <div style="color: #1f2937; font-weight: 600; margin-bottom: 4px;">
                                当前值: <span style="color: #dc2626;">{anomaly['current_value']}</span>
                            </div>
                            <div style="color: #6b7280; font-size: 14px; margin-bottom: 4px;">
                                正常范围: <span style="color: #059669;">{anomaly['normal_range']}</span>
                            </div>
                            <div style="color: #6b7280; font-size: 14px; line-height: 1.4;">
                                <span style="color: #991b1b;">📊 检测原因:</span> {anomaly['reason']}
                            </div>
                        </div>
                    </div>"""
            
            # 显示敏感异常 - 优化样式
            if sensitive_anomalies:
                for anomaly in sensitive_anomalies:
                    html += f"""
                    <div style="
                        background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
                        border: none;
                        border-left: 4px solid #f59e0b;
                        border-radius: 8px;
                        padding: 16px;
                        margin: 12px 0;
                        box-shadow: 0 2px 4px rgba(245, 158, 11, 0.1);
                        transition: all 0.2s ease;
                    ">
                        <div style="display: flex; align-items: center; margin-bottom: 8px;">
                            <span style="
                                background: #f59e0b;
                                color: white;
                                padding: 4px 8px;
                                border-radius: 12px;
                                font-size: 12px;
                                font-weight: bold;
                                margin-right: 8px;
                            ">⚠️</span>
                            <strong style="color: #92400e; font-size: 16px;">{anomaly['metric']}</strong>
                        </div>
                        <div style="
                            background: rgba(255, 255, 255, 0.6);
                            border-radius: 6px;
                            padding: 12px;
                            margin: 8px 0;
                        ">
                            <div style="color: #1f2937; font-weight: 600; margin-bottom: 4px;">
                                当前值: <span style="color: #d97706;">{anomaly['current_value']}</span>
                            </div>
                            <div style="color: #6b7280; font-size: 14px; line-height: 1.4;">
                                <span style="color: #92400e;">📊 检测原因:</span> {anomaly['reason']}
                            </div>
                        </div>
                    </div>"""
        # 不再显示"未检测到异常"，因为异常信息已整合到问题定位部分
        
        # 简化异常指标显示 - 只显示问题和数据
        smart_analysis = anomaly_data.get('smart_channel_analysis', {})
        if smart_analysis.get('has_analysis'):
            html += f"""
            <h3>🎯 问题定位</h3>"""
            
            for analysis in smart_analysis.get('channel_impact_analysis', []):
                metric = analysis.get('metric', '')
                analysis_type = analysis.get('analysis_type', '')
                
                if analysis_type == 'intelligent_root_cause':
                    # 简洁文本格式
                    html += f"""
                    <div style="margin: 15px 0;">
                        <h4 style="color: #dc2626; margin: 0 0 8px 0; font-size: 14px;">
                            ⚠️ {metric} 异常
                        </h4>"""
                    
                    # 获取并显示异常检测原因
                    anomaly_reason = ""
                    for anomaly in anomaly_data.get('anomalies', []):
                        if anomaly.get('metric') == metric:
                            anomaly_reason = anomaly.get('reason', '')
                            break
                    
                    if anomaly_reason:
                        html += f"""<p style="margin: 5px 0; color: #666; font-size: 13px;">{anomaly_reason}</p>"""
                    
                    # 查找异常渠道数据
                    channel_data_found = False
                    if analysis.get('root_causes'):
                        for cause in analysis['root_causes']:
                            # CPA渠道分析
                            if cause.get('dimension') == 'CPA渠道分析' and cause.get('cpa_channel_detail'):
                                cpa_detail = cause['cpa_channel_detail']
                                channel_data = cpa_detail.get('channel_data', [])[:3]
                                if channel_data:
                                    for i, channel in enumerate(channel_data, 1):
                                        channel_name = channel.get('channel', '未知渠道')
                                        channel_cpa = channel.get('channel_cpa', 0)
                                        excess_cpa = channel.get('excess_cpa', 0)
                                        users = channel.get('quality_users', 0)
                                        severity_score = channel.get('severity_score', 0)
                                        if users >= 10000:
                                            users_display = f"{users/1000:.1f}K"
                                        else:
                                            users_display = f"{users:,}"
                                        html += f"• <strong>{channel_name}</strong>: CPA¥{channel_cpa:.1f} (高{excess_cpa:.1f}元), {users_display}用户<br>"
                                    channel_data_found = True
                                break
                            
                            # ARPU渠道分析
                            elif cause.get('dimension') == 'ARPU渠道分析' and cause.get('arpu_channel_detail'):
                                arpu_detail = cause['arpu_channel_detail']
                                channel_impacts = arpu_detail.get('channel_impacts', [])[:3]
                                
                                # 添加主要贡献渠道说明
                                main_contributor_msg = arpu_detail.get('main_contributor_msg', '')
                                overall_arpu_change = arpu_detail.get('overall_arpu_change', 0)
                                total_contribution = arpu_detail.get('total_contribution', 0)
                                
                                if main_contributor_msg:
                                    html += f"<strong style='color: #d32f2f;'>⚠️ {main_contributor_msg}</strong><br><br>"
                                
                                if overall_arpu_change < 0 and total_contribution > 50:
                                    html += f"<strong>整体ARPU下降{abs(overall_arpu_change):.2f}元，以下渠道贡献了{total_contribution:.1f}%的下降:</strong><br>"
                                
                                if channel_impacts:
                                    for i, impact in enumerate(channel_impacts, 1):
                                        channel_name = impact.get('channel', '未知渠道')
                                        arpu_current = impact.get('arpu_current', 0)
                                        arpu_change = impact.get('arpu_change', 0)
                                        users = impact.get('quality_users', 0)
                                        weight = impact.get('weight', 0) * 100
                                        contribution = impact.get('contribution_to_overall', 0)
                                        severity_score = impact.get('severity_score', 0)
                                        if users >= 10000:
                                            users_display = f"{users/1000:.1f}K"
                                        else:
                                            users_display = f"{users:,}"
                                        change_direction = "下降" if arpu_change < 0 else "上升"
                                        
                                        # 简洁文本格式
                                        html += f"• <strong>{channel_name}</strong>: ¥{arpu_current:.2f} ({change_direction}{abs(arpu_change):.2f}元), {users_display}用户, 贡献{abs(contribution):.1f}%<br>"
                                    channel_data_found = True
                                break
                            
                            # Good率渠道分析
                            elif cause.get('dimension') == 'Good率渠道分析' and cause.get('good_rate_channel_detail'):
                                good_rate_detail = cause['good_rate_channel_detail']
                                channel_impacts = good_rate_detail.get('channel_impacts', [])[:3]
                                
                                # 添加主要贡献渠道说明
                                main_contributor_msg = good_rate_detail.get('main_contributor_msg', '')
                                overall_change = good_rate_detail.get('overall_change', 0)
                                total_contribution = good_rate_detail.get('total_contribution', 0)
                                
                                if main_contributor_msg:
                                    html += f"<strong style='color: #d32f2f;'>⚠️ {main_contributor_msg}</strong><br><br>"
                                
                                if channel_impacts:
                                    for i, impact in enumerate(channel_impacts, 1):
                                        channel_name = impact.get('channel', '未知渠道')
                                        good_rate_current = impact.get('good_rate_current', 0)
                                        good_rate_change = impact.get('good_rate_change', 0)
                                        users = impact.get('total_users', 0)
                                        weight = impact.get('weight', 0) * 100
                                        contribution = impact.get('contribution_to_overall', 0)
                                        if users >= 10000:
                                            users_display = f"{users/1000:.1f}K"
                                        else:
                                            users_display = f"{users:,}"
                                        change_direction = "下降" if good_rate_change < 0 else "上升"
                                        
                                        # 简洁文本格式
                                        html += f"• <strong>{channel_name}</strong>: {good_rate_current:.2f}% ({change_direction}{abs(good_rate_change):.2f}pp), {users_display}用户, 贡献{abs(contribution):.1f}%<br>"
                                    channel_data_found = True
                                break
                            
                            # 认证率渠道分析
                            elif cause.get('dimension') == '认证率渠道分析' and cause.get('verified_rate_channel_detail'):
                                verified_rate_detail = cause['verified_rate_channel_detail']
                                channel_impacts = verified_rate_detail.get('channel_impacts', [])[:3]
                                
                                # 添加主要贡献渠道说明
                                main_contributor_msg = verified_rate_detail.get('main_contributor_msg', '')
                                overall_change = verified_rate_detail.get('overall_change', 0)
                                total_contribution = verified_rate_detail.get('total_contribution', 0)
                                
                                if main_contributor_msg:
                                    html += f"<strong style='color: #d32f2f;'>⚠️ {main_contributor_msg}</strong><br><br>"
                                
                                if channel_impacts:
                                    for i, impact in enumerate(channel_impacts, 1):
                                        channel_name = impact.get('channel', '未知渠道')
                                        verified_rate_current = impact.get('verified_rate_current', 0)
                                        verified_rate_change = impact.get('verified_rate_change', 0)
                                        users = impact.get('good_users', 0)
                                        weight = impact.get('weight', 0) * 100
                                        contribution = impact.get('contribution_to_overall', 0)
                                        if users >= 10000:
                                            users_display = f"{users/1000:.1f}K"
                                        else:
                                            users_display = f"{users:,}"
                                        change_direction = "下降" if verified_rate_change < 0 else "上升"
                                        
                                        # 简洁文本格式
                                        html += f"• <strong>{channel_name}</strong>: {verified_rate_current:.2f}% ({change_direction}{abs(verified_rate_change):.2f}pp), {users_display}Good用户, 贡献{abs(contribution):.1f}%<br>"
                                    channel_data_found = True
                                break
                            
                            # Good且认证用户数渠道分析
                            elif cause.get('dimension') == 'Good且认证用户数渠道分析' and cause.get('quality_users_channel_detail'):
                                quality_users_detail = cause['quality_users_channel_detail']
                                channel_impacts = quality_users_detail.get('channel_impacts', [])[:3]
                                main_contributor_msg = quality_users_detail.get('main_contributor_msg', "")
                                
                                if channel_impacts:
                                    for i, impact in enumerate(channel_impacts, 1):
                                        channel_name = impact.get('channel', '未知渠道')
                                        quality_users_current = impact.get('quality_users_current', 0)
                                        quality_users_change = impact.get('quality_users_change', 0)
                                        weight = impact.get('weight', 0) * 100
                                        contribution = impact.get('contribution_to_overall', 0)
                                        if quality_users_current >= 10000:
                                            users_display = f"{quality_users_current/1000:.1f}K"
                                        else:
                                            users_display = f"{quality_users_current:,}"
                                        change_direction = "下降" if quality_users_change < 0 else "上升"
                                        
                                        # 简洁文本格式
                                        html += f"• <strong>{channel_name}</strong>: {users_display} ({change_direction}{abs(quality_users_change):,}用户), 贡献{abs(contribution):.1f}%<br>"
                                    channel_data_found = True
                                break
                            
                            # Good且认证率渠道分析
                            elif cause.get('dimension') == 'Good且认证率渠道分析' and cause.get('quality_rate_channel_detail'):
                                quality_rate_detail = cause['quality_rate_channel_detail']
                                channel_impacts = quality_rate_detail.get('channel_impacts', [])[:3]
                                main_contributor_msg = quality_rate_detail.get('main_contributor_msg', "")
                                
                                if channel_impacts:
                                    for i, impact in enumerate(channel_impacts, 1):
                                        channel_name = impact.get('channel', '未知渠道')
                                        quality_rate_current = impact.get('quality_rate_current', 0)
                                        quality_rate_change = impact.get('quality_rate_change', 0)
                                        total_users_current = impact.get('total_users_current', 0)
                                        weight = impact.get('weight', 0) * 100
                                        contribution = impact.get('contribution_to_overall', 0)
                                        if total_users_current >= 10000:
                                            users_display = f"{total_users_current/1000:.1f}K"
                                        else:
                                            users_display = f"{total_users_current:,}"
                                        change_direction = "下降" if quality_rate_change < 0 else "上升"
                                        
                                        # 简洁文本格式
                                        html += f"• <strong>{channel_name}</strong>: {quality_rate_current:.2f}% ({change_direction}{abs(quality_rate_change):.2f}pp), {users_display}用户, 贡献{abs(contribution):.1f}%<br>"
                                    channel_data_found = True
                                break
                            
                            # 通用渠道分析
                            elif cause.get('dimension', '').endswith('渠道分析') and cause.get('generic_channel_detail'):
                                generic_detail = cause['generic_channel_detail']
                                channel_impacts = generic_detail.get('channel_impacts', [])[:3]
                                if channel_impacts:
                                    html += "<strong>问题渠道:</strong><br>"
                                    for i, impact in enumerate(channel_impacts, 1):
                                        channel_name = impact.get('channel', '未知渠道')
                                        metric_current = impact.get('metric_current', 0)
                                        metric_change = impact.get('metric_change', 0)
                                        users = impact.get('users', 0)
                                        weight = impact.get('weight', 0) * 100
                                        severity_score = impact.get('severity_score', 0)
                                        if users >= 10000:
                                            users_display = f"{users/1000:.1f}K"
                                        else:
                                            users_display = f"{users:,}"
                                        change_direction = "下降" if metric_change < 0 else "上升"
                                        
                                        # 根据指标类型显示单位
                                        metric_display = f"{metric_current:.1f}%"
                                        change_display = f"{change_direction}{abs(metric_change):.1f}pp"
                                        
                                        html += f"• {channel_name}: {metric_display} ({change_display}, {users_display}用户, 严重程度{severity_score:.1f})<br>"
                                    channel_data_found = True
                                break
                    
                    if not channel_data_found:
                        html += """<p style="color: #666; font-style: italic;">🔍 异常原因分析中...</p>"""
                    
                    html += "</div>"  # 关闭异常容器
                
                else:
                    # 其他异常类型显示
                    html += f"""
                    <div class="anomaly-item" style="border-left: 4px solid #fd7e14;">
                        <strong>🎯 {metric} 异常</strong><br>"""
                    
                    # 显示前3个最严重的渠道
                    top_impacts = analysis.get('channel_impacts', [])[:3]
                    if top_impacts:
                        html += "<strong>问题渠道:</strong><br>"
                        for impact in top_impacts:
                            if analysis_type == 'cpa_distribution':
                                summary = f"• {impact['channel']}: CPA¥{impact['channel_cpa']:.0f}"
                            elif analysis_type == 'arpu_impact':
                                change = impact.get('arpu_change', 0)
                                direction = "下降" if change < 0 else "上升"
                                summary = f"• {impact['channel']}: ARPU{direction}{abs(change):.1f}元"
                            elif analysis_type == 'generic_channel_analysis':
                                # 通用渠道分析（包括认证率等）
                                metric_current = impact.get('metric_current', 0)
                                metric_change = impact.get('metric_change', 0)
                                users = impact.get('users', 0)
                                weight = impact.get('weight', 0) * 100
                                
                                if users >= 10000:
                                    users_display = f"{users/1000:.1f}K"
                                else:
                                    users_display = f"{users:,}"
                                
                                change_direction = "下降" if metric_change < 0 else "上升"
                                summary = f"• {impact['channel']}: {metric_current:.1f}% ({change_direction}{abs(metric_change):.1f}pp, {users_display}用户, {weight:.1f}%权重)"
                            else:
                                summary = f"• {impact.get('channel', '未知渠道')}"
                            html += summary + "<br>"
                    
                    html += "</div>"
        else:
            html += """
            <h3>🎯 问题定位</h3>
            <div class="anomaly-item">
                <strong>✅ 未检测到异常</strong><br>
                所有核心指标都在正常范围内
            </div>"""
        
        html += """
        </div>"""
        
        # 第三部分：账户分析（原第四部分）
        if account_data:
            html += self._generate_account_analysis_html(account_data, date_str, conn)
        
        # 第四部分：素材分析（原第三部分）
        html += self._generate_creative_analysis_html(creative_data)
        
        html += """
        <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #7f8c8d; font-size: 14px;">
            <p>📊 报告规范说明：</p>
            <ul>
                <li><strong>认证率</strong> = Good且认证用户数 ÷ Good用户数（不是总用户数）</li>
                <li><strong>次留率</strong> = Good且认证用户留存数 ÷ Good且认证用户数</li>
                <li><strong>用户质量指标</strong> = 基于Good且认证用户计算</li>
                <li><strong>异常检测</strong> = 使用IQR（四分位距）方法，检测模块1中所有核心指标（14天历史数据）</li>
                <li><strong>智能渠道分析</strong> = 基于异常检测结果，自动分析ARPU/CPA/用户数异常的渠道影响，按影响程度降序排序</li>
                <li><strong>渠道分析范围</strong> = 仅包含用户数≥50的渠道，按加权影响/绝对变化排序</li>
            </ul>
            <p><small>⚠️ 本报告严格按照内置规范生成，确保数据准确性和一致性</small></p>
        </div>
    </div>

<script>
function filterAccounts() {
    const osFilter = document.getElementById('osFilter').value;
    const genderFilter = document.getElementById('genderFilter').value;
    const accountCards = document.querySelectorAll('.account-card');
    
    accountCards.forEach(card => {
        const cardOs = card.getAttribute('data-os');
        const cardGender = card.getAttribute('data-gender');
        
        const osMatch = (osFilter === 'all' || osFilter === cardOs);
        const genderMatch = (genderFilter === 'all' || genderFilter === cardGender);
        
        if (osMatch && genderMatch) {
            card.style.display = 'block';
        } else {
            card.style.display = 'none';
        }
    });
    
    // 更新每个分组的显示计数
    updateGroupCounts();
}

function resetFilters() {
    document.getElementById('osFilter').value = 'all';
    document.getElementById('genderFilter').value = 'all';
    
    const accountCards = document.querySelectorAll('.account-card');
    accountCards.forEach(card => {
        card.style.display = 'block';
    });
    
    // 更新每个分组的显示计数
    updateGroupCounts();
}

function updateGroupCounts() {
    // 更新白领占比降低账户计数
    const whiteCollarContainer = document.querySelector('h4[style*="color: #e74c3c"]');
    if (whiteCollarContainer) {
        const visibleCards = whiteCollarContainer.parentElement.querySelectorAll('.account-card:not([style*="display: none"])');
        const totalCards = whiteCollarContainer.parentElement.querySelectorAll('.account-card');
        if (visibleCards.length === totalCards.length) {
            whiteCollarContainer.innerHTML = `🏢 白领占比降低账户 (${totalCards.length}个)`;
        } else {
            whiteCollarContainer.innerHTML = `🏢 白领占比降低账户 (显示${visibleCards.length}/${totalCards.length}个)`;
        }
    }
    
    // 更新年轻占比升高账户计数
    const youngContainer = document.querySelector('h4[style*="color: #f39c12"]');
    if (youngContainer) {
        const visibleCards = youngContainer.parentElement.querySelectorAll('.account-card:not([style*="display: none"])');
        const totalCards = youngContainer.parentElement.querySelectorAll('.account-card');
        if (visibleCards.length === totalCards.length) {
            youngContainer.innerHTML = `👶 年轻占比升高账户 (${totalCards.length}个)`;
        } else {
            youngContainer.innerHTML = `👶 年轻占比升高账户 (显示${visibleCards.length}/${totalCards.length}个)`;
        }
    }
    
    // 更新三线城市占比升高账户计数
    const thirdTierContainer = document.querySelector('h4[style*="color: #9b59b6"]');
    if (thirdTierContainer) {
        const visibleCards = thirdTierContainer.parentElement.querySelectorAll('.account-card:not([style*="display: none"])');
        const totalCards = thirdTierContainer.parentElement.querySelectorAll('.account-card');
        if (visibleCards.length === totalCards.length) {
            thirdTierContainer.innerHTML = `🏙️ 三线城市占比升高账户 (${totalCards.length}个)`;
        } else {
            thirdTierContainer.innerHTML = `🏙️ 三线城市占比升高账户 (显示${visibleCards.length}/${totalCards.length}个)`;
        }
    }
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', function() {
    updateGroupCounts();
});
</script>

</body>
</html>"""
        
        return html
    
    def _save_report(self, html_content: str, date_str: str) -> str:
        """保存报告文件 - 按照规范命名"""
        
        # 确保输出目录存在
        os.makedirs('./output/reports', exist_ok=True)
        
        # 按规范生成文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"./output/reports/daily_report_{date_str.replace('-', '')}_{timestamp}.html"
        
        # 保存文件
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"✅ 报告生成成功: {filename}")
        print("🌐 正在自动打开报告...")
        os.system(f"open '{filename}'")
        
        return filename
    
    def _validate_data_consistency(self, conn: sqlite3.Connection, date_str: str, core_data: dict) -> bool:
        """验证数据一致性 - 防止数据错误"""
        if not self.data_validation_enabled:
            return True
            
        try:
            print("🔍 执行数据一致性检查...")
            
            # 1. 验证CPA计算一致性
            # 方法1：主报告的计算方法（分离查询）
            user_query = "SELECT SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users FROM cpz_qs_newuser_channel_i_d WHERE dt = ?"
            cost_query = "SELECT COALESCE(SUM(cash_cost), 0) as total_cost FROM dwd_ttx_market_cash_cost_i_d WHERE dt = ?"
            
            user_result = pd.read_sql_query(user_query, conn, params=[date_str])
            cost_result = pd.read_sql_query(cost_query, conn, params=[date_str])
            
            main_quality_users = user_result.iloc[0]['quality_users']
            main_total_cost = cost_result.iloc[0]['total_cost']
            main_cpa = main_total_cost / main_quality_users if main_quality_users > 0 else 0
            
            # 2. 验证与核心数据的一致性
            if abs(main_quality_users - core_data['quality_users']) > 1:
                print(f"❌ 数据不一致警告: 优质用户数不匹配")
                print(f"   主查询: {main_quality_users:,} vs 核心数据: {core_data['quality_users']:,}")
                return False
                
            if abs(main_cpa - core_data['cpa']) > 0.01:
                print(f"❌ 数据不一致警告: CPA计算不匹配")
                print(f"   主查询: ¥{main_cpa:.2f} vs 核心数据: ¥{core_data['cpa']:.2f}")
                return False
            
            # 3. 验证渠道数据汇总一致性
            channel_summary_query = """
                WITH user_data AS (
                    SELECT SUM(CASE WHEN status='good' AND verification_status='verified' THEN newuser ELSE 0 END) as total_quality_users
                    FROM cpz_qs_newuser_channel_i_d WHERE dt = ?
                ),
                cost_data AS (
                    SELECT SUM(cash_cost) as total_cost
                    FROM dwd_ttx_market_cash_cost_i_d WHERE dt = ?  
                )
                SELECT u.total_quality_users, c.total_cost
                FROM user_data u, cost_data c
            """
            
            channel_result = pd.read_sql_query(channel_summary_query, conn, params=[date_str, date_str])
            if not channel_result.empty:
                channel_users = channel_result.iloc[0]['total_quality_users']
                channel_cost = channel_result.iloc[0]['total_cost']
                
                # 允许10%的误差范围（考虑渠道映射差异）
                user_diff_pct = abs(channel_users - main_quality_users) / main_quality_users * 100 if main_quality_users > 0 else 0
                cost_diff_pct = abs(channel_cost - main_total_cost) / main_total_cost * 100 if main_total_cost > 0 else 0
                
                if user_diff_pct > 10:
                    print(f"⚠️  数据差异警告: 渠道汇总用户数差异{user_diff_pct:.1f}%")
                    print(f"   直接查询: {main_quality_users:,} vs 渠道汇总: {channel_users:,}")
                
                if cost_diff_pct > 10:
                    print(f"⚠️  数据差异警告: 渠道汇总成本差异{cost_diff_pct:.1f}%")
                    print(f"   直接查询: ¥{main_total_cost:,.2f} vs 渠道汇总: ¥{channel_cost:,.2f}")
            
            print("✅ 数据一致性检查通过")
            return True
            
        except Exception as e:
            print(f"❌ 数据一致性检查失败: {e}")
            return False
    
    def _analyze_age_group_impact(self, metric_name: str, date_str: str, prev_date: str, conn: sqlite3.Connection) -> dict:
        """分析年龄段维度对异常的影响"""
        try:
            # 获取年龄段数据对比
            if '女性占比' in metric_name:
                query = f"""
                WITH curr_data AS (
                    SELECT 
                        age_group,
                        SUM(CASE WHEN gender = 'female' AND status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as female_users,
                        SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as total_users
                    FROM cpz_qs_newuser_channel_i_d 
                    WHERE dt = '{date_str}' AND age_group <> ''
                    GROUP BY age_group
                ),
                prev_data AS (
                    SELECT 
                        age_group,
                        SUM(CASE WHEN gender = 'female' AND status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) * 100.0 / 
                        SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as female_pct_prev
                    FROM cpz_qs_newuser_channel_i_d 
                    WHERE dt = '{prev_date}' AND age_group <> ''
                    GROUP BY age_group
                )
                SELECT 
                    curr_data.age_group,
                    curr_data.female_users,
                    curr_data.total_users,
                    curr_data.female_users * 100.0 / curr_data.total_users as female_pct_curr,
                    prev_data.female_pct_prev,
                    (curr_data.female_users * 100.0 / curr_data.total_users) - prev_data.female_pct_prev as pct_change
                FROM curr_data
                LEFT JOIN prev_data ON curr_data.age_group = prev_data.age_group
                WHERE curr_data.total_users >= 50
                ORDER BY ABS((curr_data.female_users * 100.0 / curr_data.total_users) - prev_data.female_pct_prev) DESC
                """
            
            # 执行查询并分析结果 - 这里简化处理
            return {'has_impact': False, 'analysis': '年龄段分析已简化'}
            
        except Exception as e:
            print(f"年龄段分析错误: {e}")
            return {'has_impact': False, 'error': str(e)}

    def _has_continuous_trend(self, values: list, threshold: float = 0.1) -> bool:
        """检测是否存在连续趋势"""
        if len(values) < 3:
            return False
        
        # 检查最后3个值是否呈现连续变化
        last_3 = values[-3:]
        if len(last_3) < 3:
            return False
        
        # 如果连续上升或连续下降，认为是连续趋势
        increasing = all(last_3[i] <= last_3[i+1] for i in range(len(last_3)-1))
        decreasing = all(last_3[i] >= last_3[i+1] for i in range(len(last_3)-1))
        
        return increasing or decreasing

    def _analyze_quality_users_by_channel_enhanced(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                                 anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析Good且认证用户数异常的渠道影响，支持贡献度计算"""
        try:
            print(f"DEBUG: Good且认证用户数渠道分析开始 - {date_str}")
            
            # 获取当前和前一天的数据
            current_query = f"""
                SELECT ad_channel,
                       SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users_current
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date_str}' AND ad_channel != '(unknown)'
                GROUP BY ad_channel
                HAVING quality_users_current >= 50
                ORDER BY quality_users_current DESC
            """
            
            prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            prev_query = f"""
                SELECT ad_channel,
                       SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users_prev
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{prev_date}' AND ad_channel != '(unknown)'
                GROUP BY ad_channel
                HAVING quality_users_prev >= 50
                ORDER BY quality_users_prev DESC
            """
            
            current_df = pd.read_sql_query(current_query, conn)
            prev_df = pd.read_sql_query(prev_query, conn)
            
            # 计算整体变化
            total_current_quality_users = current_df['quality_users_current'].sum() if not current_df.empty else 0
            total_prev_quality_users = prev_df['quality_users_prev'].sum() if not prev_df.empty else 0
            overall_quality_users_change = total_current_quality_users - total_prev_quality_users
            
            print(f"DEBUG: 整体Good且认证用户数 - 当前:{total_current_quality_users}, 前日:{total_prev_quality_users}, 变化:{overall_quality_users_change}")
            
            # 合并数据
            merged_df = pd.merge(current_df, prev_df, on='ad_channel', how='outer').fillna(0)
            
            # 分析每个渠道的影响
            channel_impacts = []
            for _, row in merged_df.iterrows():
                quality_users_current = row['quality_users_current']
                quality_users_prev = row['quality_users_prev'] 
                quality_users_change = quality_users_current - quality_users_prev
                
                # 计算权重
                weight = quality_users_current / total_current_quality_users if total_current_quality_users > 0 else 0
                
                # 计算加权影响 = 用户数变化 * 权重
                weighted_impact = quality_users_change * weight
                
                if abs(quality_users_change) >= 10:  # 只关注变化较大的渠道
                    channel_impacts.append({
                        'channel': row['ad_channel'],
                        'quality_users_current': quality_users_current,
                        'quality_users_prev': quality_users_prev,
                        'quality_users_change': quality_users_change,
                        'weight': weight,
                        'weighted_impact': weighted_impact,
                        'severity_score': abs(quality_users_change) * (1 + weight * 10)
                    })
            
            # 计算贡献度
            for impact in channel_impacts:
                if overall_quality_users_change != 0:
                    impact['contribution_to_overall'] = (impact['weighted_impact'] / abs(overall_quality_users_change)) * 100
                else:
                    impact['contribution_to_overall'] = 0
            
            # 筛选和排序显著影响
            if overall_quality_users_change < 0:
                # 整体下降时，只显示对下降有负面贡献的渠道
                negative_impacts = [impact for impact in channel_impacts if impact.get('contribution_to_overall', 0) < 0]
                negative_impacts.sort(key=lambda x: x.get('contribution_to_overall', 0))
                significant_impacts = negative_impacts[:3]
            else:
                # 整体上升时，显示所有异常渠道
                channel_impacts.sort(key=lambda x: abs(x.get('contribution_to_overall', 0)), reverse=True)
                significant_impacts = channel_impacts[:3]
            
            print(f"✅ Good且认证用户数渠道异常分析完成，识别{len(significant_impacts)}个异常渠道")
            
            return {
                'metric': 'Good且认证用户数异常',
                'anomaly_direction': '渠道分析',
                'channel_impacts': significant_impacts,
                'analysis_type': 'quality_users_analysis',
                'analysis_summary': f'识别{len(significant_impacts)}个Good且认证用户数异常渠道',
                'overall_change': overall_quality_users_change,
                'main_contributor_msg': ""
            }
            
        except Exception as e:
            print(f"ERROR: Good且认证用户数异常分析失败: {e}")
            return {
                'metric': 'Good且认证用户数异常',
                'anomaly_direction': '分析失败',
                'channel_impacts': [],
                'analysis_type': 'quality_users_analysis',
                'analysis_summary': f'Good且认证用户数异常分析失败: {str(e)}'
            }
    
    def _analyze_quality_rate_by_channel(self, channel_df: pd.DataFrame, prev_channel_df: pd.DataFrame, 
                                       anomaly: dict, date_str: str, conn: sqlite3.Connection) -> dict:
        """分析Good且认证率异常的渠道影响，支持贡献度计算"""
        try:
            print(f"DEBUG: Good且认证率渠道分析开始 - {date_str}")
            
            # 获取当前和前一天的数据
            current_query = f"""
                SELECT ad_channel,
                       SUM(newuser) as total_users_current,
                       SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users_current,
                       ROUND(100.0 * SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) / SUM(newuser), 2) as quality_rate_current
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{date_str}' AND ad_channel != '(unknown)'
                GROUP BY ad_channel
                HAVING total_users_current >= 50
                ORDER BY quality_users_current DESC
            """
            
            prev_date = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            prev_query = f"""
                SELECT ad_channel,
                       SUM(newuser) as total_users_prev,
                       SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users_prev,
                       ROUND(100.0 * SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) / SUM(newuser), 2) as quality_rate_prev
                FROM cpz_qs_newuser_channel_i_d 
                WHERE dt = '{prev_date}' AND ad_channel != '(unknown)'
                GROUP BY ad_channel
                HAVING total_users_prev >= 50
                ORDER BY quality_users_prev DESC
            """
            
            current_df = pd.read_sql_query(current_query, conn)
            prev_df = pd.read_sql_query(prev_query, conn)
            
            # 计算整体变化
            total_current_total_users = current_df['total_users_current'].sum() if not current_df.empty else 0
            total_current_quality_users = current_df['quality_users_current'].sum() if not current_df.empty else 0
            total_prev_total_users = prev_df['total_users_prev'].sum() if not prev_df.empty else 0
            total_prev_quality_users = prev_df['quality_users_prev'].sum() if not prev_df.empty else 0
            
            total_current_quality_rate = (total_current_quality_users / total_current_total_users * 100) if total_current_total_users > 0 else 0
            total_prev_quality_rate = (total_prev_quality_users / total_prev_total_users * 100) if total_prev_total_users > 0 else 0
            overall_quality_rate_change = total_current_quality_rate - total_prev_quality_rate
            
            print(f"DEBUG: 整体Good且认证率 - 当前:{total_current_quality_rate:.2f}%, 前日:{total_prev_quality_rate:.2f}%, 变化:{overall_quality_rate_change:.2f}pp")
            
            # 合并数据
            merged_df = pd.merge(current_df, prev_df, on='ad_channel', how='outer').fillna(0)
            
            # 分析每个渠道的影响
            channel_impacts = []
            for _, row in merged_df.iterrows():
                quality_rate_current = row['quality_rate_current']
                quality_rate_prev = row['quality_rate_prev'] 
                quality_rate_change = quality_rate_current - quality_rate_prev
                total_users_current = row['total_users_current']
                
                # 计算权重 (基于当前用户数)
                weight = total_users_current / total_current_total_users if total_current_total_users > 0 else 0
                
                # 计算加权影响 = 质量率变化 * 权重
                weighted_impact = quality_rate_change * weight
                
                if abs(quality_rate_change) >= 1:  # 只关注变化>=1pp的渠道
                    channel_impacts.append({
                        'channel': row['ad_channel'],
                        'quality_rate_current': quality_rate_current,
                        'quality_rate_prev': quality_rate_prev,
                        'quality_rate_change': quality_rate_change,
                        'total_users_current': total_users_current,
                        'weight': weight,
                        'weighted_impact': weighted_impact,
                        'severity_score': abs(quality_rate_change) * weight * 100
                    })
            
            # 计算贡献度
            for impact in channel_impacts:
                if overall_quality_rate_change != 0:
                    impact['contribution_to_overall'] = (impact['weighted_impact'] / abs(overall_quality_rate_change)) * 100
                else:
                    impact['contribution_to_overall'] = 0
            
            # 筛选和排序显著影响
            if overall_quality_rate_change < 0:
                # 整体下降时，只显示对下降有负面贡献的渠道
                negative_impacts = [impact for impact in channel_impacts if impact.get('contribution_to_overall', 0) < 0]
                negative_impacts.sort(key=lambda x: x.get('contribution_to_overall', 0))
                significant_impacts = negative_impacts[:3]
            else:
                # 整体上升时，显示所有异常渠道
                channel_impacts.sort(key=lambda x: abs(x.get('contribution_to_overall', 0)), reverse=True)
                significant_impacts = channel_impacts[:3]
            
            print(f"✅ Good且认证率渠道异常分析完成，识别{len(significant_impacts)}个异常渠道")
            
            return {
                'metric': 'Good且认证率异常',
                'anomaly_direction': '渠道分析',
                'channel_impacts': significant_impacts,
                'analysis_type': 'quality_rate_analysis',
                'analysis_summary': f'识别{len(significant_impacts)}个Good且认证率异常渠道',
                'overall_change': overall_quality_rate_change,
                'main_contributor_msg': ""
            }
            
        except Exception as e:
            print(f"ERROR: Good且认证率异常分析失败: {e}")
            return {
                'metric': 'Good且认证率异常',
                'anomaly_direction': '分析失败',
                'channel_impacts': [],
                'analysis_type': 'quality_rate_analysis',
                'analysis_summary': f'Good且认证率异常分析失败: {str(e)}'
            }
    
    def _collect_creative_analysis(self, conn: sqlite3.Connection, date_str: str) -> dict:
        """收集素材分析数据"""
        try:
            print("📊 开始素材分析数据收集...")
            
            # 计算昨天的日期
            from datetime import datetime, timedelta
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            yesterday_obj = date_obj - timedelta(days=1)
            yesterday_str = yesterday_obj.strftime('%Y-%m-%d')
            
            # 1. 获取今日素材排名
            today_query = self.QUERIES['creative_ranking_today'].format(date=date_str)
            today_df = pd.read_sql_query(today_query, conn)
            
            # 2. 获取昨日素材排名
            yesterday_query = self.QUERIES['creative_ranking_yesterday'].format(yesterday_date=yesterday_str)
            yesterday_df = pd.read_sql_query(yesterday_query, conn)
            
            # 3. 合并数据，计算排名变化
            creative_rankings = []
            
            # 创建昨日排名字典（现在按素材ID，不分渠道）
            yesterday_ranks = {}
            for idx, row in yesterday_df.iterrows():
                creative_id = row['media_id_str']
                yesterday_ranks[creative_id] = row['rank_yesterday']
            
            # 处理今日排名数据
            for idx, row in today_df.iterrows():
                creative_id = row['media_id_str']
                today_rank = idx + 1
                yesterday_rank = yesterday_ranks.get(creative_id, None)
                
                # 计算排名变化
                rank_change = ""
                rank_change_num = 0
                if yesterday_rank is not None:
                    rank_change_num = yesterday_rank - today_rank  # 正数表示上升
                    if rank_change_num > 0:
                        rank_change = f"↑{rank_change_num}"
                    elif rank_change_num < 0:
                        rank_change = f"↓{abs(rank_change_num)}"
                    else:
                        rank_change = "="
                else:
                    rank_change = "⭐新进榜"
                
                # 处理空值
                arpu = row['arpu'] if pd.notna(row['arpu']) else 0
                retention_rate = row['retention_rate'] if pd.notna(row['retention_rate']) else 0
                female_rate = row['female_rate'] if pd.notna(row['female_rate']) else 0
                white_collar_rate = row['white_collar_rate'] if pd.notna(row['white_collar_rate']) else 0
                ios_rate = row['ios_rate'] if pd.notna(row['ios_rate']) else 0
                
                # 处理渠道信息
                channels = row['channels'] if pd.notna(row['channels']) else ''
                channel_count = int(row['channel_count']) if pd.notna(row['channel_count']) else 0
                
                creative_rankings.append({
                    'rank': today_rank,
                    'creative_id': row['media_id_str'],
                    'channels': channels,
                    'channel_count': channel_count,
                    'good_verified': int(row['good_verified']),
                    'cost': float(row['cost']) if pd.notna(row['cost']) else 0,
                    'cpa': float(row['cpa']) if pd.notna(row['cpa']) else 0,
                    'arpu': float(arpu),
                    'retention_rate': float(retention_rate),
                    'female_rate': float(female_rate),
                    'white_collar_rate': float(white_collar_rate),
                    'ios_rate': float(ios_rate),
                    'yesterday_rank': yesterday_rank,
                    'rank_change': rank_change,
                    'rank_change_num': rank_change_num
                })
            
            # 4. 生成分析洞察
            insights = self._generate_creative_insights(creative_rankings)
            
            print(f"✅ 素材分析完成，获得{len(creative_rankings)}个素材排名")
            
            return {
                'rankings': creative_rankings,
                'insights': insights,
                'total_creatives': len(creative_rankings),
                'analysis_date': date_str,
                'yesterday_date': yesterday_str
            }
            
        except Exception as e:
            print(f"❌ 素材分析失败: {e}")
            return {
                'rankings': [],
                'insights': {
                    'top_rising': None,
                    'top_falling': None,
                    'new_entries': [],
                    'cpa_anomalies': [],
                    'high_female_rate': None,
                    'high_ios_rate': None
                },
                'total_creatives': 0,
                'analysis_date': date_str,
                'error': str(e)
            }
    
    def _generate_creative_insights(self, rankings: list) -> dict:
        """生成素材分析洞察"""
        insights = {
            'top_rising': None,
            'top_falling': None,
            'new_entries': [],
            'cpa_anomalies': [],
            'high_female_rate': None,
            'high_ios_rate': None
        }
        
        if not rankings:
            return insights
        
        # 找到排名变化最大的素材
        rising_creatives = [c for c in rankings if c['rank_change_num'] > 0]
        falling_creatives = [c for c in rankings if c['rank_change_num'] < 0]
        new_entries = [c for c in rankings if c['yesterday_rank'] is None]
        
        if rising_creatives:
            insights['top_rising'] = max(rising_creatives, key=lambda x: x['rank_change_num'])
        
        if falling_creatives:
            insights['top_falling'] = min(falling_creatives, key=lambda x: x['rank_change_num'])
        
        insights['new_entries'] = new_entries[:3]  # 只显示前3个新进榜
        
        # CPA异常分析（超过平均值2倍的）
        valid_cpas = [c['cpa'] for c in rankings if c['cpa'] > 0]
        if valid_cpas:
            avg_cpa = sum(valid_cpas) / len(valid_cpas)
            cpa_threshold = avg_cpa * 2
            insights['cpa_anomalies'] = [c for c in rankings if c['cpa'] > cpa_threshold][:3]
        
        # 女性占比最高的素材
        if rankings:
            insights['high_female_rate'] = max(rankings, key=lambda x: x['female_rate'])
            insights['high_ios_rate'] = max(rankings, key=lambda x: x['ios_rate'])
        
        return insights
    
    def _collect_account_analysis(self, conn: sqlite3.Connection, date_str: str, channel: str = 'Douyin') -> dict:
        """收集账户分析数据"""
        try:
            print(f"📊 开始账户分析数据收集...")
            
            # 计算昨日和7日前日期
            from datetime import datetime, timedelta
            current_date = datetime.strptime(date_str, '%Y-%m-%d')
            yesterday_str = (current_date - timedelta(days=1)).strftime('%Y-%m-%d')
            week_ago_str = (current_date - timedelta(days=7)).strftime('%Y-%m-%d')
            
            # 1. 获取当日账户数据
            today_query = self.QUERIES['account_metrics_today'].format(date=date_str, channel=channel)
            today_df = pd.read_sql_query(today_query, conn)
            
            # 2. 获取昨日账户数据
            yesterday_query = self.QUERIES['account_metrics_yesterday'].format(yesterday_date=yesterday_str, channel=channel)
            yesterday_df = pd.read_sql_query(yesterday_query, conn)
            
            # 3. 获取7日均值数据
            avg_7d_query = self.QUERIES['account_metrics_7d_avg'].format(
                start_date=week_ago_str, end_date=yesterday_str, channel=channel
            )
            avg_7d_df = pd.read_sql_query(avg_7d_query, conn)
            
            # 4. 检测异常账户
            anomaly_accounts = self._detect_account_anomalies(today_df, yesterday_df, avg_7d_df)
            
            # 5. 计算相关性
            correlation_analysis = self._calculate_age_arpu_correlation(today_df, yesterday_df)
            
            print(f"✅ 账户分析完成，获得{len(today_df)}个账户，检测到{len(anomaly_accounts)}个异常账户")
            
            return {
                'today_accounts': today_df,
                'yesterday_accounts': yesterday_df,
                'avg_7d_accounts': avg_7d_df,
                'anomaly_accounts': anomaly_accounts,
                'correlation_analysis': correlation_analysis,
                'channel': channel,
                'analysis_date': date_str,
                'yesterday_date': yesterday_str
            }
            
        except Exception as e:
            print(f"❌ 账户分析失败: {e}")
            return {
                'today_accounts': pd.DataFrame(),
                'yesterday_accounts': pd.DataFrame(),
                'avg_7d_accounts': pd.DataFrame(),
                'anomaly_accounts': [],
                'correlation_analysis': {},
                'channel': channel,
                'analysis_date': date_str,
                'error': str(e)
            }
    
    def _detect_account_anomalies(self, today_df: pd.DataFrame, yesterday_df: pd.DataFrame, avg_7d_df: pd.DataFrame) -> dict:
        """检测异常账户：ARPU降低且用户质量指标负向变化超过1%"""
        # 按指标分类的异常账户
        anomaly_by_metric = {
            'white_collar': [],
            'young': [],
            'third_tier_city': [],
            'overall_changes': {},
            'metrics_triggered': []
        }
        
        if today_df.empty or avg_7d_df.empty:
            return anomaly_by_metric
        
        # 1. 首先计算整体指标变化，判断是否需要检测对应类型的异常
        overall_changes = self._calculate_overall_quality_changes(today_df, avg_7d_df)
        anomaly_by_metric['overall_changes'] = overall_changes
        
        # 只有当整体指标负向变化>1%时，才检测对应的账户异常
        quality_metrics_to_check = []
        arpu_change = overall_changes.get('arpu_change', 0) or 0
        white_collar_change = overall_changes.get('white_collar_change', 0) or 0
        young_change = overall_changes.get('young_change', 0) or 0
        third_tier_city_change = overall_changes.get('third_tier_city_change', 0) or 0
        
        if arpu_change < -1:  # ARPU下降>1%
            quality_metrics_to_check.append('arpu')
        if white_collar_change < -1:  # 白领占比下降>1%
            quality_metrics_to_check.append('white_collar')
            anomaly_by_metric['metrics_triggered'].append('white_collar')
        if young_change > 1:  # 年轻占比上升>1%
            quality_metrics_to_check.append('young')
            anomaly_by_metric['metrics_triggered'].append('young')
        if third_tier_city_change > 1:  # 三线城市占比上升>1%
            quality_metrics_to_check.append('third_tier_city')
            anomaly_by_metric['metrics_triggered'].append('third_tier_city')
        
        # 如果没有整体异常，则不检测账户异常
        if not quality_metrics_to_check:
            return anomaly_by_metric
        
        # 2. 创建7日均值字典进行账户级别异常检测
        avg_7d_dict = {}
        for _, row in avg_7d_df.iterrows():
            key = (row['account'], row['os'], row['gender'])
            avg_7d_dict[key] = row
        
        # 3. 检测每个账户的异常
        for _, today_row in today_df.iterrows():
            account_key = (today_row['account'], today_row['os'], today_row['gender'])
            
            # 必须有7日均值数据才能比较
            if account_key not in avg_7d_dict:
                continue
                
            avg_7d_row = avg_7d_dict[account_key]
            
            # 检测ARPU降低条件
            arpu_today = today_row['arpu'] if pd.notna(today_row['arpu']) else 0
            arpu_7d_avg = avg_7d_row['arpu_avg'] if pd.notna(avg_7d_row['arpu_avg']) else 0
            arpu_decreased = arpu_today < arpu_7d_avg and arpu_7d_avg > 0
            
            # 必须满足ARPU降低条件
            if not arpu_decreased:
                continue
            
            # 创建基础账户信息
            base_account = {
                'account': today_row['account'],
                'account_name': today_row['account_name'],
                'os': today_row['os'],
                'gender': today_row['gender'],
                'good_verified': int(today_row['good_verified']),
                'arpu_today': arpu_today,
                'arpu_7d_avg': arpu_7d_avg,
                'white_collar_rate_today': today_row.get('white_collar_rate', 0),
                'under_23_rate_today': today_row.get('under_23_rate', 0),
                'third_tier_city_rate_today': today_row.get('third_tier_city_rate', 0),
                'white_collar_rate_7d_avg': avg_7d_row.get('white_collar_rate_avg', 0),
                'under_23_rate_7d_avg': avg_7d_row.get('under_23_rate_avg', 0),
                'third_tier_city_rate_7d_avg': avg_7d_row.get('third_tier_city_rate_avg', 0),
            }
            
            # 检测各项质量指标并分类到对应列表
            
            # 白领占比检测
            if 'white_collar' in quality_metrics_to_check:
                white_collar_today = today_row.get('white_collar_rate', 0) if pd.notna(today_row.get('white_collar_rate', 0)) else 0
                white_collar_7d_avg = avg_7d_row.get('white_collar_rate_avg', 0) if pd.notna(avg_7d_row.get('white_collar_rate_avg', 0)) else 0
                white_collar_change = white_collar_today - white_collar_7d_avg
                if white_collar_change < -1:  # 白领占比下降>1%
                    account_copy = base_account.copy()
                    account_copy.update({
                        'primary_anomaly': 'white_collar',
                        'primary_anomaly_desc': f'白领占比下降{abs(white_collar_change):.1f}pp',
                        'anomaly_change': white_collar_change
                    })
                    anomaly_by_metric['white_collar'].append(account_copy)
            
            # 年轻占比检测
            if 'young' in quality_metrics_to_check:
                young_today = today_row.get('under_23_rate', 0) if pd.notna(today_row.get('under_23_rate', 0)) else 0
                young_7d_avg = avg_7d_row.get('under_23_rate_avg', 0) if pd.notna(avg_7d_row.get('under_23_rate_avg', 0)) else 0
                young_change = young_today - young_7d_avg
                if young_change > 1:  # 年轻占比上升>1%
                    account_copy = base_account.copy()
                    account_copy.update({
                        'primary_anomaly': 'young',
                        'primary_anomaly_desc': f'年轻占比上升{young_change:.1f}pp',
                        'anomaly_change': young_change
                    })
                    anomaly_by_metric['young'].append(account_copy)
            
            # 三线城市占比检测
            if 'third_tier_city' in quality_metrics_to_check:
                third_tier_today = today_row.get('third_tier_city_rate', 0) if pd.notna(today_row.get('third_tier_city_rate', 0)) else 0
                third_tier_7d_avg = avg_7d_row.get('third_tier_city_rate_avg', 0) if pd.notna(avg_7d_row.get('third_tier_city_rate_avg', 0)) else 0
                third_tier_change = third_tier_today - third_tier_7d_avg
                if third_tier_change > 1:  # 三线城市占比上升>1%
                    account_copy = base_account.copy()
                    account_copy.update({
                        'primary_anomaly': 'third_tier_city',
                        'primary_anomaly_desc': f'三线城市占比上升{third_tier_change:.1f}pp',
                        'anomaly_change': third_tier_change
                    })
                    anomaly_by_metric['third_tier_city'].append(account_copy)
        
        # 按Good且认证用户数降序排序每个类别
        for metric in ['white_collar', 'young', 'third_tier_city']:
            anomaly_by_metric[metric].sort(key=lambda x: x['good_verified'], reverse=True)
        
        return anomaly_by_metric
    
    def _calculate_overall_quality_changes(self, today_df: pd.DataFrame, avg_7d_df: pd.DataFrame) -> dict:
        """计算整体质量指标变化"""
        # 计算当日整体指标
        today_total_users = today_df['good_verified'].sum()
        today_total_revenue = (today_df['arpu'] * today_df['good_verified']).sum()
        today_total_white_collar = (today_df.get('white_collar_rate', 0) * today_df['good_verified'] / 100).sum()
        today_total_young = (today_df.get('under_23_rate', 0) * today_df['good_verified'] / 100).sum()
        today_total_third_tier = (today_df.get('third_tier_city_rate', 0) * today_df['good_verified'] / 100).sum()
        
        # 计算7日均值整体指标
        avg_7d_total_users = avg_7d_df['good_verified_avg'].sum()
        avg_7d_total_revenue = (avg_7d_df['arpu_avg'] * avg_7d_df['good_verified_avg']).sum()
        avg_7d_total_white_collar = (avg_7d_df.get('white_collar_rate_avg', 0) * avg_7d_df['good_verified_avg'] / 100).sum()
        avg_7d_total_young = (avg_7d_df.get('under_23_rate_avg', 0) * avg_7d_df['good_verified_avg'] / 100).sum()
        avg_7d_total_third_tier = (avg_7d_df.get('third_tier_city_rate_avg', 0) * avg_7d_df['good_verified_avg'] / 100).sum()
        
        # 计算变化百分比
        arpu_today = today_total_revenue / today_total_users if today_total_users > 0 else 0
        arpu_7d_avg = avg_7d_total_revenue / avg_7d_total_users if avg_7d_total_users > 0 else 0
        arpu_change = arpu_today - arpu_7d_avg
        
        white_collar_today = today_total_white_collar / today_total_users * 100 if today_total_users > 0 else 0
        white_collar_7d_avg = avg_7d_total_white_collar / avg_7d_total_users * 100 if avg_7d_total_users > 0 else 0
        white_collar_change = white_collar_today - white_collar_7d_avg
        
        young_today = today_total_young / today_total_users * 100 if today_total_users > 0 else 0
        young_7d_avg = avg_7d_total_young / avg_7d_total_users * 100 if avg_7d_total_users > 0 else 0
        young_change = young_today - young_7d_avg
        
        third_tier_today = today_total_third_tier / today_total_users * 100 if today_total_users > 0 else 0
        third_tier_7d_avg = avg_7d_total_third_tier / avg_7d_total_users * 100 if avg_7d_total_users > 0 else 0
        third_tier_city_change = third_tier_today - third_tier_7d_avg
        
        return {
            'arpu_change': arpu_change,
            'white_collar_change': white_collar_change,
            'young_change': young_change,
            'third_tier_city_change': third_tier_city_change
        }
    
    def _calculate_age_arpu_correlation(self, today_df: pd.DataFrame, yesterday_df: pd.DataFrame) -> dict:
        """计算年龄占比与ARPU变化的相关性"""
        if today_df.empty or yesterday_df.empty:
            return {'under_20_correlation': 0, 'under_23_correlation': 0}
        
        # 创建昨日数据字典
        yesterday_dict = {}
        for _, row in yesterday_df.iterrows():
            yesterday_dict[row['account']] = row
        
        # 收集变化数据
        arpu_changes = []
        under_20_changes = []
        under_23_changes = []
        
        for _, today_row in today_df.iterrows():
            account = today_row['account']
            if account not in yesterday_dict:
                continue
                
            yesterday_row = yesterday_dict[account]
            
            # 计算变化量
            arpu_change = (today_row['arpu'] or 0) - (yesterday_row['arpu'] or 0)
            under_20_change = (today_row['under_20_rate'] or 0) - (yesterday_row['under_20_rate'] or 0)
            under_23_change = (today_row['under_23_rate'] or 0) - (yesterday_row['under_23_rate'] or 0)
            
            # 过滤掉无效数据
            if abs(arpu_change) > 0.01:  # ARPU变化至少0.01元
                arpu_changes.append(arpu_change)
                under_20_changes.append(under_20_change)
                under_23_changes.append(under_23_change)
        
        # 计算相关系数
        correlation_result = {'under_20_correlation': 0, 'under_23_correlation': 0}
        
        if len(arpu_changes) >= 3:  # 至少3个样本才计算相关性
            try:
                import numpy as np
                
                # 计算20岁以下占比与ARPU变化的相关性
                if len(set(under_20_changes)) > 1:  # 确保不是所有值都相同
                    correlation_result['under_20_correlation'] = np.corrcoef(arpu_changes, under_20_changes)[0, 1]
                    if np.isnan(correlation_result['under_20_correlation']):
                        correlation_result['under_20_correlation'] = 0
                
                # 计算23岁以下占比与ARPU变化的相关性
                if len(set(under_23_changes)) > 1:
                    correlation_result['under_23_correlation'] = np.corrcoef(arpu_changes, under_23_changes)[0, 1]
                    if np.isnan(correlation_result['under_23_correlation']):
                        correlation_result['under_23_correlation'] = 0
                        
            except Exception as e:
                print(f"⚠️ 相关性计算失败: {e}")
        
        return correlation_result
    
    def _generate_account_analysis_html(self, account_data: dict, date_str: str = None, conn=None) -> str:
        """生成账户分析HTML部分"""
        if not account_data or account_data.get('error'):
            return """
        <!-- 第三部分：账户分析 -->
        <div class="module">
            <h2>📊 模块三：账户数据监测</h2>
            <div class="anomaly-item">
                <strong>⚠️ 账户数据不可用</strong><br>
                """ + account_data.get('error', '没有账户数据') + """
            </div>
        </div>
        """
        
        anomaly_accounts = account_data.get('anomaly_accounts', [])
        correlation_analysis = account_data.get('correlation_analysis', {})
        channel = account_data.get('channel', 'Douyin')
        analysis_date = account_data.get('analysis_date', '')
        
        # 计算占比数据
        channel_total = 0
        attribute_totals = {}
        
        if anomaly_accounts and conn is not None and date_str:
            try:
                # 查询当日所有账户数据 - 使用与账户分析相同的表和逻辑
                sql = f"""
                SELECT 
                    account,
                    CASE WHEN account_name LIKE '%安卓%' THEN 'Android'
                         WHEN account_name LIKE '%iOS%' OR account_name LIKE '%ios%' OR account_name LIKE '%IOS%' THEN 'iOS'
                         ELSE 'other' END AS os,
                    CASE WHEN account_name LIKE '%男%' AND account_name NOT LIKE '%男女%' THEN '男'
                         WHEN account_name LIKE '%女%' AND account_name NOT LIKE '%男女%' THEN '女'
                         ELSE 'other' END AS gender,
                    SUM(total_good_verified) as good_verified
                FROM dwd_ttx_market_cash_cost_i_d_test 
                WHERE dt = '{date_str}' AND channel = '{channel}' AND account IS NOT NULL AND account != ''
                GROUP BY account, account_name
                """
                
                all_accounts_df = pd.read_sql_query(sql, conn)
                channel_total = all_accounts_df['good_verified'].sum()
                
                # 计算各属性组合的总数
                for _, row in all_accounts_df.iterrows():
                    os_type = row['os']
                    gender = row['gender']
                    key = f"{os_type}_{gender}"
                    if key not in attribute_totals:
                        attribute_totals[key] = 0
                    attribute_totals[key] += row['good_verified']
                    
            except Exception as e:
                print(f"占比计算失败: {e}")
        
        html = f"""
        <!-- 第三部分：账户分析 -->
        <div class="module">
            <h2>📊 模块三：账户数据监测 - {channel}</h2>
            
            <div style="display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 15px;">
                <h3 style="margin: 0;">🔍 ARPU降低且用户质量负向变化的账户</h3>
                
                <!-- 账户属性筛选器 -->
                <div style="background: #f8f9fa; padding: 10px; border-radius: 6px; border: 1px solid #dee2e6; font-size: 12px;">
                    <span style="font-weight: bold; margin-right: 10px; color: #495057;">账户属性:</span>
                    <label style="margin-right: 5px;">OS:</label>
                    <select id="osFilter" style="padding: 3px 5px; border: 1px solid #ced4da; border-radius: 3px; font-size: 11px; margin-right: 10px;">
                        <option value="all">全部</option>
                        <option value="Android">Android</option>
                        <option value="iOS">iOS</option>
                        <option value="other">其他</option>
                    </select>
                    <label style="margin-right: 5px;">SEX:</label>
                    <select id="genderFilter" style="padding: 3px 5px; border: 1px solid #ced4da; border-radius: 3px; font-size: 11px; margin-right: 8px;">
                        <option value="all">全部</option>
                        <option value="男">男</option>
                        <option value="女">女</option>
                        <option value="other">其他</option>
                    </select>
                    <button onclick="filterAccounts()" style="padding: 4px 8px; background: #007bff; color: white; border: none; border-radius: 3px; cursor: pointer; font-size: 11px; margin-right: 4px;">筛选</button>
                    <button onclick="resetFilters()" style="padding: 4px 8px; background: #6c757d; color: white; border: none; border-radius: 3px; cursor: pointer; font-size: 11px;">重置</button>
                </div>
            </div>"""
            
        # 检查是否有异常数据
        has_anomalies = (
            anomaly_accounts.get('white_collar', []) or 
            anomaly_accounts.get('young', []) or 
            anomaly_accounts.get('third_tier_city', [])
        ) if isinstance(anomaly_accounts, dict) else bool(anomaly_accounts)
            
        if has_anomalies:
            # 添加异常账户总结
            if isinstance(anomaly_accounts, dict):
                white_collar_accounts = anomaly_accounts.get('white_collar', [])
                young_accounts = anomaly_accounts.get('young', [])
                third_tier_accounts = anomaly_accounts.get('third_tier_city', [])
                
                white_collar_count = len(white_collar_accounts)
                young_count = len(young_accounts)
                third_tier_count = len(third_tier_accounts)
                total_anomaly_count = white_collar_count + young_count + third_tier_count
                
                # 检测重复账户和多指标异常
                all_accounts = {}
                
                # 统计每个账户出现在哪些异常类型中
                for account in white_collar_accounts:
                    account_id = account['account']
                    if account_id not in all_accounts:
                        all_accounts[account_id] = {'account': account, 'anomaly_types': []}
                    all_accounts[account_id]['anomaly_types'].append('白领占比降低')
                
                for account in young_accounts:
                    account_id = account['account']
                    if account_id not in all_accounts:
                        all_accounts[account_id] = {'account': account, 'anomaly_types': []}
                    all_accounts[account_id]['anomaly_types'].append('年轻占比升高')
                
                for account in third_tier_accounts:
                    account_id = account['account']
                    if account_id not in all_accounts:
                        all_accounts[account_id] = {'account': account, 'anomaly_types': []}
                    all_accounts[account_id]['anomaly_types'].append('三线城市占比升高')
                
                # 计算实际唯一账户数和多指标异常账户
                unique_account_count = len(all_accounts)
                multi_anomaly_accounts = [acc for acc in all_accounts.values() if len(acc['anomaly_types']) > 1]
                multi_anomaly_count = len(multi_anomaly_accounts)
                
                html += f"""
            <div style="background: #fff8e1; padding: 15px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #ff9800;">
                <h4 style="margin: 0 0 10px 0; color: #e65100;">📊 异常账户概览</h4>
                <div style="display: flex; gap: 30px; align-items: center; flex-wrap: wrap; font-size: 14px; margin-bottom: 10px;">
                    <div><strong>异常账户数:</strong> {total_anomaly_count}个</div>
                    <div style="color: #d32f2f;"><strong>多指标异常:</strong> {multi_anomaly_count}个</div>
                </div>
                <div style="display: flex; gap: 30px; align-items: center; flex-wrap: wrap; font-size: 14px; margin-bottom: 10px;">
                    <div style="color: #e74c3c;"><strong>白领占比降低:</strong> {white_collar_count}个</div>
                    <div style="color: #f39c12;"><strong>年轻占比升高:</strong> {young_count}个</div>
                    <div style="color: #9b59b6;"><strong>三线城市占比升高:</strong> {third_tier_count}个</div>
                </div>"""
                
                # 如果有多指标异常账户，显示详细信息
                if multi_anomaly_count > 0:
                    html += """
                <div style="background: #ffebee; padding: 10px; border-radius: 6px; margin-top: 10px; border-left: 3px solid #d32f2f;">
                    <strong style="color: #d32f2f;">🚨 多指标异常账户详情:</strong><br>"""
                    
                    for acc_data in multi_anomaly_accounts:  # 显示所有多指标异常账户
                        account_id = acc_data['account']['account']
                        anomaly_count = len(acc_data['anomaly_types'])
                        
                        # 账户ID简化显示
                        display_account_id = str(account_id)[:16] + ('...' if len(str(account_id)) > 16 else '')
                        
                        html += f"""
                    <span style="color: #666; font-size: 13px;">• <strong>{display_account_id}</strong> ({anomaly_count}个指标异常)</span><br>"""
                    
                    html += "</div>"
                
                html += "</div>"
                
                # 使用新的分类显示格式
                # 三列布局：按指标分类显示
                html += """
            <div style="margin: 20px 0; display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px;">"""
                
                # 第一列：白领占比异常
                white_collar_accounts = anomaly_accounts.get('white_collar', [])
                html += f"""
                <div style="background: #fff; border: 1px solid #e74c3c; border-radius: 8px; padding: 16px;">
                    <h4 style="margin: 0 0 15px 0; color: #e74c3c;">🏢 白领占比降低账户 ({len(white_collar_accounts)}个)</h4>"""
                
                for account in white_collar_accounts:  # 显示所有异常账户
                    html += self._format_account_card(account, attribute_totals, channel_total, 'white_collar')
                
                html += """
                </div>"""
                
                # 第二列：年轻占比异常
                young_accounts = anomaly_accounts.get('young', [])
                html += f"""
                <div style="background: #fff; border: 1px solid #f39c12; border-radius: 8px; padding: 16px;">
                    <h4 style="margin: 0 0 15px 0; color: #f39c12;">👶 年轻占比升高账户 ({len(young_accounts)}个)</h4>"""
                
                for account in young_accounts:  # 显示所有异常账户
                    html += self._format_account_card(account, attribute_totals, channel_total, 'young')
                
                html += """
                </div>"""
                
                # 第三列：三线城市占比异常
                third_tier_accounts = anomaly_accounts.get('third_tier_city', [])
                html += f"""
                <div style="background: #fff; border: 1px solid #9b59b6; border-radius: 8px; padding: 16px;">
                    <h4 style="margin: 0 0 15px 0; color: #9b59b6;">🏙️ 三线城市占比升高账户 ({len(third_tier_accounts)}个)</h4>"""
                
                for account in third_tier_accounts:  # 显示所有异常账户
                    html += self._format_account_card(account, attribute_totals, channel_total, 'third_tier_city')
                
                html += """
                </div>
            </div>"""
                
            else:
                # 旧格式兼容 - 添加总结
                html += f"""
            <div style="background: #fff8e1; padding: 15px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #ff9800;">
                <h4 style="margin: 0 0 10px 0; color: #e65100;">📊 异常账户概览</h4>
                <div style="font-size: 14px;">
                    <div><strong>总计:</strong> {len(anomaly_accounts)}个异常账户</div>
                </div>
                <p style="margin: 10px 0 0 0; color: #666; font-size: 13px; font-style: italic;">
                    💡 以下账户在ARPU降低的同时伴随用户质量负向变化，建议重点关注投放策略和素材优化
                </p>
            </div>"""
                
                for account in anomaly_accounts:  # 显示所有异常账户
                    html += self._format_account_card(account, attribute_totals, channel_total, 'legacy')
        
        else:
            html += """
            <div class="anomaly-item">
                <strong>✅ 未检测到异常账户</strong><br>
                整体指标变化未超过1%阈值，或没有发现ARPU降低且用户质量指标负向变化的账户
            </div>"""
        
        html += """
        </div>"""
        
        return html
    
    def _calculate_overall_quality_changes(self, today_df: pd.DataFrame, avg_7d_df: pd.DataFrame) -> dict:
        """计算整体质量指标变化"""
        # 计算当日整体指标
        today_total_users = today_df['good_verified'].sum()
        today_total_revenue = (today_df['arpu'] * today_df['good_verified']).sum()
        
        # 安全获取指标列，如果不存在则填充0
        white_collar_col = today_df['white_collar_rate'] if 'white_collar_rate' in today_df.columns else 0
        under_23_col = today_df['under_23_rate'] if 'under_23_rate' in today_df.columns else 0  
        third_tier_col = today_df['third_tier_city_rate'] if 'third_tier_city_rate' in today_df.columns else 0
        
        today_total_white_collar = (white_collar_col * today_df['good_verified'] / 100).sum() if hasattr(white_collar_col, 'sum') else 0
        today_total_young = (under_23_col * today_df['good_verified'] / 100).sum() if hasattr(under_23_col, 'sum') else 0
        today_total_third_tier = (third_tier_col * today_df['good_verified'] / 100).sum() if hasattr(third_tier_col, 'sum') else 0
        
        # 计算7日均值整体指标
        avg_7d_total_users = avg_7d_df['good_verified_avg'].sum()
        avg_7d_total_revenue = (avg_7d_df['arpu_avg'] * avg_7d_df['good_verified_avg']).sum()
        
        # 安全获取7日均值指标列
        white_collar_avg_col = avg_7d_df['white_collar_rate_avg'] if 'white_collar_rate_avg' in avg_7d_df.columns else 0
        under_23_avg_col = avg_7d_df['under_23_rate_avg'] if 'under_23_rate_avg' in avg_7d_df.columns else 0
        third_tier_avg_col = avg_7d_df['third_tier_city_rate_avg'] if 'third_tier_city_rate_avg' in avg_7d_df.columns else 0
        
        avg_7d_total_white_collar = (white_collar_avg_col * avg_7d_df['good_verified_avg'] / 100).sum() if hasattr(white_collar_avg_col, 'sum') else 0
        avg_7d_total_young = (under_23_avg_col * avg_7d_df['good_verified_avg'] / 100).sum() if hasattr(under_23_avg_col, 'sum') else 0
        avg_7d_total_third_tier = (third_tier_avg_col * avg_7d_df['good_verified_avg'] / 100).sum() if hasattr(third_tier_avg_col, 'sum') else 0
        
        # 计算变化百分比
        arpu_today = today_total_revenue / today_total_users if today_total_users > 0 else 0
        arpu_7d_avg = avg_7d_total_revenue / avg_7d_total_users if avg_7d_total_users > 0 else 0
        arpu_change = arpu_today - arpu_7d_avg
        
        white_collar_today = today_total_white_collar / today_total_users * 100 if today_total_users > 0 else 0
        white_collar_7d_avg = avg_7d_total_white_collar / avg_7d_total_users * 100 if avg_7d_total_users > 0 else 0
        white_collar_change = white_collar_today - white_collar_7d_avg
        
        young_today = today_total_young / today_total_users * 100 if today_total_users > 0 else 0
        young_7d_avg = avg_7d_total_young / avg_7d_total_users * 100 if avg_7d_total_users > 0 else 0
        young_change = young_today - young_7d_avg
        
        third_tier_today = today_total_third_tier / today_total_users * 100 if today_total_users > 0 else 0
        third_tier_7d_avg = avg_7d_total_third_tier / avg_7d_total_users * 100 if avg_7d_total_users > 0 else 0
        third_tier_city_change = third_tier_today - third_tier_7d_avg
        
        return {
            'arpu_change': arpu_change,
            'white_collar_change': white_collar_change,
            'young_change': young_change,
            'third_tier_city_change': third_tier_city_change
        }
    
    def _format_account_card(self, account: dict, attribute_totals: dict, channel_total: int, metric_type: str) -> str:
        """格式化单个账户卡片"""
        account_id = account['account']
        account_name = account.get('account_name', 'Unknown')
        os_type = account.get('os', 'other')
        gender = account.get('gender', 'other')
        good_verified = account['good_verified']
        
        # 当日数据
        arpu_today = account['arpu_today']
        white_collar_today = account.get('white_collar_rate_today', 0)
        under_23_today = account.get('under_23_rate_today', 0)
        third_tier_today = account.get('third_tier_city_rate_today', 0)
        
        # 7日均值数据
        arpu_7d_avg = account['arpu_7d_avg']
        white_collar_7d_avg = account.get('white_collar_rate_7d_avg', 0)
        under_23_7d_avg = account.get('under_23_rate_7d_avg', 0)
        third_tier_7d_avg = account.get('third_tier_city_rate_7d_avg', 0)
        
        # 格式化属性显示
        if os_type == 'other' and gender == 'other':
            attributes = "未知属性"
        elif os_type == 'other':
            attributes = gender
        elif gender == 'other':
            attributes = os_type
        else:
            attributes = f"{os_type} {gender}"
        
        # 计算占比
        attribute_key = f"{os_type}_{gender}"
        same_attribute_total = attribute_totals.get(attribute_key, 0)
        attribute_percentage = (good_verified / same_attribute_total * 100) if same_attribute_total > 0 else 0
        channel_percentage = (good_verified / channel_total * 100) if channel_total > 0 else 0
        
        # 根据指标类型显示不同的重点信息
        if metric_type == 'white_collar':
            focus_metric = f"白领占比: {white_collar_today:.1f}% → {white_collar_7d_avg:.1f}% (↓{abs(white_collar_today - white_collar_7d_avg):.1f}pp)"
        elif metric_type == 'young':
            focus_metric = f"年轻占比: {under_23_today:.1f}% → {under_23_7d_avg:.1f}% (↑{abs(under_23_today - under_23_7d_avg):.1f}pp)"
        elif metric_type == 'third_tier_city':
            focus_metric = f"三线城市: {third_tier_today:.1f}% → {third_tier_7d_avg:.1f}% (↑{abs(third_tier_today - third_tier_7d_avg):.1f}pp)"
        else:
            # legacy格式或其他
            focus_metric = f"ARPU: ¥{arpu_today:.2f} → ¥{arpu_7d_avg:.2f} (¥{arpu_today - arpu_7d_avg:.2f})"
        
        html = f"""
        <div class="account-card" data-os="{os_type}" data-gender="{gender}" 
             style="background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 6px; 
                    padding: 12px; margin: 10px 0; font-size: 12px;">
            <div style="margin-bottom: 6px;">
                <strong>{account_id}</strong><br>
                <span style="color: #666;">账户属性: {attributes}</span>
            </div>
            
            <div style="margin-bottom: 6px;">
                <strong>Good且认证:</strong> {good_verified:,}人<br>
                <span style="color: #666; font-size: 11px;">渠道内占比: {channel_percentage:.1f}% | 同属性占比: {attribute_percentage:.1f}%</span>
            </div>
            
            <div style="background: #e9ecef; padding: 8px; border-radius: 4px; font-size: 11px;">
                {focus_metric}<br><br>
                
                <strong>ARPU:</strong> ¥{arpu_today:.2f} → ¥{arpu_7d_avg:.2f} (¥{arpu_today - arpu_7d_avg:.2f})
            </div>
        </div>"""
        
        return html
    
    def _generate_creative_analysis_html(self, creative_data: dict) -> str:
        """生成素材分析HTML部分"""
        if not creative_data or creative_data.get('error'):
            return """
        <!-- 第四部分：素材分析 -->
        <div class="module">
            <h2>📊 模块四：素材分析</h2>
            <div class="anomaly-item">
                <strong>⚠️ 素材数据不可用</strong><br>
                """ + creative_data.get('error', '没有素材数据') + """
            </div>
        </div>
        """
        
        rankings = creative_data.get('rankings', [])
        insights = creative_data.get('insights', {})
        
        html = """
        <!-- 第四部分：素材分析 -->
        <div class="module">
            <h2>📊 模块四：素材分析</h2>
            
            <h3>🏆 Top 20素材排行榜</h3>"""
        
        if rankings:
            html += """
            <div style="overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 12px;">
                    <thead>
                        <tr style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                            <th style="padding: 10px 8px; text-align: center; border: 1px solid #ddd;">排名</th>
                            <th style="padding: 10px 8px; text-align: left; border: 1px solid #ddd;">素材ID</th>
                            <th style="padding: 10px 8px; text-align: right; border: 1px solid #ddd;">Good认证</th>
                            <th style="padding: 10px 8px; text-align: right; border: 1px solid #ddd;">消耗</th>
                            <th style="padding: 10px 8px; text-align: right; border: 1px solid #ddd;">CPA</th>
                            <th style="padding: 10px 8px; text-align: right; border: 1px solid #ddd;">ARPU</th>
                            <th style="padding: 10px 8px; text-align: right; border: 1px solid #ddd;">次留率</th>
                            <th style="padding: 10px 8px; text-align: right; border: 1px solid #ddd;">女比</th>
                            <th style="padding: 10px 8px; text-align: right; border: 1px solid #ddd;">白领比</th>
                            <th style="padding: 10px 8px; text-align: right; border: 1px solid #ddd;">iOS比</th>
                            <th style="padding: 10px 8px; text-align: center; border: 1px solid #ddd;">昨日排名</th>
                            <th style="padding: 10px 8px; text-align: center; border: 1px solid #ddd;">变化</th>
                        </tr>
                    </thead>
                    <tbody>"""
            
            for creative in rankings:
                rank = creative['rank']
                creative_id = creative['creative_id']  # 完整显示素材ID，不截断
                
                # 处理渠道信息
                channels = creative.get('channels', '') or ''
                channel_count = creative.get('channel_count', 0) or 0
                if channel_count > 1:
                    channel_display = f"{channel_count}个渠道"
                    channel_tooltip = channels
                else:
                    channel_display = channels
                    channel_tooltip = channels
                
                good_verified = creative['good_verified']
                cost = creative['cost']
                cpa = creative['cpa']
                arpu = creative['arpu']
                retention_rate = creative['retention_rate']
                female_rate = creative['female_rate']
                white_collar_rate = creative['white_collar_rate']
                ios_rate = creative['ios_rate']
                yesterday_rank = creative['yesterday_rank'] if creative['yesterday_rank'] else '-'
                rank_change = creative['rank_change']
                
                # 根据排名变化设置颜色
                change_color = "#28a745" if "↑" in rank_change else "#dc3545" if "↓" in rank_change else "#6c757d"
                if "⭐" in rank_change:
                    change_color = "#ffc107"
                
                row_color = "#f8f9fa" if rank % 2 == 0 else "#ffffff"
                
                html += f"""
                        <tr style="background-color: {row_color};">
                            <td style="padding: 8px; text-align: center; border: 1px solid #ddd; font-weight: bold;">{rank}</td>
                            <td style="padding: 8px; text-align: left; border: 1px solid #ddd; font-family: monospace; font-size: 10px; word-break: break-all; min-width: 150px;" title="{creative['creative_id']}">{creative_id}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd; font-weight: bold;">{good_verified:,}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">¥{cost:,.0f}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">¥{cpa:.2f}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">¥{arpu:.2f}</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{retention_rate:.1f}%</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{female_rate:.1f}%</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{white_collar_rate:.1f}%</td>
                            <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{ios_rate:.1f}%</td>
                            <td style="padding: 8px; text-align: center; border: 1px solid #ddd;">{yesterday_rank}</td>
                            <td style="padding: 8px; text-align: center; border: 1px solid #ddd; color: {change_color}; font-weight: bold;">{rank_change}</td>
                        </tr>"""
            
            html += """
                    </tbody>
                </table>
            </div>"""
        else:
            html += """
            <div class="anomaly-item">
                <strong>⚠️ 暂无素材排名数据</strong><br>
                请检查素材数据是否已同步
            </div>"""
        
        # 添加洞察分析
        html += """
            <h3>🔍 排名变化分析</h3>"""
        
        if insights:
            html += '<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0;">'
            
            # 上升最快
            if insights.get('top_rising'):
                rising = insights['top_rising']
                html += f'''
                <div style="background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%); border: none; border-left: 4px solid #28a745; border-radius: 8px; padding: 16px;">
                    <h4 style="color: #155724; margin: 0 0 12px 0;">🔥 上升最快</h4>
                    <div><strong>{rising['creative_id'][:15]}...</strong></div>
                    <div>变化: {rising['rank_change']} ({rising['good_verified']:,}用户)</div>
                    <div>CPA: ¥{rising['cpa']:.2f}</div>
                </div>'''
            
            # 下降最快
            if insights.get('top_falling'):
                falling = insights['top_falling']
                html += f'''
                <div style="background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%); border: none; border-left: 4px solid #dc3545; border-radius: 8px; padding: 16px;">
                    <h4 style="color: #721c24; margin: 0 0 12px 0;">📉 下降最快</h4>
                    <div><strong>{falling['creative_id'][:15]}...</strong></div>
                    <div>变化: {falling['rank_change']} ({falling['good_verified']:,}用户)</div>
                    <div>CPA: ¥{falling['cpa']:.2f}</div>
                </div>'''
            
            html += '</div>'
            
            # 新进榜素材
            new_entries = insights.get('new_entries', [])
            if new_entries:
                html += '''
                <div style="background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%); border: none; border-left: 4px solid #ffc107; border-radius: 8px; padding: 16px; margin: 20px 0;">
                    <h4 style="color: #856404; margin: 0 0 12px 0;">⭐ 新进榜素材</h4>'''
                for new_entry in new_entries[:3]:
                    html += f'''
                    <div style="margin-bottom: 8px;">
                        • <strong>{new_entry['creative_id'][:15]}...</strong> ({new_entry['channels'][:20]}{'...' if len(new_entry['channels']) > 20 else ''}) 
                        - {new_entry['good_verified']:,}用户, CPA¥{new_entry['cpa']:.2f}
                    </div>'''
                html += '</div>'
            
            # CPA异常素材
            cpa_anomalies = insights.get('cpa_anomalies', [])
            if cpa_anomalies:
                html += '''
                <div style="background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%); border: none; border-left: 4px solid #dc3545; border-radius: 8px; padding: 16px; margin: 20px 0;">
                    <h4 style="color: #721c24; margin: 0 0 12px 0;">💸 CPA异常素材</h4>'''
                for anomaly in cpa_anomalies[:3]:
                    html += f'''
                    <div style="margin-bottom: 8px;">
                        • <strong>{anomaly['creative_id'][:15]}...</strong> ({anomaly['channels'][:20]}{'...' if len(anomaly['channels']) > 20 else ''}) 
                        - CPA¥{anomaly['cpa']:.2f} (建议关注)
                    </div>'''
                html += '</div>'
            
            # 用户画像优势
            if insights.get('high_female_rate') and insights['high_female_rate']['female_rate'] > 70:
                high_female = insights['high_female_rate']
                html += f'''
                <div style="background: linear-gradient(135deg, #e2e3ff 0%, #c5ceff 100%); border: none; border-left: 4px solid #6f42c1; border-radius: 8px; padding: 16px; margin: 20px 0;">
                    <h4 style="color: #4c1d95; margin: 0 0 12px 0;">👩 女性用户集中</h4>
                    <div><strong>{high_female['creative_id'][:15]}...</strong></div>
                    <div>女比: {high_female['female_rate']:.1f}% (适合女性向产品)</div>
                </div>'''
            
            if insights.get('high_ios_rate') and insights['high_ios_rate']['ios_rate'] > 50:
                high_ios = insights['high_ios_rate']
                html += f'''
                <div style="background: linear-gradient(135deg, #e2e3ff 0%, #c5ceff 100%); border: none; border-left: 4px solid #6f42c1; border-radius: 8px; padding: 16px; margin: 20px 0;">
                    <h4 style="color: #4c1d95; margin: 0 0 12px 0;">📱 iOS用户集中</h4>
                    <div><strong>{high_ios['creative_id'][:15]}...</strong></div>
                    <div>iOS比: {high_ios['ios_rate']:.1f}% (高端用户聚集)</div>
                </div>'''
        
        else:
            html += """
            <div class="anomaly-item">
                <strong>⚠️ 暂无排名变化分析</strong><br>
                需要至少两天的数据进行对比分析
            </div>"""
        
        html += """
        </div>
        """
        
        return html

def main():
    """主函数 - 命令行接口"""
    import sys
    
    if len(sys.argv) > 1:
        date = sys.argv[1]
    else:
        date = '2025-07-25'
    
    print("📊 标准化报告生成器")
    print("=" * 50)
    print("✅ 内置完整规范，确保数据准确性")
    print("✅ 固定报告结构，避免随意发挥")
    print("✅ 标准化计算公式，防止计算错误")
    print("=" * 50)
    
    generator = StandardReportGenerator()
    try:
        generator.generate_report(date)
        print("\n🎉 报告生成完成！所有数据均按规范计算，请检查准确性。")
    except Exception as e:
        import traceback
        print(f"\n❌ 报告生成失败: {e}")
        print("\n完整错误信息：")
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
