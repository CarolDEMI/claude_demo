#!/usr/bin/env python3
"""
基于模板的每日数据报告生成器
使用标准化配置生成格式统一的报告
"""
import sys
import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config.daily_report_template import (
    REPORT_CONFIG, CORE_METRICS, CHANNEL_ANALYSIS, USER_QUALITY_SEGMENTS,
    PLATFORM_TAX_CONFIG, TREND_ANALYSIS, COST_ANALYSIS, FORMATTING,
    REPORT_SECTIONS, DATA_QUALITY_RULES, REPORT_ANNOTATIONS, EXPORT_CONFIG
)

class DailyReportGenerator:
    """标准化每日报告生成器"""
    
    def __init__(self, db_path: str = "./data/data.db"):
        self.db_path = db_path
        self.report_data = {}
        
    def generate_report(self, date: str, output_format: str = 'console') -> str:
        """
        生成指定日期的标准化报告
        
        Args:
            date: 报告日期 (YYYY-MM-DD)
            output_format: 输出格式 ('console', 'json', 'csv')
        
        Returns:
            报告内容或文件路径
        """
        self.target_date = date
        self.report_data = {'date': date, 'sections': {}}
        
        # 连接数据库
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 1. 核心指标分析
            self._analyze_core_metrics(conn)
            
            # 2. 渠道质量分析
            self._analyze_channels(conn)
            
            # 3. 用户质量分布
            self._analyze_user_quality(conn)
            
            # 4. 成本ROI分析
            self._analyze_cost_roi(conn)
            
            # 5. 平台税收分析
            self._analyze_platform_tax(conn)
            
            # 6. 趋势分析
            self._analyze_trends(conn)
            
            # 7. 数据质量验证
            self._validate_data_quality()
            
        finally:
            conn.close()
        
        # 根据格式输出报告
        if output_format == 'console':
            return self._format_console_report()
        elif output_format == 'json':
            return self._export_json_report()
        elif output_format == 'csv':
            return self._export_csv_report()
        else:
            raise ValueError(f"不支持的输出格式: {output_format}")
    
    def _analyze_core_metrics(self, conn: sqlite3.Connection):
        """分析核心业务指标"""
        query = f'''
        SELECT 
            COUNT(*) as record_count,
            SUM(newuser) as total_users,
            SUM(CASE WHEN status = "good" THEN newuser ELSE 0 END) as good_users,
            SUM(CASE WHEN verification_status = "verified" THEN newuser ELSE 0 END) as verified_users,
            SUM(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END) as quality_users,
            SUM(zizhu_revenue_1) as revenue_pre_tax,
            SUM(zizhu_revenue_1_aftertax) as revenue_after_tax,
            SUM(CASE WHEN status = "good" AND verification_status = "verified" AND zizhu_revenue_1_aftertax > 0 THEN newuser ELSE 0 END) as paying_users
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt = '{self.target_date}'
        '''
        
        df = pd.read_sql_query(query, conn)
        if df.empty:
            raise ValueError(f"未找到 {self.target_date} 的数据")
        
        data = df.iloc[0].to_dict()
        
        # 计算衍生指标
        data['arpu_after_tax'] = data['revenue_after_tax'] / data['quality_users'] if data['quality_users'] > 0 else 0
        data['conversion_rate'] = data['paying_users'] / data['quality_users'] * 100 if data['quality_users'] > 0 else 0
        data['paying_arpu'] = data['revenue_after_tax'] / data['paying_users'] if data['paying_users'] > 0 else 0
        
        # 计算百分比
        data['good_rate'] = data['good_users'] / data['total_users'] * 100 if data['total_users'] > 0 else 0
        data['verified_rate'] = data['verified_users'] / data['total_users'] * 100 if data['total_users'] > 0 else 0
        data['quality_rate'] = data['quality_users'] / data['total_users'] * 100 if data['total_users'] > 0 else 0
        
        self.report_data['sections']['core_metrics'] = data
    
    def _analyze_channels(self, conn: sqlite3.Connection):
        """分析渠道质量"""
        query = f'''
        SELECT 
            ad_channel,
            SUM(newuser) as user_count,
            SUM(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END) as quality_users,
            ROUND(SUM(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END) * 100.0 / SUM(newuser), 1) as quality_rate,
            SUM(zizhu_revenue_1_aftertax) as revenue_after_tax,
            ROUND(SUM(zizhu_revenue_1_aftertax) / NULLIF(SUM(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END), 0), 2) as arpu_after_tax
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt = '{self.target_date}'
        GROUP BY ad_channel
        ORDER BY user_count DESC
        LIMIT {CHANNEL_ANALYSIS['top_channels_limit']}
        '''
        
        df = pd.read_sql_query(query, conn)
        self.report_data['sections']['channels'] = df.to_dict('records')
    
    def _analyze_user_quality(self, conn: sqlite3.Connection):
        """分析用户质量分布"""
        quality_data = []
        total_users = self.report_data['sections']['core_metrics']['total_users']
        total_revenue = self.report_data['sections']['core_metrics']['revenue_after_tax']
        
        for segment in USER_QUALITY_SEGMENTS:
            query = f'''
            SELECT 
                SUM(newuser) as user_count,
                SUM(zizhu_revenue_1_aftertax) as revenue
            FROM cpz_qs_newuser_channel_i_d
            WHERE dt = '{self.target_date}' AND ({segment['condition']})
            '''
            
            df = pd.read_sql_query(query, conn)
            if not df.empty and df.iloc[0]['user_count'] is not None:
                users = df.iloc[0]['user_count']
                revenue = df.iloc[0]['revenue'] or 0
                
                quality_data.append({
                    'segment': segment['name'],
                    'user_count': users,
                    'user_percentage': users / total_users * 100 if total_users > 0 else 0,
                    'revenue': revenue,
                    'revenue_percentage': revenue / total_revenue * 100 if total_revenue > 0 else 0,
                    'description': segment['description']
                })
        
        self.report_data['sections']['user_quality'] = quality_data
    
    def _analyze_cost_roi(self, conn: sqlite3.Connection):
        """分析成本和ROI"""
        cost_query = f'''
        SELECT 
            COUNT(*) as cost_records,
            SUM(cash_cost) as total_cost,
            AVG(cash_cost) as avg_cost,
            MIN(cash_cost) as min_cost,
            MAX(cash_cost) as max_cost
        FROM {COST_ANALYSIS['cost_table']}
        WHERE dt = '{self.target_date}'
        '''
        
        df = pd.read_sql_query(cost_query, conn)
        if not df.empty and df.iloc[0]['cost_records'] > 0:
            cost_data = df.iloc[0].to_dict()
            
            # 计算ROI
            revenue_pre = self.report_data['sections']['core_metrics']['revenue_pre_tax']
            revenue_after = self.report_data['sections']['core_metrics']['revenue_after_tax']
            total_cost = cost_data['total_cost']
            
            cost_data['profit_pre_tax'] = revenue_pre - total_cost
            cost_data['profit_after_tax'] = revenue_after - total_cost
            cost_data['roi_pre_tax'] = (revenue_pre - total_cost) / total_cost * 100 if total_cost > 0 else 0
            cost_data['roi_after_tax'] = (revenue_after - total_cost) / total_cost * 100 if total_cost > 0 else 0
            
            self.report_data['sections']['cost_roi'] = cost_data
        else:
            self.report_data['sections']['cost_roi'] = None
    
    def _analyze_platform_tax(self, conn: sqlite3.Connection):
        """分析平台税收影响"""
        query = f'''
        SELECT 
            os_type,
            SUM(newuser) as user_count,
            SUM(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END) as quality_users,
            SUM(zizhu_revenue_1) as revenue_pre_tax,
            SUM(zizhu_revenue_1_aftertax) as revenue_after_tax,
            ROUND((SUM(zizhu_revenue_1) - SUM(zizhu_revenue_1_aftertax)) / NULLIF(SUM(zizhu_revenue_1), 0) * 100, 1) as tax_rate,
            ROUND(SUM(zizhu_revenue_1_aftertax) / NULLIF(SUM(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END), 0), 2) as arpu_after_tax
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt = '{self.target_date}' AND zizhu_revenue_1 > 0
        GROUP BY os_type
        ORDER BY revenue_after_tax DESC
        '''
        
        df = pd.read_sql_query(query, conn)
        self.report_data['sections']['platform_tax'] = df.to_dict('records')
    
    def _analyze_trends(self, conn: sqlite3.Connection):
        """分析日环比趋势"""
        # 获取对比日期范围
        target_date = datetime.strptime(self.target_date, '%Y-%m-%d')
        start_date = (target_date - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')
        
        query = f'''
        SELECT 
            dt,
            SUM(newuser) as total_users,
            SUM(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END) as quality_users,
            SUM(zizhu_revenue_1_aftertax) as revenue_after_tax,
            ROUND(SUM(zizhu_revenue_1_aftertax) / NULLIF(SUM(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END), 0), 2) as arpu_after_tax
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt IN ('{start_date}', '{self.target_date}', '{end_date}')
        GROUP BY dt
        ORDER BY dt
        '''
        
        df = pd.read_sql_query(query, conn)
        trend_data = df.to_dict('records')
        
        # 计算环比变化
        target_idx = next((i for i, item in enumerate(trend_data) if item['dt'] == self.target_date), None)
        
        if target_idx is not None:
            changes = {}
            current = trend_data[target_idx]
            
            # 与前一天对比
            if target_idx > 0:
                prev = trend_data[target_idx - 1]
                changes['vs_previous'] = {
                    'date': prev['dt'],
                    'user_change': (current['total_users'] - prev['total_users']) / prev['total_users'] * 100 if prev['total_users'] > 0 else 0,
                    'revenue_change': (current['revenue_after_tax'] - prev['revenue_after_tax']) / prev['revenue_after_tax'] * 100 if prev['revenue_after_tax'] > 0 else 0,
                    'arpu_change': (current['arpu_after_tax'] - prev['arpu_after_tax']) / prev['arpu_after_tax'] * 100 if prev['arpu_after_tax'] > 0 else 0
                }
            
            # 与后一天对比
            if target_idx < len(trend_data) - 1:
                next_day = trend_data[target_idx + 1]
                changes['vs_next'] = {
                    'date': next_day['dt'],
                    'user_change': (next_day['total_users'] - current['total_users']) / current['total_users'] * 100 if current['total_users'] > 0 else 0,
                    'revenue_change': (next_day['revenue_after_tax'] - current['revenue_after_tax']) / current['revenue_after_tax'] * 100 if current['revenue_after_tax'] > 0 else 0,
                    'arpu_change': (next_day['arpu_after_tax'] - current['arpu_after_tax']) / current['arpu_after_tax'] * 100 if current['arpu_after_tax'] > 0 else 0
                }
            
            self.report_data['sections']['trends'] = {
                'data': trend_data,
                'changes': changes
            }
        else:
            self.report_data['sections']['trends'] = None
    
    def _validate_data_quality(self):
        """验证数据质量"""
        core_data = self.report_data['sections']['core_metrics']
        warnings = []
        
        for rule_name, rule_config in DATA_QUALITY_RULES.items():
            # 这里可以实现具体的验证逻辑
            # 简化示例：检查ARPU合理性
            if rule_name == 'arpu_reasonable':
                arpu = core_data.get('arpu_after_tax', 0)
                if not (1 <= arpu <= 100):
                    warnings.append(f"ARPU异常: {arpu:.2f}元")
        
        self.report_data['data_quality'] = {
            'warnings': warnings,
            'validation_time': datetime.now().isoformat()
        }
    
    def _format_console_report(self) -> str:
        """格式化控制台报告"""
        lines = []
        width = EXPORT_CONFIG['console']['width']
        
        # 报告标题
        title = f"{REPORT_CONFIG['title']} - {self.target_date}"
        lines.append('═' * width)
        lines.append(title.center(width))
        lines.append('═' * width)
        
        # 核心指标
        core = self.report_data['sections']['core_metrics']
        lines.append('')
        lines.append('📊 核心业务指标')
        lines.append('━' * width)
        lines.append(f"📅 统计日期: {self.target_date}")
        lines.append(f"📊 数据记录: {core['record_count']:,} 条")
        lines.append(f"👥 总用户数: {core['total_users']:,} 人")
        lines.append(f"✅ Good用户: {core['good_users']:,} 人 ({core['good_rate']:.1f}%)")
        lines.append(f"🔐 认证用户: {core['verified_users']:,} 人 ({core['verified_rate']:.1f}%)")
        lines.append(f"⭐ Good且认证用户: {core['quality_users']:,} 人 ({core['quality_rate']:.1f}%)")
        lines.append(f"💰 总收入（税前）: ¥{core['revenue_pre_tax']:,.2f}")
        lines.append(f"💰 总收入（税后）: ¥{core['revenue_after_tax']:,.2f}")
        lines.append(f"📈 ARPU（税后）: ¥{core['arpu_after_tax']:.2f} 【基于Good+认证用户】")
        lines.append(f"💳 付费转化率: {core['conversion_rate']:.1f}% ({core['paying_users']:,}/{core['quality_users']:,})")
        lines.append(f"💎 付费用户ARPU（税后）: ¥{core['paying_arpu']:.2f}")
        
        # 渠道分析
        if 'channels' in self.report_data['sections']:
            lines.append('')
            lines.append('🏆 TOP10渠道质量分析')
            lines.append('━' * width)
            header = f"{'渠道':<15} {'用户数':<8} {'Good+认证':<9} {'质量率':<8} {'收入(税后)':<12} {'ARPU(税后)':<10}"
            lines.append(header)
            lines.append('─' * len(header))
            
            for channel in self.report_data['sections']['channels']:
                quality = f"{channel['quality_rate']:.1f}%"
                revenue = f"¥{channel['revenue_after_tax']:,.0f}"
                arpu = f"¥{channel['arpu_after_tax']:.2f}" if channel['arpu_after_tax'] else "¥0.00"
                line = f"{channel['ad_channel']:<15} {channel['user_count']:<8,} {channel['quality_users']:<9,} {quality:<8} {revenue:<12} {arpu:<10}"
                lines.append(line)
        
        # 报告说明
        lines.append('')
        lines.append('═' * width)
        lines.append('📋 报告说明')
        lines.append('═' * width)
        for key, value in REPORT_ANNOTATIONS.items():
            lines.append(f"✅ {value}")
        lines.append('═' * width)
        
        return '\\n'.join(lines)
    
    def _export_json_report(self) -> str:
        """导出JSON格式报告"""
        filename = f"daily_report_{self.target_date.replace('-', '')}.json"
        filepath = os.path.join(project_root, 'reports', filename)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self.report_data, f, ensure_ascii=False, indent=2, default=str)
        
        return filepath
    
    def _export_csv_report(self) -> str:
        """导出CSV格式报告"""
        filename = f"daily_report_{self.target_date.replace('-', '')}.csv"
        filepath = os.path.join(project_root, 'reports', filename)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # 将核心指标转换为DataFrame并保存
        core_df = pd.DataFrame([self.report_data['sections']['core_metrics']])
        core_df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        return filepath

def main():
    """命令行入口"""
    import argparse
    
    parser = argparse.ArgumentParser(description='生成标准化每日数据报告')
    parser.add_argument('date', help='报告日期 (YYYY-MM-DD)')
    parser.add_argument('--format', choices=['console', 'json', 'csv'], default='console', help='输出格式')
    parser.add_argument('--db', default='./data.db', help='数据库路径')
    
    args = parser.parse_args()
    
    generator = DailyReportGenerator(args.db)
    
    try:
        result = generator.generate_report(args.date, args.format)
        
        if args.format == 'console':
            print(result)
        else:
            print(f"报告已生成: {result}")
            
    except Exception as e:
        print(f"报告生成失败: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())