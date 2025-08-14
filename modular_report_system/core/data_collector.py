#!/usr/bin/env python3
"""
数据收集器
基于配置文件的统一数据收集和验证系统
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Any, Optional
import sys
import os

# 添加配置路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.data_field_config import DATA_CONFIG

class DataCollector:
    """统一数据收集器"""
    
    def __init__(self, db_path: str = './data/data.db'):
        """初始化数据收集器"""
        self.db_path = db_path
        self.config = DATA_CONFIG
    
    def collect_daily_metrics(self, date: str, metrics: Optional[List[str]] = None) -> Dict[str, Any]:
        """收集指定日期的指标数据"""
        
        if metrics is None:
            # 默认收集所有基础字段
            metrics = list(self.config.field_mappings.keys()) + ['total_cost']
        
        with sqlite3.connect(self.db_path) as conn:
            # 生成并执行SQL查询
            query = self.config.generate_sql_query(date, metrics)
            print(f"📊 执行查询: \n{query}")
            
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                raise ValueError(f"未找到 {date} 的数据")
            
            # 获取原始数据
            raw_data = df.iloc[0].to_dict()
            raw_data.pop('dt', None)  # 移除日期字段
            
            # 计算业务指标
            calculated_data = self.config.calculate_business_metrics(raw_data)
            
            # 数据验证
            validation_result = self.config.validate_data(calculated_data)
            
            return {
                'date': date,
                'raw_data': raw_data,
                'calculated_data': calculated_data,
                'validation': validation_result,
                'query_used': query
            }
    
    def collect_historical_data(self, start_date: str, end_date: str, metrics: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """收集历史数据"""
        
        if metrics is None:
            metrics = ['total_users', 'quality_users', 'revenue_after_tax', 'retained_users', 'total_cost']
        
        with sqlite3.connect(self.db_path) as conn:
            # 构建历史数据查询
            select_parts = []
            
            for field in metrics:
                if field in self.config.field_mappings:
                    config = self.config.field_mappings[field]
                    field_expr = config['field']
                    agg = config['aggregation']
                    
                    if 'filter' in config:
                        field_expr = f"CASE WHEN {config['filter']} THEN {field_expr} ELSE 0 END"
                    
                    select_parts.append(f"{agg}({field_expr}) as {field}")
            
            query = f"""
            SELECT 
                dt,
                {','.join(select_parts)}
            FROM cpz_qs_newuser_channel_i_d
            WHERE dt BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY dt
            ORDER BY dt
            """
            
            df = pd.read_sql_query(query, conn)
            
            # 获取成本数据
            cost_query = f"""
            SELECT dt, SUM(cash_cost) as total_cost
            FROM dwd_ttx_market_cash_cost_i_d
            WHERE dt BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY dt
            ORDER BY dt
            """
            
            cost_df = pd.read_sql_query(cost_query, conn)
            
            # 合并数据
            if not cost_df.empty:
                df = df.merge(cost_df, on='dt', how='left')
                df['total_cost'] = df['total_cost'].fillna(0)
            else:
                df['total_cost'] = 0
            
            # 计算业务指标
            result = []
            for _, row in df.iterrows():
                raw_data = row.to_dict()
                date = raw_data.pop('dt')
                
                calculated_data = self.config.calculate_business_metrics(raw_data)
                
                result.append({
                    'date': date,
                    'raw_data': raw_data,
                    'calculated_data': calculated_data
                })
            
            return result
    
    def get_field_info(self, field_name: str) -> Dict[str, str]:
        """获取字段信息"""
        
        info = {
            'name': field_name,
            'description': self.config.get_field_description(field_name),
            'display_name': '',
            'format': '{:.2f}',
            'unit': ''
        }
        
        if field_name in self.config.field_mappings:
            config = self.config.field_mappings[field_name]
            info.update({
                'display_name': config.get('display_name', field_name),
                'format': config.get('format', '{:.2f}'),
                'unit': config.get('unit', '')
            })
        elif field_name in self.config.business_rules:
            config = self.config.business_rules[field_name]
            info.update({
                'display_name': config.get('display_name', field_name),
                'format': config.get('format', '{:.2f}'),
                'unit': config.get('unit', '')
            })
        
        return info
    
    def format_value(self, field_name: str, value: float) -> str:
        """格式化数值"""
        return self.config.format_value(field_name, value)
    
    def print_data_summary(self, data: Dict[str, Any], show_validation: bool = True):
        """打印数据摘要"""
        
        print(f"\n📊 {data['date']} 数据摘要")
        print("=" * 60)
        
        # 打印主要指标
        main_metrics = ['total_users', 'quality_users', 'revenue_after_tax', 'arpu_after_tax', 'cpa', 'retention_rate']
        
        for metric in main_metrics:
            if metric in data['calculated_data']:
                value = data['calculated_data'][metric]
                formatted_value = self.format_value(metric, value)
                field_info = self.get_field_info(metric)
                print(f"{field_info['display_name']}: {formatted_value}")
        
        # 打印用户质量指标
        print(f"\n👥 用户质量指标")
        print("-" * 40)
        quality_metrics = ['female_ratio', 'young_ratio', 'high_tier_ratio']
        
        for metric in quality_metrics:
            if metric in data['calculated_data']:
                value = data['calculated_data'][metric]
                formatted_value = self.format_value(metric, value)
                field_info = self.get_field_info(metric)
                
                # 显示绝对数值
                if metric == 'female_ratio':
                    abs_value = data['raw_data'].get('female_users', 0)
                    total = data['raw_data'].get('total_users', 0)
                    print(f"{field_info['display_name']}: {formatted_value} ({abs_value:,}/{total:,})")
                elif metric == 'young_ratio':
                    abs_value = data['raw_data'].get('young_users', 0)
                    total = data['raw_data'].get('total_users', 0)
                    print(f"{field_info['display_name']}: {formatted_value} ({abs_value:,}/{total:,})")
                elif metric == 'high_tier_ratio':
                    abs_value = data['raw_data'].get('high_tier_users', 0)
                    total = data['raw_data'].get('total_users', 0)
                    print(f"{field_info['display_name']}: {formatted_value} ({abs_value:,}/{total:,})")
        
        # 打印转化指标
        print(f"\n🎯 注册转化指标")
        print("-" * 40)
        conversion_metrics = ['good_rate', 'verified_rate', 'quality_rate', 'conversion_rate']
        
        for metric in conversion_metrics:
            if metric in data['calculated_data']:
                value = data['calculated_data'][metric]
                formatted_value = self.format_value(metric, value)
                field_info = self.get_field_info(metric)
                print(f"{field_info['display_name']}: {formatted_value}")
        
        # 数据验证结果
        if show_validation and data['validation']:
            validation = data['validation']
            
            if validation['errors']:
                print(f"\n❌ 数据错误 ({len(validation['errors'])})")
                print("-" * 40)
                for error in validation['errors']:
                    print(f"• {error}")
            
            if validation['warnings']:
                print(f"\n⚠️  数据警告 ({len(validation['warnings'])})")
                print("-" * 40) 
                for warning in validation['warnings']:
                    print(f"• {warning}")
            
            if not validation['errors'] and not validation['warnings']:
                print(f"\n✅ 数据验证通过")
    
    def export_config_documentation(self, output_path: str = './data_config_doc.md'):
        """导出配置文档"""
        
        doc_content = """# 数据字段配置说明

## 基础字段定义

| 字段名 | 显示名 | 描述 | 格式 | 单位 |
|--------|--------|------|------|------|
"""
        
        for field_name, config in self.config.field_mappings.items():
            doc_content += f"| {field_name} | {config['display_name']} | {config['description']} | {config['format']} | {config['unit']} |\n"
        
        doc_content += """
## 业务指标定义

| 指标名 | 显示名 | 计算公式 | 格式 | 单位 |
|--------|--------|----------|------|------|
"""
        
        for metric_name, rule in self.config.business_rules.items():
            doc_content += f"| {metric_name} | {rule['display_name']} | {rule['description']} | {rule['format']} | {rule['unit']} |\n"
        
        doc_content += """
## 数据验证规则

### 范围检查
"""
        
        for metric, limits in self.config.validation_rules['range_checks'].items():
            doc_content += f"- **{metric}**: 合理范围 [{limits['min']}, {limits['max']}], 建议范围 [{limits.get('warning_min', limits['min'])}, {limits.get('warning_max', limits['max'])}]\n"
        
        doc_content += """
### 逻辑检查
"""
        
        for check in self.config.validation_rules['logic_checks']:
            doc_content += f"- **{check['name']}**: {check['message']}\n"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(doc_content)
        
        print(f"📄 配置文档已导出到: {output_path}")

if __name__ == '__main__':
    # 测试数据收集器
    collector = DataCollector()
    
    try:
        data = collector.collect_daily_metrics('2025-07-26')
        collector.print_data_summary(data)
        
        # 导出配置文档
        collector.export_config_documentation()
        
    except Exception as e:
        print(f"❌ 错误: {e}")