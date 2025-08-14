#!/usr/bin/env python3
"""
数据字段配置文件
定义数据库字段的业务含义、计算逻辑和验证规则
便于随时修改和确保数据准确性
"""

from typing import Dict, List, Any, Callable
import pandas as pd

class DataFieldConfig:
    """数据字段配置类"""
    
    def __init__(self):
        """初始化配置"""
        self.field_mappings = self._define_field_mappings()
        self.business_rules = self._define_business_rules()
        self.validation_rules = self._define_validation_rules()
    
    def _define_field_mappings(self) -> Dict[str, Dict[str, Any]]:
        """定义字段映射和含义"""
        
        return {
            # 基础用户字段
            'total_users': {
                'field': 'newuser',
                'aggregation': 'SUM',
                'description': '总新用户数',
                'display_name': '总用户数',
                'format': '{:,}',
                'unit': '人'
            },
            
            'good_users': {
                'field': 'newuser',
                'filter': 'status = "good"',
                'aggregation': 'SUM',
                'description': '状态为Good的用户数',
                'display_name': 'Good用户数',
                'format': '{:,}',
                'unit': '人'
            },
            
            'verified_users': {
                'field': 'newuser', 
                'filter': 'verification_status = "verified"',
                'aggregation': 'SUM',
                'description': '已认证用户数',
                'display_name': '认证用户数',
                'format': '{:,}',
                'unit': '人'
            },
            
            'quality_users': {
                'field': 'newuser',
                'filter': 'status = "good" AND verification_status = "verified"',
                'aggregation': 'SUM', 
                'description': 'Good且已认证的优质用户数',
                'display_name': 'Good且认证用户数',
                'format': '{:,}',
                'unit': '人'
            },
            
            # 收入字段
            'revenue_after_tax': {
                'field': 'zizhu_revenue_1_aftertax',
                'aggregation': 'SUM',
                'description': '税后收入',
                'display_name': '税后总收入',
                'format': '¥{:.2f}',
                'unit': '元'
            },
            
            'paying_users': {
                'field': 'newuser',
                'filter': 'status = "good" AND verification_status = "verified" AND zizhu_revenue_1_aftertax > 0',
                'aggregation': 'SUM',
                'description': '有付费的优质用户数',
                'display_name': '付费用户数', 
                'format': '{:,}',
                'unit': '人'
            },
            
            # 留存字段 - 基于Good且认证用户  
            'retained_users': {
                'field': 'is_returned_1_day',
                'filter': 'status = "good" AND verification_status = "verified"',
                'aggregation': 'SUM',
                'description': 'Good且认证用户的次日留存数',
                'display_name': '次留用户数(Good且认证)',
                'format': '{:,.1f}',
                'unit': '人'
            },
            
            # 用户质量字段 - 基于Good且认证用户
            'female_users': {
                'field': 'newuser',
                'filter': 'gender = "female" AND status = "good" AND verification_status = "verified"',
                'aggregation': 'SUM',
                'description': 'Good且认证的女性用户数',
                'display_name': '女性用户(Good且认证)',
                'format': '{:,}',
                'unit': '人'
            },
            
            'male_users': {
                'field': 'newuser', 
                'filter': 'gender = "male" AND status = "good" AND verification_status = "verified"',
                'aggregation': 'SUM',
                'description': 'Good且认证的男性用户数',
                'display_name': '男性用户(Good且认证)',
                'format': '{:,}',
                'unit': '人'
            },
            
            # 年龄字段 - 基于Good且认证用户
            'young_users': {
                'field': 'newuser',
                'filter': 'age_group IN ("20-", "20~23") AND status = "good" AND verification_status = "verified"',
                'aggregation': 'SUM',
                'description': 'Good且认证的年轻用户数(24岁以下)',
                'display_name': '年轻用户(Good且认证)',
                'format': '{:,}',
                'unit': '人'
            },
            
            # 城市等级字段 - 基于Good且认证用户
            'tier1_users': {
                'field': 'newuser',
                'filter': 'dengji IN ("超一线", "一线") AND status = "good" AND verification_status = "verified"',
                'aggregation': 'SUM', 
                'description': 'Good且认证的一线城市用户数',
                'display_name': '一线城市用户(Good且认证)',
                'format': '{:,}',
                'unit': '人'
            },
            
            'tier2_users': {
                'field': 'newuser',
                'filter': 'dengji = "二线" AND status = "good" AND verification_status = "verified"',
                'aggregation': 'SUM',
                'description': 'Good且认证的二线城市用户数', 
                'display_name': '二线城市用户(Good且认证)',
                'format': '{:,}',
                'unit': '人'
            },
            
            'high_tier_users': {
                'field': 'newuser',
                'filter': 'dengji IN ("超一线", "一线", "二线") AND status = "good" AND verification_status = "verified"',
                'aggregation': 'SUM',
                'description': 'Good且认证的高线城市用户数(一二线)',
                'display_name': '高线城市用户(Good且认证)',
                'format': '{:,}',
                'unit': '人'
            }
        }
    
    def _define_business_rules(self) -> Dict[str, Dict[str, Any]]:
        """定义业务指标计算规则"""
        
        return {
            # 主要KPI指标
            'arpu_after_tax': {
                'numerator': 'revenue_after_tax',
                'denominator': 'quality_users',
                'description': 'ARPU(税后) = 税后总收入 / Good且认证用户数',
                'display_name': 'ARPU（税后）',
                'format': '¥{:.2f}',
                'unit': '元',
                'min_denominator': 1  # 避免除0
            },
            
            'cpa': {
                'numerator': 'total_cost',
                'denominator': 'quality_users', 
                'description': 'CPA = 总成本 / Good且认证用户数',
                'display_name': 'CPA',
                'format': '¥{:.2f}',
                'unit': '元',
                'min_denominator': 1
            },
            
            'retention_rate': {
                'numerator': 'retained_users',
                'denominator': 'quality_users',
                'description': '次留率 = Good且认证用户次日留存数 / Good且认证用户数',
                'display_name': '次留率(Good且认证)',
                'format': '{:.1f}%',
                'unit': '%',
                'multiplier': 100,
                'min_denominator': 1
            },
            
            # 用户质量指标 - 基于Good且认证用户
            'female_ratio': {
                'numerator': 'female_users',
                'denominator': 'quality_users',
                'description': '女性占比 = Good且认证女性用户数 / Good且认证用户数',
                'display_name': '女性占比(Good且认证)',
                'format': '{:.1f}%',
                'unit': '%',
                'multiplier': 100,
                'min_denominator': 1
            },
            
            'young_ratio': {
                'numerator': 'young_users',
                'denominator': 'quality_users',
                'description': '年轻占比 = Good且认证年轻用户数(24岁以下) / Good且认证用户数',
                'display_name': '年轻占比(Good且认证)',
                'format': '{:.1f}%', 
                'unit': '%',
                'multiplier': 100,
                'min_denominator': 1
            },
            
            'high_tier_ratio': {
                'numerator': 'high_tier_users',
                'denominator': 'quality_users',
                'description': '高线城市占比 = Good且认证高线城市用户数 / Good且认证用户数',
                'display_name': '高线城市占比(Good且认证)',
                'format': '{:.1f}%',
                'unit': '%',
                'multiplier': 100,
                'min_denominator': 1
            },
            
            # 转化指标
            'good_rate': {
                'numerator': 'good_users',
                'denominator': 'total_users',
                'description': 'Good率 = Good用户数 / 总用户数',
                'display_name': 'Good率',
                'format': '{:.1f}%',
                'unit': '%',
                'multiplier': 100,
                'min_denominator': 1
            },
            
            'verified_rate': {
                'numerator': 'verified_users',
                'denominator': 'total_users',
                'description': '认证率 = 认证用户数 / 总用户数',
                'display_name': '认证率',
                'format': '{:.1f}%',
                'unit': '%', 
                'multiplier': 100,
                'min_denominator': 1
            },
            
            'quality_rate': {
                'numerator': 'quality_users',
                'denominator': 'total_users',
                'description': 'Good且认证率 = Good且认证用户数 / 总用户数',
                'display_name': 'Good且认证率',
                'format': '{:.1f}%',
                'unit': '%',
                'multiplier': 100,
                'min_denominator': 1
            },
            
            'conversion_rate': {
                'numerator': 'paying_users',
                'denominator': 'quality_users',
                'description': '付费转化率 = 付费用户数 / Good且认证用户数',
                'display_name': '付费转化率',
                'format': '{:.1f}%',
                'unit': '%',
                'multiplier': 100,
                'min_denominator': 1
            }
        }
    
    def _define_validation_rules(self) -> Dict[str, Dict[str, Any]]:
        """定义数据验证规则"""
        
        return {
            'range_checks': {
                'arpu_after_tax': {'min': 0, 'max': 1000, 'warning_min': 1, 'warning_max': 50},
                'cpa': {'min': 0, 'max': 500, 'warning_min': 5, 'warning_max': 100},
                'retention_rate': {'min': 0, 'max': 100, 'warning_min': 10, 'warning_max': 60},
                'female_ratio': {'min': 0, 'max': 100, 'warning_min': 20, 'warning_max': 80},
                'young_ratio': {'min': 0, 'max': 100, 'warning_min': 30, 'warning_max': 80},
                'good_rate': {'min': 0, 'max': 100, 'warning_min': 30, 'warning_max': 80},
                'verified_rate': {'min': 0, 'max': 100, 'warning_min': 20, 'warning_max': 80},
                'quality_rate': {'min': 0, 'max': 100, 'warning_min': 20, 'warning_max': 70}
            },
            
            'logic_checks': [
                {
                    'name': 'quality_users_vs_good_users',
                    'rule': lambda data: data.get('quality_users', 0) <= data.get('good_users', 0),
                    'message': 'Good且认证用户数不应超过Good用户数'
                },
                {
                    'name': 'quality_users_vs_verified_users', 
                    'rule': lambda data: data.get('quality_users', 0) <= data.get('verified_users', 0),
                    'message': 'Good且认证用户数不应超过认证用户数'
                },
                {
                    'name': 'paying_users_vs_quality_users',
                    'rule': lambda data: data.get('paying_users', 0) <= data.get('quality_users', 0),
                    'message': '付费用户数不应超过Good且认证用户数'
                },
                {
                    'name': 'retained_users_vs_total_users',
                    'rule': lambda data: data.get('retained_users', 0) <= data.get('total_users', 0),
                    'message': '留存用户数不应超过总用户数'
                },
                {
                    'name': 'gender_sum_check',
                    'rule': lambda data: (data.get('female_users', 0) + data.get('male_users', 0)) <= data.get('total_users', 0),
                    'message': '男女用户数之和不应超过总用户数'
                }
            ]
        }
    
    def generate_sql_query(self, date: str, fields: List[str], table: str = 'cpz_qs_newuser_channel_i_d') -> str:
        """根据配置生成SQL查询"""
        
        select_parts = []
        
        for field in fields:
            if field in self.field_mappings:
                config = self.field_mappings[field]
                field_expr = config['field']
                agg = config['aggregation']
                
                if 'filter' in config:
                    field_expr = f"CASE WHEN {config['filter']} THEN {field_expr} ELSE 0 END"
                
                select_parts.append(f"{agg}({field_expr}) as {field}")
        
        # 添加总成本查询（来自成本表）
        if 'total_cost' in fields:
            cost_query = f"(SELECT COALESCE(SUM(cash_cost), 0) FROM dwd_ttx_market_cash_cost_i_d WHERE dt = '{date}') as total_cost"
            select_parts.append(cost_query)
        
        query = f"""
        SELECT 
            '{date}' as dt,
            {','.join(select_parts)}
        FROM {table}
        WHERE dt = '{date}'
        """
        
        return query
    
    def calculate_business_metrics(self, raw_data: Dict[str, float]) -> Dict[str, float]:
        """根据业务规则计算衍生指标"""
        
        calculated_metrics = raw_data.copy()
        
        for metric_name, rule in self.business_rules.items():
            numerator_value = raw_data.get(rule['numerator'], 0)
            denominator_value = raw_data.get(rule['denominator'], 0)
            
            if denominator_value >= rule.get('min_denominator', 1):
                result = numerator_value / denominator_value
                if 'multiplier' in rule:
                    result *= rule['multiplier']
                calculated_metrics[metric_name] = result
            else:
                calculated_metrics[metric_name] = 0
        
        return calculated_metrics
    
    def validate_data(self, data: Dict[str, float]) -> Dict[str, List[str]]:
        """验证数据合理性"""
        
        issues = {
            'errors': [],
            'warnings': []
        }
        
        # 范围检查
        for metric, limits in self.validation_rules['range_checks'].items():
            if metric in data:
                value = data[metric]
                
                if value < limits['min'] or value > limits['max']:
                    issues['errors'].append(f"{metric} 值 {value} 超出合理范围 [{limits['min']}, {limits['max']}]")
                elif value < limits.get('warning_min', limits['min']) or value > limits.get('warning_max', limits['max']):
                    issues['warnings'].append(f"{metric} 值 {value} 可能异常 (建议范围: [{limits['warning_min']}, {limits['warning_max']}])")
        
        # 逻辑检查
        for check in self.validation_rules['logic_checks']:
            if not check['rule'](data):
                issues['errors'].append(f"逻辑错误: {check['message']}")
        
        return issues
    
    def format_value(self, metric_name: str, value: float) -> str:
        """格式化显示值"""
        
        if metric_name in self.field_mappings:
            format_str = self.field_mappings[metric_name]['format']
        elif metric_name in self.business_rules:
            format_str = self.business_rules[metric_name]['format']
        else:
            format_str = '{:.2f}'
        
        return format_str.format(value)
    
    def get_field_description(self, metric_name: str) -> str:
        """获取字段描述"""
        
        if metric_name in self.field_mappings:
            return self.field_mappings[metric_name]['description']
        elif metric_name in self.business_rules:
            return self.business_rules[metric_name]['description']
        else:
            return f"未知字段: {metric_name}"

# 创建全局配置实例
DATA_CONFIG = DataFieldConfig()