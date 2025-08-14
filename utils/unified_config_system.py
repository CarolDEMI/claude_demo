#!/usr/bin/env python3
"""
统一配置管理系统
确保所有组件都使用相同的配置，避免不一致问题
"""

import json
import yaml
from typing import Dict, Any, List
from datetime import datetime
import os
import sys

class UnifiedConfigManager:
    """统一配置管理器"""
    
    def __init__(self, config_file: str = './config/unified_config.yaml'):
        self.config_file = config_file
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        
        if not os.path.exists(self.config_file):
            # 创建默认配置
            default_config = self._create_default_config()
            self._save_config(default_config)
            return default_config
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"❌ 配置文件加载失败: {e}")
            return self._create_default_config()
    
    def _create_default_config(self) -> Dict[str, Any]:
        """创建默认配置"""
        
        return {
            'version': '1.0',
            'last_updated': datetime.now().isoformat(),
            
            # 数据源配置
            'data_sources': {
                'primary_table': 'cpz_qs_newuser_channel_i_d',
                'cost_table': 'dwd_ttx_market_cash_cost_i_d',
                'database_path': './data/data.db'
            },
            
            # 核心业务定义（所有组件必须遵循）
            'business_definitions': {
                # 用户分类定义
                'quality_user_condition': 'status = "good" AND verification_status = "verified"',
                'good_user_condition': 'status = "good"',
                'verified_user_condition': 'verification_status = "verified"',
                'paying_user_condition': 'status = "good" AND verification_status = "verified" AND zizhu_revenue_1_aftertax > 0',
                
                # 用户质量定义
                'female_user_condition': 'gender = "female" AND status = "good" AND verification_status = "verified"',
                'young_user_condition': 'age_group IN ("20-", "20~23") AND status = "good" AND verification_status = "verified"',
                'high_tier_city_condition': 'dengji IN ("超一线", "一线", "二线") AND status = "good" AND verification_status = "verified"',
                
                # 留存定义
                'retention_condition': 'status = "good" AND verification_status = "verified"',
                'retention_field': 'is_returned_1_day',
                
                # 年龄定义
                'young_age_definition': '24岁以下',
                'young_age_groups': ['20-', '20~23'],
                
                # 城市定义
                'high_tier_cities': ['超一线', '一线', '二线'],
                'tier1_cities': ['超一线', '一线'],
                'tier2_cities': ['二线']
            },
            
            # 计算规则（强制性）
            'calculation_rules': {
                'arpu_denominator': 'quality_users',
                'cpa_denominator': 'quality_users', 
                'retention_numerator': 'retained_quality_users',
                'retention_denominator': 'quality_users',
                'user_quality_base': 'quality_users'  # 所有用户质量指标的基准
            },
            
            # 报告模板定义
            'report_template': {
                'modules': [
                    {
                        'id': 'overview_metrics',
                        'name': '模块一：大盘指标',
                        'sections': [
                            {
                                'name': 'MAIN KPI',
                                'metrics': ['quality_users', 'cpa', 'arpu_after_tax', 'retention_rate']
                            },
                            {
                                'name': '用户质量',
                                'metrics': ['female_ratio', 'young_ratio', 'high_tier_ratio']
                            },
                            {
                                'name': '注册转化',
                                'metrics': ['good_rate', 'verified_rate', 'quality_rate']
                            }
                        ]
                    },
                    {
                        'id': 'anomaly_detection',
                        'name': '模块二：异常指标展示和渠道分析',
                        'sections': [
                            {
                                'name': '异常检测（四分位数方法）',
                                'description': '基于14天历史数据，超出1.5倍四分位距视为异常'
                            },
                            {
                                'name': '渠道影响分析',
                                'description': '各渠道对大盘异常指标的影响程度'
                            }
                        ]
                    },
                    {
                        'id': 'future_module',
                        'name': '模块三：待扩展模块',
                        'enabled': False
                    }
                ]
            },
            
            # 异常检测配置
            'anomaly_detection': {
                'method': 'quartile',
                'quartile_multiplier': 1.5,
                'history_days': 14,
                'monitored_metrics': ['quality_users', 'cpa', 'arpu_after_tax', 'retention_rate', 'good_rate', 'verified_rate', 'quality_rate']
            },
            
            # 数据验证规则
            'validation_rules': {
                'required_fields': ['total_users', 'quality_users', 'revenue_after_tax'],
                'range_checks': {
                    'arpu_after_tax': {'min': 0, 'max': 1000, 'warning_range': [1, 50]},
                    'retention_rate': {'min': 0, 'max': 100, 'warning_range': [10, 60]},
                    'female_ratio': {'min': 0, 'max': 100, 'warning_range': [20, 80]}
                },
                'logic_checks': [
                    'quality_users <= good_users',
                    'quality_users <= verified_users',
                    'paying_users <= quality_users'
                ]
            }
        }
    
    def _validate_config(self):
        """验证配置的完整性"""
        
        required_sections = ['business_definitions', 'calculation_rules', 'report_template']
        
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"配置文件缺少必需的部分: {section}")
        
        print("✅ 统一配置验证通过")
    
    def _save_config(self, config: Dict[str, Any]):
        """保存配置文件"""
        
        os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
        
        with open(self.config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
    
    def get_sql_condition(self, condition_name: str) -> str:
        """获取SQL条件"""
        return self.config['business_definitions'].get(condition_name, '')
    
    def get_calculation_rule(self, rule_name: str) -> str:
        """获取计算规则"""
        return self.config['calculation_rules'].get(rule_name, '')
    
    def get_report_template(self) -> Dict[str, Any]:
        """获取报告模板"""
        return self.config['report_template']
    
    def validate_component_compliance(self, component_name: str, component_code: str = None) -> Dict[str, Any]:
        """验证组件是否符合统一配置"""
        
        issues = []
        warnings = []
        compliant_points = []
        
        if component_code:
            # 检查是否使用了硬编码的业务逻辑
            hardcoded_patterns = [
                ('status = "good"', '应使用统一配置中的good_user_condition'),
                ('verification_status = "verified"', '应使用统一配置中的verified_user_condition'),
                ('age_group IN ("20-", "20~23")', '应使用统一配置中的young_user_condition'),
                ('dengji IN ("超一线", "一线", "二线")', '应使用统一配置中的high_tier_city_condition'),
                ('1.5', '应使用统一配置中的quartile_multiplier')
            ]
            
            for pattern, suggestion in hardcoded_patterns:
                if pattern in component_code:
                    issues.append(f"组件 {component_name} 存在硬编码: {pattern} - {suggestion}")
            
            # 检查是否使用了DataCollector
            if 'from core.data_collector import DataCollector' in component_code:
                compliant_points.append("正确使用了统一的DataCollector")
            elif 'DataCollector' not in component_code and 'sqlite3' in component_code:
                warnings.append(f"组件 {component_name} 可能应该使用DataCollector而不是直接操作数据库")
            
            # 检查是否使用了配置中的字段定义
            if 'CONFIG_MANAGER' in component_code or 'get_sql_condition' in component_code:
                compliant_points.append("正确使用了统一配置管理器")
        
        return {
            'compliant': len(issues) == 0,
            'issues': issues,
            'warnings': warnings,
            'compliant_points': compliant_points,
            'total_score': max(0, 100 - len(issues) * 20 - len(warnings) * 5)
        }
    
    def generate_compliance_report(self) -> str:
        """生成合规性报告"""
        
        report = f"""
📋 统一配置合规性报告
{'='*60}
配置版本: {self.config.get('version', 'Unknown')}
更新时间: {self.config.get('last_updated', 'Unknown')}

🔧 核心业务定义:
"""
        
        for key, value in self.config['business_definitions'].items():
            report += f"• {key}: {value}\n"
        
        report += f"""
📊 计算规则:
"""
        for key, value in self.config['calculation_rules'].items():
            report += f"• {key}: {value}\n"
        
        report += f"""
📋 报告模板:
"""
        for module in self.config['report_template']['modules']:
            status = "✅ 启用" if module.get('enabled', True) else "⏸️ 禁用"
            report += f"• {module['name']}: {status}\n"
        
        return report

# 创建全局配置管理器实例
CONFIG_MANAGER = UnifiedConfigManager()

def ensure_config_compliance(func):
    """装饰器：确保函数使用统一配置"""
    
    def wrapper(*args, **kwargs):
        # 检查函数是否在合规的组件中
        import inspect
        frame = inspect.currentframe()
        try:
            caller_name = frame.f_back.f_globals.get('__name__', 'unknown')
            if not caller_name.startswith('modular_report_system'):
                print(f"⚠️ 警告: {func.__name__} 被从非报告系统模块调用: {caller_name}")
        finally:
            del frame
        
        result = func(*args, **kwargs)
        return result
    
    return wrapper

def migrate_to_unified_config():
    """迁移现有组件到统一配置系统"""
    
    migration_tasks = [
        {
            'component': 'overview_metrics.py',
            'issues': [
                '硬编码的业务指标定义',
                '散布在代码中的格式定义',
                '缺少统一的字段映射'
            ],
            'fixes': [
                '使用CONFIG_MANAGER.get_sql_condition()获取业务条件',
                '使用统一的field_mappings配置',
                '移除硬编码的指标定义'
            ]
        },
        {
            'component': 'anomaly_detection.py', 
            'issues': [
                '硬编码的四分位数参数(1.5)',
                '直接写死的指标列表',
                '分散的异常检测逻辑'
            ],
            'fixes': [
                '使用config["quartile_multiplier"]',
                '从统一配置读取monitored_metrics',
                '统一异常检测算法'
            ]
        },
        {
            'component': 'data_collector.py',
            'issues': [
                '业务逻辑定义分散',
                '缺少字段验证',
                'SQL条件硬编码'
            ],
            'fixes': [
                '集成统一配置的business_definitions',
                '添加字段合规性验证',
                '动态生成SQL条件'
            ]
        }
    ]
    
    print("🔄 统一配置迁移计划:")
    print("="*50)
    
    for task in migration_tasks:
        print(f"\n📁 组件: {task['component']}")
        print("❌ 问题:")
        for issue in task['issues']:
            print(f"  • {issue}")
        print("✅ 修复方案:")
        for fix in task['fixes']:
            print(f"  • {fix}")
    
    return migration_tasks

def audit_existing_system():
    """审计现有系统的合规性"""
    
    print("🔍 系统合规性审计")
    print("="*60)
    
    components_to_check = [
        './modular_report_system/modules/overview_metrics.py',
        './modular_report_system/modules/anomaly_detection.py',
        './modular_report_system/core/data_collector.py'
    ]
    
    audit_results = []
    
    for component_path in components_to_check:
        component_name = component_path.split('/')[-1]
        
        try:
            with open(component_path, 'r', encoding='utf-8') as f:
                component_code = f.read()
            
            compliance_result = CONFIG_MANAGER.validate_component_compliance(
                component_name, component_code
            )
            
            audit_results.append({
                'component': component_name,
                'path': component_path,
                'result': compliance_result
            })
            
        except FileNotFoundError:
            audit_results.append({
                'component': component_name,
                'path': component_path,
                'result': {'compliant': False, 'issues': ['文件不存在'], 'warnings': [], 'total_score': 0}
            })
    
    # 显示审计结果
    total_score = 0
    max_possible_score = len(audit_results) * 100
    
    for audit in audit_results:
        result = audit['result']
        score = result.get('total_score', 0)
        total_score += score
        
        status_icon = "✅" if result['compliant'] else "❌"
        print(f"\n{status_icon} {audit['component']} (评分: {score}/100)")
        
        if result.get('compliant_points'):
            for point in result['compliant_points']:
                print(f"  ✅ {point}")
        
        if result['issues']:
            for issue in result['issues']:
                print(f"  ❌ {issue}")
        
        if result['warnings']:
            for warning in result['warnings']:
                print(f"  ⚠️ {warning}")
    
    overall_score = (total_score / max_possible_score * 100) if max_possible_score > 0 else 0
    print(f"\n📊 系统整体合规性评分: {overall_score:.1f}% ({total_score}/{max_possible_score})")
    
    if overall_score >= 80:
        print("🎉 系统合规性优秀")
    elif overall_score >= 60:
        print("👍 系统合规性良好")
    elif overall_score >= 40:
        print("⚠️ 系统合规性需要改进")
    else:
        print("🚨 系统合规性严重不足，需要立即整改")
    
    return audit_results

def main():
    """测试统一配置系统"""
    
    print("🔧 统一配置管理系统测试")
    print("="*50)
    
    # 显示配置信息
    print(CONFIG_MANAGER.generate_compliance_report())
    
    # 测试配置获取
    print(f"\n🔍 测试配置获取:")
    print(f"优质用户条件: {CONFIG_MANAGER.get_sql_condition('quality_user_condition')}")
    print(f"ARPU计算基准: {CONFIG_MANAGER.get_calculation_rule('arpu_denominator')}")
    
    # 保存配置文件
    config_path = './config/unified_config.yaml'
    print(f"\n📁 配置文件保存位置: {config_path}")
    
    # 执行系统审计
    print("\n" + "="*60)
    audit_results = audit_existing_system()
    
    # 显示迁移计划
    print("\n" + "="*60)
    migrate_to_unified_config()

if __name__ == '__main__':
    main()