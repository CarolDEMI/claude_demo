#!/usr/bin/env python3
"""
自动化合规性修复脚本
解决现有组件中的硬编码和不一致问题
"""

import os
import re
from typing import Dict, List, Tuple
from unified_config_system import CONFIG_MANAGER

class ComplianceFixer:
    """合规性自动修复器"""
    
    def __init__(self):
        self.config_manager = CONFIG_MANAGER
        self.fixes_applied = []
        
    def fix_anomaly_detection_module(self) -> List[str]:
        """修复异常检测模块的合规性问题"""
        
        file_path = './modular_report_system/modules/anomaly_detection.py'
        fixes = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # 1. 替换硬编码的SQL条件
            replacements = [
                # 替换Good且认证用户条件
                (
                    r'SUM\(CASE WHEN status = "good" AND verification_status = "verified" THEN newuser ELSE 0 END\)',
                    'SUM(CASE WHEN {} THEN newuser ELSE 0 END)'.format(
                        self.config_manager.get_sql_condition('quality_user_condition')
                    )
                ),
                # 替换四分位数参数
                (
                    r"multiplier = config\.get\('quartile_multiplier', 1\.5\)",
                    "multiplier = config.get('quartile_multiplier', {})".format(
                        self.config_manager.config['anomaly_detection']['quartile_multiplier']
                    )
                ),
                # 添加配置管理器导入
                (
                    r'from core\.data_collector import DataCollector',
                    'from core.data_collector import DataCollector\nfrom unified_config_system import CONFIG_MANAGER'
                )
            ]
            
            for pattern, replacement in replacements:
                new_content = re.sub(pattern, replacement, content)
                if new_content != content:
                    fixes.append(f"替换: {pattern[:50]}...")
                    content = new_content
            
            # 2. 使用配置中的监控指标列表
            config_metrics_code = '''
        # 从统一配置获取监控的核心KPI指标
        monitored_metrics = CONFIG_MANAGER.config['anomaly_detection']['monitored_metrics']
        
        key_metrics = []
        for metric_field in monitored_metrics:
            # 根据字段类型设置格式
            if metric_field in ['quality_users']:
                format_str = '{:,}'
                unit = '人'
                icon = '⭐'
                name = 'Good且认证用户数'
            elif metric_field in ['cpa', 'arpu_after_tax']:
                format_str = '¥{:.2f}'
                unit = '元'
                icon = '💰' if 'cpa' in metric_field else '📈'
                name = 'CPA' if 'cpa' in metric_field else 'ARPU（税后）'
            elif 'rate' in metric_field:
                format_str = '{:.1f}%'
                unit = '%'
                icon = '🔄' if 'retention' in metric_field else '✅'
                name = metric_field.replace('_', ' ').title()
            else:
                format_str = '{:.2f}'
                unit = ''
                icon = '📊'
                name = metric_field.replace('_', ' ').title()
            
            key_metrics.append({
                'field': metric_field, 
                'name': name, 
                'format': format_str, 
                'unit': unit
            })'''
            
            # 替换硬编码的指标列表
            pattern = r'# 检测的核心KPI指标\s+key_metrics = \[.*?\]'
            if re.search(pattern, content, re.DOTALL):
                content = re.sub(pattern, config_metrics_code.strip(), content, flags=re.DOTALL)
                fixes.append("使用统一配置的监控指标列表")
            
            # 保存修复后的文件
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixes.append(f"文件已更新: {file_path}")
            
        except Exception as e:
            fixes.append(f"修复失败: {str(e)}")
        
        return fixes
    
    def fix_overview_metrics_module(self) -> List[str]:
        """修复大盘指标模块（虽然评分100，但仍可优化）"""
        
        file_path = './modular_report_system/modules/overview_metrics.py'
        fixes = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # 添加配置管理器导入（如果没有的话）
            if 'from unified_config_system import CONFIG_MANAGER' not in content:
                import_line = 'from core.data_collector import DataCollector'
                replacement = 'from core.data_collector import DataCollector\nfrom unified_config_system import CONFIG_MANAGER'
                content = content.replace(import_line, replacement)
                fixes.append("添加统一配置管理器导入")
            
            # 添加配置验证逻辑
            validation_code = '''
    def __init__(self, config: Dict[str, Any]):
        """初始化时验证配置合规性"""
        super().__init__(config)
        
        # 验证组件合规性
        compliance_result = CONFIG_MANAGER.validate_component_compliance(
            self.__class__.__name__, 
            open(__file__, 'r', encoding='utf-8').read()
        )
        
        if not compliance_result['compliant']:
            print(f"⚠️ {self.__class__.__name__} 合规性警告:")
            for issue in compliance_result['issues']:
                print(f"  • {issue}")
'''
            
            # 如果没有自定义初始化方法，添加一个
            if 'def __init__(self' not in content:
                class_line = 'class OverviewMetricsModule(BaseReportModule):'
                if class_line in content:
                    content = content.replace(
                        class_line + '\n    """大盘指标模块（基于新配置系统）"""',
                        class_line + '\n    """大盘指标模块（基于新配置系统）"""' + validation_code
                    )
                    fixes.append("添加配置合规性验证")
            
            # 保存修复后的文件
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                fixes.append(f"文件已更新: {file_path}")
            
        except Exception as e:
            fixes.append(f"修复失败: {str(e)}")
        
        return fixes
    
    def create_compliance_monitor(self) -> List[str]:
        """创建持续的合规性监控脚本"""
        
        monitor_script = '''#!/usr/bin/env python3
"""
合规性持续监控脚本
定期检查系统组件的合规性状态
"""

import os
import time
from datetime import datetime
from unified_config_system import CONFIG_MANAGER

def monitor_compliance():
    """持续监控合规性"""
    
    components = [
        './modular_report_system/modules/overview_metrics.py',
        './modular_report_system/modules/anomaly_detection.py',
        './modular_report_system/core/data_collector.py'
    ]
    
    print(f"🔍 合规性监控启动 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    total_score = 0
    total_components = len(components)
    
    for component_path in components:
        component_name = os.path.basename(component_path)
        
        try:
            with open(component_path, 'r', encoding='utf-8') as f:
                code = f.read()
            
            result = CONFIG_MANAGER.validate_component_compliance(component_name, code)
            score = result.get('total_score', 0)
            total_score += score
            
            status = "✅" if result['compliant'] else "❌"
            print(f"{status} {component_name}: {score}/100")
            
            if result['issues']:
                for issue in result['issues']:
                    print(f"  ❌ {issue}")
            
            if result['warnings']:
                for warning in result['warnings']:
                    print(f"  ⚠️ {warning}")
                    
        except Exception as e:
            print(f"❌ {component_name}: 检查失败 - {e}")
    
    avg_score = total_score / total_components if total_components > 0 else 0
    print(f"\\n📊 系统平均合规性评分: {avg_score:.1f}/100")
    
    if avg_score >= 90:
        print("🎉 系统合规性优秀")
    elif avg_score >= 70:
        print("👍 系统合规性良好") 
    elif avg_score >= 50:
        print("⚠️ 系统合规性需要改进")
    else:
        print("🚨 系统合规性严重不足")
    
    return avg_score

if __name__ == '__main__':
    monitor_compliance()
'''
        
        monitor_path = './compliance_monitor.py'
        
        try:
            with open(monitor_path, 'w', encoding='utf-8') as f:
                f.write(monitor_script)
            
            # 设置执行权限
            os.chmod(monitor_path, 0o755)
            
            return [f"创建合规性监控脚本: {monitor_path}"]
            
        except Exception as e:
            return [f"创建监控脚本失败: {str(e)}"]
    
    def run_auto_fix(self) -> Dict[str, List[str]]:
        """运行自动修复"""
        
        print("🔧 开始自动合规性修复...")
        print("=" * 50)
        
        results = {}
        
        # 修复异常检测模块
        print("\n📊 修复异常检测模块...")
        results['anomaly_detection'] = self.fix_anomaly_detection_module()
        
        # 优化大盘指标模块
        print("\n📈 优化大盘指标模块...")
        results['overview_metrics'] = self.fix_overview_metrics_module()
        
        # 创建监控脚本
        print("\n🔍 创建合规性监控...")
        results['monitoring'] = self.create_compliance_monitor()
        
        # 显示修复结果
        print("\n" + "=" * 50)
        print("🎯 修复结果汇总:")
        
        for component, fixes in results.items():
            print(f"\n📁 {component}:")
            if fixes:
                for fix in fixes:
                    print(f"  ✅ {fix}")
            else:
                print("  ℹ️ 无需修复")
        
        return results

def main():
    """主函数"""
    fixer = ComplianceFixer()
    fixer.run_auto_fix()
    
    print("\n🔄 重新运行合规性检查...")
    os.system('python3 unified_config_system.py')

if __name__ == '__main__':
    main()