#!/usr/bin/env python3
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
    print(f"\n📊 系统平均合规性评分: {avg_score:.1f}/100")
    
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
