#!/usr/bin/env python3
"""
📊 日报模块管理工具
快速启用/禁用/配置报告模块
"""

import os
import sys
import json
from typing import Dict, List

# 添加当前目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from config.modules_config import MODULES_CONFIG

class ModuleManager:
    """模块管理器"""
    
    def __init__(self):
        self.config_file = os.path.join(current_dir, 'config', 'modules_config.py')
        self.modules = MODULES_CONFIG.copy()
    
    def list_modules(self):
        """列出所有模块"""
        print("📊 当前模块列表:")
        print("=" * 60)
        
        for module_id, config in sorted(self.modules.items(), key=lambda x: x[1].get('order', 999)):
            status = "✅ 启用" if config['enabled'] else "❌ 禁用"
            icon = config.get('icon', '📋')
            name = config.get('name', module_id)
            description = config.get('description', '无描述')
            order = config.get('order', '未设置')
            
            print(f"{icon} {name}")
            print(f"   ID: {module_id}")
            print(f"   状态: {status}")
            print(f"   顺序: {order}")
            print(f"   描述: {description}")
            print("-" * 40)
    
    def enable_module(self, module_id: str):
        """启用模块"""
        if module_id not in self.modules:
            print(f"❌ 模块 '{module_id}' 不存在")
            return False
        
        self.modules[module_id]['enabled'] = True
        print(f"✅ 已启用模块: {self.modules[module_id]['name']}")
        return True
    
    def disable_module(self, module_id: str):
        """禁用模块"""
        if module_id not in self.modules:
            print(f"❌ 模块 '{module_id}' 不存在")
            return False
        
        self.modules[module_id]['enabled'] = False
        print(f"❌ 已禁用模块: {self.modules[module_id]['name']}")
        return True
    
    def set_module_order(self, module_id: str, order: int):
        """设置模块顺序"""
        if module_id not in self.modules:
            print(f"❌ 模块 '{module_id}' 不存在")
            return False
        
        self.modules[module_id]['order'] = order
        print(f"📊 已设置模块 '{self.modules[module_id]['name']}' 的顺序为: {order}")
        return True
    
    def get_enabled_modules(self) -> List[str]:
        """获取启用的模块列表"""
        enabled = []
        for module_id, config in self.modules.items():
            if config['enabled']:
                enabled.append((module_id, config['name'], config.get('order', 999)))
        
        # 按顺序排序
        enabled.sort(key=lambda x: x[2])
        return enabled
    
    def save_config(self):
        """保存配置到文件"""
        config_content = '''"""
模块化报告系统配置
定义各个报告模块的配置信息
"""

# 模块配置
MODULES_CONFIG = {
'''
        
        for module_id, config in self.modules.items():
            config_content += f"    '{module_id}': {{\n"
            config_content += f"        'name': '{config['name']}',\n"
            config_content += f"        'description': '{config['description']}',\n"
            config_content += f"        'icon': '{config['icon']}',\n"
            config_content += f"        'enabled': {config['enabled']},\n"
            config_content += f"        'order': {config['order']},\n"
            config_content += f"        'class': '{config['class']}'\n"
            config_content += "    },\n"
        
        config_content += "}\n\n"
        
        # 添加其他配置（从原文件读取）
        with open(self.config_file, 'r', encoding='utf-8') as f:
            original = f.read()
            
        # 找到 OVERVIEW_CONFIG 开始的位置
        overview_start = original.find('# 大盘指标配置')
        if overview_start != -1:
            config_content += original[overview_start:]
        
        # 写入文件
        with open(self.config_file, 'w', encoding='utf-8') as f:
            f.write(config_content)
        
        print("💾 配置已保存到文件")
    
    def preset_configs(self):
        """显示预设配置"""
        presets = {
            'minimal': {
                'name': '极简模式',
                'description': '只显示大盘指标',
                'modules': ['overview_metrics']
            },
            'standard': {
                'name': '标准模式', 
                'description': '显示大盘指标和异常检测',
                'modules': ['overview_metrics', 'anomaly_detection']
            },
            'full': {
                'name': '完整模式',
                'description': '显示所有可用模块',
                'modules': ['overview_metrics', 'anomaly_detection', 'user_behavior_analysis']
            },
            'analysis': {
                'name': '分析模式',
                'description': '重点关注异常检测和用户行为',
                'modules': ['anomaly_detection', 'user_behavior_analysis']
            }
        }
        
        print("🎛️ 预设配置模式:")
        print("=" * 50)
        
        for preset_id, preset in presets.items():
            print(f"📋 {preset['name']} ({preset_id})")
            print(f"   描述: {preset['description']}")
            print(f"   包含模块: {', '.join(preset['modules'])}")
            print("-" * 30)
        
        return presets
    
    def apply_preset(self, preset_id: str):
        """应用预设配置"""
        presets = {
            'minimal': ['overview_metrics'],
            'standard': ['overview_metrics', 'anomaly_detection'],
            'full': ['overview_metrics', 'anomaly_detection', 'user_behavior_analysis'],
            'analysis': ['anomaly_detection', 'user_behavior_analysis']
        }
        
        if preset_id not in presets:
            print(f"❌ 预设配置 '{preset_id}' 不存在")
            return False
        
        # 先禁用所有模块
        for module_id in self.modules:
            self.modules[module_id]['enabled'] = False
        
        # 启用预设中的模块
        for module_id in presets[preset_id]:
            if module_id in self.modules:
                self.modules[module_id]['enabled'] = True
        
        print(f"✅ 已应用预设配置: {preset_id}")
        print(f"启用的模块: {', '.join(presets[preset_id])}")
        return True

def main():
    """主函数"""
    manager = ModuleManager()
    
    if len(sys.argv) < 2:
        print("📊 日报模块管理工具")
        print("=" * 40)
        print("使用方法:")
        print("  python module_manager.py list                    # 列出所有模块")
        print("  python module_manager.py enable <module_id>      # 启用模块") 
        print("  python module_manager.py disable <module_id>     # 禁用模块")
        print("  python module_manager.py order <module_id> <num> # 设置模块顺序")
        print("  python module_manager.py preset                  # 查看预设配置")
        print("  python module_manager.py preset <preset_id>      # 应用预设配置")
        print("  python module_manager.py status                  # 查看当前状态")
        print()
        print("示例:")
        print("  python module_manager.py enable user_behavior_analysis")
        print("  python module_manager.py preset standard")
        print("  python module_manager.py order anomaly_detection 1")
        return
    
    command = sys.argv[1]
    
    if command == 'list':
        manager.list_modules()
    
    elif command == 'enable':
        if len(sys.argv) < 3:
            print("❌ 请指定要启用的模块ID")
            return
        module_id = sys.argv[2]
        if manager.enable_module(module_id):
            manager.save_config()
    
    elif command == 'disable':
        if len(sys.argv) < 3:
            print("❌ 请指定要禁用的模块ID")
            return
        module_id = sys.argv[2]
        if manager.disable_module(module_id):
            manager.save_config()
    
    elif command == 'order':
        if len(sys.argv) < 4:
            print("❌ 请指定模块ID和顺序号")
            return
        module_id = sys.argv[2]
        try:
            order = int(sys.argv[3])
            if manager.set_module_order(module_id, order):
                manager.save_config()
        except ValueError:
            print("❌ 顺序号必须是数字")
    
    elif command == 'preset':
        if len(sys.argv) < 3:
            manager.preset_configs()
        else:
            preset_id = sys.argv[2]
            if manager.apply_preset(preset_id):
                manager.save_config()
    
    elif command == 'status':
        enabled = manager.get_enabled_modules()
        print("📊 当前启用的模块:")
        print("=" * 40)
        for module_id, name, order in enabled:
            icon = manager.modules[module_id].get('icon', '📋')
            print(f"{order}. {icon} {name} ({module_id})")
    
    else:
        print(f"❌ 未知命令: {command}")

if __name__ == '__main__':
    main()