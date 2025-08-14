#!/usr/bin/env python3
"""
模块化日常报告控制器
系统主入口，协调各模块执行并生成完整报告
"""

import os
import sys
import importlib
from typing import Dict, Any, List
from datetime import datetime
import argparse
import json

# 添加当前目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from config.modules_config import MODULES_CONFIG, OVERVIEW_CONFIG, ANOMALY_CONFIG, HTML_CONFIG, DATABASE_CONFIG
from core.html_generator import HTMLReportGenerator
from core.base_module import ModuleResult

class DailyReportController:
    """日常报告控制器"""
    
    def __init__(self, db_path: str = None):
        """
        初始化控制器
        
        Args:
            db_path: 数据库路径，如果为None则使用配置中的默认路径
        """
        self.db_path = db_path or DATABASE_CONFIG['path']
        self.modules_config = MODULES_CONFIG
        self.html_generator = HTMLReportGenerator(HTML_CONFIG)
        self.loaded_modules = {}
        
        # 确保输出目录存在
        os.makedirs(HTML_CONFIG['export_path'], exist_ok=True)
        
        print(f"📊 模块化日常报告系统初始化完成")
        print(f"📁 数据库路径: {self.db_path}")
        print(f"📁 输出路径: {HTML_CONFIG['export_path']}")
    
    def load_modules(self) -> Dict[str, Any]:
        """动态加载所有启用的模块"""
        
        loaded_count = 0
        
        for module_id, module_config in self.modules_config.items():
            if not module_config.get('enabled', False):
                print(f"⏸️  跳过模块: {module_config['name']} (已禁用)")
                continue
            
            try:
                # 动态导入模块
                class_name = module_config['class']
                
                if class_name == 'OverviewMetricsModule':
                    from modules.overview_metrics import OverviewMetricsModule
                    module_class = OverviewMetricsModule
                    config = OVERVIEW_CONFIG
                elif class_name == 'AnomalyDetectionModule':
                    from modules.anomaly_detection import AnomalyDetectionModule
                    module_class = AnomalyDetectionModule
                    config = ANOMALY_CONFIG
                elif class_name == 'UserBehaviorAnalysisModule':
                    from modules.user_behavior_analysis import UserBehaviorAnalysisModule
                    module_class = UserBehaviorAnalysisModule
                    config = {}  # 使用空配置，模块内部处理
                else:
                    print(f"❌ 未知模块类: {class_name}")
                    continue
                
                # 实例化模块
                module_instance = module_class(self.db_path, config)
                self.loaded_modules[module_id] = {
                    'instance': module_instance,
                    'config': module_config,
                    'order': module_config.get('order', 999)
                }
                
                loaded_count += 1
                print(f"✅ 加载模块: {module_config['name']} ({class_name})")
                
            except Exception as e:
                print(f"❌ 加载模块失败: {module_config['name']} - {str(e)}")
                continue
        
        print(f"📦 模块加载完成: {loaded_count}/{len([m for m in self.modules_config.values() if m.get('enabled')])}")
        return self.loaded_modules
    
    def execute_modules(self, target_date: str) -> List[ModuleResult]:
        """
        执行所有加载的模块
        
        Args:
            target_date: 目标日期 (YYYY-MM-DD)
            
        Returns:
            所有模块的执行结果列表
        """
        
        if not self.loaded_modules:
            self.load_modules()
        
        # 按顺序排序模块
        sorted_modules = sorted(
            self.loaded_modules.items(),
            key=lambda x: x[1]['order']
        )
        
        results = []
        
        print(f"\n🚀 开始执行模块 (目标日期: {target_date})")
        print("=" * 60)
        
        for module_id, module_info in sorted_modules:
            module_instance = module_info['instance']
            module_config = module_info['config']
            
            print(f"\n📊 执行模块: {module_config['name']}")
            print(f"🔧 模块类型: {module_config['class']}")
            
            start_time = datetime.now()
            
            try:
                # 执行模块
                result = module_instance.execute(target_date)
                
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                # 更新执行时间
                result.execution_time = execution_time
                
                results.append(result)
                
                if result.success:
                    print(f"✅ 模块执行成功 (耗时: {execution_time:.2f}s)")
                    if result.warnings:
                        print(f"⚠️  警告: {len(result.warnings)}个")
                        for warning in result.warnings:
                            print(f"   - {warning}")
                else:
                    print(f"❌ 模块执行失败 (耗时: {execution_time:.2f}s)")
                    for error in result.errors:
                        print(f"   ❌ {error}")
                
            except Exception as e:
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                
                print(f"💥 模块执行异常: {str(e)} (耗时: {execution_time:.2f}s)")
                
                # 创建失败结果
                failed_result = ModuleResult(
                    module_name=module_config['class'],
                    success=False,
                    data={},
                    html_content=f"<div class='error'>模块执行异常: {str(e)}</div>",
                    errors=[f"执行异常: {str(e)}"],
                    execution_time=execution_time
                )
                
                results.append(failed_result)
        
        print("\n" + "=" * 60)
        successful_count = len([r for r in results if r.success])
        print(f"📈 模块执行完成: {successful_count}/{len(results)} 成功")
        
        return results
    
    def generate_report(self, target_date: str, output_format: str = 'html') -> str:
        """
        生成完整报告
        
        Args:
            target_date: 目标日期
            output_format: 输出格式 ('html', 'json')
            
        Returns:
            报告文件路径
        """
        
        print(f"\n📝 开始生成报告...")
        
        # 执行所有模块
        module_results = self.execute_modules(target_date)
        
        if output_format == 'html':
            return self._generate_html_report(module_results, target_date)
        elif output_format == 'json':
            return self._generate_json_report(module_results, target_date)
        else:
            raise ValueError(f"不支持的输出格式: {output_format}")
    
    def _generate_html_report(self, module_results: List[ModuleResult], target_date: str) -> str:
        """生成HTML格式报告"""
        
        # 转换为HTML生成器需要的格式
        html_results = []
        for result in module_results:
            html_results.append({
                'module_name': result.module_name,
                'success': result.success,
                'html_content': result.html_content,
                'warnings': result.warnings,
                'errors': result.errors,
                'execution_time': result.execution_time
            })
        
        # 生成HTML内容
        html_content = self.html_generator.generate_report(html_results, target_date)
        
        # 保存文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"daily_report_{target_date.replace('-', '')}_{timestamp}.html"
        filepath = os.path.join(HTML_CONFIG['export_path'], filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"📄 HTML报告已生成: {filepath}")
        return filepath
    
    def _generate_json_report(self, module_results: List[ModuleResult], target_date: str) -> str:
        """生成JSON格式报告"""
        
        # 转换为JSON格式
        json_data = {
            'report_date': target_date,
            'generation_time': datetime.now().isoformat(),
            'total_modules': len(module_results),
            'successful_modules': len([r for r in module_results if r.success]),
            'modules': []
        }
        
        for result in module_results:
            json_data['modules'].append({
                'module_name': result.module_name,
                'success': result.success,
                'data': result.data,
                'warnings': result.warnings,
                'errors': result.errors,
                'execution_time': result.execution_time
            })
        
        # 保存文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"daily_report_{target_date.replace('-', '')}_{timestamp}.json"
        filepath = os.path.join(HTML_CONFIG['export_path'], filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"📄 JSON报告已生成: {filepath}")
        return filepath
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查主要表是否存在
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (DATABASE_CONFIG['user_table'],))
            user_table_exists = cursor.fetchone() is not None
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (DATABASE_CONFIG['cost_table'],))
            cost_table_exists = cursor.fetchone() is not None
            
            conn.close()
            
            print(f"✅ 数据库连接成功")
            print(f"   📋 用户表 ({DATABASE_CONFIG['user_table']}): {'存在' if user_table_exists else '不存在'}")
            print(f"   📋 成本表 ({DATABASE_CONFIG['cost_table']}): {'存在' if cost_table_exists else '不存在'}")
            
            return user_table_exists and cost_table_exists
            
        except Exception as e:
            print(f"❌ 数据库连接失败: {str(e)}")
            return False
    
    def get_available_dates(self, limit: int = 10) -> List[str]:
        """获取可用的数据日期"""
        
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT DISTINCT dt 
                FROM {DATABASE_CONFIG['user_table']} 
                ORDER BY dt DESC 
                LIMIT ?
            """, (limit,))
            
            dates = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            return dates
            
        except Exception as e:
            print(f"❌ 获取可用日期失败: {str(e)}")
            return []

def main():
    """命令行入口"""
    
    parser = argparse.ArgumentParser(description='模块化日常报告生成器')
    parser.add_argument('date', nargs='?', help='报告日期 (YYYY-MM-DD)，不提供则使用最新日期')
    parser.add_argument('--format', choices=['html', 'json'], default='html', help='输出格式')
    parser.add_argument('--db', help='数据库路径')
    parser.add_argument('--test', action='store_true', help='测试数据库连接')
    parser.add_argument('--list-dates', action='store_true', help='列出可用日期')
    parser.add_argument('--config', action='store_true', help='显示当前配置')
    
    args = parser.parse_args()
    
    # 初始化控制器
    controller = DailyReportController(args.db)
    
    # 处理特殊命令
    if args.test:
        print("🔍 测试数据库连接...")
        if controller.test_connection():
            print("✅ 数据库连接测试通过")
            return 0
        else:
            print("❌ 数据库连接测试失败")
            return 1
    
    if args.list_dates:
        print("📅 获取可用日期...")
        dates = controller.get_available_dates(20)
        if dates:
            print("可用的数据日期:")
            for i, date in enumerate(dates, 1):
                print(f"  {i:2d}. {date}")
        else:
            print("❌ 未找到可用日期")
        return 0
    
    if args.config:
        print("⚙️  当前配置:")
        print(f"  📁 数据库路径: {controller.db_path}")
        print(f"  📁 输出路径: {HTML_CONFIG['export_path']}")
        print(f"  📊 启用模块: {len([m for m in MODULES_CONFIG.values() if m.get('enabled')])}")
        for module_id, config in MODULES_CONFIG.items():
            status = "✅" if config.get('enabled') else "❌"
            print(f"    {status} {config['name']}")
        return 0
    
    # 确定报告日期
    target_date = args.date
    if not target_date:
        # 使用最新可用日期
        dates = controller.get_available_dates(1)
        if dates:
            target_date = dates[0]
            print(f"📅 使用最新可用日期: {target_date}")
        else:
            print("❌ 未找到可用日期，请手动指定日期")
            return 1
    
    # 验证日期格式
    try:
        datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        print(f"❌ 日期格式错误: {target_date}，请使用 YYYY-MM-DD 格式")
        return 1
    
    try:
        # 生成报告
        report_path = controller.generate_report(target_date, args.format)
        
        print(f"\n🎉 报告生成完成!")
        print(f"📄 报告文件: {report_path}")
        print(f"📊 报告格式: {args.format.upper()}")
        
        # 如果是HTML格式，提供打开提示
        if args.format == 'html':
            abs_path = os.path.abspath(report_path)
            print(f"🌐 在浏览器中打开: file://{abs_path}")
        
        return 0
        
    except Exception as e:
        print(f"💥 报告生成失败: {str(e)}")
        return 1

if __name__ == '__main__':
    sys.exit(main())