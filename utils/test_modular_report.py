#!/usr/bin/env python3
"""
模块化报告系统演示脚本
快速测试和体验模块化日常报告系统
"""

import os
import sys
from datetime import datetime, timedelta

def generate_modular_report(target_date: str) -> str:
    """生成模块化报告并返回HTML内容"""
    
    # 检查系统路径
    modular_system_path = "./modular_report_system"
    if not os.path.exists(modular_system_path):
        raise FileNotFoundError(f"找不到模块化报告系统: {modular_system_path}")
    
    # 添加系统路径
    sys.path.insert(0, modular_system_path)
    
    try:
        from daily_report_controller import DailyReportController
        
        # 初始化控制器
        controller = DailyReportController()
        
        # 测试数据库连接
        if not controller.test_connection():
            raise ConnectionError("数据库连接失败")
        
        # 生成HTML报告
        html_report_path = controller.generate_report(target_date, 'html')
        
        # 读取生成的HTML内容
        with open(html_report_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        return html_content
        
    except Exception as e:
        raise RuntimeError(f"生成报告失败: {str(e)}")

def main():
    print("🎯 模块化日常报告系统演示")
    print("=" * 50)
    
    # 检查系统路径
    modular_system_path = "./modular_report_system"
    if not os.path.exists(modular_system_path):
        print(f"❌ 找不到模块化报告系统: {modular_system_path}")
        print("请确保在正确的目录下运行此脚本")
        return 1
    
    # 添加系统路径
    sys.path.insert(0, modular_system_path)
    
    try:
        from daily_report_controller import DailyReportController
        
        # 初始化控制器
        print("\n📊 初始化报告控制器...")
        controller = DailyReportController()
        
        # 测试数据库连接
        print("\n🔍 测试数据库连接...")
        if not controller.test_connection():
            print("❌ 数据库连接失败，请检查数据文件")
            return 1
        
        # 获取可用日期
        print("\n📅 获取可用日期...")
        available_dates = controller.get_available_dates(5)
        if not available_dates:
            print("❌ 未找到可用数据")
            return 1
        
        print("最近可用的日期:")
        for i, date in enumerate(available_dates, 1):
            print(f"  {i}. {date}")
        
        # 选择最新日期进行演示
        target_date = available_dates[0]
        print(f"\n🎯 使用日期: {target_date}")
        
        # 加载模块
        print("\n📦 加载报告模块...")
        loaded_modules = controller.load_modules()
        if not loaded_modules:
            print("❌ 未加载到任何模块")
            return 1
        
        # 生成HTML报告
        print(f"\n🚀 生成HTML报告 ({target_date})...")
        html_report_path = controller.generate_report(target_date, 'html')
        
        # 生成JSON报告
        print(f"\n📋 生成JSON报告 ({target_date})...")
        json_report_path = controller.generate_report(target_date, 'json')
        
        # 显示结果
        print("\n" + "=" * 50)
        print("🎉 演示完成!")
        print("=" * 50)
        print(f"📄 HTML报告: {html_report_path}")
        print(f"📄 JSON报告: {json_report_path}")
        
        # 提供打开链接
        html_abs_path = os.path.abspath(html_report_path)
        print(f"\n🌐 在浏览器中查看HTML报告:")
        print(f"file://{html_abs_path}")
        
        # 显示系统特性
        print(f"\n✨ 系统特性:")
        print(f"  📊 模块化架构 - 每个分析模块独立运行")
        print(f"  🚨 智能异常检测 - 自动识别业务指标异常")
        print(f"  📈 渠道影响分析 - 分析各渠道对异常的贡献度")
        print(f"  🎨 美观HTML输出 - 现代化报告界面")
        print(f"  🔧 可扩展设计 - 易于添加新的分析模块")
        
        # 使用提示
        print(f"\n💡 使用提示:")
        print(f"  python modular_report_system/daily_report_controller.py --help")
        print(f"  python modular_report_system/daily_report_controller.py 2025-07-26")
        print(f"  python modular_report_system/daily_report_controller.py --test")
        print(f"  python modular_report_system/daily_report_controller.py --list-dates")
        
        return 0
        
    except ImportError as e:
        print(f"❌ 导入模块失败: {str(e)}")
        print("请检查模块化报告系统是否正确安装")
        return 1
        
    except Exception as e:
        print(f"💥 演示过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())