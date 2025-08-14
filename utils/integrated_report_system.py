#!/usr/bin/env python3
"""
集成报告系统 - 统一入口
使用全新的统一配置管理系统生成高质量报告
"""

import os
import sys
import argparse
from datetime import datetime, date
from typing import Dict, Any, Optional

# 导入统一配置系统
from unified_config_system import CONFIG_MANAGER

def check_system_health() -> Dict[str, Any]:
    """检查系统健康状态"""
    
    print("🔍 系统健康检查...")
    print("=" * 50)
    
    # 1. 检查配置完整性
    try:
        config_status = CONFIG_MANAGER.generate_compliance_report()
        print("✅ 统一配置系统: 正常")
    except Exception as e:
        print(f"❌ 统一配置系统: 异常 - {e}")
        return {"healthy": False, "error": f"配置系统异常: {e}"}
    
    # 2. 检查数据库连接
    try:
        import sqlite3
        db_path = CONFIG_MANAGER.config['data_sources']['database_path']
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM cpz_qs_newuser_channel_i_d LIMIT 1")
            print("✅ 数据库连接: 正常")
    except Exception as e:
        print(f"❌ 数据库连接: 异常 - {e}")
        return {"healthy": False, "error": f"数据库连接异常: {e}"}
    
    # 3. 检查组件合规性
    try:
        os.system("python3 compliance_monitor.py > /tmp/compliance_check.log 2>&1")
        with open("/tmp/compliance_check.log", "r") as f:
            log_content = f.read()
        
        if "🎉 系统合规性优秀" in log_content:
            print("✅ 组件合规性: 优秀")
            compliance_score = "100%"
        elif "👍 系统合规性良好" in log_content:
            print("⚠️ 组件合规性: 良好")
            compliance_score = "70-89%"
        else:
            print("❌ 组件合规性: 需要改进")
            compliance_score = "<70%"
            
    except Exception as e:
        print(f"⚠️ 合规性检查: 无法完成 - {e}")
        compliance_score = "未知"
    
    print(f"\n📊 系统状态汇总:")
    print(f"  • 配置系统: ✅ 正常")
    print(f"  • 数据库: ✅ 正常") 
    print(f"  • 合规性: {compliance_score}")
    
    return {
        "healthy": True, 
        "compliance_score": compliance_score,
        "config_version": CONFIG_MANAGER.config.get('version', '1.0')
    }

def generate_daily_report(target_date: str, output_format: str = "html") -> str:
    """生成指定日期的日报"""
    
    print(f"📊 生成 {target_date} 日报...")
    print("=" * 50)
    
    # 检查目标日期数据是否存在
    try:
        import sqlite3
        db_path = CONFIG_MANAGER.config['data_sources']['database_path']
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM cpz_qs_newuser_channel_i_d WHERE dt = ?", 
                (target_date,)
            )
            count = cursor.fetchone()[0]
            
            if count == 0:
                print(f"❌ 错误: {target_date} 无数据，请检查日期或运行数据同步")
                return ""
                
            print(f"✅ 数据检查: 找到 {count} 条记录")
            
    except Exception as e:
        print(f"❌ 数据检查失败: {e}")
        return ""
    
    # 生成模块化报告
    try:
        # 导入报告系统
        sys.path.append('./modular_report_system')
        from test_modular_report import generate_modular_report
        
        # 使用统一配置生成报告
        report_content = generate_modular_report(target_date)
        
        # 保存报告
        if output_format.lower() == "html":
            report_filename = f"daily_report_{target_date}.html"
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            print(f"✅ 报告已生成: {report_filename}")
            return report_filename
        else:
            print("⚠️ 目前只支持HTML格式输出")
            return ""
            
    except Exception as e:
        print(f"❌ 报告生成失败: {e}")
        import traceback
        traceback.print_exc()
        return ""

def sync_data_for_date(target_date: str) -> bool:
    """为指定日期同步数据"""
    
    print(f"🔄 同步 {target_date} 数据...")
    print("=" * 50)
    
    try:
        # 运行增强版同步脚本
        result = os.system(f"python3 daily_sync_auto_fix.py --date {target_date}")
        
        if result == 0:
            print(f"✅ {target_date} 数据同步完成")
            return True
        else:
            print(f"❌ {target_date} 数据同步失败")
            return False
            
    except Exception as e:
        print(f"❌ 数据同步异常: {e}")
        return False

def interactive_mode():
    """交互式模式"""
    
    print("🎯 集成报告系统 - 交互模式")
    print("=" * 50)
    
    while True:
        print("\n可用操作:")
        print("1. 系统健康检查")
        print("2. 生成日报")
        print("3. 数据同步") 
        print("4. 合规性检查")
        print("5. 配置信息")
        print("0. 退出")
        
        choice = input("\n请选择操作 (0-5): ").strip()
        
        if choice == "0":
            print("👋 再见!")
            break
        elif choice == "1":
            check_system_health()
        elif choice == "2":
            target_date = input("请输入日期 (YYYY-MM-DD, 回车使用昨天): ").strip()
            if not target_date:
                from datetime import datetime, timedelta
                target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            print(f"生成 {target_date} 的报告...")
            report_file = generate_daily_report(target_date)
            
            if report_file:
                print(f"\n📄 报告文件: {report_file}")
                open_file = input("是否在浏览器中打开? (y/N): ").strip().lower()
                if open_file == 'y':
                    os.system(f"open {report_file}")  # macOS
        elif choice == "3":
            target_date = input("请输入要同步的日期 (YYYY-MM-DD): ").strip()
            if target_date:
                sync_data_for_date(target_date)
        elif choice == "4":
            print("运行合规性检查...")
            os.system("python3 compliance_monitor.py")
        elif choice == "5":
            print("\n" + CONFIG_MANAGER.generate_compliance_report())
        else:
            print("❌ 无效选择，请重试")

def main():
    """主入口函数"""
    
    parser = argparse.ArgumentParser(
        description="集成报告系统 - 使用统一配置管理的报告生成工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python3 integrated_report_system.py --check           # 系统健康检查
  python3 integrated_report_system.py --report 2025-07-26  # 生成指定日期报告
  python3 integrated_report_system.py --sync 2025-07-26    # 同步指定日期数据
  python3 integrated_report_system.py --interactive        # 交互式模式
        """
    )
    
    parser.add_argument('--check', action='store_true', 
                       help='执行系统健康检查')
    parser.add_argument('--report', metavar='DATE', 
                       help='生成指定日期的报告 (YYYY-MM-DD)')
    parser.add_argument('--sync', metavar='DATE', 
                       help='同步指定日期的数据 (YYYY-MM-DD)')
    parser.add_argument('--interactive', action='store_true', 
                       help='启动交互式模式')
    parser.add_argument('--format', choices=['html'], default='html',
                       help='报告输出格式 (目前只支持html)')
    
    args = parser.parse_args()
    
    # 显示系统信息
    print("🚀 集成报告系统")
    print(f"配置版本: {CONFIG_MANAGER.config.get('version', '1.0')}")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 执行相应操作
    if args.check:
        check_system_health()
    elif args.report:
        report_file = generate_daily_report(args.report, args.format)
        if report_file:
            print(f"\n🎉 报告生成成功: {report_file}")
    elif args.sync:
        success = sync_data_for_date(args.sync)
        if success:
            print(f"\n🎉 数据同步成功: {args.sync}")
    elif args.interactive:
        interactive_mode()
    else:
        # 默认显示帮助
        parser.print_help()
        print(f"\n💡 提示: 使用 --interactive 启动交互模式")

if __name__ == '__main__':
    main()