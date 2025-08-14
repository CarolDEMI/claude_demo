#!/usr/bin/env python3
"""
统一配置报告系统 - 主入口
提供所有功能的统一访问入口
"""

import sys
import os
import time
import threading
from datetime import datetime
from typing import Optional
import subprocess

# 添加src目录到路径
sys.path.append('./src')
from user_friendly_errors import error_handler, safe_execute

def print_banner():
    """显示系统横幅"""
    # 清屏效果
    os.system('clear' if os.name == 'posix' else 'cls')
    
    # 渐进式显示横幅
    banner_lines = [
        "╔" + "═" * 68 + "╗",
        "║" + " " * 68 + "║", 
        "║" + "🚀 统一配置报告系统 (Unified Config Report System)".center(68) + "║",
        "║" + " " * 68 + "║",
        "║" + f"⏰ 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "║",
        "║" + "🔧 版本: 2.0 (统一配置版)".center(68) + "║",
        "║" + "💡 提示: 输入数字选择功能，输入 'help' 查看帮助".center(68) + "║",
        "║" + " " * 68 + "║",
        "╚" + "═" * 68 + "╝"
    ]
    
    for line in banner_lines:
        print(line)
        time.sleep(0.1)  # 渐进显示效果

def print_menu():
    """显示主菜单"""
    print("\n" + "═" * 60)
    print("📋 功能菜单".center(60))
    print("═" * 60)
    
    menu_items = [
        ("🏥 系统管理", [
            ("1", "系统健康检查", "检查系统运行状态和组件健康度"),
            ("2", "合规性监控", "监控代码合规性和配置一致性"), 
            ("3", "查看系统配置", "查看当前系统配置和参数")
        ]),
        ("📊 报告生成", [
            ("4", "生成日报", "生成指定日期的数据分析日报"),
            ("5", "生成自定义报告", "自定义报告内容和格式")
        ]),
        ("🔄 数据管理", [
            ("6", "数据同步", "从Presto数据库同步最新数据"),
            ("7", "查看数据状态", "查看本地数据库状态和统计")
        ]),
        ("🔧 高级功能", [
            ("8", "配置编辑", "编辑系统配置文件"),
            ("9", "自动修复", "自动检测并修复系统问题")
        ])
    ]
    
    for category, items in menu_items:
        print(f"\n{category}")
        for num, name, desc in items:
            print(f"  {num}. {name:<15} - {desc}")
    
    print("\n" + "─" * 60)
    print("  0. 退出系统        - 安全退出程序")
    print("  help. 帮助信息      - 显示详细使用说明")
    print("═" * 60)

def show_progress_bar(duration: float = 3.0, message: str = "正在处理"):
    """显示进度条"""
    print(f"\n{message}...")
    bar_length = 40
    for i in range(bar_length + 1):
        percent = (i / bar_length) * 100
        filled = '█' * i
        empty = '░' * (bar_length - i)
        print(f"\r[[32m{filled}[0m{empty}] {percent:6.1f}%", end='', flush=True)
        time.sleep(duration / bar_length)
    print("  ✓ 完成!")

def run_with_feedback(command: str, description: str, success_msg: str = "操作成功") -> bool:
    """带反馈的命令执行，包含友好错误处理"""
    print(f"\n🚀 {description}...")
    show_progress_bar(2.0, description)
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print(f"\n✅ {success_msg}")
            if result.stdout.strip():
                print(f"\n📝 输出信息:\n{result.stdout}")
            return True
        else:
            # 使用友好错误处理
            stderr_msg = result.stderr.strip()
            if stderr_msg:
                # 创建异常来使用错误处理器
                error = Exception(stderr_msg)
                error_handler.print_friendly_error(error, f"执行命令: {command}")
            else:
                print(f"\n⚠️  操作未成功 (Exit Code: {result.returncode})")
                if result.stdout.strip():
                    print(f"📝 部分输出: {result.stdout.strip()}")
                print("💡 建议: 尝试运行系统健康检查")
                print("🔧 快速诊断: python3 main.py --health")
            return False
    except subprocess.TimeoutExpired:
        print(f"\n⏱️  操作超时60秒")
        print("💡 可能的原因:")
        print("   1. 网络连接缓慢")
        print("   2. 数据量过大")
        print("   3. 服务器响应慢")
        print("🔧 建议: 分批处理或检查网络连接")
        return False
    except Exception as e:
        error_handler.print_friendly_error(e, f"执行命令: {command}")
        return False

def run_system_check():
    """运行系统健康检查"""
    run_with_feedback(
        "python3 utils/integrated_report_system.py --check",
        "执行系统健康检查",
        "系统健康检查完成"
    )

def run_compliance_check():
    """运行合规性检查"""
    run_with_feedback(
        "python3 utils/compliance_monitor.py",
        "执行合规性检查", 
        "合规性检查完成"
    )

def view_system_config():
    """查看系统配置"""
    run_with_feedback(
        "python3 utils/unified_config_system.py",
        "加载系统配置信息",
        "配置信息加载完成"
    )

def generate_custom_report():
    """生成自定义报告"""
    print("\n🎨 自定义报告生成器")
    print("═" * 50)
    
    # 报告类型选择
    report_types = [
        ("1", "快速报告", "生成最近7天的简化数据报告"),
        ("2", "详细分析", "包含所有模块的完整数据分析"),
        ("3", "对比报告", "比较两个日期的数据变化"),
        ("4", "趋势分析", "显示最近30天的数据趋势")
    ]
    
    print("📋 可用的报告类型:")
    for num, name, desc in report_types:
        print(f"  {num}. {name:<12} - {desc}")
    
    report_type = get_user_input_with_validation(
        "\n🎯 请选择报告类型 (1-4): ",
        lambda x: x in ['1', '2', '3', '4']
    )
    
    if report_type == '1':
        generate_quick_report()
    elif report_type == '2':
        generate_detailed_report()
    elif report_type == '3':
        generate_comparison_report()
    elif report_type == '4':
        generate_trend_report()

def generate_quick_report():
    """生成快速报告"""
    from datetime import datetime, timedelta
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)
    
    print(f"\n🚀 生成 {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')} 的快速报告...")
    
    success = run_with_feedback(
        f"python3 integrated_report_system.py --report {end_date.strftime('%Y-%m-%d')} --quick",
        "生成快速数据报告",
        "快速报告生成完成"
    )
    
    if not success:
        print("⚠️  尝试使用标准报告格式...")
        run_with_feedback(
            f"python3 integrated_report_system.py --report {end_date.strftime('%Y-%m-%d')}",
            "生成标准报告",
            "报告生成完成"
        )

def generate_detailed_report():
    """生成详细分析报告"""
    from datetime import datetime, timedelta
    default_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    date = get_user_input_with_validation(
        f"📅 请输入要分析的日期 (默认: {default_date}): ",
        validate_date,
        default_date
    )
    
    success = run_with_feedback(
        f"python3 integrated_report_system.py --report {date} --detailed",
        f"生成 {date} 的详细分析报告",
        "详细报告生成完成"
    )
    
    if not success:
        print("⚠️  尝试使用标准报告格式...")
        run_with_feedback(
            f"python3 utils/integrated_report_system.py --report {date}",
            "生成标准报告",
            "报告生成完成"
        )

def generate_comparison_report():
    """生成对比报告"""
    print("🔄 对比分析需要两个日期")
    
    from datetime import datetime, timedelta
    default_date1 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    default_date2 = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    
    date1 = get_user_input_with_validation(
        f"📅 输入第一个日期 (默认: {default_date1}): ",
        validate_date,
        default_date1
    )
    
    date2 = get_user_input_with_validation(
        f"📅 输入第二个日期 (默认: {default_date2}): ",
        validate_date,
        default_date2
    )
    
    print(f"\n🚀 生成 {date1} 与 {date2} 的对比分析...")
    
    # 由于对比功能可能未实现，生成两个单独的报告
    print("📄 正在生成两个日期的报告供对比...")
    
    success1 = run_with_feedback(
        f"python3 integrated_report_system.py --report {date1}",
        f"生成 {date1} 报告",
        f"{date1} 报告完成"
    )
    
    success2 = run_with_feedback(
        f"python3 integrated_report_system.py --report {date2}",
        f"生成 {date2} 报告",
        f"{date2} 报告完成"
    )
    
    if success1 and success2:
        print(f"\n✅ 对比报告生成完成，可在浏览器中同时打开两个报告进行对比")

def generate_trend_report():
    """生成趋势分析报告"""
    print("📈 趋势分析将显示最近30天的数据变化")
    
    from datetime import datetime, timedelta
    end_date = datetime.now() - timedelta(days=1)
    
    print(f"\n🚀 生成至 {end_date.strftime('%Y-%m-%d')} 的趋势分析...")
    
    # 由于趋势分析可能未实现，使用最新日期的报告
    success = run_with_feedback(
        f"python3 integrated_report_system.py --report {end_date.strftime('%Y-%m-%d')} --trend",
        "生成趋势分析报告",
        "趋势报告生成完成"
    )
    
    if not success:
        print("⚠️  趋势分析功能暂未实现，使用标准报告代替...")
        run_with_feedback(
            f"python3 integrated_report_system.py --report {end_date.strftime('%Y-%m-%d')}",
            "生成标准报告",
            "报告生成完成"
        )
        print("💡 建议: 可手动生成多个日期的报告来观察趋势变化")

def show_help():
    """显示帮助信息"""
    print("\n" + "═" * 60)
    print("📚 系统使用帮助".center(60))
    print("═" * 60)
    
    help_sections = [
        ("📄 基本操作", [
            "• 输入数字 1-9 选择相应功能",
            "• 输入 0 退出系统",
            "• 输入 'help' 显示本帮助信息",
            "• 按 Ctrl+C 可以随时退出"
        ]),
        ("📅 日期格式", [
            "• 正确格式: YYYY-MM-DD (例: 2025-01-15)", 
            "• 大部分操作都有默认值，直接回车即可",
            "• 默认日期通常是昨天"
        ]),
        ("📊 报告功能", [
            "• 日报: 生成指定日期的完整数据分析",
            "• 快速报告: 最近7天的简化数据",
            "• 对比报告: 比较两个日期的数据",
            "• 报告生成后可选择在浏览器中打开"
        ]),
        ("🔄 数据同步", [
            "• 只支持从Presto数据库同步真实数据",
            "• 同步前请确保网络连接正常", 
            "• 同步完成后建议查看数据状态验证结果"
        ]),
        ("⚠️ 注意事项", [
            "• 系统只处理真实数据，不支持模拟数据",
            "• 首次使用建议先运行系统健康检查",
            "• 如遇错误，可尝试运行自动修复功能",
            "• 重要操作会有进度条显示，请耐心等待"
        ])
    ]
    
    for title, items in help_sections:
        print(f"\n{title}")
        for item in items:
            print(f"  {item}")
    
    print("\n" + "═" * 60)
    print("🚀 更多技术文档请查看 docs/ 目录")
    print("═" * 60)

def validate_date(date_str: str) -> bool:
    """验证日期格式"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_user_input_with_validation(prompt: str, validator=None, default=None) -> str:
    """带验证的用户输入"""
    while True:
        user_input = input(f"\n{prompt}").strip()
        
        if not user_input and default is not None:
            return default
            
        if validator is None or validator(user_input):
            return user_input
        else:
            print("❌ 输入格式不正确，请重试")

def generate_report():
    """生成报告"""
    print("\n📊 报告生成向导")
    print("─" * 40)
    
    # 默认日期是昨天
    from datetime import datetime, timedelta
    default_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    date = get_user_input_with_validation(
        f"📅 请输入报告日期 (YYYY-MM-DD, 默认: {default_date}): ",
        validate_date,
        default_date
    )
    
    print(f"\n🚀 开始生成 {date} 的报告...")
    
    success = run_with_feedback(
        f"python3 utils/integrated_report_system.py --report {date}",
        f"生成 {date} 的数据分析报告",
        "报告生成完成"
    )
    
    if success:
        # 查找生成的报告文件
        possible_files = [
            f"daily_report_{date}.html",
            f"output/reports/daily_reports/daily_report_{date.replace('-', '')}_*.html"
        ]
        
        report_file = None
        for pattern in possible_files:
            if '*' in pattern:
                import glob
                files = glob.glob(pattern)
                if files:
                    report_file = files[-1]  # 取最新的
                    break
            elif os.path.exists(pattern):
                report_file = pattern
                break
        
        if report_file:
            print(f"\n🎉 报告生成成功: {report_file}")
            open_choice = get_user_input_with_validation(
                "🔍 是否在浏览器中打开报告? (y/N): ",
                lambda x: x.lower() in ['y', 'yes', 'n', 'no', ''],
                'n'
            )
            if open_choice.lower() in ['y', 'yes']:
                print("🌐 正在打开浏览器...")
                os.system(f"open {report_file}")
        else:
            print("⚠️  报告生成完成，但未找到输出文件")

def sync_data():
    """数据同步 - 仅支持真实数据"""
    print("\n🔄 数据同步向导")
    print("─" * 40)
    print("📋 注意: 只能从Presto数据库同步真实数据")
    
    # 默认同步昨天的数据
    from datetime import datetime, timedelta
    default_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    date = get_user_input_with_validation(
        f"📅 请输入要同步的日期 (YYYY-MM-DD, 默认: {default_date}): ",
        validate_date,
        default_date
    )
    
    if date:
        success = run_with_feedback(
            f"python3 daily_sync_real.py {date}",
            f"从Presto同步 {date} 的真实数据",
            "数据同步完成"
        )
        
        if success:
            print("\n✅ 建议接下来运行 '查看数据状态' 验证同步结果")

def view_data_status():
    """查看数据状态"""
    print("\n📈 数据状态检查...")
    os.system("python3 -c \"import sqlite3; conn = sqlite3.connect('data.db'); cursor = conn.cursor(); cursor.execute('SELECT dt, COUNT(*) FROM cpz_qs_newuser_channel_i_d GROUP BY dt ORDER BY dt DESC LIMIT 10'); print('\\n最近10天数据状态:'); [print(f'  {row[0]}: {row[1]:,} 条记录') for row in cursor.fetchall()]; conn.close()\"")

def edit_config():
    """配置编辑"""
    print("\n⚙️ 配置编辑...")
    print("配置文件位置: config/unified_config.yaml")
    edit_choice = input("是否要打开配置文件进行编辑? (y/N): ").strip().lower()
    if edit_choice == 'y':
        os.system("open config/unified_config.yaml")
        input("\n按回车键继续 (完成编辑后)...")
        print("验证配置...")
        os.system("python3 utils/unified_config_system.py")

def auto_fix():
    """自动修复"""
    run_with_feedback(
        "python3 utils/auto_fix_compliance.py",
        "执行自动修复操作",
        "自动修复完成"
    )

def main():
    """主函数"""
    print_banner()
    
    while True:
        print_menu()
        choice = input("\n请选择功能 (0-9): ").strip()
        
        if choice == '0':
            print("\n👋 感谢使用统一配置报告系统!")
            print("系统已退出。")
            break
        elif choice == '1':
            run_system_check()
        elif choice == '2':
            run_compliance_check()
        elif choice == '3':
            view_system_config()
        elif choice == '4':
            generate_report()
        elif choice == '5':
            generate_custom_report()
        elif choice == '6':
            sync_data()
        elif choice == '7':
            view_data_status()
        elif choice == '8':
            edit_config()
        elif choice == '9':
            auto_fix()
        elif choice == 'help':
            show_help()
        else:
            print("\n❌ 无效选择，请重试")
            print("💡 提示: 输入 0-9 选择功能，或输入 'help' 查看帮助")
        
        if choice != '0':
            input("\n按回车键继续...")

def parse_command_line():
    """解析命令行参数，支持快捷操作"""
    import argparse
    parser = argparse.ArgumentParser(description='数据分析系统 - 支持快捷操作')
    
    # 快捷命令
    parser.add_argument('--report', type=str, help='生成指定日期的报告 (格式: YYYY-MM-DD 或 yesterday/today)')
    parser.add_argument('--sync', type=str, help='同步指定日期的数据 (格式: YYYY-MM-DD 或 yesterday/today)')
    parser.add_argument('--status', action='store_true', help='快速查看数据状态')
    parser.add_argument('--health', action='store_true', help='系统健康检查')
    parser.add_argument('--auto-open', action='store_true', help='自动打开生成的报告')
    parser.add_argument('--batch', type=str, nargs=2, metavar=('start', 'end'), help='批量同步日期范围 start end')
    
    return parser.parse_args()

def parse_date_input(date_str):
    """解析日期输入，支持 yesterday, today 等"""
    from datetime import datetime, timedelta
    
    if date_str == 'yesterday':
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    elif date_str == 'today':
        return datetime.now().strftime('%Y-%m-%d')
    elif date_str == 'last-week':
        return (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    else:
        # 验证日期格式
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str
        except ValueError:
            print(f"❌ 日期格式错误: {date_str}")
            print("💡 支持格式: YYYY-MM-DD, yesterday, today, last-week")
            sys.exit(1)

def quick_report(date_str, auto_open=False):
    """快速生成报告"""
    date = parse_date_input(date_str)
    print(f"📊 正在生成 {date} 的报告...")
    
    success = run_with_feedback(
        f"python3 utils/integrated_report_system.py --report {date}",
        f"生成 {date} 的数据分析报告",
        "报告生成完成"
    )
    
    if success and auto_open:
        # 查找生成的报告文件
        import glob
        possible_files = [
            f"daily_report_{date}.html",
            f"output/reports/daily_reports/daily_report_{date.replace('-', '')}_*.html"
        ]
        
        report_file = None
        for pattern in possible_files:
            if '*' in pattern:
                files = glob.glob(pattern)
                if files:
                    report_file = files[-1]
                    break
            elif os.path.exists(pattern):
                report_file = pattern
                break
        
        if report_file:
            print("🌐 自动打开报告...")
            os.system(f"open {report_file}")

def quick_sync(date_str):
    """快速同步数据"""
    date = parse_date_input(date_str)
    print(f"🔄 正在同步 {date} 的数据...")
    
    success = run_with_feedback(
        f"python3 daily_sync_real.py {date}",
        f"从Presto同步 {date} 的数据",
        "数据同步完成"
    )
    
    if success:
        print("✅ 建议接下来生成报告验证数据")
        print(f"💡 快捷命令: python3 main.py --report {date} --auto-open")

def quick_status():
    """快速状态检查"""
    print("📊 快速数据状态检查...")
    
    try:
        import sqlite3
        conn = sqlite3.connect('./data/data.db')
        cursor = conn.cursor()
        
        # 获取最新数据日期
        cursor.execute('SELECT MAX(dt) FROM cpz_qs_newuser_channel_i_d')
        latest_date = cursor.fetchone()[0]
        
        if latest_date:
            from datetime import datetime
            latest_dt = datetime.strptime(latest_date, '%Y-%m-%d')
            days_old = (datetime.now() - latest_dt).days
            
            print(f"📅 最新数据: {latest_date} ({days_old}天前)")
            
            if days_old <= 1:
                print("✅ 数据新鲜度良好")
            elif days_old <= 3:
                print("⚠️ 数据稍微滞后，建议同步")
            else:
                print("❌ 数据过期，需要立即同步")
                print(f"💡 快捷命令: python3 main.py --sync yesterday")
        
        # 获取最近3天的数据概况
        cursor.execute('''
            SELECT dt, COUNT(*) as records, SUM(newuser) as users, 
                   ROUND(SUM(zizhu_revenue_1)/SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END), 2) as arpu
            FROM cpz_qs_newuser_channel_i_d 
            WHERE dt >= date('now', '-3 days')
            GROUP BY dt ORDER BY dt DESC
        ''')
        
        results = cursor.fetchall()
        
        if results:
            print("\n📈 最近3天数据概况:")
            print("日期         记录数    用户数    ARPU")
            print("-" * 40)
            for dt, records, users, arpu in results:
                arpu_str = f"{arpu:.2f}" if arpu else "N/A"
                print(f"{dt}   {records:>6,}   {users:>6,}   {arpu_str:>6}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 状态检查失败: {e}")
        print("💡 建议运行: python3 main.py --health")

def batch_sync(start_date, end_date):
    """批量同步日期范围"""
    start = parse_date_input(start_date)
    end = parse_date_input(end_date)
    
    print(f"🔄 批量同步: {start} 到 {end}")
    
    success = run_with_feedback(
        f"python3 src/presto_sync.py --start-date {start} --end-date {end} --sync-missing",
        f"批量同步 {start} 到 {end} 的数据",
        "批量同步完成"
    )
    
    if success:
        print("✅ 建议运行状态检查验证结果")
        print("💡 快捷命令: python3 main.py --status")

def handle_command_line():
    """处理命令行参数"""
    args = parse_command_line()
    
    # 如果没有参数，进入交互模式
    if not any(vars(args).values()):
        return False
    
    # 处理快捷命令
    if args.report:
        quick_report(args.report, args.auto_open)
    elif args.sync:
        quick_sync(args.sync)
    elif args.status:
        quick_status()
    elif args.health:
        run_system_check()
    elif args.batch:
        batch_sync(args.batch[0], args.batch[1])
    
    return True

if __name__ == '__main__':
    try:
        # 首先检查是否有命令行参数
        if handle_command_line():
            # 有命令行参数，直接执行后退出
            pass
        else:
            # 没有命令行参数，进入交互模式
            main()
    except KeyboardInterrupt:
        print("\n\n👋 用户取消操作，系统退出。")
    except Exception as e:
        print(f"\n❌ 系统错误: {e}")
        print("请检查系统状态或联系技术支持。")