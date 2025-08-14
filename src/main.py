#!/usr/bin/env python3
"""
数据分析工作流 - 命令行接口
"""
import argparse
import json
import sys
from pathlib import Path
from workflow import DataAnalysisWorkflow

def main():
    parser = argparse.ArgumentParser(description='数据分析工作流 - 自动SQL生成、执行和分析')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 分析命令
    analyze_parser = subparsers.add_parser('analyze', help='执行数据分析')
    analyze_parser.add_argument('requirement', help='分析需求描述')
    analyze_parser.add_argument('--type', choices=['basic', 'comprehensive', 'statistical'], 
                               default='comprehensive', help='分析类型')
    analyze_parser.add_argument('--no-save', action='store_true', help='不保存查询结果')
    analyze_parser.add_argument('--no-report', action='store_true', help='不生成报告')
    
    # 列出查询命令
    list_parser = subparsers.add_parser('list', help='列出所有保存的查询')
    
    # 重新分析命令
    reanalyze_parser = subparsers.add_parser('reanalyze', help='重新分析已保存的查询')
    reanalyze_parser.add_argument('query_id', help='查询ID')
    reanalyze_parser.add_argument('--type', choices=['basic', 'comprehensive', 'statistical'], 
                                 default='comprehensive', help='分析类型')
    reanalyze_parser.add_argument('--no-report', action='store_true', help='不生成报告')
    
    # SQL建议命令
    sql_parser = subparsers.add_parser('sql', help='仅生成SQL建议')
    sql_parser.add_argument('requirement', help='分析需求描述')
    
    # 数据库摘要命令
    db_parser = subparsers.add_parser('database', help='显示数据库摘要信息')
    
    # 健康检查命令
    health_parser = subparsers.add_parser('health', help='检查系统状态')
    
    # 演示命令
    demo_parser = subparsers.add_parser('demo', help='运行演示示例')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 初始化工作流
    try:
        workflow = DataAnalysisWorkflow()
    except Exception as e:
        print(f"错误: 工作流初始化失败 - {str(e)}")
        print("请检查 .env 配置文件和数据库连接")
        return
    
    # 执行命令
    if args.command == 'analyze':
        result = workflow.run_analysis(
            requirement=args.requirement,
            analysis_type=args.type,
            save_results=not args.no_save,
            generate_report=not args.no_report
        )
        print_result(result)
    
    elif args.command == 'list':
        result = workflow.list_saved_queries()
        print_query_list(result)
    
    elif args.command == 'reanalyze':
        result = workflow.reanalyze_query(
            query_id=args.query_id,
            analysis_type=args.type,
            generate_report=not args.no_report
        )
        print_result(result)
    
    elif args.command == 'sql':
        result = workflow.get_sql_suggestions(args.requirement)
        print_sql_suggestions(result)
    
    elif args.command == 'database':
        result = workflow.get_database_summary()
        print_database_summary(result)
    
    elif args.command == 'health':
        result = workflow.health_check()
        print_health_status(result)
    
    elif args.command == 'demo':
        run_demo(workflow)

def print_result(result):
    """打印分析结果"""
    if result['status'] == 'success':
        print("✅ 分析完成!")
        print(f"   数据形状: {result['data_shape'][0]} 行 × {result['data_shape'][1]} 列")
        
        if result.get('query_id'):
            print(f"   查询ID: {result['query_id']}")
        
        if result.get('report_path'):
            print(f"   报告路径: {result['report_path']}")
        
        print("\n📊 关键洞察:")
        for insight in result.get('insights', []):
            print(f"   • {insight}")
        
        print("\n🔍 数据预览:")
        for i, row in enumerate(result.get('data_preview', [])[:3]):
            print(f"   行{i+1}: {row}")
    
    elif result['status'] == 'warning':
        print(f"⚠️  警告: {result['message']}")
        if 'sql_info' in result:
            print(f"   SQL: {result['sql_info']['sql']}")
    
    else:
        print(f"❌ 错误: {result.get('error', '未知错误')}")

def print_query_list(result):
    """打印查询列表"""
    if 'error' in result:
        print(f"❌ 错误: {result['error']}")
        return
    
    queries = result.get('queries', [])
    storage_stats = result.get('storage_stats', {})
    
    print(f"📋 保存的查询 (共 {len(queries)} 个):")
    print()
    
    for query in queries:
        print(f"🔍 {query['query_id']}")
        print(f"   时间: {query['timestamp']}")
        print(f"   需求: {query['requirement'][:60]}...")
        print(f"   数据: {query['row_count']} 行 × {query['column_count']} 列")
        print()
    
    print(f"💾 存储统计:")
    print(f"   总查询数: {storage_stats.get('total_queries', 0)}")
    print(f"   总文件数: {storage_stats.get('total_files', 0)}")
    print(f"   存储大小: {storage_stats.get('total_size_mb', 0)} MB")

def print_sql_suggestions(result):
    """打印SQL建议"""
    if result['status'] == 'error':
        print(f"❌ 错误: {result['error']}")
        return
    
    sql_info = result['sql_info']
    print("🔍 生成的SQL查询:")
    print(f"```sql\n{sql_info['sql']}\n```")
    print()
    print(f"📝 查询说明: {sql_info['explanation']}")
    print()
    print(f"📊 使用的表: {', '.join(sql_info['tables_used'])}")
    print(f"📋 预期列: {', '.join(sql_info['expected_columns'])}")
    
    if result.get('optimizations'):
        print("\n⚡ 优化建议:")
        for opt in result['optimizations']:
            print(f"   • {opt}")

def print_database_summary(result):
    """打印数据库摘要"""
    if 'error' in result:
        print(f"❌ 错误: {result['error']}")
        return
    
    print(f"🗄️  数据库摘要:")
    print(f"   类型: {result.get('database_type', 'unknown')}")
    print(f"   表数量: {result.get('table_count', 0)}")
    print()
    
    for table_name, table_info in result.get('tables', {}).items():
        print(f"📋 {table_name}")
        print(f"   列数: {table_info['columns']}")
        print(f"   样本行数: {table_info['sample_rows']}")

def print_health_status(result):
    """打印健康状态"""
    status_icons = {
        'healthy': '✅',
        'unhealthy': '⚠️',
        'error': '❌',
        'unknown': '❓'
    }
    
    print("🏥 系统健康检查:")
    print(f"   数据库: {status_icons.get(result['database'], '❓')} {result['database']}")
    print(f"   AI服务: {status_icons.get(result['ai_service'], '❓')} {result['ai_service']}")
    print(f"   文件系统: {status_icons.get(result['file_system'], '❓')} {result['file_system']}")
    print(f"   整体状态: {status_icons.get(result['overall'], '❓')} {result['overall']}")
    
    if result['overall'] != 'healthy':
        print("\n💡 建议检查:")
        if result['database'] != 'healthy':
            print("   • 数据库连接配置 (.env 文件)")
        if result['ai_service'] != 'healthy':
            print("   • AI API密钥配置")
        if result['file_system'] != 'healthy':
            print("   • 文件系统权限和磁盘空间")

def run_demo(workflow):
    """运行演示"""
    print("🚀 运行演示示例...")
    
    # 检查系统状态
    health = workflow.health_check()
    if health['overall'] != 'healthy':
        print("⚠️  系统状态不健康，演示可能无法正常运行")
        print_health_status(health)
        print()
    
    # 显示数据库信息
    db_summary = workflow.get_database_summary()
    if 'error' not in db_summary:
        print_database_summary(db_summary)
        print()
    
    # 示例分析需求
    demo_requirements = [
        "显示所有数据的基本统计信息",
        "找出数量最多的前10个类别",
        "分析数值字段的分布情况",
        "检查是否有异常值或缺失值"
    ]
    
    print("📝 可以尝试的分析需求示例:")
    for i, req in enumerate(demo_requirements, 1):
        print(f"   {i}. {req}")
    
    print("\n💡 使用方法:")
    print("   python main.py analyze \"你的分析需求\"")
    print("   python main.py sql \"你的查询需求\"")
    print("   python main.py list")

if __name__ == '__main__':
    main()