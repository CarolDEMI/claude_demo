"""
主工作流模块 - 整合所有功能的核心工作流
"""
import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

from database import DatabaseManager
from sql_generator import SQLGenerator
from data_manager import DataManager
from data_analyzer import DataAnalyzer
from report_generator import ReportGenerator

load_dotenv()

class DataAnalysisWorkflow:
    def __init__(self):
        """初始化工作流的所有组件"""
        self.db_manager = DatabaseManager()
        self.sql_generator = SQLGenerator()
        self.data_manager = DataManager()
        self.analyzer = DataAnalyzer()
        self.report_generator = ReportGenerator()
        
        # 测试数据库连接
        if not self.db_manager.test_connection():
            print("警告: 数据库连接失败，请检查配置")
    
    def run_analysis(self, 
                    requirement: str, 
                    analysis_type: str = 'comprehensive',
                    save_results: bool = True,
                    generate_report: bool = True) -> Dict[str, Any]:
        """
        运行完整的数据分析工作流
        
        Args:
            requirement: 用户的分析需求描述
            analysis_type: 分析类型 ('basic', 'comprehensive', 'statistical')
            save_results: 是否保存查询结果
            generate_report: 是否生成分析报告
        
        Returns:
            包含所有结果的字典
        """
        print(f"开始分析工作流: {requirement}")
        
        try:
            # 第1步: 获取数据库信息
            print("步骤 1/6: 获取数据库结构信息...")
            db_info = self.db_manager.get_database_info()
            
            # 第2步: 生成SQL查询
            print("步骤 2/6: 生成SQL查询...")
            sql_info = self.sql_generator.generate_sql(requirement, db_info)
            
            # 第3步: 执行查询
            print("步骤 3/6: 执行查询...")
            query_data = self.db_manager.execute_query(sql_info['sql'])
            
            if query_data.empty:
                return {
                    'status': 'warning',
                    'message': '查询返回空结果',
                    'sql_info': sql_info
                }
            
            # 第4步: 保存查询结果
            query_id = None
            if save_results:
                print("步骤 4/6: 保存查询结果...")
                query_info = {
                    'requirement': requirement,
                    'sql_info': sql_info
                }
                query_id = self.data_manager.save_query_result(
                    requirement, sql_info, query_data
                )
                print(f"查询结果已保存，ID: {query_id}")
            else:
                print("步骤 4/6: 跳过保存查询结果")
            
            # 第5步: 数据分析
            print("步骤 5/6: 执行数据分析...")
            analysis_results = self.analyzer.analyze_dataset(query_data, analysis_type)
            insights = self.analyzer.generate_insights(analysis_results, query_data)
            
            # 第6步: 生成报告
            report_path = None
            if generate_report:
                print("步骤 6/6: 生成分析报告...")
                query_info_for_report = {
                    'requirement': requirement,
                    'sql_info': sql_info,
                    'query_id': query_id
                }
                report_path = self.report_generator.generate_comprehensive_report(
                    query_info_for_report, query_data, analysis_results, insights
                )
                print(f"分析报告已生成: {report_path}")
            else:
                print("步骤 6/6: 跳过生成报告")
            
            print("工作流执行完成！")
            
            return {
                'status': 'success',
                'query_id': query_id,
                'sql_info': sql_info,
                'data_shape': query_data.shape,
                'analysis_results': analysis_results,
                'insights': insights,
                'report_path': report_path,
                'data_preview': query_data.head().to_dict('records')
            }
            
        except Exception as e:
            print(f"工作流执行失败: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'requirement': requirement
            }
    
    def get_database_summary(self) -> Dict[str, Any]:
        """获取数据库摘要信息"""
        try:
            db_info = self.db_manager.get_database_info()
            summary = {
                'database_type': db_info['database_type'],
                'table_count': len(db_info['tables']),
                'tables': {}
            }
            
            for table_name, table_info in db_info['tables'].items():
                summary['tables'][table_name] = {
                    'columns': len(table_info['schema']['columns']),
                    'sample_rows': len(table_info['sample_data'])
                }
            
            return summary
        except Exception as e:
            return {'error': str(e)}
    
    def list_saved_queries(self) -> Dict[str, Any]:
        """列出所有保存的查询"""
        try:
            queries = self.data_manager.get_query_list()
            storage_stats = self.data_manager.get_storage_stats()
            
            return {
                'queries': queries,
                'storage_stats': storage_stats
            }
        except Exception as e:
            return {'error': str(e)}
    
    def reanalyze_query(self, 
                       query_id: str, 
                       analysis_type: str = 'comprehensive',
                       generate_report: bool = True) -> Dict[str, Any]:
        """重新分析已保存的查询结果"""
        try:
            # 加载查询结果
            query_data = self.data_manager.load_query_result(query_id)
            query_info = self.data_manager.get_query_info(query_id)
            
            # 执行分析
            analysis_results = self.analyzer.analyze_dataset(query_data, analysis_type)
            insights = self.analyzer.generate_insights(analysis_results, query_data)
            
            # 生成报告
            report_path = None
            if generate_report:
                report_path = self.report_generator.generate_comprehensive_report(
                    query_info, query_data, analysis_results, insights
                )
            
            return {
                'status': 'success',
                'query_id': query_id,
                'analysis_results': analysis_results,
                'insights': insights,
                'report_path': report_path
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'query_id': query_id
            }
    
    def get_sql_suggestions(self, requirement: str) -> Dict[str, Any]:
        """仅生成SQL建议，不执行查询"""
        try:
            db_info = self.db_manager.get_database_info()
            sql_info = self.sql_generator.generate_sql(requirement, db_info)
            optimizations = self.sql_generator.suggest_optimizations(sql_info['sql'], db_info)
            
            return {
                'status': 'success',
                'sql_info': sql_info,
                'optimizations': optimizations
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def health_check(self) -> Dict[str, Any]:
        """检查系统各组件状态"""
        health_status = {
            'database': 'unknown',
            'ai_service': 'unknown',
            'file_system': 'unknown',
            'overall': 'unknown'
        }
        
        # 检查数据库连接
        try:
            if self.db_manager.test_connection():
                health_status['database'] = 'healthy'
            else:
                health_status['database'] = 'unhealthy'
        except Exception:
            health_status['database'] = 'error'
        
        # 检查AI服务
        try:
            # 尝试一个简单的AI调用
            dummy_requirement = "测试连接"
            dummy_db_info = {'database_type': 'test', 'tables': {}}
            self.sql_generator.generate_sql(dummy_requirement, dummy_db_info)
            health_status['ai_service'] = 'healthy'
        except Exception:
            health_status['ai_service'] = 'unhealthy'
        
        # 检查文件系统
        try:
            stats = self.data_manager.get_storage_stats()
            health_status['file_system'] = 'healthy'
        except Exception:
            health_status['file_system'] = 'error'
        
        # 整体状态
        if all(status == 'healthy' for status in [health_status['database'], 
                                                 health_status['ai_service'], 
                                                 health_status['file_system']]):
            health_status['overall'] = 'healthy'
        elif any(status == 'error' for status in health_status.values()):
            health_status['overall'] = 'error'
        else:
            health_status['overall'] = 'degraded'
        
        return health_status