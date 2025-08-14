"""
数据库连接和查询模块
"""
import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.db_type = os.getenv('DB_TYPE', 'sqlite')
        self.engine = self._create_engine()
    
    def _create_engine(self):
        """根据配置创建数据库引擎"""
        if self.db_type == 'sqlite':
            db_path = os.getenv('DB_PATH', './data/data.db')
            return create_engine(f'sqlite:///{db_path}')
        
        elif self.db_type == 'mysql':
            host = os.getenv('DB_HOST', 'localhost')
            port = os.getenv('DB_PORT', '3306')
            db_name = os.getenv('DB_NAME')
            user = os.getenv('DB_USER')
            password = os.getenv('DB_PASSWORD')
            return create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}')
        
        elif self.db_type == 'postgresql':
            host = os.getenv('DB_HOST', 'localhost')
            port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME')
            user = os.getenv('DB_USER')
            password = os.getenv('DB_PASSWORD')
            return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
        
        else:
            raise ValueError(f"不支持的数据库类型: {self.db_type}")
    
    def get_table_names(self) -> List[str]:
        """获取所有表名"""
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """获取表结构"""
        # 白名单验证 - 只允许访问已知的安全表
        ALLOWED_TABLES = [
            'cpz_qs_newuser_channel_i_d',
            'dwd_ttx_market_cash_cost_i_d',
            'dws_ttx_market_media_reports_i_d',
            'dwd_ttx_market_cash_cost_i_d_test'
        ]
        
        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"不允许访问的表: {table_name}")
        
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        
        schema = {
            'table_name': table_name,
            'columns': []
        }
        
        for column in columns:
            schema['columns'].append({
                'name': column['name'],
                'type': str(column['type']),
                'nullable': column['nullable'],
                'default': column.get('default')
            })
        
        return schema
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """获取表的样本数据"""
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return pd.read_sql_query(query, self.engine)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """执行SQL查询并返回结果"""
        try:
            return pd.read_sql_query(query, self.engine)
        except Exception as e:
            raise Exception(f"查询执行失败: {str(e)}")
    
    def get_database_info(self) -> Dict[str, Any]:
        """获取数据库完整信息"""
        tables = self.get_table_names()
        db_info = {
            'database_type': self.db_type,
            'tables': {}
        }
        
        for table in tables:
            schema = self.get_table_schema(table)
            sample_data = self.get_sample_data(table)
            
            db_info['tables'][table] = {
                'schema': schema,
                'sample_data': sample_data.to_dict('records'),
                'row_count': len(sample_data)
            }
        
        return db_info
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            print(f"数据库连接失败: {str(e)}")
            return False