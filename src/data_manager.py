"""
数据管理模块 - 负责保存和管理查询结果
"""
import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

class DataManager:
    def __init__(self):
        self.data_dir = Path(os.getenv('DATA_OUTPUT_DIR', './data_exports'))
        self.report_dir = Path(os.getenv('REPORT_OUTPUT_DIR', './reports'))
        self._ensure_directories()
        
        # 元数据存储文件
        self.metadata_file = self.data_dir / 'query_metadata.json'
        self.metadata = self._load_metadata()
    
    def _ensure_directories(self):
        """确保输出目录存在"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)
    
    def _load_metadata(self) -> Dict[str, Any]:
        """加载查询元数据"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"加载元数据失败: {str(e)}")
        
        return {'queries': {}, 'last_updated': None}
    
    def _save_metadata(self):
        """保存查询元数据"""
        self.metadata['last_updated'] = datetime.now().isoformat()
        try:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"保存元数据失败: {str(e)}")
    
    def save_query_result(self, 
                         requirement: str,
                         sql_info: Dict[str, Any],
                         result_data: pd.DataFrame,
                         query_id: Optional[str] = None) -> str:
        """
        保存查询结果
        
        Args:
            requirement: 原始需求描述
            sql_info: SQL生成信息
            result_data: 查询结果数据
            query_id: 可选的查询ID，如果不提供会自动生成
        
        Returns:
            查询ID
        """
        if query_id is None:
            query_id = self._generate_query_id()
        
        timestamp = datetime.now()
        
        # 保存数据到多种格式
        file_base = self.data_dir / f"query_{query_id}"
        
        # CSV格式
        csv_file = f"{file_base}.csv"
        result_data.to_csv(csv_file, index=False, encoding='utf-8')
        
        # Excel格式
        excel_file = f"{file_base}.xlsx"
        result_data.to_excel(excel_file, index=False, engine='openpyxl')
        
        # JSON格式
        json_file = f"{file_base}.json"
        result_data.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        # 保存元数据
        query_metadata = {
            'query_id': query_id,
            'timestamp': timestamp.isoformat(),
            'requirement': requirement,
            'sql_info': sql_info,
            'result_summary': {
                'row_count': len(result_data),
                'column_count': len(result_data.columns),
                'columns': list(result_data.columns),
                'data_types': result_data.dtypes.astype(str).to_dict(),
                'null_counts': result_data.isnull().sum().to_dict()
            },
            'files': {
                'csv': str(csv_file),
                'excel': str(excel_file),
                'json': str(json_file)
            }
        }
        
        self.metadata['queries'][query_id] = query_metadata
        self._save_metadata()
        
        return query_id
    
    def _generate_query_id(self) -> str:
        """生成唯一的查询ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        count = len(self.metadata['queries']) + 1
        return f"{timestamp}_{count:03d}"
    
    def load_query_result(self, query_id: str, format: str = 'csv') -> pd.DataFrame:
        """
        加载查询结果
        
        Args:
            query_id: 查询ID
            format: 文件格式 ('csv', 'excel', 'json')
        
        Returns:
            查询结果数据
        """
        if query_id not in self.metadata['queries']:
            raise ValueError(f"查询ID不存在: {query_id}")
        
        query_info = self.metadata['queries'][query_id]
        file_path = query_info['files'].get(format)
        
        if not file_path or not Path(file_path).exists():
            raise FileNotFoundError(f"查询结果文件不存在: {file_path}")
        
        if format == 'csv':
            return pd.read_csv(file_path)
        elif format == 'excel':
            return pd.read_excel(file_path)
        elif format == 'json':
            return pd.read_json(file_path)
        else:
            raise ValueError(f"不支持的格式: {format}")
    
    def get_query_list(self) -> List[Dict[str, Any]]:
        """获取所有查询的列表"""
        queries = []
        for query_id, info in self.metadata['queries'].items():
            queries.append({
                'query_id': query_id,
                'timestamp': info['timestamp'],
                'requirement': info['requirement'],
                'row_count': info['result_summary']['row_count'],
                'column_count': info['result_summary']['column_count']
            })
        
        # 按时间倒序排列
        queries.sort(key=lambda x: x['timestamp'], reverse=True)
        return queries
    
    def get_query_info(self, query_id: str) -> Dict[str, Any]:
        """获取查询的详细信息"""
        if query_id not in self.metadata['queries']:
            raise ValueError(f"查询ID不存在: {query_id}")
        
        return self.metadata['queries'][query_id]
    
    def delete_query(self, query_id: str) -> bool:
        """删除查询及其相关文件"""
        if query_id not in self.metadata['queries']:
            return False
        
        query_info = self.metadata['queries'][query_id]
        
        # 删除文件
        for file_path in query_info['files'].values():
            try:
                Path(file_path).unlink(missing_ok=True)
            except Exception as e:
                print(f"删除文件失败 {file_path}: {str(e)}")
        
        # 从元数据中删除
        del self.metadata['queries'][query_id]
        self._save_metadata()
        
        return True
    
    def export_query_summary(self, output_file: Optional[str] = None) -> str:
        """导出查询摘要报告"""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = str(self.report_dir / f"query_summary_{timestamp}.json")
        
        summary = {
            'total_queries': len(self.metadata['queries']),
            'last_updated': self.metadata['last_updated'],
            'queries': []
        }
        
        for query_id, info in self.metadata['queries'].items():
            summary['queries'].append({
                'query_id': query_id,
                'timestamp': info['timestamp'],
                'requirement': info['requirement'],
                'sql': info['sql_info']['sql'],
                'row_count': info['result_summary']['row_count'],
                'columns': info['result_summary']['columns']
            })
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        return output_file
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        total_files = 0
        total_size = 0
        
        for query_id, info in self.metadata['queries'].items():
            for file_path in info['files'].values():
                if Path(file_path).exists():
                    total_files += 1
                    total_size += Path(file_path).stat().st_size
        
        return {
            'total_queries': len(self.metadata['queries']),
            'total_files': total_files,
            'total_size_bytes': total_size,
            'total_size_mb': round(total_size / (1024 * 1024), 2),
            'data_directory': str(self.data_dir),
            'report_directory': str(self.report_dir)
        }