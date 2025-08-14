"""
数据分析模块 - 对查询结果进行统计分析和可视化
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

class DataAnalyzer:
    def __init__(self, output_dir: str = './reports'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置可视化样式
        sns.set_style("whitegrid")
        plt.style.use('default')
    
    def analyze_dataset(self, data: pd.DataFrame, analysis_type: str = 'comprehensive') -> Dict[str, Any]:
        """
        对数据集进行综合分析
        
        Args:
            data: 要分析的数据
            analysis_type: 分析类型 ('basic', 'comprehensive', 'statistical')
        
        Returns:
            分析结果字典
        """
        analysis_results = {
            'basic_info': self._get_basic_info(data),
            'summary_statistics': self._get_summary_statistics(data),
            'data_quality': self._analyze_data_quality(data)
        }
        
        if analysis_type in ['comprehensive', 'statistical']:
            analysis_results.update({
                'correlation_analysis': self._analyze_correlations(data),
                'distribution_analysis': self._analyze_distributions(data),
                'outlier_analysis': self._detect_outliers(data)
            })
        
        if analysis_type == 'comprehensive':
            analysis_results.update({
                'pattern_analysis': self._analyze_patterns(data),
                'categorical_analysis': self._analyze_categorical_data(data)
            })
        
        return analysis_results
    
    def _get_basic_info(self, data: pd.DataFrame) -> Dict[str, Any]:
        """获取数据基本信息"""
        return {
            'shape': data.shape,
            'columns': list(data.columns),
            'data_types': data.dtypes.astype(str).to_dict(),
            'memory_usage': data.memory_usage(deep=True).sum(),
            'duplicate_rows': data.duplicated().sum()
        }
    
    def _get_summary_statistics(self, data: pd.DataFrame) -> Dict[str, Any]:
        """获取汇总统计信息"""
        numeric_data = data.select_dtypes(include=[np.number])
        categorical_data = data.select_dtypes(include=['object', 'category'])
        
        summary = {}
        
        if not numeric_data.empty:
            summary['numeric'] = {
                'describe': numeric_data.describe().to_dict(),
                'skewness': numeric_data.skew().to_dict(),
                'kurtosis': numeric_data.kurtosis().to_dict()
            }
        
        if not categorical_data.empty:
            summary['categorical'] = {}
            for col in categorical_data.columns:
                summary['categorical'][col] = {
                    'unique_count': data[col].nunique(),
                    'top_values': data[col].value_counts().head(10).to_dict(),
                    'missing_count': data[col].isnull().sum()
                }
        
        return summary
    
    def _analyze_data_quality(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析数据质量"""
        quality_report = {
            'missing_values': data.isnull().sum().to_dict(),
            'missing_percentage': (data.isnull().sum() / len(data) * 100).to_dict(),
            'completeness_score': (1 - data.isnull().sum().sum() / (len(data) * len(data.columns))) * 100
        }
        
        # 检测异常值
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        quality_report['potential_issues'] = []
        
        for col in numeric_columns:
            if data[col].isnull().sum() / len(data) > 0.5:
                quality_report['potential_issues'].append(f"{col}: 超过50%的缺失值")
            
            if data[col].nunique() == 1:
                quality_report['potential_issues'].append(f"{col}: 所有值都相同")
        
        return quality_report
    
    def _analyze_correlations(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析数值变量之间的相关性"""
        numeric_data = data.select_dtypes(include=[np.number])
        
        if numeric_data.shape[1] < 2:
            return {'message': '数值列少于2个，无法进行相关性分析'}
        
        correlation_matrix = numeric_data.corr()
        
        # 找出高相关性的变量对
        high_correlations = []
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                corr_value = correlation_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:  # 高相关性阈值
                    high_correlations.append({
                        'var1': correlation_matrix.columns[i],
                        'var2': correlation_matrix.columns[j],
                        'correlation': round(corr_value, 3)
                    })
        
        return {
            'correlation_matrix': correlation_matrix.to_dict(),
            'high_correlations': high_correlations,
            'summary': f"发现 {len(high_correlations)} 对高相关性变量"
        }
    
    def _analyze_distributions(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析数据分布"""
        numeric_data = data.select_dtypes(include=[np.number])
        distribution_analysis = {}
        
        for col in numeric_data.columns:
            col_data = numeric_data[col].dropna()
            if len(col_data) == 0:
                continue
                
            distribution_analysis[col] = {
                'mean': float(col_data.mean()),
                'median': float(col_data.median()),
                'std': float(col_data.std()),
                'min': float(col_data.min()),
                'max': float(col_data.max()),
                'quartiles': {
                    'Q1': float(col_data.quantile(0.25)),
                    'Q3': float(col_data.quantile(0.75))
                },
                'distribution_type': self._identify_distribution_type(col_data)
            }
        
        return distribution_analysis
    
    def _identify_distribution_type(self, data: pd.Series) -> str:
        """识别数据分布类型"""
        skewness = data.skew()
        kurtosis = data.kurtosis()
        
        if abs(skewness) < 0.5:
            if abs(kurtosis) < 3:
                return "近似正态分布"
            else:
                return "对称但非正态"
        elif skewness > 0.5:
            return "右偏分布"
        else:
            return "左偏分布"
    
    def _detect_outliers(self, data: pd.DataFrame) -> Dict[str, Any]:
        """检测异常值"""
        numeric_data = data.select_dtypes(include=[np.number])
        outlier_analysis = {}
        
        for col in numeric_data.columns:
            col_data = numeric_data[col].dropna()
            if len(col_data) == 0:
                continue
            
            # 使用IQR方法检测异常值
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = col_data[(col_data < lower_bound) | (col_data > upper_bound)]
            
            outlier_analysis[col] = {
                'outlier_count': len(outliers),
                'outlier_percentage': round(len(outliers) / len(col_data) * 100, 2),
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound),
                'outlier_values': outliers.tolist()[:10]  # 只保留前10个异常值
            }
        
        return outlier_analysis
    
    def _analyze_patterns(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析数据模式"""
        patterns = {
            'time_patterns': self._analyze_time_patterns(data),
            'value_patterns': self._analyze_value_patterns(data),
            'relationship_patterns': self._analyze_relationship_patterns(data)
        }
        
        return patterns
    
    def _analyze_time_patterns(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析时间模式"""
        datetime_columns = data.select_dtypes(include=['datetime64']).columns
        
        if len(datetime_columns) == 0:
            return {'message': '未发现时间类型列'}
        
        time_analysis = {}
        for col in datetime_columns:
            time_data = data[col].dropna()
            time_analysis[col] = {
                'date_range': {
                    'start': str(time_data.min()),
                    'end': str(time_data.max())
                },
                'data_points': len(time_data),
                'frequency_analysis': self._analyze_time_frequency(time_data)
            }
        
        return time_analysis
    
    def _analyze_time_frequency(self, time_data: pd.Series) -> Dict[str, Any]:
        """分析时间频率"""
        try:
            # 按月份分组
            monthly_counts = time_data.dt.month.value_counts().sort_index()
            # 按星期几分组
            weekday_counts = time_data.dt.dayofweek.value_counts().sort_index()
            
            return {
                'monthly_distribution': monthly_counts.to_dict(),
                'weekday_distribution': weekday_counts.to_dict()
            }
        except:
            return {'message': '时间频率分析失败'}
    
    def _analyze_value_patterns(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析数值模式"""
        numeric_data = data.select_dtypes(include=[np.number])
        
        patterns = {}
        for col in numeric_data.columns:
            col_data = numeric_data[col].dropna()
            if len(col_data) == 0:
                continue
            
            # 检测常见模式
            patterns[col] = {
                'zeros_count': (col_data == 0).sum(),
                'negative_count': (col_data < 0).sum(),
                'integer_percentage': round(((col_data % 1) == 0).sum() / len(col_data) * 100, 2),
                'unique_percentage': round(col_data.nunique() / len(col_data) * 100, 2)
            }
        
        return patterns
    
    def _analyze_relationship_patterns(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析变量之间的关系模式"""
        numeric_data = data.select_dtypes(include=[np.number])
        
        if numeric_data.shape[1] < 2:
            return {'message': '数值列少于2个，无法分析关系模式'}
        
        relationships = []
        columns = list(numeric_data.columns)
        
        for i in range(len(columns)):
            for j in range(i+1, len(columns)):
                col1, col2 = columns[i], columns[j]
                correlation = numeric_data[col1].corr(numeric_data[col2])
                
                if not np.isnan(correlation):
                    relationship_type = self._classify_relationship(correlation)
                    relationships.append({
                        'variable_1': col1,
                        'variable_2': col2,
                        'correlation': round(correlation, 3),
                        'relationship_type': relationship_type
                    })
        
        return {'relationships': relationships}
    
    def _classify_relationship(self, correlation: float) -> str:
        """分类关系类型"""
        abs_corr = abs(correlation)
        if abs_corr >= 0.8:
            return "强相关"
        elif abs_corr >= 0.5:
            return "中等相关"
        elif abs_corr >= 0.3:
            return "弱相关"
        else:
            return "无明显相关"
    
    def _analyze_categorical_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析分类数据"""
        categorical_data = data.select_dtypes(include=['object', 'category'])
        
        if categorical_data.empty:
            return {'message': '未发现分类列'}
        
        categorical_analysis = {}
        for col in categorical_data.columns:
            value_counts = data[col].value_counts()
            
            categorical_analysis[col] = {
                'unique_count': data[col].nunique(),
                'most_frequent': {
                    'value': str(value_counts.index[0]),
                    'count': int(value_counts.iloc[0]),
                    'percentage': round(value_counts.iloc[0] / len(data) * 100, 2)
                },
                'distribution': value_counts.head(10).to_dict(),
                'cardinality_type': self._classify_cardinality(data[col].nunique(), len(data))
            }
        
        return categorical_analysis
    
    def _classify_cardinality(self, unique_count: int, total_count: int) -> str:
        """分类基数类型"""
        ratio = unique_count / total_count
        if ratio < 0.1:
            return "低基数"
        elif ratio < 0.5:
            return "中等基数"
        else:
            return "高基数"
    
    def generate_insights(self, analysis_results: Dict[str, Any], data: pd.DataFrame) -> List[str]:
        """生成数据洞察"""
        insights = []
        
        # 基本信息洞察
        basic_info = analysis_results.get('basic_info', {})
        if basic_info:
            insights.append(f"数据集包含 {basic_info['shape'][0]} 行 {basic_info['shape'][1]} 列")
            if basic_info['duplicate_rows'] > 0:
                insights.append(f"发现 {basic_info['duplicate_rows']} 行重复数据")
        
        # 数据质量洞察
        quality = analysis_results.get('data_quality', {})
        if quality:
            completeness = quality.get('completeness_score', 0)
            insights.append(f"数据完整性评分: {completeness:.1f}%")
            
            if quality.get('potential_issues'):
                insights.append(f"发现 {len(quality['potential_issues'])} 个潜在数据质量问题")
        
        # 相关性洞察
        correlation = analysis_results.get('correlation_analysis', {})
        if correlation and 'high_correlations' in correlation:
            high_corr_count = len(correlation['high_correlations'])
            if high_corr_count > 0:
                insights.append(f"发现 {high_corr_count} 对高相关性变量")
        
        # 异常值洞察
        outliers = analysis_results.get('outlier_analysis', {})
        if outliers:
            outlier_columns = [col for col, info in outliers.items() 
                             if info.get('outlier_percentage', 0) > 5]
            if outlier_columns:
                insights.append(f"{len(outlier_columns)} 个数值列包含显著异常值")
        
        # 分布洞察
        distributions = analysis_results.get('distribution_analysis', {})
        if distributions:
            normal_dist_count = sum(1 for info in distributions.values() 
                                  if info.get('distribution_type') == "近似正态分布")
            if normal_dist_count > 0:
                insights.append(f"{normal_dist_count} 个数值列呈近似正态分布")
        
        return insights