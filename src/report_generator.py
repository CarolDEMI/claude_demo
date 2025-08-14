"""
报告生成模块 - 生成HTML/PDF格式的数据分析报告
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from jinja2 import Template
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import base64
from io import BytesIO
import json

class ReportGenerator:
    def __init__(self, output_dir: str = './reports'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置plotly默认主题
        pio.templates.default = "plotly_white"
    
    def generate_comprehensive_report(self, 
                                    query_info: Dict[str, Any],
                                    data: pd.DataFrame,
                                    analysis_results: Dict[str, Any],
                                    insights: List[str]) -> str:
        """
        生成综合分析报告
        
        Args:
            query_info: 查询信息
            data: 原始数据
            analysis_results: 分析结果
            insights: 数据洞察
        
        Returns:
            报告文件路径
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"data_analysis_report_{timestamp}.html"
        report_path = self.output_dir / report_filename
        
        # 生成可视化图表
        charts = self._generate_charts(data, analysis_results)
        
        # 生成HTML报告
        html_content = self._generate_html_report(
            query_info, data, analysis_results, insights, charts
        )
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return str(report_path)
    
    def _generate_charts(self, data: pd.DataFrame, analysis_results: Dict[str, Any]) -> Dict[str, str]:
        """生成所有可视化图表"""
        charts = {}
        
        # 数据概览图表
        charts['data_overview'] = self._create_data_overview_chart(data)
        
        # 缺失值热图
        charts['missing_values'] = self._create_missing_values_heatmap(data)
        
        # 数值变量分布图
        charts['distributions'] = self._create_distribution_plots(data)
        
        # 相关性热图
        charts['correlation'] = self._create_correlation_heatmap(data)
        
        # 异常值箱线图
        charts['outliers'] = self._create_outlier_plots(data)
        
        # 分类变量柱状图
        charts['categorical'] = self._create_categorical_plots(data)
        
        return charts
    
    def _create_data_overview_chart(self, data: pd.DataFrame) -> str:
        """创建数据概览图表"""
        # 数据类型分布
        dtype_counts = data.dtypes.value_counts()
        
        fig = go.Figure(data=[
            go.Bar(x=dtype_counts.index.astype(str), y=dtype_counts.values)
        ])
        
        fig.update_layout(
            title="数据类型分布",
            xaxis_title="数据类型",
            yaxis_title="列数量",
            height=400
        )
        
        return fig.to_html(full_html=False, include_plotlyjs='inline')
    
    def _create_missing_values_heatmap(self, data: pd.DataFrame) -> str:
        """创建缺失值热图"""
        missing_data = data.isnull()
        
        if missing_data.sum().sum() == 0:
            return "<p>数据集中没有缺失值。</p>"
        
        fig = px.imshow(
            missing_data.T,
            labels=dict(x="行索引", y="列名", color="缺失值"),
            title="缺失值分布热图",
            color_continuous_scale="Reds"
        )
        
        fig.update_layout(height=max(400, len(data.columns) * 20))
        
        return fig.to_html(full_html=False, include_plotlyjs='inline')
    
    def _create_distribution_plots(self, data: pd.DataFrame) -> str:
        """创建数值变量分布图"""
        numeric_columns = data.select_dtypes(include=['number']).columns
        
        if len(numeric_columns) == 0:
            return "<p>数据集中没有数值列。</p>"
        
        # 限制最多显示6个数值列
        columns_to_plot = numeric_columns[:6]
        
        fig = make_subplots(
            rows=(len(columns_to_plot) + 2) // 3,
            cols=3,
            subplot_titles=columns_to_plot,
            vertical_spacing=0.08
        )
        
        for i, col in enumerate(columns_to_plot):
            row = i // 3 + 1
            col_idx = i % 3 + 1
            
            fig.add_trace(
                go.Histogram(x=data[col], name=col, showlegend=False),
                row=row, col=col_idx
            )
        
        fig.update_layout(
            title="数值变量分布图",
            height=200 * ((len(columns_to_plot) + 2) // 3)
        )
        
        return fig.to_html(full_html=False, include_plotlyjs='inline')
    
    def _create_correlation_heatmap(self, data: pd.DataFrame) -> str:
        """创建相关性热图"""
        numeric_data = data.select_dtypes(include=['number'])
        
        if numeric_data.shape[1] < 2:
            return "<p>数值列少于2个，无法创建相关性热图。</p>"
        
        correlation_matrix = numeric_data.corr()
        
        fig = px.imshow(
            correlation_matrix,
            text_auto=True,
            aspect="auto",
            title="变量相关性热图",
            color_continuous_scale="RdBu_r",
            zmin=-1, zmax=1
        )
        
        fig.update_layout(height=max(400, len(correlation_matrix) * 30))
        
        return fig.to_html(full_html=False, include_plotlyjs='inline')
    
    def _create_outlier_plots(self, data: pd.DataFrame) -> str:
        """创建异常值箱线图"""
        numeric_columns = data.select_dtypes(include=['number']).columns
        
        if len(numeric_columns) == 0:
            return "<p>数据集中没有数值列。</p>"
        
        # 限制最多显示6个数值列
        columns_to_plot = numeric_columns[:6]
        
        fig = make_subplots(
            rows=(len(columns_to_plot) + 2) // 3,
            cols=3,
            subplot_titles=columns_to_plot,
            vertical_spacing=0.08
        )
        
        for i, col in enumerate(columns_to_plot):
            row = i // 3 + 1
            col_idx = i % 3 + 1
            
            fig.add_trace(
                go.Box(y=data[col], name=col, showlegend=False),
                row=row, col=col_idx
            )
        
        fig.update_layout(
            title="异常值检测箱线图",
            height=200 * ((len(columns_to_plot) + 2) // 3)
        )
        
        return fig.to_html(full_html=False, include_plotlyjs='inline')
    
    def _create_categorical_plots(self, data: pd.DataFrame) -> str:
        """创建分类变量柱状图"""
        categorical_columns = data.select_dtypes(include=['object', 'category']).columns
        
        if len(categorical_columns) == 0:
            return "<p>数据集中没有分类列。</p>"
        
        # 限制最多显示4个分类列
        columns_to_plot = categorical_columns[:4]
        
        fig = make_subplots(
            rows=(len(columns_to_plot) + 1) // 2,
            cols=2,
            subplot_titles=columns_to_plot,
            vertical_spacing=0.1
        )
        
        for i, col in enumerate(columns_to_plot):
            row = i // 2 + 1
            col_idx = i % 2 + 1
            
            value_counts = data[col].value_counts().head(10)
            
            fig.add_trace(
                go.Bar(x=value_counts.index, y=value_counts.values, name=col, showlegend=False),
                row=row, col=col_idx
            )
        
        fig.update_layout(
            title="分类变量分布图",
            height=300 * ((len(columns_to_plot) + 1) // 2)
        )
        
        return fig.to_html(full_html=False, include_plotlyjs='inline')
    
    def _generate_html_report(self, 
                            query_info: Dict[str, Any],
                            data: pd.DataFrame,
                            analysis_results: Dict[str, Any],
                            insights: List[str],
                            charts: Dict[str, str]) -> str:
        """生成HTML报告"""
        
        template_str = '''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>数据分析报告</title>
    <style>
        body {
            font-family: 'Arial', 'Microsoft YaHei', sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            border-bottom: 3px solid #007bff;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }
        .header h1 {
            color: #007bff;
            margin: 0;
            font-size: 2.5em;
        }
        .section {
            margin-bottom: 40px;
        }
        .section h2 {
            color: #333;
            border-left: 4px solid #007bff;
            padding-left: 15px;
            font-size: 1.8em;
            margin-bottom: 20px;
        }
        .section h3 {
            color: #555;
            font-size: 1.4em;
            margin-bottom: 15px;
        }
        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .info-card {
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #28a745;
        }
        .info-card h4 {
            margin: 0 0 10px 0;
            color: #333;
        }
        .metric {
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
        }
        .metric-label {
            font-weight: bold;
            color: #555;
        }
        .metric-value {
            color: #007bff;
            font-weight: bold;
        }
        .insights {
            background-color: #e3f2fd;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #2196f3;
        }
        .insights ul {
            margin: 0;
            padding-left: 20px;
        }
        .insights li {
            margin-bottom: 8px;
            color: #333;
        }
        .chart-container {
            margin-bottom: 30px;
            background-color: #fafafa;
            padding: 20px;
            border-radius: 8px;
        }
        .sql-code {
            background-color: #f4f4f4;
            padding: 15px;
            border-radius: 5px;
            font-family: 'Courier New', monospace;
            font-size: 14px;
            overflow-x: auto;
            border-left: 4px solid #ff9800;
        }
        .table-responsive {
            overflow-x: auto;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #007bff;
            color: white;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .timestamp {
            text-align: right;
            color: #666;
            font-size: 0.9em;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- 报告标题 -->
        <div class="header">
            <h1>数据分析报告</h1>
            <p>基于查询需求的自动化数据分析</p>
        </div>

        <!-- 查询信息 -->
        <div class="section">
            <h2>查询信息</h2>
            <div class="info-card">
                <h4>用户需求</h4>
                <p>{{ query_info.requirement }}</p>
                
                <h4>生成的SQL查询</h4>
                <div class="sql-code">{{ query_info.sql_info.sql }}</div>
                
                <h4>查询说明</h4>
                <p>{{ query_info.sql_info.explanation }}</p>
            </div>
        </div>

        <!-- 数据概览 -->
        <div class="section">
            <h2>数据概览</h2>
            <div class="info-grid">
                <div class="info-card">
                    <h4>基本信息</h4>
                    <div class="metric">
                        <span class="metric-label">数据行数:</span>
                        <span class="metric-value">{{ basic_info.shape[0] }}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">数据列数:</span>
                        <span class="metric-value">{{ basic_info.shape[1] }}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">重复行数:</span>
                        <span class="metric-value">{{ basic_info.duplicate_rows }}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">内存使用:</span>
                        <span class="metric-value">{{ "%.2f"|format(basic_info.memory_usage / 1024 / 1024) }} MB</span>
                    </div>
                </div>
                
                <div class="info-card">
                    <h4>数据质量</h4>
                    <div class="metric">
                        <span class="metric-label">完整性评分:</span>
                        <span class="metric-value">{{ "%.1f"|format(data_quality.completeness_score) }}%</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">质量问题:</span>
                        <span class="metric-value">{{ data_quality.potential_issues|length }} 个</span>
                    </div>
                </div>
            </div>
            
            <div class="chart-container">
                {{ charts.data_overview|safe }}
            </div>
        </div>

        <!-- 数据洞察 -->
        <div class="section">
            <h2>关键洞察</h2>
            <div class="insights">
                <ul>
                    {% for insight in insights %}
                    <li>{{ insight }}</li>
                    {% endfor %}
                </ul>
            </div>
        </div>

        <!-- 缺失值分析 -->
        <div class="section">
            <h2>缺失值分析</h2>
            <div class="chart-container">
                {{ charts.missing_values|safe }}
            </div>
        </div>

        <!-- 数据分布 -->
        <div class="section">
            <h2>数据分布分析</h2>
            <div class="chart-container">
                {{ charts.distributions|safe }}
            </div>
        </div>

        <!-- 相关性分析 -->
        <div class="section">
            <h2>相关性分析</h2>
            <div class="chart-container">
                {{ charts.correlation|safe }}
            </div>
        </div>

        <!-- 异常值分析 -->
        <div class="section">
            <h2>异常值分析</h2>
            <div class="chart-container">
                {{ charts.outliers|safe }}
            </div>
        </div>

        <!-- 分类变量分析 -->
        <div class="section">
            <h2>分类变量分析</h2>
            <div class="chart-container">
                {{ charts.categorical|safe }}
            </div>
        </div>

        <!-- 数据样本 -->
        <div class="section">
            <h2>数据样本</h2>
            <div class="table-responsive">
                {{ data_sample|safe }}
            </div>
        </div>

        <div class="timestamp">
            报告生成时间: {{ timestamp }}
        </div>
    </div>
</body>
</html>
        '''
        
        template = Template(template_str)
        
        # 准备模板数据
        template_data = {
            'query_info': query_info,
            'basic_info': analysis_results.get('basic_info', {}),
            'data_quality': analysis_results.get('data_quality', {}),
            'insights': insights,
            'charts': charts,
            'data_sample': data.head(10).to_html(classes='table', escape=False),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return template.render(**template_data)
    
    def generate_summary_report(self, analysis_results: Dict[str, Any]) -> str:
        """生成简化的摘要报告"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"summary_report_{timestamp}.json"
        report_path = self.output_dir / report_filename
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'basic_info': analysis_results.get('basic_info', {}),
            'data_quality': analysis_results.get('data_quality', {}),
            'key_statistics': self._extract_key_statistics(analysis_results),
            'recommendations': self._generate_recommendations(analysis_results)
        }
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        return str(report_path)
    
    def _extract_key_statistics(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """提取关键统计信息"""
        key_stats = {}
        
        # 基本统计
        basic = analysis_results.get('basic_info', {})
        if basic:
            key_stats['dataset_size'] = {
                'rows': basic.get('shape', [0])[0],
                'columns': basic.get('shape', [0, 0])[1],
                'memory_mb': round(basic.get('memory_usage', 0) / 1024 / 1024, 2)
            }
        
        # 数据质量
        quality = analysis_results.get('data_quality', {})
        if quality:
            key_stats['data_quality'] = {
                'completeness_score': round(quality.get('completeness_score', 0), 1),
                'issues_count': len(quality.get('potential_issues', []))
            }
        
        # 相关性统计
        correlation = analysis_results.get('correlation_analysis', {})
        if correlation and 'high_correlations' in correlation:
            key_stats['correlations'] = {
                'high_correlation_pairs': len(correlation['high_correlations'])
            }
        
        return key_stats
    
    def _generate_recommendations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """生成数据改进建议"""
        recommendations = []
        
        # 数据质量建议
        quality = analysis_results.get('data_quality', {})
        if quality:
            completeness = quality.get('completeness_score', 100)
            if completeness < 90:
                recommendations.append("建议处理缺失值以提高数据完整性")
            
            if quality.get('potential_issues'):
                recommendations.append("发现数据质量问题，建议进行数据清洗")
        
        # 异常值建议
        outliers = analysis_results.get('outlier_analysis', {})
        if outliers:
            high_outlier_columns = [col for col, info in outliers.items() 
                                  if info.get('outlier_percentage', 0) > 10]
            if high_outlier_columns:
                recommendations.append(f"建议检查以下列的异常值: {', '.join(high_outlier_columns)}")
        
        # 相关性建议
        correlation = analysis_results.get('correlation_analysis', {})
        if correlation and correlation.get('high_correlations'):
            recommendations.append("发现高相关性变量，考虑进行特征选择或降维")
        
        if not recommendations:
            recommendations.append("数据质量良好，可以进行进一步分析")
        
        return recommendations