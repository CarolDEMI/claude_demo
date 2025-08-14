"""
HTML报告生成器
负责整合所有模块输出，生成完整的HTML报告
"""

from typing import List, Dict, Any
from datetime import datetime
import os

class HTMLReportGenerator:
    """HTML报告生成器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.title = config.get('title', '每日业务数据报告')
        self.css_style = config.get('css_style', 'modern')
        
    def generate_report(self, module_results: List[Dict[str, Any]], target_date: str) -> str:
        """
        生成完整的HTML报告
        
        Args:
            module_results: 各模块的执行结果
            target_date: 目标日期
            
        Returns:
            完整的HTML报告内容
        """
        
        # 过滤成功的模块结果
        successful_modules = [r for r in module_results if r.get('success', False)]
        failed_modules = [r for r in module_results if not r.get('success', False)]
        
        html = self._generate_html_structure(successful_modules, failed_modules, target_date)
        
        return html
    
    def _generate_html_structure(self, successful_modules: List[Dict], failed_modules: List[Dict], target_date: str) -> str:
        """生成HTML结构"""
        
        html = f'''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{self.title} - {target_date}</title>
    {self._generate_css()}
</head>
<body>
    <div class="report-container">
        {self._generate_header(target_date, successful_modules, failed_modules)}
        {self._generate_navigation(successful_modules)}
        {self._generate_main_content(successful_modules)}
        {self._generate_error_section(failed_modules)}
        {self._generate_footer()}
    </div>
    {self._generate_javascript()}
</body>
</html>
        '''
        
        return html
    
    def _generate_css(self) -> str:
        """生成CSS样式"""
        
        return '''
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Microsoft YaHei', sans-serif;
            line-height: 1.6;
            color: #333;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .report-container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        /* 头部样式 */
        .report-header {
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: white;
            padding: 30px 40px;
            position: relative;
            overflow: hidden;
        }
        
        .report-header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><defs><pattern id="grid" width="10" height="10" patternUnits="userSpaceOnUse"><path d="M 10 0 L 0 0 0 10" fill="none" stroke="rgba(255,255,255,0.05)" stroke-width="1"/></pattern></defs><rect width="100" height="100" fill="url(%23grid)"/></svg>');
            opacity: 0.3;
        }
        
        .header-content {
            position: relative;
            z-index: 1;
        }
        
        .report-title {
            font-size: 2.5em;
            font-weight: 700;
            margin-bottom: 10px;
            background: linear-gradient(45deg, #fff, #e0e6ed);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .report-subtitle {
            font-size: 1.2em;
            opacity: 0.9;
            margin-bottom: 20px;
        }
        
        .report-meta {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 20px;
        }
        
        .meta-item {
            display: flex;
            align-items: center;
            gap: 8px;
            background: rgba(255,255,255,0.1);
            padding: 8px 16px;
            border-radius: 20px;
            backdrop-filter: blur(10px);
        }
        
        .meta-icon {
            font-size: 1.2em;
        }
        
        /* 导航样式 */
        .report-nav {
            background: #f8f9fa;
            padding: 0 40px;
            border-bottom: 1px solid #e9ecef;
            position: sticky;
            top: 0;
            z-index: 100;
        }
        
        .nav-list {
            display: flex;
            list-style: none;
            gap: 30px;
            overflow-x: auto;
        }
        
        .nav-item {
            white-space: nowrap;
        }
        
        .nav-link {
            display: block;
            padding: 15px 0;
            text-decoration: none;
            color: #666;
            font-weight: 500;
            border-bottom: 3px solid transparent;
            transition: all 0.3s ease;
        }
        
        .nav-link:hover, .nav-link.active {
            color: #007bff;
            border-bottom-color: #007bff;
        }
        
        /* 主内容样式 */
        .main-content {
            padding: 40px;
        }
        
        .module-container {
            margin-bottom: 50px;
            background: #fff;
            border-radius: 15px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.05);
            overflow: hidden;
            border: 1px solid #e9ecef;
        }
        
        .module-header {
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            padding: 25px 30px;
            border-bottom: 1px solid #dee2e6;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .module-header h2 {
            font-size: 1.8em;
            color: #2c3e50;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .module-meta {
            display: flex;
            gap: 15px;
            align-items: center;
        }
        
        .date {
            background: #6c757d;
            color: white;
            padding: 5px 12px;
            border-radius: 15px;
            font-size: 0.9em;
            font-weight: 500;
        }
        
        /* 性能徽章 */
        .performance-badge {
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .performance-badge.grade-a { background: #28a745; color: white; }
        .performance-badge.grade-b { background: #17a2b8; color: white; }
        .performance-badge.grade-c { background: #ffc107; color: #212529; }
        .performance-badge.grade-d { background: #dc3545; color: white; }
        
        /* 异常徽章 */
        .anomaly-badge {
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
        }
        
        .anomaly-badge.normal {
            background: #28a745;
            color: white;
        }
        
        .anomaly-badge.anomaly_detected {
            background: #dc3545;
            color: white;
        }
        
        /* 指标网格 */
        .metrics-section {
            margin-bottom: 40px;
        }
        
        .metrics-section h3 {
            color: #2c3e50;
            margin: 20px 30px 15px 30px;
            font-size: 1.4em;
            border-left: 4px solid #007bff;
            padding-left: 15px;
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 25px;
            padding: 0 30px 20px 30px;
        }
        
        .metrics-grid.main-kpi {
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        }
        
        .metrics-grid.user-quality {
            grid-template-columns: repeat(3, 1fr);
        }
        
        .metrics-grid.conversion {
            grid-template-columns: repeat(3, 1fr);
        }
        
        .metric-card {
            background: linear-gradient(135deg, #fff 0%, #f8f9fa 100%);
            border: 1px solid #e9ecef;
            border-radius: 12px;
            padding: 25px;
            display: flex;
            align-items: center;
            gap: 20px;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        
        .metric-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(45deg, #007bff, #28a745);
        }
        
        /* 不同类别卡片样式 */
        .main-kpi-card::before {
            background: linear-gradient(45deg, #e74c3c, #f39c12);
        }
        
        .quality-card::before {
            background: linear-gradient(45deg, #9b59b6, #8e44ad);
        }
        
        .conversion-card::before {
            background: linear-gradient(45deg, #3498db, #2980b9);
        }
        
        .main-kpi-card {
            border-left: 4px solid #e74c3c;
        }
        
        .quality-card {
            border-left: 4px solid #9b59b6;
        }
        
        .conversion-card {
            border-left: 4px solid #3498db;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 25px rgba(0,0,0,0.1);
        }
        
        .metric-icon {
            font-size: 2.5em;
            background: linear-gradient(135deg, #007bff, #0056b3);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .metric-content {
            flex: 1;
        }
        
        .metric-name {
            font-size: 0.9em;
            color: #6c757d;
            font-weight: 500;
            margin-bottom: 5px;
        }
        
        .metric-value {
            font-size: 1.8em;
            font-weight: 700;
            color: #2c3e50;
            margin-bottom: 5px;
        }
        
        .metric-desc {
            font-size: 0.8em;
            color: #868e96;
            line-height: 1.4;
        }
        
        /* 趋势分析 */
        .trend-section {
            padding: 30px;
            border-top: 1px solid #e9ecef;
        }
        
        .trend-section h3 {
            color: #2c3e50;
            margin-bottom: 20px;
            font-size: 1.4em;
        }
        
        .trend-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
        }
        
        .comparison-card {
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 10px;
            padding: 20px;
        }
        
        .comparison-card h4 {
            color: #495057;
            margin-bottom: 15px;
            font-size: 1.1em;
        }
        
        .comparison-metrics {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .comparison-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 0;
        }
        
        .comparison-item.significant {
            background: rgba(220, 53, 69, 0.1);
            padding: 8px 12px;
            border-radius: 6px;
            border-left: 4px solid #dc3545;
        }
        
        .change-indicator {
            font-weight: 600;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.9em;
        }
        
        .change-indicator:contains("+") {
            background: #d4edda;
            color: #155724;
        }
        
        .change-indicator:contains("-") {
            background: #f8d7da;
            color: #721c24;
        }
        
        /* 性能评分 */
        .performance-section {
            padding: 30px;
            border-top: 1px solid #e9ecef;
            background: linear-gradient(135deg, #f8f9fa 0%, #fff 100%);
        }
        
        .performance-summary {
            display: flex;
            gap: 30px;
            align-items: center;
            margin-bottom: 20px;
        }
        
        .score-display {
            display: flex;
            align-items: baseline;
            gap: 5px;
        }
        
        .score-number {
            font-size: 3em;
            font-weight: 700;
            color: #007bff;
        }
        
        .score-total {
            font-size: 1.2em;
            color: #6c757d;
        }
        
        .score-grade {
            margin-left: 15px;
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: 700;
            font-size: 1.2em;
        }
        
        .progress-bar {
            background: #e9ecef;
            height: 8px;
            border-radius: 4px;
            overflow: hidden;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(45deg, #007bff, #28a745);
            transition: width 0.5s ease;
        }
        
        /* 异常检测样式 */
        .no-anomaly-message {
            text-align: center;
            padding: 60px 30px;
            color: #28a745;
        }
        
        .success-icon {
            font-size: 4em;
            margin-bottom: 20px;
        }
        
        /* 四分位数方法相关样式 */
        .method-info {
            background: #e3f2fd;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            border-left: 4px solid #2196f3;
        }
        
        .method-info p {
            margin: 0;
            color: #1565c0;
            font-size: 0.9em;
        }
        
        .current-value.increase {
            color: #d32f2f;
            font-weight: bold;
        }
        
        .current-value.decrease {
            color: #388e3c;
            font-weight: bold;
        }
        
        .normal-range {
            font-family: monospace;
            background: #f5f5f5;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 0.9em;
        }
        
        .deviation-indicator.severe {
            color: #d32f2f;
            font-weight: bold;
        }
        
        .deviation-indicator.moderate {
            color: #f57c00;
            font-weight: bold;
        }
        
        .metric-desc {
            color: #666;
            font-style: italic;
        }
        
        .quartile-legend {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-top: 20px;
            border: 1px solid #dee2e6;
        }
        
        .quartile-legend h4 {
            color: #495057;
            margin-bottom: 15px;
        }
        
        .quartile-legend ul {
            margin: 0;
            padding-left: 20px;
        }
        
        .quartile-legend li {
            margin-bottom: 8px;
            color: #6c757d;
        }
        
        .anomaly-overview {
            padding: 30px;
        }
        
        .overview-stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-item {
            text-align: center;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 10px;
            border: 1px solid #dee2e6;
        }
        
        .stat-item.high-severity {
            background: #f8d7da;
            border-color: #f5c6cb;
        }
        
        .stat-number {
            display: block;
            font-size: 2em;
            font-weight: 700;
            color: #2c3e50;
        }
        
        .stat-label {
            font-size: 0.9em;
            color: #6c757d;
            margin-top: 5px;
        }
        
        .metrics-list {
            display: flex;
            flex-direction: column;
            gap: 10px;
        }
        
        .metric-item {
            display: flex;
            align-items: center;
            gap: 15px;
            padding: 15px;
            background: #fff;
            border: 1px solid #dee2e6;
            border-radius: 8px;
        }
        
        .metric-item.severity-high {
            border-left: 4px solid #dc3545;
            background: #fef9f9;
        }
        
        .metric-item.severity-medium {
            border-left: 4px solid #ffc107;
            background: #fffdf7;
        }
        
        /* 表格样式 */
        .anomalies-table {
            padding: 30px;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        }
        
        th, td {
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #dee2e6;
        }
        
        th {
            background: #f8f9fa;
            font-weight: 600;
            color: #495057;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        tr:hover {
            background: #f8f9fa;
        }
        
        .severity-badge {
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 0.75em;
            font-weight: 600;
            text-transform: uppercase;
        }
        
        .severity-badge.high {
            background: #dc3545;
            color: white;
        }
        
        .severity-badge.medium {
            background: #ffc107;
            color: #212529;
        }
        
        .change-indicator.increase {
            color: #dc3545;
        }
        
        .change-indicator.decrease {
            color: #28a745;
        }
        
        /* 渠道影响分析 */
        .channel-impact-analysis {
            padding: 30px;
            border-top: 1px solid #e9ecef;
        }
        
        .impact-section {
            margin-bottom: 40px;
        }
        
        .impact-summary {
            display: flex;
            gap: 30px;
            margin-bottom: 20px;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
        }
        
        .channel-contributions table {
            margin-top: 15px;
        }
        
        .major-impact {
            background: rgba(220, 53, 69, 0.05) !important;
            border-left: 4px solid #dc3545;
        }
        
        .impact-score {
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .score-bar {
            height: 6px;
            background: linear-gradient(45deg, #007bff, #28a745);
            border-radius: 3px;
            min-width: 20px;
            transition: width 0.3s ease;
        }
        
        .score-text {
            font-size: 0.85em;
            font-weight: 600;
            color: #495057;
        }
        
        /* 建议措施 */
        .recommendations-section {
            padding: 30px;
            border-top: 1px solid #e9ecef;
            background: linear-gradient(135deg, #e3f2fd 0%, #f3e5f5 100%);
        }
        
        .recommendations-list {
            display: flex;
            flex-direction: column;
            gap: 15px;
        }
        
        .recommendation-item {
            display: flex;
            align-items: flex-start;
            gap: 15px;
            padding: 20px;
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            border-left: 4px solid #007bff;
        }
        
        .rec-number {
            background: #007bff;
            color: white;
            width: 30px;
            height: 30px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.9em;
            font-weight: 600;
            flex-shrink: 0;
        }
        
        .rec-text {
            line-height: 1.6;
            color: #2c3e50;
        }
        
        /* 错误部分 */
        .error-section {
            padding: 30px 40px;
            background: #f8d7da;
            border-top: 1px solid #f5c6cb;
        }
        
        .error-item {
            background: white;
            border: 1px solid #f5c6cb;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 15px;
        }
        
        .error-module {
            font-weight: 600;
            color: #721c24;
            margin-bottom: 10px;
        }
        
        .error-message {
            color: #721c24;
            font-size: 0.9em;
        }
        
        /* 页脚 */
        .report-footer {
            background: #2c3e50;
            color: white;
            padding: 30px 40px;
            text-align: center;
        }
        
        .footer-info {
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 20px;
        }
        
        .generation-time {
            font-size: 0.9em;
            opacity: 0.8;
        }
        
        .footer-links {
            display: flex;
            gap: 20px;
        }
        
        .footer-link {
            color: white;
            text-decoration: none;
            opacity: 0.8;
            transition: opacity 0.3s ease;
        }
        
        .footer-link:hover {
            opacity: 1;
        }
        
        /* 响应式 */
        @media (max-width: 768px) {
            body {
                padding: 10px;
            }
            
            .report-header,
            .main-content,
            .report-footer {
                padding: 20px;
            }
            
            .report-nav {
                padding: 0 20px;
            }
            
            .metrics-grid {
                grid-template-columns: 1fr;
                padding: 20px;
            }
            
            .trend-grid {
                grid-template-columns: 1fr;
            }
            
            .performance-summary {
                flex-direction: column;
                align-items: flex-start;
                gap: 20px;
            }
            
            .overview-stats {
                grid-template-columns: repeat(2, 1fr);
            }
            
            .footer-info {
                flex-direction: column;
                text-align: center;
            }
        }
        
        /* 平滑滚动 */
        html {
            scroll-behavior: smooth;
        }
        
        /* 隐藏滚动条但保持功能 */
        .nav-list::-webkit-scrollbar {
            height: 3px;
        }
        
        .nav-list::-webkit-scrollbar-track {
            background: transparent;
        }
        
        .nav-list::-webkit-scrollbar-thumb {
            background: #dee2e6;
            border-radius: 3px;
        }
        
        .nav-list::-webkit-scrollbar-thumb:hover {
            background: #adb5bd;
        }
    </style>
        '''
    
    def _generate_header(self, target_date: str, successful_modules: List[Dict], failed_modules: List[Dict]) -> str:
        """生成页面头部"""
        
        generation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        total_modules = len(successful_modules) + len(failed_modules)
        success_rate = (len(successful_modules) / total_modules * 100) if total_modules > 0 else 0
        
        return f'''
        <header class="report-header">
            <div class="header-content">
                <h1 class="report-title">{self.title}</h1>
                <p class="report-subtitle">基于模块化架构的智能业务数据分析</p>
                <div class="report-meta">
                    <div class="meta-item">
                        <span class="meta-icon">📅</span>
                        <span>报告日期: {target_date}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-icon">⏰</span>
                        <span>生成时间: {generation_time}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-icon">📊</span>
                        <span>模块数量: {len(successful_modules)}/{total_modules}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-icon">✅</span>
                        <span>成功率: {success_rate:.0f}%</span>
                    </div>
                </div>
            </div>
        </header>
        '''
    
    def _generate_navigation(self, successful_modules: List[Dict]) -> str:
        """生成导航菜单"""
        
        nav_items = []
        for module in successful_modules:
            module_name = module.get('module_name', '')
            # 从模块名提取友好名称
            if 'OverviewMetrics' in module_name:
                display_name = "📊 大盘指标"
                anchor = "overview-metrics"
            elif 'AnomalyDetection' in module_name:
                display_name = "🚨 异常分析"
                anchor = "anomaly-detection"
            else:
                display_name = f"📋 {module_name}"
                anchor = module_name.lower()
            
            nav_items.append(f'<li class="nav-item"><a href="#{anchor}" class="nav-link">{display_name}</a></li>')
        
        return f'''
        <nav class="report-nav">
            <ul class="nav-list">
                {''.join(nav_items)}
            </ul>
        </nav>
        '''
    
    def _generate_main_content(self, successful_modules: List[Dict]) -> str:
        """生成主要内容区域"""
        
        content_sections = []
        for module in successful_modules:
            html_content = module.get('html_content', '')
            if html_content:
                content_sections.append(html_content)
        
        return f'''
        <main class="main-content">
            {''.join(content_sections)}
        </main>
        '''
    
    def _generate_error_section(self, failed_modules: List[Dict]) -> str:
        """生成错误信息部分"""
        
        if not failed_modules:
            return ''
        
        error_items = []
        for module in failed_modules:
            module_name = module.get('module_name', '未知模块')
            errors = module.get('errors', [])
            
            error_messages = '<br>'.join(errors) if errors else '未知错误'
            
            error_items.append(f'''
                <div class="error-item">
                    <div class="error-module">模块: {module_name}</div>
                    <div class="error-message">{error_messages}</div>
                </div>
            ''')
        
        return f'''
        <section class="error-section">
            <h2>⚠️ 模块执行错误</h2>
            <p>以下模块在执行过程中出现错误：</p>
            {''.join(error_items)}
        </section>
        '''
    
    def _generate_footer(self) -> str:
        """生成页面页脚"""
        
        generation_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return f'''
        <footer class="report-footer">
            <div class="footer-info">
                <div class="generation-time">
                    报告生成时间: {generation_time}
                </div>
                <div class="footer-links">
                    <a href="#" class="footer-link">📊 模块化报告系统</a>
                    <a href="#" class="footer-link">📈 业务数据分析</a>
                    <a href="#" class="footer-link">🔧 系统设置</a>
                </div>
            </div>
        </footer>
        '''
    
    def _generate_javascript(self) -> str:
        """生成JavaScript代码"""
        
        return '''
    <script>
        // 导航激活状态
        document.addEventListener('DOMContentLoaded', function() {
            const navLinks = document.querySelectorAll('.nav-link');
            const sections = document.querySelectorAll('.module-container');
            
            // 点击导航链接时更新激活状态
            navLinks.forEach(link => {
                link.addEventListener('click', function(e) {
                    navLinks.forEach(l => l.classList.remove('active'));
                    this.classList.add('active');
                });
            });
            
            // 滚动时更新激活状态
            window.addEventListener('scroll', function() {
                let current = '';
                sections.forEach(section => {
                    const sectionTop = section.offsetTop;
                    const sectionHeight = section.clientHeight;
                    if (scrollY >= (sectionTop - 200)) {
                        current = section.getAttribute('id');
                    }
                });
                
                navLinks.forEach(link => {
                    link.classList.remove('active');
                    if (link.getAttribute('href') === '#' + current) {
                        link.classList.add('active');
                    }
                });
            });
            
            // 平滑滚动
            navLinks.forEach(link => {
                link.addEventListener('click', function(e) {
                    e.preventDefault();
                    const targetId = this.getAttribute('href').substring(1);
                    const targetElement = document.getElementById(targetId);
                    if (targetElement) {
                        targetElement.scrollIntoView({
                            behavior: 'smooth',
                            block: 'start'
                        });
                    }
                });
            });
            
            // 初始化第一个导航项为激活状态
            if (navLinks.length > 0) {
                navLinks[0].classList.add('active');
            }
            
            // 动画效果
            const observerOptions = {
                threshold: 0.1,
                rootMargin: '0px 0px -50px 0px'
            };
            
            const observer = new IntersectionObserver(function(entries) {
                entries.forEach(entry => {
                    if (entry.isIntersecting) {
                        entry.target.style.opacity = '1';
                        entry.target.style.transform = 'translateY(0)';
                    }
                });
            }, observerOptions);
            
            // 观察所有模块容器
            sections.forEach(section => {
                section.style.opacity = '0';
                section.style.transform = 'translateY(20px)';
                section.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
                observer.observe(section);
            });
        });
        
        // 打印功能
        function printReport() {
            window.print();
        }
        
        // 导出功能
        function exportReport() {
            alert('导出功能开发中...');
        }
    </script>
        '''