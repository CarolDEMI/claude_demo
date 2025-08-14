"""
大盘指标模块（基于新配置系统）
展示核心业务指标和趋势变化
"""

import sqlite3
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime, timedelta
import sys
import os

# 添加路径以导入基类
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_module import BaseReportModule
from core.data_collector import DataCollector
from unified_config_system import CONFIG_MANAGER

class OverviewMetricsModule(BaseReportModule):
    """大盘指标模块（基于新配置系统）"""
    def __init__(self, db_path: str, config: Dict[str, Any]):
        """初始化时验证配置合规性"""
        super().__init__(db_path, config)
        
        # 验证组件合规性
        compliance_result = CONFIG_MANAGER.validate_component_compliance(
            self.__class__.__name__, 
            open(__file__, 'r', encoding='utf-8').read()
        )
        
        if not compliance_result['compliant']:
            print(f"⚠️ {self.__class__.__name__} 合规性警告:")
            for issue in compliance_result['issues']:
                print(f"  • {issue}")

    
    def collect_data(self, target_date: str, conn: sqlite3.Connection) -> Dict[str, Any]:
        """使用新的DataCollector收集大盘指标数据"""
        
        # 使用新的数据收集器
        collector = DataCollector()
        data_result = collector.collect_daily_metrics(target_date)
        
        current_data = data_result['calculated_data'].copy()
        raw_data = data_result['raw_data'].copy()
        
        # 合并原始数据供后续使用
        current_data.update(raw_data)
        
        # 2. 历史趋势数据（过去7天）
        comparison_days = self.config.get('comparison_days', 7)
        start_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=comparison_days)).strftime('%Y-%m-%d')
        
        trend_data = collector.collect_historical_data(start_date, target_date)
        
        return {
            'target_date': target_date,
            'current_metrics': current_data,
            'trend_data': [item['calculated_data'] for item in trend_data],
            'comparison_days': comparison_days
        }
    
    def analyze_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """分析大盘指标数据"""
        
        current_metrics = data['current_metrics']
        trend_data = data['trend_data']
        target_date = data['target_date']
        
        # 1. 核心指标格式化（按类别分组）
        formatted_metrics = {
            'main_kpi': [],
            'user_quality': [],
            'conversion_metrics': []
        }
        
        # 处理主要KPI指标
        main_kpi_fields = [
            {'name': 'Good且认证用户数', 'field': 'quality_users', 'format': '{:,}', 'icon': '⭐', 'description': '核心优质用户'},
            {'name': 'CPA', 'field': 'cpa', 'format': '¥{:.2f}', 'icon': '💰', 'description': '获客成本'},
            {'name': 'ARPU（税后）', 'field': 'arpu_after_tax', 'format': '¥{:.2f}', 'icon': '📈', 'description': '用户平均收入'},
            {'name': '次留率', 'field': 'retention_rate', 'format': '{:.1f}%', 'icon': '🔄', 'description': 'Good且认证用户次日留存率'}
        ]
        
        for metric_config in main_kpi_fields:
            field = metric_config['field']
            value = current_metrics.get(field, 0)
            
            formatted_metrics['main_kpi'].append({
                'name': metric_config['name'],
                'value': value,
                'formatted_value': metric_config['format'].format(value),
                'icon': metric_config['icon'],
                'description': metric_config['description'],
                'category': 'main_kpi'
            })
        
        # 处理用户质量指标
        user_quality_fields = [
            {'name': '女性占比', 'field': 'female_ratio', 'format': '{:.1f}%', 'icon': '👩', 'description': 'Good且认证女性用户占比'},
            {'name': '年轻占比（24岁以下）', 'field': 'young_ratio', 'format': '{:.1f}%', 'icon': '🧑‍💼', 'description': 'Good且认证年轻用户占比'},
            {'name': '高线城市占比', 'field': 'high_tier_ratio', 'format': '{:.1f}%', 'icon': '🏙️', 'description': 'Good且认证高线城市用户占比'}
        ]
        
        for metric_config in user_quality_fields:
            field = metric_config['field']
            value = current_metrics.get(field, 0)
            
            formatted_metrics['user_quality'].append({
                'name': metric_config['name'],
                'value': value,
                'formatted_value': metric_config['format'].format(value),
                'icon': metric_config['icon'],
                'description': metric_config['description'],
                'category': 'user_quality'
            })
        
        # 处理转化指标
        conversion_fields = [
            {'name': 'Good率', 'field': 'good_rate', 'format': '{:.1f}%', 'icon': '✅', 'description': 'Good用户转化率'},
            {'name': '认证率', 'field': 'verified_rate', 'format': '{:.1f}%', 'icon': '🔐', 'description': '用户认证转化率'},
            {'name': 'Good且认证率', 'field': 'quality_rate', 'format': '{:.1f}%', 'icon': '🎯', 'description': '优质用户转化率'}
        ]
        
        for metric_config in conversion_fields:
            field = metric_config['field']
            value = current_metrics.get(field, 0)
            
            formatted_metrics['conversion_metrics'].append({
                'name': metric_config['name'],
                'value': value,
                'formatted_value': metric_config['format'].format(value),
                'icon': metric_config['icon'],
                'description': metric_config['description'],
                'category': 'conversion'
            })
        
        # 2. 趋势分析
        trend_analysis = self._analyze_trends(trend_data, target_date)
        
        # 3. 性能评估
        performance_score = self._calculate_performance_score(current_metrics)
        
        return {
            'date': target_date,
            'core_metrics': formatted_metrics,
            'trend_analysis': trend_analysis,
            'performance_score': performance_score,
            'raw_current': current_metrics,
            'trend_data': trend_data
        }
    
    def _analyze_trends(self, trend_data: List[Dict], target_date: str) -> Dict[str, Any]:
        """分析趋势变化"""
        
        if len(trend_data) < 2:
            return {'has_comparison': False, 'message': '数据不足，无法进行趋势分析'}
        
        # 找到目标日期的数据
        target_data = None
        target_index = -1
        
        for i, day_data in enumerate(trend_data):
            if day_data.get('date') == target_date:  # 使用get以防键不存在
                target_data = day_data
                target_index = i
                break
        
        if target_data is None:
            return {'has_comparison': False, 'message': '未找到目标日期数据'}
        
        trends = {
            'has_comparison': True,
            'vs_yesterday': {},
            'recent_trend': []
        }
        
        # 与昨天对比
        if target_index > 0:
            yesterday_data = trend_data[target_index - 1]
            trends['vs_yesterday'] = self._compare_metrics(target_data, yesterday_data, 'yesterday')
        
        # 最近趋势（最近5天）
        recent_data = trend_data[-5:] if len(trend_data) >= 5 else trend_data
        trends['recent_trend'] = recent_data
        
        return trends
    
    def _compare_metrics(self, current: Dict, previous: Dict, period: str) -> Dict[str, Any]:
        """对比两个时期的指标"""
        
        metrics_to_compare = ['quality_users', 'revenue_after_tax', 'arpu_after_tax', 'conversion_rate']
        comparison = {
            'period': period,
            'date': previous.get('date', ''),
            'changes': {}
        }
        
        for metric in metrics_to_compare:
            current_value = current.get(metric, 0)
            previous_value = previous.get(metric, 0)
            
            change_pct = self.calculate_change_percentage(current_value, previous_value)
            
            comparison['changes'][metric] = {
                'current': current_value,
                'previous': previous_value,
                'change_pct': change_pct,
                'change_indicator': self.get_change_indicator(change_pct),
                'is_significant': abs(change_pct) >= self.config.get('trend_threshold', 5.0)
            }
        
        return comparison
    
    def _calculate_performance_score(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """计算性能评分"""
        
        score = 0
        max_score = 100
        details = []
        
        # ARPU评分 (30分)
        arpu = metrics.get('arpu_after_tax', 0)
        if arpu >= 10:
            arpu_score = 30
            details.append("ARPU表现优秀 (≥¥10)")
        elif arpu >= 5:
            arpu_score = 20
            details.append("ARPU表现良好 (¥5-10)")
        elif arpu >= 2:
            arpu_score = 10
            details.append("ARPU表现一般 (¥2-5)")
        else:
            arpu_score = 0
            details.append("ARPU偏低 (<¥2)")
        score += arpu_score
        
        # 留存率评分 (30分)
        retention = metrics.get('retention_rate', 0)
        if retention >= 60:
            retention_score = 30
            details.append("留存率优秀 (≥60%)")
        elif retention >= 40:
            retention_score = 20
            details.append("留存率良好 (40-60%)")
        elif retention >= 20:
            retention_score = 10
            details.append("留存率一般 (20-40%)")
        else:
            retention_score = 0
            details.append("留存率偏低 (<20%)")
        score += retention_score
        
        # 转化率评分 (25分)
        conversion = metrics.get('conversion_rate', 0)
        if conversion >= 40:
            conv_score = 25
            details.append("转化率优秀 (≥40%)")
        elif conversion >= 25:
            conv_score = 20
            details.append("转化率良好 (25-40%)")
        elif conversion >= 10:
            conv_score = 10
            details.append("转化率一般 (10-25%)")
        else:
            conv_score = 0
            details.append("转化率偏低 (<10%)")
        score += conv_score
        
        # 质量用户占比评分 (15分)
        quality_rate = metrics.get('quality_rate', 0)
        if quality_rate >= 50:
            quality_score = 15
            details.append("用户质量优秀 (≥50%)")
        elif quality_rate >= 30:
            quality_score = 10
            details.append("用户质量良好 (30-50%)")
        elif quality_rate >= 15:
            quality_score = 5
            details.append("用户质量一般 (15-30%)")
        else:
            quality_score = 0
            details.append("用户质量待改善 (<15%)")
        score += quality_score
        
        # 评级
        if score >= 80:
            grade = "A"
            grade_text = "优秀"
        elif score >= 60:
            grade = "B"
            grade_text = "良好"
        elif score >= 40:
            grade = "C"
            grade_text = "一般"
        else:
            grade = "D"
            grade_text = "待改善"
        
        return {
            'score': score,
            'max_score': max_score,
            'percentage': score / max_score * 100,
            'grade': grade,
            'grade_text': grade_text,
            'details': details
        }
    
    def generate_html(self, analysis_result: Dict[str, Any]) -> str:
        """生成大盘指标HTML（按照模块化格式）"""
        
        html = f'''
        <div class="module-container" id="overview-metrics">
            <div class="module-header">
                <h2>📊 模块一：大盘指标</h2>
                <div class="module-meta">
                    <span class="date">{analysis_result['date']}</span>
                    <span class="performance-badge grade-{analysis_result['performance_score']['grade'].lower()}">
                        {analysis_result['performance_score']['grade_text']} {analysis_result['performance_score']['grade']}
                    </span>
                </div>
            </div>
            
            <!-- MAIN KPI指标 -->
            <div class="metrics-section">
                <h3>🎯 MAIN KPI</h3>
                <div class="metrics-grid main-kpi">
        '''
        
        for metric in analysis_result['core_metrics']['main_kpi']:
            html += f'''
                    <div class="metric-card main-kpi-card">
                        <div class="metric-icon">{metric['icon']}</div>
                        <div class="metric-content">
                            <div class="metric-name">{metric['name']}</div>
                            <div class="metric-value">{metric['formatted_value']}</div>
                            <div class="metric-desc">{metric['description']}</div>
                        </div>
                    </div>
            '''
        
        html += '''
                </div>
            </div>
            
            <!-- 用户质量指标 -->
            <div class="metrics-section">
                <h3>👥 用户质量</h3>
                <div class="metrics-grid user-quality">
        '''
        
        for metric in analysis_result['core_metrics']['user_quality']:
            html += f'''
                    <div class="metric-card quality-card">
                        <div class="metric-icon">{metric['icon']}</div>
                        <div class="metric-content">
                            <div class="metric-name">{metric['name']}</div>
                            <div class="metric-value">{metric['formatted_value']}</div>
                            <div class="metric-desc">{metric['description']}</div>
                        </div>
                    </div>
            '''
        
        html += '''
                </div>
            </div>
            
            <!-- 注册转化指标 -->
            <div class="metrics-section">
                <h3>🎯 注册转化</h3>
                <div class="metrics-grid conversion">
        '''
        
        for metric in analysis_result['core_metrics']['conversion_metrics']:
            html += f'''
                    <div class="metric-card conversion-card">
                        <div class="metric-icon">{metric['icon']}</div>
                        <div class="metric-content">
                            <div class="metric-name">{metric['name']}</div>
                            <div class="metric-value">{metric['formatted_value']}</div>
                            <div class="metric-desc">{metric['description']}</div>
                        </div>
                    </div>
            '''
        
        html += '''
                </div>
            </div>
        </div>
        '''
        
        return html