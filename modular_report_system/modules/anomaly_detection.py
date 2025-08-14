"""
异常检测与渠道影响分析模块（基于新配置系统）
自动检测业务指标异常，并分析各渠道对异常的影响程度
"""

import sqlite3
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta
import sys
import os

# 添加路径以导入基类
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_module import BaseReportModule
from core.data_collector import DataCollector
from unified_config_system import CONFIG_MANAGER

class AnomalyDetectionModule(BaseReportModule):
    """异常检测与渠道影响分析模块（基于新配置系统）"""
    
    def collect_data(self, target_date: str, conn: sqlite3.Connection) -> Dict[str, Any]:
        """收集异常检测数据"""
        
        # 使用新的数据收集器
        collector = DataCollector()
        
        history_days = self.config['history_days']
        start_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=history_days)).strftime('%Y-%m-%d')
        
        # 1. 获取历史数据
        historical_data = collector.collect_historical_data(start_date, target_date)
        
        # 2. 当日数据
        current_data = collector.collect_daily_metrics(target_date)
        
        # 3. 渠道数据（简化版，实际可以根据需要扩展）
        channel_data = self._collect_channel_data(target_date, conn)
        
        return {
            'target_date': target_date,
            'current_data': current_data,
            'historical_data': historical_data,
            'channel_data': channel_data,
            'detection_config': self.config
        }
    
    def _collect_channel_data(self, target_date: str, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        """收集渠道数据"""
        
        # 使用统一配置的业务条件
        quality_condition = CONFIG_MANAGER.get_sql_condition('quality_user_condition')
        paying_condition = CONFIG_MANAGER.get_sql_condition('paying_user_condition')
        
        query = f'''
        SELECT 
            ad_channel,
            SUM(newuser) as total_users,
            SUM(CASE WHEN {quality_condition} THEN newuser ELSE 0 END) as quality_users,
            SUM(zizhu_revenue_1_aftertax) as revenue_after_tax,
            ROUND(SUM(zizhu_revenue_1_aftertax) / NULLIF(SUM(CASE WHEN {quality_condition} THEN newuser ELSE 0 END), 0), 2) as arpu_after_tax,
            SUM(CASE WHEN {paying_condition} THEN newuser ELSE 0 END) as paying_users
        FROM cpz_qs_newuser_channel_i_d
        WHERE dt = '{target_date}'
        GROUP BY ad_channel
        HAVING SUM(newuser) >= {self.config['channel_analysis']['min_users_for_analysis']}
        ORDER BY SUM(newuser) DESC
        LIMIT {self.config['channel_analysis']['top_channels_count']}
        '''
        
        channel_df = pd.read_sql_query(query, conn)
        
        if not channel_df.empty:
            channel_df['conversion_rate'] = (channel_df['paying_users'] / channel_df['quality_users'] * 100).fillna(0)
            return channel_df.to_dict('records')
        else:
            return []
    
    def analyze_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """分析异常检测数据"""
        
        target_date = data['target_date']
        current_data = data['current_data']['calculated_data']
        historical_data = data['historical_data']
        channel_data = data['channel_data']
        config = data['detection_config']
        
        # 1. 执行异常检测
        anomalies = self._detect_anomalies(current_data, historical_data, config)
        
        # 2. 分析渠道影响
        channel_analysis = self._analyze_channel_impact(channel_data, anomalies)
        
        # 3. 生成检测总结
        detection_summary = self._generate_detection_summary(anomalies, current_data)
        
        return {
            'date': target_date,
            'anomalies': anomalies,
            'channel_analysis': channel_analysis,
            'detection_summary': detection_summary,
            'detection_method': config['detection_method'],
            'current_metrics': current_data
        }
    
    def _detect_anomalies(self, current_data: Dict, historical_data: List[Dict], config: Dict) -> List[Dict[str, Any]]:
        """使用四分位数方法检测异常"""
        
        anomalies = []
        
        # 从统一配置获取监控的核心KPI指标
        monitored_metrics = CONFIG_MANAGER.config['anomaly_detection']['monitored_metrics']
        
        key_metrics = []
        for metric_field in monitored_metrics:
            # 根据字段类型设置格式
            if metric_field in ['quality_users']:
                format_str = '{:,}'
                unit = '人'
                icon = '⭐'
                name = 'Good且认证用户数'
            elif metric_field in ['cpa', 'arpu_after_tax']:
                format_str = '¥{:.2f}'
                unit = '元'
                icon = '💰' if 'cpa' in metric_field else '📈'
                name = 'CPA' if 'cpa' in metric_field else 'ARPU（税后）'
            elif 'rate' in metric_field:
                format_str = '{:.1f}%'
                unit = '%'
                icon = '🔄' if 'retention' in metric_field else '✅'
                name = metric_field.replace('_', ' ').title()
            else:
                format_str = '{:.2f}'
                unit = ''
                icon = '📊'
                name = metric_field.replace('_', ' ').title()
            
            key_metrics.append({
                'field': metric_field, 
                'name': name, 
                'format': format_str, 
                'unit': unit
            })
        
        for metric_config in key_metrics:
            field = metric_config['field']
            current_value = current_data.get(field, 0)
            
            if current_value == 0:
                continue
            
            # 获取历史数据
            historical_values = []
            for day_data in historical_data:
                hist_value = day_data['calculated_data'].get(field, 0)
                if hist_value > 0:
                    historical_values.append(hist_value)
            
            if len(historical_values) < 6:  # 需要足够的历史数据
                continue
            
            # 四分位数异常检测
            anomaly_result = self._quartile_anomaly_detection(
                field, current_value, historical_values, config, metric_config
            )
            
            if anomaly_result:
                anomalies.append(anomaly_result)
        
        return anomalies
    
    def _quartile_anomaly_detection(self, field: str, current_value: float, 
                                   historical_values: List[float], config: Dict, 
                                   metric_config: Dict) -> Dict[str, Any]:
        """四分位数异常检测"""
        
        # 计算四分位数
        q1 = np.percentile(historical_values, 25)
        q3 = np.percentile(historical_values, 75)
        iqr = q3 - q1
        median = np.percentile(historical_values, 50)
        
        # 计算异常阈值
        multiplier = CONFIG_MANAGER.config['anomaly_detection']['quartile_multiplier']
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr
        
        # 检测异常
        is_anomaly = current_value < lower_bound or current_value > upper_bound
        
        if is_anomaly:
            if current_value < lower_bound:
                direction = 'decrease'
                deviation = (lower_bound - current_value) / iqr if iqr > 0 else 0
                trend = 'down'
            else:
                direction = 'increase'
                deviation = (current_value - upper_bound) / iqr if iqr > 0 else 0
                trend = 'up'
            
            change_pct = (current_value - median) / median * 100 if median > 0 else 0
            severity = 'severe' if deviation >= 2.0 else 'moderate'
            
            return {
                'field': field,
                'name': metric_config['name'],
                'is_anomaly': True,
                'current_value': current_value,
                'formatted_value': metric_config['format'].format(current_value),
                'median': median,
                'q1': q1,
                'q3': q3,
                'iqr': iqr,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'deviation': deviation,
                'direction': direction,
                'trend': trend,
                'change_pct': change_pct,
                'severity': severity,
                'unit': metric_config['unit']
            }
        
        return None
    
    def _analyze_channel_impact(self, channel_data: List[Dict], anomalies: List[Dict]) -> Dict[str, Any]:
        """分析渠道对异常的影响"""
        
        if not channel_data or not anomalies:
            return {
                'has_analysis': False,
                'message': '无足够数据进行渠道影响分析'
            }
        
        # 简化的渠道影响分析
        channel_insights = []
        
        # 按用户数排序渠道
        sorted_channels = sorted(channel_data, key=lambda x: x['quality_users'], reverse=True)
        
        for channel in sorted_channels[:5]:  # 分析前5大渠道
            channel_name = channel['ad_channel']
            quality_users = channel['quality_users']
            arpu = channel.get('arpu_after_tax', 0)
            conversion = channel.get('conversion_rate', 0)
            
            impact_score = 0
            impact_details = []
            
            # 分析各个异常指标的渠道表现
            for anomaly in anomalies:
                if anomaly['field'] == 'arpu_after_tax' and arpu > 0:
                    if anomaly['trend'] == 'up' and arpu > anomaly['median']:
                        impact_score += 1
                        impact_details.append(f"ARPU表现优于整体({arpu:.2f} vs {anomaly['median']:.2f})")
                    elif anomaly['trend'] == 'down' and arpu < anomaly['median']:
                        impact_score -= 1
                        impact_details.append(f"ARPU拖累整体({arpu:.2f} vs {anomaly['median']:.2f})")
            
            channel_insights.append({
                'channel': channel_name,
                'quality_users': quality_users,
                'impact_score': impact_score,
                'impact_details': impact_details,
                'metrics': {
                    'arpu': arpu,
                    'conversion_rate': conversion
                }
            })
        
        return {
            'has_analysis': True,
            'channel_insights': channel_insights,
            'total_channels_analyzed': len(channel_insights)
        }
    
    def _generate_detection_summary(self, anomalies: List[Dict], current_data: Dict) -> Dict[str, Any]:
        """生成检测总结"""
        
        total_metrics_checked = 7  # 检查的指标总数
        anomalies_found = len(anomalies)
        
        if anomalies_found == 0:
            status = 'normal'
            message = '所有核心KPI指标均在正常范围内'
            severity_level = 'green'
        elif anomalies_found <= 2:
            status = 'attention'
            message = f'发现{anomalies_found}个指标异常，需要关注'
            severity_level = 'yellow'
        else:
            status = 'alert'
            message = f'发现{anomalies_found}个指标异常，需要紧急处理'
            severity_level = 'red'
        
        # 分析异常类型
        severe_anomalies = [a for a in anomalies if a['severity'] == 'severe']
        moderate_anomalies = [a for a in anomalies if a['severity'] == 'moderate']
        
        return {
            'status': status,
            'message': message,
            'severity_level': severity_level,
            'total_checked': total_metrics_checked,
            'anomalies_found': anomalies_found,
            'severe_count': len(severe_anomalies),
            'moderate_count': len(moderate_anomalies),
            'normal_count': total_metrics_checked - anomalies_found
        }
    
    def generate_html(self, analysis_result: Dict[str, Any]) -> str:
        """生成异常检测HTML（按照模块化格式）"""
        
        html = f'''
        <div class="module-container" id="anomaly-detection">
            <div class="module-header">
                <h2>🚨 模块二：异常指标展示和渠道分析</h2>
                <div class="module-meta">
                    <span class="date">{analysis_result['date']}</span>
                    <span class="detection-badge {analysis_result['detection_summary']['severity_level']}">
                        {analysis_result['detection_summary']['status'].upper()}
                    </span>
                </div>
            </div>
            
            <!-- 异常检测结果 -->
            <div class="detection-section">
                <h3>📈 异常检测（四分位数方法）</h3>
                <div class="detection-info">
                    <p>检测方法: 基于{self.config['history_days']}天历史数据，超出{self.config['quartile_multiplier']}倍四分位距视为异常</p>
                    <p>{analysis_result['detection_summary']['message']}</p>
                </div>
        '''
        
        anomalies = analysis_result['anomalies']
        
        if anomalies:
            html += '''
                <div class="anomalies-list">
                    <h4>⚠️ 检测到的异常指标</h4>
            '''
            
            for anomaly in anomalies:
                severity_class = 'severe' if anomaly['severity'] == 'severe' else 'moderate'
                trend_icon = '↗️' if anomaly['trend'] == 'up' else '↘️'
                
                html += f'''
                    <div class="anomaly-item {severity_class}">
                        <div class="anomaly-header">
                            <span class="anomaly-name">{anomaly['name']}</span>
                            <span class="anomaly-trend">{trend_icon} {anomaly['severity'].upper()}</span>
                        </div>
                        <div class="anomaly-details">
                            <div class="anomaly-values">
                                <span>当前值: {anomaly['formatted_value']}</span>
                                <span>中位数: {anomaly['unit'] == '%' and f"{anomaly['median']:.1f}%" or f"¥{anomaly['median']:.2f}" if '¥' in anomaly['formatted_value'] else f"{anomaly['median']:,.0f}"}</span>
                                <span>偏离程度: {anomaly['deviation']:.1f}×IQR</span>
                            </div>
                            <div class="anomaly-range">
                                正常范围: [{anomaly['unit'] == '%' and f"{anomaly['lower_bound']:.1f}%" or f"¥{anomaly['lower_bound']:.2f}" if '¥' in anomaly['formatted_value'] else f"{anomaly['lower_bound']:,.0f}"}, 
                                {anomaly['unit'] == '%' and f"{anomaly['upper_bound']:.1f}%" or f"¥{anomaly['upper_bound']:.2f}" if '¥' in anomaly['formatted_value'] else f"{anomaly['upper_bound']:,.0f}"}]
                            </div>
                        </div>
                    </div>
                '''
            
            html += '</div>'
        else:
            html += '''
                <div class="normal-status">
                    <p>✅ 所有核心指标均在正常范围内</p>
                </div>
            '''
        
        # 渠道影响分析
        channel_analysis = analysis_result['channel_analysis']
        
        html += '''
            </div>
            
            <!-- 渠道影响分析 -->
            <div class="channel-section">
                <h3>🔍 渠道影响分析</h3>
        '''
        
        if channel_analysis['has_analysis'] and anomalies:
            html += f'''
                <div class="channel-info">
                    <p>分析了{channel_analysis['total_channels_analyzed']}个主要渠道对异常指标的影响</p>
                </div>
                
                <div class="channel-list">
            '''
            
            for insight in channel_analysis['channel_insights']:
                impact_class = 'positive' if insight['impact_score'] > 0 else 'negative' if insight['impact_score'] < 0 else 'neutral'
                
                html += f'''
                    <div class="channel-item {impact_class}">
                        <div class="channel-header">
                            <span class="channel-name">{insight['channel']}</span>
                            <span class="channel-users">{insight['quality_users']:,}人</span>
                        </div>
                        <div class="channel-metrics">
                            <span>ARPU: ¥{insight['metrics']['arpu']:.2f}</span>
                            <span>转化率: {insight['metrics']['conversion_rate']:.1f}%</span>
                        </div>
                        {insight['impact_details'] and f"<div class='channel-impact'>{'<br>'.join(insight['impact_details'])}</div>" or ""}
                    </div>
                '''
            
            html += '</div>'
        else:
            html += '''
                <div class="channel-placeholder">
                    <p>• 各渠道对大盘异常指标的影响程度</p>
                    <p>• 基于Good且认证用户数据进行渠道拆解</p>
                    <p>• 注：当前无异常指标需要渠道分析</p>
                </div>
            '''
        
        html += '''
            </div>
        </div>
        '''
        
        return html