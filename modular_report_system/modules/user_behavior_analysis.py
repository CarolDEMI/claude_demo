"""
用户行为分析模块
分析用户行为特征和偏好
"""

from core.base_module import BaseReportModule
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class UserBehaviorAnalysisModule(BaseReportModule):
    """用户行为分析模块"""
    
    def collect_data(self, target_date, conn):
        """收集用户行为数据"""
        
        # 1. 收集当日用户行为数据
        current_sql = """
        SELECT 
            dt,
            os_type,
            ad_channel,
            COUNT(*) as user_count,
            SUM(newuser) as total_users,
            SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as good_users,
            SUM(CASE WHEN verification_status = 'verified' THEN newuser ELSE 0 END) as verified_users,
            SUM(CASE WHEN gender = 'female' THEN newuser ELSE 0 END) as female_users,
            SUM(zizhu_revenue_1_aftertax) as revenue,
            AVG(zizhu_revenue_1_aftertax / NULLIF(newuser, 0)) as avg_revenue_per_user
        FROM cpz_qs_newuser_channel_i_d 
        WHERE dt = ?
        GROUP BY dt, os_type, ad_channel
        """
        
        current_df = pd.read_sql(current_sql, conn, params=[target_date])
        
        # 2. 收集历史对比数据（过去7天）
        start_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
        
        history_sql = """
        SELECT 
            dt,
            os_type,
            COUNT(DISTINCT ad_channel) as channel_count,
            SUM(newuser) as daily_users,
            SUM(CASE WHEN status = 'good' THEN newuser ELSE 0 END) as daily_good_users,
            SUM(zizhu_revenue_1_aftertax) as daily_revenue
        FROM cpz_qs_newuser_channel_i_d 
        WHERE dt BETWEEN ? AND ?
        GROUP BY dt, os_type
        ORDER BY dt DESC
        """
        
        history_df = pd.read_sql(history_sql, conn, params=[start_date, target_date])
        
        # 3. 收集渠道偏好数据
        channel_preference_sql = """
        SELECT 
            ad_channel,
            os_type,
            SUM(newuser) as users,
            SUM(CASE WHEN gender = 'female' THEN newuser ELSE 0 END) as female_users,
            SUM(zizhu_revenue_1_aftertax) as revenue,
            COUNT(DISTINCT dt) as active_days
        FROM cpz_qs_newuser_channel_i_d 
        WHERE dt >= ? AND dt <= ?
        GROUP BY ad_channel, os_type
        HAVING SUM(newuser) >= 50
        ORDER BY SUM(newuser) DESC
        LIMIT 20
        """
        
        channel_df = pd.read_sql(channel_preference_sql, conn, params=[start_date, target_date])
        
        return {
            'current_data': current_df,
            'history_data': history_df,
            'channel_preference': channel_df,
            'target_date': target_date,
            'analysis_period': f"{start_date} 至 {target_date}"
        }
    
    def analyze_data(self, data):
        """分析用户行为数据"""
        
        current_df = data['current_data']
        history_df = data['history_data']
        channel_df = data['channel_preference']
        
        if current_df.empty:
            return {'has_data': False, 'message': '当日无用户行为数据'}
        
        analysis_result = {'has_data': True}
        
        # 1. 操作系统偏好分析
        os_analysis = current_df.groupby('os_type').agg({
            'total_users': 'sum',
            'good_users': 'sum',
            'verified_users': 'sum',
            'female_users': 'sum',
            'revenue': 'sum'
        }).reset_index()
        
        os_analysis['good_rate'] = (os_analysis['good_users'] / os_analysis['total_users'] * 100).round(1)
        os_analysis['female_ratio'] = (os_analysis['female_users'] / os_analysis['total_users'] * 100).round(1)
        os_analysis['arpu'] = (os_analysis['revenue'] / os_analysis['verified_users']).round(2)
        
        analysis_result['os_analysis'] = os_analysis.to_dict('records')
        
        # 2. 渠道行为特征分析
        if not channel_df.empty:
            channel_behavior = []
            for _, row in channel_df.iterrows():
                behavior_score = self._calculate_behavior_score(row)
                channel_behavior.append({
                    'channel': row['ad_channel'],
                    'os_type': row['os_type'],
                    'users': int(row['users']),
                    'female_ratio': round(row['female_users'] / row['users'] * 100, 1),
                    'arpu': round(row['revenue'] / max(row['users'], 1), 2),
                    'behavior_score': behavior_score,
                    'behavior_level': self._get_behavior_level(behavior_score)
                })
            
            # 按行为评分排序
            channel_behavior.sort(key=lambda x: x['behavior_score'], reverse=True)
            analysis_result['channel_behavior'] = channel_behavior[:10]  # 取TOP10
        
        # 3. 趋势分析
        if not history_df.empty:
            # iOS vs Android 趋势
            ios_trend = history_df[history_df['os_type'] == 'iOS']['daily_users'].tolist()[-7:]
            android_trend = history_df[history_df['os_type'] == 'Android']['daily_users'].tolist()[-7:]
            
            analysis_result['trend_analysis'] = {
                'ios_trend': ios_trend,
                'android_trend': android_trend,
                'ios_avg': np.mean(ios_trend) if ios_trend else 0,
                'android_avg': np.mean(android_trend) if android_trend else 0
            }
        
        # 4. 关键洞察
        insights = []
        
        # 操作系统洞察
        if len(os_analysis) >= 2:
            ios_data = os_analysis[os_analysis['os_type'] == 'iOS']
            android_data = os_analysis[os_analysis['os_type'] == 'Android']
            
            if not ios_data.empty and not android_data.empty:
                ios_arpu = ios_data.iloc[0]['arpu']
                android_arpu = android_data.iloc[0]['arpu']
                
                if ios_arpu > android_arpu * 1.2:
                    insights.append(f"iOS用户ARPU(¥{ios_arpu:.2f})显著高于Android(¥{android_arpu:.2f})")
                elif android_arpu > ios_arpu * 1.2:
                    insights.append(f"Android用户ARPU(¥{android_arpu:.2f})显著高于iOS(¥{ios_arpu:.2f})")
        
        # 渠道洞察
        if 'channel_behavior' in analysis_result and analysis_result['channel_behavior']:
            best_channel = analysis_result['channel_behavior'][0]
            insights.append(f"最佳渠道：{best_channel['channel']}（{best_channel['os_type']}），行为评分{best_channel['behavior_score']:.1f}")
        
        analysis_result['insights'] = insights
        
        return analysis_result
    
    def _calculate_behavior_score(self, row):
        """计算渠道行为评分（0-100分）"""
        # 基础分数
        score = 50
        
        # 用户数量权重（最多+20分）
        user_score = min(20, row['users'] / 100 * 20)
        score += user_score
        
        # 女性比例权重（最多+15分）
        female_ratio = row['female_users'] / row['users'] * 100
        if female_ratio >= 60:
            score += 15
        elif female_ratio >= 50:
            score += 10
        elif female_ratio >= 40:
            score += 5
        
        # ARPU权重（最多+15分）
        arpu = row['revenue'] / max(row['users'], 1)
        if arpu >= 10:
            score += 15
        elif arpu >= 5:
            score += 10
        elif arpu >= 2:
            score += 5
        
        return min(100, max(0, score))
    
    def _get_behavior_level(self, score):
        """根据评分获取行为等级"""
        if score >= 85:
            return "优秀"
        elif score >= 70:
            return "良好"
        elif score >= 60:
            return "一般"
        else:
            return "待优化"
    
    def generate_html(self, analysis_result):
        """生成HTML内容"""
        
        if not analysis_result['has_data']:
            return f"""
            <div class="module">
                <h2>👥 用户行为分析</h2>
                <div class="no-data">
                    <p>{analysis_result.get('message', '暂无数据')}</p>
                </div>
            </div>
            """
        
        html = """
        <div class="module">
            <h2>👥 用户行为分析</h2>
            
            <!-- 关键洞察 -->
            <div class="insights-section">
                <h3>💡 关键洞察</h3>
        """
        
        for insight in analysis_result.get('insights', []):
            html += f'<div class="insight-item">• {insight}</div>'
        
        html += """
            </div>
            
            <!-- 操作系统偏好分析 -->
            <h3>📱 操作系统用户行为对比</h3>
            <div class="metrics-grid">
        """
        
        for os_data in analysis_result.get('os_analysis', []):
            html += f"""
            <div class="metric-card">
                <div class="icon">{'📱' if os_data['os_type'] == 'iOS' else '🤖'}</div>
                <div class="value">{os_data['total_users']:,}</div>
                <div class="label">{os_data['os_type']} 用户</div>
                <div class="sub-metrics">
                    <small>Good率: {os_data['good_rate']:.1f}%</small><br>
                    <small>女性比例: {os_data['female_ratio']:.1f}%</small><br>
                    <small>ARPU: ¥{os_data['arpu']:.2f}</small>
                </div>
            </div>
            """
        
        html += """
            </div>
            
            <!-- 渠道行为特征排行 -->
            <h3>🏆 渠道行为质量排行 (TOP10)</h3>
            <table>
                <thead>
                    <tr>
                        <th>排名</th>
                        <th>渠道</th>
                        <th>平台</th>
                        <th>用户数</th>
                        <th>女性比例</th>
                        <th>ARPU</th>
                        <th>行为评分</th>
                        <th>等级</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for idx, channel in enumerate(analysis_result.get('channel_behavior', []), 1):
            level_color = {
                '优秀': '#28a745',
                '良好': '#17a2b8', 
                '一般': '#ffc107',
                '待优化': '#dc3545'
            }.get(channel['behavior_level'], '#6c757d')
            
            html += f"""
            <tr>
                <td>{idx}</td>
                <td>{channel['channel']}</td>
                <td>{'📱' if channel['os_type'] == 'iOS' else '🤖'}</td>
                <td>{channel['users']:,}</td>
                <td>{channel['female_ratio']:.1f}%</td>
                <td>¥{channel['arpu']:.2f}</td>
                <td>{channel['behavior_score']:.1f}</td>
                <td><span style="color: {level_color}; font-weight: bold;">{channel['behavior_level']}</span></td>
            </tr>
            """
        
        html += """
                </tbody>
            </table>
            
            <div style="margin-top: 20px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; font-size: 12px;">
                <strong>📊 评分说明：</strong><br>
                • 基础分50分，用户数量最多+20分，女性比例最多+15分，ARPU表现最多+15分<br>
                • 优秀(85+分)：高价值用户群体，建议重点投入<br>
                • 良好(70-84分)：稳定表现渠道，可持续投放<br>
                • 一般(60-69分)：表现平庸，需要优化策略<br>
                • 待优化(60分以下)：投放效果差，建议暂停或调整
            </div>
        </div>
        """
        
        return html