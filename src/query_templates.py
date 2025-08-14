"""
常见查询模板库
"""
from typing import Dict, List, Optional
import re
from business_config import TERM_MAPPING, DEFAULT_FILTERS

class QueryTemplates:
    def __init__(self):
        self.templates = self._init_templates()
        self.term_mapping = TERM_MAPPING
    
    def _init_templates(self) -> Dict:
        """初始化查询模板"""
        return {
            # 留存分析模板
            "retention_daily": {
                "name": "日留存率分析",
                "description": "计算各渠道的日留存率",
                "keywords": ["留存", "retention", "次留", "留存率"],
                "sql": """
                SELECT 
                    dt,
                    ad_channel,
                    SUM(newuser) as new_users,
                    SUM(is_returned_1_day) as retained_users,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(is_returned_1_day) * 100.0 / SUM(newuser), 2)
                        ELSE 0 
                    END as retention_rate_1d
                FROM cpz_qs_newuser_channel_i_d
                WHERE status = 'good' AND verification_status = 'verified' 
                    AND {date_condition}
                GROUP BY dt, ad_channel
                ORDER BY dt DESC, ad_channel
                """,
                "parameters": ["date_condition"],
                "examples": ["dt >= '2025-07-01'", "dt BETWEEN '2025-07-01' AND '2025-07-31'"]
            },
            
            # 渠道对比模板
            "channel_comparison": {
                "name": "渠道效果对比",
                "description": "对比不同渠道的获客效果",
                "keywords": ["渠道", "对比", "channel", "比较"],
                "sql": """
                SELECT 
                    ad_channel,
                    COUNT(DISTINCT dt) as active_days,
                    SUM(newuser) as total_new_users,
                    SUM(is_returned_1_day) as total_retained,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(is_returned_1_day) * 100.0 / SUM(newuser), 2)
                        ELSE 0 
                    END as avg_retention_rate,
                    ROUND(AVG(zizhu_revenue_1 / NULLIF(newuser, 0)), 2) as avg_arpu
                FROM cpz_qs_newuser_channel_i_d
                WHERE status = 'good' AND verification_status = 'verified'
                    AND {date_condition}
                GROUP BY ad_channel
                ORDER BY total_new_users DESC
                """,
                "parameters": ["date_condition"],
                "examples": ["dt >= '2025-07-01'"]
            },
            
            # ROI分析模板
            "roi_analysis": {
                "name": "ROI投资回报分析",
                "description": "计算各渠道的投资回报率",
                "keywords": ["ROI", "投资回报", "回本", "收益"],
                "sql": """
                SELECT 
                    a.dt,
                    a.ad_channel,
                    SUM(a.newuser) as new_users,
                    SUM(a.zizhu_revenue_1_aftertax) as revenue_after_tax,
                    SUM(b.cash_cost) as total_cost,
                    CASE 
                        WHEN SUM(b.cash_cost) > 0 
                        THEN ROUND(SUM(a.zizhu_revenue_1_aftertax) * 100.0 / SUM(b.cash_cost), 2)
                        ELSE 0 
                    END as roi_percentage,
                    CASE 
                        WHEN SUM(a.newuser) > 0 
                        THEN ROUND(SUM(b.cash_cost) / SUM(a.newuser), 2)
                        ELSE 0 
                    END as cpa
                FROM cpz_qs_newuser_channel_i_d a
                LEFT JOIN dwd_ttx_market_cash_cost_i_d b
                    ON a.dt = b.dt AND a.ad_channel = b.channel
                WHERE {date_condition}
                GROUP BY a.dt, a.ad_channel
                ORDER BY a.dt DESC, roi_percentage DESC
                """,
                "parameters": ["date_condition"],
                "examples": ["a.dt >= '2025-07-01'"]
            },
            
            # 用户质量分析
            "user_quality": {
                "name": "用户质量分析",
                "description": "分析不同状态用户的质量",
                "keywords": ["用户质量", "fake", "真实", "状态"],
                "sql": """
                SELECT 
                    status,
                    verification_status,
                    COUNT(DISTINCT dt) as days,
                    SUM(newuser) as total_users,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(is_returned_1_day) * 100.0 / SUM(newuser), 2)
                        ELSE 0 
                    END as avg_retention_rate,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(zizhu_revenue_1) / SUM(newuser), 2)
                        ELSE 0 
                    END as avg_arpu
                FROM cpz_qs_newuser_channel_i_d
                WHERE {date_condition}
                GROUP BY status, verification_status
                ORDER BY total_users DESC
                """,
                "parameters": ["date_condition"],
                "examples": ["dt >= '2025-07-01'"]
            },
            
            # 人群画像分析
            "demographic_analysis": {
                "name": "人群画像分析",
                "description": "分析不同人群特征的用户表现",
                "keywords": ["人群", "画像", "性别", "年龄", "城市"],
                "sql": """
                SELECT 
                    {group_field},
                    SUM(newuser) as total_users,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(is_returned_1_day) * 100.0 / SUM(newuser), 2)
                        ELSE 0 
                    END as retention_rate,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(zizhu_revenue_1) / SUM(newuser), 2)
                        ELSE 0 
                    END as arpu,
                    COUNT(CASE WHEN zizhu_revenue_1 > 0 THEN 1 END) as paying_users,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(COUNT(CASE WHEN zizhu_revenue_1 > 0 THEN 1 END) * 100.0 / SUM(newuser), 2)
                        ELSE 0 
                    END as paying_rate
                FROM cpz_qs_newuser_channel_i_d
                WHERE {date_condition}
                GROUP BY {group_field}
                ORDER BY total_users DESC
                """,
                "parameters": ["group_field", "date_condition"],
                "examples": {
                    "group_field": ["gender", "age_group", "dengji", "os_type"],
                    "date_condition": ["dt >= '2025-07-01'"]
                }
            },
            
            # 时间趋势分析
            "time_trend": {
                "name": "时间趋势分析",
                "description": "分析指标的时间变化趋势",
                "keywords": ["趋势", "时间", "变化", "走势"],
                "sql": """
                SELECT 
                    dt,
                    SUM(newuser) as daily_new_users,
                    SUM(is_returned_1_day) as daily_retained,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(is_returned_1_day) * 100.0 / SUM(newuser), 2)
                        ELSE 0 
                    END as daily_retention_rate,
                    SUM(zizhu_revenue_1) as daily_revenue,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(zizhu_revenue_1) / SUM(newuser), 2)
                        ELSE 0 
                    END as daily_arpu
                FROM cpz_qs_newuser_channel_i_d
                WHERE {date_condition}
                    {channel_condition}
                GROUP BY dt
                ORDER BY dt
                """,
                "parameters": ["date_condition", "channel_condition"],
                "examples": {
                    "date_condition": ["dt >= '2025-07-01'"],
                    "channel_condition": ["", "AND ad_channel = 'Douyin'"]
                }
            },
            
            # 代理商分析
            "agent_analysis": {
                "name": "代理商效果分析",
                "description": "分析各代理商的投放效果",
                "keywords": ["代理商", "agent", "代理"],
                "sql": """
                SELECT 
                    agent,
                    ad_channel,
                    SUM(newuser) as total_users,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(is_returned_1_day) * 100.0 / SUM(newuser), 2)
                        ELSE 0 
                    END as retention_rate,
                    CASE 
                        WHEN SUM(newuser) > 0 
                        THEN ROUND(SUM(zizhu_revenue_1) / SUM(newuser), 2)
                        ELSE 0 
                    END as arpu
                FROM cpz_qs_newuser_channel_i_d
                WHERE agent != '' 
                    AND {date_condition}
                GROUP BY agent, ad_channel
                ORDER BY total_users DESC
                """,
                "parameters": ["date_condition"],
                "examples": ["dt >= '2025-07-01'"]
            },
            
            # TOP N分析
            "top_n_analysis": {
                "name": "TOP N分析",
                "description": "查找表现最好/最差的维度",
                "keywords": ["TOP", "最好", "最差", "排名"],
                "sql": """
                SELECT 
                    {dimension},
                    {metric}
                FROM (
                    SELECT 
                        {dimension},
                        {metric_calculation} as {metric}
                    FROM cpz_qs_newuser_channel_i_d
                    WHERE {date_condition}
                    GROUP BY {dimension}
                )
                ORDER BY {metric} {order_direction}
                LIMIT {limit_num}
                """,
                "parameters": ["dimension", "metric", "metric_calculation", "date_condition", "order_direction", "limit_num"],
                "examples": {
                    "dimension": ["ad_channel", "agent", "dengji"],
                    "metric": ["total_users", "retention_rate", "arpu"],
                    "metric_calculation": ["SUM(newuser)", "SUM(is_returned_1_day)*100.0/SUM(newuser)", "SUM(zizhu_revenue_1)/SUM(newuser)"],
                    "order_direction": ["DESC", "ASC"],
                    "limit_num": ["10", "5", "20"]
                }
            },
            
            # 大盘核心指标模板
            "core_metrics_dashboard": {
                "name": "大盘核心指标",
                "description": "查询大盘的核心指标数据，包括带量、消耗、CPA、用户质量比等",
                "keywords": ["大盘", "核心指标", "带量", "消耗", "用户质量比"],
                "sql": """
                SELECT 
                    '{date}' as date,
                    '大盘核心指标' as metric_type,
                    -- 核心指标
                    SUM(a.newuser) as total_new_users,
                    SUM(b.cash_cost) as total_cost,
                    ROUND(SUM(b.cash_cost) / SUM(a.newuser), 2) as cpa,
                    ROUND(SUM(a.is_returned_1_day) * 100.0 / SUM(a.newuser), 2) as retention_rate,
                    ROUND(SUM(a.zizhu_revenue_1) / SUM(a.newuser), 2) as arpu,
                    -- 用户质量比
                    ROUND(SUM(CASE WHEN a.gender = 'female' THEN a.newuser ELSE 0 END) * 100.0 / SUM(a.newuser), 2) as female_ratio,
                    ROUND(SUM(CASE WHEN a.tag = 'T_NonBlueCollar' THEN a.newuser ELSE 0 END) * 100.0 / SUM(a.newuser), 2) as white_collar_ratio,
                    ROUND(SUM(CASE WHEN a.age_group IN ('20-', '20~23') THEN a.newuser ELSE 0 END) * 100.0 / SUM(a.newuser), 2) as young_ratio
                FROM cpz_qs_newuser_channel_i_d a
                LEFT JOIN dwd_ttx_market_cash_cost_i_d b 
                    ON a.dt = b.dt AND a.ad_channel = b.channel
                WHERE a.status = 'good' AND a.verification_status = 'verified'
                    AND a.dt = '{date}'
                """,
                "parameters": ["date"],
                "examples": ["2025-07-22"],
                "output_format": "dashboard"
            },
            
            # 成本效率分析
            "cost_efficiency": {
                "name": "成本效率分析",
                "description": "分析成本与收益的效率",
                "keywords": ["成本效率", "CPA", "LTV", "效率"],
                "sql": """
                SELECT 
                    a.ad_channel,
                    a.agent,
                    SUM(a.newuser) as total_users,
                    SUM(b.cash_cost) as total_cost,
                    CASE 
                        WHEN SUM(a.newuser) > 0 
                        THEN ROUND(SUM(b.cash_cost) / SUM(a.newuser), 2)
                        ELSE 0 
                    END as cpa,
                    SUM(a.zizhu_revenue_1_aftertax) as revenue_d1,
                    CASE 
                        WHEN SUM(b.cash_cost) > 0 
                        THEN ROUND(SUM(a.zizhu_revenue_1_aftertax) / SUM(b.cash_cost), 2)
                        ELSE 0 
                    END as roi_d1,
                    CASE 
                        WHEN SUM(a.zizhu_revenue_1_aftertax) > 0 
                        THEN ROUND(SUM(b.cash_cost) / SUM(a.zizhu_revenue_1_aftertax), 0)
                        ELSE NULL 
                    END as payback_days
                FROM cpz_qs_newuser_channel_i_d a
                LEFT JOIN dwd_ttx_market_cash_cost_i_d b
                    ON a.dt = b.dt 
                    AND a.ad_channel = b.channel
                    AND a.agent = b.agent
                WHERE {date_condition}
                GROUP BY a.ad_channel, a.agent
                HAVING SUM(a.newuser) > 0
                ORDER BY roi_d1 DESC
                """,
                "parameters": ["date_condition"],
                "examples": ["a.dt >= '2025-07-01'"]
            }
        }
    
    def get_template(self, template_name: str) -> Optional[Dict]:
        """获取指定模板"""
        return self.templates.get(template_name)
    
    def match_template(self, requirement: str) -> Optional[Dict]:
        """根据需求匹配最合适的模板"""
        requirement_lower = requirement.lower()
        best_match = None
        max_score = 0
        
        for template_name, template in self.templates.items():
            score = 0
            # 计算关键词匹配分数
            for keyword in template['keywords']:
                if keyword in requirement_lower:
                    score += 2
            
            # 检查模板名称匹配
            if any(word in requirement_lower for word in template['name'].split()):
                score += 1
            
            if score > max_score:
                max_score = score
                best_match = template
        
        return best_match if max_score > 0 else None
    
    def fill_template(self, template: Dict, parameters: Dict) -> str:
        """填充模板参数"""
        sql = template['sql']
        
        # 替换参数
        for param, value in parameters.items():
            placeholder = f"{{{param}}}"
            if placeholder in sql:
                sql = sql.replace(placeholder, value)
        
        # 清理未使用的可选参数
        sql = re.sub(r'\s*\{[^}]+\}\s*', ' ', sql)
        
        return sql.strip()
    
    def parse_requirement(self, requirement: str) -> Dict:
        """解析需求中的参数"""
        params = {}
        requirement_lower = requirement.lower()
        
        # 解析时间条件
        if "最近" in requirement:
            if "7天" in requirement or "七天" in requirement:
                params['date_condition'] = "dt >= DATE('now', '-7 days')"
            elif "30天" in requirement or "三十天" in requirement:
                params['date_condition'] = "dt >= DATE('now', '-30 days')"
            elif "1天" in requirement or "一天" in requirement:
                params['date_condition'] = "dt = DATE('now', '-1 day')"
        elif "本月" in requirement:
            params['date_condition'] = "strftime('%Y-%m', dt) = strftime('%Y-%m', 'now')"
        elif "上月" in requirement:
            params['date_condition'] = "strftime('%Y-%m', dt) = strftime('%Y-%m', 'now', '-1 month')"
        else:
            # 默认最近30天
            params['date_condition'] = "dt >= DATE('now', '-30 days')"
        
        # 解析分组字段
        if "性别" in requirement:
            params['group_field'] = 'gender'
        elif "年龄" in requirement:
            params['group_field'] = 'age_group'
        elif "城市" in requirement:
            params['group_field'] = 'dengji'
        elif "系统" in requirement or "操作系统" in requirement:
            params['group_field'] = 'os_type'
        
        # 解析渠道条件
        channels = ['Douyin', 'AppStore', 'Xiaohongshu', 'Wangyi']
        for channel in channels:
            if channel.lower() in requirement_lower:
                params['channel_condition'] = f"AND ad_channel = '{channel}'"
                break
        else:
            params['channel_condition'] = ""
        
        # 解析TOP N
        top_match = re.search(r'(?:TOP|top|前)\s*(\d+)', requirement)
        if top_match:
            params['limit_num'] = top_match.group(1)
        
        # 解析排序方向
        if "最好" in requirement or "最高" in requirement:
            params['order_direction'] = "DESC"
        elif "最差" in requirement or "最低" in requirement:
            params['order_direction'] = "ASC"
        
        return params
    
    def get_template_suggestions(self, requirement: str) -> List[Dict]:
        """获取可能适用的模板建议"""
        suggestions = []
        requirement_lower = requirement.lower()
        
        for template_name, template in self.templates.items():
            # 简单的关键词匹配
            if any(keyword in requirement_lower for keyword in template['keywords']):
                suggestions.append({
                    'name': template['name'],
                    'template_id': template_name,
                    'description': template['description']
                })
        
        return suggestions[:3]  # 返回前3个建议