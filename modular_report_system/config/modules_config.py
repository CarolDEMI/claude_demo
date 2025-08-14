"""
模块化报告系统配置
定义各个报告模块的配置信息
"""

# 模块配置
MODULES_CONFIG = {
    'overview_metrics': {
        'name': '大盘指标',
        'description': '核心业务指标总览',
        'icon': '📊',
        'enabled': True,
        'order': 1,
        'class': 'OverviewMetricsModule'
    },
    'anomaly_detection': {
        'name': '异常指标分析',
        'description': '异常检测与渠道影响分析',
        'icon': '🚨',
        'enabled': True,
        'order': 2,
        'class': 'AnomalyDetectionModule'
    },
    'user_behavior_analysis': {
        'name': '用户行为分析',
        'description': '分析用户行为特征和偏好',
        'icon': '👥',
        'enabled': True,
        'order': 3,
        'class': 'UserBehaviorAnalysisModule'
    },
    'future_module': {
        'name': '待扩展模块',
        'description': '预留的第四个模块',
        'icon': '🔮',
        'enabled': False,
        'order': 4,
        'class': 'FutureModule'
    }
}

# 大盘指标配置
OVERVIEW_CONFIG = {
    'main_kpi': [
        {
            'name': 'Good且认证用户数',
            'field': 'quality_users',
            'icon': '⭐',
            'format': 'number',
            'description': '核心KPI：Good状态且已认证用户',
            'category': 'main_kpi'
        },
        {
            'name': 'CPA',
            'field': 'cpa',
            'icon': '💰',
            'format': 'currency',
            'description': '获客成本（基于Good且认证用户）',
            'category': 'main_kpi'
        },
        {
            'name': 'ARPU（税后）',
            'field': 'arpu_after_tax',
            'icon': '📈',
            'format': 'currency',
            'description': '基于Good+认证用户的税后ARPU',
            'category': 'main_kpi'
        },
        {
            'name': '次留率',
            'field': 'retention_rate',
            'icon': '🔄',
            'format': 'percentage',
            'description': '次日留存率',
            'category': 'main_kpi'
        }
    ],
    'user_quality': [
        {
            'name': '女性占比',
            'field': 'female_ratio',
            'icon': '👩',
            'format': 'percentage',
            'description': '女性用户占比',
            'category': 'user_quality'
        },
        {
            'name': '白领占比',
            'field': 'whitecollar_ratio',
            'icon': '💼',
            'format': 'percentage',
            'description': '白领用户占比',
            'category': 'user_quality'
        },
        {
            'name': '年轻占比',
            'field': 'young_ratio',
            'icon': '🧑‍💼',
            'format': 'percentage',
            'description': '18-30岁用户占比',
            'category': 'user_quality'
        }
    ],
    'conversion_metrics': [
        {
            'name': 'Good率',
            'field': 'good_rate',
            'icon': '✅',
            'format': 'percentage',
            'description': '用户通过Good筛选的比例',
            'category': 'conversion'
        },
        {
            'name': '认证率',
            'field': 'verified_rate',
            'icon': '🔐',
            'format': 'percentage',
            'description': '用户完成认证的比例',
            'category': 'conversion'
        },
        {
            'name': 'Good且认证率',
            'field': 'quality_rate',
            'icon': '🎯',
            'format': 'percentage',
            'description': '用户同时通过Good筛选和认证的比例',
            'category': 'conversion'
        }
    ],
    'comparison_days': 7,  # 对比天数
    'trend_threshold': 5.0  # 趋势变化阈值（%）
}

# 异常检测配置
ANOMALY_CONFIG = {
    'detection_method': 'quartile',  # 使用四分位数方法
    'quartile_multiplier': 1.5,     # 1.5倍四分位距作为异常阈值
    'history_days': 14,              # 使用14天历史数据计算四分位数
    'detection_rules': {
        'quality_users_anomaly': {
            'name': 'Good且认证用户数异常',
            'field': 'quality_users',
            'severity': 'high',
            'description': '核心KPI指标异常'
        },
        'cpa_anomaly': {
            'name': 'CPA异常',
            'field': 'cpa',
            'severity': 'high',
            'description': '获客成本异常'
        },
        'arpu_anomaly': {
            'name': 'ARPU异常',
            'field': 'arpu_after_tax',
            'severity': 'high',
            'description': '用户平均收入异常'
        },
        'retention_anomaly': {
            'name': '次留率异常',
            'field': 'retention_rate',
            'severity': 'medium',
            'description': '用户留存率异常'
        },
        'good_rate_anomaly': {
            'name': 'Good率异常',
            'field': 'good_rate',
            'severity': 'medium',
            'description': '用户质量筛选异常'
        },
        'verified_rate_anomaly': {
            'name': '认证率异常',
            'field': 'verified_rate',
            'severity': 'medium',
            'description': '用户认证转化异常'
        },
        'quality_rate_anomaly': {
            'name': 'Good且认证率异常',
            'field': 'quality_rate',
            'severity': 'high',
            'description': '整体转化漏斗异常'
        }
    },
    'channel_analysis': {
        'min_users_for_analysis': 100,  # 最小用户数才纳入分析
        'top_channels_count': 15,       # 分析TOP15渠道
        'impact_threshold': 5.0         # 影响程度阈值（%）
    }
}

# HTML报告配置
HTML_CONFIG = {
    'title': '每日业务数据报告',
    'css_style': 'modern',  # 样式主题
    'chart_enabled': True,  # 是否启用图表
    'export_path': './reports/daily_reports/',
    'template_path': './modular_report_system/templates/'
}

# 数据库配置
DATABASE_CONFIG = {
    'path': '../data/data.db',
    'user_table': 'cpz_qs_newuser_channel_i_d',
    'cost_table': 'dwd_ttx_market_cash_cost_i_d',
    'timeout': 30
}