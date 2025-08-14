# 📊 模块化日常报告系统

一个灵活、可扩展的业务数据分析报告系统，采用模块化架构设计，支持多种数据源和输出格式。

## 🎯 系统特性

### 🏗️ 模块化架构
- **独立模块**: 每个分析模块独立运行，互不影响
- **统一接口**: 所有模块遵循相同的数据接口规范
- **动态加载**: 支持动态启用/禁用模块
- **易于扩展**: 简单添加新的分析模块

### 📊 当前模块

#### 1. 大盘指标模块 (`OverviewMetricsModule`)
- 核心业务指标总览
- 历史趋势对比分析
- 业务表现评分系统
- 关键指标变化监控

#### 2. 异常检测模块 (`AnomalyDetectionModule`)
- 智能异常指标检测
- 渠道影响程度分析
- 异常原因自动识别
- 优化建议自动生成

### 🎨 美观输出
- **现代化界面**: 响应式设计，支持移动端
- **交互式导航**: 平滑滚动，模块间快速跳转
- **数据可视化**: 丰富的图表和指标展示
- **多格式支持**: HTML网页、JSON数据格式

## 📁 项目结构

```
modular_report_system/
├── config/
│   └── modules_config.py          # 模块配置文件
├── core/
│   ├── base_module.py             # 模块基类
│   └── html_generator.py          # HTML生成器
├── modules/
│   ├── overview_metrics.py        # 大盘指标模块
│   └── anomaly_detection.py       # 异常检测模块
├── daily_report_controller.py     # 主控制器
└── README.md                      # 说明文档
```

## 🚀 快速开始

### 1. 系统要求
- Python 3.7+
- SQLite数据库
- pandas, numpy

### 2. 基本使用

```bash
# 测试数据库连接
python daily_report_controller.py --test

# 查看可用日期
python daily_report_controller.py --list-dates

# 生成最新日期的HTML报告
python daily_report_controller.py

# 生成指定日期的报告
python daily_report_controller.py 2025-07-26

# 生成JSON格式报告
python daily_report_controller.py 2025-07-26 --format json

# 查看系统配置
python daily_report_controller.py --config
```

### 3. 演示脚本

```bash
# 运行演示脚本（从上级目录）
python test_modular_report.py
```

## ⚙️ 配置说明

### 模块配置 (`config/modules_config.py`)

```python
# 启用/禁用模块
MODULES_CONFIG = {
    'overview_metrics': {
        'enabled': True,  # 设为False禁用模块
        'order': 1,       # 执行顺序
        'name': '大盘指标'
    }
}

# 大盘指标配置
OVERVIEW_CONFIG = {
    'comparison_days': 7,      # 对比天数
    'trend_threshold': 5.0     # 趋势变化阈值
}

# 异常检测配置
ANOMALY_CONFIG = {
    'detection_rules': {
        'arpu_anomaly': {
            'threshold_value': 15.0,  # 异常阈值(%)
            'severity': 'high'        # 严重程度
        }
    }
}
```

### 数据库配置

```python
DATABASE_CONFIG = {
    'path': './data.db',                           # 数据库路径
    'user_table': 'cpz_qs_newuser_channel_i_d',   # 用户数据表
    'cost_table': 'dwd_ttx_market_cash_cost_i_d'   # 成本数据表
}
```

## 📊 报告内容

### 大盘指标部分
- **核心指标**: 用户数、收入、ARPU、转化率等
- **趋势分析**: 日环比、周环比变化分析
- **性能评分**: 基于多维度的业务表现评分
- **质量分析**: 用户质量分布和占比

### 异常检测部分
- **异常概览**: 检测到的异常数量和严重程度
- **详细分析**: 每个异常指标的具体数据对比
- **渠道影响**: 各渠道对异常指标的贡献度排名
- **改进建议**: 基于异常分析的优化建议

## 🔧 扩展开发

### 添加新模块

1. **创建模块类**
```python
from core.base_module import BaseReportModule

class MyCustomModule(BaseReportModule):
    def collect_data(self, target_date, conn):
        # 数据收集逻辑
        return data
    
    def analyze_data(self, data):
        # 数据分析逻辑
        return analysis_result
    
    def generate_html(self, analysis_result):
        # HTML生成逻辑
        return html_content
```

2. **注册模块**
```python
# 在 modules_config.py 中添加
MODULES_CONFIG['my_module'] = {
    'name': '我的模块',
    'enabled': True,
    'order': 3,
    'class': 'MyCustomModule'
}
```

3. **更新控制器**
```python
# 在 daily_report_controller.py 中添加导入逻辑
elif class_name == 'MyCustomModule':
    from modules.my_custom_module import MyCustomModule
    module_class = MyCustomModule
```

### 自定义检测规则

```python
# 在 ANOMALY_CONFIG 中添加新规则
'new_metric_anomaly': {
    'name': '新指标异常',
    'threshold_type': 'percentage',
    'threshold_value': 20.0,
    'comparison_days': 7,
    'severity': 'medium'
}
```

## 📈 数据要求

### 用户数据表结构
```sql
CREATE TABLE cpz_qs_newuser_channel_i_d (
    dt TEXT,                        -- 日期
    ad_channel TEXT,                -- 渠道
    newuser INTEGER,                -- 用户数
    status TEXT,                    -- 用户状态 (good/fake)
    verification_status TEXT,       -- 认证状态 (verified/unverified)
    os_type TEXT,                   -- 系统类型 (iOS/Android)
    zizhu_revenue_1 REAL,          -- 税前收入
    zizhu_revenue_1_aftertax REAL   -- 税后收入
);
```

### 成本数据表结构
```sql
CREATE TABLE dwd_ttx_market_cash_cost_i_d (
    dt TEXT,        -- 日期
    cash_cost REAL  -- 现金成本
);
```

## 🎨 界面特性

- **响应式设计**: 支持桌面端和移动端
- **现代化样式**: 渐变背景、卡片布局、平滑动画
- **交互式导航**: 侧边栏导航，点击平滑滚动
- **数据可视化**: 进度条、徽章、图表等
- **暗色主题**: 支持暗色模式（未来版本）

## 📝 输出示例

### HTML报告特性
- 📊 实时数据可视化
- 🚨 异常指标高亮显示
- 📈 趋势变化图表
- 💡 智能分析建议
- 🔍 详细数据表格

### JSON报告内容
```json
{
  "report_date": "2025-07-26",
  "generation_time": "2025-07-28T10:30:00",
  "total_modules": 2,
  "successful_modules": 2,
  "modules": [...]
}
```

## 🚨 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查数据库文件路径
   - 确认表结构正确
   - 验证数据权限

2. **模块加载失败**
   - 检查Python路径配置
   - 确认模块依赖已安装
   - 查看错误日志信息

3. **报告生成失败**
   - 检查输出目录权限
   - 确认数据日期存在
   - 查看模块执行日志

### 调试模式

```bash
# 启用详细日志
python daily_report_controller.py --test
python daily_report_controller.py --config
```

## 📞 支持与反馈

如有问题或建议，请：
1. 查看故障排除部分
2. 运行测试命令诊断问题
3. 查看生成的错误日志
4. 联系开发团队

---

**版本**: v1.0  
**更新时间**: 2025-07-28  
**开发团队**: 数据分析团队