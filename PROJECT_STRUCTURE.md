# 项目结构整理文档

## 概述
这是一个业务数据分析和报告生成系统，主要功能包括数据同步、异常检测、报告生成和自动化监控。

## 项目结构

### 📁 根目录文件
```
claude_demo/
├── 🔧 配置文件
│   ├── .pre-commit-config.yaml     # Pre-commit hooks配置
│   ├── .env                        # 环境变量
│   ├── .env.example               # 环境变量示例
│   └── requirements.txt           # Python依赖
│
├── 📋 主要脚本
│   ├── main.py                    # 主入口程序
│   ├── generate_standard_report.py # 标准报告生成器（主要）
│   ├── generate_daily_report.sh   # 日报告生成脚本
│   ├── sync_recent_data.sh        # 数据同步脚本
│   └── app.py                     # Web应用入口
│
├── 📚 文档
│   ├── README.md                  # 项目说明
│   ├── CLAUDE.md                  # AI工作流指导原则
│   ├── AUTO_CHECK_SETUP.md        # 自动检查设置
│   ├── DATA_VALIDATION_RULES.md   # 数据验证规则
│   ├── QUICK_MODULE_GUIDE.md      # 快速模块指南
│   └── module_management_guide.md # 模块管理指南
│
└── 🛠️ 其他工具
    ├── debug_arpu.py              # ARPU调试工具
    ├── demo_module_management.py  # 模块管理演示
    └── simple_correct_report.py   # 简化报告生成器
```

### 📁 核心目录结构

#### 🔥 **src/** - 核心功能模块
```
src/
├── 🔗 数据连接
│   ├── presto_connection.py       # Presto数据库连接
│   ├── presto_sync.py            # 数据同步核心
│   ├── database.py               # 本地数据库操作
│   └── data_manager.py           # 数据管理器
│
├── 🧮 数据分析
│   ├── data_analyzer.py          # 数据分析器
│   ├── anomaly_detector.py       # 异常检测
│   ├── simple_anomaly_detector.py # 简化异常检测
│   └── sql_generator.py          # SQL生成器
│
├── 📊 报告生成
│   ├── report_generator.py       # 报告生成器
│   ├── daily_report_generator.py # 日报告生成器
│   └── dashboard_formatter.py    # 仪表板格式化
│
├── 🛠️ 工具组件
│   ├── query_cache.py            # 查询缓存
│   ├── query_templates.py        # 查询模板
│   ├── sql_validator.py          # SQL验证器
│   ├── progress_bar.py           # 进度条
│   ├── error_handler.py          # 错误处理
│   ├── user_friendly_errors.py   # 用户友好错误
│   └── workflow.py               # 工作流
│
└── 📝 配置
    ├── presto_config.py          # Presto配置
    └── main.py                   # 模块主入口
```

#### ⚙️ **utils/** - 工具和自动化
```
utils/
├── 🔍 异常检测增强
│   ├── anomaly.py                # 基础异常检测
│   ├── advanced_anomaly.py       # 高级异常检测
│   ├── full_advanced_anomaly.py  # 完整异常检测
│   └── quick_advanced_anomaly.py # 快速异常检测
│
├── 🤖 自动化工具
│   ├── auto_fixer.py             # 自动修复器
│   ├── auto_fix_compliance.py    # 合规自动修复
│   ├── conditional_auto_fix.py   # 条件自动修复
│   ├── compliance_monitor.py     # 合规监控
│   └── setup_automation.sh       # 自动化设置
│
├── ✅ 代码质量
│   ├── code_consistency_checker.py # 代码一致性检查
│   ├── consistency_rules.json     # 一致性规则
│   └── consistency_report_*.json  # 一致性报告
│
└── 🔧 系统工具
    ├── dashboard.py              # 仪表板
    ├── integrated_report_system.py # 集成报告系统
    ├── unified_config_system.py  # 统一配置系统
    └── test_modular_report.py    # 模块化报告测试
```

#### 📊 **modular_report_system/** - 模块化报告系统
```
modular_report_system/
├── 📁 config/                    # 报告配置
│   ├── modules_config.py         # 模块配置
│   └── data_field_config.py      # 数据字段配置
│
├── 🏗️ core/                     # 核心框架
│   ├── base_module.py           # 基础模块
│   ├── data_collector.py        # 数据收集器
│   └── html_generator.py        # HTML生成器
│
├── 🧩 modules/                  # 报告模块
│   ├── overview_metrics.py      # 概览指标
│   ├── user_behavior_analysis.py # 用户行为分析
│   └── anomaly_detection.py     # 异常检测
│
├── 📝 reports/                  # 生成的报告
└── 🎛️ 控制器
    ├── daily_report_controller.py # 日报告控制器
    └── module_manager.py         # 模块管理器
```

#### 📊 **data/** - 数据存储
```
data/
├── 💾 数据库
│   ├── data.db                  # 主数据库
│   ├── main.db                  # 主数据库备份
│   └── business_data.db         # 业务数据库
│
├── 🔄 备份
│   └── backups/                 # 数据备份
│       └── data_backup_*.db
│
├── 📤 导出
│   └── exports/                 # 数据导出
│       ├── query_*.csv
│       ├── query_*.json
│       ├── query_*.xlsx
│       └── query_metadata.json
│
└── 📁 临时文件
    └── temp/                    # 临时数据
        └── daily_metrics_*.csv
```

#### 📋 **config/** - 配置管理
```
config/
├── presto_config.py             # Presto数据库配置
├── unified_config.yaml          # 统一配置文件
├── requirements.txt             # 依赖配置
└── legacy/                      # 遗留配置
    └── business_config.py
```

#### 📈 **output/** - 输出文件
```
output/
├── 📊 reports/                  # 生成的报告
│   ├── daily_report_*.html      # 日报告HTML文件
│   └── daily_reports/           # 日报告目录
└── 📝 logs/                     # 日志文件
    ├── daily_sync_real.log      # 数据同步日志
    └── sql_errors.log           # SQL错误日志
```

#### 🗄️ **archive/** - 归档文件
```
archive/
├── deprecated/                  # 已废弃文件
├── old_reports/                 # 旧报告
└── old_scripts/                 # 旧脚本
```

#### 🧰 **其他工具目录**
```
├── 🌐 templates/               # HTML模板
│   ├── base.html
│   ├── index.html
│   └── about.html
│
├── 🎨 static/                  # 静态资源
│   ├── css/
│   └── js/
│
├── 🔧 scripts/                 # 脚本工具
│   ├── manual_data_import.py
│   ├── migrate_data*.sql
│   ├── optimize_tables*.sql
│   └── update_code_compatibility.py
│
├── 🤖 ai-problem-analysis/     # AI问题分析
│   ├── README.md
│   ├── problem-analysis-framework.md
│   ├── root-cause-playbook.md
│   └── *-analysis-*.md
│
└── 🏗️ clean_project/          # 项目清理
    ├── archive/
    ├── config/
    ├── data/
    ├── docs/
    ├── lib/
    └── output/
```

## 🚀 主要功能模块

### 1. 数据同步 (Data Sync)
- **入口**: `src/presto_sync.py`
- **配置**: `config/presto_config.py`
- **功能**: 从Presto数据库同步业务数据到本地SQLite

### 2. 报告生成 (Report Generation)
- **主脚本**: `generate_standard_report.py`
- **shell脚本**: `generate_daily_report.sh`
- **模块化**: `modular_report_system/`
- **功能**: 生成标准化HTML业务报告

### 3. 异常检测 (Anomaly Detection)
- **基础**: `src/anomaly_detector.py`
- **高级**: `utils/advanced_anomaly.py`
- **功能**: 检测业务数据异常并生成警报

### 4. 自动化监控 (Automation)
- **Hooks**: `.pre-commit-config.yaml`
- **工具**: `utils/setup_automation.sh`
- **功能**: 代码质量检查和自动修复

## 🔧 关键配置文件

1. **`.pre-commit-config.yaml`** - Pre-commit hooks配置
2. **`config/presto_config.py`** - Presto数据库连接配置
3. **`config/unified_config.yaml`** - 统一系统配置
4. **`CLAUDE.md`** - AI工作流指导原则
5. **`DATA_VALIDATION_RULES.md`** - 数据验证规则

## 🚦 使用入口

### 主要命令
```bash
# 生成日报告
./generate_daily_report.sh 2025-08-06

# 手动数据同步
python3 src/presto_sync.py --date 2025-08-06

# 启动主程序
python3 main.py

# 启动Web界面
python3 app.py
```

### 重要路径
- 📊 **报告输出**: `./output/reports/`
- 💾 **数据存储**: `./data/data.db`
- 📝 **日志文件**: `./output/logs/`
- ⚙️ **配置文件**: `./config/`

## 📋 项目特点

1. **模块化设计** - 核心功能模块化，便于维护和扩展
2. **自动化流程** - 支持自动数据同步和报告生成
3. **异常检测** - 内置多级异常检测机制
4. **代码质量** - Pre-commit hooks确保代码质量
5. **配置管理** - 统一的配置管理系统
6. **AI驱动** - 集成AI工作流和问题分析

---
*最后更新: 2025-08-07*