# Claude AI 工作流指导原则

## 数据源优先级规则 ⚠️ 重要

### Presto数据库连接规则
当用户明确提到需要连接Presto数据库时：

1. **绝对不要使用备份数据**
   - 备份数据可能存在问题或不是最新的
   - 即使备份中有所需数据，也不能使用
   - 用户要求Presto连接说明需要最新、准确的数据

2. **Presto连接失败诊断步骤**
   ```bash
   # 检查代理设置
   echo $HTTP_PROXY
   echo $HTTPS_PROXY
   unset HTTP_PROXY HTTPS_PROXY  # 临时禁用代理
   
   # 测试网络连通性
   ping 10.1.11.39
   telnet 10.1.11.39 8000
   
   # 验证配置
   cat config/presto_config.py
   ```

3. **错误处理流程**
   - 首先尝试标准连接
   - 如果失败，自动禁用代理重试
   - 提供详细的诊断信息
   - 建议用户检查网络和配置

## 标准操作流程

### 数据同步
```bash
# 推荐使用方式
python3 src/presto_sync.py --date YYYY-MM-DD

# 确保配置文件存在
cp config/presto_config.py src/
```

### 报告生成
```bash
# 标准报告生成
./generate_daily_report.sh YYYY-MM-DD
```

## 常用命令备忘

### 数据验证
```sql
-- 检查数据是否存在
SELECT dt, COUNT(*) FROM cpz_qs_newuser_channel_i_d WHERE dt = 'YYYY-MM-DD';

-- 验证数据质量
SELECT 
  SUM(newuser) as total_users,
  SUM(CASE WHEN status = 'good' AND verification_status = 'verified' THEN newuser ELSE 0 END) as quality_users
FROM cpz_qs_newuser_channel_i_d 
WHERE dt = 'YYYY-MM-DD';
```

### 系统健康检查
```bash
# 快速状态检查
python3 main.py --status

# 系统健康检查
python3 main.py --health
```

## 注意事项

1. ⚠️ 如果用户说要连接Presto，就必须连接Presto
2. ⚠️ 不要在用户明确要求Presto时使用备份数据
3. ⚠️ 连接失败时提供代理设置诊断建议
4. ✅ 数据同步后要验证数据完整性
5. ✅ 报告生成要按照标准模板执行

## 数据分析原则 ⚠️ 重要

**数据不要猜想、估算和编造，都要真实的数据。**

- 只使用真实、准确的数据进行分析
- 不要猜想或推测数据值
- 不要估算可能不准确的数值
- 不要编造任何数据
- 保持数据分析的准确性和可信度

# 系统架构树状图 📊

```
claude_demo/
├── 🎯 核心入口层
│   ├── main.py                     # 统一配置报告系统主入口 [活跃]
│   ├── generate_daily_report.sh    # 标准报告生成脚本 [活跃]
│   └── generate_standard_report.py # 标准化报告生成器 [活跃]
│
├── 📊 数据层 (src/)
│   ├── presto_sync.py             # Presto数据同步 [核心]
│   ├── presto_connection.py       # 数据库连接管理 [核心]
│   ├── database.py                # 本地数据库操作 [核心]
│   ├── data_manager.py            # 数据管理器 [核心]
│   └── sql_generator.py           # SQL生成器 [核心]
│
├── 📈 分析层 (src/)
│   ├── data_analyzer.py           # 数据分析引擎 [核心]
│   ├── daily_report_generator.py  # 日报生成器 [活跃]
│   └── report_generator.py        # 通用报告生成 [核心]
│
├── ⚙️ 配置层 (config/)
│   ├── presto_config.py           # Presto连接配置 [核心]
│   └── unified_config.yaml        # 统一配置文件 [核心]
│
├── 📁 数据存储 (data/)
│   ├── data.db                    # 主数据库 [核心]
│   ├── business_data.db           # 业务数据库 [活跃]
│   ├── backups/                   # 数据备份 [维护]
│   └── exports/                   # 数据导出 [输出]
│
├── 📋 输出层 (output/)
│   ├── reports/                   # 生成的报告 [输出]
│   └── logs/                      # 系统日志 [监控]
│
└── 🛠️ 工具层 (utils/ & scripts/)
    ├── auto_fixer.py              # 自动修复工具 [维护]
    ├── code_consistency_checker.py# 代码一致性检查 [维护]
    └── optimize_database.py       # 数据库优化 [维护]

🔴 需要清理的冗余模块:
├── modular_report_system/         # 旧的模块化系统 [冗余]
├── src/main.py                    # 与根目录main.py功能重叠 [冗余]
├── utils/integrated_report_system.py # 功能已整合到主系统 [冗余]
└── 多个重复的报告生成器文件      # 功能重叠 [冗余]
```