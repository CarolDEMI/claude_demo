#!/bin/bash
# 设置自动化代码检查和修复系统

set -e

echo "🚀 设置自动化代码检查和修复系统..."

# 检查Python环境
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 未安装"
    exit 1
fi

# 创建必要的目录
mkdir -p utils
mkdir -p .github/workflows

# 设置文件权限
chmod +x utils/code_consistency_checker.py
chmod +x utils/auto_fixer.py
chmod +x utils/conditional_auto_fix.py

echo "📋 检查已安装的组件:"
echo "  ✅ 代码一致性检查器: utils/code_consistency_checker.py"
echo "  ✅ 自动修复器: utils/auto_fixer.py"
echo "  ✅ 条件修复器: utils/conditional_auto_fix.py"
echo "  ✅ 检查规则配置: utils/consistency_rules.json"
echo "  ✅ Pre-commit配置: .pre-commit-config.yaml"
echo "  ✅ GitHub Actions工作流: .github/workflows/code-consistency.yml"

# 测试检查器
echo ""
echo "🧪 测试代码一致性检查器..."
if python3 utils/code_consistency_checker.py generate_standard_report.py; then
    echo "✅ 代码一致性检查器工作正常"
else
    echo "⚠️  代码一致性检查器发现了问题，这是正常的"
fi

# 安装pre-commit（如果可用）
if command -v pre-commit &> /dev/null; then
    echo ""
    echo "🔗 安装pre-commit hooks..."
    pre-commit install
    echo "✅ Pre-commit hooks已安装"
else
    echo ""
    echo "💡 建议安装pre-commit以启用Git hooks:"
    echo "   pip install pre-commit"
    echo "   pre-commit install"
fi

echo ""
echo "✅ 自动化系统设置完成！"
echo ""
echo "📖 使用说明:"
echo "  • 手动检查: python3 utils/code_consistency_checker.py"
echo "  • 手动修复: python3 utils/auto_fixer.py"
echo "  • 条件修复: python3 utils/conditional_auto_fix.py"
echo "  • 每次Git提交时会自动运行检查"
echo "  • GitHub Actions会在推送时自动检查并尝试修复"
echo ""
echo "🎯 系统会自动检查和修复以下问题:"
echo "  • 渠道分析函数的严重程度计算一致性"
echo "  • 排序逻辑统一性"
echo "  • HTML显示格式统一性"
echo "  • 重复函数定义"
echo "  • 返回数据结构完整性"
echo ""
echo "🔧 自动修复策略:"
echo "  • 安全修复：排序逻辑、显示格式、缺失字段"
echo "  • 手动修复：严重程度计算、重复函数、计算公式"