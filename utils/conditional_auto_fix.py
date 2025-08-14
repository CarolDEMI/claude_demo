#!/usr/bin/env python3
"""
条件自动修复器
仅在安全的情况下自动修复问题，否则报告问题
"""

import sys
import os
from code_consistency_checker import CodeConsistencyChecker
from auto_fixer import AutoFixer

# 定义安全修复的问题类型
SAFE_FIX_TYPES = {
    'inconsistent_sorting',  # 排序逻辑修复通常是安全的
    'missing_return_fields'  # 添加缺失字段通常是安全的
}

# 需要人工确认的问题类型
MANUAL_FIX_TYPES = {
    'missing_severity_score',  # 添加计算逻辑需要仔细检查
    'duplicate_function',      # 删除重复函数需要确认保留哪个
    'inconsistent_severity_formula'  # 公式不一致需要人工判断
}

def main():
    """主函数"""
    target_file = sys.argv[1] if len(sys.argv) > 1 else "generate_standard_report.py"
    
    # 检查问题
    checker = CodeConsistencyChecker(target_file)
    report = checker.check_all()
    
    if not report['issues'] and not report['warnings']:
        print("✅ 代码一致性检查通过")
        return 0
    
    # 分析问题类型
    safe_issues = []
    manual_issues = []
    
    for issue in report['issues']:
        if issue['type'] in SAFE_FIX_TYPES:
            safe_issues.append(issue)
        else:
            manual_issues.append(issue)
    
    # 对于安全的问题，自动修复
    if safe_issues:
        print(f"🔧 发现 {len(safe_issues)} 个可安全修复的问题，正在自动修复...")
        fixer = AutoFixer(target_file)
        
        # 创建临时报告只包含安全问题
        safe_report = {
            'issues': safe_issues,
            'warnings': [w for w in report['warnings'] if w['type'] in SAFE_FIX_TYPES]
        }
        
        # 尝试修复
        try:
            # 这里需要修改AutoFixer以接受特定问题列表
            result = fixer.fix_specific_issues(safe_report)
            if result['status'] == 'success':
                print(f"✅ 自动修复了 {len(result['fixes'])} 个问题")
            else:
                print(f"❌ 自动修复失败: {result.get('message', '未知错误')}")
                return 1
        except Exception as e:
            print(f"❌ 自动修复过程中出错: {e}")
            return 1
    
    # 对于需要人工处理的问题，报告并阻止提交
    if manual_issues:
        print(f"\n❌ 发现 {len(manual_issues)} 个需要人工处理的问题:")
        for i, issue in enumerate(manual_issues, 1):
            print(f"  {i}. [{issue['type']}] {issue['function']}: {issue['message']}")
        
        print(f"\n💡 建议操作:")
        print(f"  1. 运行 'python3 utils/code_consistency_checker.py' 查看详细报告")
        print(f"  2. 手动修复问题或运行 'python3 utils/auto_fixer.py' 进行完整修复")
        print(f"  3. 重新提交")
        
        return 1
    
    # 重新检查修复后的代码
    final_report = checker.check_all()
    if final_report['summary']['status'] != 'PASS':
        print(f"❌ 修复后仍有问题，请手动检查")
        return 1
    
    print("✅ 所有问题已修复，代码一致性检查通过")
    return 0

if __name__ == "__main__":
    sys.exit(main())