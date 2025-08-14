#!/usr/bin/env python3
"""
代码一致性检查器
自动检查渠道分析函数的一致性问题
"""

import re
import os
import sys
from typing import List, Dict, Any, Tuple
import ast
import json
from datetime import datetime

class CodeConsistencyChecker:
    def __init__(self, target_file: str = "generate_standard_report.py"):
        self.target_file = target_file
        self.issues = []
        self.warnings = []
        self.channel_analysis_functions = []
        
    def check_all(self) -> Dict[str, Any]:
        """执行所有检查"""
        print("🔍 开始代码一致性检查...")
        
        # 读取目标文件
        if not os.path.exists(self.target_file):
            return {"error": f"文件不存在: {self.target_file}"}
        
        with open(self.target_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 执行各项检查
        self.check_channel_analysis_functions(content)
        self.check_severity_score_calculation(content)
        self.check_sorting_logic(content)
        self.check_display_format(content)
        self.check_return_structure(content)
        self.check_duplicate_functions(content)
        
        return self.generate_report()
    
    def check_channel_analysis_functions(self, content: str):
        """检查渠道分析函数"""
        print("  📊 检查渠道分析函数...")
        
        # 查找所有渠道分析函数
        pattern = r'def (_analyze_\w*_by_channel)\('
        matches = re.findall(pattern, content)
        self.channel_analysis_functions = matches
        
        print(f"    发现 {len(matches)} 个渠道分析函数")
        for func in matches:
            print(f"    - {func}")
    
    def check_severity_score_calculation(self, content: str):
        """检查严重程度分数计算"""
        print("  🎯 检查严重程度分数计算...")
        
        for func_name in self.channel_analysis_functions:
            # 查找函数定义到下一个函数定义之间的内容
            func_pattern = rf'def {func_name}\(.*?\n(.*?)(?=def|\Z)'
            func_match = re.search(func_pattern, content, re.DOTALL)
            
            if func_match:
                func_content = func_match.group(1)
                
                # 检查是否计算了severity_score
                if 'severity_score' not in func_content:
                    self.issues.append({
                        'type': 'missing_severity_score',
                        'function': func_name,
                        'message': f'{func_name} 缺少 severity_score 计算'
                    })
                else:
                    # 检查计算公式是否一致
                    score_patterns = [
                        r'severity_score\s*=\s*abs\([^)]+\)\s*\*\s*\(1\s*\+\s*[^)]+\*\s*10\)',
                        r'severity_score\s*=\s*abs\([^)]+\)\s*\*\s*\([^)]+\)'
                    ]
                    
                    found_pattern = False
                    for pattern in score_patterns:
                        if re.search(pattern, func_content):
                            found_pattern = True
                            break
                    
                    if not found_pattern:
                        self.warnings.append({
                            'type': 'inconsistent_severity_formula',
                            'function': func_name,
                            'message': f'{func_name} 的 severity_score 计算公式可能不一致'
                        })
    
    def check_sorting_logic(self, content: str):
        """检查排序逻辑"""
        print("  📈 检查排序逻辑...")
        
        for func_name in self.channel_analysis_functions:
            func_pattern = rf'def {func_name}\(.*?\n(.*?)(?=def|\Z)'
            func_match = re.search(func_pattern, content, re.DOTALL)
            
            if func_match:
                func_content = func_match.group(1)
                
                # 查找排序语句
                sort_patterns = [
                    r'\.sort\(key=lambda x: x\[[\'""]severity_score[\'""]\], reverse=True\)',
                    r'\.sort\(key=lambda x: x\.get\([\'""]severity_score[\'""]\)[^)]*\), reverse=True\)'
                ]
                
                has_severity_sort = False
                for pattern in sort_patterns:
                    if re.search(pattern, func_content):
                        has_severity_sort = True
                        break
                
                # 检查是否有其他排序方式
                other_sort_patterns = [
                    r'\.sort\(key=lambda x: abs\(x\[[\'""]weighted_impact[\'""]\]\)',
                    r'\.sort\(key=lambda x: x\[[\'""]weight[\'""]\]',
                    r'\.sort\(key=lambda x: abs\(x\[[\'""].*_change[\'""]\]\)'
                ]
                
                has_other_sort = False
                for pattern in other_sort_patterns:
                    if re.search(pattern, func_content):
                        has_other_sort = True
                        break
                
                if not has_severity_sort and has_other_sort:
                    self.issues.append({
                        'type': 'inconsistent_sorting',
                        'function': func_name,
                        'message': f'{func_name} 没有使用 severity_score 排序'
                    })
                elif not has_severity_sort and not has_other_sort:
                    self.warnings.append({
                        'type': 'no_sorting_found',
                        'function': func_name,
                        'message': f'{func_name} 未找到排序逻辑'
                    })
    
    def check_display_format(self, content: str):
        """检查HTML显示格式"""
        print("  🖥️  检查HTML显示格式...")
        
        # 查找HTML显示相关的代码
        display_patterns = [
            r'html \+= f".*权重.*严重程度',
            r'html \+= f".*severity_score'
        ]
        
        found_display = False
        for pattern in display_patterns:
            if re.search(pattern, content):
                found_display = True
                break
        
        if not found_display:
            self.warnings.append({
                'type': 'missing_display_format',
                'function': 'HTML生成',
                'message': 'HTML显示中可能缺少严重程度信息'
            })
    
    def check_return_structure(self, content: str):
        """检查返回数据结构"""
        print("  📋 检查返回数据结构...")
        
        for func_name in self.channel_analysis_functions:
            func_pattern = rf'def {func_name}\(.*?\n(.*?)(?=def|\Z)'
            func_match = re.search(func_pattern, content, re.DOTALL)
            
            if func_match:
                func_content = func_match.group(1)
                
                # 检查返回结构中是否包含必要字段
                required_fields = ['channel_impacts', 'analysis_type']
                missing_fields = []
                
                for field in required_fields:
                    if f"'{field}'" not in func_content and f'"{field}"' not in func_content:
                        missing_fields.append(field)
                
                if missing_fields:
                    self.warnings.append({
                        'type': 'missing_return_fields',
                        'function': func_name,
                        'message': f'{func_name} 返回结构可能缺少字段: {missing_fields}'
                    })
    
    def check_duplicate_functions(self, content: str):
        """检查重复函数"""
        print("  🔍 检查重复函数...")
        
        function_definitions = {}
        for func_name in self.channel_analysis_functions:
            count = len(re.findall(rf'def {func_name}\(', content))
            if count > 1:
                self.issues.append({
                    'type': 'duplicate_function',
                    'function': func_name,
                    'message': f'{func_name} 有 {count} 个重复定义'
                })
    
    def generate_report(self) -> Dict[str, Any]:
        """生成检查报告"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'file': self.target_file,
            'summary': {
                'total_functions': len(self.channel_analysis_functions),
                'issues_count': len(self.issues),
                'warnings_count': len(self.warnings),
                'status': 'PASS' if len(self.issues) == 0 else 'FAIL'
            },
            'functions_found': self.channel_analysis_functions,
            'issues': self.issues,
            'warnings': self.warnings
        }
        
        return report
    
    def print_report(self, report: Dict[str, Any]):
        """打印检查报告"""
        print("\n" + "="*60)
        print("📊 代码一致性检查报告")
        print("="*60)
        
        summary = report['summary']
        print(f"文件: {report['file']}")
        print(f"时间: {report['timestamp']}")
        print(f"状态: {'✅ PASS' if summary['status'] == 'PASS' else '❌ FAIL'}")
        print(f"函数总数: {summary['total_functions']}")
        print(f"问题数量: {summary['issues_count']}")
        print(f"警告数量: {summary['warnings_count']}")
        
        if report['issues']:
            print(f"\n❌ 发现 {len(report['issues'])} 个问题:")
            for i, issue in enumerate(report['issues'], 1):
                print(f"  {i}. [{issue['type']}] {issue['function']}: {issue['message']}")
        
        if report['warnings']:
            print(f"\n⚠️  发现 {len(report['warnings'])} 个警告:")
            for i, warning in enumerate(report['warnings'], 1):
                print(f"  {i}. [{warning['type']}] {warning['function']}: {warning['message']}")
        
        if not report['issues'] and not report['warnings']:
            print("\n🎉 代码一致性检查通过，未发现问题！")
        
        print("="*60)

def main():
    """主函数"""
    target_file = sys.argv[1] if len(sys.argv) > 1 else "generate_standard_report.py"
    
    checker = CodeConsistencyChecker(target_file)
    report = checker.check_all()
    
    # 打印报告
    checker.print_report(report)
    
    # 保存报告
    report_file = f"utils/consistency_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs("utils", exist_ok=True)
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"\n📄 详细报告已保存到: {report_file}")
    
    # 返回退出码
    return 1 if report['summary']['status'] == 'FAIL' else 0

if __name__ == "__main__":
    sys.exit(main())