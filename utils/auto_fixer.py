#!/usr/bin/env python3
"""
自动代码修复器
根据一致性检查结果自动修复常见问题
"""

import re
import os
import sys
import json
import shutil
from datetime import datetime
from typing import List, Dict, Any, Tuple
from code_consistency_checker import CodeConsistencyChecker

class AutoFixer:
    def __init__(self, target_file: str = "generate_standard_report.py"):
        self.target_file = target_file
        self.backup_file = f"{target_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.fixes_applied = []
        
    def fix_all_issues(self) -> Dict[str, Any]:
        """自动修复所有可修复的问题"""
        print("🔧 开始自动修复...")
        
        # 先检查问题
        checker = CodeConsistencyChecker(self.target_file)
        report = checker.check_all()
        
        if not report['issues'] and not report['warnings']:
            print("✅ 没有发现需要修复的问题")
            return {"status": "no_issues", "fixes": []}
        
        # 创建备份
        self.create_backup()
        
        # 读取文件内容
        with open(self.target_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        try:
            # 修复各类问题
            content = self.fix_missing_severity_score(content, report['issues'])
            content = self.fix_inconsistent_sorting(content, report['issues'])
            content = self.fix_duplicate_functions(content, report['issues'])
            content = self.add_missing_fields(content, report['warnings'])
            
            # 写入修复后的内容
            with open(self.target_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"✅ 修复完成，应用了 {len(self.fixes_applied)} 个修复")
            
            # 验证修复结果
            validation_report = checker.check_all()
            
            return {
                "status": "success",
                "fixes": self.fixes_applied,
                "backup_file": self.backup_file,
                "validation": validation_report['summary']
            }
            
        except Exception as e:
            print(f"❌ 修复过程中出错: {e}")
            # 恢复原始内容
            with open(self.target_file, 'w', encoding='utf-8') as f:
                f.write(original_content)
            return {"status": "error", "message": str(e)}
    
    def create_backup(self):
        """创建文件备份"""
        shutil.copy2(self.target_file, self.backup_file)
        print(f"📄 已创建备份: {self.backup_file}")
    
    def fix_missing_severity_score(self, content: str, issues: List[Dict]) -> str:
        """修复缺少严重程度分数计算的问题"""
        for issue in issues:
            if issue['type'] == 'missing_severity_score':
                func_name = issue['function']
                print(f"  🔧 修复 {func_name} 的 severity_score 计算...")
                
                # 查找函数内容
                func_pattern = rf'(def {func_name}\(.*?\n)(.*?)((?=def|\Z))'
                func_match = re.search(func_pattern, content, re.DOTALL)
                
                if func_match:
                    func_def = func_match.group(1)
                    func_content = func_match.group(2)
                    func_end = func_match.group(3)
                    
                    # 查找适合插入severity_score计算的位置
                    insert_patterns = [
                        # 在append字典之前插入
                        r'(\s+)(channel_impacts\.append\(\{)',
                        r'(\s+)(channel_.*_analysis\.append\(\{)',
                        # 在排序之前插入
                        r'(\s+)(.*\.sort\()',
                    ]
                    
                    for pattern in insert_patterns:
                        matches = re.finditer(pattern, func_content)
                        for match in reversed(list(matches)):  # 从后往前处理
                            indent = match.group(1)
                            
                            # 添加severity_score计算
                            severity_calc = f"\n{indent}# 计算严重程度分数\n"
                            severity_calc += f"{indent}severity_score = abs(weighted_impact) * (1 + weight * 10)\n"
                            
                            # 在匹配位置之前插入
                            pos = match.start()
                            func_content = func_content[:pos] + severity_calc + func_content[pos:]
                            
                            # 在字典中添加severity_score字段
                            if 'append({' in match.group(0):
                                # 查找字典结束位置并添加字段
                                dict_end_pattern = r'(\})\)'
                                dict_match = re.search(dict_end_pattern, func_content[pos:])
                                if dict_match:
                                    dict_pos = pos + dict_match.start()
                                    func_content = (func_content[:dict_pos] + 
                                                  f",\n{indent}    'severity_score': severity_score" + 
                                                  func_content[dict_pos:])
                            
                            break
                        break
                    
                    # 重构内容
                    content = content.replace(func_match.group(0), func_def + func_content + func_end)
                    self.fixes_applied.append(f"为 {func_name} 添加了 severity_score 计算")
        
        return content
    
    def fix_inconsistent_sorting(self, content: str, issues: List[Dict]) -> str:
        """修复不一致的排序逻辑"""
        for issue in issues:
            if issue['type'] == 'inconsistent_sorting':
                func_name = issue['function']
                print(f"  🔧 修复 {func_name} 的排序逻辑...")
                
                # 查找并替换排序语句
                sort_patterns = [
                    (r'\.sort\(key=lambda x: abs\(x\[[\'""]weighted_impact[\'""]\]\), reverse=True\)',
                     '.sort(key=lambda x: x[\'severity_score\'], reverse=True)'),
                    (r'\.sort\(key=lambda x: x\[[\'""]weight[\'""]\], reverse=True\)',
                     '.sort(key=lambda x: x[\'severity_score\'], reverse=True)'),
                    (r'\.sort\(key=lambda x: abs\(x\[[\'""].*_change[\'""]\]\), reverse=True\)',
                     '.sort(key=lambda x: x[\'severity_score\'], reverse=True)')
                ]
                
                for old_pattern, new_pattern in sort_patterns:
                    if re.search(old_pattern, content):
                        content = re.sub(old_pattern, new_pattern, content)
                        self.fixes_applied.append(f"修复了 {func_name} 的排序逻辑")
                        break
        
        return content
    
    def fix_duplicate_functions(self, content: str, issues: List[Dict]) -> str:
        """修复重复函数定义"""
        for issue in issues:
            if issue['type'] == 'duplicate_function':
                func_name = issue['function']
                print(f"  🔧 删除 {func_name} 的重复定义...")
                
                # 查找所有函数定义
                func_pattern = rf'def {func_name}\([^)]*\):(.*?)(?=def |\Z)'
                matches = list(re.finditer(func_pattern, content, re.DOTALL))
                
                if len(matches) > 1:
                    # 保留最后一个定义（通常是最完整的）
                    for match in matches[:-1]:
                        content = content.replace(match.group(0), '')
                    
                    self.fixes_applied.append(f"删除了 {func_name} 的重复定义")
        
        return content
    
    def add_missing_fields(self, content: str, warnings: List[Dict]) -> str:
        """添加缺失的返回字段"""
        for warning in warnings:
            if warning['type'] == 'missing_return_fields':
                func_name = warning['function']
                print(f"  🔧 为 {func_name} 添加缺失的返回字段...")
                
                # 查找return语句并添加缺失字段
                func_pattern = rf'(def {func_name}\(.*?\n)(.*?)(return \{{[^}}]*\}})'
                func_match = re.search(func_pattern, content, re.DOTALL)
                
                if func_match:
                    return_dict = func_match.group(3)
                    
                    # 添加analysis_type字段（如果缺失）
                    if "'analysis_type'" not in return_dict and '"analysis_type"' not in return_dict:
                        return_dict = return_dict.replace(
                            '}',
                            ",\n            'analysis_type': 'channel_analysis'\n        }"
                        )
                        content = content.replace(func_match.group(3), return_dict)
                        self.fixes_applied.append(f"为 {func_name} 添加了 analysis_type 字段")
        
        return content

def main():
    """主函数"""
    target_file = sys.argv[1] if len(sys.argv) > 1 else "generate_standard_report.py"
    
    fixer = AutoFixer(target_file)
    result = fixer.fix_all_issues()
    
    print("\n" + "="*60)
    print("🔧 自动修复报告")
    print("="*60)
    
    if result['status'] == 'no_issues':
        print("✅ 没有发现需要修复的问题")
    elif result['status'] == 'success':
        print(f"✅ 修复成功，应用了 {len(result['fixes'])} 个修复:")
        for i, fix in enumerate(result['fixes'], 1):
            print(f"  {i}. {fix}")
        print(f"\n📄 备份文件: {result['backup_file']}")
        
        validation = result['validation']
        print(f"\n📊 验证结果:")
        print(f"  状态: {'✅ PASS' if validation['status'] == 'PASS' else '❌ FAIL'}")
        print(f"  剩余问题: {validation['issues_count']}")
        print(f"  剩余警告: {validation['warnings_count']}")
    else:
        print(f"❌ 修复失败: {result['message']}")
        return 1
    
    print("="*60)
    return 0

if __name__ == "__main__":
    sys.exit(main())