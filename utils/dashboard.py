#!/usr/bin/env python3
"""
大盘核心指标快捷命令
使用方法: python3 dashboard.py 2025-07-22
"""
import sys
import os
# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.append(os.path.join(project_root, 'src'))
# 切换到项目根目录以访问data.db
os.chdir(project_root)
from dashboard_formatter import show_dashboard

def main():
    if len(sys.argv) != 2:
        print("使用方法: python3 dashboard.py YYYY-MM-DD")
        print("示例: python3 dashboard.py 2025-07-22")
        return
    
    date = sys.argv[1]
    
    # 简单的日期格式验证
    if len(date) != 10 or date[4] != '-' or date[7] != '-':
        print("错误: 日期格式应为 YYYY-MM-DD")
        return
    
    try:
        result = show_dashboard(date)
        print(result)
    except Exception as e:
        print(f"错误: {str(e)}")

if __name__ == '__main__':
    main()