#!/usr/bin/env python3
"""
用户友好的错误处理模块
"""
import re
import traceback
from typing import Dict, Tuple, Optional

class UserFriendlyErrorHandler:
    """用户友好的错误处理器"""
    
    def __init__(self):
        # 错误模式匹配和友好提示
        self.error_patterns = {
            # 网络连接问题
            r'connection.*refused|timeout|network.*unreachable': {
                'type': 'connection_failed',
                'message': '🔌 网络连接问题，无法连接到数据库服务器',
                'solutions': [
                    '检查Presto服务是否正在运行',
                    '确认网络连接正常',
                    '验证服务器地址和端口配置'
                ],
                'quick_fix': 'python3 main.py --health'
            },
            
            # 权限问题
            r'permission.*denied|access.*denied|forbidden': {
                'type': 'permission_denied',
                'message': '🔐 权限不足，无法访问资源',
                'solutions': [
                    '检查数据库用户权限',
                    '确认文件访问权限',
                    '联系管理员获取必要权限'
                ],
                'quick_fix': 'ls -la data.db && ls -la config/'
            },
            
            # 数据为空
            r'empty.*result|no.*data.*found|dataframe.*empty': {
                'type': 'data_empty',
                'message': '📊 没有找到数据，可能是日期范围或查询条件有误',
                'solutions': [
                    '检查指定的日期是否有数据',
                    '尝试扩大日期范围',
                    '检查数据同步是否正常'
                ],
                'quick_fix': 'python3 main.py --status'
            },
            
            # 文件不存在
            r'file.*not.*found|no.*such.*file': {
                'type': 'file_not_found',
                'message': '📁 文件未找到，可能是路径错误或文件缺失',
                'solutions': [
                    '检查文件路径是否正确',
                    '确认文件是否存在',
                    '检查工作目录是否正确'
                ],
                'quick_fix': 'pwd && ls -la'
            },
            
            # SQL错误
            r'sql.*error|syntax.*error.*sql|table.*not.*exist': {
                'type': 'sql_error',
                'message': '🗄️ 数据库查询错误，可能是表结构或SQL语法问题',
                'solutions': [
                    '检查数据库表是否存在',
                    '验证SQL语法是否正确',
                    '确认数据库结构是否匹配'
                ],
                'quick_fix': 'python3 -c "import sqlite3; conn=sqlite3.connect(\'data.db\'); print([t[0] for t in conn.execute(\'SELECT name FROM sqlite_master WHERE type=\\\'table\\\'\').fetchall()])"'
            }
        }
    
    def handle_error(self, error: Exception, context: str = "") -> Dict[str, any]:
        """处理错误并返回用户友好的信息"""
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        # 匹配错误模式
        for pattern, info in self.error_patterns.items():
            if re.search(pattern, error_str, re.IGNORECASE):
                return {
                    'type': info['type'],
                    'user_message': info['message'],
                    'solutions': info['solutions'],
                    'quick_fix': info['quick_fix'],
                    'context': context,
                    'original_error': str(error),
                    'error_type': error_type
                }
        
        # 未匹配到模式的通用处理
        return {
            'type': 'unknown_error',
            'user_message': f'🤔 发生了未知错误 ({error_type})',
            'solutions': [
                '检查系统日志获取详细信息',
                '尝试重新运行操作',
                '如问题持续，请联系技术支持'
            ],
            'quick_fix': 'python3 main.py --health',
            'context': context,
            'original_error': str(error),
            'error_type': error_type
        }
    
    def print_friendly_error(self, error: Exception, context: str = ""):
        """打印用户友好的错误信息"""
        error_info = self.handle_error(error, context)
        
        print(f"\n❌ {error_info['user_message']}")
        
        if error_info['context']:
            print(f"📍 发生位置: {error_info['context']}")
        
        print(f"\n💡 可能的解决方案:")
        for i, solution in enumerate(error_info['solutions'], 1):
            print(f"   {i}. {solution}")
        
        if error_info['quick_fix']:
            print(f"\n🔧 快速诊断命令:")
            print(f"   {error_info['quick_fix']}")

# 全局错误处理器实例
error_handler = UserFriendlyErrorHandler()

def safe_execute(func, *args, context: str = "", **kwargs):
    """安全执行函数，带有友好的错误处理"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        error_handler.print_friendly_error(e, context)
        return None