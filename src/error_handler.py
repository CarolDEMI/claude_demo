"""
错误处理和重试机制
"""
import time
import logging
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
import json

class SQLErrorHandler:
    def __init__(self, max_retries: int = 3, retry_delay: float = 1.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.error_log = []
        self._setup_logging()
    
    def _setup_logging(self):
        """设置日志记录"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('sql_errors.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def handle_error(self, error: Exception, context: Dict) -> Dict:
        """处理错误并返回错误信息"""
        error_info = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context,
            'timestamp': datetime.now().isoformat()
        }
        
        # 记录错误
        self.error_log.append(error_info)
        self.logger.error(f"SQL Error: {error_info}")
        
        # 分析错误类型并提供建议
        suggestions = self._analyze_error(error, context)
        error_info['suggestions'] = suggestions
        
        return error_info
    
    def _analyze_error(self, error: Exception, context: Dict) -> List[str]:
        """分析错误并提供修复建议"""
        error_str = str(error).lower()
        suggestions = []
        
        # SQL语法错误
        if 'syntax error' in error_str:
            suggestions.append("检查SQL语法，特别是关键字拼写和标点符号")
            if 'near' in error_str:
                # 尝试提取错误位置
                suggestions.append(f"错误可能在: {error_str.split('near')[1].split()[0]}")
        
        # 表不存在
        elif 'no such table' in error_str or 'table not found' in error_str:
            suggestions.append("检查表名是否正确，注意大小写")
            suggestions.append("使用 .tables 命令查看所有可用的表")
        
        # 列不存在
        elif 'no such column' in error_str or 'column not found' in error_str:
            suggestions.append("检查列名是否正确")
            suggestions.append("使用 PRAGMA table_info(table_name) 查看表结构")
        
        # 类型错误
        elif 'datatype mismatch' in error_str:
            suggestions.append("检查数据类型是否匹配")
            suggestions.append("数值计算不能用于文本字段")
        
        # 除零错误
        elif 'division by zero' in error_str:
            suggestions.append("使用 CASE WHEN 或 NULLIF 避免除零")
            suggestions.append("示例: value / NULLIF(divisor, 0)")
        
        # 聚合错误
        elif 'aggregate' in error_str:
            suggestions.append("使用聚合函数时，SELECT中的非聚合列必须在GROUP BY中")
            suggestions.append("检查是否遗漏了GROUP BY子句")
        
        # API错误
        elif 'api' in error_str or 'credit' in error_str:
            suggestions.append("检查API密钥和账户余额")
            suggestions.append("考虑使用查询缓存或模板")
        
        return suggestions
    
    def retry_with_fixes(self, func: Callable, *args, **kwargs) -> Any:
        """带自动修复的重试机制"""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                # 尝试执行
                result = func(*args, **kwargs)
                if attempt > 0:
                    self.logger.info(f"Success after {attempt} retries")
                return result
            
            except Exception as e:
                last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                
                # 尝试自动修复
                if attempt < self.max_retries - 1:
                    fixed_args, fixed_kwargs = self._attempt_auto_fix(
                        e, func.__name__, args, kwargs
                    )
                    if fixed_args is not None:
                        args = fixed_args
                        kwargs = fixed_kwargs
                        self.logger.info("Attempting auto-fix...")
                    
                    time.sleep(self.retry_delay * (attempt + 1))
        
        # 所有重试都失败
        raise last_error
    
    def _attempt_auto_fix(self, error: Exception, func_name: str, 
                         args: tuple, kwargs: dict) -> tuple:
        """尝试自动修复常见错误"""
        error_str = str(error).lower()
        
        # 对于SQL生成函数
        if func_name == 'generate_sql' and len(args) > 0:
            requirement = args[0]
            
            # 修复常见的需求描述问题
            if 'syntax error' in error_str:
                # 尝试简化需求描述
                simplified = self._simplify_requirement(requirement)
                if simplified != requirement:
                    return (simplified,) + args[1:], kwargs
            
            # 添加缺失的条件
            if 'group by' in error_str:
                if "按" in requirement and "分组" not in requirement:
                    requirement += " 并按相应字段分组"
                    return (requirement,) + args[1:], kwargs
        
        return None, None
    
    def _simplify_requirement(self, requirement: str) -> str:
        """简化需求描述"""
        # 移除可能导致混淆的词
        confusing_words = ['请', '帮我', '我想', '能否', '可以']
        simplified = requirement
        for word in confusing_words:
            simplified = simplified.replace(word, '')
        
        return simplified.strip()
    
    def log_successful_recovery(self, original_error: Dict, 
                              fixed_query: str, result: Any):
        """记录成功的错误恢复"""
        recovery_info = {
            'original_error': original_error,
            'fixed_query': fixed_query,
            'result_preview': str(result)[:200],
            'timestamp': datetime.now().isoformat()
        }
        
        # 保存到恢复日志
        with open('error_recovery.json', 'a', encoding='utf-8') as f:
            json.dump(recovery_info, f, ensure_ascii=False)
            f.write('\n')
        
        self.logger.info("Successfully recovered from error")
    
    def get_error_stats(self) -> Dict:
        """获取错误统计"""
        if not self.error_log:
            return {'total_errors': 0}
        
        error_types = {}
        for error in self.error_log:
            error_type = error['error_type']
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        return {
            'total_errors': len(self.error_log),
            'error_types': error_types,
            'most_common_error': max(error_types.items(), key=lambda x: x[1])[0],
            'last_error_time': self.error_log[-1]['timestamp']
        }


class QueryOptimizer:
    """查询优化器 - 基于错误历史优化查询"""
    
    def __init__(self):
        self.optimization_rules = self._init_rules()
    
    def _init_rules(self) -> List[Dict]:
        """初始化优化规则"""
        return [
            {
                'name': 'avoid_division_by_zero',
                'pattern': r'(\w+)\s*/\s*(\w+)',
                'replacement': r'CASE WHEN \2 > 0 THEN \1 / \2 ELSE 0 END',
                'description': '避免除零错误'
            },
            {
                'name': 'add_null_handling',
                'pattern': r'SUM\((\w+)\)',
                'replacement': r'SUM(COALESCE(\1, 0))',
                'description': '处理NULL值'
            },
            {
                'name': 'optimize_like_pattern',
                'pattern': r"LIKE\s+'%(\w+)%'",
                'replacement': r"LIKE '%\1%'",
                'description': '优化LIKE查询'
            },
            {
                'name': 'add_limit_for_preview',
                'pattern': r'(SELECT\s+.*?)(\s+ORDER\s+BY\s+.*?)?$',
                'replacement': r'\1\2 LIMIT 1000',
                'description': '添加LIMIT限制结果集大小',
                'condition': lambda sql: 'LIMIT' not in sql.upper()
            }
        ]
    
    def optimize_query(self, sql: str, error_history: List[Dict] = None) -> str:
        """基于历史错误优化查询"""
        optimized_sql = sql
        
        # 应用优化规则
        for rule in self.optimization_rules:
            if 'condition' in rule:
                if not rule['condition'](sql):
                    continue
            
            # 应用正则替换
            import re
            optimized_sql = re.sub(
                rule['pattern'], 
                rule['replacement'], 
                optimized_sql,
                flags=re.IGNORECASE
            )
        
        # 基于错误历史的特定优化
        if error_history:
            for error in error_history:
                if 'division by zero' in error.get('error_message', ''):
                    # 确保所有除法都有保护
                    optimized_sql = self._protect_all_divisions(optimized_sql)
        
        return optimized_sql
    
    def _protect_all_divisions(self, sql: str) -> str:
        """保护所有除法操作"""
        import re
        
        # 查找所有除法操作
        division_pattern = r'(\w+(?:\([^)]*\))?)\s*/\s*(\w+(?:\([^)]*\))?)'
        
        def replace_division(match):
            numerator = match.group(1)
            denominator = match.group(2)
            return f"CASE WHEN {denominator} > 0 THEN {numerator} / {denominator} ELSE 0 END"
        
        return re.sub(division_pattern, replace_division, sql)
    
    def suggest_index(self, sql: str, db_info: Dict) -> List[str]:
        """建议创建索引以优化查询"""
        suggestions = []
        
        # 分析WHERE子句
        where_match = re.search(r'WHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            
            # 提取WHERE中的列
            column_pattern = r'(\w+)\s*(?:=|>|<|>=|<=|LIKE)'
            columns = re.findall(column_pattern, where_clause)
            
            for col in set(columns):
                suggestions.append(f"CREATE INDEX idx_{col} ON table_name({col});")
        
        # 分析JOIN条件
        join_pattern = r'JOIN\s+\w+\s+ON\s+(.*?)(?:WHERE|GROUP|ORDER|LIMIT|$)'
        join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
        
        for join_condition in join_matches:
            columns = re.findall(r'(\w+\.\w+)', join_condition)
            for col in set(columns):
                table, column = col.split('.')
                suggestions.append(f"CREATE INDEX idx_{table}_{column} ON {table}({column});")
        
        return suggestions