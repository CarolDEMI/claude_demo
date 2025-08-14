"""
SQL验证器 - 验证生成的SQL语句的正确性
"""
import re
import sqlite3
from typing import Dict, List, Tuple, Optional
from business_config import BUSINESS_CONFIG, DATA_QUALITY_RULES

class SQLValidator:
    def __init__(self):
        self.business_config = BUSINESS_CONFIG
        self.data_quality_rules = DATA_QUALITY_RULES
    
    def validate_sql(self, sql: str, db_info: Dict) -> Tuple[bool, List[str]]:
        """
        验证SQL语句的正确性
        返回: (是否有效, 错误/警告列表)
        """
        errors = []
        warnings = []
        
        # 1. 基础语法检查
        syntax_errors = self._check_syntax(sql)
        errors.extend(syntax_errors)
        
        # 2. 表名验证
        table_errors = self._check_tables(sql, db_info)
        errors.extend(table_errors)
        
        # 3. 列名验证
        column_errors = self._check_columns(sql, db_info)
        errors.extend(column_errors)
        
        # 4. 数据类型检查
        type_warnings = self._check_data_types(sql, db_info)
        warnings.extend(type_warnings)
        
        # 5. 聚合函数检查
        agg_errors = self._check_aggregations(sql)
        errors.extend(agg_errors)
        
        # 6. JOIN条件检查
        join_warnings = self._check_joins(sql)
        warnings.extend(join_warnings)
        
        # 7. 性能检查
        perf_warnings = self._check_performance(sql)
        warnings.extend(perf_warnings)
        
        # 8. 业务逻辑检查
        biz_warnings = self._check_business_logic(sql)
        warnings.extend(biz_warnings)
        
        all_issues = errors + warnings
        is_valid = len(errors) == 0
        
        return is_valid, all_issues
    
    def _check_syntax(self, sql: str) -> List[str]:
        """检查基础SQL语法"""
        errors = []
        sql_upper = sql.upper()
        
        # 检查基本结构
        if not any(keyword in sql_upper for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
            errors.append("SQL语句缺少主要操作关键字(SELECT/INSERT/UPDATE/DELETE)")
        
        # 检查括号匹配
        if sql.count('(') != sql.count(')'):
            errors.append("括号不匹配")
        
        # 检查引号匹配
        single_quotes = sql.count("'")
        if single_quotes % 2 != 0:
            errors.append("单引号不匹配")
        
        # 检查SELECT语句结构
        if 'SELECT' in sql_upper:
            # 检查是否有FROM子句
            if 'FROM' not in sql_upper:
                errors.append("SELECT语句缺少FROM子句")
            
            # 检查GROUP BY和聚合函数的一致性
            if 'GROUP BY' in sql_upper:
                select_part = sql_upper.split('FROM')[0]
                if not any(func in select_part for func in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(']):
                    errors.append("使用GROUP BY但没有聚合函数")
        
        return errors
    
    def _check_tables(self, sql: str, db_info: Dict) -> List[str]:
        """检查表名是否存在"""
        errors = []
        available_tables = set(db_info.get('tables', {}).keys())
        
        # 提取SQL中的表名
        # 简化的表名提取（实际应该用SQL解析器）
        sql_normalized = ' '.join(sql.split())
        table_pattern = r'(?:FROM|JOIN)\s+(\w+)'
        matches = re.findall(table_pattern, sql_normalized, re.IGNORECASE)
        
        for table in matches:
            if table.lower() not in [t.lower() for t in available_tables]:
                errors.append(f"表 '{table}' 不存在。可用的表: {', '.join(available_tables)}")
        
        return errors
    
    def _check_columns(self, sql: str, db_info: Dict) -> List[str]:
        """检查列名是否存在"""
        errors = []
        
        # 获取所有表的列名
        all_columns = {}
        for table_name, table_info in db_info.get('tables', {}).items():
            columns = [col['name'] for col in table_info['schema']['columns']]
            all_columns[table_name] = columns
        
        # 简化的列名验证（应该根据具体表来验证）
        # 这里只检查明显的错误
        column_pattern = r'(?:SELECT|WHERE|GROUP BY|ORDER BY)\s+([a-zA-Z_]\w*)'
        potential_columns = re.findall(column_pattern, sql, re.IGNORECASE)
        
        # 排除SQL关键字
        sql_keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'AS', 'AND', 'OR', 'NOT', 'NULL', 'LIKE', 'IN', 'BETWEEN', 'DESC', 'ASC'}
        potential_columns = [col for col in potential_columns if col.upper() not in sql_keywords]
        
        # 检查列是否存在于任何表中
        for col in potential_columns:
            found = False
            for table_cols in all_columns.values():
                if col in table_cols:
                    found = True
                    break
            if not found and not self._is_aggregate_or_alias(col, sql):
                errors.append(f"列 '{col}' 在任何表中都不存在")
        
        return errors
    
    def _is_aggregate_or_alias(self, col: str, sql: str) -> bool:
        """检查是否是聚合函数或别名"""
        # 检查是否是别名
        alias_pattern = rf'\w+\s+(?:as\s+)?{col}\b'
        if re.search(alias_pattern, sql, re.IGNORECASE):
            return True
        
        # 检查是否在聚合函数中
        agg_pattern = rf'(?:SUM|COUNT|AVG|MAX|MIN)\s*\([^)]*{col}[^)]*\)'
        if re.search(agg_pattern, sql, re.IGNORECASE):
            return True
        
        return False
    
    def _check_data_types(self, sql: str, db_info: Dict) -> List[str]:
        """检查数据类型使用是否正确"""
        warnings = []
        
        # 检查日期比较
        date_pattern = r"(\w+)\s*(?:=|>|<|>=|<=)\s*'([^']+)'"
        date_comparisons = re.findall(date_pattern, sql)
        
        for col, value in date_comparisons:
            # 检查是否是日期列
            for table_info in db_info.get('tables', {}).values():
                for column in table_info['schema']['columns']:
                    if column['name'] == col and column['type'] == 'TEXT':
                        # 验证日期格式
                        if not re.match(r'\d{4}-\d{2}-\d{2}', value):
                            warnings.append(f"列 '{col}' 是日期字段，但值 '{value}' 不符合YYYY-MM-DD格式")
        
        # 检查数值计算
        calc_pattern = r'(\w+)\s*[+\-*/]\s*(\w+)'
        calculations = re.findall(calc_pattern, sql)
        
        for col1, col2 in calculations:
            # 这里应该检查列的数据类型
            if col1.isalpha() and col2.isalpha():
                warnings.append(f"请确保 '{col1}' 和 '{col2}' 都是数值类型才能进行计算")
        
        return warnings
    
    def _check_aggregations(self, sql: str) -> List[str]:
        """检查聚合函数使用是否正确"""
        errors = []
        sql_upper = sql.upper()
        
        # 检查GROUP BY一致性
        if 'GROUP BY' in sql_upper:
            # 提取SELECT中的非聚合列
            select_part = sql_upper.split('FROM')[0].replace('SELECT', '')
            group_part = sql_upper.split('GROUP BY')[-1].split('ORDER BY')[0].split('HAVING')[0]
            
            # 简化检查：确保SELECT中的非聚合列都在GROUP BY中
            non_agg_pattern = r'\b(\w+)\b(?!\s*\()'
            select_cols = re.findall(non_agg_pattern, select_part)
            
            # 过滤掉聚合函数和关键字
            agg_funcs = ['SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'AS']
            select_cols = [col for col in select_cols if col not in agg_funcs]
            
            for col in select_cols:
                if col not in group_part:
                    errors.append(f"列 '{col}' 在SELECT中但不在GROUP BY中")
        
        # 检查聚合函数的正确使用
        agg_pattern = r'(SUM|AVG|MAX|MIN)\s*\(\s*(\w+)\s*\)'
        agg_uses = re.findall(agg_pattern, sql_upper)
        
        for func, col in agg_uses:
            if func in ['SUM', 'AVG'] and col in ['STATUS', 'GENDER', 'OS_TYPE']:
                errors.append(f"不能对非数值列 '{col}' 使用{func}函数")
        
        return errors
    
    def _check_joins(self, sql: str) -> List[str]:
        """检查JOIN条件"""
        warnings = []
        
        # 检查JOIN条件
        join_pattern = r'JOIN\s+(\w+)\s+(?:AS\s+\w+\s+)?ON\s+([^WHERE]+)'
        joins = re.findall(join_pattern, sql, re.IGNORECASE)
        
        for table, condition in joins:
            # 检查是否有合适的连接条件
            if '=' not in condition:
                warnings.append(f"JOIN {table} 缺少明确的连接条件")
            
            # 检查是否使用了索引字段（通常是ID或日期）
            if not any(key in condition.lower() for key in ['id', 'dt', 'date']):
                warnings.append(f"JOIN {table} 建议使用索引字段(如ID或日期)作为连接条件")
        
        # 检查笛卡尔积
        if 'JOIN' in sql.upper() and 'ON' not in sql.upper():
            warnings.append("检测到JOIN但没有ON条件，可能产生笛卡尔积")
        
        return warnings
    
    def _check_performance(self, sql: str) -> List[str]:
        """检查可能的性能问题"""
        warnings = []
        sql_upper = sql.upper()
        
        # 检查是否使用SELECT *
        if 'SELECT *' in sql_upper or 'SELECT\n*' in sql_upper:
            warnings.append("建议明确指定需要的列，而不是使用SELECT *")
        
        # 检查是否有LIMIT
        if 'LIMIT' not in sql_upper and 'GROUP BY' not in sql_upper:
            warnings.append("查询没有LIMIT限制，可能返回大量数据")
        
        # 检查LIKE的使用
        like_pattern = r"LIKE\s+'%[^']+%'"
        if re.search(like_pattern, sql_upper):
            warnings.append("使用了前后都有%的LIKE查询，可能导致全表扫描")
        
        # 检查OR条件
        or_count = sql_upper.count(' OR ')
        if or_count > 3:
            warnings.append(f"使用了{or_count}个OR条件，可能影响查询性能")
        
        # 检查子查询
        if '(SELECT' in sql_upper:
            warnings.append("使用了子查询，考虑是否可以用JOIN替代以提高性能")
        
        return warnings
    
    def _check_business_logic(self, sql: str) -> List[str]:
        """检查业务逻辑"""
        warnings = []
        
        # 检查留存率计算
        if 'is_returned_1_day' in sql and 'newuser' in sql:
            if '/ newuser' in sql and 'CASE WHEN' not in sql.upper():
                warnings.append("计算留存率时建议使用CASE WHEN处理除零情况")
        
        # 检查日期范围
        date_pattern = r"dt\s*>=?\s*'(\d{4}-\d{2}-\d{2})'"
        dates = re.findall(date_pattern, sql)
        if dates:
            # 这里可以检查日期是否合理
            for date in dates:
                if date > '2025-12-31' or date < '2020-01-01':
                    warnings.append(f"日期 '{date}' 可能不在合理范围内")
        
        # 检查数据质量规则
        for table_name, rules in self.data_quality_rules.items():
            if table_name in sql:
                for rule in rules:
                    # 简单提醒
                    warnings.append(f"注意数据质量: {rule['description']}")
        
        return warnings
    
    def suggest_improvements(self, sql: str, errors: List[str]) -> List[str]:
        """基于错误提供改进建议"""
        suggestions = []
        
        for error in errors:
            if "括号不匹配" in error:
                suggestions.append("检查所有的括号是否正确配对")
            elif "不存在" in error:
                suggestions.append("使用 SHOW TABLES 或查看数据库文档确认正确的表名和列名")
            elif "GROUP BY" in error:
                suggestions.append("使用聚合函数时，SELECT中的非聚合列都需要在GROUP BY中")
            elif "数据类型" in error:
                suggestions.append("确保对正确的数据类型使用相应的操作")
        
        return suggestions
    
    def test_sql_execution(self, sql: str, db_path: str) -> Tuple[bool, Optional[str]]:
        """实际测试SQL执行（仅用于SQLite）"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 使用EXPLAIN来测试SQL而不实际执行
            cursor.execute(f"EXPLAIN QUERY PLAN {sql}")
            cursor.fetchall()
            
            conn.close()
            return True, None
        except Exception as e:
            return False, str(e)