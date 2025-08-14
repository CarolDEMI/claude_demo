"""
使用AI自动生成SQL查询的模块
"""
import os
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
import json
import logging
from business_config import BUSINESS_CONFIG, TERM_MAPPING
from query_cache import QueryCache
from sql_validator import SQLValidator
from query_templates import QueryTemplates
from error_handler import SQLErrorHandler, QueryOptimizer

load_dotenv()
logging.basicConfig(level=logging.INFO)

class SQLGenerator:
    def __init__(self):
        self.ai_provider = os.getenv('AI_PROVIDER', 'openai').lower()
        self._setup_ai_client()
        
        # 初始化改进组件
        self.business_config = BUSINESS_CONFIG
        self.term_mapping = TERM_MAPPING
        self.query_cache = QueryCache()
        self.validator = SQLValidator()
        self.templates = QueryTemplates()
        self.error_handler = SQLErrorHandler()
        self.optimizer = QueryOptimizer()
        self.logger = logging.getLogger(__name__)
    
    def _setup_ai_client(self):
        """设置AI客户端"""
        # 检查是否有API密钥
        openai_key = os.getenv('OPENAI_API_KEY')
        anthropic_key = os.getenv('ANTHROPIC_API_KEY')
        
        if not openai_key and not anthropic_key:
            print("警告: 未配置AI API密钥，将使用模拟模式")
            self.ai_provider = 'mock'
            self.client = None
            self.model = "mock"
            return
        
        if self.ai_provider == 'openai':
            try:
                import openai
                if not openai_key:
                    raise ValueError("OPENAI_API_KEY未配置")
                self.client = openai.OpenAI(api_key=openai_key)
                self.model = "gpt-4"
            except ImportError:
                raise ImportError("请安装OpenAI库: pip install openai")
        
        elif self.ai_provider == 'anthropic':
            try:
                import anthropic
                if not anthropic_key:
                    raise ValueError("ANTHROPIC_API_KEY未配置")
                self.client = anthropic.Anthropic(api_key=anthropic_key)
                self.model = "claude-3-5-sonnet-20241022"
            except ImportError:
                raise ImportError("请安装Anthropic库: pip install anthropic")
        
        else:
            raise ValueError(f"不支持的AI提供商: {self.ai_provider}")
    
    def generate_sql(self, requirement: str, db_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据需求和数据库信息生成SQL查询
        
        Args:
            requirement: 用户的查询需求描述
            db_info: 数据库结构信息
        
        Returns:
            包含生成的SQL、解释和元数据的字典
        """
        try:
            # 1. 检查缓存
            cached_result = self.query_cache.get_cached_query(requirement)
            if cached_result:
                self.logger.info(f"Using cached query for: {requirement}")
                return cached_result['sql_info']
            
            # 2. 尝试匹配模板
            template_result = self._try_template_match(requirement, db_info)
            if template_result:
                self.logger.info(f"Using template for: {requirement}")
                return template_result
            
            # 3. 查找相似查询
            similar_queries = self.query_cache.find_similar_queries(requirement)
            if similar_queries:
                self.logger.info(f"Found {len(similar_queries)} similar queries")
            
            # 4. 构建增强的提示词
            prompt = self._build_enhanced_prompt(requirement, db_info, similar_queries)
            
            # 5. 生成SQL
            if self.ai_provider == 'mock':
                response = self._call_mock(requirement, db_info)
            elif self.ai_provider == 'openai':
                response = self.error_handler.retry_with_fixes(
                    self._call_openai, prompt
                )
            elif self.ai_provider == 'anthropic':
                response = self.error_handler.retry_with_fixes(
                    self._call_anthropic, prompt
                )
            
            # 6. 解析响应
            sql_info = self._parse_response(response)
            
            # 7. 验证SQL
            is_valid, issues = self.validator.validate_sql(sql_info['sql'], db_info)
            if not is_valid:
                self.logger.warning(f"SQL validation failed: {issues}")
                sql_info['validation_warnings'] = issues
            
            # 8. 优化SQL
            optimized_sql = self.optimizer.optimize_query(sql_info['sql'])
            if optimized_sql != sql_info['sql']:
                sql_info['original_sql'] = sql_info['sql']
                sql_info['sql'] = optimized_sql
                self.logger.info("SQL query optimized")
            
            # 9. 保存到缓存
            self.query_cache.save_query(requirement, sql_info, success=is_valid)
            
            return sql_info
        
        except Exception as e:
            error_info = self.error_handler.handle_error(e, {
                'requirement': requirement,
                'ai_provider': self.ai_provider
            })
            raise Exception(f"SQL生成失败: {str(e)}\n建议: {', '.join(error_info['suggestions'])}")
    
    def _try_template_match(self, requirement: str, db_info: Dict[str, Any]) -> Optional[Dict]:
        """尝试使用模板匹配"""
        # 尝试精确模板匹配
        template = self.templates.match_template(requirement)
        if template:
            params = self.templates.parse_requirement(requirement)
            sql = self.templates.fill_template(template, params)
            
            return {
                'sql': sql,
                'explanation': f"使用模板: {template['name']}\n{template['description']}",
                'tables_used': self._extract_tables_from_sql(sql),
                'expected_columns': self._extract_columns_from_sql(sql),
                'performance_notes': '基于预定义模板生成，性能已优化',
                'template_used': template['name']
            }
        return None
    
    def _build_enhanced_prompt(self, requirement: str, db_info: Dict[str, Any], 
                              similar_queries: List[Dict] = None) -> str:
        """构建增强的提示词"""
        # 使用原有的提示词构建
        base_prompt = self._build_prompt(requirement, db_info)
        
        # 添加相似查询参考
        if similar_queries:
            similar_examples = "\n\n相似查询参考:"
            for sq in similar_queries[:2]:  # 只取前2个
                similar_examples += f"\n需求: {sq['cached']['requirement']}\n"
                similar_examples += f"SQL: {sq['cached']['sql_info']['sql']}\n"
            base_prompt += similar_examples
        
        # 添加术语映射提示
        term_hints = "\n\n术语映射:"
        for chinese, english in self.term_mapping.items():
            if chinese in requirement:
                if isinstance(english, list):
                    term_hints += f"\n'{chinese}' 对应字段: {', '.join(english)}"
                else:
                    term_hints += f"\n'{chinese}' 对应: {english}"
        
        if term_hints != "\n\n术语映射:":
            base_prompt += term_hints
        
        return base_prompt
    
    def _build_prompt(self, requirement: str, db_info: Dict[str, Any]) -> str:
        """构建AI提示词"""
        db_schema = self._format_database_schema(db_info)
        
        # 添加业务含义说明
        business_context = """
表说明：
1. cpz_qs_newuser_channel_i_d: 新用户渠道数据表
   - dt: 日期
   - ad_channel: 广告渠道（如Douyin, AppStore, Xiaohongshu等）
   - agent: 代理商
   - newuser: 新用户数量
   - is_returned_1_day: 次日留存用户数
   - zizhu_revenue_1: 首日收入
   - gender: 性别
   - age_group: 年龄段
   - dengji: 城市等级（超一线、一线、二线、三线及以下）
   
2. dwd_ttx_market_cash_cost_i_d: 市场推广成本表
   - dt: 日期
   - channel: 渠道
   - cash_cost: 现金成本
   - ad_plan_id_str: 广告计划ID
"""
        
        prompt = f"""
你是一个数据分析SQL专家。请根据用户需求和数据库结构生成准确的SQL查询。

{business_context}

数据库详细结构:
{db_schema}

用户需求: {requirement}

重要规则:
1. 仔细理解用户的业务需求，选择正确的表和字段
2. 日期字段dt是TEXT类型，格式为'YYYY-MM-DD'
3. 数值计算时注意NULL值处理，使用COALESCE或IFNULL
4. 聚合查询时注意GROUP BY的字段选择
5. 计算比率时注意除零错误，使用CASE WHEN避免
6. 表名和字段名区分大小写

请生成符合以下要求的SQL查询:
1. 查询语法必须正确且符合SQLite语法
2. 使用合适的表连接和过滤条件
3. 考虑性能优化（如使用适当的索引字段）
4. 返回有意义的列名（使用AS别名）

常见查询模式参考:
- 留存率计算: is_returned_1_day / newuser
- ROI计算: (收入 - 成本) / 成本
- 时间序列分析: 使用GROUP BY dt ORDER BY dt
- 渠道对比: GROUP BY ad_channel

请以JSON格式返回结果，包含以下字段:
{{
    "sql": "生成的SQL查询语句",
    "explanation": "查询逻辑的详细解释",
    "tables_used": ["使用的表名列表"],
    "expected_columns": ["预期返回的列名"],
    "performance_notes": "性能相关的注意事项"
}}
"""
        return prompt
    
    def _format_database_schema(self, db_info: Dict[str, Any]) -> str:
        """格式化数据库结构信息"""
        schema_text = f"数据库类型: {db_info.get('database_type', 'unknown')}\n\n"
        
        for table_name, table_info in db_info.get('tables', {}).items():
            schema_text += f"表名: {table_name}\n"
            schema_text += f"总行数: {table_info.get('row_count', 'unknown')}\n"
            
            # 添加列信息
            schema_text += "列结构:\n"
            for column in table_info['schema']['columns']:
                col_desc = f"  - {column['name']} ({column['type']})"
                if not column['nullable']:
                    col_desc += " NOT NULL"
                if column['default'] is not None:
                    col_desc += f" DEFAULT {column['default']}"
                schema_text += col_desc + "\n"
            
            # 添加样本数据统计
            if table_info['sample_data']:
                schema_text += "\n数据特征:\n"
                sample_df = self._analyze_sample_data(table_info['sample_data'])
                schema_text += sample_df
            
            schema_text += "\n"
        
        return schema_text
    
    def _analyze_sample_data(self, sample_data: List[Dict]) -> str:
        """分析样本数据的特征"""
        if not sample_data:
            return "  无样本数据\n"
        
        analysis = ""
        
        # 获取所有列
        columns = list(sample_data[0].keys())
        
        for col in columns:
            values = [row.get(col) for row in sample_data if row.get(col) is not None]
            if not values:
                continue
                
            # 判断数据类型
            if all(isinstance(v, (int, float)) for v in values):
                # 数值类型
                min_val = min(values)
                max_val = max(values)
                analysis += f"  - {col}: 数值范围 [{min_val}, {max_val}]\n"
            else:
                # 文本类型
                unique_values = list(set(str(v) for v in values))[:5]  # 最多显示5个唯一值
                analysis += f"  - {col}: 示例值 {unique_values}\n"
        
        return analysis
    
    def _call_openai(self, prompt: str) -> str:
        """调用OpenAI API"""
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "你是一个专业的SQL数据库专家，擅长根据需求生成优化的SQL查询。"},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=1500
        )
        return response.choices[0].message.content
    
    def _call_anthropic(self, prompt: str) -> str:
        """调用Anthropic API"""
        response = self.client.messages.create(
            model=self.model,
            max_tokens=1500,
            temperature=0.1,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.content[0].text
    
    def _parse_response(self, response: str) -> Dict[str, Any]:
        """解析AI响应"""
        try:
            # 尝试提取JSON部分
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            
            if start_idx == -1 or end_idx == 0:
                raise ValueError("响应中未找到有效的JSON格式")
            
            json_str = response[start_idx:end_idx]
            parsed = json.loads(json_str)
            
            # 验证必需字段
            required_fields = ['sql', 'explanation', 'tables_used', 'expected_columns']
            for field in required_fields:
                if field not in parsed:
                    raise ValueError(f"缺少必需字段: {field}")
            
            return parsed
        
        except json.JSONDecodeError as e:
            raise ValueError(f"无法解析AI响应的JSON格式: {str(e)}")
        except Exception as e:
            raise ValueError(f"响应解析失败: {str(e)}")
    
    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """从SQL中提取表名"""
        import re
        tables = []
        # 简化的表提取
        from_pattern = r'FROM\s+(\w+)'
        join_pattern = r'JOIN\s+(\w+)'
        
        tables.extend(re.findall(from_pattern, sql, re.IGNORECASE))
        tables.extend(re.findall(join_pattern, sql, re.IGNORECASE))
        
        return list(set(tables))
    
    def _extract_columns_from_sql(self, sql: str) -> List[str]:
        """从SQL中提取列名"""
        import re
        # 提取SELECT和列名之间的内容
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        match = re.search(select_pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if match:
            select_clause = match.group(1)
            # 简单分割（实际应该用SQL解析器）
            columns = [col.strip() for col in select_clause.split(',')]
            # 提取AS别名
            result_columns = []
            for col in columns:
                if ' as ' in col.lower():
                    result_columns.append(col.split(' as ')[-1].strip())
                elif ' ' in col and 'case' not in col.lower():
                    result_columns.append(col.split()[-1].strip())
                else:
                    result_columns.append(col.strip())
            return result_columns
        return ['*']
    
    def suggest_optimizations(self, sql: str, db_info: Dict[str, Any]) -> List[str]:
        """为SQL查询提供优化建议"""
        prompt = f"""
请分析以下SQL查询并提供性能优化建议:

SQL查询:
{sql}

数据库结构:
{self._format_database_schema(db_info)}

请提供具体的优化建议，包括:
1. 索引建议
2. 查询重写建议
3. 性能风险提示

以JSON格式返回建议列表:
{{"suggestions": ["建议1", "建议2", "建议3"]}}
"""
        
        try:
            if self.ai_provider == 'openai':
                response = self._call_openai(prompt)
            elif self.ai_provider == 'anthropic':
                response = self._call_anthropic(prompt)
            
            parsed = self._parse_response(response)
            return parsed.get('suggestions', [])
        
        except Exception as e:
            return [f"优化建议生成失败: {str(e)}"]
    
    def add_query_feedback(self, requirement: str, sql: str, feedback: str, rating: int):
        """添加查询反馈用于学习"""
        self.query_cache.add_feedback(requirement, sql, feedback, rating)
        self.logger.info(f"Feedback added: rating={rating}")
    
    def get_performance_stats(self) -> Dict:
        """获取性能统计"""
        cache_stats = self.query_cache.get_cache_stats()
        error_stats = self.error_handler.get_error_stats()
        
        return {
            'cache_stats': cache_stats,
            'error_stats': error_stats,
            'suggestions': self.query_cache.get_learning_suggestions()
        }
    
    def _call_mock(self, requirement: str, db_info: Dict[str, Any]) -> Dict[str, Any]:
        """模拟AI调用，用于演示"""
        tables = list(db_info.get('tables', {}).keys())
        
        # 简单的关键词匹配生成SQL
        requirement_lower = requirement.lower()
        
        if '用户' in requirement and 'users' in tables:
            if '年龄' in requirement or 'age' in requirement_lower:
                sql = "SELECT age, COUNT(*) as count FROM users GROUP BY age ORDER BY age"
                explanation = "按年龄分组统计用户数量"
                tables_used = ['users']
                expected_columns = ['age', 'count']
            else:
                sql = "SELECT * FROM users LIMIT 10"
                explanation = "查询用户表的前10条记录"
                tables_used = ['users']
                expected_columns = ['user_id', 'username', 'email', 'age', 'gender', 'city']
        
        elif '产品' in requirement and 'products' in tables:
            if '类别' in requirement or 'category' in requirement_lower:
                sql = "SELECT category, COUNT(*) as product_count FROM products GROUP BY category ORDER BY product_count DESC"
                explanation = "按产品类别统计数量"
                tables_used = ['products']
                expected_columns = ['category', 'product_count']
            else:
                sql = "SELECT * FROM products LIMIT 10"
                explanation = "查询产品表的前10条记录"
                tables_used = ['products']
                expected_columns = ['product_id', 'product_name', 'category', 'price']
        
        elif '订单' in requirement and 'orders' in tables:
            if '统计' in requirement or 'count' in requirement_lower:
                sql = "SELECT status, COUNT(*) as order_count FROM orders GROUP BY status"
                explanation = "按状态统计订单数量"
                tables_used = ['orders']
                expected_columns = ['status', 'order_count']
            else:
                sql = "SELECT * FROM orders LIMIT 10"
                explanation = "查询订单表的前10条记录"
                tables_used = ['orders']
                expected_columns = ['order_id', 'user_id', 'order_date', 'status', 'total_amount']
        
        elif '销售' in requirement and 'sales_stats' in tables:
            sql = "SELECT date, daily_revenue, orders_count FROM sales_stats ORDER BY date DESC LIMIT 30"
            explanation = "查询最近30天的销售统计数据"
            tables_used = ['sales_stats']
            expected_columns = ['date', 'daily_revenue', 'orders_count']
        
        else:
            # 默认查询第一个表
            if tables:
                first_table = tables[0]
                sql = f"SELECT * FROM {first_table} LIMIT 10"
                explanation = f"查询{first_table}表的前10条记录"
                tables_used = [first_table]
                expected_columns = ['*']
            else:
                sql = "SELECT 1 as demo"
                explanation = "演示查询"
                tables_used = []
                expected_columns = ['demo']
        
        return {
            'sql': sql,
            'explanation': explanation,
            'tables_used': tables_used,
            'expected_columns': expected_columns,
            'performance_notes': '这是模拟生成的SQL查询，实际使用请配置AI API密钥'
        }