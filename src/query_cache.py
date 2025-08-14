"""
查询缓存和学习机制
"""
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import hashlib
from pathlib import Path

class QueryCache:
    def __init__(self, cache_dir: str = "./query_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_file = self.cache_dir / "query_cache.json"
        self.feedback_file = self.cache_dir / "query_feedback.json"
        self.patterns_file = self.cache_dir / "query_patterns.json"
        self._load_cache()
    
    def _load_cache(self):
        """加载缓存数据"""
        # 加载查询缓存
        if self.cache_file.exists():
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                self.cache = json.load(f)
        else:
            self.cache = {}
        
        # 加载反馈数据
        if self.feedback_file.exists():
            with open(self.feedback_file, 'r', encoding='utf-8') as f:
                self.feedback = json.load(f)
        else:
            self.feedback = {}
        
        # 加载查询模式
        if self.patterns_file.exists():
            with open(self.patterns_file, 'r', encoding='utf-8') as f:
                self.patterns = json.load(f)
        else:
            self.patterns = self._init_patterns()
    
    def _init_patterns(self) -> Dict:
        """初始化常见查询模式"""
        return {
            "retention_analysis": {
                "keywords": ["留存", "retention", "次留", "七留"],
                "template": "SELECT dt, ad_channel, SUM(newuser) as new_users, SUM(is_returned_1_day) as retained_users, ROUND(SUM(is_returned_1_day) * 100.0 / SUM(newuser), 2) as retention_rate FROM cpz_qs_newuser_channel_i_d WHERE {conditions} GROUP BY dt, ad_channel ORDER BY dt DESC",
                "description": "留存率分析模板"
            },
            "channel_comparison": {
                "keywords": ["渠道对比", "channel", "各渠道", "不同渠道"],
                "template": "SELECT ad_channel, COUNT(DISTINCT dt) as days, SUM(newuser) as total_users, ROUND(AVG(is_returned_1_day * 100.0 / newuser), 2) as avg_retention_rate FROM cpz_qs_newuser_channel_i_d WHERE {conditions} GROUP BY ad_channel ORDER BY total_users DESC",
                "description": "渠道对比分析模板"
            },
            "roi_analysis": {
                "keywords": ["ROI", "投资回报", "收入成本", "回本"],
                "template": """
                SELECT 
                    a.dt,
                    a.ad_channel,
                    SUM(a.newuser) as new_users,
                    SUM(a.zizhu_revenue_1_aftertax) as revenue,
                    SUM(b.cash_cost) as cost,
                    ROUND(SUM(a.zizhu_revenue_1_aftertax) / SUM(b.cash_cost), 2) as roi
                FROM cpz_qs_newuser_channel_i_d a
                LEFT JOIN dwd_ttx_market_cash_cost_i_d b
                    ON a.dt = b.dt AND a.ad_channel = b.channel
                WHERE {conditions}
                GROUP BY a.dt, a.ad_channel
                ORDER BY a.dt DESC
                """,
                "description": "ROI分析模板"
            },
            "user_quality": {
                "keywords": ["用户质量", "fake", "真实用户", "用户状态"],
                "template": "SELECT status, verification_status, COUNT(*) as user_count, SUM(newuser) as total_users, ROUND(AVG(is_returned_1_day * 100.0 / newuser), 2) as avg_retention FROM cpz_qs_newuser_channel_i_d WHERE {conditions} GROUP BY status, verification_status",
                "description": "用户质量分析模板"
            },
            "demographic_analysis": {
                "keywords": ["人群", "画像", "性别", "年龄", "城市"],
                "template": "SELECT {demographic_field}, SUM(newuser) as users, ROUND(AVG(zizhu_revenue_1 / newuser), 2) as arpu FROM cpz_qs_newuser_channel_i_d WHERE {conditions} GROUP BY {demographic_field} ORDER BY users DESC",
                "description": "人群分析模板"
            }
        }
    
    def _get_cache_key(self, requirement: str) -> str:
        """生成缓存键"""
        return hashlib.md5(requirement.lower().strip().encode()).hexdigest()
    
    def get_cached_query(self, requirement: str) -> Optional[Dict]:
        """获取缓存的查询"""
        cache_key = self._get_cache_key(requirement)
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            # 检查缓存是否过期（30天）
            cache_time = datetime.fromisoformat(cached['timestamp'])
            if (datetime.now() - cache_time).days <= 30:
                cached['hits'] = cached.get('hits', 0) + 1
                self._save_cache()
                return cached
        return None
    
    def save_query(self, requirement: str, sql_info: Dict, success: bool = True):
        """保存查询到缓存"""
        cache_key = self._get_cache_key(requirement)
        self.cache[cache_key] = {
            'requirement': requirement,
            'sql_info': sql_info,
            'timestamp': datetime.now().isoformat(),
            'success': success,
            'hits': 1
        }
        self._save_cache()
    
    def add_feedback(self, requirement: str, sql: str, feedback: str, rating: int):
        """添加查询反馈"""
        feedback_id = f"{self._get_cache_key(requirement)}_{datetime.now().timestamp()}"
        self.feedback[feedback_id] = {
            'requirement': requirement,
            'sql': sql,
            'feedback': feedback,
            'rating': rating,  # 1-5分
            'timestamp': datetime.now().isoformat()
        }
        self._save_feedback()
        
        # 如果评分高，提取为模式
        if rating >= 4:
            self._extract_pattern(requirement, sql)
    
    def _extract_pattern(self, requirement: str, sql: str):
        """从高评分查询中提取模式"""
        # 这里可以实现更复杂的模式提取逻辑
        pattern_id = f"custom_{len(self.patterns)}"
        self.patterns[pattern_id] = {
            'keywords': requirement.split()[:3],  # 简单提取前3个词
            'template': sql,
            'description': f"用户定义: {requirement[:50]}...",
            'user_defined': True,
            'rating_avg': 4.0
        }
        self._save_patterns()
    
    def find_similar_queries(self, requirement: str, threshold: float = 0.7) -> List[Dict]:
        """查找相似的历史查询"""
        similar = []
        requirement_words = set(requirement.lower().split())
        
        for cache_key, cached in self.cache.items():
            cached_words = set(cached['requirement'].lower().split())
            # 计算Jaccard相似度
            intersection = requirement_words.intersection(cached_words)
            union = requirement_words.union(cached_words)
            similarity = len(intersection) / len(union) if union else 0
            
            if similarity >= threshold:
                similar.append({
                    'similarity': similarity,
                    'cached': cached
                })
        
        return sorted(similar, key=lambda x: x['similarity'], reverse=True)[:5]
    
    def match_pattern(self, requirement: str) -> Optional[Tuple[str, Dict]]:
        """匹配查询模式"""
        requirement_lower = requirement.lower()
        
        for pattern_id, pattern in self.patterns.items():
            # 检查关键词匹配
            if any(keyword in requirement_lower for keyword in pattern['keywords']):
                return pattern_id, pattern
        
        return None
    
    def get_learning_suggestions(self) -> List[Dict]:
        """获取基于历史数据的学习建议"""
        suggestions = []
        
        # 分析高频查询
        freq_queries = {}
        for cached in self.cache.values():
            req_type = self._classify_query(cached['requirement'])
            freq_queries[req_type] = freq_queries.get(req_type, 0) + cached.get('hits', 1)
        
        # 分析低评分查询
        low_rating_patterns = []
        for feedback in self.feedback.values():
            if feedback['rating'] <= 2:
                low_rating_patterns.append(feedback['requirement'])
        
        # 生成建议
        if freq_queries:
            most_common = max(freq_queries.items(), key=lambda x: x[1])
            suggestions.append({
                'type': 'frequent_query',
                'message': f"最常见的查询类型是'{most_common[0]}'，出现了{most_common[1]}次"
            })
        
        if low_rating_patterns:
            suggestions.append({
                'type': 'improvement_needed',
                'message': f"有{len(low_rating_patterns)}个查询收到低评分，需要改进"
            })
        
        return suggestions
    
    def _classify_query(self, requirement: str) -> str:
        """简单分类查询类型"""
        requirement_lower = requirement.lower()
        if "留存" in requirement_lower or "retention" in requirement_lower:
            return "留存分析"
        elif "渠道" in requirement_lower or "channel" in requirement_lower:
            return "渠道分析"
        elif "roi" in requirement_lower or "收入" in requirement_lower:
            return "收益分析"
        elif "人群" in requirement_lower or "画像" in requirement_lower:
            return "人群分析"
        else:
            return "其他分析"
    
    def _save_cache(self):
        """保存缓存到文件"""
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)
    
    def _save_feedback(self):
        """保存反馈到文件"""
        with open(self.feedback_file, 'w', encoding='utf-8') as f:
            json.dump(self.feedback, f, ensure_ascii=False, indent=2)
    
    def _save_patterns(self):
        """保存模式到文件"""
        with open(self.patterns_file, 'w', encoding='utf-8') as f:
            json.dump(self.patterns, f, ensure_ascii=False, indent=2)
    
    def get_cache_stats(self) -> Dict:
        """获取缓存统计信息"""
        total_queries = len(self.cache)
        successful_queries = sum(1 for c in self.cache.values() if c.get('success', True))
        total_hits = sum(c.get('hits', 1) for c in self.cache.values())
        avg_rating = 0
        
        if self.feedback:
            ratings = [f['rating'] for f in self.feedback.values()]
            avg_rating = sum(ratings) / len(ratings)
        
        return {
            'total_queries': total_queries,
            'successful_queries': successful_queries,
            'success_rate': successful_queries / total_queries if total_queries > 0 else 0,
            'total_hits': total_hits,
            'cache_hit_rate': (total_hits - total_queries) / total_hits if total_hits > 0 else 0,
            'total_feedback': len(self.feedback),
            'average_rating': avg_rating,
            'total_patterns': len(self.patterns),
            'user_patterns': sum(1 for p in self.patterns.values() if p.get('user_defined', False))
        }