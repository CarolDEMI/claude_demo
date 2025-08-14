"""
报告模块基类
定义所有报告模块的统一接口
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
import sqlite3

@dataclass
class ModuleResult:
    """模块执行结果"""
    module_name: str
    success: bool
    data: Dict[str, Any]
    html_content: str
    warnings: List[str] = None
    errors: List[str] = None
    execution_time: float = 0.0
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.errors is None:
            self.errors = []

class BaseReportModule(ABC):
    """报告模块基类"""
    
    def __init__(self, db_path: str, config: Dict[str, Any]):
        self.db_path = db_path
        self.config = config
        self.module_name = self.__class__.__name__
        self.warnings = []
        self.errors = []
    
    @abstractmethod
    def collect_data(self, target_date: str, conn: sqlite3.Connection) -> Dict[str, Any]:
        """
        收集模块数据
        
        Args:
            target_date: 目标日期 (YYYY-MM-DD)
            conn: 数据库连接
            
        Returns:
            收集到的数据字典
        """
        pass
    
    @abstractmethod
    def analyze_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        分析数据
        
        Args:
            data: 收集到的原始数据
            
        Returns:
            分析结果字典
        """
        pass
    
    @abstractmethod
    def generate_html(self, analysis_result: Dict[str, Any]) -> str:
        """
        生成HTML内容
        
        Args:
            analysis_result: 分析结果
            
        Returns:
            HTML内容字符串
        """
        pass
    
    def execute(self, target_date: str) -> ModuleResult:
        """
        执行模块完整流程
        
        Args:
            target_date: 目标日期
            
        Returns:
            模块执行结果
        """
        start_time = datetime.now()
        
        try:
            # 连接数据库
            conn = sqlite3.connect(self.db_path)
            
            # 收集数据
            raw_data = self.collect_data(target_date, conn)
            
            # 分析数据
            analysis_result = self.analyze_data(raw_data)
            
            # 生成HTML
            html_content = self.generate_html(analysis_result)
            
            conn.close()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return ModuleResult(
                module_name=self.module_name,
                success=True,
                data=analysis_result,
                html_content=html_content,
                warnings=self.warnings.copy(),
                errors=self.errors.copy(),
                execution_time=execution_time
            )
            
        except Exception as e:
            self.errors.append(f"模块执行失败: {str(e)}")
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return ModuleResult(
                module_name=self.module_name,
                success=False,
                data={},
                html_content=f"<div class='error'>模块 {self.module_name} 执行失败: {str(e)}</div>",
                warnings=self.warnings.copy(),
                errors=self.errors.copy(),
                execution_time=execution_time
            )
    
    def add_warning(self, message: str):
        """添加警告信息"""
        self.warnings.append(message)
    
    def add_error(self, message: str):
        """添加错误信息"""
        self.errors.append(message)
    
    def format_number(self, value: float, format_type: str = 'number') -> str:
        """格式化数值"""
        if value is None:
            return "N/A"
        
        if format_type == 'currency':
            return f"¥{value:,.2f}"
        elif format_type == 'percentage':
            return f"{value:.1f}%"
        elif format_type == 'number':
            return f"{value:,.0f}"
        else:
            return str(value)
    
    def calculate_change_percentage(self, current: float, previous: float) -> float:
        """计算变化百分比"""
        if previous == 0:
            return 0.0
        return (current - previous) / previous * 100
    
    def get_change_indicator(self, change_pct: float) -> str:
        """获取变化指示器"""
        if change_pct > 0:
            return f"↗ +{change_pct:.1f}%"
        elif change_pct < 0:
            return f"↘ {change_pct:.1f}%"
        else:
            return "→ 0.0%"