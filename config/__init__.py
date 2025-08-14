"""
统一配置加载器
提供所有配置的统一访问入口
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from .presto_config import PRESTO_CONFIG

class ConfigLoader:
    """统一配置加载和管理类"""
    
    def __init__(self):
        self.config_dir = Path(__file__).parent
        self.project_root = self.config_dir.parent
        self._config_cache = {}
        
    def load_yaml_config(self, config_name: str = "unified_config.yaml") -> Dict[str, Any]:
        """加载YAML配置文件"""
        if config_name in self._config_cache:
            return self._config_cache[config_name]
            
        config_path = self.config_dir / config_name
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        self._config_cache[config_name] = config
        return config
    
    def get_presto_config(self) -> Dict[str, Any]:
        """获取Presto数据库配置"""
        return PRESTO_CONFIG
    
    def get_database_config(self, db_type: str = "sqlite") -> Dict[str, Any]:
        """获取数据库配置"""
        if db_type == "sqlite":
            return {
                "path": str(self.project_root / "data" / "data.db"),
                "check_same_thread": False
            }
        elif db_type == "presto":
            return self.get_presto_config()
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")
    
    def get_env_var(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """获取环境变量"""
        return os.getenv(key, default)
    
    def get_api_keys(self) -> Dict[str, Optional[str]]:
        """获取API密钥配置"""
        return {
            "openai": self.get_env_var("OPENAI_API_KEY"),
            "anthropic": self.get_env_var("ANTHROPIC_API_KEY")
        }
    
    def get_report_config(self) -> Dict[str, Any]:
        """获取报告生成配置"""
        config = self.load_yaml_config()
        return config.get("report", {})
    
    def get_data_sync_config(self) -> Dict[str, Any]:
        """获取数据同步配置"""
        config = self.load_yaml_config()
        return config.get("data_sync", {})

# 创建全局配置实例
config_loader = ConfigLoader()

# 导出常用配置
PRESTO_CONFIG = config_loader.get_presto_config()

def load_config(config_type: str = "unified") -> Dict[str, Any]:
    """
    加载指定类型的配置
    
    Args:
        config_type: 配置类型 (unified, presto, database, api_keys, report, data_sync)
    
    Returns:
        配置字典
    """
    if config_type == "unified":
        return config_loader.load_yaml_config()
    elif config_type == "presto":
        return config_loader.get_presto_config()
    elif config_type == "database":
        return config_loader.get_database_config()
    elif config_type == "api_keys":
        return config_loader.get_api_keys()
    elif config_type == "report":
        return config_loader.get_report_config()
    elif config_type == "data_sync":
        return config_loader.get_data_sync_config()
    else:
        raise ValueError(f"未知的配置类型: {config_type}")

__all__ = [
    "ConfigLoader",
    "config_loader",
    "PRESTO_CONFIG",
    "load_config"
]