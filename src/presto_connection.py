#!/usr/bin/env python3
"""
Presto连接管理器
自动检测并解决代理问题
"""
import os
import sys
import logging
import time
from typing import Dict, Optional, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class PrestoConnectionManager:
    """Presto连接管理器，自动处理代理问题"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.proxy_env_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 
                              'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']
        self._original_proxy_settings = {}
        self._connection = None
        
    def _save_proxy_settings(self):
        """保存当前代理设置"""
        self._original_proxy_settings = {}
        for var in self.proxy_env_vars:
            if var in os.environ:
                self._original_proxy_settings[var] = os.environ[var]
    
    def _restore_proxy_settings(self):
        """恢复原始代理设置"""
        # 先清除所有代理设置
        for var in self.proxy_env_vars:
            if var in os.environ:
                del os.environ[var]
        
        # 恢复原始设置
        for var, value in self._original_proxy_settings.items():
            os.environ[var] = value
    
    def _disable_proxy(self):
        """禁用代理"""
        self._save_proxy_settings()
        for var in self.proxy_env_vars:
            if var in os.environ:
                del os.environ[var]
        logger.info("🔄 已临时禁用代理设置")
    
    def _test_connection_with_settings(self, use_proxy: bool = True) -> Optional[Any]:
        """测试连接（可选择是否使用代理）"""
        try:
            if not use_proxy:
                self._disable_proxy()
            
            import prestodb
            
            conn = prestodb.dbapi.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                catalog=self.config['catalog'],
                schema=self.config['schema']
            )
            
            # 测试查询
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] == 1:
                logger.info(f"✅ 连接测试成功 {'(无代理)' if not use_proxy else '(有代理)'}")
                return conn
            else:
                conn.close()
                return None
                
        except Exception as e:
            logger.debug(f"连接失败 {'(无代理)' if not use_proxy else '(有代理)'}: {str(e)}")
            return None
        finally:
            if not use_proxy:
                self._restore_proxy_settings()
    
    def connect(self) -> Optional[Any]:
        """
        智能连接Presto
        1. 先尝试使用当前代理设置连接
        2. 如果失败，尝试不使用代理连接
        3. 记住成功的连接方式
        """
        logger.info("🔍 正在连接到Presto数据库...")
        
        # 步骤1：尝试使用当前代理设置
        logger.debug("尝试使用当前代理设置连接...")
        conn = self._test_connection_with_settings(use_proxy=True)
        if conn:
            self._connection = conn
            return conn
        
        # 步骤2：尝试不使用代理
        logger.info("⚠️ 使用代理连接失败，尝试禁用代理...")
        conn = self._test_connection_with_settings(use_proxy=False)
        if conn:
            # 如果无代理连接成功，保持无代理状态
            self._disable_proxy()
            self._connection = conn
            logger.info("✅ 已切换到无代理模式")
            return conn
        
        # 步骤3：都失败了
        logger.error("❌ 无法连接到Presto数据库")
        logger.error("请检查:")
        logger.error("1. Presto服务是否运行")
        logger.error("2. 网络连接是否正常")
        logger.error("3. 连接配置是否正确")
        logger.error(f"   Host: {self.config['host']}")
        logger.error(f"   Port: {self.config['port']}")
        return None
    
    @contextmanager
    def get_connection(self):
        """
        获取连接的上下文管理器
        自动处理代理问题和连接关闭
        """
        conn = None
        try:
            if self._connection and self._is_connection_alive(self._connection):
                conn = self._connection
            else:
                conn = self.connect()
            
            if conn:
                yield conn
            else:
                raise Exception("无法建立Presto连接")
                
        except Exception as e:
            logger.error(f"连接错误: {str(e)}")
            raise
        finally:
            # 不关闭连接，保持复用
            pass
    
    def _is_connection_alive(self, conn) -> bool:
        """检查连接是否还活着"""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            return result and result[0] == 1
        except:
            return False
    
    def close(self):
        """关闭连接并恢复代理设置"""
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None
        
        # 恢复原始代理设置
        self._restore_proxy_settings()
        logger.info("🔄 已恢复原始代理设置")
    
    def __enter__(self):
        """上下文管理器入口"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()


def create_smart_connection(config: Dict[str, Any]) -> PrestoConnectionManager:
    """创建智能Presto连接"""
    return PrestoConnectionManager(config)