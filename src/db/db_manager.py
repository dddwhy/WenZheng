"""
数据库连接管理模块
"""
import logging
import psycopg2
import psycopg2.extras
from pathlib import Path
import sys
from typing import List, Optional, Dict

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import config

logger = logging.getLogger('db_manager')

class DBManager:
    """数据库管理器"""
    
    def __init__(self, db_config: Dict = None):
        """
        初始化数据库管理器
        
        Args:
            db_config: 数据库配置，如果为None则从配置文件加载
        """
        self.conn = None
        self.cursor = None
        
        if db_config is None:
            # 从配置文件加载数据库配置
            db_config = config.get('database', 'postgres')
            
        if not db_config:
            raise ValueError("无法加载数据库配置")
            
        self.config = db_config
    
    def connect(self) -> bool:
        """
        建立数据库连接
        
        Returns:
            连接成功返回True，失败返回False
        """
        try:
            if self.conn is None:
                # 创建一个连接参数的副本
                conn_params = self.config.copy()
                
                # 移除可能的无效参数
                if 'encoding' in conn_params:
                    del conn_params['encoding']
                
                # 建立连接
                self.conn = psycopg2.connect(**conn_params)
                self.conn.autocommit = False  # 显式控制事务
                
                # 创建游标
                self.cursor = self.conn.cursor()
                
                # 设置客户端编码
                self.conn.set_client_encoding('UTF8')
                
                logger.info("数据库连接成功建立")
                return True
                
        except psycopg2.Error as e:
            logger.error(f"数据库连接失败: {e.pgerror if hasattr(e, 'pgerror') else str(e)}")
            self.conn = None
            self.cursor = None
            return False
        except Exception as e:
            logger.error(f"建立数据库连接时发生未知错误: {str(e)}")
            self.conn = None
            self.cursor = None
            return False
    
    def close(self):
        """关闭数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            self.cursor = None
            self.conn = None
        except Exception as e:
            logger.error(f"关闭数据库连接时出错: {e}")
    
    def get_connection(self):
        """获取数据库连接"""
        return self.conn
    
    def get_cursor(self):
        """获取游标"""
        if not self.cursor:
            self.cursor = self.conn.cursor()
        return self.cursor
    
    def commit(self):
        """提交事务"""
        if self.conn:
            self.conn.commit()
    
    def execute(self, sql: str, params: tuple = None) -> bool:
        """
        执行SQL语句
        
        Args:
            sql: SQL语句
            params: SQL参数
            
        Returns:
            执行成功返回True，失败返回False
        """
        try:
            cursor = self.get_cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            self.commit()
            return True
        except Exception as e:
            logger.error(f"执行SQL失败: {e}")
            try:
                self.conn.rollback()
            except:
                pass
            return False
    
    def query(self, sql: str, params: tuple = None) -> List[tuple]:
        """
        执行查询
        
        Args:
            sql: SQL语句
            params: SQL参数
            
        Returns:
            查询结果列表
        """
        try:
            cursor = self.get_cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"执行查询失败: {e}")
            return []
    
    def query_one(self, sql: str, params: tuple = None) -> Optional[tuple]:
        """
        执行查询并返回第一条结果
        
        Args:
            sql: SQL语句
            params: SQL参数
            
        Returns:
            查询结果或None
        """
        try:
            cursor = self.get_cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchone()
        except Exception as e:
            logger.error(f"执行查询失败: {e}")
            return None
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close() 