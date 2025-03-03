"""
创建数据库和用户的脚本
"""
import os
import sys
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.utils.config import config
from src.utils.logger import setup_logger

# 设置日志
logger = setup_logger('create_db')

# 管理员数据库连接配置 (需要 postgres 超级用户权限)
admin_db_config = {
    'user': 'postgres',         # PostgreSQL 超级用户名 (默认 postgres)
    'host': 'localhost',
    'database': 'postgres',     # 连接到默认的 postgres 数据库
    'password': '1234',         # 请替换为您的 postgres 超级用户密码
    'port': 5432,
}

def create_database():
    """
    创建数据库和用户
    
    Returns:
        bool: 是否成功创建
    """
    # 从配置中获取应用数据库信息
    app_db_config = config.get('database', 'postgres')
    db_name = app_db_config.get('database')
    user_name = app_db_config.get('user')
    user_password = app_db_config.get('password')
    
    admin_conn = None
    
    try:
        # 连接到默认postgres数据库
        logger.info("正在连接到postgres数据库...")
        admin_conn = psycopg2.connect(**admin_db_config)
        admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        admin_cursor = admin_conn.cursor()
        logger.info("成功连接到postgres数据库")
        
        # 1. 创建数据库（如果不存在）
        admin_cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        db_exists = admin_cursor.fetchone()
        
        if not db_exists:
            logger.info(f"数据库 '{db_name}' 不存在，开始创建...")
            admin_cursor.execute(f"CREATE DATABASE {db_name}")
            logger.info(f"数据库 '{db_name}' 创建成功")
        else:
            logger.info(f"数据库 '{db_name}' 已存在，跳过创建")
        
        # 2. 创建用户（如果不存在）
        admin_cursor.execute(f"SELECT 1 FROM pg_roles WHERE rolname = '{user_name}'")
        user_exists = admin_cursor.fetchone()
        
        if not user_exists:
            logger.info(f"用户 '{user_name}' 不存在，开始创建...")
            admin_cursor.execute(f"CREATE USER {user_name} WITH PASSWORD '{user_password}'")
            logger.info(f"用户 '{user_name}' 创建成功")
        else:
            logger.info(f"用户 '{user_name}' 已存在，跳过创建")
        
        # 3. 授权
        grant_admin_conn = psycopg2.connect(
            user=admin_db_config['user'],
            host=admin_db_config['host'],
            password=admin_db_config['password'],
            port=admin_db_config['port'],
            database=db_name
        )
        grant_admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        grant_cursor = grant_admin_conn.cursor()
        
        # 授权用户访问数据库的public模式
        grant_cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {user_name}")
        logger.info(f"已授权用户 '{user_name}' 访问数据库 '{db_name}'")
        
        # 授权用户在public模式下创建对象的权限
        grant_cursor.execute(f"GRANT ALL PRIVILEGES ON SCHEMA public TO {user_name}")
        logger.info(f"已授权用户 '{user_name}' 在数据库 '{db_name}' 的 public 模式下创建对象")
        
        grant_admin_conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"创建数据库或用户失败: {e}")
        return False
    finally:
        if admin_conn:
            admin_conn.close()
            logger.info("管理员数据库连接已关闭")

if __name__ == "__main__":
    # 创建日志目录
    Path("logs").mkdir(exist_ok=True)
    
    # 创建数据库和用户
    if create_database():
        print("数据库和用户创建成功")
    else:
        print("数据库和用户创建失败") 