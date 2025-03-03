"""
数据库初始化模块
"""
import logging
from src.utils.config import config
from src.db.db_manager import DBManager
from src.db.schema import DB_SCHEMA
from src.utils.logger import setup_logger

logger = setup_logger('init_db')

class DatabaseInitializer:
    """数据库初始化器"""
    
    def __init__(self, db_config=None):
        """
        初始化数据库初始化器
        
        Args:
            db_config: 数据库连接配置
        """
        self.db_manager = DBManager(db_config)
    
    def init_database(self):
        """
        初始化数据库
        
        Returns:
            bool: 是否成功初始化
        """
        try:
            if not self.db_manager.connect():
                return False
            
            logger.info("开始初始化数据库...")
            
            # 创建表和扩展（使用schema.py中的定义）
            self._create_schema()
            
            logger.info("数据库初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"初始化数据库时发生错误: {e}")
            return False
        finally:
            self.db_manager.close()
    
    def _create_schema(self):
        """创建数据库结构（扩展和表）"""
        try:
            # 使用schema.py中定义的SQL
            success = self.db_manager.execute(DB_SCHEMA)
            if success:
                logger.info("数据库结构创建成功")
            else:
                logger.warning("数据库结构创建失败")
        except Exception as e:
            logger.error(f"创建数据库结构时发生错误: {e}")
            raise
    
    def drop_tables(self, confirm=False):
        """
        删除所有表（危险操作，仅用于开发和测试）
        
        Args:
            confirm: 是否确认删除
            
        Returns:
            bool: 是否成功删除
        """
        if not confirm:
            logger.warning("未确认删除操作。如果确实要删除所有表，请将confirm参数设置为True")
            return False
        
        try:
            if not self.db_manager.connect():
                return False
            
            logger.warning("开始删除所有表...")
            
            # 删除表
            drop_tables_sql = """
            DROP TABLE IF EXISTS statistics CASCADE;
            DROP TABLE IF EXISTS crawl_tasks CASCADE;
            DROP TABLE IF EXISTS complaints CASCADE;
            DROP TABLE IF EXISTS threads CASCADE;
            DROP TABLE IF EXISTS organizations CASCADE;
            """
            
            success = self.db_manager.execute(drop_tables_sql)
            if success:
                logger.info("所有表删除成功")
            else:
                logger.warning("删除表失败")
            
            return success
            
        except Exception as e:
            logger.error(f"删除表时发生错误: {e}")
            return False
        finally:
            self.db_manager.close()
    
    def reset_database(self, confirm=False):
        """
        重置数据库（删除所有表并重新创建）
        
        Args:
            confirm: 是否确认重置
            
        Returns:
            bool: 是否成功重置
        """
        if not confirm:
            logger.warning("未确认重置操作。如果确实要重置数据库，请将confirm参数设置为True")
            return False
        
        # 先删除表
        if not self.drop_tables(confirm=True):
            logger.error("删除表失败，重置操作中止")
            return False
        
        # 再创建表
        return self.init_database()

def init_db():
    """初始化数据库的便捷函数"""
    initializer = DatabaseInitializer()
    return initializer.init_database()

def reset_db(confirm=False):
    """重置数据库的便捷函数"""
    initializer = DatabaseInitializer()
    return initializer.reset_database(confirm)

if __name__ == "__main__":
    import sys
    
    # 获取命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == '--reset':
        print("警告：即将重置数据库（删除所有表并重新创建）！")
        confirmation = input("请输入 'yes' 确认操作: ")
        if confirmation.lower() == 'yes':
            if reset_db(confirm=True):
                print("数据库重置成功")
            else:
                print("数据库重置失败")
        else:
            print("操作已取消")
    else:
        # 直接运行此脚本来初始化数据库
        if init_db():
            print("数据库初始化成功")
        else:
            print("数据库初始化失败") 