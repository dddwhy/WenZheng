import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.insert(0, str(project_root))

from src.db.db_manager import DBManager
from src.utils.config import config

# 配置根日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class DatabaseInitializer:
    """数据库初始化器，负责创建数据库表和初始化数据"""
    
    def __init__(self, db_manager: DBManager):
        """
        初始化数据库初始化器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
        logger.info("数据库初始化器已创建")
    
    def initialize_database(self) -> bool:
        """
        初始化数据库，创建必要的表
        
        Returns:
            bool: 是否成功初始化
        """
        try:
            logger.info("开始初始化数据库...")
            
            # 创建组织机构表
            if not self._create_organizations_table():
                logger.error("创建组织机构表失败")
                return False
            
            # 创建爬取任务表
            if not self._create_crawl_tasks_table():
                logger.error("创建爬取任务表失败")
                return False
            
            # 创建投诉表
            if not self._create_complaints_table():
                logger.error("创建投诉表失败")
                return False
            
            logger.info("数据库初始化完成")
            return True
            
        except Exception as e:
            logger.error(f"初始化数据库时出错: {str(e)}", exc_info=True)
            return False
    
    def _create_organizations_table(self) -> bool:
        """
        创建组织机构表
        
        Returns:
            bool: 是否成功创建
        """
        sql = """
        CREATE TABLE IF NOT EXISTS organizations (
            id SERIAL PRIMARY KEY,                   -- 自增主键
            org_id INTEGER UNIQUE NOT NULL,          -- 机构原始ID
            name VARCHAR(255) NOT NULL,              -- 机构名称
            parent_id INTEGER,                       -- 父级机构ID
            path TEXT,                               -- 机构路径，以字符串形式存储，如 "1.2.3"
            "type" VARCHAR(50),                      -- 机构类型，如PG, CITY, AREA等
            ext JSONB,                               -- 扩展信息，使用PostgreSQL的JSONB类型
            has_children BOOLEAN DEFAULT FALSE,      -- 是否有子机构
            level INTEGER,                           -- 层级（省=1，市=2，区县=3，等）
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        logger.info("创建组织机构表...")
        return self.db_manager.execute(sql)
    
    def _create_crawl_tasks_table(self) -> bool:
        """
        创建爬取任务表
        
        Returns:
            bool: 是否成功创建
        """
        sql = """
        CREATE TABLE IF NOT EXISTS crawl_tasks (
            id SERIAL PRIMARY KEY,
            task_id VARCHAR(50) UNIQUE NOT NULL,
            task_type VARCHAR(50) NOT NULL,
            target_id VARCHAR(50),
            status VARCHAR(20) NOT NULL,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            params JSONB,
            result_summary JSONB,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        logger.info("创建爬取任务表...")
        return self.db_manager.execute(sql)
    
    def _create_complaints_table(self) -> bool:
        """
        创建投诉表
        
        Returns:
            bool: 是否成功创建
        """
        sql = """
        CREATE TABLE IF NOT EXISTS complaints (
            thread_id VARCHAR(100) PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            content TEXT,
            assign_organization_id INTEGER,
            chosen_organization_id INTEGER,
            organization_name VARCHAR(255),
            handle_status VARCHAR(50),
            handle_status_real VARCHAR(50),
            reply_status VARCHAR(50),
            created_at TIMESTAMP,
            assign_at TIMESTAMP,
            handle_at TIMESTAMP,
            reply_at TIMESTAMP,
            done_at TIMESTAMP,
            deadline TIMESTAMP,
            updated_at TIMESTAMP,
            delete_at TIMESTAMP,
            expire_flag BOOLEAN DEFAULT FALSE,
            warn_flag BOOLEAN DEFAULT FALSE,
            apply_postpone_flag BOOLEAN DEFAULT FALSE,
            apply_satisfaction_flag BOOLEAN DEFAULT FALSE,
            apply_transfer_flag BOOLEAN DEFAULT FALSE,
            can_feedback_flag BOOLEAN DEFAULT FALSE,
            has_video INTEGER DEFAULT 0,
            satisfaction INTEGER DEFAULT 0,
            info_hidden INTEGER DEFAULT 0,
            source VARCHAR(50),
            ip VARCHAR(50),
            username VARCHAR(100),
            passport_id VARCHAR(100),
            wechat_uid VARCHAR(100),
            area_id INTEGER,
            field_id INTEGER,
            field_name VARCHAR(100),
            sort_id INTEGER,
            sort_name VARCHAR(100),
            visible_status VARCHAR(50),
            updator VARCHAR(100),
            link VARCHAR(255),
            category VARCHAR(50) DEFAULT '其他',
            attaches JSONB DEFAULT '[]'::jsonb,
            ext JSONB DEFAULT '{}'::jsonb,
            search_vector tsvector
        );
        
        -- 创建索引加速查询
        CREATE INDEX IF NOT EXISTS idx_complaints_thread_id ON complaints(thread_id);
        CREATE INDEX IF NOT EXISTS idx_complaints_assign_organization_id ON complaints(assign_organization_id);
        CREATE INDEX IF NOT EXISTS idx_complaints_created_at ON complaints(created_at);
        
        -- 创建全文搜索索引
        CREATE INDEX IF NOT EXISTS idx_complaints_search_vector ON complaints USING gin(search_vector);
        
        -- 创建触发器更新search_vector
        CREATE OR REPLACE FUNCTION complaints_search_vector_update() RETURNS trigger AS $$
        BEGIN
            NEW.search_vector = 
                setweight(to_tsvector('simple', COALESCE(NEW.title, '')), 'A') ||
                setweight(to_tsvector('simple', COALESCE(NEW.content, '')), 'B');
            RETURN NEW;
        END
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS complaints_search_vector_trigger ON complaints;
        CREATE TRIGGER complaints_search_vector_trigger
            BEFORE INSERT OR UPDATE OF title, content
            ON complaints
            FOR EACH ROW
            EXECUTE FUNCTION complaints_search_vector_update();
        """;
        
        logger.info("创建投诉表...")
        return self.db_manager.execute(sql)
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        检查表是否存在
        
        Args:
            table_name: 表名
            
        Returns:
            bool: 表是否存在
        """
        try:
            sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
            """
            
            result = self.db_manager.query_one(sql, (table_name,))
            return result[0] if result else False
            
        except Exception as e:
            logger.error(f"检查表 {table_name} 是否存在时出错: {e}")
            return False


def main():
    """主函数"""
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='初始化数据库')
    parser.add_argument('--force', action='store_true', help='强制重新初始化数据库')
    args = parser.parse_args()
    
    logger.info("开始数据库初始化过程")
    db_manager = None
    
    try:
        # 加载配置
        logger.info("加载配置...")
        db_config = config.get('database', 'postgres')
        
        if not db_config:
            logger.error("无法加载数据库配置")
            sys.exit(1)
        
        # 初始化数据库连接
        logger.info("初始化数据库连接...")
        db_manager = DBManager(db_config=db_config)
        
        # 确保连接已建立
        if not db_manager.connect():
            logger.error("无法连接到数据库")
            sys.exit(1)
        
        # 初始化数据库
        initializer = DatabaseInitializer(db_manager)
        
        if args.force:
            # 强制重新初始化
            logger.warning("强制重新初始化数据库...")
            # 删除现有表
            tables = ["organizations", "crawl_tasks", "complaints"]
            
            for table in tables:
                if initializer.check_table_exists(table):
                    logger.info(f"删除表 {table}...")
                    db_manager.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        
        # 初始化数据库
        if initializer.initialize_database():
            logger.info("数据库初始化成功")
        else:
            logger.error("数据库初始化失败")
    
    except Exception as e:
        logger.error(f"数据库初始化过程中出错: {str(e)}", exc_info=True)
        sys.exit(1)
    
    finally:
        # 关闭数据库连接
        logger.info("关闭数据库连接...")
        if db_manager:
            db_manager.close()


if __name__ == "__main__":
    main() 