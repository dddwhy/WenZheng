"""
测试投诉数据存储到数据库
"""
import os
import sys
import json
import argparse
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.db_manager import DBManager
from src.data.complaint_processor import ComplaintDataProcessor
from src.utils.config import config
from src.utils.logger import setup_logger

logger = setup_logger('test_db_storage')

def verify_db_connection(db_config=None):
    """
    验证数据库连接是否正常
    
    Args:
        db_config: 数据库配置，为None时使用配置文件中的配置
        
    Returns:
        连接成功返回True，失败返回False
    """
    logger.info("正在验证数据库连接...")
    
    db_manager = DBManager(db_config)
    if db_manager.connect():
        logger.info("数据库连接成功!")
        
        # 测试一个简单查询
        test_query = "SELECT current_timestamp"
        result = db_manager.query_one(test_query)
        if result:
            logger.info(f"测试查询成功: {result[0]}")
        
        db_manager.close()
        return True
    else:
        logger.error("数据库连接失败!")
        return False

def verify_complaints_table():
    """
    验证投诉表是否存在，并显示表结构
    
    Returns:
        表存在返回True，不存在返回False
    """
    logger.info("正在验证投诉表结构...")
    
    db_manager = DBManager()
    if not db_manager.connect():
        logger.error("数据库连接失败!")
        return False
    
    try:
        # 检查表是否存在
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'complaints'
        )
        """
        result = db_manager.query_one(check_table_sql)
        
        if result and result[0]:
            logger.info("投诉表存在!")
            
            # 获取表结构
            table_structure_sql = """
            SELECT 
                column_name, 
                data_type, 
                is_nullable
            FROM 
                information_schema.columns
            WHERE 
                table_schema = 'public' 
                AND table_name = 'complaints'
            ORDER BY 
                ordinal_position
            """
            columns = db_manager.query(table_structure_sql)
            
            if columns:
                logger.info("投诉表结构:")
                for col in columns:
                    logger.info(f"  {col[0]} ({col[1]}, {'NULL' if col[2] == 'YES' else 'NOT NULL'})")
                return True
            else:
                logger.warning("无法获取表结构信息")
                return False
        else:
            logger.error("投诉表不存在!")
            return False
            
    except Exception as e:
        logger.error(f"验证表结构时出错: {e}")
        return False
    finally:
        db_manager.close()

def test_process_sample_data(sample_file, create_table=False):
    """
    测试处理样本数据并存储到数据库
    
    Args:
        sample_file: 样本数据文件路径
        create_table: 是否需要创建表
        
    Returns:
        处理成功返回True，失败返回False
    """
    logger.info(f"正在测试处理样本数据: {sample_file}")
    
    try:
        # 检查文件是否存在
        file_path = Path(sample_file)
        if not file_path.exists():
            logger.error(f"样本文件不存在: {sample_file}")
            return False
        
        # 创建数据库管理器和处理器
        db_manager = DBManager()
        processor = ComplaintDataProcessor(db_manager)
        
        # 如果需要创建表
        if create_table:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS complaints (
                id SERIAL PRIMARY KEY,
                complaint_id VARCHAR(50) UNIQUE NOT NULL,
                title VARCHAR(255) NOT NULL,
                content TEXT,
                created_at TIMESTAMP WITHOUT TIME ZONE,
                updated_at TIMESTAMP WITHOUT TIME ZONE,
                status VARCHAR(50),
                reply_status VARCHAR(50),
                organization_id INTEGER,
                organization_name VARCHAR(255),
                category VARCHAR(100),
                source VARCHAR(50),
                raw_data JSONB,
                processed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            if db_manager.connect():
                logger.info("正在创建投诉表...")
                if db_manager.execute(create_table_sql):
                    logger.info("投诉表创建成功!")
                else:
                    logger.error("投诉表创建失败!")
                    return False
                db_manager.close()
        
        # 加载已有的投诉ID
        processor.load_existing_complaint_ids()
        
        # 处理样本文件
        processed_count = processor.process_complaint_file(str(file_path))
        
        if processed_count > 0:
            logger.info(f"成功处理并存储 {processed_count} 条投诉记录")
            return True
        else:
            logger.warning("未能处理任何投诉记录")
            return False
            
    except Exception as e:
        logger.error(f"处理样本数据时出错: {e}")
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='测试投诉数据存储到数据库')
    parser.add_argument('--sample', type=str, help='样本数据文件路径')
    parser.add_argument('--create-table', action='store_true', help='是否创建投诉表')
    parser.add_argument('--verify-only', action='store_true', help='仅验证数据库连接和表结构')
    
    args = parser.parse_args()
    
    # 验证数据库连接
    if not verify_db_connection():
        logger.error("数据库连接验证失败，终止测试")
        return
    
    # 验证表结构
    table_exists = verify_complaints_table()
    
    if args.verify_only:
        logger.info("验证完成，退出测试")
        return
    
    # 如果需要创建表或表不存在，且要处理样本数据
    create_table_needed = args.create_table or not table_exists
    
    # 处理样本数据
    if args.sample:
        success = test_process_sample_data(args.sample, create_table_needed)
        if success:
            logger.info("样本数据处理测试成功!")
        else:
            logger.error("样本数据处理测试失败!")
    else:
        logger.error("未指定样本数据文件，请使用 --sample 参数指定")

if __name__ == "__main__":
    main() 