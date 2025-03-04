"""
数据导入脚本 - 将JSON数据导入到数据库
"""
import os
import sys
import argparse
from pathlib import Path
import json

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.services.data_processor import OrganizationDataProcessor
from src.utils.logger import setup_logger

logger = setup_logger('import_data')

def import_file(file_path):
    """
    导入单个文件
    
    Args:
        file_path: 文件路径
        
    Returns:
        处理的记录数
    """
    logger.info(f"正在导入文件: {file_path}")
    processor = OrganizationDataProcessor()
    
    # 添加数据预览
    with open(file_path) as f:
        sample_data = json.load(f)
        logger.debug(f"数据结构预览：{json.dumps(sample_data, ensure_ascii=False)[:200]}...")
    
    count = processor.process_file(file_path)
    logger.info(f"文件 {file_path} 导入完成，共处理 {count} 条记录")
    return count

def import_directory(directory_path):
    """
    导入目录中的所有JSON文件
    
    Args:
        directory_path: 目录路径
        
    Returns:
        (处理的文件数, 处理的记录总数)
    """
    directory = Path(directory_path)
    if not directory.exists() or not directory.is_dir():
        logger.error(f"目录 {directory_path} 不存在或不是一个目录")
        return 0, 0
    
    processor = OrganizationDataProcessor()
    file_count, record_count = processor.process_directory(directory_path)
    
    logger.info(f"目录 {directory_path} 导入完成，共处理 {file_count} 个文件，{record_count} 条记录")
    return file_count, record_count

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据导入工具')
    parser.add_argument('--file', '-f', type=str, help='要导入的JSON文件路径')
    parser.add_argument('--dir', '-d', type=str, help='要导入的JSON文件目录')
    
    args = parser.parse_args()
    
    if not args.file and not args.dir:
        print("错误: 必须指定 --file 或 --dir 参数")
        parser.print_help()
        return
    
    if args.file:
        count = import_file(args.file)
        print(f"文件导入完成，共处理 {count} 条记录")
    
    if args.dir:
        file_count, record_count = import_directory(args.dir)
        print(f"目录导入完成，共处理 {file_count} 个文件，{record_count} 条记录")

if __name__ == "__main__":
    main() 