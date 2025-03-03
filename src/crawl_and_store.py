"""
组织机构数据爬取与存储
"""
import asyncio
import argparse
import logging
from pathlib import Path
from src.crawlers.organization_crawler import OrganizationCrawler
from src.services.data_processor import OrganizationDataProcessor
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

async def crawl_province_data(save_dir=None):
    """
    爬取省级机构数据
    
    Args:
        save_dir: 保存目录，默认为None，表示使用默认目录
    
    Returns:
        保存的文件路径
    """
    crawler = OrganizationCrawler(save_dir=save_dir)
    logger.info("开始获取省级机构树数据...")
    province_data = await crawler.fetch_province_tree()
    if province_data:
        logger.info("省级机构树数据获取成功")
        return crawler.get_last_saved_file()
    else:
        logger.error("获取省级机构树数据失败")
        return None

async def crawl_city_data(city_id, save_dir=None):
    """
    爬取市级机构数据
    
    Args:
        city_id: 城市ID
        save_dir: 保存目录，默认为None，表示使用默认目录
    
    Returns:
        保存的文件路径
    """
    crawler = OrganizationCrawler(save_dir=save_dir)
    logger.info(f"开始获取城市 {city_id} 的机构树数据...")
    city_data = await crawler.fetch_city_tree(city_id)
    if city_data:
        logger.info(f"城市 {city_id} 的机构树数据获取成功")
        return crawler.get_last_saved_file()
    else:
        logger.error(f"获取城市 {city_id} 的机构树数据失败")
        return None

async def crawl_all_cities(province_data, save_dir=None):
    """
    爬取所有城市的机构数据
    
    Args:
        province_data: 省级机构数据
        save_dir: 保存目录，默认为None，表示使用默认目录
    
    Returns:
        保存的文件路径列表
    """
    if not province_data or 'data' not in province_data:
        logger.error("省级数据无效，无法获取城市列表")
        return []
    
    saved_files = []
    cities = []
    
    # 从省级数据中提取城市ID
    try:
        # 假设省级数据中的城市列表在data字段中
        for city in province_data.get('data', {}).get('children', []):
            if city.get('type') == 'CITY':
                cities.append(city)
    except Exception as e:
        logger.error(f"从省级数据提取城市列表时出错: {e}")
        return []
    
    logger.info(f"共发现 {len(cities)} 个城市")
    
    # 爬取每个城市的数据
    for city in cities:
        city_id = city.get('id')
        if city_id:
            file_path = await crawl_city_data(city_id, save_dir)
            if file_path:
                saved_files.append(file_path)
    
    logger.info(f"成功爬取 {len(saved_files)} 个城市的数据")
    return saved_files

async def main(args):
    # 创建保存目录
    save_dir = Path(args.save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    
    if args.action == 'crawl':
        # 爬取数据
        province_file = await crawl_province_data(save_dir)
        
        if province_file and args.crawl_cities:
            # 读取省级数据，获取城市列表
            with open(province_file, 'r', encoding='utf-8') as f:
                import json
                province_data = json.load(f)
            
            # 爬取所有城市数据
            await crawl_all_cities(province_data, save_dir)
    
    elif args.action == 'store':
        # 处理数据并存储到数据库
        processor = OrganizationDataProcessor()
        file_count, org_count = processor.process_directory(args.save_dir)
        logger.info(f"数据处理完成，共处理 {file_count} 个文件，{org_count} 个组织机构")
    
    elif args.action == 'all':
        # 先爬取再存储
        province_file = await crawl_province_data(save_dir)
        
        if province_file and args.crawl_cities:
            # 读取省级数据，获取城市列表
            with open(province_file, 'r', encoding='utf-8') as f:
                import json
                province_data = json.load(f)
            
            # 爬取所有城市数据
            await crawl_all_cities(province_data, save_dir)
        
        # 处理数据并存储到数据库
        processor = OrganizationDataProcessor()
        file_count, org_count = processor.process_directory(args.save_dir)
        logger.info(f"数据处理完成，共处理 {file_count} 个文件，{org_count} 个组织机构")

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='组织机构数据爬取与存储')
    parser.add_argument('--action', choices=['crawl', 'store', 'all'], default='all',
                        help='执行操作: crawl=仅爬取数据, store=仅存储数据, all=爬取并存储')
    parser.add_argument('--save-dir', default='data/samples',
                        help='数据保存目录，默认为 data/samples')
    parser.add_argument('--crawl-cities', action='store_true',
                        help='是否爬取所有城市数据')
    
    args = parser.parse_args()
    
    # 创建日志目录
    Path("logs").mkdir(exist_ok=True)
    
    # 执行主程序
    asyncio.run(main(args)) 