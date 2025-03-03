import asyncio
from src.crawlers.organization_crawler import OrganizationCrawler
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

async def main():
    crawler = OrganizationCrawler()
    
    # 获取省级机构树
    logger.info("开始获取省级机构树数据...")
    province_data = await crawler.fetch_province_tree()
    
    # 获取第一个城市的数据作为样本
    if province_data and province_data.get('data'):
        first_city = province_data['data'][0]
        city_id = first_city.get('id')
        if city_id:
            logger.info(f"开始获取城市 {city_id} 的机构树数据...")
            await crawler.fetch_city_tree(city_id)

if __name__ == "__main__":
    asyncio.run(main())