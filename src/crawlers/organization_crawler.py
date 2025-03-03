from src.crawlers.base_crawler import BaseCrawler
from src.utils.config import config
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class OrganizationCrawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.endpoints = config.get('api', 'endpoints')
        
    async def fetch_province_tree(self):
        """
        获取省级机构树数据
        """
        endpoint = self.endpoints.get('province_tree')
        logger.info(f"正在请求省级机构树数据: {endpoint}")
        try:
            # 尝试使用POST方法
            data = await self.fetch(endpoint, method="POST")
            if data and data.get('code') == 0:
                # 保存响应数据到文件
                self.save_response(data, "province_tree")
                logger.info("省级机构树数据获取成功")
                return data
            else:
                logger.error(f"省级机构树数据获取失败: {data}")
                return None
        except Exception as e:
            logger.error(f"获取省级机构树数据时发生异常: {e}")
            return None
    
    async def fetch_city_tree(self, city_id):
        """
        获取市级机构树数据
        
        Args:
            city_id (int): 城市ID
        """
        endpoint = self.endpoints.get('city_tree')
        params = {'cityId': city_id}
        logger.info(f"正在请求城市 {city_id} 的机构树数据: {endpoint}")
        try:
            # 尝试使用POST方法，同时传递参数
            data = await self.fetch(endpoint, params=params, method="POST")
            if data and data.get('code') == 0:
                # 保存响应数据到文件
                self.save_response(data, f"city_tree_{city_id}")
                logger.info(f"城市 {city_id} 的机构树数据获取成功")
                return data
            else:
                logger.error(f"城市 {city_id} 的机构树数据获取失败: {data}")
                return None
        except Exception as e:
            logger.error(f"获取城市 {city_id} 的机构树数据时发生异常: {e}")
            return None 