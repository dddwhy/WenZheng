import aiohttp
import asyncio
import json
from pathlib import Path
from datetime import datetime
from src.utils.config import config
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class BaseCrawler:
    def __init__(self, save_dir=None):
        """
        初始化爬虫
        
        Args:
            save_dir: 数据保存目录，为None时使用默认目录
        """
        self.base_url = config.get('api', 'base_url')
        self.headers = config.get('api', 'headers')
        self.retry_config = config.get('api', 'retry')
        
        # 设置保存目录
        if save_dir is None:
            self.save_path = Path("data/samples")
        else:
            self.save_path = Path(save_dir)
            
        # 确保目录存在
        self.save_path.mkdir(parents=True, exist_ok=True)
        
        # 保存最后一次保存的文件路径
        self.last_saved_file = None
        
    async def fetch(self, endpoint: str, params: dict = None, method: str = "GET", data: dict = None) -> dict:
        """
        发送HTTP请求并获取响应数据
        
        Args:
            endpoint: API端点
            params: URL参数
            method: 请求方法，GET或POST
            data: POST请求体数据
            
        Returns:
            响应数据（JSON格式）
        """
        url = f"{self.base_url}{endpoint}"
        logger.info(f"请求URL: {url}, 方法: {method}")
        
        for attempt in range(self.retry_config['max_attempts']):
            try:
                async with aiohttp.ClientSession() as session:
                    if method.upper() == "GET":
                        async with session.get(url, headers=self.headers, params=params) as response:
                            data = await response.json()
                            return data
                    elif method.upper() == "POST":
                        async with session.post(url, headers=self.headers, params=params, json=data) as response:
                            data = await response.json()
                            return data
                    else:
                        logger.error(f"不支持的请求方法: {method}")
                        return {"code": -1, "msg": f"不支持的请求方法: {method}"}
            except Exception as e:
                logger.error(f"请求失败 {url}: {e}")
                if attempt == self.retry_config['max_attempts'] - 1:
                    raise
                await asyncio.sleep(self.retry_config['delay'])
    
    def save_response(self, data: dict, filename: str):
        """
        保存响应数据到文件
        
        Args:
            data: 响应数据
            filename: 文件名前缀
        
        Returns:
            保存的文件路径
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_path = self.save_path / f"{filename}_{timestamp}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"数据已保存到: {file_path}")
        
        # 记录最后保存的文件路径
        self.last_saved_file = file_path
        
        return file_path
    
    def get_last_saved_file(self):
        """
        获取最后一次保存的文件路径
        
        Returns:
            文件路径或None
        """
        return self.last_saved_file