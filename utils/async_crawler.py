import asyncio
import aiohttp
from typing import List, Dict, Any

class AsyncCrawler:
    def __init__(self, config: dict):
        self.config = config
        self.semaphore = asyncio.Semaphore(5)  # 控制并发数
        
    async def fetch(self, url: str) -> Dict[str, Any]:
        async with self.semaphore:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url) as response:
                        return await response.json()
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e}")
                    return None

    async def batch_fetch(self, urls: List[str]) -> List[Dict[str, Any]]:
        tasks = [self.fetch(url) for url in urls]
        return await asyncio.gather(*tasks)