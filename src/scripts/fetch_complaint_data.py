"""
投诉线索数据抓取脚本
"""
import os
import sys
import json
import argparse
import aiohttp
import asyncio
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.utils.config import config
from src.utils.logger import setup_logger

logger = setup_logger('complaint_fetcher')

class ComplaintDataFetcher:
    """投诉线索数据抓取器"""
    
    def __init__(self):
        """初始化抓取器"""
        api_config = config.get('api')
        if api_config:
            self.base_url = api_config.get('base_url')
            self.headers = api_config.get('headers')
            self.thread_page_endpoint = api_config.get('endpoints', {}).get('thread_page')
        else:
            logger.error("无法获取API配置")
            self.base_url = "https://wz-api.chuanbaoguancha.cn/api/v1"
            self.headers = {}
            self.thread_page_endpoint = "/thread/page"
        
        crawler_config = config.get('crawler')
        if crawler_config:
            self.save_dir = Path(crawler_config.get('save_directory', 'data/complaints'))
        else:
            self.save_dir = Path('data/complaints')
        
        # 确保保存目录存在
        self.save_dir.mkdir(parents=True, exist_ok=True)
        
        # 打印配置信息
        logger.info(f"API基础URL: {self.base_url}")
        logger.info(f"API终端点: {self.thread_page_endpoint}")
        logger.info(f"请求头: {json.dumps(self.headers, ensure_ascii=False, indent=2)}")
        logger.info(f"保存目录: {self.save_dir}")
    
    async def fetch_complaints(self, organization_id, page=1, size=20, save_to_file=True):
        """
        抓取指定组织机构的投诉线索数据
        
        Args:
            organization_id: 组织机构ID
            page: 页码
            size: 每页数量
            save_to_file: 是否保存到文件
            
        Returns:
            投诉线索数据
        """
        url = f"{self.base_url}{self.thread_page_endpoint}"
        
        # 确保组织ID是整数类型
        try:
            organization_id = int(organization_id)
        except ValueError:
            logger.error(f"组织ID必须是整数，但收到了: {organization_id}")
            return None
        
        # 构建请求数据
        payload = {
            "sort_id": None,
            "field_id": None,
            "reply_status": "",
            "assign_organization_id": organization_id,  # 现在是整数类型
            "page": page,
            "size": size,
            "need_total": True
        }
        
        logger.info(f"请求URL: {url}")
        logger.info(f"请求体: {json.dumps(payload, ensure_ascii=False)}")
        logger.info(f"开始抓取组织 {organization_id} 的投诉线索数据，页码: {page}, 每页数量: {size}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=self.headers) as response:
                    status_code = response.status
                    logger.info(f"响应状态码: {status_code}")
                    
                    if status_code != 200:
                        logger.error(f"请求失败，状态码: {status_code}")
                        error_text = await response.text()
                        logger.error(f"错误响应: {error_text[:500]}")  # 只打印前500个字符
                        return None
                    
                    data = await response.json()
                    
                    if save_to_file:
                        filename = f"complaints_{organization_id}_p{page}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                        file_path = self.save_dir / filename
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        logger.info(f"数据保存到: {file_path}")
                    
                    return data
                    
        except Exception as e:
            logger.error(f"抓取数据出错: {e}")
            return None
    
    async def fetch_all_pages(self, organization_id, start_page=1, size=20, max_pages=None):
        """
        抓取指定组织机构的所有页投诉线索数据
        
        Args:
            organization_id: 组织机构ID
            start_page: 起始页码
            size: 每页数量
            max_pages: 最大页数限制
            
        Returns:
            所有投诉线索数据列表
        """
        all_data = []
        page = start_page
        total_pages = float('inf') if max_pages is None else max_pages + start_page - 1
        
        while page <= total_pages:
            data = await self.fetch_complaints(organization_id, page, size)
            
            if not data or data.get('code') != 0:
                error_msg = data.get('msg') if data else "未知错误"
                logger.error(f"获取数据失败: {error_msg}")
                break
            
            # 提取数据
            result = data.get('data', {})
            items = result.get('records', [])
            
            if not items:
                logger.info(f"没有更多数据，已获取 {page - start_page} 页")
                break
                
            all_data.extend(items)
            logger.info(f"已获取第 {page} 页数据，共 {len(items)} 条")
            
            # 计算总页数
            total = result.get('total', 0)
            total_pages_from_api = (total + size - 1) // size
            
            if max_pages is None:
                total_pages = total_pages_from_api
            else:
                total_pages = min(total_pages_from_api, max_pages + start_page - 1)
            
            logger.info(f"总记录数: {total}, 总页数: {total_pages_from_api}")
            
            # 判断是否还有下一页
            if page >= total_pages:
                logger.info(f"已到达最大页数 {page}/{total_pages}")
                break
                
            page += 1
            
            # 简单的频率控制
            await asyncio.sleep(1)
        
        # 保存所有数据到一个文件
        if all_data:
            filename = f"all_complaints_{organization_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            file_path = self.save_dir / filename
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=2)
            logger.info(f"所有数据保存到: {file_path}，共 {len(all_data)} 条")
        
        return all_data

async def main_async():
    """异步主函数"""
    parser = argparse.ArgumentParser(description='投诉线索数据抓取工具')
    parser.add_argument('--org_id', '-o', type=str, required=True, help='组织机构ID')
    parser.add_argument('--page', '-p', type=int, default=1, help='页码（默认1）')
    parser.add_argument('--size', '-s', type=int, default=20, help='每页数量（默认20）')
    parser.add_argument('--all', '-a', action='store_true', help='抓取所有页')
    parser.add_argument('--max_pages', '-m', type=int, help='最大页数限制')
    
    args = parser.parse_args()
    
    fetcher = ComplaintDataFetcher()
    
    if args.all:
        await fetcher.fetch_all_pages(args.org_id, args.page, args.size, args.max_pages)
    else:
        await fetcher.fetch_complaints(args.org_id, args.page, args.size)

def main():
    """入口函数"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main() 