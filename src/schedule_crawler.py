"""
定时执行爬虫任务
"""
import os
import sys
import time
import asyncio
import logging
import argparse
import schedule
from datetime import datetime
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.auto_complaint_crawler import AutoComplaintCrawler
from src.utils.logger import setup_logger
from src.utils.config import config

logger = setup_logger('schedule_crawler')

class CrawlerScheduler:
    """爬虫调度器"""
    
    def __init__(self):
        """初始化爬虫调度器"""
        self.is_running = False
        self.crawler = None
        
        # 创建日志目录
        Path("logs").mkdir(exist_ok=True)
    
    async def run_crawl_task(self, level=None, org_types=None, limit=None, pages=None):
        """
        执行爬取任务
        
        Args:
            level: 组织机构级别
            org_types: 组织机构类型列表
            limit: 最大爬取组织数量
            pages: 每个组织爬取的页数
        """
        if self.is_running:
            logger.warning("已有爬取任务正在运行，跳过本次任务")
            return
        
        self.is_running = True
        start_time = datetime.now()
        logger.info(f"开始执行定时爬取任务，时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 创建爬虫实例
            self.crawler = AutoComplaintCrawler()
            
            # 执行批量爬取
            result = await self.crawler.crawl_batch(
                level=level,
                org_types=org_types,
                limit=limit,
                pages_per_org=pages
            )
            
            # 计算耗时
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 打印结果
            logger.info(f"爬取任务完成，耗时: {duration:.2f} 秒")
            logger.info(f"爬取结果: {result}")
            
        except Exception as e:
            logger.error(f"执行爬取任务时出错: {e}")
        finally:
            self.is_running = False
    
    def schedule_job(self, schedule_time, level=None, org_types=None, limit=None, pages=None):
        """
        调度定时任务
        
        Args:
            schedule_time: 调度时间，格式为 "HH:MM" 或 "HH:MM:SS"
            level: 组织机构级别
            org_types: 组织机构类型列表
            limit: 最大爬取组织数量
            pages: 每个组织爬取的页数
        """
        logger.info(f"设置定时任务，执行时间: {schedule_time}")
        
        # 定义任务函数
        def job():
            logger.info(f"触发定时任务，当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            asyncio.run(self.run_crawl_task(level, org_types, limit, pages))
        
        # 设置定时任务
        schedule.every().day.at(schedule_time).do(job)
        
        return job
    
    def schedule_interval_job(self, interval_hours, level=None, org_types=None, limit=None, pages=None):
        """
        设置间隔执行的任务
        
        Args:
            interval_hours: 间隔小时数
            level: 组织机构级别
            org_types: 组织机构类型列表
            limit: 最大爬取组织数量
            pages: 每个组织爬取的页数
        """
        logger.info(f"设置间隔任务，每 {interval_hours} 小时执行一次")
        
        # 定义任务函数
        def job():
            logger.info(f"触发间隔任务，当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            asyncio.run(self.run_crawl_task(level, org_types, limit, pages))
        
        # 设置间隔任务
        schedule.every(interval_hours).hours.do(job)
        
        return job
    
    def run_scheduler(self):
        """运行调度器"""
        logger.info("启动调度器...")
        
        try:
            # 无限循环，持续检查是否有任务需要执行
            while True:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            logger.info("收到中断信号，停止调度器")
        except Exception as e:
            logger.error(f"调度器运行出错: {e}")

def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='定时执行爬虫任务')
    parser.add_argument('--time', type=str, help='每日执行时间，格式为 "HH:MM"，如 "02:30"')
    parser.add_argument('--interval', type=float, help='间隔执行时间，单位为小时，如 12 表示每12小时执行一次')
    parser.add_argument('--level', type=int, help='组织机构级别，如3表示区县级')
    parser.add_argument('--types', type=str, help='组织机构类型，多个类型用逗号分隔，如AREA,DEPARTMENT')
    parser.add_argument('--limit', type=int, help='最大爬取组织数量')
    parser.add_argument('--pages', type=int, help='每个组织爬取的页数，不指定则爬取全部')
    parser.add_argument('--now', action='store_true', help='是否立即执行一次任务')
    
    args = parser.parse_args()
    
    # 解析组织类型
    org_types = None
    if args.types:
        org_types = [t.strip() for t in args.types.split(',')]
    
    # 创建调度器
    scheduler = CrawlerScheduler()
    
    # 设置定时任务
    if args.time:
        job = scheduler.schedule_job(
            args.time, 
            level=args.level,
            org_types=org_types,
            limit=args.limit,
            pages=args.pages
        )
    elif args.interval:
        job = scheduler.schedule_interval_job(
            args.interval,
            level=args.level,
            org_types=org_types,
            limit=args.limit,
            pages=args.pages
        )
    else:
        logger.error("请指定任务执行时间(--time)或间隔时间(--interval)")
        sys.exit(1)
    
    # 是否立即执行
    if args.now:
        logger.info("立即执行一次任务")
        job()
    
    # 运行调度器
    scheduler.run_scheduler()

if __name__ == "__main__":
    main() 