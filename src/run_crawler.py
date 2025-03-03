"""
爬虫执行脚本
"""
import os
import sys
import uuid
import argparse
import json
from datetime import datetime
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.config import config
from src.utils.logger import setup_logger
from src.db.db_manager import DBManager
from src.crawlers.organization_crawler import OrganizationCrawler
from src.services.data_processor import OrganizationDataProcessor

logger = setup_logger('crawler_runner')

class CrawlerRunner:
    """爬虫执行器"""
    
    def __init__(self, db_config=None):
        """
        初始化爬虫执行器
        
        Args:
            db_config: 数据库配置
        """
        self.db_manager = DBManager(db_config)
        save_dir = config.get('crawler', 'save_directory')
        self.save_dir = save_dir if save_dir else 'data/scraped'
        
        # 确保保存目录存在
        Path(self.save_dir).mkdir(parents=True, exist_ok=True)
    
    def run_organization_crawler(self, start_org_id=None, is_recursive=True, max_depth=None):
        """
        执行组织机构爬虫
        
        Args:
            start_org_id: 起始组织ID，默认为None（从省级开始）
            is_recursive: 是否递归爬取子机构
            max_depth: 最大爬取深度
            
        Returns:
            任务ID
        """
        # 生成任务ID
        task_id = str(uuid.uuid4())
        
        # 记录任务开始
        self._log_task_start(task_id, 'organization', start_org_id)
        
        try:
            # 创建爬虫并设置保存目录
            crawler = OrganizationCrawler()
            crawler.set_save_directory(self.save_dir)
            
            # 运行爬虫
            result = crawler.crawl(start_org_id, is_recursive, max_depth)
            
            # 处理爬取结果
            processor = OrganizationDataProcessor()
            processed_files = processor.process_directory(self.save_dir)
            
            # 记录任务完成
            self._log_task_complete(task_id, True, {
                'crawled_organizations': result.get('count', 0),
                'processed_files': processed_files[0],
                'processed_organizations': processed_files[1]
            })
            
            return task_id
            
        except Exception as e:
            # 记录任务异常
            logger.error(f"爬虫任务执行失败: {e}")
            self._log_task_complete(task_id, False, {'error': str(e)})
            return task_id
    
    def _log_task_start(self, task_id, task_type, target=None):
        """记录任务开始"""
        if not self.db_manager.connect():
            logger.error("无法连接到数据库记录任务开始")
            return False
        
        try:
            # 构建任务参数
            params = {}
            if task_type == 'organization':
                params = {
                    'start_org_id': target
                }
            
            # 任务名称
            name = f"{task_type.capitalize()} Crawl Task"
            if target:
                name += f" (Start: {target})"
            
            # 插入任务记录
            sql = """
            INSERT INTO crawl_tasks
                (task_id, name, type, status, params, started_at)
            VALUES
                (%s, %s, %s, %s, %s, %s)
            """
            
            success = self.db_manager.execute(sql, (
                task_id,
                name,
                task_type,
                'running',
                json.dumps(params),
                datetime.now()
            ))
            
            if success:
                logger.info(f"任务 {task_id} 已开始记录")
            
            return success
        except Exception as e:
            logger.error(f"记录任务开始失败: {e}")
            return False
        finally:
            self.db_manager.close()
    
    def _log_task_complete(self, task_id, is_success, result_summary=None):
        """记录任务完成"""
        if not self.db_manager.connect():
            logger.error("无法连接到数据库记录任务完成")
            return False
        
        try:
            # 更新任务状态
            sql = """
            UPDATE crawl_tasks
            SET status = %s, result_summary = %s, completed_at = %s, updated_at = %s
            WHERE task_id = %s
            """
            
            status = 'completed' if is_success else 'failed'
            
            success = self.db_manager.execute(sql, (
                status,
                json.dumps(result_summary) if result_summary else None,
                datetime.now(),
                datetime.now(),
                task_id
            ))
            
            if success:
                logger.info(f"任务 {task_id} 已更新为 {status}")
            
            return success
        except Exception as e:
            logger.error(f"记录任务完成失败: {e}")
            return False
        finally:
            self.db_manager.close()

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='爬虫执行脚本')
    parser.add_argument('--type', '-t', type=str, default='organization', help='爬虫类型 (默认: organization)')
    parser.add_argument('--start_id', '-s', type=str, help='起始ID (默认: None)')
    parser.add_argument('--recursive', '-r', action='store_true', help='是否递归爬取')
    parser.add_argument('--max_depth', '-d', type=int, help='最大爬取深度')
    
    args = parser.parse_args()
    
    runner = CrawlerRunner()
    
    if args.type == 'organization':
        task_id = runner.run_organization_crawler(args.start_id, args.recursive, args.max_depth)
        print(f"爬虫任务已启动，任务ID: {task_id}")
    else:
        print(f"不支持的爬虫类型: {args.type}")
        sys.exit(1)

if __name__ == "__main__":
    main() 