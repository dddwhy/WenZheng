"""
自动爬取组织机构投诉数据并存入数据库
"""
import os
import sys
import asyncio
import argparse
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.db_manager import DBManager
from src.services.org_query import OrganizationQuery
from src.scripts.fetch_complaint_data import ComplaintDataFetcher
# 使用优化后的投诉数据处理器
from src.data.complaint_processor_improved import ComplaintProcessorImproved
from src.utils.config import config
from src.utils.logger import setup_logger

logger = setup_logger('auto_complaint_crawler')

class AutoComplaintCrawler:
    """自动投诉爬取器"""
    
    def __init__(self, save_dir=None, db_config=None, force_reprocess=False):
        """
        初始化自动投诉爬取器
        
        Args:
            save_dir: 保存目录路径，为None则使用配置中的目录
            db_config: 数据库配置，为None则使用配置中的数据库配置
            force_reprocess: 是否强制重新处理已存在的投诉数据
        """
        self.db_manager = DBManager(db_config)
        self.org_query = OrganizationQuery(db_config)
        self.force_reprocess = force_reprocess
        
        # 设置保存目录
        if save_dir:
            self.save_dir = Path(save_dir)
        else:
            save_dir_config = config.get('crawler', 'save_directory')
            self.save_dir = Path(save_dir_config if save_dir_config else 'data/complaints')
        
        # 确保保存目录存在
        self.save_dir.mkdir(parents=True, exist_ok=True)
        
        # 确保数据库表结构存在
        self._ensure_table_exists()
        
        logger.info(f"爬虫保存目录: {self.save_dir}")
    
    def _ensure_table_exists(self):
        """确保complaints表存在"""
        try:
            if not self.db_manager.connect():
                logger.error("无法连接数据库创建表")
                return False
            
            # 检查表是否存在
            check_sql = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'complaints'
            )
            """
            result = self.db_manager.query_one(check_sql)
            
            # 如果表不存在，创建它
            if not result or not result[0]:
                logger.info("投诉表不存在，正在创建...")
                
                create_sql = """
                CREATE TABLE IF NOT EXISTS complaints (
                    id SERIAL PRIMARY KEY,
                    thread_id VARCHAR(50) UNIQUE NOT NULL,
                    title VARCHAR(255),
                    content TEXT,
                    assign_organization_id INTEGER,
                    chosen_organization_id INTEGER,
                    organization_name VARCHAR(255),
                    handle_status VARCHAR(50),
                    handle_status_real VARCHAR(50),
                    reply_status VARCHAR(50),
                    created_at TIMESTAMP WITHOUT TIME ZONE,
                    assign_at TIMESTAMP WITHOUT TIME ZONE,
                    handle_at TIMESTAMP WITHOUT TIME ZONE,
                    reply_at TIMESTAMP WITHOUT TIME ZONE,
                    done_at TIMESTAMP WITHOUT TIME ZONE,
                    deadline TIMESTAMP WITHOUT TIME ZONE,
                    updated_at TIMESTAMP WITHOUT TIME ZONE,
                    delete_at TIMESTAMP WITHOUT TIME ZONE,
                    expire_flag BOOLEAN,
                    warn_flag BOOLEAN,
                    apply_postpone_flag BOOLEAN,
                    apply_satisfaction_flag BOOLEAN,
                    apply_transfer_flag BOOLEAN,
                    can_feedback_flag BOOLEAN,
                    has_video INTEGER,
                    satisfaction INTEGER,
                    info_hidden INTEGER,
                    source VARCHAR(50),
                    ip VARCHAR(50),
                    username VARCHAR(100),
                    passport_id VARCHAR(100),
                    wechat_uid VARCHAR(100),
                    area_id INTEGER,
                    field_id INTEGER,
                    field_name VARCHAR(100),
                    sort_id INTEGER,
                    sort_name VARCHAR(100),
                    visible_status VARCHAR(50),
                    updator VARCHAR(100),
                    link VARCHAR(255),
                    category VARCHAR(100),
                    attaches JSONB,
                    ext JSONB,
                    search_vector TSVECTOR
                );
                """
                
                if self.db_manager.execute(create_sql):
                    logger.info("投诉表创建成功")
                    return True
                else:
                    logger.error("投诉表创建失败")
                    return False
            else:
                logger.info("投诉表已存在")
                return True
                
        except Exception as e:
            logger.error(f"检查/创建表时出错: {e}")
            return False
    
    async def crawl_organization_complaints(self, org_id: int, 
                                           pages: int = None, 
                                           page_size: int = 20) -> Tuple[int, int]:
        """
        爬取指定组织的投诉数据
        
        Args:
            org_id: 组织ID
            pages: 要爬取的页数，None表示爬取所有页
            page_size: 每页数据量
            
        Returns:
            (爬取的页数, 爬取的投诉数量)
        """
        logger.info(f"开始爬取组织 {org_id} 的投诉数据...")
        
        # 创建投诉数据抓取器
        fetcher = ComplaintDataFetcher()
        
        try:
            # 爬取所有页的投诉数据
            all_complaints = await fetcher.fetch_all_pages(
                organization_id=int(org_id),
                start_page=1,
                size=page_size,
                max_pages=pages
            )
            
            if all_complaints:
                complaint_count = len(all_complaints)
                logger.info(f"成功爬取组织 {org_id} 的投诉数据，共 {complaint_count} 条")
                
                # 计算页数
                if pages:
                    page_count = min(pages, (complaint_count + page_size - 1) // page_size)
                else:
                    page_count = (complaint_count + page_size - 1) // page_size
                
                return page_count, complaint_count
            else:
                logger.warning(f"组织 {org_id} 未获取到投诉数据")
                return 0, 0
                
        except Exception as e:
            logger.error(f"爬取组织 {org_id} 的投诉数据时出错: {e}")
            return 0, 0
    
    def process_crawled_data(self) -> Tuple[int, int]:
        """
        处理爬取的投诉数据，将其存入数据库
        
        Returns:
            (处理的文件数, 处理的投诉数)
        """
        logger.info(f"开始处理爬取的投诉数据...")
        
        try:
            # 创建优化后的投诉数据处理器，传入强制重新处理参数
            processor = ComplaintProcessorImproved(self.db_manager, force_reprocess=self.force_reprocess)
            
            # 处理目录下的所有投诉数据文件
            file_count = processor.process_directory(str(self.save_dir))
            
            logger.info(f"投诉数据处理完成，共处理 {file_count} 个文件，{processor.processed_count} 条记录")
            return file_count, processor.processed_count
            
        except Exception as e:
            logger.error(f"处理投诉数据时出错: {e}")
            return 0, 0
    
    def get_organizations_to_crawl(self, level=None, org_types=None, limit=None, end_nodes_only=False) -> List[Dict[str, Any]]:
        """
        获取需要爬取的组织机构列表
        
        Args:
            level: 组织机构级别，为None表示不限级别
            org_types: 组织机构类型列表，为None表示不限类型
            limit: 最大返回数量，为None表示不限数量
            end_nodes_only: 是否只返回末尾节点（没有子节点的组织）
            
        Returns:
            符合条件的组织机构列表
        """
        logger.info(f"开始查询需要爬取的组织机构...")
        
        try:
            if not self.db_manager.connect():
                logger.error("连接数据库失败")
                return []
            
            # 构建查询条件
            conditions = []
            params = []
            
            # 添加末尾节点过滤条件
            if end_nodes_only:
                conditions.append("has_children = FALSE")
                # 如果是末尾节点模式，默认只查询第三级或更高级别的组织
                if level is None:
                    conditions.append("level >= 3")
                    logger.info("只查询第三级或更高级别的末尾节点（没有子节点的组织）")
                else:
                    logger.info(f"只查询第{level}级别的末尾节点（没有子节点的组织）")
            
            # 添加级别过滤
            if level is not None:
                conditions.append("level = %s")
                params.append(level)
            
            if org_types and isinstance(org_types, list):
                placeholders = ', '.join(['%s'] * len(org_types))
                conditions.append(f"\"type\" IN ({placeholders})")
                params.extend(org_types)
            
            # 组合WHERE子句
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # 构建完整SQL
            sql = f"""
            SELECT id, org_id, name, type, level
            FROM organizations
            WHERE {where_clause}
            ORDER BY level, org_id
            """
            
            # 添加LIMIT子句
            if limit is not None:
                sql += " LIMIT %s"
                params.append(limit)
            
            logger.info(f"执行SQL查询: {sql} 参数: {params}")
            
            # 执行查询
            results = self.db_manager.query(sql, tuple(params))
            
            if not results:
                logger.warning("未找到符合条件的组织机构")
                return []
            
            # 转换结果
            orgs = []
            for result in results:
                org = {
                    'id': result[0],
                    'org_id': result[1],
                    'name': result[2],
                    'type': result[3],
                    'level': result[4]
                }
                orgs.append(org)
            
            logger.info(f"找到 {len(orgs)} 个符合条件的组织机构")
            for i, org in enumerate(orgs[:5], 1):  # 只打印前5个
                logger.info(f"{i}. 组织ID: {org['org_id']}, 名称: {org['name']}, 级别: {org['level']}, 类型: {org['type']}")
            
            if len(orgs) > 5:
                logger.info(f"... 还有 {len(orgs) - 5} 个组织")
                
            return orgs
            
        except Exception as e:
            logger.error(f"查询组织机构时出错: {e}")
            return []
        finally:
            self.db_manager.close()
    
    async def crawl_batch(self, 
                         level=None, 
                         org_types=None, 
                         limit=None, 
                         pages_per_org=None,
                         page_size=20,
                         end_nodes_only=False) -> Dict[str, Any]:
        """
        批量爬取一组组织的投诉数据
        
        Args:
            level: 组织级别，为None表示不限级别
            org_types: 组织类型列表，为None表示不限类型
            limit: 最大组织数量，为None表示不限数量
            pages_per_org: 每个组织爬取的页数，为None表示爬取全部
            page_size: 每页数据量
            end_nodes_only: 是否只爬取末尾节点（没有子节点的组织）
            
        Returns:
            爬取结果统计
        """
        # 获取需要爬取的组织列表
        orgs = self.get_organizations_to_crawl(level, org_types, limit, end_nodes_only)
        
        if not orgs:
            return {
                'status': 'failed',
                'message': '未找到符合条件的组织机构',
                'crawled_orgs': 0,
                'total_pages': 0,
                'total_complaints': 0
            }
        
        # 爬取统计
        total_pages = 0
        total_complaints = 0
        successful_orgs = 0
        failed_orgs = 0
        
        # 循环爬取每个组织的投诉
        for org in orgs:
            org_id = org['org_id']
            org_name = org['name']
            
            try:
                logger.info(f"开始爬取组织 [{org_id}] {org_name} 的投诉数据...")
                
                # 爬取投诉数据
                page_count, complaint_count = await self.crawl_organization_complaints(
                    org_id=org_id,
                    pages=pages_per_org,
                    page_size=page_size
                )
                
                # 更新统计
                if complaint_count > 0:
                    total_pages += page_count
                    total_complaints += complaint_count
                    successful_orgs += 1
                    logger.info(f"组织 [{org_id}] {org_name} 爬取成功，获取了 {complaint_count} 条投诉")
                else:
                    failed_orgs += 1
                    logger.warning(f"组织 [{org_id}] {org_name} 未获取到投诉数据")
                
                # 每爬取5个组织休息一下，避免请求过快
                if (successful_orgs + failed_orgs) % 5 == 0:
                    logger.info("休息5秒钟...")
                    await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"爬取组织 [{org_id}] {org_name} 时出错: {e}")
                failed_orgs += 1
        
        # 处理爬取的数据
        file_count, processed_count = self.process_crawled_data()
        
        # 返回统计结果
        return {
            'status': 'success',
            'message': '批量爬取完成',
            'crawled_orgs': successful_orgs,
            'failed_orgs': failed_orgs,
            'total_orgs': len(orgs),
            'total_pages': total_pages,
            'total_complaints': total_complaints,
            'processed_files': file_count,
            'processed_complaints': processed_count
        }

async def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='自动爬取组织机构投诉数据')
    parser.add_argument('--level', type=int, help='组织机构级别，如3表示区县级')
    parser.add_argument('--types', type=str, help='组织机构类型，多个类型用逗号分隔，如AREA,DEPARTMENT')
    parser.add_argument('--limit', type=int, help='最大爬取组织数量')
    parser.add_argument('--pages', type=int, help='每个组织爬取的页数，不指定则爬取全部')
    parser.add_argument('--page-size', type=int, default=20, help='每页数据量，默认20')
    parser.add_argument('--save-dir', type=str, help='保存目录路径')
    parser.add_argument('--test-db', action='store_true', help='测试数据库连接并退出')
    parser.add_argument('--end-nodes-only', action='store_true', help='只爬取末尾节点（没有子节点的组织）')
    parser.add_argument('--org-id', type=int, help='直接指定要爬取的组织ID，优先于其他选项')
    parser.add_argument('--force', action='store_true', help='强制重新处理已存在的投诉数据')
    
    args = parser.parse_args()
    
    # 创建爬虫实例
    crawler = AutoComplaintCrawler(save_dir=args.save_dir, force_reprocess=args.force)
    
    # 如果只是测试数据库连接
    if args.test_db:
        from src.test_db_storage import verify_db_connection, verify_complaints_table
        logger.info("正在测试数据库连接...")
        if verify_db_connection():
            verify_complaints_table()
        return
    
    # 如果指定了特定组织ID
    if args.org_id:
        logger.info(f"直接爬取指定组织ID: {args.org_id}")
        page_count, complaint_count = await crawler.crawl_organization_complaints(
            org_id=args.org_id,
            pages=args.pages,
            page_size=args.page_size
        )
        
        if complaint_count > 0:
            logger.info(f"成功爬取组织 {args.org_id} 的投诉数据，共 {complaint_count} 条，{page_count} 页")
            # 处理爬取的数据
            file_count, processed_count = crawler.process_crawled_data()
            logger.info(f"投诉数据处理完成，共处理 {file_count} 个文件，{processed_count} 条记录")
        else:
            logger.warning(f"组织 {args.org_id} 未获取到投诉数据")
        return
    
    # 解析组织类型
    org_types = None
    if args.types:
        org_types = [t.strip() for t in args.types.split(',')]
    
    # 开始批量爬取
    result = await crawler.crawl_batch(
        level=args.level,
        org_types=org_types,
        limit=args.limit,
        pages_per_org=args.pages,
        page_size=args.page_size,
        end_nodes_only=args.end_nodes_only
    )
    
    # 打印结果
    logger.info("爬取任务完成!")
    for key, value in result.items():
        logger.info(f"{key}: {value}")

if __name__ == "__main__":
    asyncio.run(main()) 