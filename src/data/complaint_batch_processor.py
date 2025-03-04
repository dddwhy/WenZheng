"""
批量投诉数据处理器
实现高效的数据批量导入和更新
最终版本
"""
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json
import asyncio
import aiohttp
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_batch
from contextlib import contextmanager
import traceback

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import config
from src.utils.logger import setup_logger

# 设置日志记录
logger = setup_logger('complaint_batch_processor')

class ComplaintBatchProcessor:
    """批量投诉数据处理器"""
    
    def __init__(self, pool_size: int = 5):
        """
        初始化处理器
        
        Args:
            pool_size: 数据库连接池大小
        """
        db_config = config.get('database')
        if not isinstance(db_config, dict) or 'postgres' not in db_config:
            raise ValueError("无法加载数据库配置")
            
        # 创建数据库连接池
        self.pool = SimpleConnectionPool(
            minconn=1,
            maxconn=pool_size,
            **db_config['postgres']
        )
        
        # 失败记录存储
        self.failed_records = []
        
        logger.info(f"批量处理器初始化完成，连接池大小: {pool_size}")
    
    @contextmanager
    def get_db_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
    
    async def fetch_complaints(self, organization_id: int, page: int = 1, page_size: int = 1000) -> Optional[Dict]:
        """
        从API获取投诉数据
        
        Args:
            organization_id: 组织ID
            page: 页码
            page_size: 每页数量
            
        Returns:
            API响应数据或None
        """
        api_config = config.get('api')
        if not isinstance(api_config, dict):
            logger.error("无法获取API配置")
            return None
            
        base_url = api_config.get('base_url')
        endpoints = api_config.get('endpoints', {})
        endpoint = endpoints.get('thread_page')
        
        if not base_url or not endpoint:
            logger.error(f"API配置不完整: base_url={base_url}, endpoint={endpoint}")
            return None
            
        url = f"{base_url}{endpoint}"
        logger.info(f"开始获取组织 {organization_id} 的第 {page} 页数据 (每页 {page_size} 条)")
        
        # 构建请求参数
        payload = {
            "sort_id": None,
            "field_id": None,
            "reply_status": "",
            "assign_organization_id": organization_id,
            "page": page,
            "size": page_size,
            "need_total": True
        }
        
        try:
            headers = api_config.get('headers', {})
            # 增加超时时间以适应更大的数据量
            timeout = aiohttp.ClientTimeout(total=60)  # 设置60秒超时
            async with aiohttp.ClientSession(timeout=timeout) as session:
                logger.info(f"发送请求到 {url}")
                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"API请求失败: {response.status} {response.reason}")
                        return None
                    data = await response.json()
                    record_count = len(data.get('data', {}).get('data', []))
                    logger.info(f"成功获取数据: {record_count} 条记录")
                    return data
        except asyncio.TimeoutError:
            logger.error(f"请求超时: {url}")
            return None
        except Exception as e:
            logger.error(f"获取投诉数据时出错: {str(e)}")
            return None
    
    def process_batch(self, records: List[Dict]) -> Tuple[int, int]:
        """
        批量处理投诉记录
        
        Args:
            records: 投诉记录列表
            
        Returns:
            (成功数, 失败数)
        """
        if not records:
            return 0, 0
            
        success_count = 0
        failed_count = 0
        
        insert_sql = """
        INSERT INTO complaints (
            thread_id, title, content, assign_organization_id, 
            chosen_organization_id, organization_name, handle_status,
            handle_status_real, reply_status, created_at, assign_at,
            handle_at, reply_at, done_at, deadline, updated_at,
            delete_at, expire_flag, warn_flag, apply_postpone_flag,
            apply_satisfaction_flag, apply_transfer_flag, can_feedback_flag,
            has_video, satisfaction, info_hidden, source, ip, username,
            passport_id, wechat_uid, area_id, field_id, field_name,
            sort_id, sort_name, visible_status, updator, link,
            category, attaches, ext
        ) VALUES (
            %(thread_id)s, %(title)s, %(content)s, %(assign_organization_id)s,
            %(chosen_organization_id)s, %(organization_name)s, %(handle_status)s,
            %(handle_status_real)s, %(reply_status)s, %(created_at)s, %(assign_at)s,
            %(handle_at)s, %(reply_at)s, %(done_at)s, %(deadline)s, %(updated_at)s,
            %(delete_at)s, %(expire_flag)s, %(warn_flag)s, %(apply_postpone_flag)s,
            %(apply_satisfaction_flag)s, %(apply_transfer_flag)s, %(can_feedback_flag)s,
            %(has_video)s, %(satisfaction)s, %(info_hidden)s, %(source)s, %(ip)s, %(username)s,
            %(passport_id)s, %(wechat_uid)s, %(area_id)s, %(field_id)s, %(field_name)s,
            %(sort_id)s, %(sort_name)s, %(visible_status)s, %(updator)s, %(link)s,
            %(category)s, %(attaches)s, %(ext)s
        )
        ON CONFLICT (thread_id) DO NOTHING
        """
        
        with self.get_db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # 开始事务
                    conn.autocommit = False
                    
                    # 准备数据
                    batch_data = []
                    for record in records:
                        try:
                            # 数据清洗和转换
                            processed_record = self._clean_record(record)
                            if processed_record:
                                batch_data.append(processed_record)
                            else:
                                failed_count += 1
                                self.failed_records.append({
                                    'record': record,
                                    'error': '数据清洗失败'
                                })
                        except Exception as e:
                            failed_count += 1
                            self.failed_records.append({
                                'record': record,
                                'error': str(e)
                            })
                    
                    # 批量插入
                    if batch_data:
                        execute_batch(cur, insert_sql, batch_data)
                        conn.commit()
                        success_count = len(batch_data)
                        
            except Exception as e:
                conn.rollback()
                logger.error(f"批量处理失败: {e}")
                failed_count = len(records)
                self.failed_records.extend([
                    {'record': r, 'error': str(e)} for r in records
                ])
            finally:
                conn.autocommit = True
        
        return success_count, failed_count
    
    def _clean_record(self, record: Dict) -> Optional[Dict]:
        """
        清理和转换投诉记录
        
        Args:
            record: 原始记录
            
        Returns:
            处理后的记录或None
        """
        try:
            # 提取必需字段
            thread_id = str(record.get('id'))
            if not thread_id:
                logger.warning("记录缺少ID")
                return None
            
            # 处理日期时间
            created_at = self._parse_datetime(record.get('created_at'))
            if not created_at:
                logger.warning(f"记录 {thread_id} 缺少创建时间")
                return None
            
            # 构建清理后的记录
            cleaned = {
                'thread_id': thread_id[:100],  # 限制长度
                'title': (record.get('title', '') or '')[:255],  # 处理None值并截断
                'content': record.get('content', ''),
                'assign_organization_id': record.get('assign_organization_id'),
                'chosen_organization_id': record.get('chosen_organization_id'),
                'organization_name': (record.get('organization_name', '') or '')[:255],
                'handle_status': (record.get('handle_status', '') or '')[:50],
                'handle_status_real': (record.get('handle_status_real', '') or '')[:50],
                'reply_status': (record.get('reply_status', '') or '')[:50],
                'created_at': created_at,
                'assign_at': self._parse_datetime(record.get('assign_at')),
                'handle_at': self._parse_datetime(record.get('handle_at')),
                'reply_at': self._parse_datetime(record.get('reply_at')),
                'done_at': self._parse_datetime(record.get('done_at')),
                'deadline': self._parse_datetime(record.get('deadline')),
                'updated_at': self._parse_datetime(record.get('updated_at')),
                'delete_at': self._parse_datetime(record.get('delete_at')),
                'expire_flag': bool(record.get('expire_flag')),
                'warn_flag': bool(record.get('warn_flag')),
                'apply_postpone_flag': bool(record.get('apply_postpone_flag')),
                'apply_satisfaction_flag': bool(record.get('apply_satisfaction_flag')),
                'apply_transfer_flag': bool(record.get('apply_transfer_flag')),
                'can_feedback_flag': bool(record.get('can_feedback_flag')),
                'has_video': int(record.get('has_video', 0)),
                'satisfaction': int(record.get('satisfaction', 0)),
                'info_hidden': int(record.get('info_hidden', 0)),
                'source': (record.get('source', '') or '')[:50],
                'ip': (record.get('ip', '') or '')[:50],
                'username': (record.get('username', '') or '')[:100],
                'passport_id': (record.get('passport_id', '') or '')[:100],
                'wechat_uid': (record.get('wechat_uid', '') or '')[:100],
                'area_id': record.get('area_id'),
                'field_id': record.get('field_id'),
                'field_name': (record.get('field_name', '') or '')[:100],
                'sort_id': record.get('sort_id'),
                'sort_name': (record.get('sort_name', '') or '')[:100],
                'visible_status': (record.get('visible_status', '') or '')[:50],
                'updator': (record.get('updator', '') or '')[:100],
                'link': (record.get('link', '') or '')[:255],
                'category': self._categorize_complaint(
                    record.get('title', ''), 
                    record.get('content', '')
                )[:50],
                'attaches': json.dumps(record.get('attaches', []), ensure_ascii=False),
                'ext': json.dumps(record.get('ext', {}), ensure_ascii=False)
            }
            
            return cleaned
            
        except Exception as e:
            logger.error(f"清理记录时出错: {str(e)}")
            return None
    
    def _parse_datetime(self, dt_str: str) -> Optional[datetime]:
        """解析日期时间字符串"""
        if not dt_str:
            return None
            
        try:
            formats = [
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(dt_str, fmt)
                except ValueError:
                    continue
            
            return None
            
        except Exception as e:
            logger.error(f"解析日期时间出错: {e}")
            return None
    
    def _categorize_complaint(self, title: str, content: str) -> str:
        """对投诉进行分类"""
        keywords = {
            '交通': ['交通', '公路', '道路', '高速', '公交', '地铁', '出行'],
            '教育': ['学校', '教育', '学生', '老师', '课程', '教学'],
            '医疗': ['医院', '医生', '治疗', '药品', '卫生', '疾病', '诊所'],
            '环境': ['垃圾', '污染', '环境', '噪音', '绿化', '卫生'],
            '住房': ['房屋', '住房', '楼盘', '开发商', '物业', '小区'],
            '就业': ['工作', '就业', '劳动', '工资', '待遇', '解雇'],
            '社会保障': ['社保', '保险', '医保', '养老', '低保', '救助']
        }
        
        text = f"{title} {content}"
        category_counts = {}
        
        for category, words in keywords.items():
            count = sum(1 for word in words if word in text)
            if count > 0:
                category_counts[category] = count
        
        return max(category_counts.items(), key=lambda x: x[1])[0] if category_counts else '其他'
    
    async def process_organization(self, organization_id: int, page_size: int = 1000) -> Dict[str, int]:
        """
        处理单个组织的投诉数据
        
        Args:
            organization_id: 组织ID
            page_size: 每页数量
            
        Returns:
            处理统计信息
        """
        total_success = 0
        total_failed = 0
        page = 1
        
        logger.info(f"开始处理组织 {organization_id} 的数据")
        
        # 首先获取总记录数
        first_page = await self.fetch_complaints(organization_id, 1, page_size)
        if not first_page:
            logger.error(f"无法获取组织 {organization_id} 的数据")
            return {
                'organization_id': organization_id,
                'total_success': 0,
                'total_failed': 0,
                'pages_processed': 0
            }
        
        total_records = first_page.get('data', {}).get('total', 0)
        total_pages = (total_records + page_size - 1) // page_size
        
        logger.info(f"组织 {organization_id} 共有 {total_records} 条记录，预计 {total_pages} 页")
        
        # 处理第一页数据
        records = first_page.get('data', {}).get('data', [])
        if records:
            success, failed = self.process_batch(records)
            total_success += success
            total_failed += failed
            logger.info(f"组织 {organization_id} 第 1/{total_pages} 页处理完成: 成功 {success}, 失败 {failed}")
        
        # 并发处理剩余页面，但限制并发数量
        if total_pages > 1:
            # 分批处理，每批2个请求
            for batch_start in range(2, total_pages + 1, 2):
                batch_end = min(batch_start + 2, total_pages + 1)
                tasks = []
                for page in range(batch_start, batch_end):
                    tasks.append(self.fetch_complaints(organization_id, page, page_size))
                
                # 并发执行当前批次的请求
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 处理响应
                for page_num, response in enumerate(responses, start=batch_start):
                    if isinstance(response, Exception):
                        logger.error(f"获取第 {page_num}/{total_pages} 页数据时出错: {str(response)}")
                        continue
                        
                    if not response:
                        continue
                    
                    records = response.get('data', {}).get('data', [])
                    if records:
                        success, failed = self.process_batch(records)
                        total_success += success
                        total_failed += failed
                        logger.info(f"组织 {organization_id} 第 {page_num}/{total_pages} 页处理完成: 成功 {success}, 失败 {failed}")
                
                # 批次间等待，避免请求过于频繁
                await asyncio.sleep(2)
        
        logger.info(f"组织 {organization_id} 处理完成: 成功 {total_success}, 失败 {total_failed}")
        
        return {
            'organization_id': organization_id,
            'total_success': total_success,
            'total_failed': total_failed,
            'pages_processed': total_pages
        }
    
    async def process_multiple_organizations(
        self,
        org_ids: List[int],
        page_size: int = 1000
    ) -> List[Dict[str, int]]:
        """
        处理多个组织的投诉数据
        
        Args:
            org_ids: 组织ID列表
            page_size: 每页数量
            
        Returns:
            处理结果列表
        """
        results = []
        for org_id in org_ids:
            try:
                result = await self.process_organization(org_id, page_size)
                results.append(result)
                logger.info(f"组织 {org_id} 处理完成: 成功 {result['total_success']}, 失败 {result['total_failed']}")
            except Exception as e:
                logger.error(f"处理组织 {org_id} 时出错: {e}")
                results.append({
                    'organization_id': org_id,
                    'error': str(e)
                })
        
        return results
    
    def save_failed_records(self, filename: str = 'failed_records.json'):
        """保存失败记录到文件"""
        if not self.failed_records:
            return
            
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.failed_records, f, ensure_ascii=False, indent=2)
            logger.info(f"失败记录已保存到 {filename}")
        except Exception as e:
            logger.error(f"保存失败记录时出错: {e}")
    
    def close(self):
        """关闭连接池"""
        if self.pool:
            self.pool.closeall()
            logger.info("数据库连接池已关闭")
    
    async def fetch_all_organizations(self) -> List[Dict[str, Any]]:
        """
        获取所有可用的组织列表
        
        Returns:
            组织列表，每个组织包含id和name
        """
        api_config = config.get('api')
        if not isinstance(api_config, dict):
            logger.error("无法获取API配置")
            return []
            
        base_url = api_config.get('base_url')
        endpoints = api_config.get('endpoints', {})
        endpoint = endpoints.get('organizations')  # 需要在config中添加organizations endpoint
        
        if not base_url or not endpoint:
            logger.error(f"API配置不完整: base_url={base_url}, endpoint={endpoint}")
            return []
            
        url = f"{base_url}{endpoint}"
        logger.info("开始获取所有组织列表")
        
        try:
            headers = api_config.get('headers', {})
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"获取组织列表失败: {response.status} {response.reason}")
                        return []
                    data = await response.json()
                    organizations = data.get('data', [])
                    logger.info(f"成功获取 {len(organizations)} 个组织")
                    return organizations
        except Exception as e:
            logger.error(f"获取组织列表时出错: {str(e)}")
            return []

    def fetch_all_organization_ids(self) -> List[int]:
        """
        从数据库中获取所有组织ID
        
        Returns:
            组织ID列表
        """
        query = "SELECT org_id FROM organizations"
        organization_ids = []
        
        with self.get_db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                    organization_ids = [row[0] for row in rows]
            except Exception as e:
                logger.error(f"获取组织ID时出错: {e}")
        
        return organization_ids

async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='批量处理投诉数据')
    parser.add_argument('--org-ids', type=int, nargs='+', help='要处理的组织ID列表')
    parser.add_argument('--page-size', type=int, default=1000, help='每页数据量')
    parser.add_argument('--pool-size', type=int, default=5, help='数据库连接池大小')
    parser.add_argument('--all', action='store_true', help='处理所有组织的数据')
    args = parser.parse_args()
    
    try:
        processor = ComplaintBatchProcessor(pool_size=args.pool_size)
        
        if args.all:
            logger.info("正在从数据库获取所有组织ID...")
            org_ids = processor.fetch_all_organization_ids()
            if not org_ids:
                logger.error("无法获取组织ID")
                return
            logger.info(f"获取到 {len(org_ids)} 个组织ID")
        elif not args.org_ids:
            logger.error("请指定组织ID列表或使用--all参数获取所有组织")
            return
        else:
            org_ids = args.org_ids
        
        logger.info(f"开始批量处理，组织ID数量: {len(org_ids)}, 页大小: {args.page_size}, 连接池大小: {args.pool_size}")
        
        # 检查配置
        db_config = config.get('database')
        api_config = config.get('api')
        
        logger.info("数据库配置:")
        logger.info(f"  host: {db_config.get('postgres', {}).get('host')}")
        logger.info(f"  database: {db_config.get('postgres', {}).get('database')}")
        logger.info(f"  user: {db_config.get('postgres', {}).get('user')}")
        
        logger.info("API配置:")
        logger.info(f"  base_url: {api_config.get('base_url')}")
        logger.info(f"  endpoints: {api_config.get('endpoints')}")
        
        # 处理数据
        results = await processor.process_multiple_organizations(
            org_ids,
            args.page_size
        )
        
        # 显示处理结果
        total_success = sum(r.get('total_success', 0) for r in results)
        total_failed = sum(r.get('total_failed', 0) for r in results)
        
        logger.info("\n处理结果:")
        logger.info(f"总成功数: {total_success}")
        logger.info(f"总失败数: {total_failed}")
        
        # 保存失败记录
        if processor.failed_records:
            # 创建logs目录（如果不存在）
            logs_dir = Path(project_root) / 'logs'
            logs_dir.mkdir(exist_ok=True)
            
            # 生成带时间戳的文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            failed_records_file = logs_dir / f'failed_records_{timestamp}.json'
            
            processor.save_failed_records(str(failed_records_file))
        
    except Exception as e:
        logger.error(f"处理过程中出错: {str(e)}")
        logger.error(f"错误详情:\n{traceback.format_exc()}")
    finally:
        if 'processor' in locals():
            processor.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}") 