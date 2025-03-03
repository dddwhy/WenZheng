"""
批量投诉数据处理器
实现高效的数据批量导入和更新
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

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import config
from src.utils.logger import setup_logger

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
    
    async def fetch_complaints(self, organization_id: int, page: int = 1, page_size: int = 100) -> Optional[Dict]:
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
        logger.info(f"开始获取组织 {organization_id} 的第 {page} 页数据")
        
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
            timeout = aiohttp.ClientTimeout(total=30)  # 设置30秒超时
            async with aiohttp.ClientSession(timeout=timeout) as session:
                logger.info(f"发送请求到 {url}")
                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status != 200:
                        logger.error(f"API请求失败: {response.status} {response.reason}")
                        return None
                    data = await response.json()
                    logger.info(f"成功获取数据: {len(data.get('data', {}).get('data', []))} 条记录")
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
                'thread_id': thread_id,
                'title': record.get('title', '')[:255],  # 截断过长的标题
                'content': record.get('content', ''),
                'assign_organization_id': record.get('assign_organization_id'),
                'chosen_organization_id': record.get('chosen_organization_id'),
                'organization_name': record.get('organization_name', '')[:255],
                'handle_status': record.get('handle_status', ''),
                'handle_status_real': record.get('handle_status_real', ''),
                'reply_status': record.get('reply_status', ''),
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
                'source': record.get('source', ''),
                'ip': record.get('ip', ''),
                'username': record.get('username', '')[:100],
                'passport_id': record.get('passport_id', '')[:100],
                'wechat_uid': record.get('wechat_uid', '')[:100],
                'area_id': record.get('area_id'),
                'field_id': record.get('field_id'),
                'field_name': record.get('field_name', '')[:100],
                'sort_id': record.get('sort_id'),
                'sort_name': record.get('sort_name', '')[:100],
                'visible_status': record.get('visible_status', ''),
                'updator': record.get('updator', '')[:100],
                'link': record.get('link', '')[:255],
                'category': self._categorize_complaint(
                    record.get('title', ''), 
                    record.get('content', '')
                ),
                'attaches': json.dumps(record.get('attaches', [])),
                'ext': json.dumps(record.get('ext', {}))
            }
            
            return cleaned
            
        except Exception as e:
            logger.error(f"清理记录时出错: {e}")
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
    
    async def process_organization(self, organization_id: int, page_size: int = 100) -> Dict[str, int]:
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
        
        while True:
            # 获取数据
            data = await self.fetch_complaints(organization_id, page, page_size)
            if not data:
                logger.warning(f"组织 {organization_id} 第 {page} 页数据获取失败")
                break
            
            # 提取记录
            records = data.get('data', {}).get('data', [])
            if not records:
                logger.info(f"组织 {organization_id} 第 {page} 页没有数据")
                break
            
            logger.info(f"处理组织 {organization_id} 第 {page} 页数据，共 {len(records)} 条记录")
            
            # 处理批次
            success, failed = self.process_batch(records)
            total_success += success
            total_failed += failed
            
            logger.info(f"组织 {organization_id} 第 {page} 页处理完成: 成功 {success}, 失败 {failed}")
            
            # 检查是否还有下一页
            total_records = data.get('data', {}).get('total', 0)
            if page * page_size >= total_records:
                logger.info(f"组织 {organization_id} 的数据已处理完成")
                break
            
            page += 1
            # 控制请求频率
            await asyncio.sleep(1)
        
        return {
            'organization_id': organization_id,
            'total_success': total_success,
            'total_failed': total_failed,
            'pages_processed': page
        }
    
    async def process_multiple_organizations(
        self,
        org_ids: List[int],
        page_size: int = 100
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

async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='批量处理投诉数据')
    parser.add_argument('--org-ids', type=int, nargs='+', required=True, help='要处理的组织ID列表')
    parser.add_argument('--page-size', type=int, default=100, help='每页数据量')
    parser.add_argument('--pool-size', type=int, default=5, help='数据库连接池大小')
    args = parser.parse_args()
    
    logger.info(f"开始批量处理，组织ID: {args.org_ids}, 页大小: {args.page_size}, 连接池大小: {args.pool_size}")
    
    try:
        processor = ComplaintBatchProcessor(pool_size=args.pool_size)
        
        # 处理数据
        results = await processor.process_multiple_organizations(
            args.org_ids,
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
            processor.save_failed_records()
        
    except Exception as e:
        logger.error(f"处理过程中出错: {str(e)}")
    finally:
        processor.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}") 