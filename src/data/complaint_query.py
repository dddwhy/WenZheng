"""
投诉数据查询模块，提供各种查询和统计功能
"""
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Union
import logging
import sys

from src.db.db_manager import DBManager
from src.utils.config import config

# 配置根日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class ComplaintQuery:
    """投诉查询类，用于查询和分析投诉数据"""
    
    def __init__(self, db_manager: DBManager):
        """
        初始化投诉查询类
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db_manager = db_manager
    
    def get_complaint_by_id(self, complaint_id: str) -> Optional[Dict[str, Any]]:
        """
        根据ID获取投诉详情
        
        Args:
            complaint_id: 投诉ID
            
        Returns:
            投诉详情或None
        """
        query = """
        SELECT 
            id, complaint_id, title, content, created_at, updated_at,
            status, reply_status, organization_id, organization_name,
            category, source, raw_data
        FROM complaints
        WHERE complaint_id = %s
        """
        
        result = self.db_manager.query_one(query, (complaint_id,))
        if not result:
            return None
            
        # 转换为字典
        columns = [
            'id', 'complaint_id', 'title', 'content', 'created_at', 'updated_at',
            'status', 'reply_status', 'organization_id', 'organization_name',
            'category', 'source', 'raw_data'
        ]
        
        return dict(zip(columns, result))
    
    def search_complaints(self, 
                         keywords: Optional[str] = None,
                         organization_id: Optional[int] = None,
                         category: Optional[str] = None,
                         status: Optional[str] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         limit: int = 100,
                         offset: int = 0) -> Tuple[List[Dict[str, Any]], int]:
        """
        搜索投诉
        
        Args:
            keywords: 关键词
            organization_id: 组织ID
            category: 分类
            status: 状态
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            limit: 返回数量限制
            offset: 偏移量
            
        Returns:
            投诉列表和总数
        """
        # 构建查询条件
        conditions = []
        params = []
        
        if keywords:
            conditions.append("to_tsvector('simple', COALESCE(title, '') || ' ' || COALESCE(content, '')) @@ plainto_tsquery('simple', %s)")
            params.append(keywords)
        
        if organization_id:
            conditions.append("organization_id = %s")
            params.append(organization_id)
        
        if category:
            conditions.append("category = %s")
            params.append(category)
        
        if status:
            conditions.append("status = %s")
            params.append(status)
        
        if start_date:
            conditions.append("created_at >= %s")
            params.append(start_date)
        
        if end_date:
            conditions.append("created_at <= %s")
            params.append(end_date + " 23:59:59")
        
        # 构建WHERE子句
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # 查询总数
        count_query = f"SELECT COUNT(*) FROM complaints WHERE {where_clause}"
        count_result = self.db_manager.query_one(count_query, tuple(params))
        total_count = count_result[0] if count_result else 0
        
        # 查询数据
        query = f"""
        SELECT 
            id, complaint_id, title, content, created_at, updated_at,
            status, reply_status, organization_id, organization_name,
            category, source
        FROM complaints
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """
        
        params.extend([limit, offset])
        results = self.db_manager.query(query, tuple(params))
        
        # 转换为字典列表
        columns = [
            'id', 'complaint_id', 'title', 'content', 'created_at', 'updated_at',
            'status', 'reply_status', 'organization_id', 'organization_name',
            'category', 'source'
        ]
        
        complaints = [dict(zip(columns, row)) for row in results]
        
        return complaints, total_count
    
    def get_complaint_stats(self, 
                           days: int = 30,
                           organization_id: Optional[int] = None) -> Dict[str, Any]:
        """
        获取投诉统计数据
        
        Args:
            days: 统计天数
            organization_id: 组织ID
            
        Returns:
            统计数据
        """
        # 计算开始日期
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # 构建查询条件
        conditions = ["created_at >= %s AND created_at <= %s"]
        params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
        
        if organization_id:
            conditions.append("organization_id = %s")
            params.append(organization_id)
        
        where_clause = " AND ".join(conditions)
        
        # 总投诉数
        total_query = f"SELECT COUNT(*) FROM complaints WHERE {where_clause}"
        total_result = self.db_manager.query_one(total_query, tuple(params))
        total_count = total_result[0] if total_result else 0
        
        # 按状态统计
        status_query = f"""
        SELECT status, COUNT(*) 
        FROM complaints 
        WHERE {where_clause}
        GROUP BY status
        """
        status_results = self.db_manager.query(status_query, tuple(params))
        status_stats = {row[0]: row[1] for row in status_results}
        
        # 按分类统计
        category_query = f"""
        SELECT category, COUNT(*) 
        FROM complaints 
        WHERE {where_clause}
        GROUP BY category
        ORDER BY COUNT(*) DESC
        """
        category_results = self.db_manager.query(category_query, tuple(params))
        category_stats = {row[0]: row[1] for row in category_results}
        
        # 按日期统计
        date_query = f"""
        SELECT DATE(created_at), COUNT(*) 
        FROM complaints 
        WHERE {where_clause}
        GROUP BY DATE(created_at)
        ORDER BY DATE(created_at)
        """
        date_results = self.db_manager.query(date_query, tuple(params))
        date_stats = {row[0].strftime('%Y-%m-%d'): row[1] for row in date_results}
        
        # 返回统计结果
        return {
            'total_count': total_count,
            'status_stats': status_stats,
            'category_stats': category_stats,
            'date_stats': date_stats,
            'period': {
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'days': days
            }
        }
    
    def get_organization_performance(self, 
                                    days: int = 30,
                                    limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取组织处理投诉的表现
        
        Args:
            days: 统计天数
            limit: 返回数量限制
            
        Returns:
            组织表现列表
        """
        # 计算开始日期
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        query = """
        SELECT 
            organization_id,
            organization_name,
            COUNT(*) as total_complaints,
            AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600) as avg_response_hours,
            SUM(CASE WHEN status = 'HANDLING' THEN 1 ELSE 0 END) as handling_count,
            SUM(CASE WHEN status = 'DONE' THEN 1 ELSE 0 END) as done_count,
            SUM(CASE WHEN reply_status = 'REPLIED' THEN 1 ELSE 0 END) as replied_count
        FROM complaints
        WHERE created_at >= %s AND created_at <= %s
        GROUP BY organization_id, organization_name
        ORDER BY total_complaints DESC
        LIMIT %s
        """
        
        params = (
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            limit
        )
        
        results = self.db_manager.query(query, params)
        
        # 转换为字典列表
        columns = [
            'organization_id', 'organization_name', 'total_complaints',
            'avg_response_hours', 'handling_count', 'done_count', 'replied_count'
        ]
        
        performance = []
        for row in results:
            org_data = dict(zip(columns, row))
            
            # 计算回复率
            total = org_data['total_complaints']
            replied = org_data['replied_count']
            org_data['reply_rate'] = round(replied / total * 100, 2) if total > 0 else 0
            
            # 计算完成率
            done = org_data['done_count']
            org_data['completion_rate'] = round(done / total * 100, 2) if total > 0 else 0
            
            performance.append(org_data)
        
        return performance
    
    def get_categories(self) -> List[str]:
        """
        获取所有投诉分类
        
        Returns:
            分类列表
        """
        query = """
        SELECT DISTINCT category
        FROM complaints
        ORDER BY category
        """
        
        results = self.db_manager.query(query)
        return [row[0] for row in results]
    
    def get_organizations_with_complaints(self) -> List[Dict[str, Any]]:
        """
        获取有投诉的组织列表
        
        Returns:
            组织列表
        """
        query = """
        SELECT DISTINCT organization_id, organization_name, COUNT(*) as complaint_count
        FROM complaints
        GROUP BY organization_id, organization_name
        ORDER BY complaint_count DESC
        """
        
        results = self.db_manager.query(query)
        
        # 转换为字典列表
        return [
            {
                'organization_id': row[0],
                'organization_name': row[1],
                'complaint_count': row[2]
            }
            for row in results
        ]

def main():
    """主函数"""
    import argparse
    import json
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='查询投诉数据')
    parser.add_argument('--id', help='投诉ID')
    parser.add_argument('--search', help='搜索关键词')
    parser.add_argument('--org', type=int, help='组织ID')
    parser.add_argument('--category', help='分类')
    parser.add_argument('--stats', action='store_true', help='获取统计数据')
    parser.add_argument('--days', type=int, default=30, help='统计天数')
    parser.add_argument('--performance', action='store_true', help='获取组织表现')
    parser.add_argument('--limit', type=int, default=10, help='返回数量限制')
    args = parser.parse_args()
    
    # 加载配置
    db_config = config.get('database', 'postgres')
    
    if not db_config:
        logger.error("无法加载数据库配置")
        sys.exit(1)
    
    # 初始化数据库连接
    db_manager = DBManager(db_config)
    
    # 初始化查询类
    query = ComplaintQuery(db_manager)
    
    try:
        # 执行查询
        if args.id:
            # 查询单个投诉
            result = query.get_complaint_by_id(args.id)
            if result:
                print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
            else:
                print(f"未找到ID为 {args.id} 的投诉")
                
        elif args.search or args.org or args.category:
            # 搜索投诉
            complaints, total = query.search_complaints(
                keywords=args.search,
                organization_id=args.org,
                category=args.category,
                limit=args.limit
            )
            
            print(f"找到 {total} 条投诉，显示前 {len(complaints)} 条:")
            print(json.dumps(complaints, ensure_ascii=False, indent=2, default=str))
            
        elif args.stats:
            # 获取统计数据
            stats = query.get_complaint_stats(days=args.days, organization_id=args.org)
            print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
            
        elif args.performance:
            # 获取组织表现
            performance = query.get_organization_performance(days=args.days, limit=args.limit)
            print(json.dumps(performance, ensure_ascii=False, indent=2, default=str))
            
        else:
            # 默认显示分类和组织
            categories = query.get_categories()
            print("投诉分类:")
            for category in categories:
                print(f"- {category}")
                
            print("\n有投诉的组织:")
            organizations = query.get_organizations_with_complaints()
            for org in organizations[:10]:  # 只显示前10个
                print(f"- {org['organization_name']} (ID: {org['organization_id']}): {org['complaint_count']} 条投诉")
            
            if len(organizations) > 10:
                print(f"... 共 {len(organizations)} 个组织")
    
    finally:
        # 关闭数据库连接
        db_manager.close()

if __name__ == "__main__":
    main() 