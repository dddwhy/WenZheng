"""
组织机构查询服务
"""
import json
from src.db.db_manager import DBManager
from src.utils.logger import setup_logger

logger = setup_logger('org_query')

class OrganizationQuery:
    """组织机构查询服务"""
    
    def __init__(self, db_config=None):
        """
        初始化查询服务
        
        Args:
            db_config: 数据库连接配置
        """
        self.db_manager = DBManager(db_config)
    
    def get_organization_by_id(self, org_id):
        """
        根据ID获取组织机构
        
        Args:
            org_id: 组织机构ID
            
        Returns:
            组织机构信息dict，若未找到返回None
        """
        if not self.db_manager.connect():
            logger.error("连接数据库失败")
            return None
        
        try:
            sql = """
            SELECT id, org_id, name, parent_id, path::text, type, ext, 
                   has_children, level, created_at, updated_at
            FROM organizations
            WHERE org_id = %s
            """
            
            result = self.db_manager.query_one(sql, (org_id,))
            if result:
                # 转换为dict并处理ext字段
                org = dict(result)
                if org.get('ext'):
                    org['ext'] = json.loads(org['ext'])
                return org
            return None
            
        except Exception as e:
            logger.error(f"查询组织机构出错: {e}")
            return None
        finally:
            self.db_manager.close()
    
    def get_children(self, parent_id=None, level=None):
        """
        获取子组织机构
        
        Args:
            parent_id: 父级组织ID，若为None则获取顶级组织
            level: 指定层级，若为None则不限制层级
            
        Returns:
            组织机构列表
        """
        if not self.db_manager.connect():
            logger.error("连接数据库失败")
            return []
        
        try:
            conditions = []
            params = []
            
            if parent_id is not None:
                conditions.append("parent_id = %s")
                params.append(parent_id)
            else:
                conditions.append("parent_id IS NULL")
            
            if level is not None:
                conditions.append("level = %s")
                params.append(level)
            
            where_clause = " AND ".join(conditions)
            
            sql = f"""
            SELECT id, org_id, name, parent_id, path::text, type, ext, 
                   has_children, level, created_at, updated_at
            FROM organizations
            WHERE {where_clause}
            ORDER BY name
            """
            
            results = self.db_manager.query(sql, tuple(params))
            
            # 转换结果并处理ext字段
            orgs = []
            for result in results:
                org = dict(result)
                if org.get('ext'):
                    org['ext'] = json.loads(org['ext'])
                orgs.append(org)
            
            return orgs
            
        except Exception as e:
            logger.error(f"查询子组织机构出错: {e}")
            return []
        finally:
            self.db_manager.close()
    
    def search_organizations(self, query, limit=20):
        """
        搜索组织机构
        
        Args:
            query: 搜索关键词
            limit: 返回结果的最大数量
            
        Returns:
            匹配的组织机构列表
        """
        if not self.db_manager.connect():
            logger.error("连接数据库失败")
            return []
        
        try:
            # 模糊搜索
            search_query = f"%{query}%"
            
            sql = """
            SELECT id, org_id, name, parent_id, path::text, type, ext, 
                   has_children, level, created_at, updated_at
            FROM organizations
            WHERE name ILIKE %s
            ORDER BY level, name
            LIMIT %s
            """
            
            results = self.db_manager.query(sql, (search_query, limit))
            
            # 转换结果并处理ext字段
            orgs = []
            for result in results:
                org = dict(result)
                if org.get('ext'):
                    org['ext'] = json.loads(org['ext'])
                orgs.append(org)
            
            return orgs
            
        except Exception as e:
            logger.error(f"搜索组织机构出错: {e}")
            return []
        finally:
            self.db_manager.close()
    
    def get_organization_tree(self, start_org_id=None, max_depth=None):
        """
        获取组织机构树
        
        Args:
            start_org_id: 起始组织ID，若为None则从顶级开始
            max_depth: 最大深度，若为None则不限制深度
            
        Returns:
            组织机构树
        """
        if not self.db_manager.connect():
            logger.error("连接数据库失败")
            return None
        
        try:
            # 查询起始节点
            if start_org_id:
                start_org = self.get_organization_by_id(start_org_id)
                if not start_org:
                    logger.error(f"找不到起始组织: {start_org_id}")
                    return None
                
                start_path = start_org['path']
                start_level = start_org['level']
            else:
                # 从顶级开始
                start_path = ''
                start_level = 1
            
            # 构建查询条件
            conditions = []
            params = []
            
            if start_org_id:
                # 查询自身和所有子节点
                conditions.append("path <@ %s")
                params.append(start_path)
            else:
                # 从顶级开始
                conditions.append("level >= 1")
            
            if max_depth and start_level:
                # 限制深度
                max_level = start_level + max_depth - 1 if max_depth else None
                if max_level:
                    conditions.append("level <= %s")
                    params.append(max_level)
            
            where_clause = " AND ".join(conditions)
            
            # 查询所有符合条件的节点
            sql = f"""
            SELECT id, org_id, name, parent_id, path::text, type, ext, 
                   has_children, level, created_at, updated_at
            FROM organizations
            WHERE {where_clause}
            ORDER BY path
            """
            
            logger.info(f"执行树查询SQL: {sql}, 参数: {params}")
            results = self.db_manager.query(sql, tuple(params))
            
            if not results:
                logger.warning("未找到符合条件的组织机构")
                return None
                
            # 构建树结构
            org_map = {}
            root = None
            
            for result in results:
                org = dict(result)
                if org.get('ext'):
                    org['ext'] = json.loads(org['ext'])
                
                # 添加children字段
                org['children'] = []
                
                # 存入映射
                org_id = org['org_id']
                org_map[org_id] = org
                
                # 如果是起始节点，设为根节点
                if start_org_id and org_id == start_org_id:
                    root = org
            
            # 构建树
            for org_id, org in org_map.items():
                parent_id = org['parent_id']
                if parent_id and parent_id in org_map and org_id != parent_id:
                    # 将当前节点添加到父节点的children中
                    org_map[parent_id]['children'].append(org)
            
            # 确定返回的根节点
            if start_org_id:
                # 返回指定的起始节点
                return root
            else:
                # 返回所有顶级节点
                top_orgs = [org for org_id, org in org_map.items() 
                        if not org['parent_id'] or org['parent_id'] not in org_map]
                return top_orgs if top_orgs else list(org_map.values())
                
        except Exception as e:
            logger.error(f"获取组织机构树出错: {e}")
            return None
        finally:
            self.db_manager.close()
            
    def get_statistics(self):
        """
        获取组织机构统计信息
        
        Returns:
            统计信息dict
        """
        if not self.db_manager.connect():
            logger.error("连接数据库失败")
            return {}
        
        try:
            stats = {}
            
            # 总数统计
            sql_total = "SELECT COUNT(*) FROM organizations"
            result = self.db_manager.query_one(sql_total)
            stats['total'] = result[0] if result else 0
            
            # 按级别统计
            sql_by_level = """
            SELECT level, COUNT(*) 
            FROM organizations 
            GROUP BY level 
            ORDER BY level
            """
            results = self.db_manager.query(sql_by_level)
            stats['by_level'] = {r[0]: r[1] for r in results}
            
            # 按类型统计
            sql_by_type = """
            SELECT type, COUNT(*) 
            FROM organizations 
            GROUP BY type 
            ORDER BY COUNT(*) DESC
            """
            results = self.db_manager.query(sql_by_type)
            stats['by_type'] = {r[0]: r[1] for r in results}
            
            return stats
            
        except Exception as e:
            logger.error(f"获取统计信息出错: {e}")
            return {}
        finally:
            self.db_manager.close() 