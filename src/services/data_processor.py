"""
数据处理与存储模块
"""
import json
import logging
from psycopg2.extras import Json
from datetime import datetime
from pathlib import Path
from src.utils.config import config
from src.db.db_manager import DBManager
from src.utils.logger import setup_logger

logger = setup_logger('data_processor')

class OrganizationDataProcessor:
    """组织机构数据处理器"""

    def __init__(self, db_config=None):
        """
        初始化数据处理器
        
        Args:
            db_config: 数据库连接配置
        """
        self.db_manager = DBManager(db_config)
    
    def process_file(self, file_path):
        """
        处理指定的JSON文件，将数据存储到数据库
        
        Args:
            file_path: JSON文件路径
        
        Returns:
            成功处理的组织机构数量
        """
        try:
            # 加载JSON文件
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if data.get('code') != 0:
                logger.error(f"数据文件 {file_path} 的状态码不为0: {data.get('code')}")
                return 0
            
            # 提取数据部分
            org_data = data.get('data', {})
            if not org_data:
                logger.warning(f"数据文件 {file_path} 不包含有效的组织机构数据")
                return 0
            
            # 连接数据库
            if not self.db_manager.connect():
                return 0
            
            # 处理组织机构数据
            count = self._process_organization_node(org_data)
            
            logger.info(f"成功处理文件 {file_path}，共处理 {count} 个组织机构")
            return count
            
        except Exception as e:
            logger.error(f"处理数据文件 {file_path} 时发生错误: {e}")
            return 0
        finally:
            self.db_manager.close()
    
    def _process_organization_node(self, node, parent_id=None, parent_path=None, level=1):
        """
        递归处理组织机构节点及其子节点
        
        Args:
            node: 组织机构节点数据
            parent_id: 父节点ID
            parent_path: 父节点路径
            level: 当前层级
        
        Returns:
            处理的节点总数
        """
        if not node:
            return 0
        
        count = 0
        
        try:
            org_id = node.get('id')
            name = node.get('name')
            org_type = node.get('type')
            has_children = node.get('has_children', False)
            
            # 跳过无效节点
            if not org_id or not name:
                logger.warning(f"跳过无效节点: {node}")
                return count
            
            # 构建ltree路径
            current_path = f"{org_id}"
            if parent_path:
                current_path = f"{parent_path}.{current_path}"
            
            # 提取扩展信息
            ext_keys = ['originId', 'pid', 'mayor', 'link', 'created_at', 'updated_at']
            ext = {k: node.get(k) for k in ext_keys if k in node}
            
            # 插入或更新组织机构数据
            sql = """
            INSERT INTO organizations 
                (org_id, name, parent_id, path, "type", ext, has_children, level)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (org_id) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                parent_id = EXCLUDED.parent_id,
                path = EXCLUDED.path,
                "type" = EXCLUDED."type",
                ext = EXCLUDED.ext,
                has_children = EXCLUDED.has_children,
                level = EXCLUDED.level,
                updated_at = CURRENT_TIMESTAMP
            """
            
            success = self.db_manager.execute(sql, (
                org_id, 
                name,
                parent_id,
                current_path,
                org_type,
                Json(ext) if ext else None,
                has_children,
                level
            ))
            
            if success:
                count += 1
            
            # 递归处理子节点
            children = node.get('children', [])
            if children:
                for child in children:
                    count += self._process_organization_node(
                        child, 
                        parent_id=org_id, 
                        parent_path=current_path,
                        level=level + 1
                    )
            
            return count
            
        except Exception as e:
            logger.error(f"处理组织机构节点时发生错误: {e}")
            return count
    
    def process_directory(self, directory_path):
        """
        处理指定目录下的所有JSON文件
        
        Args:
            directory_path: 目录路径
        
        Returns:
            成功处理的文件数量和组织机构数量
        """
        directory = Path(directory_path)
        if not directory.exists() or not directory.is_dir():
            logger.error(f"目录 {directory_path} 不存在或不是一个目录")
            return 0, 0
        
        file_count = 0
        org_count = 0
        
        try:
            # 处理所有JSON文件
            for file_path in directory.glob('*.json'):
                logger.info(f"正在处理文件: {file_path}")
                count = self.process_file(file_path)
                if count > 0:
                    file_count += 1
                    org_count += count
            
            logger.info(f"共处理 {file_count} 个文件，{org_count} 个组织机构")
            return file_count, org_count
            
        except Exception as e:
            logger.error(f"处理目录 {directory_path} 时发生错误: {e}")
            return file_count, org_count 