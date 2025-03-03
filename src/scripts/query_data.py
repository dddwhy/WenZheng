"""
数据查询脚本 - 查询组织机构数据
"""
import os
import sys
import argparse
import json
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.services.org_query import OrganizationQuery
from src.utils.logger import setup_logger

logger = setup_logger('query_data')

def query_by_id(org_id):
    """
    根据ID查询组织机构
    
    Args:
        org_id: 组织机构ID
    """
    query = OrganizationQuery()
    org = query.get_organization_by_id(org_id)
    
    if org:
        print(f"找到组织机构: {org['name']} (ID: {org['org_id']})")
        print(f"类型: {org['type']}")
        print(f"路径: {org['path']}")
        print(f"父级: {org['parent_id']}")
        print(f"是否有子机构: {org['has_children']}")
        print(f"层级: {org['level']}")
        if org.get('ext'):
            print("扩展信息:")
            for k, v in org['ext'].items():
                print(f"  {k}: {v}")
    else:
        print(f"未找到ID为 {org_id} 的组织机构")

def query_children(parent_id=None, level=None):
    """
    查询子组织机构
    
    Args:
        parent_id: 父级组织ID，为None则查询顶级组织
        level: 指定层级
    """
    query = OrganizationQuery()
    orgs = query.get_children(parent_id, level)
    
    if orgs:
        if parent_id:
            print(f"ID为 {parent_id} 的组织机构的子机构，共 {len(orgs)} 个:")
        else:
            print(f"顶级组织机构，共 {len(orgs)} 个:")
        
        for i, org in enumerate(orgs, 1):
            print(f"{i}. {org['name']} (ID: {org['org_id']}, 类型: {org['type']})")
    else:
        if parent_id:
            print(f"ID为 {parent_id} 的组织机构没有子机构")
        else:
            print("没有找到顶级组织机构")

def search_orgs(query_str, limit=20):
    """
    搜索组织机构
    
    Args:
        query_str: 搜索关键词
        limit: 返回结果限制
    """
    query = OrganizationQuery()
    orgs = query.search_organizations(query_str, limit)
    
    if orgs:
        print(f"搜索 '{query_str}' 的结果，共 {len(orgs)} 个:")
        for i, org in enumerate(orgs, 1):
            print(f"{i}. {org['name']} (ID: {org['org_id']}, 类型: {org['type']})")
    else:
        print(f"没有找到匹配 '{query_str}' 的组织机构")

def get_statistics():
    """获取组织机构统计信息"""
    query = OrganizationQuery()
    stats = query.get_statistics()
    
    if stats:
        print("组织机构统计信息:")
        print(f"总数: {stats.get('total', 0)}")
        
        if stats.get('by_level'):
            print("\n按层级统计:")
            for level, count in stats['by_level'].items():
                print(f"  层级 {level}: {count} 个")
        
        if stats.get('by_type'):
            print("\n按类型统计:")
            for type_name, count in stats['by_type'].items():
                print(f"  类型 {type_name or '未知'}: {count} 个")
    else:
        print("获取统计信息失败")

def get_tree(start_org_id=None, max_depth=None):
    """
    获取组织机构树
    
    Args:
        start_org_id: 起始组织ID
        max_depth: 最大深度
    """
    query = OrganizationQuery()
    tree = query.get_organization_tree(start_org_id, max_depth)
    
    if tree:
        if isinstance(tree, list):
            print(f"顶级组织机构树，共 {len(tree)} 个根节点")
            for i, root in enumerate(tree, 1):
                print(f"{i}. {root['name']} (ID: {root['org_id']})")
                print_tree(root, 1)
        else:
            print(f"组织机构树，根节点: {tree['name']} (ID: {tree['org_id']})")
            print_tree(tree, 0)
    else:
        print("获取组织机构树失败")

def print_tree(node, level):
    """
    打印树形结构
    
    Args:
        node: 节点
        level: 当前层级（用于缩进）
    """
    if node.get('children'):
        for child in node['children']:
            print(f"{'  ' * (level + 1)}├─ {child['name']} (ID: {child['org_id']})")
            print_tree(child, level + 1)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='组织机构数据查询工具')
    parser.add_argument('--id', type=str, help='根据ID查询组织机构')
    parser.add_argument('--top', action='store_true', help='查询顶级组织机构')
    parser.add_argument('--children', type=str, help='查询指定ID的子组织机构')
    parser.add_argument('--search', type=str, help='搜索组织机构')
    parser.add_argument('--stats', action='store_true', help='获取统计信息')
    parser.add_argument('--tree', action='store_true', help='获取顶级组织机构树')
    parser.add_argument('--node-tree', type=str, help='获取指定ID的组织机构树')
    parser.add_argument('--depth', type=int, help='树的最大深度（与--tree或--node-tree一起使用）')
    parser.add_argument('--level', type=int, help='指定层级（与--top或--children一起使用）')
    parser.add_argument('--limit', type=int, default=20, help='结果数量限制（与--search一起使用）')
    
    args = parser.parse_args()
    
    # 没有参数时显示帮助
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    if args.id:
        query_by_id(args.id)
    elif args.top:
        query_children(None, args.level)
    elif args.children:
        query_children(args.children, args.level)
    elif args.search:
        search_orgs(args.search, args.limit)
    elif args.stats:
        get_statistics()
    elif args.tree:
        get_tree(None, args.depth)
    elif args.node_tree:
        get_tree(args.node_tree, args.depth)
    else:
        print("错误: 必须指定查询参数")
        parser.print_help()

if __name__ == "__main__":
    main() 