"""
测试投诉数据处理器
"""
import os
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.insert(0, str(project_root))

import aiohttp
import asyncio
from src.data.complaint_processor_improved import ComplaintProcessorImproved

async def process_organization_complaints(
    processor: ComplaintProcessorImproved,
    organization_id: int,
    page_size: int = 20,
    debug: bool = False
) -> Dict[str, Any]:
    """
    处理单个组织的投诉数据
    
    Args:
        processor: 投诉处理器实例
        organization_id: 组织ID
        page_size: 每页数据量
        debug: 是否为调试模式
        
    Returns:
        处理结果统计
    """
    base_url = "https://wz-api.chuanbaoguancha.cn/api/v1"
    endpoint = "/thread/page"
    url = f"{base_url}{endpoint}"
    
    page = 1
    total_processed = 0
    successful_count = 0
    failed_count = 0
    
    try:
        while True:
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
            
            # 发送请求获取数据
            print(f"\n正在处理组织 {organization_id} 的第 {page} 页数据...")
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as response:
                    if response.status != 200:
                        print(f"请求失败: {response.status} {response.reason}")
                        break
                    
                    data = await response.json()
            
            # 提取记录
            records = processor._extract_complaint_records(data)
            if not records:
                print("未找到更多记录")
                break
            
            # 处理本页数据
            page_total = len(records)
            page_success = 0
            
            print(f"开始处理第 {page} 页的 {page_total} 条记录:")
            for i, record in enumerate(records, 1):
                # 显示处理进度
                print(f"\n处理第 {i}/{page_total} 条记录:")
                print(f"ID: {record.get('id')}")
                print(f"标题: {record.get('title')}")
                
                # 处理记录
                success = processor._process_complaint_record(record)
                if success:
                    page_success += 1
                    successful_count += 1
                else:
                    failed_count += 1
                
                total_processed += 1
            
            print(f"\n第 {page} 页处理完成: 成功 {page_success}/{page_total} 条")
            
            # 检查是否还有下一页
            total_records = data.get('data', {}).get('total', 0)
            if page * page_size >= total_records:
                print("已处理所有页面")
                break
            
            page += 1
            # 控制请求频率
            await asyncio.sleep(1)
        
        return {
            'status': 'success',
            'organization_id': organization_id,
            'total_processed': total_processed,
            'successful': successful_count,
            'failed': failed_count,
            'pages_processed': page
        }
        
    except Exception as e:
        print(f"处理组织 {organization_id} 时出错: {e}")
        return {
            'status': 'error',
            'organization_id': organization_id,
            'error': str(e),
            'total_processed': total_processed,
            'successful': successful_count,
            'failed': failed_count,
            'pages_processed': page - 1
        }

async def process_multiple_organizations(org_ids: List[int], page_size: int = 200, debug: bool = False):
    """
    批量处理多个组织的投诉数据
    
    Args:
        org_ids: 组织ID列表
        page_size: 每页数据量,默认200条
        debug: 是否为调试模式
    """
    # 创建处理器实例 - 关闭强制模式，只处理新数据
    processor = ComplaintProcessorImproved(force_reprocess=False)
    print("\n数据处理配置:")
    print("- 强制模式: 关闭（将跳过已存在的投诉）")
    print("- 每页数量: ", page_size, "条")
    print("- 预计请求次数: 约", 10000 // page_size + 1, "次")
    print("- 调试模式: ", "开启" if debug else "关闭")
    print("\n开始处理数据...")
    
    # 处理每个组织
    results = []
    for org_id in org_ids:
        result = await process_organization_complaints(processor, org_id, page_size, debug)
        results.append(result)
        
        # 显示处理结果
        if result['status'] == 'success':
            print(f"\n组织 {org_id} 处理完成:")
            print(f"总处理数: {result['total_processed']}")
            print(f"成功数: {result['successful']}")
            print(f"失败数: {result['failed']}")
            print(f"处理页数: {result['pages_processed']}")
        else:
            print(f"\n组织 {org_id} 处理失败:")
            print(f"错误: {result['error']}")
            print(f"已处理数: {result['total_processed']}")
    
    return results

async def main():
    """主函数"""
    # 测试组织ID列表
    org_ids = [1779,1778]  # 可以添加更多组织ID
    
    # 设置每页数据量为200条
    page_size = 200  # 权衡效率和稳定性的最佳值
    
    # 执行批量处理
    results = await process_multiple_organizations(org_ids, page_size)
    
    # 显示总体统计
    total_processed = sum(r['total_processed'] for r in results)
    total_successful = sum(r['successful'] for r in results)
    total_failed = sum(r['failed'] for r in results)
    
    print("\n处理总结:")
    print(f"处理组织数: {len(org_ids)}")
    print(f"总处理记录数: {total_processed}")
    print(f"总成功数: {total_successful}")
    print(f"总失败数: {total_failed}")

if __name__ == "__main__":
    asyncio.run(main()) 