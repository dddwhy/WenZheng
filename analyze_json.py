import json
import pprint

print("开始分析JSON文件...")
file_path = 'data/complaints/complaints_4756_p1_20250303_105913.json'

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"文件类型: {type(data)}")
    
    if isinstance(data, dict):
        print(f"顶层键: {list(data.keys())}")
        
        if 'code' in data:
            print(f"code值: {data['code']}")
        
        if 'data' in data:
            print(f"data类型: {type(data['data'])}")
            
            if isinstance(data['data'], dict):
                print(f"data键: {list(data['data'].keys())}")
                
                # 检查data.data字段
                if 'data' in data['data']:
                    inner_data = data['data']['data']
                    print(f"\ndata.data类型: {type(inner_data)}")
                    
                    if isinstance(inner_data, list):
                        print(f"data.data列表长度: {len(inner_data)}")
                        
                        if len(inner_data) > 0:
                            first_item = inner_data[0]
                            print(f"第一条记录类型: {type(first_item)}")
                            
                            if isinstance(first_item, dict):
                                print(f"第一条记录键: {list(first_item.keys())}")
                                print("\n第一条记录值示例:")
                                
                                # 打印第一条记录的部分关键字段
                                important_fields = ['id', 'title', 'content', 'created_at', 'status']
                                for field in important_fields:
                                    if field in first_item:
                                        value = first_item[field]
                                        if isinstance(value, str) and len(value) > 100:
                                            print(f"{field}: {value[:100]}... (截断)")
                                        else:
                                            print(f"{field}: {value}")
                                
                                # 打印所有字段及其类型
                                print("\n所有字段及类型:")
                                for key, value in first_item.items():
                                    value_type = type(value).__name__
                                    value_preview = str(value)[:50] + "..." if isinstance(value, str) and len(str(value)) > 50 else value
                                    print(f"{key} ({value_type}): {value_preview}")
            elif isinstance(data['data'], list):
                print(f"data是列表，长度: {len(data['data'])}")
                if len(data['data']) > 0:
                    print(f"第一个元素类型: {type(data['data'][0])}")
                    if isinstance(data['data'][0], dict):
                        print(f"第一个元素键: {list(data['data'][0].keys())}")
    else:
        print("JSON不是字典格式")
    
except Exception as e:
    print(f"分析时出错: {str(e)}") 