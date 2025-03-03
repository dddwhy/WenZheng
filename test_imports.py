"""
测试模块导入是否正常
"""
print("开始测试导入...")

try:
    from src.utils.config import config
    print("✓ 成功导入 config 模块")
except Exception as e:
    print(f"✗ 导入 config 模块失败: {e}")

try:
    from src.utils.logger import setup_logger
    print("✓ 成功导入 logger 模块")
except Exception as e:
    print(f"✗ 导入 logger 模块失败: {e}")

try:
    from src.crawlers.base_crawler import BaseCrawler
    print("✓ 成功导入 base_crawler 模块")
except Exception as e:
    print(f"✗ 导入 base_crawler 模块失败: {e}")

try:
    from src.crawlers.organization_crawler import OrganizationCrawler
    print("✓ 成功导入 organization_crawler 模块")
except Exception as e:
    print(f"✗ 导入 organization_crawler 模块失败: {e}")

print("导入测试完成") 