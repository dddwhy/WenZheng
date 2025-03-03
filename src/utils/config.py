import yaml
import os
from pathlib import Path

class Config:
    def __init__(self, config_file=None):
        if not config_file:
            # 默认配置文件路径
            config_file = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
        
        self.config_file = config_file
        self.config_data = {}
        self.reload()
    
    def reload(self):
        """重新加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self.config_data = yaml.safe_load(f)
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            self.config_data = {}
    
    def get(self, section, key=None):
        """
        获取配置项
        
        Args:
            section: 配置节
            key: 配置键，为None时返回整个节
            
        Returns:
            配置值或整个配置节
        """
        if section not in self.config_data:
            return {} if key is None else None
            
        if key is None:
            return self.config_data.get(section, {})
            
        return self.config_data.get(section, {}).get(key)
        
# 创建一个全局配置实例
config = Config() 