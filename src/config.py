import os
import configparser
from typing import Dict, Any
from dotenv import load_dotenv

class Config:
    """
    配置加载器，负责从环境变量、config.ini文件和默认值中读取配置。
    优先级：环境变量 > config.ini配置 > 默认值
    """
    def __init__(self):
        # 在本地开发环境中，可以加载.env文件
        load_dotenv()
        
        # 读取config.ini文件，使用绝对路径
        self.config_parser = configparser.ConfigParser()
        self.config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini')
        
        # 如果config.ini文件存在，则读取
        if os.path.exists(self.config_file):
            try:
                self.config_parser.read(self.config_file, encoding='utf-8')
            except (configparser.Error, UnicodeDecodeError):
                pass
    
    def _get_config_value(self, section: str, key: str, env_var: str, default_value: Any, value_type=str) -> Any:
        """
        按优先级获取配置值：环境变量 > config.ini > 默认值
        
        Args:
            section: config.ini中的section名称
            key: config.ini中的key名称
            env_var: 环境变量名称
            default_value: 默认值
            value_type: 值类型转换函数
            
        Returns:
            配置值
        """
        # 1. 优先检查环境变量
        env_value = os.getenv(env_var)
        if env_value is not None:
            try:
                return value_type(env_value)
            except (ValueError, TypeError):
                return default_value
        
        # 2. 检查config.ini文件
        try:
            if self.config_parser.has_section(section) and self.config_parser.has_option(section, key):
                config_value = self.config_parser.get(section, key)
                try:
                    return value_type(config_value)
                except (ValueError, TypeError):
                    return default_value
        except (configparser.Error, UnicodeDecodeError):
            pass
        
        # 3. 返回默认值
        return default_value

    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置，优先级：环境变量 > config.ini > 默认值。"""
        config = {
            'host': self._get_config_value('database', 'host', 'DB_HOST', None),
            'user': self._get_config_value('database', 'user', 'DB_USER', None),
            'password': self._get_config_value('database', 'password', 'DB_PASSWORD', None),
            'database': self._get_config_value('database', 'database', 'DB_NAME', None),
            'port': self._get_config_value('database', 'port', 'DB_PORT', 3306, int),
            'ssl_mode': self._get_config_value('database', 'ssl_mode', 'DB_SSL_MODE', 'disabled')
        }
        if not all([config['host'], config['user'], config['password'], config['database']]):
            raise ValueError("数据库核心配置 (host, user, password, database) 必须在环境变量或config.ini中设置。")
        return config

    def get_crawler_config(self) -> Dict[str, Any]:
        """获取爬虫配置，优先级：环境变量 > config.ini > 默认值。"""
        return {
            # General settings
            'scan_pages': self._get_config_value('crawler', 'scan_pages', 'CRAWLER_SCAN_PAGES', 4, int),
            'delay_seconds': self._get_config_value('crawler', 'delay_seconds', 'CRAWLER_DELAY_SECONDS', 2.0, float),
            'max_retries': self._get_config_value('crawler', 'max_retries', 'CRAWLER_MAX_RETRIES', 4, int),
            'timeout_seconds': self._get_config_value('crawler', 'timeout_seconds', 'CRAWLER_TIMEOUT_SECONDS', 30, int),

            # Concurrency settings
            'max_concurrent_boards': self._get_config_value('crawler', 'max_concurrent_boards', 'CRAWLER_MAX_CONCURRENT_BOARDS', 3, int),
            'max_concurrent_pages': self._get_config_value('crawler', 'max_concurrent_pages', 'CRAWLER_MAX_CONCURRENT_PAGES', 5, int),
            'max_concurrent_details': self._get_config_value('crawler', 'max_concurrent_details', 'CRAWLER_MAX_CONCURRENT_DETAILS', 8, int)
        }

    def get_data_retention_days(self) -> int:
        """获取数据保留天数，优先级：环境变量 > config.ini > 默认值。"""
        return self._get_config_value('data_retention', 'days', 'DATA_RETENTION_DAYS', 120, int)

    def get_logging_config(self) -> Dict[str, str]:
        """获取日志配置，优先级：环境变量 > config.ini > 默认值。"""
        return {
            'log_level': self._get_config_value('logging', 'log_level', 'LOGGING_LOG_LEVEL', 'INFO'),
            'log_file': self._get_config_value('logging', 'log_file', 'LOGGING_LOG_FILE', 'crawler.log')
        }

    def get_target_urls(self) -> Dict[str, str]:
        """
        获取目标板块URL，优先级：环境变量 > config.ini > 默认值。
        环境变量格式: TARGETS="name1=url1;name2=url2"
        config.ini格式: 在[targets] section中配置
        """
        # 1. 优先从环境变量读取
        env_targets_str = os.getenv('TARGETS')
        if env_targets_str:
            targets = self._parse_targets_string(env_targets_str, "TARGETS环境变量")
            if targets:
                return targets
        
        # 2. 从config.ini文件读取
        targets = self._parse_targets_from_config()
        if targets:
            return targets
        
        # 3. 使用默认值
        default_targets = {
            '人工智能': 'https://linux.do/tag/%E4%BA%BA%E5%B7%A5%E6%99%BA%E8%83%BD',
            #'开发调优': 'https://linux.do/c/develop/4',
            #'前沿快讯': 'https://linux.do/c/news/34',
            #'福利羊毛': 'https://linux.do/c/welfare/36'
        }
        return default_targets
    
    def _parse_targets_string(self, targets_str: str, source_name: str) -> Dict[str, str]:
        """解析目标URL字符串"""
        if not targets_str:
            return {}
        
        targets = {}
        try:
            # 去除可能存在的前后引号
            targets_str = targets_str.strip('\'"')
            pairs = targets_str.split(';')
            for pair in pairs:
                if '=' in pair:
                    name, url = pair.split('=', 1)
                    if name.strip() and url.strip():
                        targets[name.strip()] = url.strip()
        except Exception as e:
            raise ValueError(f"解析{source_name}时出错: {e}")
        
        if not targets:
            raise ValueError(f"{source_name}解析后为空，请检查格式。")

        return targets
    
    def _parse_targets_from_config(self) -> Dict[str, str]:
        """从config.ini文件解析目标URL"""
        targets = {}
        try:
            if self.config_parser.has_section('targets'):
                for key, value in self.config_parser.items('targets'):
                    if key.strip() and value.strip():
                        targets[key.strip()] = value.strip()
        except (configparser.Error, UnicodeDecodeError):
            pass
        
        return targets
    
    def get_llm_config(self) -> Dict[str, Any]:
        """获取LLM配置，优先级：环境变量 > config.ini > 默认值。"""
        return {
            # OpenAI Compatible API 配置
            'openai_api_key': self._get_config_value('llm', 'openai_api_key', 'OPENAI_API_KEY', None),
            'openai_model': self._get_config_value('llm', 'openai_model', 'OPENAI_MODEL', 'gpt-3.5-turbo'),
            'openai_base_url': self._get_config_value('llm', 'openai_base_url', 'OPENAI_BASE_URL', 'https://api.openai.com/v1'),
            
            # 内容处理配置
            'max_content_length': self._get_config_value('llm', 'max_content_length', 'LLM_MAX_CONTENT_LENGTH', 380000, int)
        }

# 创建一个全局配置实例，供其他模块使用
config = Config()
