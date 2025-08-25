"""
LLM集成模块
支持多种大语言模型服务的统一接口
"""
import logging
import json
import asyncio
from typing import Dict, Any, Optional, List
from abc import ABC, abstractmethod
import aiohttp
from urllib.parse import urljoin

from .config import config


class BaseLLMClient(ABC):
    """LLM客户端基类"""
    
    def __init__(self, provider_name: str):
        self.provider_name = provider_name
        self.logger = logging.getLogger(__name__)
    
    @abstractmethod
    async def analyze_content(self, content: str, prompt_template: str) -> Dict[str, Any]:
        """分析内容并返回结果"""
        pass
    
    def _format_prompt(self, content: str, template: str) -> str:
        """格式化提示词"""
        return template.format(content=content)


class OpenAIClient(BaseLLMClient):
    """OpenAI API客户端"""
    
    def __init__(self):
        super().__init__("OpenAI")
        llm_config = config.get_llm_config()
        self.api_key = llm_config.get('openai_api_key')
        self.model = llm_config.get('openai_model', 'gpt-3.5-turbo')
        self.base_url = llm_config.get('openai_base_url', 'https://api.openai.com/v1')
        self.max_retries = llm_config.get('max_retries', 3)
        self.timeout = llm_config.get('timeout', 30)
        
        if not self.api_key:
            raise ValueError("OpenAI API key not configured")
    
    async def analyze_content(self, content: str, prompt_template: str) -> Dict[str, Any]:
        """使用OpenAI API分析内容"""
        prompt = self._format_prompt(content, prompt_template)
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        data = {
            'model': self.model,
            'messages': [
                {'role': 'system', 'content': '你是一个专业的内容分析师，擅长总结和提取关键信息。'},
                {'role': 'user', 'content': prompt}
            ],
            'temperature': 0.3,
            'max_tokens': 1000
        }
        
        url = urljoin(self.base_url, '/chat/completions')
        
        for attempt in range(self.max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=self.timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, headers=headers, json=data) as response:
                        if response.status == 200:
                            result = await response.json()
                            content = result['choices'][0]['message']['content']
                            
                            return {
                                'success': True,
                                'content': content,
                                'provider': self.provider_name,
                                'model': self.model,
                                'usage': result.get('usage', {})
                            }
                        else:
                            error_text = await response.text()
                            error_msg = f"API请求失败: {response.status} - {error_text}"
                            self.logger.warning(error_msg)
                            
                            if attempt == self.max_retries - 1:
                                return {
                                    'success': False,
                                    'error': error_msg,
                                    'provider': self.provider_name
                                }
                    
            except Exception as e:
                error_msg = f"请求异常: {str(e)}"
                self.logger.warning(f"第{attempt + 1}次尝试失败: {error_msg}")
                
                if attempt == self.max_retries - 1:
                    return {
                        'success': False,
                        'error': error_msg,
                        'provider': self.provider_name
                    }
                
                # 指数退避
                await asyncio.sleep(2 ** attempt)


class DeepSeekClient(BaseLLMClient):
    """DeepSeek API客户端"""
    
    def __init__(self):
        super().__init__("DeepSeek")
        llm_config = config.get_llm_config()
        self.api_key = llm_config.get('deepseek_api_key')
        self.model = llm_config.get('deepseek_model', 'deepseek-chat')
        self.base_url = llm_config.get('deepseek_base_url', 'https://api.deepseek.com/v1')
        self.max_retries = llm_config.get('max_retries', 3)
        self.timeout = llm_config.get('timeout', 30)
        
        if not self.api_key:
            raise ValueError("DeepSeek API key not configured")
    
    async def analyze_content(self, content: str, prompt_template: str) -> Dict[str, Any]:
        """使用DeepSeek API分析内容"""
        prompt = self._format_prompt(content, prompt_template)
        
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        
        data = {
            'model': self.model,
            'messages': [
                {'role': 'system', 'content': '你是一个专业的内容分析师，擅长总结和提取关键信息。'},
                {'role': 'user', 'content': prompt}
            ],
            'temperature': 0.3,
            'max_tokens': 1000
        }
        
        url = urljoin(self.base_url, '/chat/completions')
        
        for attempt in range(self.max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=self.timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, headers=headers, json=data) as response:
                        if response.status == 200:
                            result = await response.json()
                            content = result['choices'][0]['message']['content']
                            
                            return {
                                'success': True,
                                'content': content,
                                'provider': self.provider_name,
                                'model': self.model,
                                'usage': result.get('usage', {})
                            }
                        else:
                            error_text = await response.text()
                            error_msg = f"API请求失败: {response.status} - {error_text}"
                            self.logger.warning(error_msg)
                            
                            if attempt == self.max_retries - 1:
                                return {
                                    'success': False,
                                    'error': error_msg,
                                    'provider': self.provider_name
                                }
                    
            except Exception as e:
                error_msg = f"请求异常: {str(e)}"
                self.logger.warning(f"第{attempt + 1}次尝试失败: {error_msg}")
                
                if attempt == self.max_retries - 1:
                    return {
                        'success': False,
                        'error': error_msg,
                        'provider': self.provider_name
                    }
                
                # 指数退避
                await asyncio.sleep(2 ** attempt)


class MockLLMClient(BaseLLMClient):
    """模拟LLM客户端，用于测试"""
    
    def __init__(self):
        super().__init__("Mock")
    
    async def analyze_content(self, content: str, prompt_template: str) -> Dict[str, Any]:
        """模拟分析内容"""
        return {
            'success': True,
            'content': f"[模拟分析结果] 这是对内容的模拟分析：{content[:100]}...",
            'provider': self.provider_name,
            'model': 'mock-model'
        }


class LLMManager:
    """LLM管理器，负责选择和调用合适的LLM客户端"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.clients: Dict[str, BaseLLMClient] = {}
        self._init_clients()
    
    def _init_clients(self):
        """初始化可用的LLM客户端"""
        llm_config = config.get_llm_config()
        
        # 初始化OpenAI客户端
        if llm_config.get('openai_api_key'):
            try:
                self.clients['openai'] = OpenAIClient()
                self.logger.info("OpenAI客户端初始化成功")
            except Exception as e:
                self.logger.warning(f"OpenAI客户端初始化失败: {e}")
        
        # 初始化DeepSeek客户端
        if llm_config.get('deepseek_api_key'):
            try:
                self.clients['deepseek'] = DeepSeekClient()
                self.logger.info("DeepSeek客户端初始化成功")
            except Exception as e:
                self.logger.warning(f"DeepSeek客户端初始化失败: {e}")
        
        # 总是初始化Mock客户端作为后备
        self.clients['mock'] = MockLLMClient()
        
        if not self.clients:
            self.logger.warning("没有可用的LLM客户端")
    
    def get_available_providers(self) -> List[str]:
        """获取可用的LLM提供商列表"""
        return list(self.clients.keys())
    
    async def analyze_content(self, content: str, prompt_template: str, 
                            preferred_provider: str = None) -> Dict[str, Any]:
        """分析内容，自动选择最佳的LLM客户端"""
        
        # 确定使用的客户端
        client = None
        
        if preferred_provider and preferred_provider in self.clients:
            client = self.clients[preferred_provider]
            self.logger.info(f"使用指定的LLM提供商: {preferred_provider}")
        else:
            # 按优先级选择客户端
            priority_order = ['openai', 'deepseek', 'mock']
            for provider in priority_order:
                if provider in self.clients:
                    client = self.clients[provider]
                    self.logger.info(f"自动选择LLM提供商: {provider}")
                    break
        
        if not client:
            return {
                'success': False,
                'error': '没有可用的LLM客户端',
                'provider': None
            }
        
        try:
            result = await client.analyze_content(content, prompt_template)
            return result
        except Exception as e:
            self.logger.error(f"LLM分析失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'provider': client.provider_name
            }


# 全局LLM管理器实例
llm_manager = LLMManager()