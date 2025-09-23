"""LLM客户端模块
支持OpenAI compatible接口的streaming实现，包含重试机制
"""
import logging
import time
from typing import Dict, Any, Optional
from openai import OpenAI

try:
    from .config import config
except ImportError:
    # 当作脚本直接运行时的导入
    from config import config


class LLMClient:
    """统一的LLM客户端，支持重试机制"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        # 从配置文件获取配置（按优先级：环境变量 > config.ini > 默认值）
        llm_config = config.get_llm_config()
        self.api_key = llm_config.get('openai_api_key')
        self.model = llm_config.get('openai_model', 'gpt-3.5-turbo')
        priority_model = llm_config.get('priority_model')
        if isinstance(priority_model, str):
            priority_model = priority_model.strip()
        self.priority_model = priority_model or None
        self.base_url = llm_config.get('openai_base_url', 'https://api.openai.com/v1')

        if not self.api_key:
            raise ValueError("未找到OPENAI_API_KEY配置，请在环境变量或config.ini中设置")

        # 初始化OpenAI客户端
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )

        priority_info = f", Priority Model: {self.priority_model}" if self.priority_model else ""
        self.logger.info(f"LLM客户端初始化成功 - Model: {self.model}{priority_info}, Base URL: {self.base_url}")

    def analyze_content(
        self,
        content: str,
        prompt_template: str,
        max_retries: int = 3,
        model_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        """使用streaming方式分析内容，支持重试机制

        Args:
            content: 需要分析的原始内容
            prompt_template: 提示词模板
            max_retries: 最大重试次数
            model_override: 指定使用的模型，提供时将跳过优先模型回退逻辑
        """
        # 格式化提示词
        prompt = prompt_template.format(content=content)

        models_to_try = []
        use_fallback_chain = model_override is None

        if model_override:
            models_to_try.append(model_override)
            self.logger.info(f"使用指定模型执行LLM分析: {model_override}")
        else:
            if self.priority_model:
                models_to_try.append(self.priority_model)
            if self.model not in models_to_try:
                models_to_try.append(self.model)

        last_response = None
        for model_name in models_to_try:
            result = self._make_request(prompt, model_name, 0.3, max_retries)
            if result.get('success'):
                return result

            last_response = result

            if (
                use_fallback_chain
                and model_name == self.priority_model
                and self.model != self.priority_model
            ):
                self.logger.warning(
                    f"优先模型 {self.priority_model} 在 {max_retries} 次尝试后失败，回退至 {self.model}"
                )

        return last_response

    def _make_request(self, prompt: str, model_name: str, temperature: float, max_retries: int = 3) -> Dict[str, Any]:
        """
        执行具体的LLM请求，支持streaming和重试机制

        Args:
            prompt: 提示词
            model_name: 模型名称
            temperature: 生成温度
            max_retries: 最大重试次数

        Returns:
            响应结果字典
        """
        for attempt in range(max_retries):
            try:
                self.logger.info(f"调用LLM: {model_name} (尝试 {attempt + 1}/{max_retries})")
                self.logger.info(f"提示词长度: {len(prompt)} 字符")

                # 创建streaming请求
                response = self.client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {'role': 'system', 'content': '你是一个专业的内容分析师，擅长总结和提取关键信息。'},
                        {'role': 'user', 'content': prompt}
                    ],
                    temperature=temperature,
                    stream=True
                )

                # 收集所有streaming内容
                full_content = ""
                reasoning_content_full = ""
                chunk_count = 0

                self.logger.info("开始streaming响应处理...")

                for chunk in response:
                    chunk_count += 1

                    try:
                        # 安全检查chunk结构
                        choices = getattr(chunk, 'choices', None)
                        if not choices:
                            self.logger.debug(f"跳过空chunk {chunk_count}")
                            continue

                        if len(choices) == 0:
                            self.logger.debug(f"跳过没有choices的chunk {chunk_count}")
                            continue

                        delta = choices[0].delta

                        # 安全地获取reasoning_content和content
                        reasoning_content = getattr(delta, 'reasoning_content', None)
                        content_chunk = getattr(delta, 'content', None)

                        if reasoning_content:
                            # 推理内容单独收集，但不加入最终结果
                            reasoning_content_full += reasoning_content
                            self.logger.debug(f"Chunk {chunk_count} - Reasoning: {reasoning_content[:50]}...")

                        if content_chunk:
                            # 只收集最终的content内容
                            full_content += content_chunk
                            self.logger.debug(f"Chunk {chunk_count} - Content: {content_chunk[:50]}...")
                    except Exception as chunk_error:
                        self.logger.warning(f"Chunk {chunk_count} 处理异常，已跳过: {chunk_error}")
                        self.logger.debug("异常chunk详情: %r", chunk, exc_info=True)
                        continue

                self.logger.info(f"LLM调用完成 - 处理了 {chunk_count} 个chunks")
                self.logger.info(f"响应内容长度: {len(full_content)} 字符")

                # 检查响应内容是否为空
                if not full_content.strip():
                    raise ValueError("LLM返回空响应")

                return {
                    'success': True,
                    'content': full_content.strip(),
                    'model': model_name,
                    'provider': 'openai_compatible',
                    'attempt': attempt + 1
                }

            except Exception as e:
                error_msg = f"LLM调用失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}"
                self.logger.error(error_msg)

                # 如果是最后一次尝试，记录详细错误信息并返回失败
                if attempt == max_retries - 1:
                    self.logger.error(error_msg, exc_info=True)
                    return {
                        'success': False,
                        'error': error_msg,
                        'model': model_name,
                        'total_attempts': max_retries
                    }
                else:
                    # 等待后重试
                    wait_time = (attempt + 1) * 2  # 递增等待时间: 2, 4, 6秒
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)


# 全局LLM客户端实例
try:
    llm_client = LLMClient()
except Exception as e:
    logging.getLogger(__name__).warning(f"LLM客户端初始化失败: {e}")
    llm_client = None
