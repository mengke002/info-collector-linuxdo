"""
基于 curl_cffi 的 HTTP 客户端模块
完美模拟真实浏览器 TLS 指纹，以绕过 Cloudflare 等反爬虫机制，
速度远快于完整的无头浏览器。
"""
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from curl_cffi.requests import AsyncSession

from .config import config

class TLSClient:
    """
    提供完美 TLS 指纹伪造的异步 HTTP 客户端。
    取代了之前的 PlaywrightClient。
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.crawler_config = config.get_crawler_config()
        self.session = None

    async def start(self):
        """启动会话。"""
        if self.session is None:
            self.logger.info("正在启动 TLS HTTP 客户端...")
            self.session = AsyncSession(impersonate="chrome120")

    async def close(self):
        """关闭会话。"""
        if self.session:
            # For curl_cffi AsyncSession, close is sometimes async or sync depending on context/version
            # Safely handle both
            try:
                import inspect
                close_meth = self.session.close
                if inspect.iscoroutinefunction(close_meth):
                    await close_meth()
                else:
                    close_meth()
            except Exception as e:
                self.logger.warning(f"Error closing session: {e}")

            self.session = None
            self.logger.info("TLS HTTP 客户端已关闭。")

    async def __aenter__(self):
        """异步上下文管理器入口。"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口。"""
        await self.close()

    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        提供一个可以用于发出请求的会话。
        向后兼容旧的 Playwright API 习惯，但直接返回 AsyncSession。
        """
        if not self.session:
            raise RuntimeError("客户端未启动。")

        # 对于 curl_cffi，我们可以直接 yield session
        yield self.session
