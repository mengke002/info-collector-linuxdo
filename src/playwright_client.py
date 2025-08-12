"""
基于Playwright的HTTP客户端模块
用于处理现代网站的反爬虫机制，支持JavaScript渲染
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator

from playwright.async_api import async_playwright, Browser, Page, BrowserContext

from .config import config


class PlaywrightClient:
    """
    一个管理Playwright浏览器生命周期的客户端。
    它被设计为在整个应用程序中作为单例使用，以管理一个浏览器实例。
    任务可以通过 get_page() 方法获取隔离的页面。
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.browser: Optional[Browser] = None
        self._playwright = None
        self.crawler_config = config.get_crawler_config()

    async def start(self):
        """启动Playwright并创建一个浏览器实例。"""
        if self.browser:
            return
        try:
            self.logger.info("正在启动Playwright...")
            self._playwright = await async_playwright().start()
            self.browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor'
                ]
            )
            self.logger.info("Playwright浏览器实例启动成功。")
        except Exception as e:
            self.logger.error(f"启动Playwright浏览器失败: {e}")
            raise

    async def close(self):
        """关闭浏览器和Playwright。"""
        if self.browser:
            await self.browser.close()
            self.browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
        self.logger.info("Playwright浏览器已关闭。")

    async def __aenter__(self):
        """异步上下文管理器入口，确保浏览器已启动。"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口，确保浏览器被关闭。"""
        await self.close()

    @asynccontextmanager
    async def get_page(self) -> AsyncGenerator[Page, None]:
        """
        提供一个隔离的浏览器页面用于单个任务。
        这是一个异步上下文管理器，会自动创建和销毁上下文和页面。
        """
        if not self.browser:
            raise RuntimeError("浏览器未启动。请在 'async with' 块中使用PlaywrightClient。")

        context: Optional[BrowserContext] = None
        try:
            # 为每个任务创建一个新的、隔离的上下文
            context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                extra_http_headers={
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                }
            )
            page = await context.new_page()
            
            # 从配置中设置默认超时
            page.set_default_timeout(self.crawler_config.get('timeout_seconds', 30) * 1000)
            
            yield page
            
        finally:
            if context:
                await context.close()
