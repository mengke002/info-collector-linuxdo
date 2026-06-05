import logging
import requests
import random
import time
from typing import Optional
from fp.fp import FreeProxy

class ProxyManager:
    """
    代理管理器，负责获取免费代理。
    结合了免本地爬取（GitHub订阅源）和 free-proxy 库的方法。
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Github ProxyGather 源 (Method 3)
        self.github_proxy_url = "https://raw.githubusercontent.com/Skillter/ProxyGather/refs/heads/master/proxies/working-proxies-http.txt"
        self.proxies_pool = []
        self.last_fetch_time = 0

    async def _fetch_online_proxies(self) -> list:
        """从 GitHub 获取代理列表"""
        try:
            import asyncio
            self.logger.info("正在从 GitHub (ProxyGather) 获取代理列表...")
            response = await asyncio.to_thread(requests.get, self.github_proxy_url, timeout=10)
            if response.status_code == 200:
                proxy_list = response.text.strip().split('\n')
                # 简单过滤空行
                proxies = [p.strip() for p in proxy_list if p.strip()]
                self.logger.info(f"从 GitHub 成功获取到 {len(proxies)} 个代理。")
                return proxies
        except Exception as e:
            self.logger.warning(f"获取在线代理列表失败: {e}")
        return []

    async def get_proxy(self) -> Optional[str]:
        """获取一个可用的代理（带有简单的缓存轮换）"""
        import asyncio

        # 1. 尝试使用 GitHub 列表 (每30分钟刷新一次列表)
        current_time = time.time()
        if not self.proxies_pool or (current_time - self.last_fetch_time > 1800):
            self.proxies_pool = await self._fetch_online_proxies()
            self.last_fetch_time = current_time

        if self.proxies_pool:
            # 随机挑选一个并返回 (以 http:// 开头，因为 TLSClient 需要这种格式)
            proxy = random.choice(self.proxies_pool)
            formatted_proxy = f"http://{proxy}" if not proxy.startswith("http") else proxy
            self.logger.info(f"分配在线列表代理: {formatted_proxy}")
            return formatted_proxy

        # 2. 如果 GitHub 列表为空或获取失败，则回退到 free-proxy 库
        self.logger.info("在线列表不可用，尝试使用 free-proxy 实时获取代理...")
        try:
            # FreeProxy 会自动寻找并验证一个可用代理
            def fetch_free_proxy():
                return FreeProxy(timeout=3, rand=True).get()

            proxy = await asyncio.to_thread(fetch_free_proxy)
            self.logger.info(f"FreeProxy 分配代理: {proxy}")
            return proxy
        except Exception as e:
            self.logger.error(f"FreeProxy 获取代理失败: {e}")
            return None

proxy_manager = ProxyManager()
