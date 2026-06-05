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
        self.verify_url = "https://linux.do/"
        import asyncio
        self._lock = asyncio.Lock()

    def remove_proxy(self, proxy: str):
        """将失效的代理从池中剔除"""
        if proxy in self.proxies_pool:
            self.proxies_pool.remove(proxy)
            self.logger.info(f"剔除失效代理: {proxy}，当前池余量: {len(self.proxies_pool)}")

    async def _verify_proxy(self, proxy: str) -> Optional[str]:
        """验证代理是否可用且未被 Cloudflare 拦截"""
        import asyncio
        from curl_cffi.requests import AsyncSession

        formatted_proxy = f"http://{proxy}" if not proxy.startswith("http") else proxy
        proxies = {"all": formatted_proxy}

        try:
            async with AsyncSession(impersonate="chrome120", proxies=proxies) as session:
                headers = {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }
                response = await session.get(self.verify_url, headers=headers, timeout=5)
                # 必须是200，不能是 403 / 429 盾
                if response.status_code == 200:
                    return formatted_proxy
        except Exception:
            pass
        return None

    async def _filter_proxies(self, raw_proxies: list) -> list:
        """并发验证一批代理，使用信号量控制最大并发数，返回可用的代理列表"""
        import asyncio
        self.logger.info(f"开始并发验证 {len(raw_proxies)} 个代理连通性...")

        # 限制最大并发数，防止超过系统文件描述符限制或瞬间耗尽资源
        sem = asyncio.Semaphore(100)

        async def bounded_verify(p):
            async with sem:
                return await self._verify_proxy(p)

        tasks = [bounded_verify(p) for p in raw_proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_proxies = [res for res in results if isinstance(res, str) and res]
        self.logger.info(f"验证完成，有效代理: {len(valid_proxies)} / {len(raw_proxies)}")
        return valid_proxies

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
        """获取一个可用的代理（带有缓存轮换和线程安全锁）"""
        import asyncio

        current_time = time.time()

        # 1. 如果池子为空或已过期，触发获取与验证（加锁防止雷群效应）
        # 使用 last_fetch_time 控制冷却，如果最近30秒内刚拉过（即便结果是空的），也不要频繁去拉，防止死循环轰炸
        if not self.proxies_pool and (current_time - self.last_fetch_time > 30) or (current_time - self.last_fetch_time > 1800):
            async with self._lock:
                # 再次检查，防止等待锁的期间其他协程已经更新了池子
                if not self.proxies_pool and (time.time() - self.last_fetch_time > 30) or (time.time() - self.last_fetch_time > 1800):
                    # 防止多次快速重试导致死循环：立刻更新上次拉取时间
                    self.last_fetch_time = time.time()
                    raw_proxies = await self._fetch_online_proxies()
                    if raw_proxies:
                        self.proxies_pool = await self._filter_proxies(raw_proxies)

        if self.proxies_pool:
            proxy = random.choice(self.proxies_pool)
            self.logger.info(f"分配经验证的在线列表代理: {proxy}")
            return proxy

        # 2. 如果所有在线列表全部失效或为空，则回退到 free-proxy 库
        self.logger.info("在线验证代理池耗尽，尝试使用 free-proxy 实时获取兜底代理...")
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
