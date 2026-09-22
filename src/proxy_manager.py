import logging
import requests
import random
import time
import asyncio
from typing import Optional

try:
    from fp.fp import FreeProxy
except ImportError:
    FreeProxy = None


class ProxyManager:
    """
    代理管理器，负责获取、验证和管理高可用代理。
    结合了 GitHub 优质高频订阅源（Proxifly HTTP/SOCKS5 及备用源）和 free-proxy 实时兜底机制。
    支持抽样验证与早停机制，避免海量节点验证耗尽系统资源。
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.proxy_sources = [
            {
                "name": "Proxifly-HTTP",
                "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
                "type": "http"
            },
            {
                "name": "Proxifly-SOCKS5",
                "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
                "type": "socks5"
            },
            {
                "name": "ProxyGather-HTTP",
                "url": "https://raw.githubusercontent.com/Skillter/ProxyGather/refs/heads/master/proxies/working-proxies-http.txt",
                "type": "http"
            }
        ]
        self.proxies_pool = []
        self.last_fetch_time = 0
        self.verify_url = "https://linux.do/"
        self._lock = asyncio.Lock()

    def remove_proxy(self, proxy: str):
        """将失效的代理从池中剔除"""
        if not proxy:
            return
        formatted = proxy if "://" in proxy else f"http://{proxy}"
        if formatted in self.proxies_pool:
            self.proxies_pool.remove(formatted)
            self.logger.info(f"剔除失效代理: {formatted}，当前池余量: {len(self.proxies_pool)}")
        elif proxy in self.proxies_pool:
            self.proxies_pool.remove(proxy)
            self.logger.info(f"剔除失效代理: {proxy}，当前池余量: {len(self.proxies_pool)}")

    async def _verify_proxy(self, proxy: str) -> Optional[str]:
        """验证代理是否可用且未被 Cloudflare 拦截"""
        from curl_cffi.requests import AsyncSession

        formatted_proxy = proxy if "://" in proxy else f"http://{proxy}"
        proxies = {"all": formatted_proxy}

        try:
            async with AsyncSession(impersonate="chrome120", proxies=proxies) as session:
                headers = {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                }
                response = await session.get(self.verify_url, headers=headers, timeout=6)
                # 必须是200，不能是 403 / 429 / 5xx 等盾
                if response.status_code == 200:
                    return formatted_proxy
        except Exception:
            pass
        return None

    async def _filter_proxies(self, raw_proxies: list, sample_size: int = 350, target_valid: int = 20) -> list:
        """
        从候选代理中抽样并并发验证，一旦达到 target_valid 个可用代理即提前结束（早停机制）。
        避免全量验证上万个代理导致网络拥堵与资源耗尽。
        """
        if not raw_proxies:
            return []

        # 随机抽样候选节点，打乱顺序
        if len(raw_proxies) > sample_size:
            candidate_proxies = random.sample(raw_proxies, sample_size)
        else:
            candidate_proxies = list(raw_proxies)
            random.shuffle(candidate_proxies)

        self.logger.info(f"开始抽样验证 {len(candidate_proxies)} 个代理连通性 (目标可用: {target_valid} 个)...")

        sem = asyncio.Semaphore(60)

        async def bounded_verify(p):
            async with sem:
                return await self._verify_proxy(p)

        tasks = [asyncio.create_task(bounded_verify(p)) for p in candidate_proxies]
        valid_proxies = []

        try:
            for fut in asyncio.as_completed(tasks):
                try:
                    res = await fut
                    if res:
                        valid_proxies.append(res)
                        if len(valid_proxies) >= target_valid:
                            self.logger.info(f"已达到目标可用代理数量 ({len(valid_proxies)} 个)，提前结束验证以节省时间。")
                            break
                except Exception:
                    pass
        finally:
            # 取消剩余未完成的验证任务，释放连接和协程
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        self.logger.info(f"验证阶段结束，本轮获得有效代理: {len(valid_proxies)} / {len(candidate_proxies)}")
        return valid_proxies

    async def _fetch_online_proxies(self) -> list:
        """从各订阅源（Proxifly HTTP / SOCKS5 及备用源）获取代理列表"""
        all_proxies = set()

        def fetch_source(src):
            try:
                self.logger.info(f"正在从 [{src['name']}] 获取代理列表...")
                resp = requests.get(src['url'], timeout=10)
                if resp.status_code == 200:
                    lines = resp.text.strip().splitlines()
                    valid_lines = []
                    for line in lines:
                        p = line.strip()
                        if not p or p.startswith("#"):
                            continue
                        if "://" not in p:
                            p = f"{src.get('type', 'http')}://{p}"
                        valid_lines.append(p)
                    self.logger.info(f"从 [{src['name']}] 成功获取到 {len(valid_lines)} 个代理。")
                    return valid_lines
            except Exception as e:
                self.logger.warning(f"从 [{src['name']}] 获取代理失败: {e}")
            return []

        # 并行拉取所有数据源
        fetch_tasks = [asyncio.to_thread(fetch_source, src) for src in self.proxy_sources]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, list):
                all_proxies.update(res)

        total_list = list(all_proxies)
        self.logger.info(f"所有订阅源拉取完成，合并去重后总计 {len(total_list)} 个代理。")
        return total_list

    async def get_proxy(self) -> Optional[str]:
        """获取一个可用的代理（带有缓存轮换、水位检查和并发保护）"""
        current_time = time.time()

        # 检查是否需要刷新代理池：
        # 1. 池子完全为空，且距离上次拉取超过 15 秒（防止死循环快速重试）
        # 2. 池子余量偏低（<= 2 个）且距离上次拉取超过 90 秒
        # 3. 代理池已超过最大保质期（30 分钟）
        need_refresh = False
        if not self.proxies_pool and (current_time - self.last_fetch_time > 15):
            need_refresh = True
        elif len(self.proxies_pool) <= 2 and (current_time - self.last_fetch_time > 90):
            need_refresh = True
        elif (current_time - self.last_fetch_time > 1800):
            need_refresh = True

        if need_refresh:
            async with self._lock:
                # 双重检查加锁，防止并发协程重复触发拉取
                recheck_time = time.time()
                recheck_need = False
                if not self.proxies_pool and (recheck_time - self.last_fetch_time > 15):
                    recheck_need = True
                elif len(self.proxies_pool) <= 2 and (recheck_time - self.last_fetch_time > 90):
                    recheck_need = True
                elif (recheck_time - self.last_fetch_time > 1800):
                    recheck_need = True

                if recheck_need:
                    self.last_fetch_time = time.time()
                    raw_proxies = await self._fetch_online_proxies()
                    if raw_proxies:
                        new_valid = await self._filter_proxies(raw_proxies)
                        # 将新验证出的代理合并进池并去重
                        self.proxies_pool = list(dict.fromkeys(self.proxies_pool + new_valid))
                        self.logger.info(f"当前代理池总余量: {len(self.proxies_pool)}")

        if self.proxies_pool:
            proxy = random.choice(self.proxies_pool)
            self.logger.info(f"分配经验证的在线列表代理: {proxy}")
            return proxy

        # 如果在线列表全部失效或为空，回退到 free-proxy 库
        self.logger.warning("在线验证代理池耗尽，尝试使用 free-proxy 实时获取兜底代理...")
        if FreeProxy is not None:
            try:
                def fetch_free_proxy():
                    return FreeProxy(timeout=3, rand=True).get()

                raw_proxy = await asyncio.to_thread(fetch_free_proxy)
                if raw_proxy:
                    proxy = raw_proxy if "://" in raw_proxy else f"http://{raw_proxy}"
                    self.logger.info(f"FreeProxy 分配代理: {proxy}")
                    return proxy
            except Exception as e:
                self.logger.error(f"FreeProxy 获取代理失败: {e}")
        return None


proxy_manager = ProxyManager()
