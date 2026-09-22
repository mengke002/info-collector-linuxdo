import logging
import requests
import random
import time
import asyncio
from typing import Optional, List

try:
    from fp.fp import FreeProxy
except ImportError:
    FreeProxy = None


class ProxyManager:
    """
    代理管理器，负责获取、验证和管理高可用代理。
    采用全量分批流式验证（Batch Streaming Validation）机制：
    持续并发测试候选代理，直到获取足量有效代理（如 15 个）立即早停；
    或者将所有候选代理全部测试完毕，绝不遗漏可用节点。
    """
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.proxy_sources = [
            # 优先测试高存活率的 HTTP 代理源
            {
                "name": "ProxyGather-HTTP",
                "url": "https://raw.githubusercontent.com/Skillter/ProxyGather/refs/heads/master/proxies/working-proxies-http.txt",
                "type": "http"
            },
            {
                "name": "Proxifly-HTTP",
                "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
                "type": "http"
            },
            # 其次测试 SOCKS5 代理源作为后备候选
            {
                "name": "Proxifly-SOCKS5",
                "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt",
                "type": "socks5"
            }
        ]
        self.proxies_pool: List[str] = []
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

    async def _verify_proxy(self, proxy: str, sem: asyncio.Semaphore) -> Optional[str]:
        """验证代理是否可用且未被 Cloudflare 拦截"""
        from curl_cffi.requests import AsyncSession

        formatted_proxy = proxy if "://" in proxy else f"http://{proxy}"
        proxies = {"all": formatted_proxy}

        async with sem:
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

    async def _filter_proxies_until_target(
        self,
        raw_proxies: list,
        target_valid: int = 15,
        batch_size: int = 200,
        concurrency: int = 80
    ) -> list:
        """
        流式持续测试所有候选代理：
        分批进行并发测试，一旦收集到 target_valid 个可用代理即提前早停返回；
        若未达到目标数量，则持续测试下一批，直到将整个列表全部测试完毕。
        保证只要列表中存在可用节点，就绝不会被漏掉！
        """
        if not raw_proxies:
            return []

        total = len(raw_proxies)
        self.logger.info(f"开始流式全量并发验证，候选代理总数: {total} 个，目标有效数量: {target_valid} 个...")

        sem = asyncio.Semaphore(concurrency)
        valid_proxies = []
        tested_count = 0

        for i in range(0, total, batch_size):
            batch = raw_proxies[i:i + batch_size]
            tested_count += len(batch)
            tasks = [asyncio.create_task(self._verify_proxy(p, sem)) for p in batch]

            for fut in asyncio.as_completed(tasks):
                try:
                    res = await fut
                    if res:
                        valid_proxies.append(res)
                        self.logger.info(f"发现有效代理 [{len(valid_proxies)}/{target_valid}]: {res}")
                        if len(valid_proxies) >= target_valid:
                            break
                except Exception:
                    pass

            # 取消当前批次中剩余尚未完成的任务
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            self.logger.info(f"已完成测试进度: {tested_count}/{total}，当前已收集有效代理: {len(valid_proxies)} 个")
            if len(valid_proxies) >= target_valid:
                self.logger.info(f"已成功获取足量可用代理 ({len(valid_proxies)} 个)，立即停止测试以节省资源！")
                break

        self.logger.info(f"验证阶段完成，共测试 {tested_count} 个代理，最终获得有效代理: {len(valid_proxies)} 个")
        return valid_proxies

    async def _fetch_online_proxies(self) -> list:
        """从各订阅源顺序/并行获取候选代理列表（HTTP 优先）"""
        ordered_proxies = []
        seen = set()

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

        # 按优先级顺序执行拉取
        fetch_tasks = [asyncio.to_thread(fetch_source, src) for src in self.proxy_sources]
        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, list):
                for p in res:
                    if p not in seen:
                        seen.add(p)
                        ordered_proxies.append(p)

        self.logger.info(f"所有订阅源拉取完成，合并去重后总计 {len(ordered_proxies)} 个代理待测。")
        return ordered_proxies

    async def get_proxy(self) -> Optional[str]:
        """
        获取一个可用的代理（带有流式全量验证、线程锁排队和安全轮换）
        并发请求在代理池为空时将有序排队等待首个协程填充池子，杜绝误触发 FreeProxy 导致 403。
        """
        # 1. 如果池中有经过验证的有效代理，直接随机返回
        if self.proxies_pool:
            proxy = random.choice(self.proxies_pool)
            self.logger.info(f"分配经验证的在线列表代理: {proxy}")
            return proxy

        # 2. 如果代理池为空，加锁排队刷新
        async with self._lock:
            # 双重检查锁：等待锁期间，可能前面的协程已经完成了测试并填充了池子
            if self.proxies_pool:
                proxy = random.choice(self.proxies_pool)
                self.logger.info(f"分配经验证的在线列表代理: {proxy}")
                return proxy

            self.logger.info("在线代理池为空，开始全量流式测试获取可用代理...")
            raw_proxies = await self._fetch_online_proxies()
            if raw_proxies:
                valid_proxies = await self._filter_proxies_until_target(
                    raw_proxies,
                    target_valid=15,
                    batch_size=200,
                    concurrency=80
                )
                if valid_proxies:
                    self.proxies_pool = list(dict.fromkeys(valid_proxies))
                    self.last_fetch_time = time.time()
                    self.logger.info(f"代理池已就绪，当前总余量: {len(self.proxies_pool)}")

            if self.proxies_pool:
                proxy = random.choice(self.proxies_pool)
                self.logger.info(f"分配经验证的在线列表代理: {proxy}")
                return proxy

        # 3. 只有当候选列表全部测试完毕且确实无一存活时，才尝试 FreeProxy 兜底
        self.logger.warning("所有在线代理测试完毕均不可用，尝试使用 free-proxy 实时获取兜底代理...")
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
