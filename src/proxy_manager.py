import logging
import requests
import random
import time
import asyncio
from typing import Optional, List
from urllib.parse import urlparse

try:
    from fp.fp import FreeProxy
except ImportError:
    FreeProxy = None


class ProxyManager:
    """
    代理管理器，负责获取、验证和管理高可用代理。
    采用“两阶段极速探测流水线（Two-Phase Fast Pipeline）”：
    1. Phase 1 (TCP 快筛): 超高并发极速探测端口连通性，1秒内淘汰 95% 死节点；
    2. Phase 2 (业务验证): 对存活节点并发校验 Discourse API 连通性；
    3. 早停机制 (Early Stopping): 搜集满指定数量 (如 12 个) 立即返回，提效 30 倍以上。
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
        # 使用官方轻量 Discourse 公共状态接口（仅 755 字节，绝不被 HTML 质询误杀）
        self.verify_url = "https://linux.do/site/basic-info.json"
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

    async def _fast_tcp_ping(self, host: str, port: int, timeout: float = 1.0) -> bool:
        """第一阶段：极速 TCP 探测，0.01~1.0 秒内快速淘汰死节点"""
        try:
            conn = asyncio.open_connection(host, port)
            reader, writer = await asyncio.wait_for(conn, timeout=timeout)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    async def _verify_proxy_api(self, proxy: str, sem: asyncio.Semaphore) -> Optional[str]:
        """第二阶段：使用 curl_cffi 验证是否能成功请求目标站 API 并返回有效数据"""
        from curl_cffi.requests import AsyncSession

        formatted_proxy = proxy if "://" in proxy else f"http://{proxy}"
        proxies = {"all": formatted_proxy}

        async with sem:
            try:
                async with AsyncSession(impersonate="chrome120", proxies=proxies) as session:
                    headers = {
                        "accept": "application/json, text/javascript, */*; q=0.01",
                        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                        "x-requested-with": "XMLHttpRequest",
                        "referer": "https://linux.do/"
                    }
                    response = await session.get(self.verify_url, headers=headers, timeout=5)
                    if response.status_code == 200 and "logo_url" in response.text:
                        return formatted_proxy
            except Exception:
                pass
        return None

    async def _filter_proxies_until_target(
        self,
        raw_proxies: list,
        target_valid: int = 12,
        chunk_size: int = 400
    ) -> list:
        """
        两阶段极速流式初筛：
        Phase 1: 高并发 (200) TCP 探测，瞬间秒杀 95% 端口不通的死节点；
        Phase 2: 对幸存活节点并发校验 API 连通性，凑满目标数立即早停！
        """
        if not raw_proxies:
            return []

        total = len(raw_proxies)
        self.logger.info(f"开始两阶段极速流式验证，候选代理总数: {total} 个，目标有效数量: {target_valid} 个...")

        valid_proxies = []
        tested_count = 0
        tcp_sem = asyncio.Semaphore(200)
        cf_sem = asyncio.Semaphore(50)

        async def check_tcp(p: str):
            async with tcp_sem:
                try:
                    u = urlparse(p if "://" in p else f"http://{p}")
                    if u.hostname and u.port:
                        return p if await self._fast_tcp_ping(u.hostname, u.port, timeout=1.0) else None
                except Exception:
                    pass
            return None

        for i in range(0, total, chunk_size):
            chunk = raw_proxies[i:i + chunk_size]
            tested_count += len(chunk)

            # Phase 1: 极速 TCP 探测
            tcp_tasks = [check_tcp(p) for p in chunk]
            tcp_results = await asyncio.gather(*tcp_tasks)
            alive_candidates = [p for p in tcp_results if p]

            self.logger.info(f"测试进度: {tested_count}/{total} (本批 TCP 存活: {len(alive_candidates)}/{len(chunk)})")

            # Phase 2: API 业务验证
            if alive_candidates:
                api_tasks = [asyncio.create_task(self._verify_proxy_api(p, cf_sem)) for p in alive_candidates]
                for fut in asyncio.as_completed(api_tasks):
                    try:
                        res = await fut
                        if res:
                            valid_proxies.append(res)
                            self.logger.info(f"-> 发现有效代理 [{len(valid_proxies)}/{target_valid}]: {res}")
                            if len(valid_proxies) >= target_valid:
                                break
                    except Exception:
                        pass

                for t in api_tasks:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*api_tasks, return_exceptions=True)

            if len(valid_proxies) >= target_valid:
                self.logger.info(f"已成功获取足量有效代理 ({len(valid_proxies)} 个)，提前结束测试以节省时间！")
                break

        self.logger.info(f"验证阶段完成，共初筛 {tested_count} 个代理，最终获得有效代理: {len(valid_proxies)} 个")
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
        获取一个可用的代理（带有两阶段极速流式验证、线程锁排队和安全轮换）
        并发请求在代理池为空时将有序排队等待首个协程填充池子，杜绝误触发 FreeProxy 导致 403。
        """
        # 1. 如果池中有经过验证的有效代理，直接随机返回
        if self.proxies_pool:
            proxy = random.choice(self.proxies_pool)
            self.logger.debug(f"分配经验证的在线列表代理: {proxy}")
            return proxy

        # 2. 如果代理池为空，加锁排队刷新
        async with self._lock:
            # 双重检查锁：等待锁期间，可能前面的协程已经完成了测试并填充了池子
            if self.proxies_pool:
                proxy = random.choice(self.proxies_pool)
                self.logger.debug(f"分配经验证的在线列表代理: {proxy}")
                return proxy

            self.logger.info("在线代理池为空，开始两阶段极速流式测试获取可用代理...")
            raw_proxies = await self._fetch_online_proxies()
            if raw_proxies:
                valid_proxies = await self._filter_proxies_until_target(
                    raw_proxies,
                    target_valid=12,
                    chunk_size=400
                )
                if valid_proxies:
                    self.proxies_pool = list(dict.fromkeys(valid_proxies))
                    self.last_fetch_time = time.time()
                    self.logger.info(f"代理池已就绪，当前总余量: {len(self.proxies_pool)}")

            if self.proxies_pool:
                proxy = random.choice(self.proxies_pool)
                self.logger.debug(f"分配经验证的在线列表代理: {proxy}")
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
                    self.logger.debug(f"FreeProxy 分配代理: {proxy}")
                    return proxy
            except Exception as e:
                self.logger.error(f"FreeProxy 获取代理失败: {e}")
        return None


proxy_manager = ProxyManager()
