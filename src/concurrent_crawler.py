"""
并发爬虫模块
使用异步并发提升爬取效率
"""
import asyncio
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone, timedelta
import random

from .config import config
from .playwright_client import PlaywrightClient
from .database import db_manager
from .html_to_markdown import html_to_markdown


class ConcurrentCrawler:
    """并发爬虫"""
    
    def __init__(self, max_concurrent_boards=2, max_concurrent_pages=2, max_concurrent_details=5):
        self.crawler_config = config.get_crawler_config()
        self.target_urls = config.get_target_urls()
        self.logger = logging.getLogger(__name__)
        
        # 并发控制参数（可配置）
        self.max_concurrent_boards = max_concurrent_boards
        self.max_concurrent_pages = max_concurrent_pages
        self.max_concurrent_details = max_concurrent_details
        
        # 信号量控制并发数
        self.board_semaphore = asyncio.Semaphore(self.max_concurrent_boards)
        self.page_semaphore = asyncio.Semaphore(self.max_concurrent_pages)
        self.detail_semaphore = asyncio.Semaphore(self.max_concurrent_details)
    
    def _parse_datetime(self, time_str: str) -> datetime:
        """解析时间字符串为时区感知的datetime对象 (UTC)"""
        try:
            if 'T' in time_str:
                # 确保 'Z' 被替换为 +00:00 以便 fromisoformat 正确解析
                clean_time = time_str.replace('Z', '+00:00')
                # fromisoformat 会自动创建时区感知的对象
                return datetime.fromisoformat(clean_time)
            # 如果没有时间信息，返回一个时区感知的当前UTC时间
            return datetime.now(timezone.utc)
        except Exception as e:
            self.logger.warning(f"时间解析失败: {time_str} - {e}")
            return datetime.now(timezone.utc)
    
    def _get_beijing_time(self):
        """获取北京时间（UTC+8）"""
        utc_time = datetime.now(timezone.utc)
        beijing_time = utc_time + timedelta(hours=8)
        return beijing_time.replace(tzinfo=None)

    def _is_meaningful_post(self, content: str, min_length: int = 15) -> bool:
        """
        判断一个帖子内容是否有意义
        - 检查长度
        - 检查是否为常见的无意义回复
        """
        if not content or not content.strip():
            return False

        # 1. 检查内容长度
        content_strip = content.strip()
        if len(content_strip) < min_length:
            self.logger.debug(f"过滤短内容: {content_strip}")
            return False

        # 2. 检查常见的无意义回复 (转换为小写以忽略大小写)
        meaningless_phrases = [
            'thanks', 'thank you', '感谢分享', '谢谢分享', '学习了',
            '支持', 'mark', '+1', '插眼', '好人一生平安'
        ]
        normalized_content = content_strip.lower()
        for phrase in meaningless_phrases:
            if phrase == normalized_content:
                self.logger.debug(f"过滤无意义回复: {content_strip}")
                return False
        
        return True
    
    def _build_json_url(self, base_url: str, page: int = 1) -> str:
        """构建JSON API URL"""
        clean_url = base_url.rstrip('/')
        if page > 1:
            return f"{clean_url}.json?page={page}"
        else:
            return f"{clean_url}.json"
    
    def _extract_topics_from_json(self, json_data: Dict[str, Any], base_url: str) -> List[Dict[str, Any]]:
        """从JSON数据中提取主题信息"""
        topics = []
        
        topic_list = json_data.get('topic_list', {})
        topics_data = topic_list.get('topics', [])
        users_data = json_data.get('users', [])
        users_map = {user['id']: user for user in users_data}
        
        for topic_data in topics_data:
            try:
                topic_id = topic_data.get('id')
                if not topic_id:
                    continue
                
                slug = topic_data.get('slug', '')
                full_url = f"https://linux.do/t/{slug}/{topic_id}"
                
                tags = topic_data.get('tags', [])
                tags_str = ','.join(tags) if tags else ''
                
                category_id = topic_data.get('category_id')
                category = str(category_id) if category_id else 'Unknown'
                
                beijing_timezone = timezone(timedelta(hours=8))
                created_at_utc = self._parse_datetime(topic_data.get('created_at', ''))
                last_posted_at_utc = self._parse_datetime(topic_data.get('last_posted_at')) if topic_data.get('last_posted_at') else created_at_utc

                topic_info = {
                    'id': topic_id,
                    'title': topic_data.get('title', ''),
                    'url': full_url,
                    'category': category,
                    'author_id': None,
                    'reply_count': topic_data.get('reply_count', 0),
                    'view_count': topic_data.get('views', 0),
                    'last_activity_at': last_posted_at_utc.astimezone(beijing_timezone).replace(tzinfo=None),
                    'created_at': created_at_utc.astimezone(beijing_timezone).replace(tzinfo=None),
                    'tags': tags_str,
                    '_last_activity_at_utc': last_posted_at_utc # 临时存储，用于后续比较
                }
                
                topics.append(topic_info)
                
            except Exception as e:
                self.logger.warning(f"解析主题项失败: {e}")
                continue
        
        return topics
    
    async def _crawl_single_page(self, client: PlaywrightClient, url: str, page_num: int) -> List[Dict[str, Any]]:
        """使用独立的页面实例爬取单个页面，包含重试逻辑。"""
        async with self.page_semaphore:
            json_url = self._build_json_url(url, page_num)
            self.logger.info(f"准备爬取页面: {json_url}")

            max_retries = self.crawler_config.get('max_retries', 3)
            for attempt in range(max_retries + 1):
                try:
                    async with client.get_page() as page:
                        self.logger.debug(f"请求URL: {json_url} (尝试 {attempt + 1}/{max_retries + 1})")
                        response = await page.goto(json_url, wait_until='networkidle')

                        if response and response.ok:
                            json_data = await response.json()
                            topics = self._extract_topics_from_json(json_data, url)
                            self.logger.info(f"页面 {page_num} 成功提取到 {len(topics)} 个主题")
                            # 成功后随机延迟
                            await asyncio.sleep(random.uniform(0.8, 1.5))
                            return topics
                        else:
                            status = response.status if response else 'No Response'
                            self.logger.warning(f"请求失败 (尝试 {attempt + 1}): {json_url} - 状态码: {status}")

                except Exception as e:
                    self.logger.warning(f"请求异常 (尝试 {attempt + 1}): {json_url} - {e}")

                if attempt < max_retries:
                    retry_delay = (2 ** attempt) + random.uniform(0, 1)
                    self.logger.info(f"将在 {retry_delay:.2f} 秒后重试...")
                    await asyncio.sleep(retry_delay)
            
            self.logger.error(f"获取JSON数据最终失败: {json_url}")
            return []
    
    async def _crawl_board_pages(self, client: PlaywrightClient, board_name: str, url: str) -> List[Dict[str, Any]]:
        """并发爬取单个板块的所有页面"""
        async with self.board_semaphore:
            self.logger.info(f"开始并发爬取板块: {board_name}")
            
            pages = self.crawler_config['scan_pages']
            
            # 创建所有页面的爬取任务
            page_tasks = []
            for page_num in range(1, pages + 1):
                task = self._crawl_single_page(client, url, page_num)
                page_tasks.append(task)
            
            # 并发执行所有页面爬取
            page_results = await asyncio.gather(*page_tasks, return_exceptions=True)
            
            # 合并结果
            all_topics = []
            for i, result in enumerate(page_results, 1):
                if isinstance(result, Exception):
                    self.logger.error(f"板块 {board_name} 页面 {i} 爬取失败: {result}")
                else:
                    all_topics.extend(result)
            
            self.logger.info(f"板块 {board_name} 总共提取到 {len(all_topics)} 个主题")
            return all_topics
    
    async def crawl_all_topic_lists(self) -> List[str]:
        """并发爬取所有板块的主题列表"""
        self.logger.info("开始并发爬取所有板块主题列表")
        
        async with PlaywrightClient() as client:
            # 创建所有板块的爬取任务
            board_tasks = []
            for board_name, url in self.target_urls.items():
                task = self._crawl_board_pages(client, board_name, url)
                board_tasks.append((board_name, task))
            
            # 并发执行所有板块爬取
            board_results = await asyncio.gather(*[task for _, task in board_tasks], return_exceptions=True)
            
            # 合并所有主题
            all_topics = []
            for i, (board_name, _) in enumerate(board_tasks):
                result = board_results[i]
                if isinstance(result, Exception):
                    self.logger.error(f"板块 {board_name} 爬取失败: {result}")
                else:
                    all_topics.extend(result)
            
            self.logger.info(f"所有板块总共提取到 {len(all_topics)} 个主题")
        
        # 批量判断哪些主题需要详细爬取
        if not all_topics:
            return []
        
        topic_ids = [topic['id'] for topic in all_topics]
        db_last_activities = db_manager.get_topics_last_activity_batch(topic_ids)
        
        topics_to_crawl = []
        for topic_data in all_topics:
            topic_id = topic_data['id']
            db_last_activity = db_last_activities.get(topic_id)
            
            should_crawl = False
            if db_last_activity is None:
                should_crawl = True
                self.logger.debug(f"新主题需要爬取: {topic_id}")
            else:
                # 从临时字段获取原始的、带时区的UTC时间
                current_last_activity_utc = topic_data.get('_last_activity_at_utc')
                if not current_last_activity_utc:
                    # 作为备用，重新解析
                    current_last_activity_utc = self._parse_datetime(topic_data.get('last_activity_at', ''))

                # 将新的UTC时间转换为无时区的北京时间，用于比较
                beijing_timezone = timezone(timedelta(hours=8))
                current_last_activity_naive_bjt = current_last_activity_utc.astimezone(beijing_timezone).replace(tzinfo=None)
                
                # 现在与数据库中的无时区北京时间进行比较
                if current_last_activity_naive_bjt > db_last_activity:
                    should_crawl = True
                    self.logger.debug(f"主题有更新需要爬取: {topic_id} (Web: {current_last_activity_naive_bjt} > DB: {db_last_activity})")
            
            if should_crawl:
                topics_to_crawl.append(topic_data['url'])
        
        # 批量保存主题基本信息
        try:
            db_manager.batch_insert_or_update_topics(all_topics)
            self.logger.info(f"批量保存 {len(all_topics)} 个主题基本信息成功")
        except Exception as e:
            self.logger.error(f"批量保存主题基本信息失败: {e}")
        
        self.logger.info(f"需要详细爬取 {len(topics_to_crawl)} 个主题")
        return topics_to_crawl
    
    async def _get_json_with_retry(self, client: PlaywrightClient, url: str) -> Optional[Dict[str, Any]]:
        """封装了重试逻辑的JSON获取方法"""
        max_retries = self.crawler_config.get('max_retries', 3)
        for attempt in range(max_retries + 1):
            try:
                async with client.get_page() as page:
                    self.logger.debug(f"请求URL: {url} (尝试 {attempt + 1}/{max_retries + 1})")
                    response = await page.goto(url, wait_until='networkidle')
                    if response and response.ok:
                        return await response.json()
                    status = response.status if response else 'No Response'
                    self.logger.warning(f"请求失败 (尝试 {attempt + 1}): {url} - 状态码: {status}")
            except Exception as e:
                self.logger.warning(f"请求异常 (尝试 {attempt + 1}): {url} - {e}")

            if attempt < max_retries:
                retry_delay = (2 ** attempt) + random.uniform(0, 1)
                await asyncio.sleep(retry_delay)

        self.logger.error(f"请求JSON最终失败: {url}")
        return None

    async def _crawl_single_topic_detail(self, client: PlaywrightClient, topic_url: str) -> bool:
        """使用独立的页面实例爬取单个主题详情，包含分页和重试。"""
        async with self.detail_semaphore:
            base_json_url = topic_url.replace('/t/', '/t/').rstrip('/') + '.json'
            self.logger.info(f"开始爬取主题详情: {topic_url}")

            try:
                # 获取第一页数据
                json_data = await self._get_json_with_retry(client, base_json_url)
                if not json_data:
                    return False

                topic_id = json_data.get('id')
                if not topic_id:
                    self.logger.warning(f"无法从 {base_json_url} 中获取 topic_id")
                    return False

                all_users = self._extract_users_from_json(json_data)
                all_posts = self._extract_posts_from_json(json_data, topic_id)
                
                # 检查是否需要分页
                post_stream = json_data.get('post_stream', {})
                posts_in_stream = post_stream.get('posts', [])
                total_posts_count = json_data.get('posts_count', len(posts_in_stream))

                if posts_in_stream and len(posts_in_stream) < total_posts_count:
                    posts_per_page = len(posts_in_stream)
                    total_pages = (total_posts_count + posts_per_page - 1) // posts_per_page
                    self.logger.info(f"主题 {topic_id} 有 {total_posts_count} 个回复，共 {total_pages} 页，将进行分页爬取。")

                    for page_num in range(2, total_pages + 1):
                        page_url = f"{base_json_url}?page={page_num}"
                        page_data = await self._get_json_with_retry(client, page_url)
                        if page_data:
                            all_users.extend(self._extract_users_from_json(page_data))
                            all_posts.extend(self._extract_posts_from_json(page_data, topic_id))
                            self.logger.debug(f"第 {page_num} 页获取成功")
                        else:
                            self.logger.warning(f"第 {page_num} 页数据获取失败")
                        await asyncio.sleep(random.uniform(0.5, 1.0))

                # 数据处理与入库
                if all_users:
                    unique_users = {user['id']: user for user in all_users if user.get('id')}
                    db_manager.batch_insert_users(list(unique_users.values()))
                
                topic_info = self._extract_topic_info_from_json(json_data)
                if topic_info:
                    db_manager.insert_or_update_topic(topic_info)
                
                if all_posts:
                    db_manager.batch_insert_posts(all_posts)

                self.logger.info(f"主题详情爬取成功: {topic_url}")
                await asyncio.sleep(random.uniform(1.0, 2.0))
                return True

            except Exception as e:
                self.logger.error(f"爬取主题详情时发生严重错误: {topic_url} - {e}", exc_info=True)
                return False
    
    def _extract_users_from_json(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """从JSON数据中提取用户信息"""
        users = []
        unique_users = {}
        
        if 'details' in json_data and 'participants' in json_data['details']:
            for user_data in json_data['details']['participants']:
                user_info = {
                    'id': user_data.get('id'),
                    'username': user_data.get('username'),
                    'avatar_url': user_data.get('avatar_template', '').replace('{size}', '120') if user_data.get('avatar_template') else None
                }
                if user_info['id'] and user_info['username']:
                    unique_users[user_info['id']] = user_info
        
        if 'post_stream' in json_data and 'posts' in json_data['post_stream']:
            for post in json_data['post_stream']['posts']:
                if 'user_id' in post and 'username' in post:
                    user_info = {
                        'id': post['user_id'],
                        'username': post['username'],
                        'avatar_url': post.get('avatar_template', '').replace('{size}', '120') if post.get('avatar_template') else None
                    }
                    unique_users[user_info['id']] = user_info
        
        return list(unique_users.values())
    
    def _extract_topic_info_from_json(self, json_data: Dict[str, Any]) -> Dict[str, Any]:
        """从JSON数据中提取主题信息"""
        try:
            beijing_timezone = timezone(timedelta(hours=8))
            created_at_utc = self._parse_datetime(json_data.get('created_at', ''))
            last_activity_at_utc = self._parse_datetime(json_data.get('last_posted_at', ''))

            topic_data = {
                'id': json_data.get('id'),
                'title': json_data.get('title'),
                'url': f"https://linux.do/t/{json_data.get('slug', '')}/{json_data.get('id')}",
                'category': json_data.get('category_id'),
                'author_id': None,
                'reply_count': json_data.get('reply_count', 0),
                'view_count': json_data.get('views', 0),
                'created_at': created_at_utc.astimezone(beijing_timezone).replace(tzinfo=None),
                'last_activity_at': last_activity_at_utc.astimezone(beijing_timezone).replace(tzinfo=None),
                'tags': ','.join(json_data.get('tags', []))
            }
            
            if ('post_stream' in json_data and 
                'posts' in json_data['post_stream'] and 
                len(json_data['post_stream']['posts']) > 0):
                first_post = json_data['post_stream']['posts'][0]
                topic_data['author_id'] = first_post.get('user_id')
            
            return topic_data
            
        except Exception as e:
            self.logger.error(f"解析主题信息失败: {e}")
            return None
    
    def _extract_posts_from_json(self, json_data: Dict[str, Any], topic_id: int) -> List[Dict[str, Any]]:
        """从JSON数据中提取帖子和回复信息"""
        posts = []
        
        if 'post_stream' not in json_data or 'posts' not in json_data['post_stream']:
            return posts
        
        for post_data in json_data['post_stream']['posts']:
            try:
                # 从 'cooked' 字段获取HTML内容并转换为Markdown
                html_content = post_data.get('cooked', '')
                # 将HTML转换为Markdown
                markdown_content = html_to_markdown.convert(html_content)

                # 过滤无意义的帖子
                if not self._is_meaningful_post(markdown_content):
                    self.logger.debug(f"过滤无意义帖子: Post ID {post_data.get('id')}")
                    continue

                like_count = 0
                actions_summary = post_data.get('actions_summary', [])
                if actions_summary and len(actions_summary) > 0:
                    first_action = actions_summary[0]
                    if first_action.get('id') == 2:
                        like_count = first_action.get('count', 0)
                
                beijing_timezone = timezone(timedelta(hours=8))
                created_at_utc = self._parse_datetime(post_data.get('created_at', ''))

                post_info = {
                    'id': post_data.get('id'),
                    'topic_id': topic_id,
                    'user_id': post_data.get('user_id'),
                    'post_number': post_data.get('post_number'),
                    'reply_to_post_number': post_data.get('reply_to_post_number'),
                    'content_raw': markdown_content,
                    'like_count': like_count,
                    'created_at': created_at_utc.astimezone(beijing_timezone).replace(tzinfo=None)
                }
                
                if post_info['id'] and post_info['post_number']:
                    posts.append(post_info)
                    
            except Exception as e:
                self.logger.warning(f"解析帖子数据失败: {e}")
                continue
        
        return posts
    
    async def _topic_detail_worker(self, name: str, client: PlaywrightClient, queue: asyncio.Queue, results: list):
        """消费者worker，从队列中获取URL并执行爬取"""
        while True:
            try:
                topic_url = await queue.get()
                self.logger.info(f"Worker [{name}] 开始处理: {topic_url} (队列剩余: {queue.qsize()})")

                is_success = await self._crawl_single_topic_detail(client, topic_url)
                if is_success:
                    results.append(topic_url)

                queue.task_done()
                self.logger.info(f"Worker [{name}] 完成处理: {topic_url}")

            except asyncio.CancelledError:
                self.logger.debug(f"Worker [{name}] 被取消")
                break
            except Exception as e:
                self.logger.error(f"Worker [{name}] 处理时发生严重错误: {e}", exc_info=True)
                # 即使出错，也要标记任务完成，避免队列阻塞
                queue.task_done()

    async def crawl_topics_details_concurrent(self, topic_urls: List[str]) -> Tuple[int, int]:
        """使用生产者-消费者模式并发爬取主题详情"""
        if not topic_urls:
            return 0, 0

        total_count = len(topic_urls)
        self.logger.info(f"启动生产者-消费者模式，并发爬取 {total_count} 个主题详情")
        
        results = []
        queue = asyncio.Queue()
        for url in topic_urls:
            queue.put_nowait(url)

        async with PlaywrightClient() as client:
            # 创建并启动消费者worker
            num_workers = self.max_concurrent_details
            workers = [
                asyncio.create_task(self._topic_detail_worker(f"worker-{i+1}", client, queue, results))
                for i in range(num_workers)
            ]
            self.logger.info(f"已启动 {num_workers} 个消费者worker")

            # 等待队列中的所有任务被处理
            await queue.join()
            self.logger.info("所有主题已处理完毕，队列为空")

            # 所有任务完成后，取消worker
            for worker in workers:
                worker.cancel()
            
            await asyncio.gather(*workers, return_exceptions=True)
            self.logger.info("所有worker已安全停止")

        success_count = len(results)
        self.logger.info(f"并发爬取完成: 成功 {success_count}/{total_count}")
        return success_count, total_count

