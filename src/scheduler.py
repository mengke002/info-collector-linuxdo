"""
调度与执行模块
负责编排和自动化所有任务
"""
import logging
from datetime import datetime
from typing import Dict, Any

from .logger import setup_logging, log_task_start, log_task_end, log_error
from .database import db_manager
from .concurrent_crawler import ConcurrentCrawler
from .data_cleaner import data_cleaner
from .config import config


class TaskScheduler:
    """任务调度器"""
    
    def __init__(self):
        # 设置日志
        setup_logging()
        self.logger = logging.getLogger(__name__)
    
    async def run_crawl_task(self, use_concurrent: bool = True) -> Dict[str, Any]:
        """执行爬取任务"""
        task_name = "增量数据爬取" + (" (并发)" if use_concurrent else " (串行)")
        start_time = log_task_start(task_name)
        
        try:
            # 初始化数据库
            self.logger.info("初始化数据库...")
            db_manager.init_database()
            
            # 根据模式选择或创建爬虫实例
            if use_concurrent:
                crawler_config = config.get_crawler_config()
                crawler = ConcurrentCrawler(
                    max_concurrent_boards=crawler_config['max_concurrent_boards'],
                    max_concurrent_pages=crawler_config['max_concurrent_pages'],
                    max_concurrent_details=crawler_config['max_concurrent_details']
                )
                mode_name = "并发"
            else:
                # 串行模式，所有并发数强制为1
                crawler = ConcurrentCrawler(max_concurrent_boards=1, max_concurrent_pages=1, max_concurrent_details=1)
                mode_name = "串行"
            
            # 步骤1: 爬取主题列表
            self.logger.info(f"步骤1: {mode_name}爬取主题列表...")
            topics_to_crawl = await crawler.crawl_all_topic_lists()
            
            if not topics_to_crawl:
                self.logger.info("没有需要更新的主题")
                log_task_end(task_name, start_time, topics_found=0)
                return {
                    'success': True,
                    'topics_found': 0,
                    'topics_crawled': 0,
                    'message': '没有需要更新的主题'
                }
            
            # 步骤2: 爬取主题详情
            self.logger.info(f"步骤2: {mode_name}爬取 {len(topics_to_crawl)} 个主题详情...")
            success_count, total_count = await crawler.crawl_topics_details_concurrent(topics_to_crawl)
            
            # 记录任务完成
            log_task_end(task_name, start_time, 
                        topics_found=len(topics_to_crawl),
                        topics_crawled=success_count)
            
            return {
                'success': True,
                'topics_found': len(topics_to_crawl),
                'topics_crawled': success_count,
                'success_rate': f"{success_count}/{total_count}",
                'concurrent_mode': use_concurrent
            }
            
        except Exception as e:
            log_error(task_name, e)
            return {
                'success': False,
                'error': str(e)
            }
    
    def run_cleanup_task(self, retention_days: int = None) -> Dict[str, Any]:
        """执行数据清理任务"""
        task_name = "数据清理"
        start_time = log_task_start(task_name)
        
        try:
            # 获取清理前的统计信息
            stats_before = data_cleaner.get_database_stats()
            self.logger.info(f"清理前统计: {stats_before}")
            
            # 执行数据清理
            cleanup_result = data_cleaner.clean_expired_data(retention_days)
            
            # 清理孤立数据
            orphan_result = data_cleaner.cleanup_orphaned_data()
            
            # 获取清理后的统计信息
            stats_after = data_cleaner.get_database_stats()
            self.logger.info(f"清理后统计: {stats_after}")
            
            log_task_end(task_name, start_time,
                        deleted_topics=cleanup_result.get('deleted_topics', 0))
            
            return {
                'success': True,
                'cleanup_result': cleanup_result,
                'orphan_result': orphan_result,
                'stats_before': stats_before,
                'stats_after': stats_after
            }
            
        except Exception as e:
            log_error(task_name, e)
            return {
                'success': False,
                'error': str(e)
            }
    
    def run_stats_task(self) -> Dict[str, Any]:
        """执行统计任务"""
        task_name = "数据统计"
        start_time = log_task_start(task_name)
        
        try:
            stats = data_cleaner.get_database_stats()
            
            log_task_end(task_name, start_time)
            
            return {
                'success': True,
                'stats': stats
            }
            
        except Exception as e:
            log_error(task_name, e)
            return {
                'success': False,
                'error': str(e)
            }
    
    async def run_full_maintenance(self) -> Dict[str, Any]:
        """执行完整维护任务"""
        task_name = "完整维护"
        start_time = log_task_start(task_name)
        
        try:
            results = {}
            
            # 1. 执行爬取任务
            self.logger.info("执行爬取任务...")
            results['crawl'] = await self.run_crawl_task()
            
            # 2. 执行清理任务
            self.logger.info("执行清理任务...")
            results['cleanup'] = self.run_cleanup_task()
            
            # 3. 获取最终统计
            self.logger.info("获取统计信息...")
            results['stats'] = self.run_stats_task()
            
            log_task_end(task_name, start_time)
            
            return {
                'success': True,
                'results': results
            }
            
        except Exception as e:
            log_error(task_name, e)
            return {
                'success': False,
                'error': str(e)
            }


# 全局任务调度器实例
scheduler = TaskScheduler()
