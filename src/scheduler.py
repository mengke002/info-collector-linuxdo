"""
调度与执行模块
负责编排和自动化所有任务
"""
import logging
from datetime import datetime
from typing import Dict, Any

from .logger import setup_logging, log_task_start, log_task_end, log_error
from .database import db_manager
from .data_cleaner import data_cleaner
from .analyzer import hotness_analyzer
from .report_generator import report_generator
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
            # 延迟导入ConcurrentCrawler，避免非爬虫任务因playwright缺失而失败
            try:
                from .concurrent_crawler import ConcurrentCrawler
            except ImportError as e:
                error_msg = f"无法导入爬虫模块：{e}。请确保已安装playwright: pip install playwright && playwright install"
                self.logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg
                }
            
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
            # 初始化数据库（确保表结构存在）
            self.logger.info("初始化数据库...")
            db_manager.init_database()
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
            # 初始化数据库（确保表结构存在）
            self.logger.info("初始化数据库...")
            db_manager.init_database()
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
    
    def run_analysis_task(self, hours_back: int = 24, analyze_all: bool = False) -> Dict[str, Any]:
        """执行热度分析任务"""
        task_name = f"热度分析 ({'全量' if analyze_all else f'最近{hours_back}小时'})"
        start_time = log_task_start(task_name)
        
        try:
            # 初始化数据库（确保表结构存在）
            self.logger.info("初始化数据库...")
            db_manager.init_database()
            if analyze_all:
                # 分析所有主题
                result = hotness_analyzer.analyze_all_topics()
            else:
                # 分析最近活跃的主题
                result = hotness_analyzer.analyze_recent_topics(hours_back)
            
            # 获取热度统计
            stats_result = hotness_analyzer.get_hotness_stats()
            result['hotness_stats'] = stats_result
            
            log_task_end(task_name, start_time,
                        analyzed_topics=result.get('analyzed_topics', result.get('updated_scores', 0)))
            
            return result
            
        except Exception as e:
            log_error(task_name, e)
            return {
                'success': False,
                'error': str(e)
            }
    
    async def run_report_task(self, category: str = None, hours_back: int = 24) -> Dict[str, Any]:
        """执行智能分析报告任务"""
        if category:
            task_name = f"智能分析报告 - {category}板块"
        else:
            task_name = "智能分析报告 - 所有板块"
        
        start_time = log_task_start(task_name)
        
        try:
            # 初始化数据库（确保reports表存在）
            self.logger.info("初始化数据库...")
            db_manager.init_database()
            if category:
                # 生成指定分类的报告
                result = await report_generator.generate_category_report(category, hours_back)
            else:
                # 生成所有分类的报告
                result = await report_generator.generate_all_categories_report(hours_back)
            
            if result.get('success'):
                if category:
                    log_task_end(task_name, start_time, 
                                topics_analyzed=result.get('topics_analyzed', 0))
                else:
                    log_task_end(task_name, start_time,
                                total_reports=result.get('successful_reports', 0),
                                total_topics=result.get('total_topics_analyzed', 0))
            
            return result
            
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
            
            # 2. 执行热度分析任务（分析爬取到的数据）
            self.logger.info("执行热度分析任务...")
            results['analysis'] = self.run_analysis_task(hours_back=24, analyze_all=False)
            
            # 3. 执行清理任务
            self.logger.info("执行清理任务...")
            results['cleanup'] = self.run_cleanup_task()
            
            # 4. 获取最终统计
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
