"""
自动化数据清理模块
负责管理数据库存储空间，清理过期数据
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

from .config import config
from .database import db_manager


class DataCleaner:
    """数据清理器"""
    
    def __init__(self):
        self.retention_days = config.get_data_retention_days()
        self.logger = logging.getLogger(__name__)
    
    def _get_beijing_time(self):
        """获取北京时间（UTC+8）"""
        utc_time = datetime.now(timezone.utc)
        beijing_time = utc_time + timedelta(hours=8)
        return beijing_time.replace(tzinfo=None)
    
    def clean_expired_data(self, retention_days: int = None) -> Dict[str, Any]:
        """清理过期数据"""
        if retention_days is None:
            retention_days = self.retention_days
        
        self.logger.info(f"开始清理 {retention_days} 天前的数据")
        start_time = self._get_beijing_time()
        
        try:
            # 清理过期主题及其相关数据
            deleted_count = db_manager.clean_old_data(retention_days)
            
            end_time = self._get_beijing_time()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'success': True,
                'deleted_topics': deleted_count,
                'retention_days': retention_days,
                'start_time': start_time,
                'end_time': end_time,
                'duration_seconds': duration
            }
            
            self.logger.info(f"数据清理完成: 删除了 {deleted_count} 个过期主题，耗时 {duration:.2f} 秒")
            return result
            
        except Exception as e:
            self.logger.error(f"数据清理失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'retention_days': retention_days,
                'start_time': start_time,
                'end_time': self._get_beijing_time()
            }
    
    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        try:
            with db_manager.get_cursor() as (cursor, connection):
                stats = {}
                
                # 统计用户数量
                cursor.execute("SELECT COUNT(*) as count FROM users")
                stats['users_count'] = cursor.fetchone()['count']
                
                # 统计主题数量
                cursor.execute("SELECT COUNT(*) as count FROM topics")
                stats['topics_count'] = cursor.fetchone()['count']
                
                # 统计回复数量
                cursor.execute("SELECT COUNT(*) as count FROM posts")
                stats['posts_count'] = cursor.fetchone()['count']
                
                # 统计最新数据时间
                cursor.execute("SELECT MAX(last_activity_at) as latest FROM topics")
                result = cursor.fetchone()
                stats['latest_activity'] = result['latest'] if result['latest'] else None
                
                # 统计最旧数据时间
                cursor.execute("SELECT MIN(last_activity_at) as oldest FROM topics")
                result = cursor.fetchone()
                stats['oldest_activity'] = result['oldest'] if result['oldest'] else None
                
                # 统计今天的数据量（使用北京时间）
                cursor.execute("""
                    SELECT COUNT(*) as count FROM topics 
                    WHERE DATE(last_activity_at) = DATE(CURRENT_TIMESTAMP + INTERVAL 8 HOUR)
                """)
                stats['today_topics'] = cursor.fetchone()['count']
                
                return stats
                
        except Exception as e:
            self.logger.error(f"获取数据库统计信息失败: {e}")
            return {}
    
    def cleanup_orphaned_data(self) -> Dict[str, Any]:
        """清理孤立数据"""
        self.logger.info("开始清理孤立数据")
        start_time = self._get_beijing_time()
        
        try:
            with db_manager.get_cursor() as (cursor, connection):
                # 清理没有关联主题的回复（理论上不应该存在，因为有外键约束）
                cursor.execute("""
                    DELETE p FROM posts p
                    LEFT JOIN topics t ON p.topic_id = t.id
                    WHERE t.id IS NULL
                """)
                orphaned_posts = cursor.rowcount
                
                # 清理没有关联用户的数据（设置为NULL，不删除）
                cursor.execute("""
                    UPDATE topics SET author_id = NULL
                    WHERE author_id NOT IN (SELECT id FROM users)
                """)
                orphaned_topic_authors = cursor.rowcount
                
                cursor.execute("""
                    UPDATE posts SET user_id = NULL
                    WHERE user_id NOT IN (SELECT id FROM users)
                """)
                orphaned_post_authors = cursor.rowcount
                
                connection.commit()
                
                end_time = self._get_beijing_time()
                duration = (end_time - start_time).total_seconds()
                
                result = {
                    'success': True,
                    'orphaned_posts_deleted': orphaned_posts,
                    'orphaned_topic_authors_fixed': orphaned_topic_authors,
                    'orphaned_post_authors_fixed': orphaned_post_authors,
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration_seconds': duration
                }
                
                self.logger.info(f"孤立数据清理完成: 删除 {orphaned_posts} 个孤立回复，"
                               f"修复 {orphaned_topic_authors} 个主题作者，"
                               f"修复 {orphaned_post_authors} 个回复作者，"
                               f"耗时 {duration:.2f} 秒")
                
                return result
                
        except Exception as e:
            self.logger.error(f"清理孤立数据失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'start_time': start_time,
                'end_time': self._get_beijing_time()
            }


# 全局数据清理器实例
data_cleaner = DataCleaner()