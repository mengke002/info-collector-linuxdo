"""
数据库模块
负责MySQL数据库连接、表创建和数据持久化操作
"""
import pymysql
import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta

from .config import config


class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self):
        self.db_config = config.get_database_config()
        self.logger = logging.getLogger(__name__)
    
    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)
    
    def _sanitize_user_data(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """清理和验证用户数据，确保符合数据库字段限制"""
        sanitized = user_data.copy()
        
        # 截断字符串字段
        if 'username' in sanitized and sanitized['username']:
            original = str(sanitized['username'])
            sanitized['username'] = original[:50]
            if len(original) > 50:
                self.logger.warning(f"用户名被截断: {original[:20]}... -> {sanitized['username']}")
        
        if 'avatar_url' in sanitized and sanitized['avatar_url']:
            original = str(sanitized['avatar_url'])
            sanitized['avatar_url'] = original[:200]
            if len(original) > 200:
                self.logger.warning(f"头像URL被截断: {original[:50]}...")
        
        return sanitized
    
    def _sanitize_topic_data(self, topic_data: Dict[str, Any]) -> Dict[str, Any]:
        """清理和验证主题数据，确保符合数据库字段限制"""
        sanitized = topic_data.copy()
        
        # 截断字符串字段
        if 'title' in sanitized and sanitized['title']:
            original = str(sanitized['title'])
            sanitized['title'] = original[:500]
            if len(original) > 500:
                self.logger.warning(f"标题被截断: {original[:50]}...")
        
        if 'url' in sanitized and sanitized['url']:
            original = str(sanitized['url'])
            sanitized['url'] = original[:200]
            if len(original) > 200:
                self.logger.warning(f"URL被截断: {original}")
        
        if 'category' in sanitized and sanitized['category']:
            original = str(sanitized['category'])
            sanitized['category'] = original[:50]
            if len(original) > 50:
                self.logger.warning(f"分类名被截断: {original}")
        
        if 'tags' in sanitized and sanitized['tags']:
            original = str(sanitized['tags'])
            sanitized['tags'] = original[:500]
            if len(original) > 500:
                self.logger.warning(f"标签被截断: {original[:50]}...")
        
        # 限制数值字段范围
        if 'reply_count' in sanitized:
            original_count = int(sanitized['reply_count'] or 0)
            sanitized['reply_count'] = min(max(original_count, 0), 65535)
            if original_count > 65535:
                self.logger.warning(f"回复数超出范围被限制: {original_count} -> 65535")
        
        if 'view_count' in sanitized:
            original_count = int(sanitized['view_count'] or 0)
            sanitized['view_count'] = min(max(original_count, 0), 4294967295)
            if original_count > 4294967295:
                self.logger.warning(f"浏览数超出范围被限制: {original_count} -> 4294967295")
        
        return sanitized
    
    def _sanitize_post_data(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """清理和验证帖子数据，确保符合数据库字段限制"""
        sanitized = post_data.copy()
        
        # 限制数值字段范围
        if 'post_number' in sanitized:
            original_num = int(sanitized['post_number'] or 1)
            sanitized['post_number'] = min(max(original_num, 1), 65535)
            if original_num > 65535:
                self.logger.warning(f"楼层号超出范围被限制: {original_num} -> 65535")
        
        if 'reply_to_post_number' in sanitized and sanitized['reply_to_post_number']:
            original_num = int(sanitized['reply_to_post_number'])
            sanitized['reply_to_post_number'] = min(max(original_num, 1), 65535)
            if original_num > 65535:
                self.logger.warning(f"回复楼层号超出范围被限制: {original_num} -> 65535")
        
        if 'like_count' in sanitized:
            original_count = int(sanitized['like_count'] or 0)
            sanitized['like_count'] = min(max(original_count, 0), 255)
            if original_count > 255:
                self.logger.warning(f"点赞数超出范围被限制: {original_count} -> 255")
        
        return sanitized
    
    def get_connection(self):
        """获取数据库连接"""
        try:
            # 根据配置决定SSL是否启用
            ssl_enabled = self.db_config.get('ssl_mode', 'disabled').lower() != 'disabled'

            # 从配置中安全地获取数据库连接参数
            db_host = self.db_config.get('host')
            db_port_str = self.db_config.get('port')
            db_user = self.db_config.get('user')
            db_password = self.db_config.get('password')
            db_name = self.db_config.get('database')

            # 确保端口是整数
            db_port = int(db_port_str) if db_port_str else 3306

            connection = pymysql.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                database=db_name,
                charset='utf8mb4',
                ssl={} if ssl_enabled else None,  # 传递字典启用SSL，None禁用
                autocommit=False
            )
            return connection
        except Exception as e:
            self.logger.error(f"数据库连接失败: {e}")
            raise
    
    @contextmanager
    def get_cursor(self):
        """获取数据库游标的上下文管理器"""
        connection = None
        cursor = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            yield cursor, connection
        except Exception as e:
            if connection:
                connection.rollback()
            self.logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def init_database(self):
        """初始化数据库表结构"""
        create_tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS users (
                id INT PRIMARY KEY COMMENT '用户在论坛的唯一ID',
                username VARCHAR(50) UNIQUE NOT NULL COMMENT '用户名',
                avatar_url VARCHAR(200) COMMENT '头像URL',
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '首次采集到该用户的时间'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,
            """
            CREATE TABLE IF NOT EXISTS topics (
                id INT PRIMARY KEY COMMENT '帖子在论坛的唯一ID',
                title VARCHAR(500) NOT NULL COMMENT '帖子标题',
                url VARCHAR(200) UNIQUE NOT NULL COMMENT '帖子URL',
                category VARCHAR(50) COMMENT '分类名称',
                author_id INT COMMENT '作者用户ID',
                reply_count SMALLINT UNSIGNED DEFAULT 0 COMMENT '回复数',
                view_count INT UNSIGNED DEFAULT 0 COMMENT '浏览数',
                created_at TIMESTAMP NOT NULL COMMENT '帖子创建时间',
                last_activity_at TIMESTAMP NOT NULL COMMENT '最后活跃时间',
                tags VARCHAR(500) COMMENT '标签，逗号分隔',
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录首次抓取和更新的时间',
                
                INDEX idx_last_activity (last_activity_at),
                INDEX idx_created_at (created_at),
                INDEX idx_reply_count (reply_count),
                FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """,
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INT PRIMARY KEY COMMENT '回复在论坛的唯一ID',
                topic_id INT NOT NULL COMMENT '所属帖子的ID',
                user_id INT COMMENT '回复用户的ID',
                post_number SMALLINT UNSIGNED NOT NULL COMMENT '楼层号',
                reply_to_post_number SMALLINT UNSIGNED COMMENT '回复目标的楼层号，主楼则为NULL',
                content_raw MEDIUMTEXT COMMENT '原始文本内容（Markdown等），对AI至关重要',
                like_count TINYINT UNSIGNED DEFAULT 0 COMMENT '点赞数',
                created_at TIMESTAMP NOT NULL COMMENT '本条回复的创建时间',
                
                UNIQUE KEY uk_topic_post (topic_id, post_number),
                INDEX idx_user (user_id),
                INDEX idx_created_at (created_at),
                
                FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
        ]
        
        with self.get_cursor() as (cursor, connection):
            for sql in create_tables_sql:
                cursor.execute(sql)
            connection.commit()
            self.logger.info("数据库表结构初始化完成")
    
    def insert_or_update_user(self, user_data: Dict[str, Any]):
        """插入或忽略用户数据"""
        # 清理数据
        sanitized_data = self._sanitize_user_data(user_data)
        
        # 如果没有提供首次看到时间，使用当前北京时间
        if 'first_seen_at' not in sanitized_data:
            sanitized_data['first_seen_at'] = self.get_beijing_time()
        
        sql = """
        INSERT IGNORE INTO users (id, username, avatar_url, first_seen_at)
        VALUES (%(id)s, %(username)s, %(avatar_url)s, %(first_seen_at)s)
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, sanitized_data)
            connection.commit()
    
    def insert_or_update_topic(self, topic_data: Dict[str, Any]):
        """插入或更新主题数据"""
        # 清理数据
        sanitized_data = self._sanitize_topic_data(topic_data)
        
        with self.get_cursor() as (cursor, connection):
            # 如果有作者信息，先插入用户
            if sanitized_data.get('author_id') and sanitized_data.get('author_username'):
                user_data = self._sanitize_user_data({
                    'id': sanitized_data['author_id'],
                    'username': sanitized_data['author_username'],
                    'avatar_url': None,
                    'first_seen_at': self.get_beijing_time()
                })
                user_sql = """
                INSERT IGNORE INTO users (id, username, avatar_url, first_seen_at)
                VALUES (%(id)s, %(username)s, %(avatar_url)s, %(first_seen_at)s)
                """
                cursor.execute(user_sql, user_data)
            
            # 插入或更新主题，使用北京时间作为抓取时间
            topic_sql = """
            INSERT INTO topics (id, title, url, category, author_id, reply_count, view_count, 
                               created_at, last_activity_at, tags, crawled_at)
            VALUES (%(id)s, %(title)s, %(url)s, %(category)s, %(author_id)s, %(reply_count)s, 
                    %(view_count)s, %(created_at)s, %(last_activity_at)s, %(tags)s, %(crawled_at)s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                category = VALUES(category),
                reply_count = VALUES(reply_count),
                view_count = VALUES(view_count),
                last_activity_at = VALUES(last_activity_at),
                tags = VALUES(tags),
                crawled_at = %(crawled_at)s
            """
            
            # 如果没有提供抓取时间，使用当前北京时间
            if 'crawled_at' not in sanitized_data:
                sanitized_data['crawled_at'] = self.get_beijing_time()
            
            cursor.execute(topic_sql, sanitized_data)
            connection.commit()
    
    def insert_or_update_post(self, post_data: Dict[str, Any]):
        """插入或更新帖子回复数据"""
        # 清理数据
        sanitized_data = self._sanitize_post_data(post_data)
        
        sql = """
        INSERT INTO posts (id, topic_id, user_id, post_number, reply_to_post_number,
                          content_raw, like_count, created_at)
        VALUES (%(id)s, %(topic_id)s, %(user_id)s, %(post_number)s, %(reply_to_post_number)s,
                %(content_raw)s, %(like_count)s, %(created_at)s)
        ON DUPLICATE KEY UPDATE
            content_raw = VALUES(content_raw),
            like_count = VALUES(like_count)
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, sanitized_data)
            connection.commit()
    
    def batch_insert_users(self, users_data: List[Dict[str, Any]]):
        """批量插入用户数据"""
        if not users_data:
            return
        
        # 清理和验证所有用户数据
        beijing_time = self.get_beijing_time()
        sanitized_users = []
        
        for user_data in users_data:
            sanitized = self._sanitize_user_data(user_data)
            if 'first_seen_at' not in sanitized:
                sanitized['first_seen_at'] = beijing_time
            sanitized_users.append(sanitized)
        
        sql = """
        INSERT IGNORE INTO users (id, username, avatar_url, first_seen_at)
        VALUES (%(id)s, %(username)s, %(avatar_url)s, %(first_seen_at)s)
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.executemany(sql, sanitized_users)
            connection.commit()
            self.logger.info(f"批量插入 {len(sanitized_users)} 个用户")
    
    def batch_insert_posts(self, posts_data: List[Dict[str, Any]]):
        """批量插入帖子回复数据"""
        if not posts_data:
            return
        
        # 清理和验证所有帖子数据
        sanitized_posts = []
        for post_data in posts_data:
            sanitized = self._sanitize_post_data(post_data)
            sanitized_posts.append(sanitized)
        
        sql = """
        INSERT INTO posts (id, topic_id, user_id, post_number, reply_to_post_number,
                          content_raw, like_count, created_at)
        VALUES (%(id)s, %(topic_id)s, %(user_id)s, %(post_number)s, %(reply_to_post_number)s,
                %(content_raw)s, %(like_count)s, %(created_at)s)
        ON DUPLICATE KEY UPDATE
            content_raw = VALUES(content_raw),
            like_count = VALUES(like_count)
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.executemany(sql, sanitized_posts)
            connection.commit()
            self.logger.info(f"批量插入 {len(sanitized_posts)} 个回复")
    
    def get_topic_last_activity(self, topic_id: int) -> Optional[datetime]:
        """获取主题的最后活跃时间"""
        sql = "SELECT last_activity_at FROM topics WHERE id = %s"
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, (topic_id,))
            result = cursor.fetchone()
            return result['last_activity_at'] if result else None
    
    def get_topics_last_activity_batch(self, topic_ids: List[int]) -> Dict[int, datetime]:
        """批量获取多个主题的最后活跃时间"""
        if not topic_ids:
            return {}
        
        # 构建IN查询
        placeholders = ','.join(['%s'] * len(topic_ids))
        sql = f"SELECT id, last_activity_at FROM topics WHERE id IN ({placeholders})"
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, topic_ids)
            results = cursor.fetchall()
            
            # 转换为字典格式
            return {row['id']: row['last_activity_at'] for row in results}
    
    def batch_insert_or_update_topics(self, topics_data: List[Dict[str, Any]]):
        """批量插入或更新主题信息"""
        if not topics_data:
            return
        
        # 清理和验证所有主题数据
        beijing_time = self.get_beijing_time()
        sanitized_topics = []
        
        for topic_data in topics_data:
            sanitized = self._sanitize_topic_data(topic_data)
            if 'crawled_at' not in sanitized:
                sanitized['crawled_at'] = beijing_time
            sanitized_topics.append(sanitized)
        
        sql = """
        INSERT INTO topics (
            id, title, url, category, author_id, 
            reply_count, view_count, last_activity_at, created_at, tags, crawled_at
        ) VALUES (
            %(id)s, %(title)s, %(url)s, %(category)s, %(author_id)s,
            %(reply_count)s, %(view_count)s, %(last_activity_at)s, %(created_at)s, %(tags)s, %(crawled_at)s
        ) ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            url = VALUES(url),
            category = VALUES(category),
            author_id = VALUES(author_id),
            reply_count = VALUES(reply_count),
            view_count = VALUES(view_count),
            last_activity_at = VALUES(last_activity_at),
            tags = VALUES(tags),
            crawled_at = VALUES(crawled_at)
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.executemany(sql, sanitized_topics)
            connection.commit()
            self.logger.info(f"批量插入/更新 {len(sanitized_topics)} 个主题")
    
    def clean_old_data(self, retention_days: int) -> int:
        """清理过期数据"""
        sql = """
        DELETE FROM topics 
        WHERE last_activity_at < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL %s DAY)
        """
        
        with self.get_cursor() as (cursor, connection):
            cursor.execute(sql, (retention_days,))
            deleted_count = cursor.rowcount
            connection.commit()
            self.logger.info(f"清理了 {deleted_count} 个过期主题及其相关数据")
            return deleted_count


# 全局数据库管理实例
db_manager = DatabaseManager()
