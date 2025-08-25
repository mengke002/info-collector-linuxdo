"""
热度分析模块
负责计算主题热度分数和总点赞数
"""
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from .database import db_manager


class HotnessAnalyzer:
    """热度分析器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = db_manager
        
        # 热度计算权重配置
        self.view_weight = 1.0      # 浏览数权重
        self.reply_weight = 5.0     # 回复数权重  
        self.like_weight = 3.0      # 点赞数权重
        self.time_decay_hours = 168  # 时间衰减周期：7天
        self.max_hotness_score = 999999.0  # 热度分数最大值限制
    
    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)
    
    def update_total_likes(self, topic_ids: List[int] = None) -> int:
        """
        更新主题的总点赞数
        
        Args:
            topic_ids: 指定要更新的主题ID列表，None表示更新所有主题
            
        Returns:
            更新的主题数量
        """
        try:
            updated_count = self.db.update_total_likes(topic_ids)
            self.logger.info(f"成功更新 {updated_count} 个主题的总点赞数")
            return updated_count
        except Exception as e:
            self.logger.error(f"更新总点赞数失败: {e}")
            raise
    
    def update_hotness_scores(self, topic_ids: List[int] = None,
                            view_weight: Optional[float] = None,
                            reply_weight: Optional[float] = None,
                            like_weight: Optional[float] = None,
                            time_decay_hours: Optional[int] = None) -> int:
        """
        更新主题的热度分数
        
        Args:
            topic_ids: 指定要更新的主题ID列表，None表示更新所有主题
            view_weight: 浏览数权重，None使用默认值
            reply_weight: 回复数权重，None使用默认值
            like_weight: 点赞数权重，None使用默认值
            time_decay_hours: 时间衰减周期（小时），None使用默认值
            
        Returns:
            更新的主题数量
        """
        try:
            # 使用提供的权重或默认权重
            vw = view_weight if view_weight is not None else self.view_weight
            rw = reply_weight if reply_weight is not None else self.reply_weight
            lw = like_weight if like_weight is not None else self.like_weight
            tdh = time_decay_hours if time_decay_hours is not None else self.time_decay_hours
            
            updated_count = self.db.update_hotness_scores(
                topic_ids=topic_ids,
                view_weight=vw,
                reply_weight=rw,
                like_weight=lw,
                time_decay_hours=tdh,
                max_score=self.max_hotness_score  # 传递最大分数限制
            )
            
            self.logger.info(f"成功更新 {updated_count} 个主题的热度分数（最大值限制: {self.max_hotness_score}）")
            return updated_count
        except Exception as e:
            self.logger.error(f"更新热度分数失败: {e}")
            raise
    
    def analyze_recent_topics(self, hours_back: int = 24) -> Dict[str, Any]:
        """
        分析最近活跃的主题
        
        Args:
            hours_back: 回溯的小时数
            
        Returns:
            分析结果字典
        """
        try:
            self.logger.info(f"开始分析最近 {hours_back} 小时的活跃主题")
            
            # 获取最近活跃的主题
            recent_topics = self.db.get_recent_active_topics(hours_back)
            
            if not recent_topics:
                self.logger.warning(f"未找到最近 {hours_back} 小时的活跃主题")
                return {
                    'success': True,
                    'analyzed_topics': 0,
                    'updated_likes': 0,
                    'updated_scores': 0
                }
            
            # 提取主题ID
            topic_ids = [topic['id'] for topic in recent_topics]
            
            # 更新总点赞数
            updated_likes = self.update_total_likes(topic_ids)
            
            # 更新热度分数
            updated_scores = self.update_hotness_scores(topic_ids)
            
            result = {
                'success': True,
                'analyzed_topics': len(recent_topics),
                'updated_likes': updated_likes,
                'updated_scores': updated_scores,
                'analysis_time': self.get_beijing_time()
            }
            
            self.logger.info(f"分析完成：{len(recent_topics)} 个主题，更新点赞数 {updated_likes}，更新热度分数 {updated_scores}")
            return result
            
        except Exception as e:
            self.logger.error(f"分析最近主题失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'analyzed_topics': 0,
                'updated_likes': 0,
                'updated_scores': 0
            }
    
    def analyze_all_topics(self) -> Dict[str, Any]:
        """
        分析所有主题的热度
        
        Returns:
            分析结果字典
        """
        try:
            self.logger.info("开始分析所有主题的热度")
            
            # 更新所有主题的总点赞数
            updated_likes = self.update_total_likes()
            
            # 更新所有主题的热度分数
            updated_scores = self.update_hotness_scores()
            
            result = {
                'success': True,
                'updated_likes': updated_likes,
                'updated_scores': updated_scores,
                'analysis_time': self.get_beijing_time()
            }
            
            self.logger.info(f"全量分析完成：更新点赞数 {updated_likes}，更新热度分数 {updated_scores}")
            return result
            
        except Exception as e:
            self.logger.error(f"分析所有主题失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'updated_likes': 0,
                'updated_scores': 0
            }
    
    def get_hotness_stats(self) -> Dict[str, Any]:
        """
        获取热度统计信息
        
        Returns:
            统计信息字典
        """
        try:
            with self.db.get_cursor() as (cursor, connection):
                # 获取基本统计
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_topics,
                        AVG(hotness_score) as avg_hotness,
                        MAX(hotness_score) as max_hotness,
                        MIN(hotness_score) as min_hotness,
                        AVG(total_like_count) as avg_likes,
                        MAX(total_like_count) as max_likes
                    FROM topics 
                    WHERE hotness_score > 0
                """)
                
                stats = cursor.fetchone()
                
                # 获取热度分布
                cursor.execute("""
                    SELECT 
                        CASE 
                            WHEN hotness_score >= 1000 THEN 'very_hot'
                            WHEN hotness_score >= 100 THEN 'hot' 
                            WHEN hotness_score >= 10 THEN 'warm'
                            ELSE 'cool'
                        END as heat_level,
                        COUNT(*) as count
                    FROM topics 
                    WHERE hotness_score > 0
                    GROUP BY heat_level
                """)
                
                heat_distribution = {}
                for row in cursor.fetchall():
                    heat_distribution[row['heat_level']] = row['count']
                
                # 获取分类热度排行
                cursor.execute("""
                    SELECT 
                        category,
                        COUNT(*) as topic_count,
                        AVG(hotness_score) as avg_hotness,
                        MAX(hotness_score) as max_hotness
                    FROM topics 
                    WHERE category IS NOT NULL AND hotness_score > 0
                    GROUP BY category
                    ORDER BY avg_hotness DESC
                    LIMIT 10
                """)
                
                category_stats = cursor.fetchall()
                
                return {
                    'success': True,
                    'total_topics': stats.get('total_topics', 0),
                    'avg_hotness': round(float(stats.get('avg_hotness', 0)), 2),
                    'max_hotness': float(stats.get('max_hotness', 0)),
                    'min_hotness': float(stats.get('min_hotness', 0)),
                    'avg_likes': round(float(stats.get('avg_likes', 0)), 2),
                    'max_likes': stats.get('max_likes', 0),
                    'heat_distribution': heat_distribution,
                    'category_stats': category_stats,
                    'stats_time': self.get_beijing_time()
                }
                
        except Exception as e:
            self.logger.error(f"获取热度统计失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }


# 全局分析器实例
hotness_analyzer = HotnessAnalyzer()