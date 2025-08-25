"""
智能分析报告生成器
基于板块的热点内容分析和Markdown报告生成
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta

from .database import db_manager
from .llm_client import llm_manager
from .config import config


class ReportGenerator:
    """智能分析报告生成器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = db_manager
        self.llm = llm_manager
        
        # 报告配置
        self.top_topics_per_category = 30
        self.top_replies_per_topic = 10
        self.max_content_length = config.get_llm_config().get('max_content_length', 8000)
    
    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)
    
    def _truncate_content(self, content: str, max_length: int = None) -> str:
        """截断内容到指定长度"""
        if max_length is None:
            max_length = self.max_content_length
        
        if len(content) <= max_length:
            return content
        
        # 在合适的位置截断，避免截断到句子中间
        truncated = content[:max_length]
        
        # 尝试在最后一个句号、问号或感叹号处截断
        for delimiter in ['。', '！', '？', '.', '!', '?']:
            last_delimiter = truncated.rfind(delimiter)
            if last_delimiter > max_length * 0.8:  # 确保不会截掉太多内容
                return truncated[:last_delimiter + 1]
        
        # 如果找不到合适的分割点，就在最后一个空格处截断
        last_space = truncated.rfind(' ')
        if last_space > max_length * 0.8:
            return truncated[:last_space] + "..."
        
        return truncated + "..."
    
    def _format_content_for_analysis(self, topic_data: Dict[str, Any]) -> str:
        """格式化主题内容用于LLM分析"""
        topic_info = topic_data['topic']
        main_post = topic_data.get('main_post')
        replies = topic_data.get('replies', [])
        
        # 构建分析内容
        content_parts = []
        
        # 主题标题和基本信息
        content_parts.append(f"标题: {topic_info['title']}")
        content_parts.append(f"分类: {topic_info.get('category', '未知')}")
        content_parts.append(f"回复数: {topic_info.get('reply_count', 0)}")
        content_parts.append(f"浏览数: {topic_info.get('view_count', 0)}")
        content_parts.append(f"总点赞数: {topic_info.get('total_like_count', 0)}")
        content_parts.append(f"热度分数: {topic_info.get('hotness_score', 0)}")
        content_parts.append("")
        
        # 主贴内容
        if main_post and main_post.get('content_raw'):
            content_parts.append("主要内容:")
            main_content = main_post['content_raw'].strip()
            if main_content:
                content_parts.append(main_content)
                content_parts.append("")
        
        # 精选回复
        if replies:
            content_parts.append("热门回复:")
            for i, reply in enumerate(replies[:self.top_replies_per_topic], 1):
                if reply.get('content_raw'):
                    reply_content = reply['content_raw'].strip()
                    if reply_content:
                        content_parts.append(f"{i}. (点赞数: {reply.get('like_count', 0)})")
                        content_parts.append(reply_content)
                        content_parts.append("")
        
        # 合并所有内容并截断
        full_content = "\n".join(content_parts)
        return self._truncate_content(full_content)
    
    def _get_analysis_prompt_template(self) -> str:
        """获取分析提示词模板"""
        return """请分析以下论坛主题的内容，并按照指定格式提供分析结果：

{content}

请按以下格式返回分析结果：

## 核心摘要
[生成一段不超过150字的摘要，精准概括核心内容、主要讨论的观点和最终结论]

## 关键信息点
- [信息点1：最有价值的信息、技巧或观点]
- [信息点2：重要的讨论内容或技术要点]
- [信息点3：值得关注的结论或建议]

注意：
1. 摘要要简洁明了，突出重点
2. 关键信息点要具体实用，避免空泛描述
3. 如果内容涉及技术、工具或方法，请重点提炼
4. 保持客观中性的表述
"""
    
    async def _analyze_topic_with_llm(self, topic_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """使用LLM分析单个主题"""
        try:
            # 格式化内容
            content = self._format_content_for_analysis(topic_data)
            
            # 获取提示词模板
            prompt_template = self._get_analysis_prompt_template()
            
            # 调用LLM分析
            result = await self.llm.analyze_content(content, prompt_template)
            
            if result.get('success'):
                return {
                    'topic_id': topic_data['topic']['id'],
                    'title': topic_data['topic']['title'],
                    'url': topic_data['topic']['url'],
                    'hotness_score': topic_data['topic'].get('hotness_score', 0),
                    'analysis': result['content'],
                    'provider': result.get('provider'),
                    'model': result.get('model')
                }
            else:
                self.logger.warning(f"LLM分析失败: {result.get('error')}")
                return None
                
        except Exception as e:
            self.logger.error(f"分析主题时出错: {e}")
            return None
    
    def _generate_report_markdown(self, category: str, analysis_results: List[Dict[str, Any]], 
                                 period_start: datetime, period_end: datetime) -> str:
        """生成Markdown格式的分析报告"""
        
        # 报告标题
        title = f"📈 [{category}] 板块24小时热点报告"
        
        # 时间信息
        start_str = period_start.strftime('%Y-%m-%d %H:%M:%S')
        end_str = period_end.strftime('%Y-%m-%d %H:%M:%S')
        generate_time = self.get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')
        
        # 构建报告内容
        report_lines = [
            f"# {title}",
            "",
            f"*报告生成时间: {generate_time}*  ",
            f"*数据范围: {start_str} - {end_str}*",
            "",
            "---",
            "",
            f"## 🔥 本时段热门主题 Top {len(analysis_results)}",
            ""
        ]
        
        # 添加每个主题的分析
        for i, result in enumerate(analysis_results, 1):
            report_lines.extend([
                f"### {i}. {result['title']}",
                f"- **原始链接**: [{result['url']}]({result['url']})",
                f"- **热度分数**: {result['hotness_score']:.2f}",
                ""
            ])
            
            # 解析LLM分析结果
            analysis_content = result.get('analysis', '')
            if analysis_content:
                # 简单解析分析结果，提取摘要和关键点
                lines = analysis_content.split('\n')
                current_section = None
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    if line.startswith('## 核心摘要') or line.startswith('## 摘要'):
                        current_section = 'summary'
                        continue
                    elif line.startswith('## 关键信息点') or line.startswith('## 关键点'):
                        current_section = 'points'
                        report_lines.append("- **关键讨论点**:")
                        continue
                    elif line.startswith('##'):
                        current_section = None
                        continue
                    
                    if current_section == 'summary' and not line.startswith('-'):
                        # 摘要内容
                        if line:
                            report_lines.append(f"  > {line}")
                    elif current_section == 'points' and line.startswith('-'):
                        # 关键信息点
                        point = line[1:].strip()
                        if point:
                            report_lines.append(f"  {line}")
                
                # 如果没有成功解析，直接显示原始分析结果
                if current_section is None and analysis_content:
                    report_lines.extend([
                        "- **分析结果**:",
                        f"  > {analysis_content.replace(chr(10), chr(10) + '  > ')}"
                    ])
            
            # 技术信息（可选显示）
            if result.get('provider'):
                report_lines.append(f"- *分析引擎: {result['provider']} ({result.get('model', 'unknown')})*")
            
            report_lines.extend(["", "---", ""])
        
        # 报告尾部
        report_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(analysis_results)} 个热门主题",
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        
        return "\n".join(report_lines)
    
    async def generate_category_report(self, category: str = None, hours_back: int = 24) -> Dict[str, Any]:
        """生成热点分析报告（不再按分类筛选，从所有数据中获取热门主题）"""
        try:
            report_title = "全站热点分析报告" if category is None else f"{category} 板块热点分析报告"
            self.logger.info(f"开始生成 {report_title} (回溯 {hours_back} 小时)")
            
            # 设置分析时间范围
            end_time = self.get_beijing_time()
            start_time = end_time - timedelta(hours=hours_back)
            
            # 获取所有热门主题（不按分类筛选）
            hot_topics = self.db.get_hot_topics_all(
                limit=self.top_topics_per_category, 
                hours_back=hours_back
            )
            
            if not hot_topics:
                self.logger.warning(f"过去 {hours_back} 小时内没有热门主题")
                return {
                    'success': True,
                    'category': category or '全站',
                    'topics_analyzed': 0,
                    'message': f'过去 {hours_back} 小时内暂无热门内容'
                }
            
            self.logger.info(f"找到 {len(hot_topics)} 个热门主题，开始LLM分析")
            
            # 并发分析主题
            analysis_tasks = []
            for topic in hot_topics:
                # 获取主题的详细内容
                topic_data = self.db.get_topic_posts_for_analysis(
                    topic['id'], 
                    limit=self.top_replies_per_topic
                )
                
                if topic_data:
                    analysis_tasks.append(self._analyze_topic_with_llm(topic_data))
            
            # 等待所有分析完成
            analysis_results = await asyncio.gather(*analysis_tasks, return_exceptions=True)
            
            # 过滤掉失败的分析结果
            successful_analyses = []
            for result in analysis_results:
                if isinstance(result, dict) and result:
                    successful_analyses.append(result)
                elif isinstance(result, Exception):
                    self.logger.error(f"主题分析异常: {result}")
            
            if not successful_analyses:
                self.logger.warning(f"{category} 板块的所有主题分析都失败了")
                return {
                    'success': False,
                    'error': f'{category} 板块主题分析失败',
                    'category': category,
                    'topics_analyzed': 0
                }
            
            # 按热度分数排序
            successful_analyses.sort(key=lambda x: x.get('hotness_score', 0), reverse=True)
            
            # 生成Markdown报告
            report_content = self._generate_report_markdown(
                category=category or '全站',
                analysis_results=successful_analyses,
                period_start=start_time,
                period_end=end_time
            )
            
            # 保存报告到数据库
            report_data = {
                'category': category or '全站',
                'report_type': 'hotspot',
                'analysis_period_start': start_time,
                'analysis_period_end': end_time,
                'topics_analyzed': len(successful_analyses),
                'report_title': f'[{category or "全站"}] 热点分析报告',
                'report_content': report_content
            }
            
            report_id = self.db.save_report(report_data)
            
            result = {
                'success': True,
                'category': category or '全站',
                'report_id': report_id,
                'topics_analyzed': len(successful_analyses),
                'total_topics_found': len(hot_topics),
                'analysis_period': {
                    'start': start_time,
                    'end': end_time,
                    'hours_back': hours_back
                },
                'report_preview': report_content[:500] + "..." if len(report_content) > 500 else report_content
            }
            
            self.logger.info(f"{category or '全站'} 分析完成: 分析了 {len(successful_analyses)}/{len(hot_topics)} 个主题，报告ID: {report_id}")
            return result
            
        except Exception as e:
            self.logger.error(f"生成 {category or '全站'} 报告时出错: {e}")
            return {
                'success': False,
                'error': str(e),
                'category': category or '全站',
                'topics_analyzed': 0
            }
    
    async def generate_all_categories_report(self, hours_back: int = 24) -> Dict[str, Any]:
        """生成全站热点分析报告（不再按分类）"""
        try:
            self.logger.info(f"开始生成全站热点分析报告 (回溯 {hours_back} 小时)")
            
            # 直接生成一个全站报告
            result = await self.generate_category_report(category=None, hours_back=hours_back)
            
            if result.get('success'):
                # 包装成与原格式兼容的结构
                return {
                    'success': True,
                    'total_categories': 1,
                    'successful_reports': 1,
                    'failed_reports': 0,
                    'total_topics_analyzed': result.get('topics_analyzed', 0),
                    'reports': [result],
                    'failures': [],
                    'generation_time': self.get_beijing_time()
                }
            else:
                return {
                    'success': False,
                    'total_categories': 1,
                    'successful_reports': 0,
                    'failed_reports': 1,
                    'total_topics_analyzed': 0,
                    'reports': [],
                    'failures': [{
                        'category': '全站',
                        'error': result.get('error', '未知错误')
                    }],
                    'generation_time': self.get_beijing_time()
                }
            
        except Exception as e:
            self.logger.error(f"生成全站报告时出错: {e}")
            return {
                'success': False,
                'error': str(e),
                'reports': []
            }


# 全局报告生成器实例
report_generator = ReportGenerator()