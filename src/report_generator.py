"""
智能分析报告生成器
基于板块的热点内容分析和Markdown报告生成
"""
import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta

from .database import db_manager
from .llm_client import llm_client
from .config import config


class ReportGenerator:
    """智能分析报告生成器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db = db_manager
        self.llm = llm_client
        
        # 报告配置
        self.top_topics_per_category = 30
        self.top_replies_per_topic = 10
        # 增加内容长度限制以容纳更多主题
        self.max_content_length = config.get_llm_config().get('max_content_length', 50000)
    
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
    
    def _generate_unified_report_markdown(self, category: str, unified_analysis: Dict[str, Any],
                                         hot_topics_data: List[Dict[str, Any]],
                                         period_start: datetime, period_end: datetime) -> str:
        """生成统一分析报告的Markdown格式 (V2.1)"""

        # 报告标题
        title = f"📈 [{category}] 社区情报洞察报告"

        # 时间信息
        start_str = period_start.strftime('%Y-%m-%d %H:%M:%S')
        end_str = period_end.strftime('%Y-%m-%d %H:%M:%S')
        generate_time = self.get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')

        # 获取AI分析内容
        analysis_content = unified_analysis.get('analysis', '分析内容生成失败。')

        # 构建报告内容
        report_lines = [
            f"# {title}",
            "",
            f"*报告生成时间: {generate_time}*",
            f"*数据范围: {start_str} - {end_str}*",
            "",
            "---",
            "",
            analysis_content,  # 插入LLM生成的完整报告
            "",
            "---",
            "",
            "## 📚 来源清单 (Source List)",
            ""
        ]

        # 生成来源清单
        for i, topic_data in enumerate(hot_topics_data, 1):
            topic_info = topic_data['topic']
            report_lines.append(
                f"- **[T{i}]**: [{topic_info['title']}]({topic_info['url']})"
            )

        report_lines.extend(["", "---", ""])

        # 技术信息
        if unified_analysis.get('provider'):
            report_lines.append(
                f"*分析引擎: {unified_analysis['provider']} ({unified_analysis.get('model', 'unknown')})*"
            )

        report_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(hot_topics_data)} 个热门主题",
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
            
            self.logger.info(f"找到 {len(hot_topics)} 个热门主题，开始获取详细数据")
            
            # 获取所有主题的详细数据
            hot_topics_data = []
            for i, topic in enumerate(hot_topics, 1):
                self.logger.info(f"获取第 {i}/{len(hot_topics)} 个主题详细数据: {topic.get('title', '未知标题')[:50]}...")
                
                # 获取主题的详细内容
                topic_data = self.db.get_topic_posts_for_analysis(
                    topic['id'], 
                    limit=self.top_replies_per_topic
                )
                
                if topic_data:
                    hot_topics_data.append(topic_data)
                    self.logger.info(f"主题 {i}/{len(hot_topics)} 数据获取成功")
                else:
                    self.logger.warning(f"主题 {i}/{len(hot_topics)} 无法获取详细数据")
            
            if not hot_topics_data:
                self.logger.warning(f"所有主题都无法获取详细数据")
                return {
                    'success': False,
                    'error': '无法获取主题详细数据',
                    'category': category,
                    'topics_analyzed': 0
                }
            
            self.logger.info(f"共获取到 {len(hot_topics_data)} 个主题的详细数据，开始统一LLM分析")
            
            # 使用统一LLM分析
            unified_result = self._analyze_all_topics_with_llm(hot_topics_data)
            
            if not unified_result or not unified_result.get('success'):
                self.logger.warning(f"{category or '全站'} 板块的统一主题分析失败")
                return {
                    'success': False,
                    'error': f'{category or "全站"} 板块主题分析失败',
                    'category': category,
                    'topics_analyzed': 0
                }
            
            self.logger.info(f"统一分析完成，分析了 {len(hot_topics_data)} 个主题")
            
            # 生成Markdown报告
            report_content = self._generate_unified_report_markdown(
                category=category or '全站',
                unified_analysis=unified_result,
                hot_topics_data=hot_topics_data,
                period_start=start_time,
                period_end=end_time
            )
            
            # 保存报告到数据库
            report_data = {
                'category': category or '全站',
                'report_type': 'hotspot',
                'analysis_period_start': start_time,
                'analysis_period_end': end_time,
                'topics_analyzed': len(hot_topics_data),
                'report_title': f'[{category or "全站"}] 社区情报洞察报告',
                'report_content': report_content
            }
            
            report_id = self.db.save_report(report_data)
            
            result = {
                'success': True,
                'category': category or '全站',
                'report_id': report_id,
                'topics_analyzed': len(hot_topics_data),
                'total_topics_found': len(hot_topics),
                'analysis_period': {
                    'start': start_time,
                    'end': end_time,
                    'hours_back': hours_back
                },
                'report_preview': report_content[:500] + "..." if len(report_content) > 500 else report_content
            }
            
            self.logger.info(f"{category or '全站'} 分析完成: 分析了 {len(hot_topics_data)}/{len(hot_topics)} 个主题，报告ID: {report_id}")
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
    
    def _format_all_topics_for_analysis(self, hot_topics_data: List[Dict[str, Any]]) -> str:
        """将所有热门主题合并为一个文档用于LLM统一分析 (V2.1)"""
        content_parts = []
        
        # 添加文档头部
        content_parts.extend([
            "=== 热门主题综合分析文档 ===",
            f"总计 {len(hot_topics_data)} 个热门主题",
            "",
        ])
        
        # 按热度排序处理每个主题
        for i, topic_data in enumerate(hot_topics_data, 1):
            topic_info = topic_data['topic']
            main_post = topic_data.get('main_post')
            replies = topic_data.get('replies', [])
            
            content_parts.extend([
                f"\n### [Source: T{i}] {topic_info['title']}",
                f"热度分数: {topic_info.get('hotness_score', 0):.2f}",
                f"分类: {topic_info.get('category', '未知')}",
                f"回复数: {topic_info.get('reply_count', 0)}",
                f"浏览数: {topic_info.get('view_count', 0)}",
                f"总点赞数: {topic_info.get('total_like_count', 0)}",
                f"URL: {topic_info.get('url', '')}",
                ""
            ])
            
            # 主贴内容（精简版）
            if main_post and main_post.get('content_raw'):
                main_content = main_post['content_raw'].strip()
                if main_content:
                    # 限制主贴内容长度，避免过长
                    if len(main_content) > 800:
                        main_content = main_content[:800] + "..."
                    content_parts.extend([
                        "**主贴内容:**",
                        main_content,
                        ""
                    ])
            
            # 热门回复（精简版）
            if replies:
                content_parts.append("**热门回复:**")
                # 限制回复数量和长度
                for j, reply in enumerate(replies[:min(3, self.top_replies_per_topic)], 1):
                    if reply.get('content_raw'):
                        reply_content = reply['content_raw'].strip()
                        if reply_content:
                            # 限制回复内容长度
                            if len(reply_content) > 200:
                                reply_content = reply_content[:200] + "..."
                            content_parts.extend([
                                f"{j}. (点赞: {reply.get('like_count', 0)}): {reply_content}",
                                ""
                            ])
            
            content_parts.append("---\n")  # 主题分割线
        
        # 合并所有内容
        full_content = "\n".join(content_parts)
        
        # 如果内容过长，这里不再截断，让LLM看到所有主题
        self.logger.info(f"格式化后的主题内容总长度: {len(full_content)} 字符")
        return full_content
    
    def _get_unified_analysis_prompt_template(self) -> str:
        """获取统一分析的提示词模板（V2.2 - 混合模式）"""
        return """你是一位为顶级技术公司服务的资深行业分析师。你的任务是分析以下来自一线开发者社区的、已编号的原始讨论材料，并为技术决策者撰写一份循序渐进、可追溯来源的情报简报。

**原始讨论材料:**
{content}

---

**你的分析任务:**
请严格按照以下两个阶段进行分析和内容生成。**至关重要的一点是：你的每一条分析、洞察和建议都必须在结尾处使用 `[Source: T_n]` 或 `[Sources: T_n, T_m]` 的格式明确标注其信息来源。**

**第一阶段：热门主题速览 (Top Topics Summary)**
首先，请通读所有材料，对每个热门主题进行简明扼要的总结。

**第二阶段：深度情报洞察 (In-depth Intelligence Report)**
在完成速览后，请转换视角，基于第一阶段你总结的所有信息，进行更高层级的趋势分析和洞察提炼。

---

**请严格遵照以下Markdown格式输出完整报告:**

# 📈 社区热点与情报洞察报告

## 🔥 本时段热门主题速览

[在此处罗列最重要的5-10个热门主题的速览]

### **1. [主题A的标题]**
*   **核心内容**: [对该主题的核心内容、讨论焦点和主要结论进行3-5句话的摘要。] [Source: T_n]

### **2. [主题B的标题]**
*   **核心内容**: [对该主题的核心内容、讨论焦点和主要结论进行3-5句话的摘要。] [Source: T_m]

...(以此类推)

---

## 💡 核心洞察 (Executive Summary)

*   **[洞察一]**: [用一句话高度概括你发现的最重要的趋势或洞察。例如：对低代码/无代码平台的讨论激增，反映出开发效率已成为社区核心关切点。] [Sources: T1, T5, T8]
*   **[洞察二]**: [第二个重要洞察。例如：AI Agent的实现和应用成为新的技术焦点，多个热门项目围绕此展开。] [Sources: T2, T9]
*   **[洞察三]**: [第三个重要洞察。] [Source: T4]

## 🔍 趋势与信号分析 (Trends & Signals Analysis)

### 🚀 新兴技术与工具风向
*   **[技术/工具A]**: [描述它是什么，为什么它现在很热门，以及在讨论中是如何体现的。] [Source: T3]
*   **[技术/工具B]**: [同上。] [Source: T7]

### 🔗 讨论热点的内在关联
*   **[关联性分析]**: [详细阐述你发现的不同热点之间的联系。例如：对“XX框架性能瓶颈”的抱怨（主题A）与“YY轻量级替代方案”的出现（主题B）形成了呼应，共同指向了前端开发的轻量化趋势。] [Sources: T1, T6]

### ⚠️ 普遍痛点与潜在需求
*   **[痛点一]**: [描述社区开发者普遍遇到的一个问题或挑战。] [Source: T5]
*   **[痛点二]**: [同上。] [Source: T10]

##  actionable 建议 (Actionable Recommendations)

*   **对于开发者**: [基于以上分析，给个人开发者提出1-2条具体建议。例如：建议关注XX技术，尝试将YY工具集成到当前工作流中以提高效率。] [Sources: T3, T7]
*   **对于技术团队**: [给技术团队或决策者提出1-2条建议。例如：建议评估引入XX解决方案的可行性，以解决团队在YY方面遇到的普遍问题。] [Source: T5]

---

## 📚 来源清单 (Source List)
[这里由程序自动生成，你不需要填写这部分。]
"""
    
    def _analyze_all_topics_with_llm(self, hot_topics_data: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """使用LLM对所有主题进行统一分析"""
        try:
            # 检查LLM客户端是否可用
            if not self.llm:
                self.logger.warning("LLM客户端未初始化")
                return None
            
            # 合并所有主题内容
            content = self._format_all_topics_for_analysis(hot_topics_data)
            
            # 获取统一分析提示词模板
            prompt_template = self._get_unified_analysis_prompt_template()
            
            self.logger.info(f"开始对 {len(hot_topics_data)} 个主题进行统一LLM分析，内容总长度: {len(content)} 字符")
            
            # 调用LLM分析
            result = self.llm.analyze_content(content, prompt_template)
            
            if result.get('success'):
                return {
                    'success': True,
                    'topics_count': len(hot_topics_data),
                    'analysis': result['content'],
                    'provider': result.get('provider'),
                    'model': result.get('model')
                }
            else:
                self.logger.warning(f"LLM统一分析失败: {result.get('error')}")
                return None
                
        except Exception as e:
            self.logger.error(f"统一分析主题时出错: {e}")
            return None


# 全局报告生成器实例
report_generator = ReportGenerator()