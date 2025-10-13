"""
智能分析报告生成器
基于板块的热点内容分析和Markdown报告生成
"""
import logging
import asyncio
import concurrent.futures
from typing import Dict, Any, List, Optional, Tuple
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
        report_config = config.get_report_config()
        self.top_topics_per_category = report_config.get('top_topics_per_category', 35)
        self.top_replies_per_topic = 10
        # 增加内容长度限制以容纳更多主题
        self.max_content_length = config.get_llm_config().get('max_content_length', 50000)
    
    def get_beijing_time(self) -> datetime:
        """获取当前北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)

    def _get_report_models(self) -> List[str]:
        """获取用于生成报告的模型列表（优先模型 + 默认模型）"""
        if not self.llm:
            return []

        models = []
        raw_models = getattr(self.llm, 'models', None) or []

        for model_name in raw_models:
            if model_name and model_name not in models:
                models.append(model_name)

        if models:
            return models

        fallback_model = getattr(self.llm, 'model', None)
        return [fallback_model] if fallback_model else []

    def _get_model_display_name(self, model_name: str) -> str:
        """根据模型名称生成用于展示的友好名称,保留版本号避免冲突"""
        if not model_name:
            return 'LLM'

        lower_name = model_name.lower()

        # GLM 系列 - 保留版本号区分
        if 'glm' in lower_name:
            if '4.6' in lower_name or '4-6' in lower_name:
                return 'GLM-4.6'
            elif '4.5' in lower_name or '4-5' in lower_name:
                return 'GLM-4.5'
            return 'GLM'

        # Qwen 系列
        if 'qwen' in lower_name:
            if 'max' in lower_name:
                return 'Qwen-Max'
            elif 'plus' in lower_name:
                return 'Qwen-Plus'
            elif 'turbo' in lower_name:
                return 'Qwen-Turbo'
            return 'Qwen'

        # Gemini 系列
        if 'gemini' in lower_name:
            if 'flash' in lower_name:
                return 'Gemini-Flash'
            elif 'pro' in lower_name:
                return 'Gemini-Pro'
            return 'Gemini'

        # DeepSeek 系列
        if 'deepseek' in lower_name:
            if 'v3' in lower_name:
                return 'DeepSeek-V3'
            elif 'v2' in lower_name:
                return 'DeepSeek-V2'
            return 'DeepSeek'

        # Kimi 系列
        if 'kimi' in lower_name or 'moonshot' in lower_name:
            return 'Kimi'

        # 未识别的模型,返回原始名称
        return model_name

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
            f"*报告生成时间: {generate_time}*",
            "",
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

    def _enhance_source_links(self, report_content: str, hot_topics_data: List[Dict[str, Any]]) -> str:
        """
        增强报告中的来源链接，将 [Source: T1, T2] 中的每个 Txx 转换为可点击的链接
        """
        import re

        # 构建来源ID到链接的映射
        source_link_map = {}
        for i, topic_data in enumerate(hot_topics_data, 1):
            topic_info = topic_data['topic']
            source_link_map[f'T{i}'] = topic_info.get('url', '')

        def replace_source_refs(match):
            # 提取完整的 Source 引用内容
            full_source_text = match.group(0)  # 如 "[Source: T2, T9, T18]"
            source_content = match.group(1)    # 如 "T2, T9, T18"

            # 分割并处理每个来源ID
            source_ids = [sid.strip() for sid in source_content.split(',')]
            linked_sources = []

            for sid in source_ids:
                if sid in source_link_map:
                    # 将 Txx 转换为链接
                    linked_sources.append(f"[{sid}]({source_link_map[sid]})")
                else:
                    # 如果找不到对应链接，保持原样
                    linked_sources.append(sid)

            # 重新组合
            return f"📎 [Source: {', '.join(linked_sources)}]"

        # 查找所有 [Source: ...] 或 [Sources: ...] 模式并替换
        pattern = r'\[Sources?:\s*([T\d\s,]+)\]'
        enhanced_content = re.sub(pattern, replace_source_refs, report_content)

        return enhanced_content

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
            "",
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
            # 将标题中的方括号替换为中文方括号，避免干扰Markdown链接解析
            clean_title = topic_info['title'].replace('[', '【').replace(']', '】')
            # 添加锚点标识，方便内部引用
            report_lines.append(
                f"- **[T{i}]** 📌: [{clean_title}]({topic_info['url']})"
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

        # 生成原始报告内容
        raw_report = "\n".join(report_lines)

        # 增强源链接，将 [Source: T1, T2] 转换为可点击链接
        enhanced_report = self._enhance_source_links(raw_report, hot_topics_data)

        return enhanced_report

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
            
            # 并发获取所有主题的详细数据
            total_topics = len(hot_topics)
            max_workers = min(8, total_topics) or 1
            hot_topics_data: List[Optional[Dict[str, Any]]] = [None] * total_topics

            loop = asyncio.get_running_loop()
            self.logger.info(
                f"开始并发获取 {total_topics} 个主题详细数据 (max_workers={max_workers})"
            )

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                fetch_tasks = [
                    loop.run_in_executor(
                        executor,
                        self._fetch_topic_detail_sync,
                        index,
                        total_topics,
                        topic
                    )
                    for index, topic in enumerate(hot_topics, 1)
                ]

                for future in asyncio.as_completed(fetch_tasks):
                    try:
                        index, topic_data = await future
                    except Exception as exc:
                        self.logger.warning(f"并发获取主题详情时发生异常: {exc}")
                        continue

                    if topic_data:
                        hot_topics_data[index - 1] = topic_data

            hot_topics_data = [data for data in hot_topics_data if data]

            if not hot_topics_data:
                self.logger.warning(f"所有主题都无法获取详细数据")
                return {
                    'success': False,
                    'error': '无法获取主题详细数据',
                    'category': category,
                    'topics_analyzed': 0
                }
            
            self.logger.info(
                f"共获取到 {len(hot_topics_data)} 个主题的详细数据，准备生成统一分析上下文"
            )

            formatted_content = self._format_all_topics_for_analysis(hot_topics_data)
            self.logger.info(
                f"统一分析上下文构建完成，长度: {len(formatted_content)} 字符"
            )

            models_to_generate = self._get_report_models()
            if not models_to_generate:
                self.logger.warning("未配置任何可用于生成报告的模型")
                return {
                    'success': False,
                    'error': '未配置可用的LLM模型',
                    'category': category or '全站',
                    'topics_analyzed': 0
                }

            model_reports: List[Dict[str, Any]] = []
            failures: List[Dict[str, Any]] = []
            tasks = []
            task_meta: List[Dict[str, str]] = []

            category_label = category or '全站'

            for model_name in models_to_generate:
                display_name = self._get_model_display_name(model_name)
                task_meta.append({'model': model_name, 'display': display_name})
                tasks.append(
                    self._generate_report_for_model(
                        model_name=model_name,
                        display_name=display_name,
                        category_label=category_label,
                        hot_topics_data=hot_topics_data,
                        formatted_content=formatted_content,
                        start_time=start_time,
                        end_time=end_time
                    )
                )

            self.logger.info(
                f"开始并行生成 {len(tasks)} 份模型报告: {[meta['display'] for meta in task_meta]}"
            )

            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            for meta, task_result in zip(task_meta, task_results):
                model_name = meta['model']
                display_name = meta['display']

                if isinstance(task_result, Exception):
                    error_msg = str(task_result)
                    self.logger.warning(
                        f"模型 {model_name} ({display_name}) 报告生成过程中出现未处理异常: {error_msg}"
                    )
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': error_msg
                    })
                    continue

                if task_result.get('success'):
                    model_reports.append(task_result)
                else:
                    failure_entry = {
                        'model': model_name,
                        'model_display': display_name,
                        'error': task_result.get('error', '报告生成失败')
                    }
                    failures.append(failure_entry)

            overall_success = len(model_reports) > 0

            result = {
                'success': overall_success,
                'category': category_label,
                'topics_analyzed': len(hot_topics_data) if overall_success else 0,
                'total_topics_found': len(hot_topics),
                'analysis_period': {
                    'start': start_time,
                    'end': end_time,
                    'hours_back': hours_back
                },
                'model_reports': model_reports,
                'failures': failures
            }

            if overall_success:
                primary_report = model_reports[0]
                result['report_id'] = primary_report['report_id']
                result['report_title'] = primary_report['report_title']
                result['report_preview'] = primary_report['report_preview']
                result['notion_push'] = primary_report.get('notion_push')
                result['report_ids'] = [mr['report_id'] for mr in model_reports]
            else:
                if failures:
                    result['error'] = failures[0]['error']
                else:
                    result['error'] = '报告生成失败'

            self.logger.info(
                f"{category_label} 分析完成: 成功生成 {len(model_reports)} 份报告，失败 {len(failures)} 份"
            )

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

    async def generate_light_report(self, hours_back: int = 24) -> Dict[str, Any]:
        """生成日报资讯报告(智能筛选最有价值的主题)

        Args:
            hours_back: 回溯小时数,默认24小时

        Returns:
            报告生成结果
        """
        try:
            self.logger.info(f"开始生成日报资讯报告 (回溯 {hours_back} 小时)")

            # 设置分析时间范围
            end_time = self.get_beijing_time()
            start_time = end_time - timedelta(hours=hours_back)

            # 从配置中获取日报资讯的主题数量限制
            report_config = config.get_report_config()
            light_report_limit = report_config.get('light_report_topics_limit', 100)

            # 获取配置的权重参数
            hotness_weight = report_config.get('light_report_hotness_weight', 0.4)
            engagement_weight = report_config.get('light_report_engagement_weight', 0.3)
            recency_weight = report_config.get('light_report_recency_weight', 0.2)
            content_quality_weight = report_config.get('light_report_content_quality_weight', 0.1)

            self.logger.info(
                f"日报资讯配置: 主题数={light_report_limit}, "
                f"权重(热度={hotness_weight}, 互动={engagement_weight}, "
                f"时效={recency_weight}, 质量={content_quality_weight})"
            )

            # 使用智能筛选获取最有价值的主题
            hot_topics = self.db.get_valuable_topics_with_smart_filter(
                limit=light_report_limit,
                hours_back=hours_back,
                hotness_weight=hotness_weight,
                engagement_weight=engagement_weight,
                recency_weight=recency_weight,
                content_quality_weight=content_quality_weight
            )

            if not hot_topics:
                self.logger.warning(f"过去 {hours_back} 小时内没有主题")
                return {
                    'success': True,
                    'report_type': 'light',
                    'topics_analyzed': 0,
                    'message': f'过去 {hours_back} 小时内暂无内容'
                }

            self.logger.info(f"智能筛选后找到 {len(hot_topics)} 个有价值主题,开始获取详细数据")

            # 并发获取所有主题的详细数据
            total_topics = len(hot_topics)
            max_workers = min(8, total_topics) or 1
            hot_topics_data: List[Optional[Dict[str, Any]]] = [None] * total_topics

            loop = asyncio.get_running_loop()
            self.logger.info(
                f"开始并发获取 {total_topics} 个主题详细数据 (max_workers={max_workers})"
            )

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                fetch_tasks = [
                    loop.run_in_executor(
                        executor,
                        self._fetch_topic_detail_sync,
                        index,
                        total_topics,
                        topic
                    )
                    for index, topic in enumerate(hot_topics, 1)
                ]

                for future in asyncio.as_completed(fetch_tasks):
                    try:
                        index, topic_data = await future
                    except Exception as exc:
                        self.logger.warning(f"并发获取主题详情时发生异常: {exc}")
                        continue

                    if topic_data:
                        hot_topics_data[index - 1] = topic_data

            hot_topics_data = [data for data in hot_topics_data if data]

            if not hot_topics_data:
                self.logger.warning(f"所有主题都无法获取详细数据")
                return {
                    'success': False,
                    'error': '无法获取主题详细数据',
                    'report_type': 'light',
                    'topics_analyzed': 0
                }

            self.logger.info(
                f"共获取到 {len(hot_topics_data)} 个主题的详细数据,准备生成日报资讯上下文"
            )

            formatted_content = self._format_all_topics_for_analysis(hot_topics_data)
            self.logger.info(
                f"日报资讯上下文构建完成,长度: {len(formatted_content)} 字符"
            )

            models_to_generate = self._get_report_models()
            if not models_to_generate:
                self.logger.warning("未配置任何可用于生成报告的模型")
                return {
                    'success': False,
                    'error': '未配置可用的LLM模型',
                    'report_type': 'light',
                    'topics_analyzed': 0
                }

            model_reports: List[Dict[str, Any]] = []
            failures: List[Dict[str, Any]] = []
            tasks = []
            task_meta: List[Dict[str, str]] = []

            category_label = '全站'

            for model_name in models_to_generate:
                display_name = self._get_model_display_name(model_name)
                task_meta.append({'model': model_name, 'display': display_name})
                tasks.append(
                    self._generate_light_report_for_model(
                        model_name=model_name,
                        display_name=display_name,
                        category_label=category_label,
                        hot_topics_data=hot_topics_data,
                        formatted_content=formatted_content,
                        start_time=start_time,
                        end_time=end_time
                    )
                )

            self.logger.info(
                f"开始并行生成 {len(tasks)} 份日报资讯: {[meta['display'] for meta in task_meta]}"
            )

            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            for meta, task_result in zip(task_meta, task_results):
                model_name = meta['model']
                display_name = meta['display']

                if isinstance(task_result, Exception):
                    error_msg = str(task_result)
                    self.logger.warning(
                        f"模型 {model_name} ({display_name}) 日报资讯生成过程中出现未处理异常: {error_msg}"
                    )
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': error_msg
                    })
                    continue

                if task_result.get('success'):
                    model_reports.append(task_result)
                else:
                    failure_entry = {
                        'model': model_name,
                        'model_display': display_name,
                        'error': task_result.get('error', '日报资讯生成失败')
                    }
                    failures.append(failure_entry)

            overall_success = len(model_reports) > 0

            result = {
                'success': overall_success,
                'report_type': 'light',
                'topics_analyzed': len(hot_topics_data) if overall_success else 0,
                'total_topics_found': len(hot_topics),
                'analysis_period': {
                    'start': start_time,
                    'end': end_time,
                    'hours_back': hours_back
                },
                'model_reports': model_reports,
                'failures': failures
            }

            if overall_success:
                primary_report = model_reports[0]
                result['report_id'] = primary_report['report_id']
                result['report_title'] = primary_report['report_title']
                result['report_preview'] = primary_report['report_preview']
                result['notion_push'] = primary_report.get('notion_push')
                result['report_ids'] = [mr['report_id'] for mr in model_reports]
            else:
                if failures:
                    result['error'] = failures[0]['error']
                else:
                    result['error'] = '日报资讯生成失败'

            self.logger.info(
                f"日报资讯生成完成: 成功生成 {len(model_reports)} 份报告,失败 {len(failures)} 份"
            )

            return result

        except Exception as e:
            self.logger.error(f"生成日报资讯时出错: {e}")
            return {
                'success': False,
                'error': str(e),
                'report_type': 'light',
                'topics_analyzed': 0
            }

    async def generate_deep_report(self, hours_back: int = 24) -> Dict[str, Any]:
        """生成深度洞察报告(热点主题深度分析)

        这是对现有 generate_category_report 的包装,提供统一的双轨制接口

        Args:
            hours_back: 回溯小时数,默认24小时

        Returns:
            报告生成结果
        """
        self.logger.info(f"开始生成深度洞察报告 (回溯 {hours_back} 小时)")
        result = await self.generate_category_report(category=None, hours_back=hours_back)

        # 添加报告类型标识
        if result.get('success'):
            result['report_type'] = 'deep'

        return result

    async def run_dual_report_generation(self, hours_back: int = 24) -> Dict[str, Any]:
        """双轨制报告生成总调度器

        依次生成日报资讯和深度洞察报告

        Args:
            hours_back: 回溯小时数,默认24小时

        Returns:
            包含两种报告生成结果的汇总
        """
        try:
            self.logger.info(f"=" * 80)
            self.logger.info(f"开始双轨制报告生成 (回溯 {hours_back} 小时)")
            self.logger.info(f"=" * 80)

            # 生成日报资讯
            self.logger.info(">>> 第1轨: 开始生成日报资讯报告")
            light_result = await self.generate_light_report(hours_back=hours_back)

            # 生成深度洞察报告
            self.logger.info(">>> 第2轨: 开始生成深度洞察报告")
            deep_result = await self.generate_deep_report(hours_back=hours_back)

            # 汇总结果
            overall_success = light_result.get('success', False) or deep_result.get('success', False)

            result = {
                'success': overall_success,
                'generation_time': self.get_beijing_time(),
                'light_report': light_result,
                'deep_report': deep_result,
                'summary': {
                    'light_success': light_result.get('success', False),
                    'light_topics': light_result.get('topics_analyzed', 0),
                    'deep_success': deep_result.get('success', False),
                    'deep_topics': deep_result.get('topics_analyzed', 0),
                    'total_light_reports': len(light_result.get('model_reports', [])),
                    'total_deep_reports': len(deep_result.get('model_reports', []))
                }
            }

            self.logger.info(f"=" * 80)
            self.logger.info(f"双轨制报告生成完成:")
            self.logger.info(f"  - 日报资讯: {'成功' if light_result.get('success') else '失败'} "
                           f"(分析{light_result.get('topics_analyzed', 0)}个主题, "
                           f"生成{len(light_result.get('model_reports', []))}份报告)")
            self.logger.info(f"  - 深度洞察: {'成功' if deep_result.get('success') else '失败'} "
                           f"(分析{deep_result.get('topics_analyzed', 0)}个主题, "
                           f"生成{len(deep_result.get('model_reports', []))}份报告)")
            self.logger.info(f"=" * 80)

            return result

        except Exception as e:
            self.logger.error(f"双轨制报告生成时出错: {e}")
            return {
                'success': False,
                'error': str(e),
                'generation_time': self.get_beijing_time()
            }

    async def _generate_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        category_label: str,
        hot_topics_data: List[Dict[str, Any]],
        formatted_content: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """在独立线程中生成指定模型的报告"""
        return await asyncio.to_thread(
            self._generate_report_for_model_sync,
            model_name,
            display_name,
            category_label,
            hot_topics_data,
            formatted_content,
            start_time,
            end_time
        )

    def _fetch_topic_detail_sync(
        self,
        index: int,
        total: int,
        topic: Dict[str, Any]
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """在线程池中获取单个主题的详细数据，返回其索引以便还原顺序"""

        title = topic.get('title', '未知标题')
        preview = f"{title[:50]}..." if title and len(title) > 50 else title
        self.logger.info(f"获取第 {index}/{total} 个主题详细数据: {preview}")

        topic_data = None
        try:
            topic_data = self.db.get_topic_posts_for_analysis(
                topic['id'],
                limit=self.top_replies_per_topic
            )
        except Exception as exc:
            self.logger.warning(f"主题 {index}/{total} 数据获取失败: {exc}")
            raise

        if topic_data:
            self.logger.info(f"主题 {index}/{total} 数据获取成功")
        else:
            self.logger.warning(f"主题 {index}/{total} 无法获取详细数据")

        return index, topic_data

    def _generate_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        category_label: str,
        hot_topics_data: List[Dict[str, Any]],
        formatted_content: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行指定模型的LLM分析、报告生成和Notion推送"""

        self.logger.info(
            f"[{display_name}] 模型线程启动，开始统一分析"
        )

        unified_result = self._analyze_all_topics_with_llm(
            hot_topics_data,
            model_override=model_name,
            formatted_content=formatted_content
        )

        if not unified_result.get('success'):
            error_msg = unified_result.get('error', f'{model_name} 分析失败')
            self.logger.warning(
                f"{category_label} 板块使用模型 {model_name} 生成分析失败: {error_msg}"
            )
            return {
                'success': False,
                'model': model_name,
                'model_display': display_name,
                'error': error_msg
            }

        self.logger.info(
            f"{category_label} 板块模型 {model_name} 统一分析完成，开始生成报告"
        )

        report_content = self._generate_unified_report_markdown(
            category=category_label,
            unified_analysis=unified_result,
            hot_topics_data=hot_topics_data,
            period_start=start_time,
            period_end=end_time
        )

        report_title = f'[{category_label}] 社区情报洞察报告 - {display_name}'

        report_data = {
            'category': category_label,
            'report_type': 'deep_insight',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'topics_analyzed': len(hot_topics_data),
            'report_title': report_title,
            'report_content': report_content
        }

        report_id = self.db.save_report(report_data)

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_id': report_id,
            'report_title': report_title,
            'provider': unified_result.get('provider'),
            'topics_analyzed': len(hot_topics_data),
            'report_preview': report_content[:500] + "..." if len(report_content) > 500 else report_content
        }

        notion_push_info = None
        try:
            from .notion_client import notion_client

            beijing_time = self.get_beijing_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = (
                f"[{time_str}] [{display_name}] {category_label}热点洞察 "
                f"({len(hot_topics_data)}个主题)"
            )

            self.logger.info(
                f"开始推送深度洞察报告到Notion ({display_name}): {notion_title}"
            )

            notion_result = notion_client.create_report_page_in_hierarchy(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time,
                report_type='deep'
            )

            if notion_result.get('success'):
                self.logger.info(
                    f"报告成功推送到Notion ({display_name}): {notion_result.get('page_url')}"
                )
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                self.logger.warning(
                    f"推送到Notion失败 ({display_name}): {error_msg}"
                )
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            self.logger.warning(f"推送到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

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

    def _get_light_report_prompt_template(self) -> str:
        """获取日报资讯的提示词模板 (V1.0 - Linuxdo 资讯版)"""
        return '''# Role: 资深科技资讯编辑，擅长从社区讨论中快速提炼关键信息并分类呈现。

# Context:
你正在为忙碌的开发者和AI爱好者编写一份每日快讯。读者希望在最短时间内获取过去24小时内 Linuxdo 社区的重要动态。你收到的是经过筛选的、该时间段内发布的主题帖子。

# Core Principles:
1.  **全面覆盖 (Comprehensive Coverage)**: 尽可能涵盖所有有价值的信息点，不遗漏重要动态。
2.  **分类清晰 (Clear Categorization)**: 按主题分类组织信息，便于快速查找。
3.  **详略得当 (Appropriate Detail)**: 每条信息都应提供足够上下文，确保读者能理解其核心价值。避免过度压缩，但保持精炼。
4.  **可追溯性 (Content Traceability)**: 每条信息必须在句末标注来源 `[Source: T_n]`。

# Input Data Format:
你将收到一系列帖子，格式为：
`### [Source: T_id] Post Title`
`**主贴内容:**`
`Post Content...`
`**热门回复:**`
`1. (点赞: X): Reply content...`

# Your Task:
生成一份结构化的日报资讯，严格按照以下Markdown格式。请为每条资讯提供详实、清晰的描述。

## 📰 今日要闻
*本节汇总最重要的10条左右核心动态*
- **[新闻标题]**: 简要描述，说清楚新闻核心内容 (50-200字)。[Source: T_n]

---

## 🛠️ 技术分享与教程
*编程技巧、开发经验、实战教程、代码分享*
- **[技术主题]**: 详细说明分享的核心内容、技术亮点和实用价值 (50-200字)。[Source: T_n]

---

## 🤖 AI 模型与应用
*AI大模型讨论、AIGC应用、Agent实践、模型训练与微调*
- **[AI相关主题]**: 阐述讨论的核心、技术实现或应用场景 (50-200字)。[Source: T_m]

---

## 💡 灵感与思考
*产品想法、行业洞察、有趣的观点和深度思考*
- **[观点/想法]**: 清晰阐述其核心观点、论据和启发意义 (50-200字)。[Source: T_x]

---

## 📦 资源与工具分享
*开源项目、实用工具、软件推荐、福利分享*
- **[资源名称]**: 详细说明其用途、特点和推荐理由 (50-200字)。[Source: T_y]

---

## 💬 问题求助与讨论
*有代表性的技术问题、解决方案探讨、社区热议话题*
- **[问题/话题]**: 总结问题背景、社区提供的主要解决方案或讨论焦点 (50-150字)。[Source: T_z]

# Input Data:
```
{content}
```

# Important Notes:
1.  **信息要全面**：你的输入是全部帖子，请确保不要遗漏任何分类下的有价值内容。
2.  **分类准确**：将每个主题归入最合适的分类。
3.  **来源标注**：每条信息末尾必须有 `[Source: T_n]` 标注。
4.  **内容为王**: 确保每条信息的描述足够清晰、完整，能够独立成文。
5.  如果某个分类下没有内容，请在最终报告中省略该分类的标题。
'''

    def _get_unified_analysis_prompt_template(self) -> str:
        """获取统一分析的提示词模板（V2.4 - Linuxdo 深度内容版）"""
        return """你是一位熟悉开发者文化和技术生态的资深社区分析师。你的任务是分析以下来自 Linuxdo 社区的、已编号的原始讨论材料，并为广大的开发者和AI爱好者撰写一份信息密度高、内容详尽、可读性强的情报简报。

**分析原则:**
1.  **价值导向与深度优先**: 你的核心目标是挖掘出对开发者和AI爱好者有直接价值的信息。在撰写每个部分时，都应追求内容的深度和完整性，避免过于简短的概括。
2.  **忠于原文与可追溯性**: 所有分析都必须基于原文，并且每一条结论都必须在句末使用 `[Source: T_n]` 或 `[Sources: T_n, T_m]` 的格式明确标注来源。
3.  **识别帖子类型**: 在分析时，请注意识别每个主题的潜在类型，例如：技术分享、问题求助、资源发布、信息发现、观点讨论、社区活动等。这有助于你判断其核心价值。

---

**原始讨论材料 (已编号):**
{content}

---

**你的报告生成任务:**
请严格按照以下结构和要求，生成一份内容丰富详实的完整Markdown报告。

**第一部分：本时段焦点速报 (Top Topics Overview)**
*   任务：通读所有材料，为每个值得关注的热门主题撰写一份详细摘要。
*   要求：不仅要总结主题的核心内容，还要尽可能列出主要的讨论方向和关键回复的观点。篇幅无需严格限制，力求全面。

**第二部分：核心洞察与趋势 (Executive Summary & Trends)**
*   任务：基于第一部分的所有信息，从全局视角提炼出关键洞察与趋势。
*   要求：
    *   **核心洞察**: 尽可能全面地提炼你发现的重要趋势或洞察，并详细阐述，不要局限于少数几点。
    *   **技术风向与工具箱**: 详细列出并介绍被热议的新技术、新框架或工具。对于每个项目，请提供更详尽的描述，包括其用途、优点、以及社区讨论中的具体评价。
    *   **社区热议与需求点**: 详细展开社区普遍关心的话题、遇到的痛点或潜在的需求，说明其背景、当前讨论的焦点以及潜在的影响。

**第三部分：价值信息挖掘 (Valuable Information Mining)**
*   任务：深入挖掘帖子和回复中的高价值信息，并进行详细介绍。
*   要求：
    *   **高价值资源/工具**: 详细列出并介绍讨论中出现的可以直接使用的软件、库、API、开源项目或学习资料。包括资源的链接（如果原文提供）、用途和社区评价。
    *   **有趣观点/深度讨论**: 详细阐述那些引人深思、具有启发性的个人观点或高质量的讨论串。分析该观点为何重要或具有启发性，以及它引发了哪些后续讨论。

**第四部分：行动建议 (Actionable Recommendations)**
*   任务：基于以上所有分析，为社区成员提供丰富且具体的建议。
*   要求：
    *   **个人成长与技能提升**: 应该学习什么新技术？关注哪个领域的发展？对于每条建议，请阐述其背后的逻辑和预期效果。
    *   **项目实践与效率工具**: 有哪些工具可以立刻用到项目中？有哪些开源项目值得参与或借鉴？请给出具体的操作性建议。

---

**请严格遵照以下Markdown格式输出你的完整报告:**

# 📈 Linuxdo 社区情报洞察

## 一、本时段焦点速报

### **1. [主题A的标题]**
*   **详细摘要**: [详细摘要该主题的核心内容，并列出主要的讨论方向和关键回复的观点。篇幅无需严格限制，力求全面。] [Source: T_n]

### **2. [主题B的标题]**
*   **详细摘要**: [同上。] [Source: T_m]

...(尽可能多地罗列值得报告的热门主题，不少于18条，力求全面覆盖及每个主题详尽解读)

---

## 二、核心洞察与趋势

*   **核心洞察**:
    *   [详细阐述你发现的一个重要趋势或洞察。例如：AI Agent的实现和应用成为新的技术焦点，社区内涌现了多个围绕此展开的开源项目和实践讨论，具体表现在...] [Sources: T2, T9]
    *   [详细阐述第二个重要洞察。] [Sources: T3, T7]
    *   ...(尽可能多地列出洞察)

*   **技术风向与工具箱**:
    *   **[技术/工具A]**: [详细介绍它是什么，为什么它现在很热门，社区成员如何评价它，以及它解决了什么具体问题。] [Source: T3]
    *   **[技术/工具B]**: [同上。] [Source: T7]
    *   ...(尽可能多地列出技术/工具)

*   **社区热议与需求点**:
    *   **[热议话题A]**: [详细展开一个被广泛讨论的话题，例如“大模型在特定场景下的落地成本”，包括讨论的背景、各方观点、争议点以及对未来的展望。] [Source: T5]
    *   **[普遍需求B]**: [详细总结一个普遍存在的需求，例如“需要更稳定、更便宜的GPU算力资源”，并分析该需求产生的原因和社区提出的潜在解决方案。] [Source: T10]
    *   ...(尽可能多地列出话题/需求)

---

## 三、价值信息挖掘

*   **高价值资源/工具**:
    *   **[资源/工具A]**: [详细介绍该资源/工具，包括其名称、功能、优点、潜在缺点以及社区成员分享的使用技巧或经验。例如：`XX-Agent-Framework` - 一个用于快速构建AI Agent的开源框架，社区反馈其优点是上手快、文档全，但缺点是...。] [Source: T2]
    *   **[资源/工具B]**: [同上。] [Source: T8]
    *   ...(尽可能多地列出资源/工具)

*   **有趣观点/深度讨论**:
    *   **[关于“XX”的观点]**: [详细阐述一个有启发性的观点，分析其重要性，并总结因此引发的精彩后续讨论。例如：有用户认为，当前阶段的AI应用开发，工程化能力比算法创新更重要。这一观点引发了关于“算法工程师”与“AI应用工程师”职责边界的大量讨论，主流看法是...] [Source: T4]
    *   **[关于“YY”的讨论]**: [同上。] [Source: T6]
    *   ...(尽可能多地列出观点/讨论)

---

## 四、行动建议

*   **个人成长与技能提升**:
    *   [建议1：[提出具体建议]。理由与预期效果：[阐述该建议的逻辑依据，以及采纳后可能带来的好处]。例如：建议深入学习 `LangChain` 或类似框架，因为社区讨论表明这是当前构建复杂AI应用的主流方式，掌握后能显著提升项目开发能力和求职竞争力。] [Sources: T2, T9]
    *   [建议2：...]
    *   ...(给出丰富、可操作的建议)

*   **项目实践与效率工具**:
    *   [建议1：[提出具体建议]。理由与预期效果：[阐述该建议的逻辑依据，以及采纳后可能带来的好处]。例如：可以尝试将 `XX监控工具` 集成到你的项目中，因为它在社区中被证实能以极低成本实现全链路监控，帮助你快速定位性能瓶颈。] [Source: T1]
    *   [建议2：...]
    *   ...(给出丰富、可操作的建议)

---

**注意：请不要生成"来源清单"部分，这部分将由程序自动添加。**
"""
    
    def _analyze_all_topics_with_llm(
        self,
        hot_topics_data: List[Dict[str, Any]],
        model_override: Optional[str] = None,
        formatted_content: Optional[str] = None
    ) -> Dict[str, Any]:
        """使用LLM对所有主题进行统一分析"""
        try:
            # 检查LLM客户端是否可用
            if not self.llm:
                self.logger.warning("LLM客户端未初始化")
                return {
                    'success': False,
                    'error': 'LLM客户端未初始化',
                    'model': model_override
                }
            
            # 合并所有主题内容（可复用预生成上下文）
            content = formatted_content if formatted_content is not None else self._format_all_topics_for_analysis(hot_topics_data)
            
            # 获取统一分析提示词模板
            prompt_template = self._get_unified_analysis_prompt_template()
            
            available_models = getattr(self.llm, 'models', None) or []
            fallback_model = getattr(self.llm, 'model', None)
            target_model = model_override or next((m for m in available_models if m), None) or fallback_model
            self.logger.info(
                f"开始对 {len(hot_topics_data)} 个主题进行统一LLM分析，指定模型: {target_model}，内容总长度: {len(content)} 字符"
            )
            
            # 调用LLM分析
            result = self.llm.analyze_content(
                content,
                prompt_template,
                model_override=model_override
            )
            
            if result.get('success'):
                return {
                    'success': True,
                    'topics_count': len(hot_topics_data),
                    'analysis': result['content'],
                    'provider': result.get('provider'),
                    'model': result.get('model')
                }
            else:
                error_msg = result.get('error', 'LLM统一分析失败')
                self.logger.warning(f"LLM统一分析失败: {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'provider': result.get('provider'),
                    'model': result.get('model', target_model)
                }
                
        except Exception as e:
            self.logger.error(f"统一分析主题时出错: {e}")
            return {
                'success': False,
                'error': str(e),
                'model': model_override
            }

    def _generate_light_report_markdown(self, category: str, light_analysis: Dict[str, Any],
                                       hot_topics_data: List[Dict[str, Any]],
                                       period_start: datetime, period_end: datetime) -> str:
        """生成日报资讯的Markdown格式"""
        # 报告标题
        title = f"📰 [{category}] 社区日报资讯"

        # 时间信息
        start_str = period_start.strftime('%Y-%m-%d %H:%M:%S')
        end_str = period_end.strftime('%Y-%m-%d %H:%M:%S')
        generate_time = self.get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')

        # 获取AI分析内容
        analysis_content = light_analysis.get('analysis', '分析内容生成失败。')

        # 构建报告内容
        report_lines = [
            f"# {title}",
            "",
            f"*报告生成时间: {generate_time}*",
            "",
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
            clean_title = topic_info['title'].replace('[', '【').replace(']', '】')
            report_lines.append(
                f"- **[T{i}]** 📌: [{clean_title}]({topic_info['url']})"
            )

        report_lines.extend(["", "---", ""])

        # 技术信息
        if light_analysis.get('provider'):
            report_lines.append(
                f"*分析引擎: {light_analysis['provider']} ({light_analysis.get('model', 'unknown')})*"
            )

        report_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(hot_topics_data)} 个主题",
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])

        # 生成原始报告内容
        raw_report = "\n".join(report_lines)

        # 增强源链接
        enhanced_report = self._enhance_source_links(raw_report, hot_topics_data)

        return enhanced_report

    def _generate_light_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        category_label: str,
        hot_topics_data: List[Dict[str, Any]],
        formatted_content: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行指定模型的日报资讯LLM分析、报告生成和Notion推送"""

        self.logger.info(
            f"[{display_name}] 日报资讯模型线程启动，开始分析"
        )

        # 使用日报资讯的prompt进行分析
        try:
            if not self.llm:
                return {
                    'success': False,
                    'model': model_name,
                    'model_display': display_name,
                    'error': 'LLM客户端未初始化'
                }

            prompt_template = self._get_light_report_prompt_template()

            result = self.llm.analyze_content(
                formatted_content,
                prompt_template,
                model_override=model_name
            )

            if not result.get('success'):
                error_msg = result.get('error', f'{model_name} 分析失败')
                self.logger.warning(
                    f"{category_label} 日报资讯使用模型 {model_name} 生成分析失败: {error_msg}"
                )
                return {
                    'success': False,
                    'model': model_name,
                    'model_display': display_name,
                    'error': error_msg
                }

            self.logger.info(
                f"{category_label} 日报资讯模型 {model_name} 分析完成，开始生成报告"
            )

            light_analysis = {
                'success': True,
                'analysis': result['content'],
                'provider': result.get('provider'),
                'model': result.get('model')
            }

        except Exception as e:
            self.logger.error(f"日报资讯LLM分析时出错: {e}")
            return {
                'success': False,
                'model': model_name,
                'model_display': display_name,
                'error': str(e)
            }

        # 生成Markdown报告
        report_content = self._generate_light_report_markdown(
            category=category_label,
            light_analysis=light_analysis,
            hot_topics_data=hot_topics_data,
            period_start=start_time,
            period_end=end_time
        )

        report_title = f'[{category_label}] 社区日报资讯 - {display_name}'

        report_data = {
            'category': category_label,
            'report_type': 'daily_light',
            'analysis_period_start': start_time,
            'analysis_period_end': end_time,
            'topics_analyzed': len(hot_topics_data),
            'report_title': report_title,
            'report_content': report_content
        }

        report_id = self.db.save_report(report_data)

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_id': report_id,
            'report_title': report_title,
            'provider': light_analysis.get('provider'),
            'topics_analyzed': len(hot_topics_data),
            'report_preview': report_content[:500] + "..." if len(report_content) > 500 else report_content
        }

        # 推送到Notion (使用层级结构)
        notion_push_info = None
        try:
            from .notion_client import notion_client

            beijing_time = self.get_beijing_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = (
                f"[{time_str}] [{display_name}] {category_label}日报资讯 "
                f"({len(hot_topics_data)}个主题)"
            )

            self.logger.info(
                f"开始推送日报资讯到Notion ({display_name}): {notion_title}"
            )

            notion_result = notion_client.create_report_page_in_hierarchy(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time,
                report_type='light'
            )

            if notion_result.get('success'):
                self.logger.info(
                    f"日报资讯成功推送到Notion ({display_name}): {notion_result.get('page_url')}"
                )
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                self.logger.warning(
                    f"推送日报资讯到Notion失败 ({display_name}): {error_msg}"
                )
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            self.logger.warning(f"推送日报资讯到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    async def _generate_light_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        category_label: str,
        hot_topics_data: List[Dict[str, Any]],
        formatted_content: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """在独立线程中生成指定模型的日报资讯"""
        return await asyncio.to_thread(
            self._generate_light_report_for_model_sync,
            model_name,
            display_name,
            category_label,
            hot_topics_data,
            formatted_content,
            start_time,
            end_time
        )


# 全局报告生成器实例
report_generator = ReportGenerator()
