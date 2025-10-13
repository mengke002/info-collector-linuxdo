#!/usr/bin/env python3
"""
Linux.do论坛自动化数据运维系统
主执行脚本
"""
import sys
import argparse
import json
from datetime import datetime, timezone, timedelta

from src.scheduler import scheduler


def get_beijing_time():
    """获取北京时间（UTC+8）"""
    utc_time = datetime.now(timezone.utc)
    beijing_time = utc_time + timedelta(hours=8)
    return beijing_time.replace(tzinfo=None)


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Linux.do论坛自动化数据运维系统')
    parser.add_argument('--task', choices=['crawl', 'cleanup', 'stats', 'analysis', 'report', 'full'], 
                       default='crawl', help='要执行的任务类型')
    parser.add_argument('--retention-days', type=int, 
                       help='数据保留天数（仅用于cleanup任务）')
    parser.add_argument('--output', choices=['json', 'text'], default='text',
                       help='输出格式')
    parser.add_argument('--concurrent', action='store_true', default=True,
                       help='使用并发模式（默认启用）')
    parser.add_argument('--serial', action='store_true',
                       help='使用串行模式（覆盖--concurrent）')
    parser.add_argument('--hours-back', type=int, default=24,
                       help='分析任务回溯的小时数（默认24小时）')
    parser.add_argument('--analyze-all', action='store_true',
                       help='分析所有主题（仅用于analysis任务）')
    parser.add_argument('--category', type=str,
                       help='指定板块分类（仅用于report任务）')
    
    args = parser.parse_args()
    
    print(f"Linux.do论坛自动化数据运维系统")
    print(f"执行时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"执行任务: {args.task}")
    print("-" * 50)
    
    # 确定是否使用并发模式
    use_concurrent = args.concurrent and not args.serial
    
    # 执行对应任务
    if args.task == 'crawl':
        result = await scheduler.run_crawl_task(use_concurrent=use_concurrent)
    elif args.task == 'cleanup':
        result = scheduler.run_cleanup_task(args.retention_days)
    elif args.task == 'stats':
        result = scheduler.run_stats_task()
    elif args.task == 'analysis':
        result = scheduler.run_analysis_task(args.hours_back, args.analyze_all)
    elif args.task == 'report':
        result = await scheduler.run_report_task(args.category, args.hours_back)
    elif args.task == 'full':
        result = await scheduler.run_full_maintenance()
    else:
        print(f"未知任务类型: {args.task}")
        sys.exit(1)
    
    # 输出结果
    if args.output == 'json':
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print_result(result, args.task)
    
    # 根据结果设置退出码
    if result.get('success', False):
        print("\n✅ 任务执行成功")
        sys.exit(0)
    else:
        print(f"\n❌ 任务执行失败: {result.get('error', '未知错误')}")
        sys.exit(1)


def print_result(result: dict, task_type: str):
    """打印结果"""
    if not result.get('success', False):
        print(f"❌ 任务失败: {result.get('error', '未知错误')}")
        return
    
    if task_type == 'crawl':
        print(f"✅ 爬取任务完成")
        print(f"   发现主题: {result.get('topics_found', 0)} 个")
        print(f"   成功爬取: {result.get('topics_crawled', 0)} 个")
        if 'success_rate' in result:
            print(f"   成功率: {result['success_rate']}")
    
    elif task_type == 'cleanup':
        cleanup_result = result.get('cleanup_result', {})
        orphan_result = result.get('orphan_result', {})
        
        print(f"✅ 清理任务完成")
        print(f"   删除过期主题: {cleanup_result.get('deleted_topics', 0)} 个")
        print(f"   清理孤立回复: {orphan_result.get('orphaned_posts_deleted', 0)} 个")
        print(f"   修复孤立作者: {orphan_result.get('orphaned_topic_authors_fixed', 0)} + {orphan_result.get('orphaned_post_authors_fixed', 0)} 个")
        
        if 'stats_after' in result:
            stats = result['stats_after']
            print(f"   当前数据量: 用户 {stats.get('users_count', 0)}, 主题 {stats.get('topics_count', 0)}, 回复 {stats.get('posts_count', 0)}")
    
    elif task_type == 'stats':
        stats = result.get('stats', {})
        print(f"✅ 统计信息")
        print(f"   用户数量: {stats.get('users_count', 0)}")
        print(f"   主题数量: {stats.get('topics_count', 0)}")
        print(f"   回复数量: {stats.get('posts_count', 0)}")
        print(f"   今日主题: {stats.get('today_topics', 0)}")
        if stats.get('latest_activity'):
            print(f"   最新活动: {stats['latest_activity']}")
        if stats.get('oldest_activity'):
            print(f"   最旧数据: {stats['oldest_activity']}")
    
    elif task_type == 'analysis':
        print(f"✅ 热度分析完成")
        print(f"   分析主题: {result.get('analyzed_topics', result.get('updated_scores', 0))} 个")
        print(f"   更新点赞: {result.get('updated_likes', 0)} 个")
        print(f"   更新热度: {result.get('updated_scores', 0)} 个")
        
        stats_result = result.get('hotness_stats', {})
        if stats_result.get('success'):
            stats = stats_result
            print(f"   平均热度: {stats.get('avg_hotness', 0)}")
            print(f"   最高热度: {stats.get('max_hotness', 0)}")
    
    elif task_type == 'report':
        print(f"✅ 智能分析报告完成")

        # 检查是否是双轨制报告
        if 'light_report' in result and 'deep_report' in result:
            # 双轨制报告输出
            print(f"   === 双轨制报告生成结果 ===")
            summary = result.get('summary', {})

            # 日报资讯部分
            light_report = result.get('light_report', {})
            light_success = summary.get('light_success', False)
            light_topics = summary.get('light_topics', 0)
            total_light_reports = summary.get('total_light_reports', 0)

            print(f"\n   📰 日报资讯: {'✅ 成功' if light_success else '❌ 失败'}")
            print(f"      分析主题: {light_topics} 个")
            print(f"      生成报告: {total_light_reports} 份")

            # 显示每个模型的报告
            light_model_reports = light_report.get('model_reports', [])
            if light_model_reports:
                print(f"      模型报告:")
                for mr in light_model_reports:
                    display = mr.get('model_display') or mr.get('model') or 'LLM'
                    if mr.get('success'):
                        print(f"         - {display}: 报告ID {mr.get('report_id')}")
                        notion_push = mr.get('notion_push')
                        if notion_push and notion_push.get('success'):
                            print(f"           📄 Notion: 成功 - {notion_push.get('page_url')}")

            # 深度洞察部分
            deep_report = result.get('deep_report', {})
            deep_success = summary.get('deep_success', False)
            deep_topics = summary.get('deep_topics', 0)
            total_deep_reports = summary.get('total_deep_reports', 0)

            print(f"\n   📈 深度洞察: {'✅ 成功' if deep_success else '❌ 失败'}")
            print(f"      分析主题: {deep_topics} 个")
            print(f"      生成报告: {total_deep_reports} 份")

            # 显示每个模型的报告
            deep_model_reports = deep_report.get('model_reports', [])
            if deep_model_reports:
                print(f"      模型报告:")
                for mr in deep_model_reports:
                    display = mr.get('model_display') or mr.get('model') or 'LLM'
                    if mr.get('success'):
                        print(f"         - {display}: 报告ID {mr.get('report_id')}")
                        notion_push = mr.get('notion_push')
                        if notion_push and notion_push.get('success'):
                            print(f"           📄 Notion: 成功 - {notion_push.get('page_url')}")

            # 总体统计
            total_reports = total_light_reports + total_deep_reports
            print(f"\n   📊 总计: {total_reports} 份报告生成成功")

        elif 'category' in result:
            # 单个板块报告（保持向后兼容）
            print(f"   板块: {result.get('category')}")
            print(f"   分析主题: {result.get('topics_analyzed', 0)} 个")
            model_reports = result.get('model_reports', [])
            if model_reports:
                print(f"   模型输出:")
                for mr in model_reports:
                    display = mr.get('model_display') or mr.get('model') or 'LLM'
                    if mr.get('success'):
                        print(f"      - {display}: 报告ID {mr.get('report_id')}")
                        notion_push = mr.get('notion_push')
                        if notion_push:
                            if notion_push.get('success'):
                                print(f"        📄 Notion: 成功 - {notion_push.get('page_url')}")
                            else:
                                print(f"        📄 Notion: 失败 - {notion_push.get('error')}")
                    else:
                        error_msg = mr.get('error', '未知错误')
                        print(f"      - {display}: 失败 - {error_msg}")
            else:
                if result.get('report_id'):
                    print(f"   报告ID: {result.get('report_id')}")

                notion_push = result.get('notion_push')
                if notion_push:
                    if notion_push.get('success'):
                        print(f"   📄 Notion推送: 成功 - {notion_push.get('page_url')}")
                    else:
                        print(f"   📄 Notion推送: 失败 - {notion_push.get('error')}")
        else:
            # 旧版格式报告（兼容性）
            print(f"   成功板块: {result.get('successful_reports', 0)}/{result.get('total_categories', 0)}")
            print(f"   总分析主题: {result.get('total_topics_analyzed', 0)} 个")
            if result.get('failures'):
                print(f"   失败板块: {len(result['failures'])} 个")

            # 显示Notion推送统计
            reports = result.get('reports', [])
            notion_success = sum(1 for r in reports if r.get('notion_push', {}).get('success'))
            if notion_success > 0:
                print(f"   📄 Notion推送: {notion_success}/{len(reports)} 个报告成功推送")
    
    elif task_type == 'full':
        results = result.get('results', {})
        print(f"✅ 完整维护任务完成")
        
        # 爬取结果
        crawl_result = results.get('crawl', {})
        if crawl_result.get('success'):
            print(f"   爬取: 发现 {crawl_result.get('topics_found', 0)} 个主题，成功 {crawl_result.get('topics_crawled', 0)} 个")
        
        # 清理结果
        cleanup_result = results.get('cleanup', {})
        if cleanup_result.get('success'):
            cleanup_data = cleanup_result.get('cleanup_result', {})
            print(f"   清理: 删除 {cleanup_data.get('deleted_topics', 0)} 个过期主题")
        
        # 统计结果
        stats_result = results.get('stats', {})
        if stats_result.get('success'):
            stats = stats_result.get('stats', {})
            print(f"   统计: 用户 {stats.get('users_count', 0)}, 主题 {stats.get('topics_count', 0)}, 回复 {stats.get('posts_count', 0)}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
