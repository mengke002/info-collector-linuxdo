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
    parser.add_argument('--task', choices=['crawl', 'cleanup', 'stats', 'full'], 
                       default='crawl', help='要执行的任务类型')
    parser.add_argument('--retention-days', type=int, 
                       help='数据保留天数（仅用于cleanup任务）')
    parser.add_argument('--output', choices=['json', 'text'], default='text',
                       help='输出格式')
    parser.add_argument('--concurrent', action='store_true', default=True,
                       help='使用并发模式（默认启用）')
    parser.add_argument('--serial', action='store_true',
                       help='使用串行模式（覆盖--concurrent）')
    
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