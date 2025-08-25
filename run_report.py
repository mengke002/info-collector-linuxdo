#!/usr/bin/env python3
"""
本地报告生成脚本
生成24小时内全局top 30主题的分析报告
"""
import asyncio
import os
import sys
import logging
from datetime import datetime, timezone, timedelta

# 添加src路径到Python模块搜索路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.report_generator import report_generator
from src.config import config
from src.database import db_manager
from src.logger import setup_logging


def setup_local_logging():
    """设置本地日志配置"""
    # 使用项目的标准日志设置
    setup_logging()
    
    # 返回主日志记录器
    return logging.getLogger(__name__)


async def test_database_connection():
    """测试数据库连接"""
    try:
        print("🔗 测试数据库连接...")
        
        # 获取数据库配置
        db_config = config.get_database_config()
        print(f"📍 数据库地址: {db_config['host']}:{db_config['port']}")
        print(f"📋 数据库名称: {db_config['database']}")
        
        # 测试连接
        with db_manager.get_cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
        print("✅ 数据库连接成功!")
        return True
        
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return False


async def check_hot_topics():
    """检查热门主题数据"""
    try:
        print("\n🔍 检查热门主题数据...")
        
        # 获取24小时内的热门主题
        hot_topics = db_manager.get_hot_topics_all(limit=30, hours_back=24)
        
        if not hot_topics:
            print("⚠️  过去24小时内没有找到热门主题数据")
            return False
        
        print(f"📊 找到 {len(hot_topics)} 个热门主题")
        
        # 显示前5个主题的信息
        print("\n🔝 热门主题预览:")
        for i, topic in enumerate(hot_topics[:5], 1):
            print(f"  {i}. {topic.get('title', '未知标题')[:60]}...")
            print(f"     热度分数: {topic.get('hotness_score', 0):.2f}")
            print(f"     分类: {topic.get('category', '未知')}")
            print()
        
        if len(hot_topics) > 5:
            print(f"   ... 还有 {len(hot_topics) - 5} 个热门主题")
        
        return True
        
    except Exception as e:
        print(f"❌ 检查热门主题时出错: {e}")
        return False


async def check_llm_client():
    """检查LLM客户端"""
    try:
        print("\n🤖 检查LLM客户端...")
        
        if not report_generator.llm:
            print("❌ LLM客户端未初始化")
            return False
        
        print(f"✅ LLM客户端已初始化")
        print(f"📍 模型: {report_generator.llm.model}")
        print(f"🔗 API Base URL: {report_generator.llm.base_url}")
        
        return True
        
    except Exception as e:
        print(f"❌ 检查LLM客户端时出错: {e}")
        return False


async def generate_and_save_report():
    """生成并保存报告"""
    try:
        print("\n📝 开始生成24小时全局热点分析报告...")
        print("⏳ 这可能需要几分钟时间，请耐心等待...")
        
        # 生成报告
        result = await report_generator.generate_category_report(
            category=None,  # 全站分析
            hours_back=24   # 24小时
        )
        
        if not result.get('success'):
            print(f"❌ 报告生成失败: {result.get('error', '未知错误')}")
            return None
        
        print(f"✅ 报告生成成功!")
        print(f"📊 分析主题数: {result.get('topics_analyzed', 0)}")
        print(f"🆔 报告ID: {result.get('report_id', 'unknown')}")
        
        # 获取完整报告内容
        report_id = result.get('report_id')
        if report_id:
            report_data = db_manager.get_report_content(report_id)
            if report_data:
                report_content = report_data.get('report_content', '')
                
                # 保存到本地文件
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"linuxdo_热点报告_{timestamp}.md"
                
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(report_content)
                
                print(f"💾 报告已保存到本地文件: {filename}")
                
                # 显示报告预览
                print("\n📋 报告内容预览:")
                print("=" * 80)
                
                # 显示前800字符的内容
                preview_content = report_content[:1500]
                print(preview_content)
                
                if len(report_content) > 1500:
                    print("\n... (内容太长，已截断。请查看完整报告文件)")
                
                print("=" * 80)
                
                return {
                    'filename': filename,
                    'content': report_content,
                    'result': result
                }
        
        return result
        
    except Exception as e:
        print(f"❌ 生成报告时出错: {e}")
        import traceback
        print(f"详细错误信息: {traceback.format_exc()}")
        return None


async def main():
    """主函数"""
    print("🚀 开始本地报告生成流程")
    print("=" * 60)
    
    # 设置日志
    logger = setup_local_logging()
    
    try:
        # 1. 测试数据库连接
        if not await test_database_connection():
            print("❌ 数据库连接失败，无法继续")
            return
        
        # 2. 检查热门主题数据
        if not await check_hot_topics():
            print("❌ 没有足够的热门主题数据，无法生成报告")
            return
        
        # 3. 检查LLM客户端
        if not await check_llm_client():
            print("❌ LLM客户端不可用，无法进行AI分析")
            return
        
        # 4. 生成并保存报告
        result = await generate_and_save_report()
        
        if result:
            print("\n🎉 报告生成流程完成!")
            
            if isinstance(result, dict) and result.get('filename'):
                print(f"📄 完整报告已保存到: {result['filename']}")
                print(f"📊 共分析了 {result.get('result', {}).get('topics_analyzed', 0)} 个热门主题")
        else:
            print("❌ 报告生成失败")
            
    except Exception as e:
        print(f"❌ 执行过程中出错: {e}")
        import traceback
        print(f"详细错误信息: {traceback.format_exc()}")
    
    print("\n" + "=" * 60)
    print("🏁 程序执行完成")


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())