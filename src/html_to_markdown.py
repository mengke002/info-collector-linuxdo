"""
HTML到Markdown转换模块
将论坛的HTML内容转换为干净的Markdown格式
"""
import re
import html
from typing import Optional
import logging


class HTMLToMarkdownConverter:
    """HTML到Markdown转换器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def convert(self, html_content: str) -> str:
        """将HTML内容转换为Markdown"""
        if not html_content:
            return ""
        
        try:
            # 解码HTML实体
            content = html.unescape(html_content)
            
            # 移除多余的空白字符
            content = re.sub(r'\s+', ' ', content)
            
            # 转换各种HTML标签为Markdown
            content = self._convert_tags(content)
            
            # 清理多余的空行
            content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
            content = content.strip()
            
            return content
            
        except Exception as e:
            self.logger.warning(f"HTML转Markdown失败: {e}")
            return self._fallback_convert(html_content)
    
    def _convert_tags(self, content: str) -> str:
        """转换HTML标签为Markdown"""
        
        # 处理代码块
        content = re.sub(r'<pre><code[^>]*>(.*?)</code></pre>', r'```\n\1\n```', content, flags=re.DOTALL)
        content = re.sub(r'<code[^>]*>(.*?)</code>', r'`\1`', content)
        
        # 处理标题
        for i in range(6, 0, -1):
            content = re.sub(f'<h{i}[^>]*>(.*?)</h{i}>', f'{"#" * i} \\1\n', content)
        
        # 处理粗体和斜体
        content = re.sub(r'<strong[^>]*>(.*?)</strong>', r'**\1**', content)
        content = re.sub(r'<b[^>]*>(.*?)</b>', r'**\1**', content)
        content = re.sub(r'<em[^>]*>(.*?)</em>', r'*\1*', content)
        content = re.sub(r'<i[^>]*>(.*?)</i>', r'*\1*', content)
        
        # 处理链接
        content = re.sub(r'<a[^>]*href=["\']([^"\']*)["\'][^>]*>(.*?)</a>', r'[\2](\1)', content)
        
        # 处理图片
        content = re.sub(r'<img[^>]*src=["\']([^"\']*)["\'][^>]*alt=["\']([^"\']*)["\'][^>]*/?>', r'![\2](\1)', content)
        content = re.sub(r'<img[^>]*src=["\']([^"\']*)["\'][^>]*/?>', r'![](\1)', content)
        
        # 处理列表
        content = re.sub(r'<ul[^>]*>', '\n', content)
        content = re.sub(r'</ul>', '\n', content)
        content = re.sub(r'<ol[^>]*>', '\n', content)
        content = re.sub(r'</ol>', '\n', content)
        content = re.sub(r'<li[^>]*>(.*?)</li>', r'- \1\n', content)
        
        # 处理引用
        content = re.sub(r'<blockquote[^>]*>(.*?)</blockquote>', r'\n> \1\n', content, flags=re.DOTALL)
        
        # 处理换行
        content = re.sub(r'<br\s*/?>', '\n', content)
        content = re.sub(r'<p[^>]*>(.*?)</p>', r'\1\n\n', content)
        
        # 处理分割线
        content = re.sub(r'<hr[^>]*/?>', '\n---\n', content)
        
        # 处理表格（简化版）
        content = re.sub(r'<table[^>]*>', '\n', content)
        content = re.sub(r'</table>', '\n', content)
        content = re.sub(r'<tr[^>]*>', '', content)
        content = re.sub(r'</tr>', '\n', content)
        content = re.sub(r'<td[^>]*>(.*?)</td>', r'\1 | ', content)
        content = re.sub(r'<th[^>]*>(.*?)</th>', r'\1 | ', content)
        
        # 处理论坛特有的结构
        content = self._convert_forum_specific(content)
        
        # 移除剩余的HTML标签
        content = re.sub(r'<[^>]+>', '', content)
        
        return content
    
    def _convert_forum_specific(self, content: str) -> str:
        """转换论坛特有的HTML结构"""
        
        # 处理lightbox图片包装器
        content = re.sub(
            r'<div class="lightbox-wrapper"><a class="lightbox"[^>]*href=["\']([^"\']*)["\'][^>]*>.*?</a></div>',
            r'![](\1)',
            content,
            flags=re.DOTALL
        )
        
        # 处理用户提及
        content = re.sub(r'<a class="mention"[^>]*href=["\'][^"\']*["\'][^>]*>@([^<]*)</a>', r'@\1', content)
        
        # 处理引用回复
        content = re.sub(
            r'<aside class="quote"[^>]*>.*?<div class="title">.*?</div>(.*?)</aside>',
            r'\n> \1\n',
            content,
            flags=re.DOTALL
        )
        
        # 处理代码语法高亮
        content = re.sub(
            r'<div class="highlight"><pre class="highlight"><code[^>]*>(.*?)</code></pre></div>',
            r'```\n\1\n```',
            content,
            flags=re.DOTALL
        )
        
        return content
    
    def _fallback_convert(self, html_content: str) -> str:
        """备用转换方法，简单移除HTML标签"""
        try:
            # 简单移除所有HTML标签
            content = re.sub(r'<[^>]+>', '', html_content)
            content = html.unescape(content)
            content = re.sub(r'\s+', ' ', content)
            return content.strip()
        except Exception:
            return html_content


# 全局转换器实例
html_to_markdown = HTMLToMarkdownConverter()