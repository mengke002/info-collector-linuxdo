"""
Notion API 客户端
用于将报告推送到Notion页面
"""
import logging
import requests
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from .config import config


class NotionClient:
    """Notion API 客户端"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.base_url = "https://api.notion.com/v1"
        self.version = "2022-06-28"
        
        # 从配置获取Notion设置
        notion_config = config.get_notion_config()
        self.integration_token = notion_config.get('integration_token')
        self.parent_page_id = notion_config.get('parent_page_id')
        
        if not self.integration_token:
            self.logger.warning("Notion集成token未配置")
        if not self.parent_page_id:
            self.logger.warning("Notion父页面ID未配置")
    
    def _get_headers(self) -> Dict[str, str]:
        """获取API请求头"""
        return {
            "Authorization": f"Bearer {self.integration_token}",
            "Content-Type": "application/json",
            "Notion-Version": self.version
        }
    
    def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict[str, Any]:
        """发送API请求"""
        url = f"{self.base_url}/{endpoint}"
        headers = self._get_headers()
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, timeout=30)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data, timeout=30)
            elif method.upper() == "PATCH":
                response = requests.patch(url, headers=headers, json=data, timeout=30)
            else:
                raise ValueError(f"不支持的HTTP方法: {method}")
            
            response.raise_for_status()
            return {"success": True, "data": response.json()}
            
        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            
            # 尝试获取更详细的错误信息
            try:
                if hasattr(e, 'response') and e.response is not None:
                    error_detail = e.response.json()
                    if 'message' in error_detail:
                        error_msg = f"{e}: {error_detail['message']}"
                    elif 'error' in error_detail:
                        error_msg = f"{e}: {error_detail['error']}"
            except:
                pass
            
            self.logger.error(f"Notion API请求失败: {error_msg}")
            return {"success": False, "error": error_msg}
    
    def get_page_children(self, page_id: str) -> Dict[str, Any]:
        """获取页面的子页面"""
        return self._make_request("GET", f"blocks/{page_id}/children")
    
    def create_page(self, parent_id: str, title: str, content_blocks: List[Dict] = None) -> Dict[str, Any]:
        """创建新页面"""
        data = {
            "parent": {"page_id": parent_id},
            "properties": {
                "title": {
                    "title": [
                        {
                            "text": {
                                "content": title
                            }
                        }
                    ]
                }
            }
        }
        
        if content_blocks:
            data["children"] = content_blocks
        
        return self._make_request("POST", "pages", data)
    
    def find_or_create_year_page(self, year: str) -> Optional[str]:
        """查找或创建年份页面"""
        try:
            # 获取父页面的子页面
            children_result = self.get_page_children(self.parent_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取父页面子页面失败: {children_result.get('error')}")
                return None
            
            # 查找年份页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == year:
                        return child["id"]
            
            # 创建年份页面
            self.logger.info(f"创建年份页面: {year}")
            create_result = self.create_page(self.parent_page_id, year)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建年份页面失败: {create_result.get('error')}")
                return None
                
        except Exception as e:
            self.logger.error(f"查找或创建年份页面时出错: {e}")
            return None
    
    def find_or_create_month_page(self, year_page_id: str, month: str) -> Optional[str]:
        """查找或创建月份页面"""
        try:
            # 获取年份页面的子页面
            children_result = self.get_page_children(year_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取年份页面子页面失败: {children_result.get('error')}")
                return None
            
            # 查找月份页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == month:
                        return child["id"]
            
            # 创建月份页面
            self.logger.info(f"创建月份页面: {month}")
            create_result = self.create_page(year_page_id, month)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建月份页面失败: {create_result.get('error')}")
                return None
                
        except Exception as e:
            self.logger.error(f"查找或创建月份页面时出错: {e}")
            return None
    
    def find_or_create_day_page(self, month_page_id: str, day: str) -> Optional[str]:
        """查找或创建日期页面"""
        try:
            # 获取月份页面的子页面
            children_result = self.get_page_children(month_page_id)
            if not children_result.get("success"):
                self.logger.error(f"获取月份页面子页面失败: {children_result.get('error')}")
                return None
            
            # 查找日期页面
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == day:
                        return child["id"]
            
            # 创建日期页面
            self.logger.info(f"创建日期页面: {day}")
            create_result = self.create_page(month_page_id, day)
            if create_result.get("success"):
                return create_result["data"]["id"]
            else:
                self.logger.error(f"创建日期页面失败: {create_result.get('error')}")
                return None
                
        except Exception as e:
            self.logger.error(f"查找或创建日期页面时出错: {e}")
            return None
    
    def check_report_exists(self, day_page_id: str, report_title: str) -> Optional[Dict[str, Any]]:
        """检查报告是否已经存在"""
        try:
            # 获取日期页面的子页面
            children_result = self.get_page_children(day_page_id)
            if not children_result.get("success"):
                return None
            
            # 查找同名报告
            for child in children_result["data"].get("results", []):
                if child.get("type") == "child_page":
                    page_title = self._extract_page_title(child)
                    if page_title == report_title:
                        page_id = child["id"]
                        page_url = f"https://www.notion.so/{page_id.replace('-', '')}"
                        return {
                            "exists": True,
                            "page_id": page_id,
                            "page_url": page_url
                        }
            
            return {"exists": False}
                
        except Exception as e:
            self.logger.error(f"检查报告是否存在时出错: {e}")
            return None
    
    def _extract_page_title(self, page_data: Dict) -> str:
        """从页面数据中提取标题"""
        try:
            if page_data.get("type") == "child_page":
                title_data = page_data.get("child_page", {}).get("title", "")
                return title_data
            return ""
        except Exception:
            return ""
    
    def _parse_rich_text(self, text: str) -> List[Dict]:
        """解析文本中的Markdown格式，支持链接、粗体等"""
        import re
        
        rich_text = []
        
        # 首先处理Source引用 [Source: T1] 或 [Sources: T1, T2]
        source_pattern = r'\[Sources?:\s*([T\d\s,]+)\]'
        
        # 处理Source引用
        last_end = 0
        for match in re.finditer(source_pattern, text):
            # 添加Source引用前的普通文本
            if match.start() > last_end:
                before_text = text[last_end:match.start()]
                if before_text:
                    rich_text.extend(self._parse_links_and_formatting(before_text))
            
            # 添加Source引用（带特殊格式和提示）
            source_text = match.group(0)  # 完整的 [Source: T1] 文本
            rich_text.append({
                "type": "text",
                "text": {"content": f"📎 {source_text}"},
                "annotations": {
                    "italic": True,
                    "color": "blue",
                    "bold": False
                }
            })
            
            last_end = match.end()
        
        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.extend(self._parse_links_and_formatting(remaining_text))
        
        # 如果没有找到Source引用，处理整个文本
        if not rich_text:
            rich_text = self._parse_links_and_formatting(text)
        
        return rich_text
    
    def _parse_links_and_formatting(self, text: str) -> List[Dict]:
        """解析链接和格式，不包括Source引用"""
        import re
        
        rich_text = []
        
        # 处理Markdown链接 [text](url)
        link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
        
        last_end = 0
        for match in re.finditer(link_pattern, text):
            # 添加链接前的普通文本
            if match.start() > last_end:
                before_text = text[last_end:match.start()]
                if before_text:
                    rich_text.extend(self._parse_text_formatting(before_text))
            
            # 添加链接
            link_text = match.group(1)
            link_url = match.group(2)
            rich_text.append({
                "type": "text",
                "text": {
                    "content": link_text,
                    "link": {"url": link_url}
                }
            })
            
            last_end = match.end()
        
        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.extend(self._parse_text_formatting(remaining_text))
        
        # 如果没有找到任何链接，处理整个文本
        if not rich_text:
            rich_text = self._parse_text_formatting(text)
        
        return rich_text
    
    def _parse_text_formatting(self, text: str) -> List[Dict]:
        """解析文本格式（粗体、斜体等）"""
        import re
        
        rich_text = []
        
        # 处理粗体 **text**
        bold_pattern = r'\*\*([^*]+)\*\*'
        
        last_end = 0
        for match in re.finditer(bold_pattern, text):
            # 添加粗体前的普通文本
            if match.start() > last_end:
                before_text = text[last_end:match.start()]
                if before_text:
                    rich_text.append({
                        "type": "text",
                        "text": {"content": before_text}
                    })
            
            # 添加粗体文本
            bold_text = match.group(1)
            rich_text.append({
                "type": "text",
                "text": {"content": bold_text},
                "annotations": {"bold": True}
            })
            
            last_end = match.end()
        
        # 添加剩余的普通文本
        if last_end < len(text):
            remaining_text = text[last_end:]
            if remaining_text:
                rich_text.append({
                    "type": "text",
                    "text": {"content": remaining_text}
                })
        
        # 如果没有找到任何格式，返回普通文本
        if not rich_text:
            rich_text = [{
                "type": "text",
                "text": {"content": text}
            }]
        
        return rich_text

    def markdown_to_notion_blocks(self, markdown_content: str) -> List[Dict]:
        """将Markdown内容转换为Notion块，支持链接和格式"""
        blocks = []
        lines = markdown_content.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if not line:
                i += 1
                continue
            
            try:
                # 标题处理
                if line.startswith('# '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_1",
                        "heading_1": {
                            "rich_text": self._parse_rich_text(line[2:])
                        }
                    })
                elif line.startswith('## '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_2",
                        "heading_2": {
                            "rich_text": self._parse_rich_text(line[3:])
                        }
                    })
                elif line.startswith('### '):
                    blocks.append({
                        "object": "block",
                        "type": "heading_3",
                        "heading_3": {
                            "rich_text": self._parse_rich_text(line[4:])
                        }
                    })
                # 分割线
                elif line.startswith('---'):
                    blocks.append({
                        "object": "block",
                        "type": "divider",
                        "divider": {}
                    })
                # 列表项
                elif line.startswith('- ') or line.startswith('* '):
                    blocks.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": self._parse_rich_text(line[2:])
                        }
                    })
                # 普通段落
                else:
                    # 处理可能的多行段落
                    paragraph_lines = [line]
                    j = i + 1
                    while j < len(lines) and lines[j].strip() and not lines[j].startswith(('#', '-', '*', '---')):
                        paragraph_lines.append(lines[j].strip())
                        j += 1
                    
                    paragraph_text = ' '.join(paragraph_lines)
                    if paragraph_text:
                        blocks.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": self._parse_rich_text(paragraph_text)
                            }
                        })
                    
                    i = j - 1
                
            except Exception as e:
                # 如果解析失败，添加为普通文本
                self.logger.warning(f"解析Markdown行失败，使用普通文本: {line[:50]}... 错误: {e}")
                blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": line}}]
                    }
                })
            
            i += 1
        
        return blocks
    
    def create_report_page(self, report_title: str, report_content: str, 
                          report_date: datetime = None) -> Dict[str, Any]:
        """创建报告页面，按年/月/日层级组织"""
        try:
            if not self.integration_token or not self.parent_page_id:
                return {
                    "success": False,
                    "error": "Notion配置不完整"
                }
            
            # 使用报告日期或当前日期
            if report_date is None:
                report_date = datetime.now(timezone.utc) + timedelta(hours=8)  # 北京时间
            
            year = str(report_date.year)
            month = f"{report_date.month:02d}月"
            day = f"{report_date.day:02d}日"
            
            self.logger.info(f"开始创建报告页面: {year}/{month}/{day} - {report_title}")
            
            # 1. 查找或创建年份页面
            year_page_id = self.find_or_create_year_page(year)
            if not year_page_id:
                return {"success": False, "error": "无法创建年份页面"}
            
            # 2. 查找或创建月份页面
            month_page_id = self.find_or_create_month_page(year_page_id, month)
            if not month_page_id:
                return {"success": False, "error": "无法创建月份页面"}
            
            # 3. 查找或创建日期页面
            day_page_id = self.find_or_create_day_page(month_page_id, day)
            if not day_page_id:
                return {"success": False, "error": "无法创建日期页面"}
            
            # 3.5. 检查报告是否已经存在
            existing_report = self.check_report_exists(day_page_id, report_title)
            if existing_report and existing_report.get("exists"):
                self.logger.info(f"报告已存在，跳过创建: {existing_report.get('page_url')}")
                return {
                    "success": True,
                    "page_id": existing_report.get("page_id"),
                    "page_url": existing_report.get("page_url"),
                    "path": f"{year}/{month}/{day}/{report_title}",
                    "skipped": True,
                    "reason": "报告已存在"
                }
            
            # 4. 在日期页面下创建报告页面
            content_blocks = self.markdown_to_notion_blocks(report_content)
            
            # 严格限制块数量，避免API限制
            max_blocks = 50  # 降低限制以避免API错误
            if len(content_blocks) > max_blocks:
                self.logger.warning(f"报告内容过长({len(content_blocks)}个块)，截断到{max_blocks}个块")
                content_blocks = content_blocks[:max_blocks]
                
                # 添加截断提示
                content_blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{
                            "type": "text",
                            "text": {"content": "⚠️ 内容过长已截断，完整内容请查看数据库记录"},
                            "annotations": {"italic": True, "color": "gray"}
                        }]
                    }
                })
            
            # 验证每个块的内容长度
            validated_blocks = []
            for block in content_blocks:
                try:
                    # 检查rich_text内容长度
                    if block.get("type") in ["paragraph", "heading_1", "heading_2", "heading_3", "bulleted_list_item"]:
                        block_type = block["type"]
                        rich_text = block[block_type].get("rich_text", [])
                        
                        # 限制每个rich_text项的长度
                        for text_item in rich_text:
                            if text_item.get("text", {}).get("content"):
                                content = text_item["text"]["content"]
                                if len(content) > 2000:  # Notion限制
                                    text_item["text"]["content"] = content[:1997] + "..."
                    
                    validated_blocks.append(block)
                except Exception as e:
                    self.logger.warning(f"验证块时出错，跳过: {e}")
                    continue
            
            create_result = self.create_page(day_page_id, report_title, validated_blocks)
            
            if create_result.get("success"):
                page_id = create_result["data"]["id"]
                page_url = f"https://www.notion.so/{page_id.replace('-', '')}"
                
                self.logger.info(f"报告页面创建成功: {page_url}")
                return {
                    "success": True,
                    "page_id": page_id,
                    "page_url": page_url,
                    "path": f"{year}/{month}/{day}/{report_title}"
                }
            else:
                self.logger.error(f"创建报告页面失败: {create_result.get('error')}")
                return {"success": False, "error": create_result.get("error")}
                
        except Exception as e:
            self.logger.error(f"创建报告页面时出错: {e}")
            return {"success": False, "error": str(e)}


# 全局Notion客户端实例
notion_client = NotionClient()