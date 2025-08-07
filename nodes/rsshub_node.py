import asyncio
import json
import logging
import time
import traceback
from typing import Any, Dict, List

import aiohttp

from tradingflow.station.common.edge import Edge
from tradingflow.station.common.node_decorators import register_node_type
from tradingflow.station.common.signal_types import SignalType
from tradingflow.station.nodes.node_base import NodeBase, NodeStatus

# Define input/output handle names
DATA_OUTPUT_HANDLE = "data_output_handle"
ERROR_HANDLE = "error_handle"


@register_node_type(
    "rss_listener_node",
    default_params={
        "rsshub_url": "https://rsshub.app",
        "route": "",  # 例如: "/telegram/channel/awesomeRSSHub"
        "parameters": {},
        "keywords": "",
        "token": None,
        "timeout": 120,  # 请求超时时间（秒）
        "max_items": 20,  # 最大返回条目数
        "include_content": True,  # 是否包含内容详情
    },
)
class RSSHubNode(NodeBase):
    """
    RSSHub Node - Used to fetch RSS content provided by RSSHub

    Input parameters:
    - rsshub_url: RSSHub instance URL, defaults to 'https://rsshub.app'
    - route: RSSHub route, e.g., '/telegram/channel/awesomeRSSHub'
    - parameters: Custom parameters for some routes
    - keywords: Filter titles and descriptions
    - timeout: Request timeout in seconds
    - max_items: Maximum number of items to return
    - include_content: Whether to include content details

    Output signals:
    - DATA_OUTPUT_HANDLE: Retrieved RSS data
    - ERROR_HANDLE: Error information
    """

    def __init__(
            self,
            flow_id: str,
            component_id: int,
            cycle: int,
            node_id: str,
            name: str,
            rsshub_url: str = "https://rsshub.app",
            route: str = "",
            parameters: Dict[str, Any] = None,
            keywords: str = None,
            token: str = None,
            timeout: int = 120,
            max_items: int = 20,
            include_content: bool = True,
            input_edges: List[Edge] = None,
            output_edges: List[Edge] = None,
            state_store=None,
            **kwargs,
    ):
        """
        Initialize RSSHub node

        Args:
            flow_id: Flow ID
            component_id: Component ID
            cycle: Node execution cycle
            node_id: Node unique identifier
            name: Node name
            rsshub_url: RSSHub instance URL
            route: RSSHub route
            timeout: Request timeout in seconds
            max_items: Maximum number of items to return
            include_content: Whether to include content details
            input_edges: Input edge list
            output_edges: Output edge list
            state_store: State storage
            **kwargs: Other parameters passed to base class
        """
        super().__init__(
            flow_id=flow_id,
            component_id=component_id,
            cycle=cycle,
            node_id=node_id,
            name=name,
            input_edges=input_edges,
            output_edges=output_edges,
            state_store=state_store,
            **kwargs,
        )

        # Save parameters
        self.rsshub_url = rsshub_url.rstrip("/")  # Remove trailing slash
        self.route = route.strip("/")  # Remove leading and trailing slashes
        self.timeout = max(1, min(300, timeout))  # Limit to 1-300 seconds
        self.max_items = max(1, min(100, max_items))  # Limit to 1-100 items
        self.include_content = include_content
        self.parameters = parameters
        self.keywords = keywords
        self.token = token

    async def fetch_rss(self) -> Dict[str, Any]:
        """
        Fetch RSS data from RSSHub

        Returns:
            Dict[str, Any]: Dictionary containing RSS data
        """
        # Build complete URL
        url = f"{self.rsshub_url}/{self.route}"

        # Add custom parameters
        if self.parameters:
            url += '/' + '&'.join(f'{k}={v}' for k, v in self.parameters.items())

        # Add query parameters
        if "?" not in url:
            url += "?"
        else:
            url += "&"

        # Add limit parameter for number of items
        url += f"limit={self.max_items}"
        # Set format to JSON
        url += "&format=json"

        # Filter titles and descriptions, supports regex
        if self.keywords:
            url += f"&filter={self.keywords}"

        # Add full content output parameter if needed
        if self.include_content:
            url += "&format=json"  # Use JSON format to get complete content

        await self.persist_log(f"Fetching RSS from: {url}", "INFO")

        try:
            # Create async HTTP session
            async with aiohttp.ClientSession() as session:
                # Configure aiohttp proxy
                # Send GET request
                async with session.get(url, timeout=self.timeout) as response:
                    # Check response status
                    if response.status != 200:
                        error_msg = f"Failed to fetch RSS: HTTP {response.status}"
                        await self.persist_log(error_msg, "ERROR")
                        return {"error": error_msg, "status_code": response.status}

                    # Try to parse JSON response
                    try:
                        data = await response.json()
                        await self.persist_log(f"Successfully fetched RSS data with {len(data.get('items', []))} items", "INFO")
                        return data
                    except json.JSONDecodeError:
                        # If not JSON format, try to parse as XML/RSS
                        content = await response.text()
                        await self.persist_log("Response is not JSON, parsing as XML/RSS using feedparser", "INFO")

                        # Use feedparser to parse RSS content
                        # Note: Need to install feedparser first: pip install feedparser
                        try:
                            import feedparser
                            import io
                            from datetime import datetime

                            # Use feedparser to parse content
                            feed = feedparser.parse(io.BytesIO(content.encode('utf-8')))

                            # Extract channel information
                            title = feed.feed.get('title', '')
                            description = feed.feed.get('description', '')
                            link = feed.feed.get('link', '')

                            # Extract items
                            items = []
                            for entry in feed.entries[:self.max_items]:
                                # Handle date format
                                published = ''
                                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                                    try:
                                        published = datetime(*entry.published_parsed[:6]).isoformat()
                                    except Exception as e:
                                        await self.persist_log(f"Failed to parse date: {e}", "WARNING")

                                # Build item dictionary
                                item = {
                                    "title": entry.get('title', ''),
                                    "description": entry.get('description', ''),
                                    "content": entry.get('content', [{}])[0].get('value',
                                                                                 '') if 'content' in entry else '',
                                    "link": entry.get('link', ''),
                                    "published": published,
                                    "id": entry.get('id', ''),
                                    "author": entry.get('author', ''),
                                    "tags": [tag.get('term', '') for tag in entry.get('tags', [])]
                                }
                                items.append(item)

                            await self.persist_log(f"Successfully parsed RSS feed with {len(items)} items", "INFO")
                            return {
                                "title": title,
                                "description": description,
                                "link": link,
                                "items": items
                            }
                        except Exception as e:
                            error_msg = f"Failed to parse RSS content: {str(e)}"
                            await self.persist_log(error_msg, "ERROR")
                            return {"error": error_msg, "raw_content": content[:1000]}  # Only return partial content to avoid being too large

        except aiohttp.ClientError as e:
            error_msg = f"HTTP request error: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            return {"error": error_msg}
        except asyncio.TimeoutError:
            error_msg = f"Request timed out after {self.timeout} seconds"
            await self.persist_log(error_msg, "ERROR")
            return {"error": error_msg}
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            return {"error": error_msg}

    async def execute(self) -> bool:
        """Execute node logic to fetch RSS data"""
        start_time = time.time()
        try:
            await self.persist_log(f"Executing RSSHubNode for route: {self.route}", "INFO")

            # Validate required parameters
            if not self.route:
                error_msg = "Route parameter is required"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

            await self.set_status(NodeStatus.RUNNING)

            # Fetch RSS data
            rss_data = await self.fetch_rss()

            # Check for errors
            if "error" in rss_data:
                error_msg = f"Failed to fetch RSS data: {rss_data['error']}"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

            # Process retrieved data
            processed_data = rss_data

            # Send data signal
            if await self.send_signal(DATA_OUTPUT_HANDLE, SignalType.DATASET, payload=processed_data):
                await self.persist_log(f"Successfully sent RSS data with {len(processed_data.get('items', []))} items", "INFO")
                await self.set_status(NodeStatus.COMPLETED)
                return True
            else:
                error_msg = "Failed to send RSS data signal"
                await self.persist_log(error_msg, "ERROR")
                await self.set_status(NodeStatus.FAILED, error_msg)
                await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
                return False

        except asyncio.CancelledError:
            # Task was cancelled
            await self.set_status(NodeStatus.TERMINATED, "Task cancelled")
            return True
        except Exception as e:
            error_msg = f"Error executing RSSHubNode: {str(e)}"
            await self.persist_log(error_msg, "ERROR")
            await self.persist_log(traceback.format_exc(), "DEBUG")
            await self.set_status(NodeStatus.FAILED, error_msg)
            await self.send_signal(ERROR_HANDLE, SignalType.TEXT, payload=error_msg)
            return False
