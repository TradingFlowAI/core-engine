import asyncio
import json
import hashlib
from typing import Any, Dict, List, Optional

import httpx

from infra.config import CONFIG
from infra.utils.redis_manager import RedisManager
from common.edge import Edge
from common.node_decorators import register_node_type
from common.signal_types import SignalType
from nodes.node_base import NodeBase, NodeStatus

DATA_HANDLE = "data"

# ChainCatcher API endpoint
CHAINCATCHER_BASE_URL = "https://www.chaincatcher.com/OpenApi"


@register_node_type(
    "chaincatcher_node",
    default_params={
        "content_type": 1,  # 1: articles, 2: news flash
        "language": "en",
    },
)
class ChainCatcherNode(NodeBase):
    """
    ChainCatcher Node - Fetch articles and news flash from ChainCatcher.

    Inputs:
      - content_type: 1 for articles, 2 for news flash (required)
      - language: Language code (default: en). Supported: zh, zh-TW, en, ja, ko
      - feat_type: Column type (1: ChainCatcher Featured)
      - article_type: Article type (1: Original, only valid when content_type=1)
      - news_flash_type: News flash type (1: Featured, only valid when content_type=2)
      - page: Page number (default: 1)
      - limit: Items per page, 1-30 (default: 10)

    Outputs:
      - data: JSON response from ChainCatcher containing articles/news list

    Caching:
      - All queries cached in Redis for 10 minutes (news data updates frequently)
    """

    TTL_SECONDS = 600  # 10 minutes cache

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        content_type: int = 1,
        language: str = "en",
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        # Preserve raw params before super consumes metadata defaults
        raw_params = dict(kwargs)

        kwargs.setdefault("version", "0.0.1")
        kwargs.setdefault("display_name", "ChainCatcher Node")
        kwargs.setdefault("node_category", "data")
        kwargs.setdefault("description", "Fetch articles and news from ChainCatcher")
        kwargs.setdefault("tags", ["chaincatcher", "news", "articles", "crypto"])

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

        self.content_type = int(content_type) if content_type else 1
        self.language = language or "en"
        self.api_token = CONFIG.get("CHAINCATCHER_API_TOKEN", "")
        self.redis_client = None
        
        # Filter out known meta/infra keys when storing params
        meta_keys = {
            "version",
            "display_name",
            "node_category",
            "description",
            "author",
            "tags",
            "user_id",
            "enable_credits",
        }
        self.params = {k: v for k, v in raw_params.items() if k not in meta_keys}

    def _register_input_handles(self) -> None:
        # No upstream signals; parameters come from node config
        pass

    def _register_output_handles(self) -> None:
        try:
            self.register_output_handle(
                name=DATA_HANDLE,
                data_type=dict,
                description="ChainCatcher response",
                example={"result": 1, "data": {"totle": 100, "list": []}},
            )
        except Exception:
            return

    async def _get_redis_client(self):
        if self.redis_client:
            return self.redis_client
        try:
            self.redis_client = RedisManager.get_client()
        except Exception as e:
            await self.persist_log(f"Redis init failed: {e}", "WARNING")
            self.redis_client = None
        return self.redis_client

    def _make_cache_key(self, payload: Dict[str, Any]) -> str:
        base = {
            "content_type": self.content_type,
            "language": self.language,
            "payload": payload,
        }
        raw = json.dumps(base, sort_keys=True, default=str)
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
        return f"chaincatcher:cache:{digest}"

    async def _get_cache(self, key: str) -> Optional[Dict[str, Any]]:
        client = await self._get_redis_client()
        if not client:
            return None
        try:
            cached = await asyncio.to_thread(client.get, key)
            if cached:
                return json.loads(cached)
        except Exception as e:
            await self.persist_log(f"Redis get failed: {e}", "DEBUG")
        return None

    async def _set_cache(self, key: str, value: Dict[str, Any]) -> None:
        client = await self._get_redis_client()
        if not client:
            return
        try:
            data = json.dumps(value)
            await asyncio.to_thread(client.setex, key, self.TTL_SECONDS, data)
        except Exception as e:
            await self.persist_log(f"Redis set failed: {e}", "DEBUG")

    def _build_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Build the API request configuration."""
        # Map language codes
        lang_map = {
            "zh": "",  # Default Chinese
            "zh-tw": "zh-TW",
            "zh-TW": "zh-TW",
            "en": "en",
            "ja": "ja",
            "ko": "ko",
        }
        language = params.get("language") or self.language or "en"
        mapped_lang = lang_map.get(language, language)

        headers = {
            "token": self.api_token,
            "language": mapped_lang,
            "Content-Type": "application/json",
        }

        # Build request body
        body: Dict[str, Any] = {}
        
        # Content type: 1=articles, 2=news flash
        content_type = params.get("content_type") or self.content_type
        if content_type:
            body["type"] = int(content_type)

        # Optional parameters
        if params.get("feat_type"):
            body["featType"] = int(params["feat_type"])
        
        if params.get("article_type") and int(content_type) == 1:
            body["articleType"] = int(params["article_type"])
        
        if params.get("news_flash_type") and int(content_type) == 2:
            body["newsFlashType"] = int(params["news_flash_type"])
        
        if params.get("page"):
            body["page"] = int(params["page"])
        
        if params.get("limit"):
            # Clamp limit to 1-30
            limit = max(1, min(30, int(params["limit"])))
            body["limit"] = limit

        return {
            "endpoint": "FetchListByType",
            "headers": headers,
            "body": body,
        }

    async def _request_chaincatcher(
        self, endpoint: str, body: Dict[str, Any], headers: Dict[str, Any]
    ) -> Dict[str, Any]:
        url = f"{CHAINCATCHER_BASE_URL}/{endpoint}"
        timeout = httpx.Timeout(30.0, connect=10.0)
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, headers=headers, json=body)
            try:
                data = resp.json()
            except Exception:
                text = await resp.aread()
                return {
                    "result": 0,
                    "message": f"Invalid JSON response: {text[:500]}",
                    "data": {},
                }

            # Normalize structure
            result_code = data.get("result", 0)
            return {
                "result": result_code,
                "message": data.get("message"),
                "data": data.get("data"),
            }

    async def execute(self) -> bool:
        if not self.api_token:
            await self.persist_log("CHAINCATCHER_API_TOKEN not set", "ERROR")
            await self.set_status(NodeStatus.FAILED, "Missing API token")
            return False

        params = getattr(self, "params", {}) if hasattr(self, "params") else {}
        params = {**params}
        params.setdefault("language", self.language)
        params.setdefault("content_type", self.content_type)

        try:
            request_cfg = self._build_request(params)
        except Exception as e:
            await self.persist_log(f"Invalid parameters: {e}", "ERROR")
            await self.set_status(NodeStatus.FAILED, str(e))
            return False

        headers = request_cfg["headers"]
        body = request_cfg["body"]
        endpoint = request_cfg["endpoint"]

        # Check cache
        cache_key = self._make_cache_key(body)
        cache_hit = await self._get_cache(cache_key)
        if cache_hit:
            await self.persist_log("Cache hit for ChainCatcher query", "INFO")
            await self._emit_result(cache_hit, from_cache=True)
            await self.set_status(NodeStatus.COMPLETED)
            return True

        await self.persist_log(
            f"Calling ChainCatcher {endpoint}",
            "INFO",
            log_metadata={"body": body}
        )

        try:
            response = await self._request_chaincatcher(endpoint, body, headers)
        except Exception as e:
            await self.persist_log(f"Request failed: {e}", "ERROR")
            await self.set_status(NodeStatus.FAILED, str(e))
            return False

        # Cache on success (result==1)
        if response.get("result") == 1:
            await self._set_cache(cache_key, response)

        await self._emit_result(response, from_cache=False)

        if response.get("result") == 1:
            await self.set_status(NodeStatus.COMPLETED)
            return True
        else:
            msg = response.get("message") or f"ChainCatcher result={response.get('result')}"
            await self.set_status(NodeStatus.FAILED, msg)
            return False

    async def _emit_result(self, payload: Dict[str, Any], from_cache: bool):
        # Attach metadata
        enriched = {
            **payload,
            "_meta": {
                "source": "chaincatcher",
                "content_type": self.content_type,
                "language": self.language,
                "from_cache": from_cache,
            },
        }
        # Publish via signal bus
        try:
            await self.send_signal(
                source_handle=DATA_HANDLE,
                signal_type=SignalType.ANY,
                payload=enriched,
            )
        except Exception as e:
            await self.persist_log(f"Failed to emit signal: {e}", "ERROR")
