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

# RootData endpoints
ROOTDATA_BASE_URL = "https://api.rootdata.com/open"


@register_node_type(
    "rootdata_node",
    default_params={
        "operation": "search",
        "language": "en",
    },
)
class RootDataNode(NodeBase):
    """
    RootData Node - Query crypto project/VC/people data via RootData APIs.

    Inputs:
      - operation: RootData API operation (see below).
      - language: Optional language code (default: en).
      - Additional parameters depend on operation (query/type/ids/paging/time range).

    Outputs:
      - data: JSON response from RootData (including result/status/message).

    Caching:
      - Query class operations cached in Redis for 1 hour.
      - Balance/credits endpoint is NOT cached.
    """

    CACHEABLE_OPS = {
        "search",
        "id_map",
        "get_item",
        "get_org",
        "get_people",
        "get_invest",
        "twitter_map",
        "get_fac",
        "ser_change",
        "hot_index",
        "hot_project_on_x",
        "leading_figures_on_crypto_x",
        "job_changes",
        "new_tokens",
        "ecosystem_map",
        "tag_map",
        "projects_by_ecosystems",
        "projects_by_tags",
    }
    TTL_SECONDS = 3600  # 1 hour

    def __init__(
        self,
        flow_id: str,
        component_id: int,
        cycle: int,
        node_id: str,
        name: str,
        operation: str = "search",
        language: str = "en",
        input_edges: List[Edge] = None,
        output_edges: List[Edge] = None,
        state_store=None,
        **kwargs,
    ):
        # Preserve raw params before super consumes metadata defaults
        raw_params = dict(kwargs)

        kwargs.setdefault("version", "0.0.1")
        kwargs.setdefault("display_name", "RootData Node")
        kwargs.setdefault("node_category", "data")
        kwargs.setdefault("description", "Fetch data from RootData APIs")
        kwargs.setdefault("tags", ["rootdata", "data", "api"])

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

        self.operation = operation.lower().strip()
        self.language = language or "en"
        self.api_key = CONFIG.get("ROOTDATA_API_KEY", "")
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
        # Best-effort compatibility; NodeBase currently auto-forwards inputs only.
        # We still declare for clarity.
        try:
            self.register_output_handle(
                name=DATA_HANDLE,
                data_type=dict,
                description="RootData response",
                example={"result": 200, "data": {}},
            )
        except Exception:
            # If base class lacks output handle helpers, continue silently.
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
            "op": self.operation,
            "payload": payload,
        }
        raw = json.dumps(base, sort_keys=True, default=str)
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
        return f"rootdata:cache:{self.operation}:{digest}"

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
        op = self.operation
        body: Dict[str, Any] = {}
        headers = {
            "apikey": self.api_key,
            "language": params.get("language") or self.language or "en",
            "Content-Type": "application/json",
        }
        endpoint = None

        # Map operations to endpoints and body shapes
        if op == "search":
            endpoint = "ser_inv"
            body = {
                "query": params.get("query", ""),
                "precise_x_search": params.get("precise_x_search", False),
            }
        elif op == "quotacredits":
            endpoint = "quotacredits"
        elif op == "id_map":
            endpoint = "id_map"
            body = {"type": params.get("type")}
        elif op == "get_item":
            endpoint = "get_item"
            body = {
                "project_id": params.get("project_id"),
                "contract_address": params.get("contract_address"),
                "include_team": params.get("include_team", False),
                "include_investors": params.get("include_investors", False),
            }
        elif op == "get_org":
            endpoint = "get_org"
            body = {
                "org_id": params.get("org_id"),
                "include_team": params.get("include_team", False),
                "include_investments": params.get("include_investments", False),
            }
        elif op == "get_people":
            endpoint = "get_people"
            body = {"people_id": params.get("people_id")}
        elif op == "get_invest":
            endpoint = "get_invest"
            body = {
                "page": params.get("page", 1),
                "page_size": params.get("page_size", 10),
            }
        elif op == "twitter_map":
            endpoint = "twitter_map"
            body = {"type": params.get("type")}
        elif op == "get_fac":
            endpoint = "get_fac"
            body = {
                "page": params.get("page", 1),
                "page_size": params.get("page_size", 10),
                "start_time": params.get("start_time"),
                "end_time": params.get("end_time"),
                "min_amount": params.get("min_amount"),
                "max_amount": params.get("max_amount"),
                "project_id": params.get("project_id"),
            }
        elif op == "ser_change":
            endpoint = "ser_change"
            body = {
                "begin_time": params.get("begin_time"),
                "end_time": params.get("end_time"),
            }
        elif op == "hot_index":
            endpoint = "hot_index"
            body = {"days": params.get("days")}
        elif op == "hot_project_on_x":
            endpoint = "hot_project_on_x"
            body = {
                "heat": params.get("heat", False),
                "influence": params.get("influence", True),
                "followers": params.get("followers", False),
            }
        elif op == "leading_figures_on_crypto_x":
            endpoint = "leading_figures_on_crypto_x"
            body = {
                "page": params.get("page", 1),
                "page_size": params.get("page_size", 10),
                "rank_type": params.get("rank_type", "heat"),
            }
        elif op == "job_changes":
            endpoint = "job_changes"
            body = {
                "recent_joinees": params.get("recent_joinees", True),
                "recent_resignations": params.get("recent_resignations", True),
            }
        elif op == "new_tokens":
            endpoint = "new_tokens"
        elif op == "ecosystem_map":
            endpoint = "ecosystem_map"
        elif op == "tag_map":
            endpoint = "tag_map"
        elif op == "projects_by_ecosystems":
            endpoint = "projects_by_ecosystems"
            body = {"ecosystem_ids": params.get("ecosystem_ids")}
        elif op == "projects_by_tags":
            endpoint = "projects_by_tags"
            body = {"tag_ids": params.get("tag_ids")}
        else:
            raise ValueError(f"Unsupported operation: {op}")

        if not endpoint:
            raise ValueError(f"Unsupported operation: {op}")

        # Clean None values
        body = {k: v for k, v in body.items() if v is not None and v != ""}

        return {
            "endpoint": endpoint,
            "headers": headers,
            "body": body,
        }

    async def _request_rootdata(self, endpoint: str, body: Dict[str, Any], headers: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{ROOTDATA_BASE_URL}/{endpoint}"
        timeout = httpx.Timeout(30.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, headers=headers, json=body)
            try:
                data = resp.json()
            except Exception:
                text = await resp.aread()
                return {
                    "result": resp.status_code,
                    "message": f"Invalid JSON response: {text[:500]}",
                    "data": {},
                }

            # Normalize structure
            result_code = data.get("result", resp.status_code)
            return {
                "result": result_code,
                "message": data.get("message"),
                "data": data.get("data"),
            }

    async def execute(self) -> bool:
        if not self.api_key:
            await self.persist_log("ROOTDATA_API_KEY not set", "ERROR")
            await self.set_status(NodeStatus.FAILED, "Missing API key")
            return False

        params = getattr(self, "params", {}) if hasattr(self, "params") else {}
        # Merge instance attributes for convenience
        params = {**params}
        params.setdefault("language", self.language)

        try:
            request_cfg = self._build_request(params)
        except Exception as e:
            await self.persist_log(f"Invalid parameters: {e}", "ERROR")
            await self.set_status(NodeStatus.FAILED, str(e))
            return False

        headers = request_cfg["headers"]
        headers["apikey"] = self.api_key
        body = request_cfg["body"]
        endpoint = request_cfg["endpoint"]

        # Cache
        cache_key = None
        cache_hit = None
        if self.operation in self.CACHEABLE_OPS:
            cache_key = self._make_cache_key(body)
            cache_hit = await self._get_cache(cache_key)
            if cache_hit:
                await self.persist_log(f"Cache hit for {self.operation}", "INFO")
                await self._emit_result(cache_hit, from_cache=True)
                await self.set_status(NodeStatus.COMPLETED)
                return True

        await self.persist_log(f"Calling RootData {self.operation} at {endpoint}", "INFO", log_metadata={"body": body})

        try:
            response = await self._request_rootdata(endpoint, body, headers)
        except Exception as e:
            await self.persist_log(f"Request failed: {e}", "ERROR")
            await self.set_status(NodeStatus.FAILED, str(e))
            return False

        # Cache on success (result==200)
        if cache_key and response.get("result") == 200:
            await self._set_cache(cache_key, response)

        await self._emit_result(response, from_cache=False)

        if response.get("result") == 200:
            await self.set_status(NodeStatus.COMPLETED)
            return True
        else:
            msg = response.get("message") or f"RootData result={response.get('result')}"
            await self.set_status(NodeStatus.FAILED, msg)
            return False

    async def _emit_result(self, payload: Dict[str, Any], from_cache: bool):
        # Attach metadata
        enriched = {
            **payload,
            "_meta": {
                "operation": self.operation,
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


