"""OpenRouter Models Service

获取和缓存 OpenRouter 可用模型列表的服务。

根据 OpenRouter API 文档:
- GET /models - 列出所有模型
- GET /models/count - 获取模型数量
- GET /embeddings/models - 列出嵌入模型
- GET /providers - 列出所有 providers

参考: https://openrouter.ai/docs/api-reference/list-models
"""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

import httpx

from infra.config import CONFIG

logger = logging.getLogger(__name__)


class OpenRouterModelsService:
    """OpenRouter 模型列表服务"""
    
    # 缓存配置
    CACHE_TTL_SECONDS = 3600  # 缓存 1 小时
    
    # API 端点
    BASE_URL = "https://openrouter.ai/api/v1"
    
    # 单例实例
    _instance = None
    
    def __init__(self):
        self.api_key = CONFIG.get("OPENROUTER_API_KEY", "")
        self._models_cache: Dict[str, Any] = {}
        self._providers_cache: Dict[str, Any] = {}
        self._cache_timestamp: float = 0
        self._providers_cache_timestamp: float = 0
        
    @classmethod
    def get_instance(cls) -> "OpenRouterModelsService":
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def _is_cache_valid(self, timestamp: float) -> bool:
        """检查缓存是否有效"""
        return time.time() - timestamp < self.CACHE_TTL_SECONDS
    
    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers
    
    async def list_models(
        self,
        category: Optional[str] = None,
        force_refresh: bool = False
    ) -> List[Dict[str, Any]]:
        """
        获取所有可用模型列表
        
        Args:
            category: 可选的分类过滤（如 "programming"）
            force_refresh: 是否强制刷新缓存
            
        Returns:
            模型列表，每个模型包含:
            - id: 模型 ID（如 "openai/gpt-4"）
            - name: 显示名称
            - context_length: 上下文窗口大小
            - pricing: 价格信息
            - top_provider: 顶级 provider 信息
            - per_request_limits: 请求限制
        """
        cache_key = f"models_{category or 'all'}"
        
        # 检查缓存
        if not force_refresh and cache_key in self._models_cache:
            if self._is_cache_valid(self._cache_timestamp):
                logger.debug(f"Using cached models for category: {category}")
                return self._models_cache[cache_key]
        
        try:
            url = f"{self.BASE_URL}/models"
            params = {}
            if category:
                params["category"] = category
                
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    url,
                    headers=self._get_headers(),
                    params=params if params else None
                )
                response.raise_for_status()
                data = response.json()
                
                models = data.get("data", [])
                
                # 更新缓存
                self._models_cache[cache_key] = models
                self._cache_timestamp = time.time()
                
                logger.info(f"Fetched {len(models)} models from OpenRouter (category: {category})")
                return models
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching models: {e.response.status_code} - {e.response.text}")
            # 返回缓存数据（如果有）
            if cache_key in self._models_cache:
                logger.warning("Returning stale cache due to API error")
                return self._models_cache[cache_key]
            return []
        except Exception as e:
            logger.error(f"Error fetching models: {str(e)}")
            if cache_key in self._models_cache:
                return self._models_cache[cache_key]
            return []
    
    async def get_model_count(self) -> int:
        """获取模型总数"""
        try:
            url = f"{self.BASE_URL}/models/count"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=self._get_headers())
                response.raise_for_status()
                data = response.json()
                return data.get("count", 0)
                
        except Exception as e:
            logger.error(f"Error fetching model count: {str(e)}")
            return 0
    
    async def list_providers(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        获取所有 providers 列表
        
        Returns:
            Provider 列表
        """
        if not force_refresh and self._providers_cache:
            if self._is_cache_valid(self._providers_cache_timestamp):
                return list(self._providers_cache.values())
        
        try:
            url = f"{self.BASE_URL}/providers"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=self._get_headers())
                response.raise_for_status()
                data = response.json()
                
                providers = data.get("data", [])
                
                # 更新缓存
                self._providers_cache = {p.get("id", ""): p for p in providers}
                self._providers_cache_timestamp = time.time()
                
                logger.info(f"Fetched {len(providers)} providers from OpenRouter")
                return providers
                
        except Exception as e:
            logger.error(f"Error fetching providers: {str(e)}")
            return list(self._providers_cache.values()) if self._providers_cache else []
    
    async def list_embeddings_models(self) -> List[Dict[str, Any]]:
        """获取嵌入模型列表"""
        try:
            url = f"{self.BASE_URL}/embeddings/models"
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=self._get_headers())
                response.raise_for_status()
                data = response.json()
                return data.get("data", [])
                
        except Exception as e:
            logger.error(f"Error fetching embeddings models: {str(e)}")
            return []
    
    async def get_model_by_id(self, model_id: str) -> Optional[Dict[str, Any]]:
        """
        根据 ID 获取单个模型信息
        
        Args:
            model_id: 模型 ID（如 "openai/gpt-4"）
            
        Returns:
            模型信息或 None
        """
        models = await self.list_models()
        for model in models:
            if model.get("id") == model_id:
                return model
        return None
    
    async def get_models_with_min_context(
        self,
        min_context_length: int
    ) -> List[Dict[str, Any]]:
        """
        获取满足最小上下文长度的模型
        
        Args:
            min_context_length: 最小上下文长度
            
        Returns:
            满足条件的模型列表
        """
        models = await self.list_models()
        return [
            m for m in models
            if m.get("context_length", 0) >= min_context_length
        ]
    
    async def recommend_model_for_tokens(
        self,
        estimated_tokens: int,
        preferred_providers: List[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        根据估算的 token 数量推荐合适的模型
        
        middle-out 需要模型 context >= estimated_tokens / 2
        
        Args:
            estimated_tokens: 估算的 token 数量
            preferred_providers: 优先的 provider 列表
            
        Returns:
            推荐的模型或 None
        """
        # middle-out 需要至少一半的 context
        min_context = estimated_tokens // 2
        
        models = await self.get_models_with_min_context(min_context)
        
        if not models:
            logger.warning(
                f"No models found with context >= {min_context} "
                f"for {estimated_tokens} estimated tokens"
            )
            return None
        
        # 按 context_length 排序，选择最小满足条件的
        models.sort(key=lambda x: x.get("context_length", 0))
        
        # 如果有首选 provider，优先选择
        if preferred_providers:
            for model in models:
                model_id = model.get("id", "")
                for provider in preferred_providers:
                    if model_id.startswith(provider):
                        return model
        
        return models[0] if models else None
    
    def get_recommended_models_for_ai_node(self) -> List[Dict[str, str]]:
        """
        获取 AI Node 推荐使用的模型列表（用于前端下拉）
        
        这是一个同步方法，返回静态推荐列表。
        如果需要动态列表，使用 list_models()。
        
        Returns:
            推荐模型列表，格式与前端兼容
        """
        return [
            # DeepSeek 系列 - 高性价比
            {"value": "deepseek/deepseek-chat-v3-0324", "label": "DeepSeek Chat V3", "context": 128000},
            {"value": "deepseek/deepseek-r1", "label": "DeepSeek R1 (Reasoning)", "context": 64000},
            
            # OpenAI 系列
            {"value": "openai/gpt-4o", "label": "GPT-4o", "context": 128000},
            {"value": "openai/gpt-4-turbo", "label": "GPT-4 Turbo", "context": 128000},
            {"value": "openai/gpt-4", "label": "GPT-4", "context": 8192},
            {"value": "openai/gpt-3.5-turbo", "label": "GPT-3.5 Turbo", "context": 16385},
            
            # Anthropic Claude 系列
            {"value": "anthropic/claude-3.5-sonnet", "label": "Claude 3.5 Sonnet", "context": 200000},
            {"value": "anthropic/claude-3-opus", "label": "Claude 3 Opus", "context": 200000},
            {"value": "anthropic/claude-3-haiku", "label": "Claude 3 Haiku", "context": 200000},
            
            # Google Gemini 系列
            {"value": "google/gemini-pro-1.5", "label": "Gemini Pro 1.5", "context": 1000000},
            {"value": "google/gemini-flash-1.5", "label": "Gemini Flash 1.5", "context": 1000000},
            
            # Meta Llama 系列
            {"value": "meta-llama/llama-3.1-70b-instruct", "label": "Llama 3.1 70B", "context": 131072},
            {"value": "meta-llama/llama-3.1-8b-instruct", "label": "Llama 3.1 8B", "context": 131072},
            
            # Qwen 系列
            {"value": "qwen/qwen-2.5-72b-instruct", "label": "Qwen 2.5 72B", "context": 32768},
            
            # Mistral 系列
            {"value": "mistralai/mistral-large-latest", "label": "Mistral Large", "context": 128000},
        ]


# 便捷函数
async def get_available_models(category: Optional[str] = None) -> List[Dict[str, Any]]:
    """获取可用模型列表的便捷函数"""
    service = OpenRouterModelsService.get_instance()
    return await service.list_models(category=category)


async def get_model_for_context(estimated_tokens: int) -> Optional[Dict[str, Any]]:
    """根据 token 数量获取推荐模型的便捷函数"""
    service = OpenRouterModelsService.get_instance()
    return await service.recommend_model_for_tokens(estimated_tokens)


# 测试代码
if __name__ == "__main__":
    async def test():
        service = OpenRouterModelsService.get_instance()
        
        # 测试获取模型列表
        print("\n=== Testing list_models ===")
        models = await service.list_models()
        print(f"Total models: {len(models)}")
        if models:
            print(f"First model: {models[0].get('id')} - context: {models[0].get('context_length')}")
        
        # 测试获取模型数量
        print("\n=== Testing get_model_count ===")
        count = await service.get_model_count()
        print(f"Model count: {count}")
        
        # 测试获取 providers
        print("\n=== Testing list_providers ===")
        providers = await service.list_providers()
        print(f"Total providers: {len(providers)}")
        
        # 测试推荐模型
        print("\n=== Testing recommend_model_for_tokens ===")
        recommended = await service.recommend_model_for_tokens(50000)
        if recommended:
            print(f"Recommended model for 50k tokens: {recommended.get('id')} - context: {recommended.get('context_length')}")
        
        # 打印静态推荐列表
        print("\n=== Static recommended models ===")
        for m in service.get_recommended_models_for_ai_node()[:5]:
            print(f"  {m['value']}: {m['label']} (context: {m['context']})")
    
    asyncio.run(test())
