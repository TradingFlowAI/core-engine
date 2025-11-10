"""
Community Node API
Handle execution of community-defined nodes
"""

from fastapi import APIRouter, HTTPException, Body
from typing import Dict, Any, Optional
from pydantic import BaseModel

from core.community_node import CommunityNode
from core.community_node_executor import CommunityNodeExecutor
from core.node_registry import NodeRegistry

router = APIRouter(prefix="/node/community", tags=["community_nodes"])


class CommunityNodeExecuteRequest(BaseModel):
    """社区节点执行请求"""
    nodeConfig: Dict[str, Any]  # 完整的节点配置
    inputs: Dict[str, Any]  # 输入数据
    

class CommunityNodeValidateRequest(BaseModel):
    """社区节点验证请求"""
    executionType: str  # 'python' or 'http'
    executionConfig: Dict[str, Any]  # 执行配置


@router.post("/execute")
async def execute_community_node(request: CommunityNodeExecuteRequest):
    """
    执行社区节点
    
    Args:
        request: 包含节点配置和输入的请求体
        
    Returns:
        节点执行结果
    """
    try:
        # 创建社区节点实例
        node = CommunityNode(request.nodeConfig)
        
        # 执行节点
        result = node.execute(**request.inputs)
        
        # 获取日志
        logs = node.get_logs()
        
        return {
            "success": True,
            "outputs": result,
            "logs": logs,
            "metadata": node.get_metadata()
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__
            }
        )


@router.post("/validate")
async def validate_community_node(request: CommunityNodeValidateRequest):
    """
    验证社区节点配置
    
    Args:
        request: 包含执行类型和配置的请求体
        
    Returns:
        验证结果
    """
    try:
        if request.executionType == 'python':
            code = request.executionConfig.get('pythonCode', '')
            validation = CommunityNodeExecutor.validate_python_code(code)
        elif request.executionType == 'http':
            validation = CommunityNodeExecutor.validate_http_config(request.executionConfig)
        else:
            return {
                "valid": False,
                "issues": [f"Unsupported execution type: {request.executionType}"]
            }
        
        return validation
        
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail={
                "valid": False,
                "issues": [str(e)]
            }
        )


@router.get("/info/{nodeId}")
async def get_community_node_info(nodeId: str):
    """
    获取社区节点信息（从Control服务）
    
    这个端点主要用于验证节点是否存在
    实际的节点配置会由Control服务提供
    
    Args:
        nodeId: 节点ID
        
    Returns:
        节点基本信息
    """
    # 这里应该调用 Control 服务获取节点信息
    # 暂时返回占位符响应
    return {
        "nodeId": nodeId,
        "message": "Please fetch node config from Control service"
    }


@router.get("/registry/list")
async def list_registered_nodes(include_versions: bool = False):
    """
    列出所有已注册的节点（包括内置节点）
    
    Args:
        include_versions: 是否包含所有版本
        
    Returns:
        节点列表
    """
    try:
        nodes = NodeRegistry.list_all_nodes(include_versions=include_versions)
        
        return {
            "success": True,
            "nodes": nodes,
            "total": len(nodes)
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(e)
            }
        )


@router.get("/registry/{nodeType}")
async def get_node_info(nodeType: str, version: Optional[str] = None):
    """
    获取特定节点的信息
    
    Args:
        nodeType: 节点类型
        version: 版本号（可选，默认最新）
        
    Returns:
        节点信息
    """
    try:
        if not NodeRegistry.is_registered(nodeType, version):
            raise HTTPException(
                status_code=404,
                detail=f"Node type '{nodeType}' not found"
            )
        
        info = NodeRegistry.get_node_info(nodeType, version)
        
        return {
            "success": True,
            **info
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(e)
            }
        )


@router.get("/registry/{nodeType}/versions")
async def get_node_versions(nodeType: str):
    """
    获取节点的所有版本
    
    Args:
        nodeType: 节点类型
        
    Returns:
        版本列表
    """
    try:
        versions = NodeRegistry.get_all_versions(nodeType)
        latest = NodeRegistry.get_latest_version(nodeType)
        
        return {
            "success": True,
            "nodeType": nodeType,
            "versions": versions,
            "latest": latest
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(e)
            }
        )
