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
    """Community node execution request"""
    nodeConfig: Dict[str, Any]  # Complete node configuration
    inputs: Dict[str, Any]  # Input data
    

class CommunityNodeValidateRequest(BaseModel):
    """Community node validation request"""
    executionType: str  # 'python' or 'http'
    executionConfig: Dict[str, Any]  # Execution configuration


@router.post("/execute")
async def execute_community_node(request: CommunityNodeExecuteRequest):
    """
    Execute community node
    
    Args:
        request: Request body containing node configuration and inputs
        
    Returns:
        Node execution result
    """
    try:
        # Create community node instance
        node = CommunityNode(request.nodeConfig)
        
        # Execute node
        result = node.execute(**request.inputs)
        
        # Get logs
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
    Validate community node configuration
    
    Args:
        request: Request body containing execution type and configuration
        
    Returns:
        Validation result
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
    Get community node information (from Control service)
    
    This endpoint is mainly used to verify if node exists.
    The actual node configuration is provided by Control service.
    
    Args:
        nodeId: Node ID
        
    Returns:
        Node basic information
    """
    # Should call Control service to get node information
    # Returning placeholder response for now
    return {
        "nodeId": nodeId,
        "message": "Please fetch node config from Control service"
    }


@router.get("/registry/list")
async def list_registered_nodes(include_versions: bool = False):
    """
    List all registered nodes (including built-in nodes)
    
    Args:
        include_versions: Whether to include all versions
        
    Returns:
        Node list
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
    Get specific node information
    
    Args:
        nodeType: Node type
        version: Version number (optional, defaults to latest)
        
    Returns:
        Node information
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
    Get all versions of a node
    
    Args:
        nodeType: Node type
        
    Returns:
        Version list
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
