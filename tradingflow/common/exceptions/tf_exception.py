class BaseException(Exception):
    """基础异常类"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class ResourceNotFoundException(BaseException):
    """资源未找到异常"""


class DuplicateResourceException(BaseException):
    """资源重复异常"""


class ValidationException(BaseException):
    """数据验证异常"""


class DatabaseException(BaseException):
    """数据库操作异常"""


class AuthorizationException(BaseException):
    """授权异常"""


# 节点执行相关异常
class NodeExecutionException(BaseException):
    """节点执行异常基类"""

    def __init__(self, message: str, node_id: str = None, status: str = None):
        super().__init__(message)
        self.node_id = node_id
        self.status = status


class NodeStopExecutionException(NodeExecutionException):
    """节点停止执行异常"""

    def __init__(
        self,
        message: str,
        node_id: str = None,
        reason: str = None,
        source_node: str = None,
    ):
        super().__init__(message, node_id, "terminated")
        self.reason = reason
        self.source_node = source_node


class NodeTimeoutException(NodeExecutionException):
    """节点执行超时异常"""

    def __init__(self, message: str, node_id: str = None, timeout_seconds: int = None):
        super().__init__(message, node_id, "timeout")
        self.timeout_seconds = timeout_seconds


class NodeValidationException(NodeExecutionException):
    """节点参数验证异常"""

    def __init__(self, message: str, node_id: str = None, invalid_params: dict = None):
        super().__init__(message, node_id, "validation_failed")
        self.invalid_params = invalid_params


class NodeResourceException(NodeExecutionException):
    """节点资源异常"""

    def __init__(self, message: str, node_id: str = None, resource_type: str = None):
        super().__init__(message, node_id, "resource_error")
        self.resource_type = resource_type
