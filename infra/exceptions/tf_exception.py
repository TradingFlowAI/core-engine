"""TradingFlow Exception Classes"""


class BaseException(Exception):
    """Base exception class"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class ResourceNotFoundException(BaseException):
    """Resource not found exception"""


class DuplicateResourceException(BaseException):
    """Duplicate resource exception"""


class ValidationException(BaseException):
    """Data validation exception"""


class DatabaseException(BaseException):
    """Database operation exception"""


class AuthorizationException(BaseException):
    """Authorization exception"""


# Node execution related exceptions
class NodeExecutionException(BaseException):
    """Node execution exception base class"""

    def __init__(self, message: str, node_id: str = None, status: str = None):
        super().__init__(message)
        self.node_id = node_id
        self.status = status


class NodeStopExecutionException(NodeExecutionException):
    """Node stop execution exception"""

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
    """Node execution timeout exception"""

    def __init__(self, message: str, node_id: str = None, timeout_seconds: int = None):
        super().__init__(message, node_id, "timeout")
        self.timeout_seconds = timeout_seconds


class NodeValidationException(NodeExecutionException):
    """Node parameter validation exception"""

    def __init__(self, message: str, node_id: str = None, invalid_params: dict = None):
        super().__init__(message, node_id, "validation_failed")
        self.invalid_params = invalid_params


class NodeResourceException(NodeExecutionException):
    """Node resource exception"""

    def __init__(self, message: str, node_id: str = None, resource_type: str = None):
        super().__init__(message, node_id, "resource_error")
        self.resource_type = resource_type


class InsufficientCreditsException(NodeExecutionException):
    """Insufficient credits exception - used to stop flow execution when credits balance is insufficient"""

    def __init__(
        self,
        message: str,
        node_id: str = None,
        user_id: str = None,
        required_credits: int = None,
        current_balance: int = None,
    ):
        super().__init__(message, node_id, "insufficient_credits")
        self.user_id = user_id
        self.required_credits = required_credits
        self.current_balance = current_balance
