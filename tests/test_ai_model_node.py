import os
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

# Ensure project root is on PYTHONPATH when running tests directly
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.join(CURRENT_DIR, "..")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from nodes.ai_model_node_v0_0_1 import AIModelNode  # noqa: E402


def _create_ai_node_stub():
    """Create a lightweight AIModelNode instance without running NodeBase.__init__."""
    node = object.__new__(AIModelNode)
    node.logger = MagicMock()
    node.flow_id = "flow_test"
    node.cycle = 0
    node.node_id = "ai_model_node_test"
    node._input_signals = {}
    return node


def test_normalize_parameters_supports_param_matrix_list():
    node = _create_ai_node_stub()
    param_matrix = [
        {"id": "param_1", "name": "price_data", "value": {"symbol": "BTC"}},
        {"id": "param_2", "name": "temperature", "value": "0.9"},
        {"id": "param_3", "value": "fallback", "extra": 123},
    ]

    result = AIModelNode._normalize_parameters(node, param_matrix)

    assert result["price_data"] == {"symbol": "BTC"}
    assert result["temperature"] == "0.9"
    # When name missing, should fall back to id, preserving direct value
    assert result["param_3"] == "fallback"


def test_normalize_parameters_parses_json_string():
    node = _create_ai_node_stub()
    json_params = '{"temperature": 1.5, "max_tokens": 256}'

    result = AIModelNode._normalize_parameters(node, json_params)

    assert result == {"temperature": 1.5, "max_tokens": 256}


def test_coerce_numeric_clamps_and_casts():
    node = _create_ai_node_stub()

    clamped_int = AIModelNode._coerce_numeric(node, "10", default=1, min_value=0, max_value=5, integer=True)
    clamped_float = AIModelNode._coerce_numeric(node, None, default=0.5, min_value=0.0, max_value=1.0, integer=False)
    fallback_default = AIModelNode._coerce_numeric(node, "abc", default=2, min_value=0, max_value=3, integer=True)

    assert clamped_int == 5  # capped at max_value
    assert clamped_float == 0.5  # default already within bounds
    assert fallback_default == 2  # invalid input falls back to default


@pytest.mark.asyncio
async def test_prepare_context_awaits_format_requirements_and_renders_examples():
    node = _create_ai_node_stub()
    node.prompt = "Signal payload: {price_data}"
    node.parameters = {"price_data": {"current_price": 123}}
    node.auto_format_output = True
    node._output_edges = [{"source_handle": "ai_response", "target_handle": "action"}]
    node._input_signals = {}
    node.persist_log = AsyncMock()

    node._analyze_output_format_requirements = AsyncMock(
        return_value={
            "required_fields": ["action"],
            "field_descriptions": {"action": "Operation type"},
            "field_examples": {"action": "sell"},
        }
    )

    context = await AIModelNode._prepare_context(node)

    node._analyze_output_format_requirements.assert_awaited_once()
    assert '"current_price": 123' in context
    assert '"action": "sell"' in context
