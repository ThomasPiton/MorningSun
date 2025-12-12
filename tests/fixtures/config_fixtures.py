"""Shared fixtures for configuration testing."""
import pytest
from morningpy.core.auth import AuthType


@pytest.fixture
def valid_auth_types():
    """List of valid authentication types."""
    return [AuthType.BEARER_TOKEN, AuthType.WAF_TOKEN, AuthType.API_KEY, AuthType.NONE]


@pytest.fixture
def valid_url_patterns():
    """Valid URL pattern regex."""
    return r'^https?://[^\s/$.?#].[^\s]*$'


@pytest.fixture
def common_column_types():
    """Common column name patterns that should be snake_case."""
    return {
        'snake_case': r'^[a-z][a-z0-9_]*$',
        'no_uppercase': r'^[^A-Z]*$',
    }