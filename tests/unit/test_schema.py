# tests/conftest.py

import inspect
from dataclasses import is_dataclass
import pytest

import morningpy.schema as schema_module
from morningpy.core.dataframe_schema import DataFrameSchema
from tests.mocks.schema_mocks import SCHEMA_MOCKS


def get_all_schema_classes():
    """Auto-discover all dataclass schemas."""
    classes = []
    for name, obj in inspect.getmembers(schema_module):
        if (
            inspect.isclass(obj)
            and is_dataclass(obj)
            and issubclass(obj, DataFrameSchema)
            and obj is not DataFrameSchema
        ):
            classes.append(obj)
    return sorted(classes, key=lambda c: c.__name__)


@pytest.fixture(scope="session")
def schema_classes():
    return get_all_schema_classes()


@pytest.fixture(scope="session")
def schema_mocks():
    """Returns the dict mapping SchemaClass → mock instance."""
    return SCHEMA_MOCKS