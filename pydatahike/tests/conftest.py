"""Pytest configuration and shared fixtures."""
import pytest
import uuid


@pytest.fixture
def mem_config():
    """Generate a unique in-memory database config."""
    db_id = str(uuid.uuid4())
    return f'{{:store {{:backend :memory :id #uuid "{db_id}"}}}}'


@pytest.fixture
def mem_config_flexible():
    """Generate a unique in-memory database config with schema flexibility."""
    db_id = str(uuid.uuid4())
    return f'{{:store {{:backend :memory :id #uuid "{db_id}"}} :schema-flexibility :read}}'
