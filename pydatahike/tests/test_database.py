"""Tests for database lifecycle operations."""
import pytest
from datahike import (
    create_database,
    delete_database,
    database_exists,
    database,
    DatahikeException,
)


def test_create_and_delete(mem_config):
    """Test create and delete database."""
    # Create
    create_database(mem_config)

    # Check exists
    assert database_exists(mem_config) is True

    # Delete
    delete_database(mem_config)

    # Check deleted
    assert database_exists(mem_config) is False


def test_database_context_manager(mem_config):
    """Test database context manager for automatic cleanup."""
    # Database should not exist before
    assert database_exists(mem_config) is False

    # Use context manager
    with database(mem_config) as cfg:
        # Should exist inside context
        assert database_exists(cfg) is True
        assert cfg == mem_config

    # Should be deleted after context
    assert database_exists(mem_config) is False


def test_exception_on_nonexistent_database(mem_config):
    """Test that operations on nonexistent database raise exception."""
    from datahike import q

    # Database doesn't exist
    assert database_exists(mem_config) is False

    # Try to query should raise exception
    with pytest.raises(DatahikeException):
        q('[:find ?e :where [?e :name "test"]]', [('db', mem_config)])
