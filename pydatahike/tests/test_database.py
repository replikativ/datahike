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
    """Test create and delete database via the low-level functional API."""
    create_database(mem_config)
    assert database_exists(mem_config) is True

    delete_database(mem_config)
    assert database_exists(mem_config) is False


def test_database_context_manager(mem_config):
    """Test database() context manager creates on enter, deletes on exit.

    The context manager yields a Database instance, not the config string.
    Use Database methods (db.exists(), db.transact(), ...) inside the block.
    The underlying config is available via db.config if you need to pass it
    to the low-level functional API."""
    # Database should not exist before
    assert database_exists(mem_config) is False

    with database(mem_config) as db:
        assert db.exists() is True
        # The yielded Database carries the config we passed in.
        assert db.config == mem_config

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
