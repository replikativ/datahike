"""Tests for query and data retrieval operations."""
import json
import pytest
from datahike import (
    create_database,
    delete_database,
    transact,
    q,
    pull,
    database,
)


def test_transact_and_query_json(mem_config_flexible):
    """Test transact and query with JSON format."""
    with database(mem_config_flexible) as cfg:
        # Transact data using JSON format
        tx_data = json.dumps([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ])
        result = transact(cfg, tx_data, input_format='json')
        assert result is not None

        # Query for Alice
        query_result = q(
            '[:find ?e ?name :where [?e :name ?name] [?e :name "Alice"]]',
            [('db', cfg)],
            output_format='json'
        )
        assert len(query_result) > 0


def test_pull_entity(mem_config_flexible):
    """Test pull API to retrieve entity."""
    with database(mem_config_flexible) as cfg:
        # Transact data
        tx_data = json.dumps([{"name": "Alice", "age": 30}])
        transact(cfg, tx_data, input_format='json')

        # Query to get entity ID
        query_result = q(
            '[:find ?e ?name :where [?e :name ?name] [?e :name "Alice"]]',
            [('db', cfg)],
            output_format='json'
        )

        # Extract entity ID from result
        actual_results = query_result[1] if query_result[0] == '!set' else query_result
        entity_id = actual_results[0][0]

        # Pull entity
        entity = pull(cfg, '[*]', entity_id, output_format='json')
        assert entity.get('name') == 'Alice' or entity.get(':name') == 'Alice'


def test_query_with_multiple_results(mem_config_flexible):
    """Test query returning multiple results."""
    with database(mem_config_flexible) as cfg:
        # Transact multiple entities
        tx_data = json.dumps([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35}
        ])
        transact(cfg, tx_data, input_format='json')

        # Query all names
        query_result = q(
            '[:find ?name :where [?e :name ?name]]',
            [('db', cfg)],
            output_format='json'
        )

        # Should have 3 results
        actual_results = query_result[1] if query_result[0] == '!set' else query_result
        assert len(actual_results) == 3


def test_query_with_parameters(mem_config_flexible):
    """Test query with input parameters."""
    with database(mem_config_flexible) as cfg:
        # Transact data
        tx_data = json.dumps([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ])
        transact(cfg, tx_data, input_format='json')

        # Query with parameter
        query_result = q(
            '[:find ?e :in $ ?name :where [?e :name ?name]]',
            [('db', cfg), ('param', '"Alice"')],
            output_format='json'
        )

        # Should find Alice
        actual_results = query_result[1] if query_result[0] == '!set' else query_result
        assert len(actual_results) > 0
