"""Tests for schema operations."""
import pytest
from datahike import (
    transact,
    schema,
    q,
    database,
)


def test_explicit_schema(mem_config):
    """Test transact and query with explicit schema."""
    with database(mem_config) as cfg:
        # Transact schema using EDN format
        schema_tx = '''[
            {:db/ident :name
             :db/valueType :db.type/string
             :db/cardinality :db.cardinality/one}
            {:db/ident :age
             :db/valueType :db.type/long
             :db/cardinality :db.cardinality/one}
        ]'''
        transact(cfg, schema_tx, input_format='edn')

        # Check schema was added
        db_schema = schema(cfg, output_format='json')
        assert db_schema is not None

        # Transact data using EDN
        tx_data = '[{:name "Charlie" :age 35} {:name "Diana" :age 28}]'
        transact(cfg, tx_data, input_format='edn')

        # Query
        query_result = q(
            '[:find ?e ?name ?age :where [?e :name ?name] [?e :age ?age]]',
            [('db', cfg)],
            output_format='json'
        )
        actual_results = query_result[1] if query_result[0] == '!set' else query_result
        assert len(actual_results) == 2


def test_schema_flexibility_read(mem_config_flexible):
    """Test schema-flexibility :read allows dynamic attributes."""
    with database(mem_config_flexible) as cfg:
        # Transact data without predefined schema
        tx_data = '[{:dynamic-attr "test" :another-attr 42}]'
        transact(cfg, tx_data, input_format='edn')

        # Query should work
        query_result = q(
            '[:find ?e ?val :where [?e :dynamic-attr ?val]]',
            [('db', cfg)]
        )
        assert len(query_result) > 0
