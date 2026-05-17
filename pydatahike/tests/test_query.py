"""Tests for query and data retrieval operations.

These exercise the high-level Database API (db.transact / db.q / db.pull)
inside the database() context manager. The low-level functional API
(transact / q / pull on bare config strings) is covered by test_basic.py."""
import pytest
from datahike import database


def _unwrap_set(result):
    """Strip the !set wrapper from JSON-formatted query results."""
    if isinstance(result, list) and len(result) == 2 and result[0] == '!set':
        return result[1]
    return result


def test_transact_and_query_json(mem_config_flexible):
    """Transact via JSON, query via Datalog."""
    with database(mem_config_flexible) as db:
        db.transact([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ])

        results = db.q(
            '[:find ?e ?name :where [?e :name ?name] [?e :name "Alice"]]',
            output_format='json'
        )
        assert len(_unwrap_set(results)) > 0


def test_pull_entity(mem_config_flexible):
    """Pull an entity by id after transacting."""
    with database(mem_config_flexible) as db:
        db.transact([{"name": "Alice", "age": 30}])

        results = _unwrap_set(db.q(
            '[:find ?e ?name :where [?e :name ?name] [?e :name "Alice"]]',
            output_format='json'
        ))
        entity_id = results[0][0]

        entity = db.pull('[*]', entity_id, output_format='json')
        assert entity.get('name') == 'Alice' or entity.get(':name') == 'Alice'


def test_query_with_multiple_results(mem_config_flexible):
    """Query returns one row per matching entity."""
    with database(mem_config_flexible) as db:
        db.transact([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35}
        ])

        results = _unwrap_set(db.q(
            '[:find ?name :where [?e :name ?name]]',
            output_format='json'
        ))
        assert len(results) == 3


def test_query_with_parameters(mem_config_flexible):
    """Query with a bound parameter passed as an additional input source.

    libdatahike's loadInput supports input formats: db / history / since /
    asof / json / edn / cbor. Parameters are passed as ('edn', '<edn-form>')
    pairs — the EDN parser produces a plain Clojure value bound to the
    query's :in variable."""
    with database(mem_config_flexible) as db:
        db.transact([
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25}
        ])

        results = _unwrap_set(db.q(
            '[:find ?e :in $ ?name :where [?e :name ?name]]',
            ('edn', '"Alice"'),
            output_format='json'
        ))
        assert len(results) > 0
