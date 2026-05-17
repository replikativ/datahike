"""Tests for schema operations."""
import pytest
from datahike import database


def _unwrap_set(result):
    if isinstance(result, list) and len(result) == 2 and result[0] == '!set':
        return result[1]
    return result


def test_explicit_schema(mem_config):
    """Transact an explicit schema, then JSON data; the schema-aware coercion
    in libdatahike's transact entry point should convert JSON Integer values
    to Java Long for :db.type/long attributes. This is the canary for the
    codegen template's JSON-aware transact body (see
    src/datahike/codegen/native.clj generate-transact)."""
    with database(mem_config) as db:
        schema_tx = '''[
            {:db/ident :name
             :db/valueType :db.type/string
             :db/cardinality :db.cardinality/one}
            {:db/ident :age
             :db/valueType :db.type/long
             :db/cardinality :db.cardinality/one}
        ]'''
        db.transact(schema_tx, input_format='edn')

        # Schema should be present
        db_schema = db.schema(output_format='json')
        assert db_schema is not None

        # JSON data — values get coerced via the schema (Integer → Long for :age)
        db.transact([
            {"name": "Charlie", "age": 35},
            {"name": "Diana", "age": 28}
        ])

        results = _unwrap_set(db.q(
            '[:find ?e ?name ?age :where [?e :name ?name] [?e :age ?age]]',
            output_format='json'
        ))
        assert len(results) == 2


def test_schema_flexibility_read(mem_config_flexible):
    """Schema-flexibility :read accepts dynamic attributes without a declared
    schema."""
    with database(mem_config_flexible) as db:
        db.transact('[{:dynamic-attr "test" :another-attr 42}]',
                    input_format='edn')

        results = db.q('[:find ?e ?val :where [?e :dynamic-attr ?val]]')
        assert len(results) > 0
