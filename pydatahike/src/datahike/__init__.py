"""Datahike Python bindings.

This package provides Python bindings to Datahike, a durable Datalog database
powered by an efficient Datalog query engine.

High-Level API (Recommended):
    >>> from datahike import Database
    >>>
    >>> db = Database(backend=':mem', id='example')
    >>> db.create()
    >>>
    >>> db.transact([{"name": "Alice", "age": 30}])
    >>> result = db.q('[:find ?name :where [?e :name ?name]]')
    >>> print(result)  # [['Alice']]
    >>>
    >>> db.delete()

Context Manager:
    >>> from datahike import database
    >>> with database(backend=':mem', id='test') as db:
    ...     db.transact([{'name': 'Alice'}])
    ...     result = db.q('[:find ?name :where [?e :name ?name]]')
    ...     print(result)

Time Travel:
    >>> import time
    >>> t = int(time.time() * 1000)
    >>> past = db.as_of(t)
    >>> results = past.q('[:find ?name :where [?e :name ?name]]')

EDN Helpers:
    >>> from datahike import edn, kw
    >>> schema = [{
    ...     "db/ident": edn.keyword("person/name"),
    ...     "db/valueType": kw.STRING,
    ...     "db/cardinality": kw.ONE
    ... }]
    >>> db.transact(schema)

Low-Level API (Advanced):
    >>> from datahike import create_database, transact, q, delete_database
    >>>
    >>> config = '{:store {:backend :mem :id "example"}}'
    >>> create_database(config)
    >>> transact(config, '[{"name": "Alice"}]')
    >>> result = q('[:find ?name :where [?e :name ?name]]', [('db', config)])
    >>> delete_database(config)
"""

from ._version import __version__
from ._native import DatahikeException

# High-level API (recommended)
from .database import Database, DatabaseSnapshot, database

# EDN helpers
from .edn import (
    # Types
    Keyword,
    Symbol,
    UUID,
    Inst,
    # Functions
    keyword,
    symbol,
    uuid,
    inst,
    string,
    # Constants
    Keywords,
    kw,
)

# Low-level API (for advanced use)
from .generated import (
    database_exists,
    create_database,
    delete_database,
    q,
    transact,
    pull,
    pull_many,
    entity,
    datoms,
    seek_datoms,
    index_range,
    schema,
    reverse_schema,
    metrics,
    gc_storage,
)

__all__ = [
    # Version
    '__version__',

    # Exception
    'DatahikeException',

    # High-level API (recommended)
    'Database',
    'DatabaseSnapshot',
    'database',

    # EDN helpers
    'Keyword',
    'Symbol',
    'UUID',
    'Inst',
    'keyword',
    'symbol',
    'uuid',
    'inst',
    'string',
    'Keywords',
    'kw',

    # Low-level API
    'database_exists',
    'create_database',
    'delete_database',
    'q',
    'transact',
    'pull',
    'pull_many',
    'entity',
    'datoms',
    'seek_datoms',
    'index_range',
    'schema',
    'reverse_schema',
    'metrics',
    'gc_storage',
]
