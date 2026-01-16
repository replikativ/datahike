"""Generated Datahike Python bindings.

This file is auto-generated from datahike.api.specification.
DO NOT EDIT MANUALLY - changes will be overwritten.

To regenerate: bb codegen-python

All functions use callback-based FFI to libdatahike with:
- Multiple output formats (json, edn, cbor)
- Proper exception handling
- Type annotations (PEP 484)

Temporal query variants via input_format parameter:
- 'db': Current database state
- 'history': Full history including retractions
- 'since:{timestamp_ms}': Database since timestamp
- 'asof:{timestamp_ms}': Database as-of timestamp
"""
from typing import Any, Dict, List, Tuple, Optional
from ._native import (
    _dll,
    _isolatethread,
    make_callback,
    prepare_query_inputs,
    DatahikeException,
)

__all__ = [
    'DatahikeException',
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

def delete_database(
    config: str,
    output_format: str = 'cbor'
) -> None:
    '''Deletes a database given via configuration map.

    Examples:
        Delete database:
            (delete-database {:store {:backend :mem :id "example"}})

    Args:
        config: Database configuration as EDN string
        output_format: Output format ('json', 'edn', or 'cbor')
    '''
    callback, get_result = make_callback(output_format)
    _dll.delete_database(
        _isolatethread,
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    get_result()  # Check for exceptions


def pull(
    config: str,
    selector: str,
    eid: int,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> Optional[Dict[str, Any]]:
    '''Fetches data using recursive declarative pull pattern.

    Examples:
        Pull with pattern:
            (pull db [:db/id :name :likes {:friends [:db/id :name]}] 1)
        Pull with arg-map:
            (pull db {:selector [:db/id :name] :eid 1})

    Args:
        config: Database configuration as EDN string
        selector: Pull pattern as EDN string (e.g., '[:db/id :name :age]' or '[*]')
        eid: Entity ID
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Optional[Dict[str, Any]]

    Example:
        >>> pull(config, '[*]', 1)
        {':db/id': 1, ':name': 'Alice', ':age': 30}
    '''
    callback, get_result = make_callback(output_format)
    _dll.pull(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        selector.encode('utf8'),
        eid,
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def entity(
    config: str,
    eid: int,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> Dict[str, Any]:
    '''Retrieves an entity by its id. Returns lazy map-like structure.

    Examples:
        Get entity by id:
            (entity db 1)
        Get entity by lookup ref:
            (entity db [:email "alice@example.com"])
        Navigate entity attributes:
            (:name (entity db 1))

    Args:
        config: Database configuration as EDN string
        eid: Entity ID
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Dict[str, Any]
    '''
    callback, get_result = make_callback(output_format)
    _dll.entity(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        eid,
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def metrics(
    config: str,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> Dict[str, Any]:
    '''Returns database metrics (datom counts, index sizes, etc).

    Examples:
        Get metrics:
            (metrics @conn)

    Args:
        config: Database configuration as EDN string
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Dict[str, Any]
    '''
    callback, get_result = make_callback(output_format)
    _dll.metrics(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def reverse_schema(
    config: str,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> Dict[str, Any]:
    '''Returns reverse schema definition (attribute id to ident mapping).

    Examples:
        Get reverse schema:
            (reverse-schema @conn)

    Args:
        config: Database configuration as EDN string
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Dict[str, Any]
    '''
    callback, get_result = make_callback(output_format)
    _dll.reverse_schema(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def datoms(
    config: str,
    index: str,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> List[List[Any]]:
    '''Index lookup. Returns sequence of datoms matching index components.

    Examples:
        Find all datoms for entity:
            (datoms db {:index :eavt :components [1]})
        Find datoms for entity and attribute:
            (datoms db {:index :eavt :components [1 :likes]})
        Find by attribute and value (requires :db/index):
            (datoms db {:index :avet :components [:likes "pizza"]})

    Args:
        config: Database configuration as EDN string
        index: Index keyword as EDN string (':eavt', ':aevt', ':avet', ':vaet')
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        List[List[Any]] (list of [e a v tx added?] vectors)
    '''
    callback, get_result = make_callback(output_format)
    _dll.datoms(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        index.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def q(
    query: str,
    inputs: List[Tuple[str, str]],
    output_format: str = 'cbor'
) -> Any:
    '''Executes a datalog query.

    Examples:
        Query with vector syntax:
            (q '[:find ?value :where [_ :likes ?value]] db)
        Query with map syntax:
            (q '{:find [?value] :where [[_ :likes ?value]]} db)
        Query with pagination:
            (q {:query '[:find ?value :where [_ :likes ?value]]
                           :args [db]
                           :offset 2
                           :limit 10})

    Args:
        query: Datalog query as EDN string
        inputs: List of (format, value) tuples. Formats:
            - 'db': Current database (value is config EDN)
            - 'history': Full history database
            - 'since:{timestamp_ms}': Database since timestamp
            - 'asof:{timestamp_ms}': Database as-of timestamp
            - 'json': JSON data
            - 'edn': EDN data
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Query result

    Example:
        >>> query('[:find ?e :where [?e :name "Alice"]]', [('db', config)])
    '''
    n, formats, values = prepare_query_inputs(inputs)
    callback, get_result = make_callback(output_format)
    _dll.q(
        _isolatethread,
        query.encode('utf8'),
        n,
        formats,
        values,
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def schema(
    config: str,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> Dict[str, Any]:
    '''Returns current schema definition.

    Examples:
        Get schema:
            (schema @conn)

    Args:
        config: Database configuration as EDN string
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Dict[str, Any]
    '''
    callback, get_result = make_callback(output_format)
    _dll.schema(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def index_range(
    config: str,
    attrid: str,
    start: Any,
    end: Any,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> List[List[Any]]:
    '''Returns part of :avet index between start and end values.

    Examples:
        Find datoms in value range:
            (index-range db {:attrid :likes :start "a" :end "z"})
        Find entities with age in range:
            (->> (index-range db {:attrid :age :start 18 :end 60}) (map :e))

    Args:
        config: Database configuration as EDN string
        attrid: Attribute keyword as EDN string (e.g., ':age')
        start: Start value (will be converted to EDN)
        end: End value (will be converted to EDN)
        input_format: Database input format
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        List[List[Any]]
    '''
    # Convert Python values to EDN representation
    def to_edn(v):
        if isinstance(v, str):
            return f'"{v}"'
        elif isinstance(v, bool):
            return 'true' if v else 'false'
        elif v is None:
            return 'nil'
        else:
            return str(v)

    callback, get_result = make_callback(output_format)
    _dll.index_range(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        attrid.encode('utf8'),
        to_edn(start).encode('utf8'),
        to_edn(end).encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def pull_many(
    config: str,
    selector: str,
    eids: List[int],
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> List[Dict[str, Any]]:
    '''Same as pull, but accepts sequence of ids and returns sequence of maps.

    Examples:
        Pull multiple entities:
            (pull-many db [:db/id :name] [1 2 3])

    Args:
        config: Database configuration as EDN string
        selector: Pull pattern as EDN string
        eids: List of entity IDs
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        List[Dict[str, Any]]
    '''
    eids_edn = '[' + ' '.join(str(e) for e in eids) + ']'
    callback, get_result = make_callback(output_format)
    _dll.pull_many(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        selector.encode('utf8'),
        eids_edn.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def gc_storage(
    config: str,
    before_timestamp_ms: Optional[int] = None,
    output_format: str = 'cbor'
) -> Any:
    '''Invokes garbage collection on connection's store. Removes old snapshots before given time point.

    Examples:
        GC all old snapshots:
            (gc-storage conn)
        GC snapshots before date:
            (gc-storage conn (java.util.Date.))

    Args:
        config: Database configuration as EDN string
        before_timestamp_ms: Unix timestamp in milliseconds (optional)
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Any
    '''
    import time
    if before_timestamp_ms is None:
        before_timestamp_ms = int(time.time() * 1000)

    callback, get_result = make_callback(output_format)
    _dll.gc_storage(
        _isolatethread,
        config.encode('utf8'),
        before_timestamp_ms,
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def create_database(
    config: str,
    output_format: str = 'cbor'
) -> None:
    '''Creates a database via configuration map.

    Examples:
        Create empty database:
            (create-database {:store {:backend :mem :id "example"}})
        Create with schema-flexibility :read:
            (create-database {:store {:backend :mem :id "example"} :schema-flexibility :read})
        Create without history:
            (create-database {:store {:backend :mem :id "example"} :keep-history? false})
        Create with initial schema:
            (create-database {:store {:backend :mem :id "example"}
                                          :initial-tx [{:db/ident :name
                                                        :db/valueType :db.type/string
                                                        :db/cardinality :db.cardinality/one}]})

    Args:
        config: Database configuration as EDN string
        output_format: Output format ('json', 'edn', or 'cbor')
    '''
    callback, get_result = make_callback(output_format)
    _dll.create_database(
        _isolatethread,
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    get_result()  # Check for exceptions


def database_exists(
    config: str,
    output_format: str = 'cbor'
) -> bool:
    '''Checks if a database exists via configuration map.

    Examples:
        Check if in-memory database exists:
            (database-exists? {:store {:backend :mem :id "example"}})
        Check with default config:
            (database-exists?)

    Args:
        config: Database configuration as EDN string
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        bool
    '''
    callback, get_result = make_callback(output_format)
    _dll.database_exists(
        _isolatethread,
        config.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def transact(
    config: str,
    tx_data: str,
    input_format: str = 'json',
    output_format: str = 'cbor'
) -> Dict[str, Any]:
    '''Applies transaction to the database and updates connection.

    Examples:
        Add single datom:
            (transact conn [[:db/add 1 :name "Ivan"]])
        Retract datom:
            (transact conn [[:db/retract 1 :name "Ivan"]])
        Create entity with tempid:
            (transact conn [[:db/add -1 :name "Ivan"]])
        Create entity (map form):
            (transact conn [{:db/id -1 :name "Ivan" :likes ["fries" "pizza"]}])
        Read from stdin (CLI):
            

    Args:
        config: Database configuration as EDN string
        tx_data: Transaction data (format depends on input_format)
        input_format: Input data format ('json', 'edn', or 'cbor')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        Transaction metadata

    Example:
        >>> transact(config, '[{"name": "Alice", "age": 30}]')
    '''
    callback, get_result = make_callback(output_format)
    _dll.transact(
        _isolatethread,
        config.encode('utf8'),
        input_format.encode('utf8'),
        tx_data.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


def seek_datoms(
    config: str,
    index: str,
    input_format: str = 'db',
    output_format: str = 'cbor'
) -> List[List[Any]]:
    '''Like datoms, but returns datoms starting from specified components through end of index.

    Examples:
        Seek from entity:
            (seek-datoms db {:index :eavt :components [1]})

    Args:
        config: Database configuration as EDN string
        index: Index keyword as EDN string (':eavt', ':aevt', ':avet', ':vaet')
        input_format: Database input format ('db', 'history', 'since:ts', 'asof:ts')
        output_format: Output format ('json', 'edn', or 'cbor')

    Returns:
        List[List[Any]] (list of [e a v tx added?] vectors)
    '''
    callback, get_result = make_callback(output_format)
    _dll.seek_datoms(
        _isolatethread,
        input_format.encode('utf8'),
        config.encode('utf8'),
        index.encode('utf8'),
        output_format.encode('utf8'),
        callback
    )
    return get_result()


# Re-export exception
DatahikeException = DatahikeException
