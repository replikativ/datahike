"""High-level Datahike database interface.

Provides a Pythonic wrapper around the low-level FFI bindings with:
- Configuration as Python dicts/kwargs instead of EDN strings
- Auto-serialization of Python objects to JSON/EDN
- Simplified query interface with smart defaults
- Automatic result unwrapping
- Time-travel query support

Example:
    >>> from datahike import Database
    >>>
    >>> db = Database(backend='mem', id='test')
    >>> db.create()
    >>>
    >>> db.transact([{"name": "Alice", "age": 30}])
    >>> results = db.q('[:find ?name :where [?e :name ?name]]')
    >>> print(results)  # [['Alice']]
    >>>
    >>> db.delete()
"""

import json
from typing import Any, Dict, List, Optional, Union, Tuple, Iterator
from contextlib import contextmanager

from ._native import OutputFormat, InputFormat
from . import generated as _gen


# =============================================================================
# EDN Conversion
# =============================================================================

class EDNType:
    """Base class for explicit EDN types."""
    def __init__(self, value: Any):
        self.value = value

    def to_edn(self) -> str:
        """Convert to EDN string representation."""
        raise NotImplementedError


def _key_to_keyword(key: str) -> str:
    """Convert Python dict key to EDN keyword.

    Keys are always keywordized:
        "name"         → :name
        "person/name"  → :person/name
        ":db/id"       → :db/id (strip leading :)
        "keep-history?" → :keep-history?

    Args:
        key: Dictionary key string

    Returns:
        EDN keyword string
    """
    # Strip leading : if present (convenience)
    if key.startswith(':'):
        return key
    else:
        return f':{key}'


def _value_to_edn(v: Any) -> str:
    """Convert Python value to EDN representation.

    Rules:
    - EDNType instances: use their to_edn() method
    - str starting with '<STRING>': forced string (strip marker, quote)
    - str starting with '\\:': escaped colon → literal string
    - str starting with ':': keyword
    - str without ':': string (quoted)
    - int, float: number
    - bool: true/false
    - None: nil
    - list: vector (recursive)
    - dict: map (recursive)

    Args:
        v: Python value to convert

    Returns:
        EDN string representation
    """
    # Check for explicit EDN types first
    if isinstance(v, EDNType):
        return v.to_edn()

    # Check for forced string marker
    if isinstance(v, str) and v.startswith('<STRING>'):
        # Forced string - strip marker and quote
        return f'"{v[8:]}"'

    # Regular value conversion
    if isinstance(v, str):
        if v.startswith('\\:'):
            # Escaped colon - literal string with :
            # "\\:active" → ":active" (remove escape, quote as string)
            return f'"{v[1:]}"'
        elif v.startswith(':'):
            # Unescaped colon - keyword
            return v
        else:
            # Regular string - quote it
            # Escape any quotes inside the string
            escaped = v.replace('\\', '\\\\').replace('"', '\\"')
            return f'"{escaped}"'

    elif isinstance(v, bool):
        # Must check bool before int (bool is subclass of int in Python)
        return 'true' if v else 'false'

    elif isinstance(v, (int, float)):
        return str(v)

    elif v is None:
        return 'nil'

    elif isinstance(v, list):
        items = [_value_to_edn(item) for item in v]
        return '[' + ' '.join(items) + ']'

    elif isinstance(v, dict):
        return _dict_to_edn(v)

    else:
        # Fallback: stringify
        return str(v)


def _dict_to_edn(d: Dict[str, Any]) -> str:
    """Convert Python dict to EDN map.

    Keys are keywordized, values are converted recursively.

    Args:
        d: Python dictionary

    Returns:
        EDN map string
    """
    items = []
    for k, v in d.items():
        edn_key = _key_to_keyword(k)
        edn_val = _value_to_edn(v)
        items.append(f'{edn_key} {edn_val}')
    return '{' + ' '.join(items) + '}'


def _python_to_edn(data: Union[List, Dict, Any]) -> str:
    """Convert Python data structure to EDN string.

    Main entry point for EDN conversion.

    Args:
        data: Python list, dict, or primitive value

    Returns:
        EDN string representation
    """
    if isinstance(data, list):
        items = [_python_to_edn(item) for item in data]
        return '[' + ' '.join(items) + ']'
    elif isinstance(data, dict):
        return _dict_to_edn(data)
    else:
        return _value_to_edn(data)


# =============================================================================
# Database Class
# =============================================================================

class Database:
    """High-level Datahike database interface.

    Wraps low-level API with Pythonic conveniences:
    - Config as dict/kwargs instead of EDN strings
    - Auto-serialize Python objects to JSON
    - Simplified query interface
    - Auto-unwrap result sets

    Args:
        config: Database configuration as dict or EDN string
        **kwargs: Config as keyword arguments (for simple store configs)

    Examples:
        # Nested dict (recommended for complex configs)
        db = Database({"store": {"backend": ":memory", "id": "test"}})

        # Flat kwargs (convenience for simple store config)
        db = Database(backend=':memory', id='test')

        # EDN string (escape hatch)
        db = Database('{:store {:backend :memory :id "test"}}')
    """

    def __init__(
        self,
        config: Union[str, Dict[str, Any], None] = None,
        **kwargs
    ):
        if isinstance(config, str):
            # EDN string passthrough
            self._config = config

        elif isinstance(config, dict):
            # Dict config - convert to EDN
            self._config = _dict_to_edn(config)

        elif kwargs:
            # Flat kwargs - wrap in :store
            store_config = kwargs
            full_config = {"store": store_config}
            self._config = _dict_to_edn(full_config)

        else:
            raise ValueError(
                "Must provide config as dict, EDN string, or keyword arguments"
            )

    @property
    def config(self) -> str:
        """Get EDN config string (for low-level API compatibility)."""
        return self._config

    @staticmethod
    def memory(id: str) -> 'Database':
        """Create in-memory database configuration.

        Convenience factory for the most common use case.

        Args:
            id: Database identifier. **Must be a UUID string** (use str(uuid.uuid4())).
                This is required by the konserve store and is essential for distributed
                database tracking.

        Returns:
            Database instance with memory backend

        Example:
            >>> import uuid
            >>> db = Database.memory(str(uuid.uuid4()))
            >>> db.create()
        """
        return Database({"store": {"backend": ":memory", "id": id}})

    @staticmethod
    def file(path: str) -> 'Database':
        """Create file-based database configuration.

        Args:
            path: File system path for database

        Returns:
            Database instance with file backend

        Example:
            >>> db = Database.file('/tmp/mydb')
            >>> db.create()
        """
        return Database({"store": {"backend": ":file", "path": path}})

    def create(self) -> None:
        """Create this database.

        Raises:
            DatahikeException: If database creation fails
        """
        _gen.create_database(self._config)

    def delete(self) -> None:
        """Delete this database.

        Raises:
            DatahikeException: If database deletion fails
        """
        _gen.delete_database(self._config)

    def exists(self) -> bool:
        """Check if database exists.

        Returns:
            True if database exists, False otherwise
        """
        return _gen.database_exists(self._config)

    def transact(
        self,
        data: Union[str, List[Dict], Dict],
        input_format: str = 'json'
    ) -> Any:
        """Transact data into database.

        Automatically serializes Python objects to JSON or EDN.

        Args:
            data: Transaction data as Python list/dict or EDN/JSON string
            input_format: 'json' (default) or 'edn'

        Returns:
            Transaction result

        Raises:
            DatahikeException: If transaction fails
            ValueError: If format is invalid

        Examples:
            # Python objects (auto-serialized to JSON)
            db.transact([
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ])

            # Single entity
            db.transact({"name": "Charlie", "age": 35})

            # With entity ID (update)
            db.transact([{":db/id": 1, "age": 31}])

            # EDN string (passthrough)
            db.transact('[{:name "Alice"}]', input_format='edn')
        """
        if isinstance(data, str):
            # Already serialized
            tx_data = data

        elif isinstance(data, (list, dict)):
            # Python objects - serialize based on format
            if input_format == 'json':
                # Serialize to JSON
                tx_data = json.dumps(data)
            elif input_format == 'edn':
                # Convert Python → EDN
                tx_data = _python_to_edn(data)
            else:
                raise ValueError(
                    f"Unknown format: {input_format}. "
                    f"Expected 'json' or 'edn'"
                )

        else:
            raise TypeError(f"Cannot transact type: {type(data)}")

        # Call low-level API
        return _gen.transact(self._config, tx_data, input_format=input_format)

    def q(
        self,
        query: str,
        *args,
        inputs: Optional[List] = None,
        output_format: OutputFormat = 'cbor',
        unwrap: bool = True
    ) -> Any:
        """Execute Datalog query with smart defaults.

        Args:
            query: Datalog query string
            *args: Additional inputs (other DBs, params)
            inputs: Override input list completely
            output_format: 'json', 'edn', or 'cbor' (default)
            unwrap: Auto-unwrap '!set' wrapper (default True)

        Returns:
            Query results (list of tuples by default)

        Raises:
            DatahikeException: If query fails

        Examples:
            # Simple - implicit 'db' input
            results = db.q('[:find ?name :where [?e :name ?name]]')

            # With parameter
            results = db.q(
                '[:find ?e :in $ ?name :where [?e :name ?name]]',
                ('param', '"Alice"')
            )

            # Multiple databases
            results = db.q(
                '[:find ?name :in $ $2 :where ...]',
                other_db  # Another Database instance
            )

            # Get raw result (no unwrap)
            raw = db.q(query, unwrap=False)
        """
        # Build inputs list
        if inputs is None:
            # Start with this database as primary input
            query_inputs = [('db', self._config)]

            # Process additional args
            for arg in args:
                if isinstance(arg, Database):
                    # Another Database instance
                    query_inputs.append(('db', arg.config))
                elif isinstance(arg, DatabaseSnapshot):
                    # Snapshot with specific input format
                    query_inputs.append((arg._input_format, arg._config))
                elif isinstance(arg, tuple):
                    # Direct input tuple like ('param', 'value')
                    query_inputs.append(arg)
                else:
                    # Assume it's a parameter value
                    query_inputs.append(('param', str(arg)))
        else:
            # User provided complete inputs list
            query_inputs = inputs

        # Execute low-level query
        result = _gen.q(query, query_inputs, output_format=output_format)

        # Auto-unwrap if requested
        if unwrap and isinstance(result, (list, tuple)):
            # Check for CBOR '!set' wrapper
            if len(result) == 2 and result[0] == '!set':
                return result[1]

        return result

    def pull(
        self,
        pattern: Union[str, List],
        eid: int,
        input_format: InputFormat = 'db',
        output_format: OutputFormat = 'cbor'
    ) -> Optional[Dict[str, Any]]:
        """Pull entity by pattern.

        Args:
            pattern: Pull pattern as string or list
            eid: Entity ID
            input_format: 'db', 'history', 'since:<ts>', 'asof:<ts>'
            output_format: Output format

        Returns:
            Entity dict or None if not found

        Examples:
            # Pull with pattern
            entity = db.pull('[:name :age]', 1)

            # Pull all attributes
            entity = db.pull('[*]', 1)

            # Pull with relationships
            entity = db.pull('[:name {:friends [:name]}]', 1)
        """
        if isinstance(pattern, list):
            # Convert Python list to EDN-like string
            # Simple conversion for common cases
            pattern = str(pattern).replace("'", ":")

        return _gen.pull(
            self._config,
            pattern,
            eid,
            input_format=input_format,
            output_format=output_format
        )

    def pull_many(
        self,
        pattern: Union[str, List],
        eids: List[int],
        input_format: InputFormat = 'db',
        output_format: OutputFormat = 'cbor'
    ) -> List[Dict[str, Any]]:
        """Pull multiple entities by pattern.

        Args:
            pattern: Pull pattern
            eids: List of entity IDs
            input_format: Input format
            output_format: Output format

        Returns:
            List of entity dicts
        """
        if isinstance(pattern, list):
            pattern = str(pattern).replace("'", ":")

        # Convert Python list to EDN
        eids_edn = str(eids)

        return _gen.pull_many(
            self._config,
            pattern,
            eids_edn,
            input_format=input_format,
            output_format=output_format
        )

    def entity(
        self,
        eid: int,
        input_format: InputFormat = 'db',
        output_format: OutputFormat = 'cbor'
    ) -> Optional[Dict[str, Any]]:
        """Get entity by ID.

        Args:
            eid: Entity ID
            input_format: Input format
            output_format: Output format

        Returns:
            Entity dict or None
        """
        return _gen.entity(
            self._config,
            eid,
            input_format=input_format,
            output_format=output_format
        )

    def schema(
        self,
        output_format: OutputFormat = 'cbor'
    ) -> Any:
        """Get database schema.

        Args:
            output_format: Output format

        Returns:
            Schema data
        """
        return _gen.schema(self._config, output_format=output_format)

    def as_of(self, timestamp_ms: int) -> 'DatabaseSnapshot':
        """Get database snapshot at specific timestamp.

        Args:
            timestamp_ms: Unix timestamp in milliseconds

        Returns:
            DatabaseSnapshot that queries at that point in time

        Example:
            >>> past = db.as_of(1234567890)
            >>> results = past.q('[:find ?name :where [?e :name ?name]]')
        """
        return DatabaseSnapshot(self._config, f'asof:{timestamp_ms}')

    def since(self, timestamp_ms: int) -> 'DatabaseSnapshot':
        """Get changes since timestamp.

        Args:
            timestamp_ms: Unix timestamp in milliseconds

        Returns:
            DatabaseSnapshot showing changes since that time
        """
        return DatabaseSnapshot(self._config, f'since:{timestamp_ms}')

    @property
    def history(self) -> 'DatabaseSnapshot':
        """Get full history view (all transactions).

        Returns:
            DatabaseSnapshot with complete history

        Example:
            >>> all_changes = db.history.q('[:find ?name :where [?e :name ?name]]')
        """
        return DatabaseSnapshot(self._config, 'history')

    def __enter__(self):
        """Context manager entry - create database."""
        self.create()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - delete database."""
        try:
            self.delete()
        except Exception:
            pass  # Suppress cleanup errors
        return False

    def __repr__(self):
        return f'Database({self._config})'


# =============================================================================
# Database Snapshot (Time Travel)
# =============================================================================

class DatabaseSnapshot:
    """Read-only snapshot of database at a point in time.

    This is a lightweight object that overrides the input format
    when executing queries, enabling time-travel queries.

    Args:
        config: EDN config string (from parent Database)
        input_format: 'db', 'history', 'asof:<ts>', 'since:<ts>'
    """

    def __init__(self, config: str, input_format: str):
        self._config = config
        self._input_format = input_format

    def q(
        self,
        query: str,
        *args,
        output_format: OutputFormat = 'cbor',
        unwrap: bool = True
    ) -> Any:
        """Query this snapshot.

        Works like Database.q() but uses snapshot's input format.

        Args:
            query: Datalog query string
            *args: Additional inputs
            output_format: Output format
            unwrap: Auto-unwrap results

        Returns:
            Query results
        """
        # Build inputs with our format
        query_inputs = [(self._input_format, self._config)]

        # Add any additional inputs from args
        for arg in args:
            if isinstance(arg, Database):
                query_inputs.append(('db', arg.config))
            elif isinstance(arg, DatabaseSnapshot):
                query_inputs.append((arg._input_format, arg._config))
            elif isinstance(arg, tuple):
                query_inputs.append(arg)
            else:
                query_inputs.append(('param', str(arg)))

        # Execute query
        result = _gen.q(query, query_inputs, output_format=output_format)

        # Auto-unwrap
        if unwrap and isinstance(result, (list, tuple)):
            if len(result) == 2 and result[0] == '!set':
                return result[1]

        return result

    def pull(
        self,
        pattern: Union[str, List],
        eid: int,
        output_format: OutputFormat = 'cbor'
    ) -> Optional[Dict[str, Any]]:
        """Pull from this snapshot.

        Args:
            pattern: Pull pattern
            eid: Entity ID
            output_format: Output format

        Returns:
            Entity dict or None
        """
        if isinstance(pattern, list):
            pattern = str(pattern).replace("'", ":")

        return _gen.pull(
            self._config,
            pattern,
            eid,
            input_format=self._input_format,
            output_format=output_format
        )

    def __repr__(self):
        return f'DatabaseSnapshot({self._input_format}, {self._config})'


# =============================================================================
# Convenience Context Manager
# =============================================================================

@contextmanager
def database(config: Union[str, Dict[str, Any], None] = None, **kwargs) -> Iterator[Database]:
    """Context manager for database lifecycle.

    Automatically creates database on entry and deletes on exit.
    Useful for tests and temporary databases.

    Args:
        config: Database configuration
        **kwargs: Config as keyword arguments

    Yields:
        Database instance

    Example:
        >>> with database(backend=':memory', id='test') as db:
        ...     db.transact([{'name': 'Alice'}])
        ...     result = db.q('[:find ?name :where [?e :name ?name]]')
        ...     print(result)
    """
    db = Database(config, **kwargs) if config or kwargs else None
    if db is None:
        raise ValueError("Must provide config")

    db.create()
    try:
        yield db
    finally:
        try:
            db.delete()
        except Exception:
            # Suppress cleanup errors - database may already be deleted
            pass
