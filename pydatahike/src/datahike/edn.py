"""EDN type helpers and common keyword constants.

This module provides explicit type constructors for EDN values and
pre-defined constants for frequently-used Datahike keywords.

Use these when you need fine-grained control over EDN conversion or
want to avoid string prefixes.

Example:
    >>> from datahike import edn, kw
    >>>
    >>> # Explicit types
    >>> schema = [{
    ...     "db/ident": edn.keyword("person/name"),
    ...     "db/valueType": kw.STRING,
    ...     "db/cardinality": kw.ONE,
    ...     "db/doc": edn.string(":literal-colon-in-doc")
    ... }]
    >>>
    >>> db.transact(schema)
"""

from typing import Optional
from .database import EDNType


# =============================================================================
# Explicit EDN Types
# =============================================================================

class Keyword(EDNType):
    """Explicit EDN keyword.

    Use when you need to construct keywords programmatically or force
    a value to be a keyword regardless of the string prefix rules.

    Args:
        name: Keyword name
        namespace: Optional namespace

    Examples:
        >>> edn.keyword("name")
        # → :name

        >>> edn.keyword("name", "person")
        # → :person/name
    """

    def __init__(self, name: str, namespace: Optional[str] = None):
        if namespace:
            self.value = f"{namespace}/{name}"
        else:
            self.value = name

    def to_edn(self) -> str:
        return f":{self.value}"

    def __repr__(self):
        return f"Keyword('{self.value}')"


class Symbol(EDNType):
    """Explicit EDN symbol.

    Symbols are used for function names and special forms in Clojure.
    Rarely needed in data transactions.

    Args:
        name: Symbol name
        namespace: Optional namespace

    Examples:
        >>> edn.symbol("my-fn")
        # → my-fn

        >>> edn.symbol("assoc", "clojure.core")
        # → clojure.core/assoc
    """

    def __init__(self, name: str, namespace: Optional[str] = None):
        if namespace:
            self.value = f"{namespace}/{name}"
        else:
            self.value = name

    def to_edn(self) -> str:
        return self.value

    def __repr__(self):
        return f"Symbol('{self.value}')"


class UUID(EDNType):
    """Explicit EDN UUID.

    Args:
        value: UUID string (with or without #uuid prefix)

    Examples:
        >>> edn.uuid("550e8400-e29b-41d4-a716-446655440000")
        # → #uuid "550e8400-e29b-41d4-a716-446655440000"
    """

    def __init__(self, value: str):
        # Strip #uuid prefix if present
        if value.startswith('#uuid'):
            value = value[5:].strip().strip('"')
        self.value = value

    def to_edn(self) -> str:
        return f'#uuid "{self.value}"'

    def __repr__(self):
        return f"UUID('{self.value}')"


class Inst(EDNType):
    """Explicit EDN instant (timestamp).

    Args:
        value: ISO 8601 timestamp string

    Examples:
        >>> edn.inst("2024-01-01T00:00:00Z")
        # → #inst "2024-01-01T00:00:00Z"
    """

    def __init__(self, value: str):
        # Strip #inst prefix if present
        if value.startswith('#inst'):
            value = value[5:].strip().strip('"')
        self.value = value

    def to_edn(self) -> str:
        return f'#inst "{self.value}"'

    def __repr__(self):
        return f"Inst('{self.value}')"


# =============================================================================
# Convenience Functions
# =============================================================================

def keyword(name: str, namespace: Optional[str] = None) -> Keyword:
    """Create an EDN keyword.

    Args:
        name: Keyword name
        namespace: Optional namespace

    Returns:
        Keyword instance

    Examples:
        >>> keyword("name")
        # → :name

        >>> keyword("name", "person")
        # → :person/name
    """
    return Keyword(name, namespace)


def symbol(name: str, namespace: Optional[str] = None) -> Symbol:
    """Create an EDN symbol.

    Args:
        name: Symbol name
        namespace: Optional namespace

    Returns:
        Symbol instance
    """
    return Symbol(name, namespace)


def uuid(value: str) -> UUID:
    """Create an EDN UUID.

    Args:
        value: UUID string

    Returns:
        UUID instance
    """
    return UUID(value)


def inst(value: str) -> Inst:
    """Create an EDN instant.

    Args:
        value: ISO 8601 timestamp string

    Returns:
        Inst instance
    """
    return Inst(value)


def string(value: str) -> str:
    """Force a value to be treated as a string.

    Use this to create strings that start with ':' without them
    becoming keywords.

    Args:
        value: String value (can start with :)

    Returns:
        Marked string that will be quoted in EDN

    Examples:
        >>> edn.string(":literal-colon")
        # → ":literal-colon" (not :literal-colon keyword)
    """
    # Use special marker that database.py recognizes
    return f"<STRING>{value}"


# =============================================================================
# Common Keyword Constants
# =============================================================================

class Keywords:
    """Pre-defined Datahike keyword constants.

    Use these for common Datahike schema and transaction attributes
    to avoid typos and get IDE autocompletion.

    Examples:
        >>> from datahike import kw
        >>>
        >>> schema = [{
        ...     kw.DB_IDENT: edn.keyword("person/name"),
        ...     kw.DB_VALUE_TYPE: kw.STRING,
        ...     kw.DB_CARDINALITY: kw.ONE
        ... }]
    """

    # Entity attributes
    DB_ID = ":db/id"
    DB_IDENT = ":db/ident"

    # Schema attributes
    DB_VALUE_TYPE = ":db/valueType"
    DB_CARDINALITY = ":db/cardinality"
    DB_DOC = ":db/doc"
    DB_UNIQUE = ":db/unique"
    DB_IS_COMPONENT = ":db/isComponent"
    DB_NO_HISTORY = ":db/noHistory"
    DB_INDEX = ":db/index"
    DB_FULLTEXT = ":db/fulltext"

    # Value types
    STRING = ":db.type/string"
    BOOLEAN = ":db.type/boolean"
    LONG = ":db.type/long"
    BIGINT = ":db.type/bigint"
    FLOAT = ":db.type/float"
    DOUBLE = ":db.type/double"
    BIGDEC = ":db.type/bigdec"
    INSTANT = ":db.type/instant"
    UUID_TYPE = ":db.type/uuid"
    KEYWORD_TYPE = ":db.type/keyword"
    SYMBOL_TYPE = ":db.type/symbol"
    REF = ":db.type/ref"
    BYTES = ":db.type/bytes"

    # Cardinality
    ONE = ":db.cardinality/one"
    MANY = ":db.cardinality/many"

    # Uniqueness
    UNIQUE_VALUE = ":db.unique/value"
    UNIQUE_IDENTITY = ":db.unique/identity"

    # Transaction metadata
    TX = ":db/tx"
    TX_INSTANT = ":db/txInstant"

    # Schema flexibility
    SCHEMA_READ = ":read"
    SCHEMA_WRITE = ":write"


# Export singleton instance
kw = Keywords()


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    # Types
    'Keyword',
    'Symbol',
    'UUID',
    'Inst',
    # Functions
    'keyword',
    'symbol',
    'uuid',
    'inst',
    'string',
    # Constants
    'Keywords',
    'kw',
]
