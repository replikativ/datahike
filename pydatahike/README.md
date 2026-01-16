# Datahike Python Bindings

**Status: Beta** - API is stable and tested, but may receive breaking changes as we refine the bindings.

Python bindings for [Datahike](https://github.com/replikativ/datahike), a durable Datalog database powered by an efficient Datalog query engine.

## Features

- **Datalog queries** - Expressive declarative queries
- **Durable storage** - Persistent databases with ACID transactions
- **Time travel** - Query database history and point-in-time snapshots
- **Flexible schema** - Schema-on-write or schema-on-read
- **Pull API** - Recursive pattern-based entity retrieval
- **Multiple backends** - Memory, file system, and more
- **Pythonic API** - Work with Python dicts, not EDN strings

## Installation

### Prerequisites

- Python 3.8+
- GraalVM (for building native library)
- Babashka (for build tasks)

### Building from Source

```bash
# Clone the Datahike repository
git clone https://github.com/replikativ/datahike.git
cd datahike

# Build the native library
bb ni-compile

# Install Python package
pip install ./pydatahike
```

### Setting Library Path

If the library isn't found automatically, set the environment variable:

```bash
export LIBDATAHIKE_PATH=/path/to/datahike/libdatahike/target/libdatahike.so
```

## Quick Start

```python
from datahike import Database

# Create database
db = Database.memory("quickstart")
db.create()

# Transact data
db.transact([
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
])

# Query with Datalog
result = db.q('[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]')
print(result)  # [['Alice', 30], ['Bob', 25]]

# Pull entity
entity = db.pull('[:name :age]', 1)
print(entity)  # {':name': 'Alice', ':age': 30}

# Cleanup
db.delete()
```

## High-Level API (Recommended)

The high-level API provides a Pythonic interface using native Python data structures.

### Database Class

```python
from datahike import Database

# Create in-memory database
db = Database.memory("mydb")
db.create()

# Create file-based database
db = Database.file("/tmp/mydb")
db.create()

# Create with full configuration (Python dict)
db = Database({
    "store": {
        "backend": ":memory",
        "id": "mydb"
    },
    "schema-flexibility": ":read",
    "keep-history?": True
})
db.create()

# Don't forget to delete when done
db.delete()
```

### Context Manager (Auto-Cleanup)

```python
from datahike import database

# Database automatically created and deleted
with database(backend=':memory', id='test') as db:
    db.transact([{"name": "Alice"}])
    result = db.q('[:find ?name :where [?e :name ?name]]')
    print(result)  # [['Alice']]
# Database deleted when context exits
```

### Transactions with Python Dicts

```python
# Single entity
db.transact({"name": "Alice", "age": 30})

# Multiple entities
db.transact([
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
])

# Schema transaction with EDN helpers
from datahike import edn, kw

db.transact([{
    kw.DB_IDENT: edn.keyword("person/name"),
    kw.DB_VALUE_TYPE: kw.STRING,
    kw.DB_CARDINALITY: kw.ONE,
    kw.DB_DOC: "Person's full name"
}])
```

### Queries

```python
# Simple query
result = db.q('[:find ?name :where [?e :name ?name]]')

# Query with parameters
result = db.q(
    '[:find ?e :in $ ?name :where [?e :name ?name]]',
    ('param', '"Alice"')
)

# Query multiple databases
other_db = Database.memory("other")
result = db.q(
    '[:find ?name :in $ $2 :where [$ ?e :name ?name] [$2 ?e :active true]]',
    other_db
)
```

### Time Travel

```python
import time

# Store current timestamp
t1 = int(time.time() * 1000)
db.transact({"name": "Alice", "status": ":active"})

time.sleep(0.1)
t2 = int(time.time() * 1000)
db.transact({"name": "Bob", "status": ":inactive"})

# Query current state
current = db.q('[:find ?name :where [?e :name ?name]]')
print(current)  # [['Alice'], ['Bob']]

# Query as of t1 (before Bob was added)
past = db.as_of(t1)
result = past.q('[:find ?name :where [?e :name ?name]]')
print(result)  # [['Alice']]

# Query changes since t1
changes = db.since(t1)
result = changes.q('[:find ?name :where [?e :name ?name]]')
print(result)  # [['Bob']]

# Query full history
history = db.history
result = history.q('[:find ?name :where [?e :name ?name]]')
print(result)  # All historical values
```

## EDN Conversion Rules

Datahike uses [EDN (Extensible Data Notation)](https://github.com/edn-format/edn), Clojure's data format. The Python bindings automatically convert between Python and EDN using simple, predictable rules:

### The Universal Rule

> **Keys are always keywordized. Values starting with `:` become keywords, everything else remains literal.**

### Examples

```python
# Python dict → EDN map
{
    "store": {
        "backend": ":memory",              # ":memory" → :memory (keyword)
        "id": "test"                    # "test" → "test" (string)
    },
    "schema-flexibility": ":read",      # ":read" → :read (keyword)
    "keep-history?": True               # True → true (boolean)
}
# → {:store {:backend :memory :id "test"}
#    :schema-flexibility :read
#    :keep-history? true}

# Transaction data
[
    {"name": "Alice", "status": ":active"},     # "Alice" → string, ":active" → keyword
    {"name": "Bob", "age": 25}                  # 25 → number
]
# → [{:name "Alice" :status :active}
#    {:name "Bob" :age 25}]
```

### Escape Hatches

For fine-grained control, use the `edn` helper module:

```python
from datahike import edn, kw

# Explicit keyword construction
edn.keyword("name")              # → :name
edn.keyword("name", "person")    # → :person/name

# Force string (even with : prefix)
edn.string(":literal-colon")     # → ":literal-colon" (string, not keyword)

# UUID and timestamps
edn.uuid("550e8400-e29b-41d4-a716-446655440000")  # → #uuid "..."
edn.inst("2024-01-01T00:00:00Z")                  # → #inst "..."

# Pre-defined constants (avoid typos)
kw.DB_ID                         # → ":db/id"
kw.DB_IDENT                      # → ":db/ident"
kw.DB_VALUE_TYPE                 # → ":db/valueType"
kw.STRING                        # → ":db.type/string"
kw.ONE                           # → ":db.cardinality/one"
```

For complete EDN conversion rules and edge cases, see [EDN Conversion Documentation](../doc/bindings/edn-conversion.md).

## Usage Examples

### Schema Definition

```python
from datahike import Database, edn, kw

db = Database.memory("schema-example")
db.create()

# Define schema using Python dicts and EDN helpers
schema = [{
    kw.DB_IDENT: edn.keyword("person/name"),
    kw.DB_VALUE_TYPE: kw.STRING,
    kw.DB_CARDINALITY: kw.ONE,
    kw.DB_DOC: "Person's full name"
}, {
    kw.DB_IDENT: edn.keyword("person/age"),
    kw.DB_VALUE_TYPE: kw.LONG,
    kw.DB_CARDINALITY: kw.ONE
}, {
    kw.DB_IDENT: edn.keyword("person/friends"),
    kw.DB_VALUE_TYPE: kw.REF,
    kw.DB_CARDINALITY: kw.MANY,
    kw.DB_DOC: "Person's friends (refs)"
}]

db.transact(schema)

# Get schema
db_schema = db.schema()
print(db_schema)
```

### Pull API

```python
# Pull single entity
entity = db.pull('[:name :age]', 1)

# Pull with wildcard
entity = db.pull('[*]', 1)

# Pull with relationships
pattern = '[:name {:friends [:name]}]'
entity = db.pull(pattern, 1)

# Pull multiple entities
entities = db.pull_many('[:name]', [1, 2, 3])
```

### Custom Backends

The dict-based API naturally supports custom backends with arbitrary configuration:

```python
# Custom S3 backend (hypothetical)
db = Database({
    "store": {
        "backend": ":my-s3",
        "bucket": "my-bucket",
        "region": "us-west-2",
        "encryption": {
            "type": ":aes256",
            "key-id": "secret-key"
        }
    }
})
db.create()
```

### Index Operations

```python
# Get datoms from index
datoms = db.datoms(':eavt')

# Seek to position in index
datoms = db.seek_datoms(':avet', [':name', '"Alice"'])

# Get index range
range_data = db.index_range(':name', '"A"', '"M"')
```

### Error Handling

```python
from datahike import Database, DatahikeException

db = Database.memory("errors")
db.create()

try:
    result = db.q('[:find ?e :where [?e :nonexistent]]')
except DatahikeException as e:
    print(f"Query failed: {e}")
```

## Advanced: Low-Level API

For advanced use cases, you can use the low-level API that works directly with EDN strings:

```python
from datahike import create_database, delete_database, transact, q

# Create database with EDN config
config = '{:store {:backend :memory :id "lowlevel"}}'
create_database(config)

# Transact EDN data
transact(config, '[{:name "Alice" :age 30}]')

# Query with explicit inputs
result = q(
    '[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]',
    [('db', config)],
    output_format='cbor'
)

# Cleanup
delete_database(config)
```

The low-level API gives you full control over EDN serialization and output formats, but requires manual string construction.

## Output Formats

All query and retrieval functions support multiple output formats:

- `cbor` (default) - Compact binary format, best for structured data
- `json` - Human-readable, good for debugging
- `edn` - Clojure data format (returned as string)

```python
# High-level API
result = db.q(query, output_format='json')

# Low-level API
result = q(query, inputs, output_format='edn')
```

## API Reference

### High-Level API

#### Database Class

**Factory Methods:**
- `Database.memory(id)` - Create in-memory database config
- `Database.file(path)` - Create file-based database config
- `Database(config_dict)` - Create from Python dict
- `Database(edn_string)` - Create from EDN string

**Lifecycle:**
- `db.create()` - Create the database
- `db.delete()` - Delete the database
- `db.exists()` - Check if database exists

**Transactions:**
- `db.transact(data, input_format='json')` - Execute transaction with Python dict/list

**Queries:**
- `db.q(query, *args, **kwargs)` - Execute Datalog query
- `db.pull(selector, eid)` - Pull entity by pattern
- `db.pull_many(selector, eids)` - Pull multiple entities
- `db.entity(eid)` - Get entity by ID

**Time Travel:**
- `db.as_of(timestamp_ms)` - Return DatabaseSnapshot at point in time
- `db.since(timestamp_ms)` - Return DatabaseSnapshot with changes since time
- `db.history` - Return DatabaseSnapshot with full history

**Schema & Metadata:**
- `db.schema()` - Get database schema
- `db.reverse_schema()` - Get reverse schema mapping
- `db.metrics()` - Get database metrics

**Index Operations:**
- `db.datoms(index)` - Get datoms from index
- `db.seek_datoms(index, components)` - Seek to position in index
- `db.index_range(attr, start, end)` - Get index range

**Maintenance:**
- `db.gc_storage()` - Garbage collect storage

#### Context Manager

- `database(**kwargs)` - Context manager for automatic database lifecycle
- `database(config_dict)` - Context manager with dict config

#### EDN Helpers

**Types:**
- `edn.keyword(name, namespace=None)` - Create EDN keyword
- `edn.symbol(name, namespace=None)` - Create EDN symbol
- `edn.uuid(value)` - Create EDN UUID
- `edn.inst(value)` - Create EDN instant (timestamp)
- `edn.string(value)` - Force string (escape : prefix)

**Constants (`kw` object):**
- `kw.DB_ID`, `kw.DB_IDENT`
- `kw.DB_VALUE_TYPE`, `kw.DB_CARDINALITY`, `kw.DB_DOC`
- `kw.DB_UNIQUE`, `kw.DB_IS_COMPONENT`, `kw.DB_NO_HISTORY`
- `kw.STRING`, `kw.BOOLEAN`, `kw.LONG`, `kw.BIGINT`, `kw.FLOAT`, `kw.DOUBLE`
- `kw.INSTANT`, `kw.UUID_TYPE`, `kw.KEYWORD_TYPE`, `kw.SYMBOL_TYPE`
- `kw.REF`, `kw.BYTES`
- `kw.ONE`, `kw.MANY`
- `kw.UNIQUE_VALUE`, `kw.UNIQUE_IDENTITY`
- `kw.SCHEMA_READ`, `kw.SCHEMA_WRITE`

### Low-Level API

**Database Operations:**
- `create_database(config)` - Create a new database
- `delete_database(config)` - Delete a database
- `database_exists(config)` - Check if database exists

**Data Operations:**
- `transact(config, tx_data, input_format='edn')` - Execute transaction
- `q(query, inputs, output_format='cbor')` - Execute Datalog query
- `pull(config, selector, eid, output_format='cbor')` - Pull entity
- `pull_many(config, selector, eids, output_format='cbor')` - Pull multiple
- `entity(config, eid, output_format='cbor')` - Get entity by ID

**Index Operations:**
- `datoms(config, index, output_format='cbor')` - Get datoms
- `seek_datoms(config, index, components, output_format='cbor')` - Seek datoms
- `index_range(config, attr, start, end, output_format='cbor')` - Get range

**Schema & Metadata:**
- `schema(config, output_format='cbor')` - Get schema
- `reverse_schema(config, output_format='cbor')` - Get reverse schema
- `metrics(config, output_format='cbor')` - Get metrics

**Maintenance:**
- `gc_storage(config)` - Garbage collect storage

## Development

### Running Tests

```bash
# Build native library first
cd datahike && bb ni-compile

# Run all tests
cd pydatahike
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_edn_conversion.py -v
```

### Type Checking

```bash
pip install mypy
mypy src/datahike
```

## Migration from Low-Level API

If you're using the low-level API with EDN strings, migrating to the high-level API is straightforward:

**Before:**
```python
from datahike import create_database, transact, q, delete_database

config = '{:store {:backend :memory :id "mydb"}}'
create_database(config)
transact(config, '[{:name "Alice"}]')
result = q('[:find ?name :where [?e :name ?name]]', [('db', config)])
delete_database(config)
```

**After:**
```python
from datahike import Database

db = Database.memory("mydb")
db.create()
db.transact({"name": "Alice"})
result = db.q('[:find ?name :where [?e :name ?name]]')
db.delete()
```

**Or with context manager:**
```python
from datahike import database

with database(backend=':memory', id='mydb') as db:
    db.transact({"name": "Alice"})
    result = db.q('[:find ?name :where [?e :name ?name]]')
```

## License

Eclipse Public License 1.0 (EPL-1.0)

## Links

- [Datahike GitHub](https://github.com/replikativ/datahike)
- [Datahike Documentation](https://github.com/replikativ/datahike/blob/main/doc/index.md)
- [EDN Conversion Rules](../doc/bindings/edn-conversion.md)
- [Datalog Tutorial](https://docs.datomic.com/on-prem/query.html)
