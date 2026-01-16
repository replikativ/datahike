# EDN Conversion Rules for Language Bindings

This document describes the universal rules for converting native data structures (Python dicts, JavaScript objects, Java Maps) to EDN (Extensible Data Notation) across all Datahike language bindings.

## Design Goal

Provide a **consistent, explicit, and predictable** mapping between native language data structures and EDN that works the same way across Python, JavaScript, and Java bindings.

## The Universal Rule

> **Keys are always keywordized. Values starting with `:` become keywords, everything else remains literal.**

### Key Conversion

All map/dict/object keys are automatically converted to EDN keywords:

| Input | EDN Output | Notes |
|-------|------------|-------|
| `"name"` | `:name` | Simple key |
| `"person/name"` | `:person/name` | Namespaced key |
| `":db/id"` | `:db/id` | Leading `:` is stripped (convenience) |
| `"keep-history?"` | `:keep-history?` | Question mark preserved |

### Value Conversion

Values are converted based on their type and content:

| Input Type | Input Value | EDN Output | Rule |
|------------|-------------|------------|------|
| String starting with `:` | `":active"` | `:active` | Keyword |
| String without `:` | `"Alice"` | `"Alice"` | String |
| String with `\:` prefix | `"\\:literal"` | `":literal"` | Escaped literal |
| Integer | `42` | `42` | Number |
| Float | `3.14` | `3.14` | Number |
| Boolean | `true` / `false` | `true` / `false` | Boolean |
| Null/None | `null` / `None` | `nil` | Nil |
| List/Array | `[1, 2, 3]` | `[1 2 3]` | Vector |
| Nested Map/Dict | `{"a": 1}` | `{:a 1}` | Recursive |

## Examples by Language

### Python

```python
# Configuration
import uuid

config = {
    "store": {
        "backend": ":memory",              # Keyword
        "id": str(uuid.uuid4())         # String (memory backend requires UUID)
    },
    "schema-flexibility": ":read",      # Keyword
    "keep-history?": True               # Boolean
}
# → {:store {:backend :memory :id "<uuid-string>"}
#    :schema-flexibility :read
#    :keep-history? true}

# Transaction data
data = [
    {"name": "Alice", "status": ":active"},
    {"name": "Bob", "status": ":inactive"}
]
# → [{:name "Alice" :status :active}
#    {:name "Bob" :status :inactive}]

# Schema transaction
schema = [
    {
        "db/ident": ":person/name",
        "db/valueType": ":db.type/string",
        "db/cardinality": ":db.cardinality/one"
    }
]
# → [{:db/ident :person/name
#     :db/valueType :db.type/string
#     :db/cardinality :db.cardinality/one}]
```

### JavaScript

```javascript
// Configuration
const crypto = require('crypto');
const config = {
    store: {
        backend: ':memory',                // Keyword
        id: crypto.randomUUID()         // String (memory backend requires UUID)
    },
    'schema-flexibility': ':read',      // Keyword
    'keep-history?': true               // Boolean
};
// → {:store {:backend :memory :id "<uuid-string>"}
//    :schema-flexibility :read
//    :keep-history? true}

// Transaction data
const data = [
    {name: 'Alice', status: ':active'},
    {name: 'Bob', status: ':inactive'}
];
// → [{:name "Alice" :status :active}
//    {:name "Bob" :status :inactive}]
```

### Java

```java
// Configuration
import java.util.UUID;

Map<String, Object> config = Map.of(
    "store", Map.of(
        "backend", ":memory",              // Keyword
        "id", UUID.randomUUID().toString() // String (memory backend requires UUID)
    ),
    "schema-flexibility", ":read"       // Keyword
);
// → {:store {:backend :memory :id "<uuid-string>"}
//    :schema-flexibility :read}

// Or use builders (convenience)
// Note: memory backend requires UUID identifier
Database db = Database.memory(UUID.randomUUID().toString())
    .schemaFlexibility(SchemaFlexibility.READ)
    .build();
```

## Edge Cases and Escape Hatches

### Literal Colon Strings

To include a string that starts with `:` without it becoming a keyword, use backslash escape:

```python
# Python
{"literal": "\\:starts-with-colon"}
# → {:literal ":starts-with-colon"}

# JavaScript
{literal: '\\:starts-with-colon'}
# → {:literal ":starts-with-colon"}

# Java
Map.of("literal", "\\:starts-with-colon")
// → {:literal ":starts-with-colon"}
```

**Note:** In Python/Java string literals, you need to escape the backslash itself: `"\\:"` → literal `\:` → EDN `":value"`

### Helper Modules for Complex Types

All languages provide helper modules for explicit type control:

```python
# Python
from datahike import edn

{
    "uuid": edn.uuid("550e8400-e29b-41d4-a716-446655440000"),
    "inst": edn.inst("2024-01-01T00:00:00Z"),
    "literal": edn.string(":force-string"),
    "keyword": edn.keyword("name", "person")  # :person/name
}
```

```javascript
// JavaScript
import {edn} from 'datahike';

{
    uuid: edn.uuid('550e8400-e29b-41d4-a716-446655440000'),
    inst: edn.inst('2024-01-01T00:00:00Z'),
    literal: edn.string(':force-string'),
    keyword: edn.keyword('person', 'name')  // :person/name
}
```

```java
// Java
import static datahike.edn.EDN.*;

Map.of(
    "uuid", uuid("550e8400-e29b-41d4-a716-446655440000"),
    "inst", inst("2024-01-01T00:00:00Z"),
    "literal", string(":force-string"),
    "keyword", keyword("person", "name")  // :person/name
)
```

### EDN String Escape Hatch

For truly complex cases (database functions, unusual syntax), all bindings accept raw EDN strings:

```python
# Python
db.transact('''
[{:db/id #db/id[:db.part/user]
  :db/fn (fn [db] (inc 1))}]
''', input_format='edn')
```

```javascript
// JavaScript
await d.transact(conn, `
[{:db/id #db/id[:db.part/user]
  :db/fn (fn [db] (inc 1))}]
`);
```

```java
// Java
Datahike.transact(conn,
    "[{:db/id #db/id[:db.part/user] " +
    "  :db/fn (fn [db] (inc 1))}]");
```

## Custom Backend Support

Custom storage backends may require arbitrary configuration keys that are not known in advance. The map/dict/object-based API handles these naturally:

```python
# Python - custom S3 backend
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
```

```javascript
// JavaScript - custom S3 backend
const db = new Database({
    store: {
        backend: ':my-s3',
        bucket: 'my-bucket',
        region: 'us-west-2',
        encryption: {
            type: ':aes256',
            'key-id': 'secret-key'
        }
    }
});
```

```java
// Java - custom S3 backend (Map API)
Map<String, Object> config = Map.of(
    "store", Map.of(
        "backend", ":my-s3",
        "bucket", "my-bucket",
        "region", "us-west-2",
        "encryption", Map.of(
            "type", ":aes256",
            "key-id", "secret-key"
        )
    )
);
Database db = new Database(config);
```

## Common Keyword Constants

All language bindings provide pre-defined constants for frequently-used Datahike keywords:

```python
# Python
from datahike import kw

kw.DB_ID                    # :db/id
kw.DB_IDENT                 # :db/ident
kw.DB_VALUE_TYPE            # :db/valueType
kw.DB_CARDINALITY           # :db/cardinality
kw.STRING                   # :db.type/string
kw.LONG                     # :db.type/long
kw.REF                      # :db.type/ref
kw.ONE                      # :db.cardinality/one
kw.MANY                     # :db.cardinality/many
```

```javascript
// JavaScript
import {kw} from 'datahike';

kw.DB_ID                    // :db/id
kw.DB_IDENT                 // :db/ident
// ... etc
```

```java
// Java
import static datahike.edn.Keywords.*;

DB_ID                       // :db/id
DB_IDENT                    // :db/ident
// ... etc
```

## Design Rationale

### Why `:` prefix for keyword values?

**Explicit is better than implicit.** Previous versions used heuristics (e.g., `"backend": "mem"` → `:memory`), but this created ambiguity:
- Is `"person/name"` a keyword or a string with a slash?
- Is `"active"` a keyword or a string?

The `:` prefix makes the distinction unambiguous and works consistently across all contexts.

### Why not separate config rules from data rules?

**Consistency reduces cognitive load.** Having one universal rule means:
- No need to remember context-specific rules
- Easier to teach and document
- Predictable behavior everywhere

### Why backslash escaping?

**Familiar to programmers.** Backslash escaping is used in JSON, regex, shell scripts, and most programming languages. It's a well-understood convention.

### Why allow EDN string fallback?

**Pragmatic escape hatch.** While the conversion rules handle 99% of cases, edge cases exist (database functions, unusual Clojure syntax). Allowing raw EDN strings ensures users are never blocked.

## Migration from Previous Versions

### JavaScript (Breaking Change)

**Old Syntax (v0.6 and earlier):**
```javascript
const crypto = require('crypto');
const config = {
    store: {
        backend: 'memory',      // String → keyword (implicit)
        id: crypto.randomUUID() // Memory backend requires UUID
    }
};
```

**New Syntax (v0.7+):**
```javascript
const crypto = require('crypto');
const config = {
    store: {
        backend: ':memory',     // Explicit : required
        id: crypto.randomUUID() // Memory backend requires UUID
    }
};
```

**Migration:** Add `:` prefix to all values that should be keywords. Note that memory backend requires UUID identifiers.

### Java (Enhancement)

**Old Syntax (still works):**
```java
import java.util.UUID;

Map<String, Object> config = Map.of(
    "store", Map.of(
        "backend", "mem",       // String passed through
        "id", UUID.randomUUID().toString() // Memory backend requires UUID
    )
);
```

**New Syntax (recommended):**
```java
import java.util.UUID;

Map<String, Object> config = Map.of(
    "store", Map.of(
        "backend", ":memory",      // Explicit : for keyword
        "id", UUID.randomUUID().toString() // Memory backend requires UUID
    )
);

// Or use builders (memory backend requires UUID)
Database.memory(UUID.randomUUID().toString()).build();
```

**Migration:** Both syntaxes work. Update to `:` prefix for consistency with other languages.

## See Also

- [Python Binding Documentation](python.md)
- [Java Binding Documentation](java.md)
- [JavaScript Binding Documentation](javascript.md)
- [Custom Backends Guide](custom-backends.md)
