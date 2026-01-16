# Datahike Java API

**Status: Beta** - The Java API is stable and tested, but may receive refinements as we gather feedback from production use.

Datahike provides a comprehensive Java API that enables you to use the full power of Datalog databases from Java applications without writing Clojure code. The API offers both high-level convenience methods and low-level access for advanced use cases.

## Features

- **Type-Safe Configuration** - Fluent builder pattern with compile-time checks
- **Modern Java API** - Works with Java Maps, UUIDs, and standard collections
- **Full Datalog Support** - Expressive declarative queries with joins, aggregates, and rules
- **Time Travel** - Query database history and point-in-time snapshots
- **Pull API** - Recursive pattern-based entity retrieval
- **Schema Support** - Optional strict or flexible schema enforcement
- **Multiple Backends** - Memory, file system, and extensible to custom stores

## Installation

### Maven

Add the Clojars repository and Datahike dependency to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>clojars</id>
        <name>Clojars</name>
        <url>https://repo.clojars.org/</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>org.replikativ</groupId>
        <artifactId>datahike</artifactId>
        <version>CURRENT</version> <!-- Check https://clojars.org/org.replikativ/datahike for latest -->
    </dependency>
</dependencies>
```

### Gradle

Add to your `build.gradle`:

```gradle
repositories {
    maven { url "https://repo.clojars.org/" }
    mavenCentral()
}

dependencies {
    implementation 'org.replikativ:datahike:CURRENT' // Check https://clojars.org/org.replikativ/datahike for latest
}
```

## Quick Start

```java
import datahike.java.Datahike;
import datahike.java.Database;
import datahike.java.SchemaFlexibility;
import java.util.*;

// Create and connect to database
Map<String, Object> config = Database.memory(UUID.randomUUID())
    .schemaFlexibility(SchemaFlexibility.READ)
    .build();

Datahike.createDatabase(config);
Object conn = Datahike.connect(config);

// Transact data using Java Maps
Datahike.transact(conn, List.of(
    Map.of("name", "Alice", "age", 30),
    Map.of("name", "Bob", "age", 25)
));

// Query with Datalog
Set<?> results = (Set<?>) Datahike.q(
    "[:find ?name ?age :where [?e :name ?name] [?e :age ?age]]",
    Datahike.deref(conn)
);

System.out.println(results);
// => #{["Alice" 30] ["Bob" 25]}

// Cleanup
Datahike.deleteDatabase(config);
```

## Configuration

Datahike offers three approaches to configuration, each suited for different needs.

### Approach 1: Database Builder (Recommended)

The fluent builder pattern provides type safety and IDE autocompletion:

```java
import datahike.java.Database;
import datahike.java.SchemaFlexibility;
import java.util.UUID;

// In-memory database (requires UUID)
Map<String, Object> config = Database.memory(UUID.randomUUID())
    .keepHistory(true)
    .schemaFlexibility(SchemaFlexibility.READ)
    .build();

// File-based database
Map<String, Object> config = Database.file("/var/lib/mydb")
    .keepHistory(true)
    .name("production-db")
    .build();

// With initial schema
import static datahike.java.Keywords.*;
import static datahike.java.Util.*;

Object schema = vec(
    map(DB_IDENT, kwd(":person/name"),
        DB_VALUE_TYPE, STRING,
        DB_CARDINALITY, ONE)
);

Map<String, Object> config = Database.memory(UUID.randomUUID())
    .initialTx(schema)
    .build();
```

**When to use:** New projects, when you want type safety and clear code.

### Approach 2: Java Maps

Use standard Java collections with string keys (automatically converted to Clojure keywords):

```java
import java.util.*;

// Configuration with nested maps
Map<String, Object> config = Map.of(
    "store", Map.of(
        "backend", ":memory",  // : prefix makes it a keyword
        "id", UUID.randomUUID()
    ),
    "schema-flexibility", ":read",
    "keep-history?", true
);

// Custom backends
Map<String, Object> config = Map.of(
    "store", Map.of(
        "backend", ":pg",
        "host", "localhost",
        "port", 5432,
        "username", "user",
        "password", "secret"
    )
);
```

**When to use:** Dynamic configuration, config files (JSON/YAML → Map), custom backends.

### Approach 3: EDN Strings (Advanced)

For advanced use cases, work directly with Clojure's Extensible Data Notation:

```java
import static datahike.java.Util.*;

// Parse EDN string
Object config = ednFromString(
    "{:store {:backend :memory :id #uuid \"550e8400-e29b-41d4-a716-446655440000\"}}"
);

// Build EDN programmatically
Object config = map(
    kwd(":store"), map(
        kwd(":backend"), kwd(":memory"),
        kwd(":id"), UUID.randomUUID()
    )
);
```

**When to use:** Interop with Clojure code, advanced EDN features, maximum control.

## Database Lifecycle

### Creating and Connecting

```java
import datahike.java.Datahike;

// Create database (idempotent - safe to call multiple times)
Datahike.createDatabase(config);

// Check if database exists
boolean exists = Datahike.databaseExists(config);

// Connect to database (returns connection object)
Object conn = Datahike.connect(config);

// Get current database value
Object db = Datahike.deref(conn);
```

### Deleting Databases

```java
// Delete all database files/data
Datahike.deleteDatabase(config);

// Release connection (required for some backends like LevelDB)
Datahike.release(conn);
```

## Transactions

Transactions are atomic and consistent. Add, update, or retract data.

### Simple Transactions

```java
import java.util.*;

// Add entities with auto-generated IDs
Datahike.transact(conn, List.of(
    Map.of("name", "Alice", "age", 30),
    Map.of("name", "Bob", "age", 25)
));

// Update existing entity (requires :db/id)
Datahike.transact(conn, List.of(
    Map.of(":db/id", 1, "age", 31)
));

// Retract attribute
import static datahike.java.Util.*;

Datahike.transact(conn, vec(
    vec(kwd(":db/retract"), 1, kwd(":age"), 30)
));

// Retract entire entity
Datahike.transact(conn, vec(
    vec(kwd(":db.fn/retractEntity"), 1)
));
```

### EDN Conversion Rules

Datahike automatically converts between Java and Clojure data:

| Java Type | EDN Type | Example |
|-----------|----------|---------|
| `String` starting with `:` | Keyword | `":memory"` → `:memory` |
| Other `String` | String | `"Alice"` → `"Alice"` |
| `Integer`, `Long` | Long | `42` → `42` |
| `Boolean` | Boolean | `true` → `true` |
| `Map<String, ?>` | Map | `{"a": 1}` → `{:a 1}` |
| `List<?>`, `Object[]` | Vector | `[1, 2]` → `[1 2]` |
| `UUID` | UUID | `UUID` → `#uuid "..."` |

**Important:** Map keys are always converted to keywords. Use `:` prefix in string values to create keyword values.

See [EDN Conversion Documentation](bindings/edn-conversion.md) for complete rules and edge cases.

## Queries

Datahike uses Datalog, a declarative query language similar to SQL but more expressive.

### Basic Queries

```java
// Find all names
Set<?> results = (Set<?>) Datahike.q(
    "[:find ?name :where [?e :name ?name]]",
    Datahike.deref(conn)
);

// Find with conditions
Set<?> results = (Set<?>) Datahike.q(
    "[:find ?name ?age :where [?e :name ?name] [?e :age ?age] [(>= ?age 25)]]",
    Datahike.deref(conn)
);

// Joins across entities
Set<?> results = (Set<?>) Datahike.q(
    """
    [:find ?person-name ?friend-name
     :where
     [?p :person/name ?person-name]
     [?p :person/friends ?f]
     [?f :person/name ?friend-name]]
    """,
    Datahike.deref(conn)
);
```

### Parameterized Queries

```java
// Query with input parameters
Set<?> results = (Set<?>) Datahike.q(
    "[:find ?e :in $ ?name :where [?e :name ?name]]",
    Datahike.deref(conn),
    "Alice"
);

// Multiple databases
Object conn2 = Datahike.connect(otherConfig);
Set<?> results = (Set<?>) Datahike.q(
    "[:find ?name :in $ $2 :where [$ ?e :name ?name] [$2 ?e :active true]]",
    Datahike.deref(conn),
    Datahike.deref(conn2)
);
```

### Aggregates

```java
// Count, sum, min, max, avg
Set<?> results = (Set<?>) Datahike.q(
    "[:find (count ?e) (avg ?age) :where [?e :age ?age]]",
    Datahike.deref(conn)
);

// Group by
Set<?> results = (Set<?>) Datahike.q(
    """
    [:find ?department (avg ?salary)
     :where
     [?e :employee/department ?department]
     [?e :employee/salary ?salary]]
    """,
    Datahike.deref(conn)
);
```

## Pull API

The Pull API retrieves entities with nested relationships using pattern-based selectors.

### Basic Pull

```java
// Pull single entity by ID
Map<?, ?> entity = (Map<?, ?>) Datahike.pull(
    Datahike.deref(conn),
    "[:name :age]",
    1
);
// => {:name "Alice" :age 30}

// Pull all attributes
Map<?, ?> entity = (Map<?, ?>) Datahike.pull(
    Datahike.deref(conn),
    "[*]",
    1
);

// Pull multiple entities
List<?> entities = (List<?>) Datahike.pullMany(
    Datahike.deref(conn),
    "[:name :age]",
    List.of(1, 2, 3)
);
```

### Nested Pull

```java
// Pull with nested relationships
Map<?, ?> person = (Map<?, ?>) Datahike.pull(
    Datahike.deref(conn),
    """
    [:person/name
     {:person/friends [:person/name :person/email]}]
    """,
    1
);
// => {:person/name "Alice"
//     :person/friends [{:person/name "Bob" :person/email "bob@example.com"}]}

// Recursive pull (follow references up to 3 levels)
Map<?, ?> org = (Map<?, ?>) Datahike.pull(
    Datahike.deref(conn),
    "[:org/name {:org/parent 3}]",  // 3 = recursion depth
    orgId
);
```

## Time Travel

Query database state at any point in history.

### Historical Queries

```java
import java.time.Instant;
import java.util.Date;

// Query as of specific time
Date timestamp = Date.from(Instant.parse("2024-01-01T00:00:00Z"));
Object pastDb = Datahike.asOf(Datahike.deref(conn), timestamp);
Set<?> results = (Set<?>) Datahike.q(
    "[:find ?name :where [?e :name ?name]]",
    pastDb
);

// Query changes since timestamp
Object recentDb = Datahike.since(Datahike.deref(conn), timestamp);
Set<?> changes = (Set<?>) Datahike.q(
    "[:find ?name :where [?e :name ?name]]",
    recentDb
);

// Query full history (includes all assertions and retractions)
Object historyDb = Datahike.history(Datahike.deref(conn));
Set<?> allValues = (Set<?>) Datahike.q(
    "[:find ?name :where [?e :name ?name]]",
    historyDb
);
```

### Transaction Metadata

```java
import static datahike.java.Util.*;

// Add metadata to transaction
Datahike.transact(conn, Map.of(
    ":tx-data", List.of(Map.of("name", "Alice")),
    ":tx-meta", Map.of("author", "user@example.com", "reason", "user signup")
));

// Query transaction metadata
Set<?> txData = (Set<?>) Datahike.q(
    """
    [:find ?tx ?author ?time
     :where
     [?tx :author ?author]
     [?tx :db/txInstant ?time]]
    """,
    Datahike.deref(conn)
);
```

## Schema Definition

Schemas define attributes and their properties, enabling validation and optimizations.

### Defining Schema

```java
import static datahike.java.Keywords.*;
import static datahike.java.Util.*;

// Define schema attributes
Object schema = vec(
    map(
        DB_IDENT, kwd(":person/name"),
        DB_VALUE_TYPE, STRING,
        DB_CARDINALITY, ONE,
        DB_DOC, "Person's full name",
        DB_UNIQUE, UNIQUE_IDENTITY
    ),
    map(
        DB_IDENT, kwd(":person/age"),
        DB_VALUE_TYPE, LONG,
        DB_CARDINALITY, ONE
    ),
    map(
        DB_IDENT, kwd(":person/friends"),
        DB_VALUE_TYPE, REF,
        DB_CARDINALITY, MANY,
        DB_DOC, "Person's friends (entity references)"
    )
);

// Option 1: Set schema at database creation
Map<String, Object> config = Database.memory(UUID.randomUUID())
    .initialTx(schema)
    .build();

Datahike.createDatabase(config);

// Option 2: Transact schema after creation
Datahike.createDatabase(config);
Object conn = Datahike.connect(config);
Datahike.transact(conn, schema);
```

### Schema Flexibility

```java
import datahike.java.SchemaFlexibility;

// Strict: Only defined attributes allowed
Map<String, Object> config = Database.memory(UUID.randomUUID())
    .build();  // Default: strict schema enforcement

// Flexible read: Allow reading undefined attributes, reject writes
Map<String, Object> config = Database.memory(UUID.randomUUID())
    .schemaFlexibility(SchemaFlexibility.READ)
    .build();

// Flexible write: Allow both reading and writing undefined attributes
Map<String, Object> config = Database.memory(UUID.randomUUID())
    .schemaFlexibility(SchemaFlexibility.WRITE)
    .build();
```

### Schema Constants

Use the `Keywords` class for type-safe schema definitions:

```java
import static datahike.java.Keywords.*;

// Entity attributes
DB_ID, DB_IDENT

// Schema definition
DB_VALUE_TYPE, DB_CARDINALITY, DB_DOC, DB_UNIQUE, DB_INDEX

// Value types
STRING, BOOLEAN, LONG, BIGINT, FLOAT, DOUBLE, BIGDEC,
INSTANT, UUID_TYPE, KEYWORD_TYPE, SYMBOL_TYPE, REF, BYTES

// Cardinality
ONE, MANY

// Uniqueness
UNIQUE_VALUE, UNIQUE_IDENTITY

// Schema flexibility
SCHEMA_READ, SCHEMA_WRITE

// Storage backends
BACKEND_MEMORY, BACKEND_FILE
```

## Advanced Features

### Index Access

```java
// Get datoms from index (EAVT, AEVT, AVET)
Iterable<?> datoms = Datahike.datoms(
    Datahike.deref(conn),
    ":eavt"
);

// Seek to position in index
import static datahike.java.Util.*;

Iterable<?> datoms = Datahike.seekDatoms(
    Datahike.deref(conn),
    ":avet",
    kwd(":name"), "Alice"
);

// Get index range
Iterable<?> range = Datahike.indexRange(
    Datahike.deref(conn),
    ":name",
    "A", "M"  // Lexicographic range
);
```

### Entity API

```java
import datahike.java.IEntity;
import static datahike.java.Util.*;

// Get entity by ID
IEntity entity = (IEntity) Datahike.entity(Datahike.deref(conn), 1);

// Access attributes
String name = (String) entity.valAt(kwd(":name"));
Long age = (Long) entity.valAt(kwd(":age"));

// Touch entity (load all attributes)
Object touchedEntity = Datahike.touch(entity);
```

### Database Metrics

```java
// Get database statistics
Map<?, ?> metrics = (Map<?, ?>) Datahike.metrics(
    Datahike.deref(conn)
);

System.out.println(metrics);
// => {:datoms 1000 :indexed-datoms 1000 ...}
```

### Schema Introspection

```java
// Get current schema
Map<?, ?> schema = (Map<?, ?>) Datahike.schema(
    Datahike.deref(conn)
);

// Get reverse schema (ident -> attribute map)
Map<?, ?> reverseSchema = (Map<?, ?>) Datahike.reverseSchema(
    Datahike.deref(conn)
);
```

## Storage Backends

### Memory Backend

Fast in-memory storage, requires UUID identifier:

```java
Map<String, Object> config = Database.memory(UUID.randomUUID())
    .build();
```

**Use cases:** Testing, caching, temporary data, development.

### File Backend

Persistent file-based storage:

```java
Map<String, Object> config = Database.file("/var/lib/myapp/db")
    .build();
```

**Use cases:** Local applications, single-server deployments, development persistence.

### Custom Backends

Extend Datahike with custom storage implementations:

```java
Map<String, Object> config = Database.custom(Map.of(
    "backend", ":my-backend",
    "custom-option-1", "value1",
    "custom-option-2", "value2"
)).build();
```

Available via plugins: PostgreSQL, S3, Redis, and more.

## Examples

### Complete Application

```java
import datahike.java.*;
import static datahike.java.Keywords.*;
import static datahike.java.Util.*;
import java.util.*;

public class DatahikeExample {
    public static void main(String[] args) {
        // 1. Configure database
        Object schema = vec(
            map(DB_IDENT, kwd(":user/email"),
                DB_VALUE_TYPE, STRING,
                DB_CARDINALITY, ONE,
                DB_UNIQUE, UNIQUE_IDENTITY),
            map(DB_IDENT, kwd(":user/name"),
                DB_VALUE_TYPE, STRING,
                DB_CARDINALITY, ONE)
        );

        Map<String, Object> config = Database.file("/tmp/app-db")
            .initialTx(schema)
            .schemaFlexibility(SchemaFlexibility.READ)
            .keepHistory(true)
            .build();

        // 2. Create and connect
        Datahike.createDatabase(config);
        Object conn = Datahike.connect(config);

        // 3. Add data
        Datahike.transact(conn, List.of(
            Map.of(":user/email", "alice@example.com",
                   ":user/name", "Alice"),
            Map.of(":user/email", "bob@example.com",
                   ":user/name", "Bob")
        ));

        // 4. Query data
        Set<?> users = (Set<?>) Datahike.q(
            "[:find ?email ?name :where [?e :user/email ?email] [?e :user/name ?name]]",
            Datahike.deref(conn)
        );

        System.out.println("Users: " + users);

        // 5. Update data
        Datahike.transact(conn, List.of(
            Map.of(":user/email", "alice@example.com",  // Upsert by unique attr
                   ":user/name", "Alice Smith")
        ));

        // 6. Time travel
        var pastDb = Datahike.asOf(Datahike.deref(conn), new Date(0));
        var pastUsers = Datahike.q(
            "[:find ?name :where [?e :user/name ?name]]",
            pastDb
        );
        System.out.println("Past users: " + pastUsers);

        // 7. Cleanup
        Datahike.deleteDatabase(config);
    }
}
```

## API Reference

Full Javadoc available at: `https://javadoc.io/doc/org.replikativ/datahike/latest/`

### Key Classes

- **`Datahike`** - Main API with all database operations
- **`Database`** - Fluent builder for configuration
- **`Keywords`** - Pre-defined constants for schema and configuration
- **`EDN`** - EDN data type constructors and conversion
- **`Util`** - Low-level utilities (map, vec, kwd, etc.)
- **`SchemaFlexibility`** - Enum for schema modes
- **`IEntity`** - Entity interface for direct attribute access

### Core Methods

See auto-generated bindings in `Datahike.java` for complete list. All Datahike API functions are available.

## Comparison with Other JVM Databases

### vs Datomic

Datahike is Datomic-compatible with similar semantics:
- ✅ Same query language and API
- ✅ Same time-travel capabilities
- ✅ Similar schema system
- ✅ Open source and free
- ✅ Multiple storage backends
- ⚠️ Smaller community and ecosystem
- ⚠️ Single-node only (distributed features in development)

### vs SQL Databases

| Feature | Datahike | SQL |
|---------|----------|-----|
| Query Language | Declarative Datalog | Declarative SQL |
| Schema | Optional, flexible | Usually required |
| Joins | Implicit, natural | Explicit with JOIN |
| Time Travel | Built-in | Requires audit tables |
| Immutability | Yes, all data versioned | No, updates in-place |
| Transactions | ACID | ACID |

## Performance Tips

1. **Use indexes** - Define `:db/index true` for frequently queried attributes
2. **Batch transactions** - Transact multiple entities at once
3. **Disable history** - Set `:keep-history? false` for write-heavy workloads
4. **Pull API** - More efficient than multiple queries for related data
5. **Index selection** - Use appropriate index (:eavt, :aevt, :avet) for datoms access

## Troubleshooting

### Common Issues

**"Could not locate Clojure runtime"**
- Ensure Clojure is on your classpath
- Datahike includes it transitively, but check for conflicts

**"Memory backend requires UUID"**
- Use `UUID.randomUUID()` not string IDs
- Required by konserve store for distributed tracking

**"Schema validation failed"**
- Check schema flexibility setting
- Verify attribute definitions match data types

**ClassCastException in results**
- Query results are Clojure collections (Set, List, Map)
- Cast appropriately: `(Set<?>) Datahike.q(...)`

## Further Reading

- [Main README](../README.md) - Project overview and installation
- [Schema Documentation](schema.md) - Detailed schema guide
- [Time Travel Guide](time_variance.md) - Historical queries
- [Storage Backends](storage-backends.md) - Backend configuration
- [EDN Conversion](bindings/edn-conversion.md) - Java ↔ EDN mapping
- [Datalog Tutorial](https://docs.datomic.com/on-prem/query.html) - Query language guide

## License

Eclipse Public License 1.0 (EPL-1.0)

## Support

- **GitHub Issues**: https://github.com/replikativ/datahike/issues
- **Discussions**: https://github.com/replikativ/datahike/discussions
- **Professional Support**: Contact christian@weilbach.name for consulting
