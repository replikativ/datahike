# Datahike Database Configuration

Datahike is highly configurable to support different deployment models and use cases. Configuration is set at database creation and cannot be changed afterward (though data can be migrated to a new configuration).

## Configuration Methods

Datahike uses the [environ library](https://github.com/weavejester/environ) for configuration, supporting three methods:

1. **Environment variables** (lowest priority)
2. **Java system properties** (middle priority)
3. **Configuration map argument** (highest priority - overwrites others)

This allows flexible deployment: hardcode configs in development, use environment variables in containers, or Java properties in production JVMs.

## Basic Configuration

The minimal configuration map includes:

```clojure
{:store              {:backend :memory      ;keyword - storage backend
                      :id #uuid "550e8400-e29b-41d4-a716-446655440020"} ;UUID - database identifier
 :name               nil                    ;string - optional database name (auto-generated if nil)
 :schema-flexibility :write                 ;keyword - :read or :write
 :keep-history?      true                   ;boolean - enable time-travel queries
 :attribute-refs?    false                  ;boolean - use entity IDs for attributes (Datomic-compatible)
 :index              :datahike.index/persistent-set  ;keyword - index implementation
 :store-cache-size   1000                   ;number - store cache entries
 :search-cache-size  10000}                 ;number - search cache entries
```

**Quick start** with defaults (in-memory database):

```clojure
(require '[datahike.api :as d])
(d/create-database)  ;; Creates memory DB with sensible defaults
```

## Storage Backends

Datahike supports multiple storage backends via [konserve](https://github.com/replikativ/konserve). The choice of backend determines durability, scalability, and deployment model.

**Built-in backends:**
- `:memory` - In-memory (ephemeral)
- `:file` - File-based persistent storage

**External backend libraries:**
- [LMDB](https://github.com/replikativ/datahike-lmdb) - High-performance local storage
- [JDBC](https://github.com/replikativ/datahike-jdbc) - PostgreSQL, MySQL, H2
- [Redis](https://github.com/replikativ/konserve-redis) - High write throughput
- [S3](https://github.com/replikativ/konserve-s3) - AWS cloud storage
- [GCS](https://github.com/replikativ/konserve-gcs) - Google Cloud storage
- [DynamoDB](https://github.com/replikativ/konserve-dynamodb) - AWS NoSQL
- [IndexedDB](https://github.com/replikativ/konserve-indexeddb) - Browser storage

**For detailed backend selection guidance**, see [Storage Backends Documentation](./storage-backends.md).

### Environment Variable Configuration

When using environment variables or Java system properties, name them like:

properties                  | envvar
----------------------------|--------------------------
datahike.store.backend      | DATAHIKE_STORE_BACKEND
datahike.store.username     | DATAHIKE_STORE_USERNAME
datahike.schema.flexibility | DATAHIKE_SCHEMA_FLEXIBILITY
datahike.keep.history       | DATAHIKE_KEEP_HISTORY
datahike.attribute.refs     | DATAHIKE_ATTRIBUTE_REFS
datahike.name               | DATAHIKE_NAME
etc.

**Note**: Do not use `:` in keyword strings for environment variables—it will be added automatically.

### Backend Configuration Examples

#### Memory (Built-in)

Ephemeral storage for testing and development:

```clojure
{:store {:backend :memory
         :id #uuid "550e8400-e29b-41d4-a716-446655440021"}}
```

Environment variables:
```bash
DATAHIKE_STORE_BACKEND=memory
DATAHIKE_STORE_CONFIG='{:id #uuid "550e8400-e29b-41d4-a716-446655440021"}'
```

#### File (Built-in)

Persistent local file storage:

```clojure
{:store {:backend :file
         :path "/var/db/datahike"}}
```

Environment variables:
```bash
DATAHIKE_STORE_BACKEND=file
DATAHIKE_STORE_CONFIG='{:path "/var/db/datahike"}'
```

#### LMDB (External Library)

High-performance local storage via [datahike-lmdb](https://github.com/replikativ/datahike-lmdb):

```clojure
{:store {:backend :lmdb
         :path "/var/db/datahike-lmdb"}}
```

#### JDBC (External Library)

PostgreSQL or other JDBC databases via [datahike-jdbc](https://github.com/replikativ/datahike-jdbc):

```clojure
{:store {:backend :jdbc
         :dbtype "postgresql"
         :host "db.example.com"
         :port 5432
         :dbname "datahike"
         :user "datahike"
         :password "secret"}}
```

#### S3 (External Library)

AWS S3 storage via [konserve-s3](https://github.com/replikativ/konserve-s3):

```clojure
{:store {:backend :s3
         :bucket "my-datahike-bucket"
         :region "us-east-1"}}
```

#### TieredStore (Composable)

Memory hierarchy (e.g., Memory → IndexedDB for browsers):

```clojure
{:store {:backend :tiered
         :id #uuid "550e8400-e29b-41d4-a716-446655440022"
         :frontend-config {:backend :memory
                          :id #uuid "550e8400-e29b-41d4-a716-446655440022"}
         :backend-config {:backend :indexeddb
                         :name "persistent-db"
                         :id #uuid "550e8400-e29b-41d4-a716-446655440022"}}}
         ;; All :id values must match for konserve validation
```

For complete backend options and selection guidance, see [Storage Backends](./storage-backends.md).


## Core Configuration Options

### Database Name

Optional identifier for the database. Auto-generated if not specified. Useful when running multiple databases:

```clojure
{:name "production-db"
 :store {:backend :file :path "/var/db/prod"}}
```

### Schema Flexibility

Controls when schema validation occurs:

- **`:write`** (default): Strict schema—attributes must be defined before use. Catches errors early.
- **`:read`**: Schema-less—accept any data, validate on read. Flexible for evolving data models.

```clojure
{:schema-flexibility :read}  ;; Allow any data structure
```

With `:read` flexibility, you can still define critical schema like `:db/unique`, `:db/cardinality`, or `:db.type/ref` where needed.

See [Schema Documentation](./schema.md) for details.

### Time-Travel Queries

Enable historical query capabilities:

```clojure
{:keep-history? true}  ;; Default: true
```

When enabled, use `history`, `as-of`, and `since` to query past states:

```clojure
(d/q '[:find ?e :where [?e :name "Alice"]] (d/as-of db #inst "2024-01-01"))
```

**Disable if**: You never need historical queries and want to save storage space.

See [Time Variance Documentation](./time_variance.md) for time-travel query examples.

### Attribute References (Datomic Compatibility)

Store attributes as entity IDs instead of keywords for Datomic compatibility and faster comparisons:

```clojure
{:attribute-refs? true}  ;; Default: false
```

**Benefits**:
- Datomic query compatibility
- Faster integer comparisons vs. keyword comparisons
- Attributes become queryable entities

**Trade-off**: Requires ID → keyword lookups in some operations.

### Index Selection

Choose the underlying index implementation:

```clojure
{:index :datahike.index/persistent-set}  ;; Default (recommended)
```

**Available indexes**:
- `:datahike.index/persistent-set` - Default, actively maintained, supports all features
- `:datahike.index/hitchhiker-tree` - Legacy, requires explicit library and namespace loading

Most users should use the default. Hitchhiker-tree is maintained for backward compatibility with existing databases.

## Advanced Configuration

### Single-Writer Model (Distributed Access)

For distributed deployments, configure a writer to handle all transactions while readers access storage directly via Distributed Index Space.

#### HTTP Server Writer

```clojure
{:store {:backend :file :path "/shared/db"}
 :writer {:backend :datahike-server
          :url "http://writer.example.com:4444"
          :token "secure-token"}}
```

Clients connect and transact through the HTTP server. Reads happen locally from shared storage.

#### Kabel WebSocket Writer (Beta)

Real-time reactive updates via WebSocket:

```clojure
{:store {:backend :indexeddb :name "app-db" :id store-id}
 :writer {:backend :kabel
          :peer-id server-peer-id
          :local-peer @client-peer}}  ;; Set up via kabel/distributed-scope
```

Enables browser clients with live synchronization. See [Distributed Architecture](./distributed.md) for setup details.

### Branching (Beta)

Access specific database branches (git-like versioning):

```clojure
{:store {:backend :file :path "/var/db"}
 :branch :staging}  ;; Default branch is :db
```

Create and merge branches for testing, staging, or experiments. See [Versioning](./versioning.md) for the branching API.

### Remote Procedure Calls

Send all operations (reads and writes) to a remote server:

```clojure
{:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440023"}
 :remote-peer {:backend :datahike-server
               :url "http://server.example.com:4444"
               :token "secure-token"}}
```

Useful for thin clients or when you want centralized query execution. See [Distributed Architecture](./distributed.md) for RPC vs. DIS trade-offs.

### Initial Transaction

Seed the database with schema or data on creation:

```clojure
{:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440024"}
 :initial-tx [{:db/ident :name
               :db/valueType :db.type/string
               :db/cardinality :db.cardinality/one}
              {:db/ident :email
               :db/valueType :db.type/string
               :db/unique :db.unique/identity
               :db/cardinality :db.cardinality/one}]}
```

Convenient for testing or deploying databases with predefined schema.

### Complete Configuration Example

```clojure
{:store {:backend :file
         :path "/var/datahike/production"
         :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
 :name "production-db"
 :schema-flexibility :write
 :keep-history? true
 :attribute-refs? false
 :index :datahike.index/persistent-set
 :store-cache-size 10000
 :search-cache-size 100000
 :initial-tx [{:db/ident :user/email
               :db/valueType :db.type/string
               :db/unique :db.unique/identity
               :db/cardinality :db.cardinality/one}]
 :writer {:backend :datahike-server
          :url "http://writer.example.com:4444"
          :token "secure-token"}
 :branch :db}
```

## Migration and Compatibility

### URI Scheme (Pre-0.3.0, Deprecated)

Prior to version 0.3.0, Datahike used URI-style configuration. This is **still supported** but deprecated in favor of the more flexible hashmap format.

**Old URI format**:
```clojure
"datahike:memory://my-db?temporal-index=true&schema-on-read=true"
```

**New hashmap format** (equivalent):
```clojure
{:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440025"}
 :keep-history? true
 :schema-flexibility :read}
```

**Key changes**:
- `:temporal-index` → `:keep-history?`
- `:schema-on-read` → `:schema-flexibility` (`:read` or `:write`)
- Store parameters moved to `:store` map
- Memory backend: `:host`/`:path` → `:id`
- Direct support for advanced features (writer, branches, initial-tx)

Existing URI configurations continue to work—no migration required unless you need new features.

## Further Documentation

- [Storage Backends](./storage-backends.md) - Choosing and configuring storage
- [Schema](./schema.md) - Schema definition and flexibility
- [Time Variance](./time_variance.md) - Historical queries (as-of, history, since)
- [Versioning](./versioning.md) - Git-like branching and merging
- [Distributed Architecture](./distributed.md) - DIS, writers, and RPC
- [JavaScript API](./javascript-api.md) - Node.js and browser usage
