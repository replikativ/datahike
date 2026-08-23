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

### Attribute References

Store attributes as entity IDs (integers) instead of keywords in datoms for performance and Datomic compatibility:

```clojure
{:attribute-refs? true}  ;; Default: false
```

**How it works:**

Without attribute references (default):
```clojure
;; Datoms store attribute keywords directly
#datahike/Datom [1 :name "Alice" 536870913 true]
```

With attribute references enabled:
```clojure
;; Datoms store attribute entity IDs (integers)
#datahike/Datom [1 73 "Alice" 536870913 true]  ;; where 73 is the entity ID for :name
```

**Benefits:**
- **Better performance**: Integer comparisons are significantly faster than keyword comparisons, especially with many attributes
- **Datomic compatibility**: Matches Datomic's internal representation for easier migration
- **Attributes as entities**: Attributes become queryable entities in the database
- **Recommended for production**: Generally beneficial unless you have specific reasons to use keywords

**Considerations:**
- Must use `:schema-flexibility :write` (cannot use with `:read`)
- Requires ID ↔ keyword mapping (maintained automatically)
- System schema is bootstrapped into the index on database creation
- You still use keyword syntax in queries and transactions - translation is automatic

**Example:**

```clojure
;; Create database with attribute references
(def cfg {:store {:backend :memory
                  :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
          :attribute-refs? true
          :schema-flexibility :write})

(d/create-database cfg)
(def conn (d/connect cfg))

;; Use normal keyword syntax in transactions and queries
(d/transact conn [{:db/ident :name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}])

(d/transact conn [{:name "Alice"}])

;; Queries use keywords as usual - translation happens automatically
(d/q '[:find ?n :where [?e :name ?n]] @conn)
;; => #{["Alice"]}

;; But internally, datoms store integer attribute IDs for performance
```

**When to use:**
- **Use `:attribute-refs? true`** for production databases (recommended for performance)
- Use `:attribute-refs? false` only if you need `:schema-flexibility :read` or have specific compatibility requirements

### Index Selection

Choose the underlying index implementation:

```clojure
{:index :datahike.index/persistent-set}  ;; Default (recommended)
```

**Available indexes**:
- `:datahike.index/persistent-set` - Default, actively maintained, supports all features
- `:datahike.index/hitchhiker-tree` - Legacy, requires explicit library and namespace loading

Most users should use the default. Hitchhiker-tree is maintained for backward compatibility with existing databases.

### Reducing Write Amplification (Object Stores)

On request-priced object stores (S3, R2, Tigris, GCS) the number of objects
written per commit dominates cost and latency. Three opt-in, per-store options —
diff buffering (`:index-config {:diff-buf-size N}`), root fusion
(`:fuse-index-roots? true`), and commit-graph opt-out (`:commit-graph? false`) —
cut that object count, approaching a single write per small commit. Each is
create-time-fixed and adopted from the store on reconnect. See [Reducing write
amplification](./write-amplification.md) for how they work and when to enable
them.

## Advanced Configuration

### Single-Writer Model (Distributed Access)

For distributed deployments, configure a writer to handle all transactions while readers access storage directly via Distributed Index Space.

#### Safe Default (`:writer-ownership :shared`)

Datahike's local writer (`{:backend :self}`) defaults to shared ownership: it
re-reads the branch head before each batch rather than assuming this JVM owns
the branch exclusively. Serverless runtimes make that safety important. Each AWS Lambda execution
environment is a separate JVM that believes it is the only writer, and Lambda
keeps several warm and routes to them *alternately*. With exclusive ownership,
each one would commit on top of its own stale head and silently overwrite the
other's transactions — no error, lost data.

```clojure
{:store  {:backend :s3 :bucket "my-bucket"}
 :writer {:backend :self}} ; :writer-ownership :shared is the default
```

With shared ownership the writer re-reads the branch head from storage before
each *batch* of transactions, so they are applied to whatever is actually
stored, and `@conn` reads through to storage as well.

- **Cost:** one branch-head GET per batch (~10-40 ms on S3, ~$0.0000004 at
  $0.0004/1000 GET). Transactions already queued when one commits are chained
  onto it and share its head read, the way the exclusive writer chains onto
  `:db-after`, so **commit batching survives**: a burst of 500 concurrent
  transactions costs ~9 head reads and ~20 commits, not 500 of each. The chain
  is bounded and never *waits* for more work to arrive, so it costs no latency
  — a caller that awaits each transaction before issuing the next has nothing
  to batch and does pay one read per commit.
- **Opting into `:exclusive`:** do this only when one process exclusively owns the
  writer and avoiding the branch-head GET is worth the weaker safety.
- **Serialisation, plus a fence:** re-reading the head avoids the race between
  writers that alternate. Writers that **overlap** need the head write itself to
  be conditional, which is what head fencing does ([issue #878]) — see
  [Concurrent processes](#concurrent-processes-head-fencing) below. Fencing is
  automatic wherever the store supports it, so shared ownership is safe for
  alternating writers on any store and for concurrent writers on a store that
  can compare-and-set.
- **Secondary indices are re-read too**, whenever the head moved — they are
  named by the same commit, so another process's writes reach them like any
  other part of the db. Stratum and proximum are konserve-backed copy-on-write
  values, which is what makes that work (and what makes them usable on S3).
  Scriptum is the exception: it keeps its own Lucene directory with a
  per-branch write lock and is NOT multi-process safe. That is transitional —
  what it still lacks is a conditional write on its manifest — not a property
  of secondary indices.

#### Concurrent Processes (head fencing)

Shared ownership re-reads the branch head, and datahike also writes it back
*conditionally*: the commit lands only if the head is still the one that was
read. If another process moved it in between, this commit is rejected rather
than overwriting theirs, and the transaction is re-applied against the new head.
Nothing is lost and nothing is partially applied — the values a commit writes
before the head flip are immutable and content-addressed, so a rejected commit
leaves collectable orphans, never a dangling pointer.

This needs a store that can compare-and-set. Konserve reports how far its
guarantee reaches as a *domain*:

| Domain | Meaning | Stores |
| --- | --- | --- |
| `:process` | threads in one JVM | memory |
| `:machine` | processes on one host | filestore (OS advisory file lock) |
| `:global` | processes on any host | S3 (`If-Match` on the object) |

Fencing is used automatically when the store offers it and skipped when it does
not, which keeps single-writer setups working unchanged on every backend. **If
your deployment depends on it, say so** — otherwise a store that cannot fence
degrades quietly to the unconditional write, which is exactly the failure the
mechanism exists to remove:

```clojure
{:store  {:backend :s3 :bucket "my-bucket"}
 :writer {:backend :self
          :writer-ownership :shared
          :require-fencing :global}}   ; refuse to connect without it
```

`:require-fencing` names the domain the deployment needs — `:machine` for
several processes on one host (`dthk` from two shells), `:global` for several
hosts (Lambda on S3). A store offering more than asked passes. It requires
`:writer-ownership :shared`: an exclusive writer never re-reads the head, so it has no
revision to fence against and the option would be inert.

The experimental self-writer `:streaming?` option from #959 remains a deprecated
compatibility alias (`false` means `:shared`, `true` means `:exclusive`).
Streaming itself remains a writer capability: both self writers stream their own
completed writes into the connection, Kabel streams synchronized remote writes,
and HTTP does not stream updates.

Three further knobs, all optional:

| Key | Default | Meaning |
| --- | --- | --- |
| `:head-conflict-retries` | 3 | How many times a rejected transaction is re-applied against the re-read head before the caller is told. `0` reports `:datahike/head-conflict` immediately, which is what you want if the caller must see every conflict. |
| `:head-conflict-backoff-ms` | 25 | Base for the jittered exponential backoff between retries. |
| `:max-batch` | 64 | Upper bound on transactions chained into one commit. |

Only `transact!` and `load-entities` are retried. Anything that merges branches
carries a conflict that belongs to the caller, and re-running it against a head
that moved would silently change what the merge means.

Branch lifecycle operations use the same store capability directly. Creating a
branch conditionally publishes its head; creating, deleting or forcing a branch
updates the shared `:branches` GC whitelist with a CAS loop; and database
creation conditionally claims the initial `:db` head. Stores without revisions
retain the historical best-effort behavior. `force-branch!` remains a deliberate
reset operation: it retries until its overwrite can be linearized, so use it as
exclusive administration rather than ordinary application traffic.

[issue #878]: https://github.com/replikativ/datahike/issues/878

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
