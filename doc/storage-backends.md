# Storage Backends

Datahike provides pluggable storage through [konserve](https://github.com/replikativ/konserve), allowing you to choose the backend that best fits your deployment model and performance requirements.

## Quick Reference

| Backend | Best For | Distribution | Durability | Write Throughput |
|---------|----------|--------------|------------|------------------|
| **File** | Unix tools, rsync, git-like workflows | Single machine | High | Good |
| **LMDB** | High-performance single machine | Single filesystem | High | Excellent |
| **Memory** | Testing, ephemeral data | Single process | None | Excellent |
| **JDBC** | Existing SQL infrastructure | Multi-machine | High | Good |
| **Redis** | High write throughput | Multi-machine | Medium | Excellent |
| **S3** | Distributed scale-out, cost-effective | Multi-region | Very high | Good |
| **GCS** | Google Cloud scale-out | Multi-region | Very high | Good |
| **DynamoDB** | Low latency, AWS-native | Multi-region | Very high | Excellent (expensive) |
| **IndexedDB** | Browser persistence | Browser | Medium | Good |

## Local Backends

### File Backend

**Use when**: You want to use Unix tools (rsync, git, backup scripts) to manage your database.

**Key advantage**: Deltas in persistent data structures translate directly into individual file deltas, making incremental backups and synchronization highly efficient.

```clojure
{:store {:backend :file
         :path "/var/lib/myapp/db"}}
```

**Characteristics**:
- Each index stored as separate files
- Efficient incremental backups with rsync
- Can version database directories with git
- Good for single-machine deployments
- Natural fit for container volumes

### LMDB Backend

**Use when**: You need maximum performance on a single machine within a single filesystem.

**Key advantage**: Lightning-fast memory-mapped database with ACID transactions, optimized for read-heavy workloads.

```clojure
;; Requires: io.replikativ/datahike-lmdb
{:store {:backend :lmdb
         :path "/var/lib/myapp/db"}}
```

**Characteristics**:
- Memory-mapped for zero-copy reads
- Copy-on-write B+ trees
- Single filesystem only (not distributed)
- Excellent read performance
- Lower memory overhead than file backend

**Note**: The LMDB backend is available as a separate library: [datahike-lmdb](https://github.com/replikativ/datahike-lmdb), extending [konserve-lmdb](https://github.com/replikativ/lmdb).

### Memory Backend

**Use when**: Testing, development, or ephemeral data that doesn't need to survive process restarts.

```clojure
{:store {:backend :memory
         :id #uuid "550e8400-e29b-41d4-a716-446655440030"}}
```

**Characteristics**:
- No persistence - data lost on process exit
- Fastest possible performance
- Ideal for unit tests and REPL development
- Multiple databases distinguished by `:id`

## Distributed Backends

All distributed backends support **Distributed Index Space (DIS)**: multiple reader processes can directly access shared storage without database connections, enabling massive read scalability.

**Important**: Datahike uses a single-writer model. Multiple readers can access indices concurrently, but only one writer process should transact at a time. This is the same model used by Datomic, Datalevin, and XTDB.

### JDBC Backend

**Use when**: You already have PostgreSQL or another JDBC database in your infrastructure.

**Key advantage**: Leverage existing SQL database skills, backup procedures, and monitoring tools.

```clojure
;; Requires: io.replikativ/datahike-jdbc
{:store {:backend :jdbc
         :dbtype "postgresql"
         :host "db.example.com"
         :port 5432
         :dbname "datahike"
         :user "datahike"
         :password "..."}}
```

**Characteristics**:
- Use familiar SQL database operations
- Existing backup/restore procedures work
- Read scaling via DIS (readers don't need JDBC connections)
- Good for teams already operating PostgreSQL
- Available for: PostgreSQL, MySQL, H2, and others

**Note**: Available as separate library: [datahike-jdbc](https://github.com/replikativ/datahike-jdbc)

### Redis Backend

**Use when**: You need high write throughput and can tolerate weaker durability guarantees.

**Key advantage**: Excellent write performance with in-memory speed.

```clojure
;; Requires: io.replikativ/konserve-redis
{:store {:backend :redis
         :host "redis.example.com"
         :port 6379}}
```

**Characteristics**:
- Very high write throughput
- Durability depends on Redis persistence settings (RDB/AOF)
- Can lose recent writes on Redis crash
- Good for high-traffic applications where some data loss is acceptable
- Distributed reads via DIS

### S3 Backend

**Use when**: You want cost-effective distributed storage that scales to massive datasets.

**Key advantage**: Extremely scalable, pay-per-use pricing, natural fit for cloud-native architectures.

```clojure
;; Requires: io.replikativ/konserve-s3
{:store {:backend :s3
         :bucket "my-datahike-bucket"
         :region "us-east-1"}}
```

**Characteristics**:
- Unlimited scalability
- Very low storage costs (compared to databases)
- High durability (11 nines)
- Eventually consistent (may have slight read lag)
- Ideal for read-heavy workloads with occasional writes
- Works well with AWS Lambda deployments

**Performance note**: Higher latency than local storage, but cost-effective for billions of datoms.

### Google Cloud Storage (GCS) Backend

**Use when**: You're on Google Cloud Platform and want distributed storage.

**Key advantage**: Similar to S3 but optimized for GCP infrastructure.

```clojure
;; Requires: io.replikativ/konserve-gcs
{:store {:backend :gcs
         :bucket "my-datahike-bucket"
         :project-id "my-project"}}
```

**Characteristics**:
- Similar to S3 in characteristics
- Native GCP integration
- Good latency within GCP regions
- Cost-effective for large datasets

### DynamoDB Backend

**Use when**: You need low-latency distributed storage and are willing to pay premium pricing.

**Key advantage**: Single-digit millisecond latency with strong consistency options.

```clojure
;; Requires: io.replikativ/konserve-dynamodb
{:store {:backend :dynamodb
         :table "datahike"
         :region "us-east-1"}}
```

**Characteristics**:
- Very low latency
- Strong consistency available
- Higher costs than S3
- Good for latency-sensitive applications
- On-demand or provisioned capacity modes

## Browser Backend

### IndexedDB Backend

**Use when**: Building offline-capable browser applications with persistent local storage.

**Key advantage**: Durable browser-local storage with ClojureScript support.

```clojure
;; ClojureScript only
{:store {:backend :indexeddb
         :id "my-app-db"}}
```

**Characteristics**:
- Persistent across browser sessions
- ~50MB-unlimited quota (browser-dependent)
- Asynchronous API
- Often paired with TieredStore for performance

## Advanced: TieredStore

**TieredStore** creates memory hierarchies by layering backends, with faster storage in front of slower, more durable storage.

**Use cases**:
- **Browser**: Memory (fast) → IndexedDB (persistent)
- **Server**: Memory → LMDB → S3 (hot → warm → cold)
- **AWS**: LMDB (fast local) → S3 (distributed backup)

```clojure
;; Example: Fast memory cache backed by S3
{:store {:backend :tiered
         :id #uuid "550e8400-e29b-41d4-a716-446655440031"
         :frontend-config {:backend :memory
                          :id #uuid "550e8400-e29b-41d4-a716-446655440031"}
         :backend-config {:backend :s3
                         :bucket "persistent-store"
                         :region "us-east-1"
                         :id #uuid "550e8400-e29b-41d4-a716-446655440031"}
         :write-policy :write-through
         :read-policy :frontend-first}}
```

**How it works**:
- Reads check tiers in order (cache-first)
- Writes go to all tiers
- Stacking multiple tiers supported but rarely needed
- Provided by konserve's tiered store implementation

**Common patterns**:

**Browser with offline support**:
```clojure
{:store {:backend :tiered
         :id #uuid "550e8400-e29b-41d4-a716-446655440032"
         :frontend-config {:backend :memory
                          :id #uuid "550e8400-e29b-41d4-a716-446655440032"}
         :backend-config {:backend :indexeddb
                         :id #uuid "550e8400-e29b-41d4-a716-446655440032"}
         :write-policy :write-through}}
```

**AWS Lambda with S3 backing**:
```clojure
{:store {:backend :tiered
         :id #uuid "550e8400-e29b-41d4-a716-446655440033"
         :frontend-config {:backend :lmdb
                          :path "/tmp/cache"
                          :id #uuid "550e8400-e29b-41d4-a716-446655440033"}
         :backend-config {:backend :s3
                         :bucket "lambda-data"
                         :region "us-east-1"
                         :id #uuid "550e8400-e29b-41d4-a716-446655440033"}}}
```

## Backend-Specific Configuration

Each backend may have additional configuration options. See the konserve backend documentation for details:

- [konserve](https://github.com/replikativ/konserve) - Core abstraction
- [konserve-lmdb](https://github.com/replikativ/lmdb) - LMDB implementation
- [datahike-lmdb](https://github.com/replikativ/datahike-lmdb) - Datahike LMDB integration
- [datahike-jdbc](https://github.com/replikativ/datahike-jdbc) - JDBC backends
- [konserve-s3](https://github.com/replikativ/konserve-s3) - S3 backend
- [konserve-redis](https://github.com/replikativ/konserve-redis) - Redis backend

## Choosing a Backend

### For Development
→ **Memory** or **File** backend for simplicity

### For Single-Machine Production
→ **LMDB** for best performance
→ **File** if you need Unix tool integration

### For Distributed Production (Read Scaling)
→ **S3/GCS** for cost-effective scale
→ **DynamoDB** for low latency (higher cost)
→ **JDBC** if you already operate PostgreSQL

### For High Write Throughput
→ **Redis** if you can tolerate some data loss
→ **LMDB** for durable local writes
→ **DynamoDB** for distributed writes (expensive)

### For Browser Applications
→ **IndexedDB** for persistence
→ **TieredStore** (Memory → IndexedDB) for speed + durability

### For Cost Optimization
→ **File** backend with rsync for cheap backups
→ **S3** for large datasets (pennies per GB)
→ **TieredStore** to minimize expensive tier access

## Migration Between Backends

To migrate from one backend to another:

1. Export from source database:
```clojure
(require '[datahike.migrate :refer [export-db import-db]])
(export-db source-conn "/tmp/datoms-export")
```

2. Create destination database with new backend:
```clojure
(d/create-database new-config)
(def dest-conn (d/connect new-config))
```

3. Import into destination:
```clojure
(import-db dest-conn "/tmp/datoms-export")
```

The export format (CBOR) preserves all data types including binary data.

## Performance Considerations

### Read Performance
- **Fastest**: Memory, LMDB (memory-mapped)
- **Fast**: File (SSD), Redis
- **Good**: JDBC, S3 (with tiering)
- **Variable**: DynamoDB (provisioned vs on-demand)

### Write Performance
- **Fastest**: Memory, Redis
- **Fast**: LMDB, DynamoDB (provisioned)
- **Good**: File, JDBC, S3
- **Slower**: S3 (especially small writes)

### Distribution
- **No distribution**: Memory, File, LMDB (single filesystem)
- **Distributed reads**: All cloud backends via DIS
- **Single writer**: All backends (architectural constraint)

### Durability
- **None**: Memory (ephemeral)
- **Medium**: Redis (depends on persistence settings), IndexedDB
- **High**: File, LMDB, JDBC
- **Very high**: S3, GCS, DynamoDB (11 nines)

## Custom Backends

Datahike can use any konserve backend. To create a custom backend:

1. Implement the [konserve protocols](https://github.com/replikativ/konserve/blob/main/src/konserve/protocols.cljc)
2. Register your backend with konserve
3. Use it in Datahike configuration

See the [konserve documentation](https://github.com/replikativ/konserve) for details on implementing custom backends.
