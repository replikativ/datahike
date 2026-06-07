# Time Variance

As a [temporal database](https://en.wikipedia.org/wiki/Temporal_database), Datahike tracks transaction time for every change, enabling auditing, analytics, and time-travel queries. Each transaction records `:db/txInstant`, allowing you to view data at different points in time.

## Time Travel Views

| View | Function | Returns | Use Case |
|------|----------|---------|----------|
| **Current** | `@conn` or `(d/db conn)` | Latest state | Normal queries |
| **As-of** | `(d/as-of db date)` | State at specific time | "What did the data look like on July 1st?" |
| **History** | `(d/history db)` | All versions (current + historical) | Audit trails, change analysis |
| **Since** | `(d/since db date)` | Changes after specific time | "What changed since yesterday?" |

**Related:** For git-like branching and merging of database snapshots, see [Versioning](versioning.md). For removing old historical data from storage, see [Garbage Collection](gc.md).

## Disabling History

If you don't need time-travel queries, disable history tracking to save storage:

```clojure
(require '[datahike.api :as d])

(d/create-database {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440000"} :keep-history? false})
```

**Trade-off:** Saves storage and improves write performance, but removes as-of/history/since queries and purging capabilities.

## Setup for Examples

All examples below use this shared setup:

```clojure
(require '[datahike.api :as d])

;; Simple schema for person data
(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/index true
              :db/cardinality :db.cardinality/one}
             {:db/ident :age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}])

(def cfg {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440001"} :initial-tx schema})

(d/create-database cfg)
(def conn (d/connect cfg))

;; Query to find names and ages
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])
```

## DB (Current State)

`@conn` or `(d/db conn)` returns the current state - the most common view for queries:

```clojure

;; add first data
(d/transact conn {:tx-data [{:name "Alice" :age 25}]})

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

(d/q query @conn)
;; => #{["Alice" 25]}

;; update the entity
(d/transact conn {:tx-data [{:db/id [:name "Alice"] :age 30}]})

;; `db` reflects the latest state of the database
(d/q query @conn)
;; => #{["Alice" 30]}
```

## As-Of

You can query the database at a specific point in time using `as-of`:

```clojure
(require '[datahike.api :as d])

;; define simple schema
(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/index true
              :db/cardinality :db.cardinality/one}
             {:db/ident :age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}])

;; create our temporal database
(def cfg {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440003"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))


;; add first data
(d/transact conn {:tx-data [{:name "Alice" :age 25}]})

(def first-date (java.util.Date.))

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

(d/q query  @conn)
;; => #{["Alice" 25]}

;; update the entity
(d/transact conn {:tx-data [{:db/id [:name "Alice"] :age 30}]})

;; let's compare the current and the as-of value:
(d/q query  @conn)
;; => #{["Alice" 30]}

(d/q query (d/as-of @conn first-date))
;; => #{["Alice" 25]}
```

## History

For querying all data over the whole time span you may use `history` which joins
current and all historical data:

```clojure
(require '[datahike.api :as d])

;; define simple schema
(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/index true
              :db/cardinality :db.cardinality/one}
             {:db/ident :age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}])

;; create our temporal database
(def cfg {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440004"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))

;; add first data
(d/transact conn {:tx-data [{:name "Alice" :age 25}]})

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

;; history should have only one entry
(d/q query (d/history @conn))
;; => #{["Alice" 25]}

;; update the entity
(d/transact conn {:tx-data [{:db/id [:name "Alice"] :age 30}]})

;; both entries are present
(d/q query (d/history @conn))
;; => #{["Alice" 30] ["Alice" 25]}
```

## Since

Changes since a specific point in time can be searched by using the `since`
database:

```clojure
(require '[datahike.api :as d])

;; define simple schema
(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/index true
              :db/cardinality :db.cardinality/one}
             {:db/ident :age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}])

;; create our temporal database
(def cfg {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440005"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))


;; add first data
(d/transact conn {:tx-data [{:name "Alice" :age 25}]})

(def first-date (java.util.Date.))

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

(d/q query @conn)
;; => #{["Alice" 25]}

;; update the entity
(d/transact conn {:tx-data [{:db/id [:name "Alice"] :age 30}]})

;; let's compare the current and the as-of value:
(d/q query @conn)
;; => #{["Alice" 30]}

;; now we want to know any additions after a specific time
(d/q query (d/since @conn first-date))
;; => {}, because :name was transacted before the first date

;; let's build a query where we use the latest db to find the name and the since db to find out who's age changed
(d/q '[:find ?n ?a
       :in $ $since
       :where
       [$ ?e :name ?n]
       [$since ?e :age ?a]]
     @conn
     (d/since @conn first-date))
```

## Meta Entity

With each transaction a meta entity is added to the index that stores the
current point in time in the `:db/txInstant` attribute.

With this data present in the current index, you can search and analyze them for
your purposes.

```clojure
(require '[datahike.api :as d])

;; define simple schema
(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/index true
              :db/cardinality :db.cardinality/one}
             {:db/ident :age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}])

;; create our temporal database
(def cfg {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440006"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))

;; add first data
(d/transact conn {:tx-data [{:name "Alice" :age 25}]})

;; let's find all transaction dates, should be two: one for the schema and one
;; for the first data
(d/q '[:find ?t :where [_ :db/txInstant ?t]] @conn)
;; => #{[#inst "2019-08-16T11:40:28.794-00:00"] [#inst "2019-08-16T11:40:26.587-00:00"]}

;; you might join over the tx id to get the date of any transaction
(d/q '[:find ?n ?t :where [_ :name ?n ?tx] [?tx :db/txInstant ?t]] @conn)
;; => #{["Alice" #inst "2019-08-16T11:40:28.794-00:00"]}
```

### `:db/txInstant` is strictly monotonic

Auto-stamped `:db/txInstant` values are guaranteed to be strictly
increasing across a connection's tx-log — even when multiple
transactions commit within the same wall-clock millisecond. The
allocator returns `max(get-date, prev-tx-instant + 1ms)`; under
write bursts the stamp advances 1ms per tx until wall-clock catches
up. This matches Datomic's contract and removes the `d/as-of
<Date>` tied-instant ambiguity at the source: an instant resolves
to one unique snapshot, never two.

For exact temporal cuts ("snapshot just after tx X") use the tx-id
directly — tx-ids are the canonical precise ordering primitive,
while wall-clock instants remain useful for human-readable queries
("what did the world look like at 2024-12-31 23:59?").

User-provided `:db/txInstant` (via `:tx-meta {:db/txInstant
<date>}`) still wins over the allocator default. This preserves
the historical-import pattern: writing past events with their
original dates — and pinned-clock test fixtures — both keep
working unchanged.

A useful side-effect: a pinned wall-clock turns into a *logical
clock* automatically. Bind `datahike.tools/get-date` to a constant
(e.g., for replay or regulator audits on a fresh database) and the
allocator produces deterministic `[pinned, pinned+1ms, pinned+2ms,
…]` stamps — fully reproducible without any separate logical-clock
machinery. (This holds when the pin dominates the prev-tx-instant,
i.e., the pin is at or after the most recently committed tx.)

The allocator (`datahike.db.transaction/next-tx-instant`) is itself
`^:dynamic`, so a future caller that wants a different allocation
policy (hybrid logical clock, microsecond `Instant`, …) can swap
it in via `binding` without touching the call site.

## Data Purging

**Retraction vs Purging:** Normal retractions preserve data in history. Purging permanently deletes data from both current and historical indices.

**When to purge:** Privacy regulations (GDPR, HIPAA, CCPA) requiring "right to deletion", sensitive data removal, or retention policy enforcement.

**⚠️ Warning:** Purging is permanent and irreversible. Use only when legally required or explicitly needed.

### Purge Operations

- **`:db/purge`** - Remove specific datom (entity, attribute, value)
- **`:db.purge/attribute`** - Remove all values for an attribute on an entity
- **`:db.purge/entity`** - Remove entire entity and all its attributes
- **`:db.history.purge/before`** - Remove all historical data before a date (retention policy cleanup)

```clojure
(require '[datahike.api :as d])

;; define simple schema
(def schema [{:db/ident :name
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/index true
              :db/cardinality :db.cardinality/one}
             {:db/ident :age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}])

;; create our temporal database
(def cfg {:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440007"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))


;; add data
(d/transact conn {:tx-data [{:name "Alice" :age 25}]})

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

(d/q query  @conn)
;; => #{["Alice" 25]}

(d/transact conn {:tx-data [[:db.purge/entity [:name "Alice"]]]})

;; data was removed from current database view
(d/q query  @conn)
;; => #{}

;; data was also removed from history
(d/q query (d/history @conn))
;; => #{}
```

**Requirements for purging:**
- History must be enabled (`:keep-history? true`)
- Cannot purge schema attributes
- Use retractions for normal data lifecycle - reserve purging for compliance requirements

### Purge and storage

The example above shows the new commit's indices: the datom is gone from `@conn` and from `(d/history @conn)`. Layers below the new commit need explicit attention:

- **Pre-purge commit.** Each commit owns its own index roots. The commit that existed *before* the purge still references the old tree nodes containing the datom — `(d/commit-as-db conn <pre-purge-uuid>)` still sees the purged data until garbage collection sweeps that intermediate commit. The eviction step is `d/gc-storage` with a grace-period cutoff old enough to drop the pre-purge commit; see [Garbage Collection](./gc.md). Branch heads are always kept; only intermediate commits get swept.

- **Secondary indices.** Purge routes a retraction event in-transaction to every secondary index covering an affected attribute, the same way `:db/retract` does. After purge, Scriptum full-text search, Proximum KNN, and Stratum columnar aggregates no longer return the purged datom on the live store. Scriptum has a filesystem caveat (Lucene tombstones-until-segment-merge); see [Secondary indices: purge propagation](./secondary-indices.md#purge-propagation).

- **Multiple branches.** Purge is a transaction on one branch. If the same datom is reachable from another branch's head, or from its commits inside the GC window, purge that branch too. Structural sharing means the bytes are physically one copy in konserve, but the *paths to reach* them are independent.

- **Backups and storage-layer history.** `purge` operates on the live store. Backups of the konserve store, S3 object versioning, ZFS snapshots, git-backed konserve backends, and logical replicas all retain pre-purge state on their own terms; addressing those is operational policy outside Datahike.
