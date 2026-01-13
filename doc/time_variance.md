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
