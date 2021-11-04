# Time Variance

For the purpose of auditing and analytics modern business information systems
need to be time variant. This
means, they should have the ability to store, track and query data entities that
change over time. As a [temporal database](https://en.wikipedia.org/wiki/Temporal_database),
Datahike tracks by default the transaction time for each entity by using the
`:db/txInstant` attribute in the meta entity that is added to each
transaction. This uni-temporal approach allows different perspectives of the
data present in the index. Entities can be searched either at the [current point
in time](#db), [at a specific point in time](#as-of), [over the whole database
existence](#history), or [since a specific point in time](#since).

If the database does not require to be time variant you can choose to ignore the
temporal data and set the `keep-history?` parameter to `false` at database
creation like so:

```clojure
(require '[datahike.api :as d])

(d/create-database {:store {:backend :mem :id "time-invariant"} :keep-history? true})

```

Have a look at the `examples/time-travel` namespace in the examples project for more example queries and
interactions.

## DB

The most common perspective of your data is the current state of the
system. Use `db` for this view. The following example shows a simple interaction:

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
              
(def cfg {:store {:backend :mem :id "current-db"} :initial-tx schema})

;; create our temporal database
(d/create-database cfg)

(def conn (d/connect cfg))

;; add first data
(d/transact conn [{:name "Alice" :age 25}])

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

(d/q query @conn)
;; => #{["Alice" 25]}

;; update the entity
(d/transact conn [{:db/id [:name "Alice"] :age 30}])

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
(def cfg {:store {:backend :mem :id "as-of-db"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))


;; add first data
(d/transact conn [{:name "Alice" :age 25}])

(def first-date (java.util.Date.))

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

(d/q query  @conn)
;; => #{["Alice" 25]}

;; update the entity
(d/transact conn [{:db/id [:name "Alice"] :age 30}])

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
(def cfg {:store {:backend :mem :id "history-db"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))

;; add first data
(d/transact conn [{:name "Alice" :age 25}])

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

;; history should have only one entry
(d/q query (d/history @conn))
;; => #{["Alice" 25]}

;; update the entity
(d/transact conn [{:db/id [:name "Alice"] :age 30}])

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
(def cfg {:store {:backend :mem :id "since-db"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))


;; add first data
(d/transact conn [{:name "Alice" :age 25}])

(def first-date (java.util.Date.))

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

(d/q query @conn)
;; => #{["Alice" 25]}

;; update the entity
(d/transact conn [{:db/id [:name "Alice"] :age 30}])

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
(def cfg {:store {:backend :mem :id "meta-db"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))

;; add first data
(d/transact conn [{:name "Alice" :age 25}])

;; let's find all transaction dates, should be two: one for the schema and one
;; for the first data
(d/q '[:find ?t :where [_ :db/txInstant ?t]] @conn)
;; => #{[#inst "2019-08-16T11:40:28.794-00:00"] [#inst "2019-08-16T11:40:26.587-00:00"]}

;; you might join over the tx id to get the date of any transaction
(d/q '[:find ?n ?t :where [_ :name ?n ?tx] [?tx :db/txInstant ?t]] @conn)
;; => #{["Alice" #inst "2019-08-16T11:40:28.794-00:00"]}
```

## Data Purging

Since retraction only moves the datoms from the current index to a history, data
is in that way never completely deleted. If your use case (for instance related
to GDPR compliance) requires complete data removal use the `db.purge` functions
available in transactions:

- `:db/purge`: removes a datom with given entity identifier, attribute and value
- `:db.purge/attribute`: removes attribute datoms given an identifier and attribute name
- `:db.purge/entity`: removes all datoms related to an entity given an entity identifier
- `:db.history.purge/before`: removes all datoms from historical data before given date, useful for cleanup after some retention period

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
(def cfg {:store {:backend :mem :id "purge-db"} :initial-tx schema})

(d/create-database cfg)

(def conn (d/connect cfg))


;; add data
(d/transact conn [{:name "Alice" :age 25}])

;; define simple query for name and age
(def query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]])

(d/q query  @conn)
;; => #{["Alice" 25]}

(d/transact [[:db.purge/entity [:name "Alice"]]])

;; data was removed from current database view
(d/q query  @conn)
;; => #{}

;; data was also removed from history
(d/q query (history @conn))
;; => #{}
```

Have a look at the the `time-travel` namespace in the examples project for
more examples.

Be aware: these functions are only available if temporal index is active. Don't
use these functions to remove data by default.
