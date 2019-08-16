(ns datahike.api
  (:refer-clojure :exclude [filter])
  (:require [datahike.connector :as dc]
            [datahike.pull-api :as dp]
            [datahike.query :as dq]
            [datahike.db :as db #?@(:cljs [:refer [CurrentDB]])]
            [superv.async :refer [<?? S]]
            [datahike.impl.entity :as de]
            [clojure.spec.test.alpha :as st]
            [clojure.spec.alpha :as s])
  #?(:clj
     (:import [datahike.db HistoricalDB AsOfDB SinceDB FilteredDB]
              [datahike.impl.entity Entity]
              [java.util Date])))

(def
  ^{:arglists '([uri])
    :doc
              "Connects to a datahike database via URI. URI contains storage backend type
            and additional information for backends like database name, credentials, or
            location. Refer to the store project in the examples folder or the documention
            in the config markdown file in the doc folder.

            Usage:

              (connect \"datahike:mem://example\")"}
  connect dc/connect)

(def
  ^{:arglists '([config & opts])
    :doc
              "Creates a database using backend configuration with optional database configuration
            by providing either a URI that encodes storage backend data like database name,
            credentials, or location, or by providing a configuration hash map. Refer to the
            store project in the examples folder or the documention in the config markdown
            file of the doc folder.

            Usage:

              Create an empty database with default configuration:

                `(create-database \"datahike:mem://example\")`

              Initial data after creation may be added using the `:initial-tx` parameter:

                (create-database \"datahike:mem://example\" :initial-tx [{:db/ident :name :db/valueType :db.type/string :db.cardinality/one}])

              Datahike has a strict schema validation (schema-on-write) policy per default,
              that only allows data that has been defined via schema definition in advance.
              You may influence this behaviour using the `:schema-on-read` parameter:

                (create-database \"datahike:mem://example\" :schema-on-read true)

              By storing historical data in a separate index, datahike has the capability of
              querying data from any point in time. You may control this feature using the
              `:temporal-index` parameter:

                (create-database \"datahike:mem://example\" :temporal-index false)"}
  create-database
  dc/create-database)

(def ^{:arglists '([uri])
       :doc      "Deletes a database at given URI."}
  delete-database
  dc/delete-database)

(def ^{:arglists '([conn tx-data])
       :doc      "Same as [[transact!]] but returns realized value directly."}
  transact
  dc/transact)

(def ^{:arglists '([conn tx-data tx-meta])
       :doc      "Applies transaction the underlying database value and atomically updates connection reference to point to the result of that transaction, new db value."}
  transact!
  dc/transact!)

(def ^{:arglists '([conn])
       :doc      "Releases a database connection"}
  release dc/release)

(def ^{:arglists '([db selector eid])
       :doc      "Fetches data from database using recursive declarative description. See [docs.datomic.com/on-prem/pull.html](https://docs.datomic.com/on-prem/pull.html).

             Unlike [[entity]], returns plain Clojure map (not lazy).

             Usage:

                 (pull db [:db/id, :name, :likes, {:friends [:db/id :name]}] 1)
                 ; => {:db/id   1,
                 ;     :name    \"Ivan\"
                 ;     :likes   [:pizza]
                 ;     :friends [{:db/id 2, :name \"Oleg\"}]"}
  pull dp/pull)

(def ^{:arglists '([db selector eids])
       :doc      "Same as [[pull]], but accepts sequence of ids and returns sequence of maps.

             Usage:

             ```
             (pull-many db [:db/id :name] [1 2])
             ; => [{:db/id 1, :name \"Ivan\"}
             ;     {:db/id 2, :name \"Oleg\"}]
             ```"}
  pull-many dp/pull-many)


(defmulti q
  "Executes a datalog query. See [docs.datomic.com/on-prem/query.html](https://docs.datomic.com/on-prem/query.html).

   Usage:

   ```
   (q '[:find ?value
        :where [_ :likes ?value]]
      db)
   ; => #{[\"fries\"] [\"candy\"] [\"pie\"] [\"pizza\"]}
   ```"
  {:arglists '([query & inputs])}
  (fn [query & inputs] (type query)))

(defmethod q clojure.lang.PersistentVector
  [query & inputs]
  (apply dq/q query inputs))

(defmethod q clojure.lang.PersistentArrayMap
  [query-map & arg-list]
  (let [query (if (contains? query-map :query)
                (:query query-map)
                query-map)
        args (if (contains? query-map :args)
               (:args query-map)
               arg-list)
        query-vector (->> query
                          (mapcat (fn [[k v]]
                                    (into [k] v)))
                          vec)]
    (apply dq/q query args)))

(defn seek-datoms
  "Similar to [[datoms]], but will return datoms starting from specified components and including rest of the database until the end of the index.

   If no datom matches passed arguments exactly, iterator will start from first datom that could be considered “greater” in index order.

   Usage:

       (seek-datoms db :eavt 1)
       ; => (#datahike/Datom [1 :friends 2]
       ;     #datahike/Datom [1 :likes \"fries\"]
       ;     #datahike/Datom [1 :likes \"pizza\"]
       ;     #datahike/Datom [1 :name \"Ivan\"]
       ;     #datahike/Datom [2 :likes \"candy\"]
       ;     #datahike/Datom [2 :likes \"pie\"]
       ;     #datahike/Datom [2 :likes \"pizza\"])

       (seek-datoms db :eavt 1 :name)
       ; => (#datahike/Datom [1 :name \"Ivan\"]
       ;     #datahike/Datom [2 :likes \"candy\"]
       ;     #datahike/Datom [2 :likes \"pie\"]
       ;     #datahike/Datom [2 :likes \"pizza\"])

       (seek-datoms db :eavt 2)
       ; => (#datahike/Datom [2 :likes \"candy\"]
       ;     #datahike/Datom [2 :likes \"pie\"]
       ;     #datahike/Datom [2 :likes \"pizza\"])

       ; no datom [2 :likes \"fish\"], so starts with one immediately following such in index
       (seek-datoms db :eavt 2 :likes \"fish\")
       ; => (#datahike/Datom [2 :likes \"pie\"]
       ;     #datahike/Datom [2 :likes \"pizza\"])"
  ([db index] {:pre [(db/db? db)]} (db/-seek-datoms db index []))
  ([db index c1] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1]))
  ([db index c1 c2] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2]))
  ([db index c1 c2 c3] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3 c4])))

(def ^:private last-tempid (atom -1000000))

(defn tempid
  "Allocates and returns an unique temporary id (a negative integer). Ignores `part`. Returns `x` if it is specified.

   Exists for Datomic API compatibility. Prefer using negative integers directly if possible."
  ([part]
   (if (= part :db.part/tx)
     :db/current-tx
     (swap! last-tempid dec)))
  ([part x]
   (if (= part :db.part/tx)
     :db/current-tx
     x)))

(def ^{:arglists '([db eid])
       :doc      "Retrieves an entity by its id from database. Entities are lazy map-like structures to navigate DataScript database content.

             For `eid` pass entity id or lookup attr:

                 (entity db 1)
                 (entity db [:unique-attr :value])

             If entity does not exist, `nil` is returned:

                 (entity db -1) ; => nil

             Creating an entity by id is very cheap, almost no-op, as attr access is on-demand:

                 (entity db 1) ; => {:db/id 1}

             Entity attributes can be lazily accessed through key lookups:

                 (:attr (entity db 1)) ; => :value
                 (get (entity db 1) :attr) ; => :value

             Cardinality many attributes are returned sequences:

                 (:attrs (entity db 1)) ; => [:v1 :v2 :v3]

             Reference attributes are returned as another entities:

                 (:ref (entity db 1)) ; => {:db/id 2}
                 (:ns/ref (entity db 1)) ; => {:db/id 2}

             References can be walked backwards by prepending `_` to name part of an attribute:

                 (:_ref (entity db 2)) ; => [{:db/id 1}]
                 (:ns/_ref (entity db 2)) ; => [{:db/id 1}]

             Reverse reference lookup returns sequence of entities unless attribute is marked as `:db/component`:

                 (:_component-ref (entity db 2)) ; => {:db/id 1}

             Entity gotchas:

             - Entities print as map, but are not exactly maps (they have compatible get interface though).
             - Entities are effectively immutable “views” into a particular version of a database.
             - Entities retain reference to the whole database.
             - You can’t change database through entities, only read.
             - Creating an entity by id is very cheap, almost no-op (attributes are looked up on demand).
             - Comparing entities just compares their ids. Be careful when comparing entities taken from differenct dbs or from different versions of the same db.
             - Accessed entity attributes are cached on entity itself (except backward references).
             - When printing, only cached attributes (the ones you have accessed before) are printed. See [[touch]]."}
  entity de/entity)

(defn entity-db
  "Returns a db that entity was created from."
  [^Entity entity]
  {:pre [(de/entity? entity)]}
  (.-db entity))

(defn is-filtered
  "Returns `true` if this database was filtered using [[filter]], `false` otherwise."
  [x]
  (instance? FilteredDB x))

(defn filter
  "Returns a view over database that has same interface but only includes datoms for which the `(pred db datom)` is true. Can be applied multiple times.

   Filtered DB gotchas:

   - All operations on filtered database are proxied to original DB, then filter pred is applied.
   - Not cached. You pay filter penalty every time.
   - Supports entities, pull, queries, index access.
   - Does not support hashing of DB.
   - Does not support [[with]] and [[db-with]]."
  [db pred]
  {:pre [(db/db? db)]}
  (if (is-filtered db)
    (let [^FilteredDB fdb db
          orig-pred (.-pred fdb)
          orig-db (.-unfiltered-db fdb)]
      (FilteredDB. orig-db #(and (orig-pred %) (pred orig-db %))))
    (FilteredDB. db #(pred db %))))

(defn- is-temporal? [x]
  (or (instance? HistoricalDB x)
      (instance? AsOfDB x)
      (instance? SinceDB x)))

(defn with
  "Same as [[transact!]], but applies to an immutable database value. Returns transaction report (see [[transact!]])."
  ([db tx-data] (with db tx-data nil))
  ([db tx-data tx-meta]
   {:pre [(db/db? db)]}
   (if (or (is-filtered db) (is-temporal? db))
     (throw (ex-info "Filtered DB cannot be modified" {:error :transaction/filtered}))
     (db/transact-tx-data (db/map->TxReport
                            {:db-before db
                             :db-after  db
                             :tx-data   []
                             :tempids   {}
                             :tx-meta   tx-meta}) tx-data))))

(defn db
  "Returns the current state of the database you may interact with."
  [conn]
  @conn)

(defn history
  "Returns the full historical state of the database you may interact with."
  [db]
  (if (db/-temporal-index? db)
    (HistoricalDB. db)
    (throw (ex-info "as-of is only allowed on temporal indexed dbs" {:config (db/-config db)}))))

(defn- date? [d]
  #?(:cljs (instance? js/Date d)
     :clj  (instance? Date d)))

(defn as-of
  "Returns the database state at given Date (you may use either java.util.Date or Epoch Time as long)."
  [db date]
  {:pre [(or (int? date) (date? date))]}
  (if (db/-temporal-index? db)
    (AsOfDB. db (if (date? date) date (java.util.Date. ^long date)))
    (throw (ex-info "as-of is only allowed on temporal indexed dbs"))))

(defn since
  "Returns the database state since a given Date (you may use either java.util.Date or Epoch Time as long).
  Be aware: the database contains only the datoms that were added since the date."
  [db date]
  {:pre [(or (int? date) (date? date))]}
  (if (db/-temporal-index? db)
    (SinceDB. db (if (date? date) date (java.util.Date. ^long date)))
    (throw (ex-info "since is only allowed on temporal indexed dbs"))))
