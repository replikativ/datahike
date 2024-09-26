(ns ^:no-doc datahike.core
  (:refer-clojure :exclude [filter])
  (:require
   [datahike.constants :as dc]
   [datahike.datom :as dd]
   [datahike.db :as db #?@(:cljs [:refer [FilteredDB]])]
   [datahike.db.interface :as dbi]
   [datahike.db.transaction :as dbt]
   [datahike.db.utils :as dbu]
   [datahike.impl.entity :as de]
   [datahike.pull-api :as dp]
   [datahike.query :as dq]
   [datahike.tools :as tools])
  #?(:clj
     (:import
      [datahike.db FilteredDB]
      [datahike.impl.entity Entity]
      [java.util UUID]
      (clojure.lang IDeref IBlockingDeref IAtom IPending))))

(def ^:const ^:no-doc tx0 dc/tx0)

; Entities

(def ^{:arglists '([db eid])}

  entity de/entity)

(def ^{:arglists '([db eid])
       :doc      "Given lookup ref `[unique-attr value]`, returns numeric entity id.

             If entity does not exist, returns `nil`.

             For numeric `eid` returns `eid` itself (does not check for entity existence in that case)."}
  entid dbu/entid)

(defn entity-db
  "Returns a db that entity was created from."
  [^Entity entity]
  {:pre [(de/entity? entity)]}
  (.-db entity))

(def ^{:arglists '([e])
       :doc      "Forces all entity attributes to be eagerly fetched and cached. Only usable for debug output.

             Usage:

             ```
             (entity db 1) ; => {:db/id 1}
             (touch (entity db 1)) ; => {:db/id 1, :dislikes [:pie], :likes [:pizza]}
             ```"}
  touch de/touch)

; Pull

(def ^{:arglists '([db selector eid])}
  pull dp/pull)

(def ^{:arglists '([db selector eids])}
  pull-many dp/pull-many)

; Query

(def ^{:arglists '([query & inputs])}
  q dq/q)

; Creating DB

(def ^{:arglists '([] [schema] [schema config])
       :doc      "Creates an empty database with an optional schema and configuration.

             Usage:
             ```
             (empty-db) ; => #datahike/DB {:schema {}, :datoms []}

             (empty-db {:likes {:db/cardinality :db.cardinality/many}})
             ; => #datahike/DB {:schema {:likes {:db/cardinality :db.cardinality/many}}
             ;                    :datoms []}

             (empty-db {} {:keep-history? false :index datahike.index.hitchhiker-tree :schema-flexibility :write})
             ```"}
  empty-db db/empty-db)

(def ^{:arglists '([x])
       :doc      "Returns `true` if the given value is an immutable database, `false` otherwise."}
  db? dbu/db?)

(def ^{:arglists '([e a v] [e a v tx] [e a v tx added])
       :doc      "Low-level fn to create raw datoms.

             Optionally with transaction id (number) and `added` flag (`true` for addition, `false` for retraction).

             See also [[init-db]]."}
  datom dd/datom)

(def ^{:arglists '([x])
       :doc      "Returns `true` if the given value is a datom, `false` otherwise."}
  datom? dd/datom?)

(def ^{:arglists '([datoms] [datoms schema] [datoms schema config])
       :doc      "Low-level fn for creating database quickly from a trusted sequence of datoms.

             Does no validation on inputs, so `datoms` must be well-formed and match schema.

             Used internally in db (de)serialization. See also [[datom]]."}
  init-db db/init-db)

; Filtered db

(defn is-filtered
  "Returns `true` if this database was filtered using [[filter]], `false` otherwise."
  [x]
  (instance? FilteredDB x))

(defn filter
  [db pred]
  {:pre [(dbu/db? db)]}
  (if (is-filtered db)
    (let [^FilteredDB fdb db
          orig-pred (.-pred fdb)
          orig-db (.-unfiltered-db fdb)]
      (FilteredDB. orig-db #(and (orig-pred %) (pred orig-db %))))
    (FilteredDB. db #(pred db %))))

; Changing DB

(defn with
  "Same as [[transact!]], but applies to an immutable database value. Returns transaction report (see [[transact!]])."
  ([db tx-data] (with db tx-data nil))
  ([db tx-data tx-meta]
   {:pre [(dbu/db? db)]}
   (if (is-filtered db)
     (throw (ex-info "Filtered DB cannot be modified" {:error :transaction/filtered}))
     (dbt/transact-tx-data (db/map->TxReport
                            {:db-before db
                             :db-after  db
                             :tx-data   []
                             :tempids   {}
                             :tx-meta   tx-meta}) tx-data))))

(defn load-entities-with [db entities tx-meta]
  (dbt/transact-entities-directly
   (db/map->TxReport {:db-before db
                      :db-after  db
                      :tx-data   []
                      :tempids   {}
                      :tx-meta   tx-meta})
   entities))

(defn db-with
  "Applies transaction to an immutable db value, returning new immutable db value. Same as `(:db-after (with db tx-data))`."
  [db tx-data]
  {:pre [(dbu/db? db)]}
  (:db-after (with db tx-data)))

; Index lookups

(defn datoms
  ([db index] {:pre [(dbu/db? db)]} (dbi/datoms db index []))
  ([db index c1] {:pre [(dbu/db? db)]} (dbi/datoms db index [c1]))
  ([db index c1 c2] {:pre [(dbu/db? db)]} (dbi/datoms db index [c1 c2]))
  ([db index c1 c2 c3] {:pre [(dbu/db? db)]} (dbi/datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(dbu/db? db)]} (dbi/datoms db index [c1 c2 c3 c4])))

(defn seek-datoms
  ([db index] {:pre [(dbu/db? db)]} (dbi/seek-datoms db index []))
  ([db index c1] {:pre [(dbu/db? db)]} (dbi/seek-datoms db index [c1]))
  ([db index c1 c2] {:pre [(dbu/db? db)]} (dbi/seek-datoms db index [c1 c2]))
  ([db index c1 c2 c3] {:pre [(dbu/db? db)]} (dbi/seek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(dbu/db? db)]} (dbi/seek-datoms db index [c1 c2 c3 c4])))

(defn rseek-datoms
  "Same as [[seek-datoms]], but goes backwards until the beginning of the index."
  ([db index] {:pre [(dbu/db? db)]} (dbi/rseek-datoms db index []))
  ([db index c1] {:pre [(dbu/db? db)]} (dbi/rseek-datoms db index [c1]))
  ([db index c1 c2] {:pre [(dbu/db? db)]} (dbi/rseek-datoms db index [c1 c2]))
  ([db index c1 c2 c3] {:pre [(dbu/db? db)]} (dbi/rseek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(dbu/db? db)]} (dbi/rseek-datoms db index [c1 c2 c3 c4])))

(defn index-range
  [db attr start end]
  {:pre [(dbu/db? db)]}
  (dbi/index-range db attr start end))

;; Conn

(defn conn?
  "Returns `true` if this is a connection to a DataScript db, `false` otherwise."
  [conn]
  (and #?(:clj  (instance? IDeref conn)
          :cljs (satisfies? cljs.core/IDeref conn))
       (dbu/db? @conn)))

(defn- atom? [a]
  #?(:cljs (instance? Atom a)
     :clj  (instance? IAtom a)))

(defn listen!
  "Listen for changes on the given connection. Whenever a transaction is applied to the database via [[transact!]], the callback is called
   with the transaction report. `key` is any opaque unique value.

   Idempotent. Calling [[listen!]] with the same twice will override old callback with the new value.

   Returns the key under which this listener is registered. See also [[unlisten!]]."
  ([conn callback] (listen! conn (rand) callback))
  ([conn key callback]
   {:pre [(conn? conn) (atom? (:listeners (meta conn)))]}
   (swap! (:listeners (meta conn)) assoc key callback)
   key))

(defn unlisten!
  "Removes registered listener from connection. See also [[listen!]]."
  [conn key]
  {:pre [(conn? conn)
         (atom? (:listeners (meta conn)))]}
  (swap! (:listeners (meta conn)) dissoc key))

;; Datomic compatibility layer

(def ^:private last-tempid (atom -1000000))

(defn tempid
  "Allocates and returns a unique temporary id (a negative integer). Ignores `part`. Returns `x` if it is specified.

   Exists for Datomic API compatibility. Prefer using negative integers directly if possible."
  ([part]
   (if (= part :db.part/tx)
     :db/current-tx
     (swap! last-tempid dec)))
  ([part x]
   (if (= part :db.part/tx)
     :db/current-tx
     x)))

(defn resolve-tempid
  "Does a lookup in tempids map, returning an entity id that tempid was resolved to.
   
   Exists for Datomic API compatibility. Prefer using map lookup directly if possible."
  [_db tempids tempid]
  (get tempids tempid))

(defn db
  "Returns the underlying immutable database value from a connection.
   
   Exists for Datomic API compatibility. Prefer using `@conn` directly if possible."
  [conn]
  {:pre [(conn? conn)]}
  @conn)

(defn- rand-bits [pow]
  (rand-int (bit-shift-left 1 pow)))

#?(:cljs
   (defn- to-hex-string [n l]
     (let [s (.toString n 16)
           c (count s)]
       (cond
         (> c l) (subs s 0 l)
         (< c l) (str (apply str (repeat (- l c) "0")) s)
         :else s))))

;; for backwards compatibility
(def squuid tools/squuid)

(def squuid-time-millis tools/squuid-time-millis)
