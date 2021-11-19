(ns ^:no-doc datahike.core
  (:refer-clojure :exclude [filter])
  (:require
   [datahike.db :as db #?@(:cljs [:refer [FilteredDB]])]
   [datahike.datom :as dd]
   [datahike.pull-api :as dp]
   [datahike.query :as dq]
   [datahike.constants :as dc]
   [datahike.impl.entity :as de])
  #?(:clj
     (:import
      [datahike.db FilteredDB]
      [datahike.impl.entity Entity]
      [java.util UUID])))

(def ^:const ^:no-doc tx0 dc/tx0)

; Entities

(def ^{:arglists '([db eid])}

  entity de/entity)

(def ^{:arglists '([db eid])
       :doc      "Given lookup ref `[unique-attr value]`, returns numberic entity id.

             If entity does not exist, returns `nil`.

             For numeric `eid` returns `eid` itself (does not check for entity existence in that case)."}
  entid db/entid)

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
  db? db/db?)

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
  {:pre [(db/db? db)]}
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
   {:pre [(db/db? db)]}
   (if (is-filtered db)
     (throw (ex-info "Filtered DB cannot be modified" {:error :transaction/filtered}))
     (db/transact-tx-data (db/map->TxReport
                           {:db-before db
                            :db-after  db
                            :tx-data   []
                            :tempids   {}
                            :tx-meta   tx-meta}) tx-data))))

(defn load-entities-with [db entities]
  (db/transact-entities-directly
   (db/map->TxReport {:db-before db
                      :db-after  db
                      :tx-data   []
                      :tempids   {}
                      :tx-meta []})
   entities))

(defn db-with
  "Applies transaction to an immutable db value, returning new immutable db value. Same as `(:db-after (with db tx-data))`."
  [db tx-data]
  {:pre [(db/db? db)]}
  (:db-after (with db tx-data)))

; Index lookups

(defn datoms
  ([db index] {:pre [(db/db? db)]} (db/-datoms db index []))
  ([db index c1] {:pre [(db/db? db)]} (db/-datoms db index [c1]))
  ([db index c1 c2] {:pre [(db/db? db)]} (db/-datoms db index [c1 c2]))
  ([db index c1 c2 c3] {:pre [(db/db? db)]} (db/-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-datoms db index [c1 c2 c3 c4])))

(defn seek-datoms
  ([db index] {:pre [(db/db? db)]} (db/-seek-datoms db index []))
  ([db index c1] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1]))
  ([db index c1 c2] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2]))
  ([db index c1 c2 c3] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-seek-datoms db index [c1 c2 c3 c4])))

(defn rseek-datoms
  "Same as [[seek-datoms]], but goes backwards until the beginning of the index."
  ([db index] {:pre [(db/db? db)]} (db/-rseek-datoms db index []))
  ([db index c1] {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1]))
  ([db index c1 c2] {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2]))
  ([db index c1 c2 c3] {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2 c3]))
  ([db index c1 c2 c3 c4] {:pre [(db/db? db)]} (db/-rseek-datoms db index [c1 c2 c3 c4])))

(defn index-range
  [db attr start end]
  {:pre [(db/db? db)]}
  (db/-index-range db attr start end))

;; Conn

(defn conn?
  "Returns `true` if this is a connection to a DataScript db, `false` otherwise."
  [conn]
  (and #?(:clj  (instance? clojure.lang.IDeref conn)
          :cljs (satisfies? cljs.core/IDeref conn))
       (db/db? @conn)))

(defn conn-from-db
  "Creates a mutable reference to a given immutable database. See [[create-conn]]."
  [db]
  (atom db :meta {:listeners (atom {})}))

(defn conn-from-datoms
  "Creates an empty DB and a mutable reference to it. See [[create-conn]]."
  ([datoms] (conn-from-db (init-db datoms)))
  ([datoms schema] (conn-from-db (init-db datoms schema))))

(defn create-conn
  "Creates a mutable reference (a “connection”) to an empty immutable database.

   Connections are lightweight in-memory structures (~atoms) with direct support of transaction listeners ([[listen!]], [[unlisten!]]) and other handy DataScript APIs ([[transact!]], [[reset-conn!]], [[db]]).

   To access underlying immutable DB value, deref: `@conn`."
  ([] (conn-from-db (empty-db)))
  ([schema] (conn-from-db (empty-db schema))))

(defn ^:no-doc -transact! [conn tx-data tx-meta]
  {:pre [(conn? conn)]}
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (with db tx-data tx-meta)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn -load-entities! [conn entities]
  (let [report (atom nil)]
    (swap! conn (fn [db]
                  (let [r (load-entities-with db entities)]
                    (reset! report r)
                    (:db-after r))))
    @report))

(defn transact!
  ([conn tx-data] (transact! conn tx-data nil))
  ([conn tx-data tx-meta]
   {:pre [(conn? conn)]}
   (let [report (-transact! conn tx-data tx-meta)]
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     report)))

(defn reset-conn!
  "Forces underlying `conn` value to become `db`. Will generate a tx-report that will remove everything from old value and insert everything from the new one."
  ([conn db] (reset-conn! conn db nil))
  ([conn db tx-meta]
   (let [report (db/map->TxReport
                 {:db-before @conn
                  :db-after  db
                  :tx-data   (concat
                              (map #(assoc % :added false) (datoms @conn :eavt))
                              (datoms db :eavt))
                  :tx-meta   tx-meta})]
     (reset! conn db)
     (doseq [[_ callback] (some-> (:listeners (meta conn)) (deref))]
       (callback report))
     db)))

(defn- atom? [a]
  #?(:cljs (instance? Atom a)
     :clj  (instance? clojure.lang.IAtom a)))

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

;; Data Readers

(def ^{:doc "Data readers for EDN readers. In CLJS they’re registered automatically. In CLJ, if `data_readers.clj` do not work, you can always do

             ```
             (clojure.edn/read-string {:readers data-readers} \"...\")
             ```"}
  data-readers {'datahike/Datom dd/datom-from-reader
                'db/id          tempid
                'datahike/DB    db/db-from-reader})

#?(:cljs
   (doseq [[tag cb] data-readers] (cljs.reader/register-tag-parser! tag cb)))

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

(defn transact
  "Same as [[transact!]], but returns an immediately realized future.
  
   Exists for Datomic API compatibility. Prefer using [[transact!]] if possible."
  ([conn tx-data] (transact conn tx-data nil))
  ([conn tx-data tx-meta]
   {:pre [(conn? conn)]}
   (let [res (transact! conn tx-data tx-meta)]
     #?(:cljs
        (reify
          IDeref
          (-deref [_] res)
          IDerefWithTimeout
          (-deref-with-timeout [_ _ _] res)
          IPending
          (-realized? [_] true))
        :clj
        (reify
          clojure.lang.IDeref
          (deref [_] res)
          clojure.lang.IBlockingDeref
          (deref [_ _ _] res)
          clojure.lang.IPending
          (isRealized [_] true))))))

(defn load-entities [conn entities]
  {:pre [(conn? conn)]}
  (let [res (-load-entities! conn entities)]
    (reify
      clojure.lang.IDeref
      (deref [_] res)
      clojure.lang.IBlockingDeref
      (deref [_ _ _] res)
      clojure.lang.IPending
      (isRealized [_] true))))

;; ersatz future without proper blocking
#?(:cljs
   (defn- future-call [f]
     (let [res (atom nil)
           realized (atom false)]
       (js/setTimeout #(do (reset! res (f)) (reset! realized true)) 0)
       (reify
         IDeref
         (-deref [_] @res)
         IDerefWithTimeout
         (-deref-with-timeout [_ _ timeout-val] (if @realized @res timeout-val))
         IPending
         (-realized? [_] @realized)))))

(defn transact-async
  "In CLJ, calls [[transact!]] on a future thread pool, returning immediately.
  
   In CLJS, just calls [[transact!]] and returns a realized future."
  ([conn tx-data] (transact-async conn tx-data nil))
  ([conn tx-data tx-meta]
   {:pre [(conn? conn)]}
   (future-call #(transact! conn tx-data tx-meta))))

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

(defn squuid
  "Generates a UUID that grow with time. Such UUIDs will always go to the end  of the index and that will minimize insertions in the middle.
  
   Consist of 64 bits of current UNIX timestamp (in seconds) and 64 random bits (2^64 different unique values per second)."
  ([]
   (squuid #?(:clj  (System/currentTimeMillis)
              :cljs (.getTime (js/Date.)))))
  ([msec]
   #?(:clj
      (let [uuid (UUID/randomUUID)
            time (int (/ msec 1000))
            high (.getMostSignificantBits uuid)
            low (.getLeastSignificantBits uuid)
            new-high (bit-or (bit-and high 0x00000000FFFFFFFF)
                             (bit-shift-left time 32))]
        (UUID. new-high low))
      :cljs
      (uuid
       (str
        (-> (int (/ msec 1000))
            (to-hex-string 8))
        "-" (-> (rand-bits 16) (to-hex-string 4))
        "-" (-> (rand-bits 16) (bit-and 0x0FFF) (bit-or 0x4000) (to-hex-string 4))
        "-" (-> (rand-bits 16) (bit-and 0x3FFF) (bit-or 0x8000) (to-hex-string 4))
        "-" (-> (rand-bits 16) (to-hex-string 4))
        (-> (rand-bits 16) (to-hex-string 4))
        (-> (rand-bits 16) (to-hex-string 4)))))))

(defn squuid-time-millis
  "Returns time that was used in [[squuid]] call, in milliseconds, rounded to the closest second."
  [uuid]
  #?(:clj  (-> (.getMostSignificantBits ^UUID uuid)
               (bit-shift-right 32)
               (* 1000))
     :cljs (-> (subs (str uuid) 0 8)
               (js/parseInt 16)
               (* 1000))))
