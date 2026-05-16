(ns datahike.api.impl
  "API input and backwards compatibility. This namespace only ensures
  compatibility and does not implement underlying functionality."
  (:refer-clojure :exclude [filter])
  (:require [datahike.connector :as dc]
            [datahike.config :as config]
            [datahike.writer :as dw]
            [datahike.writing :as writing]
            [datahike.constants :as const]
            [datahike.core :as dcore]
            [datahike.pull-api :as dp]
            [datahike.query :as dq]
            [datahike.schema :as ds]
            [datahike.tools :as dt]
            [datahike.db :as db #?@(:cljs [:refer [HistoricalDB AsOfDB SinceDB FilteredDB]])]
            [datahike.db.interface :as dbi]
            [datahike.db.transaction :as dbt]
            [datahike.impl.entity :as de]
            [datahike.versioning :as dv]
            [replikativ.logging :as log]
            #?(:cljs [clojure.core.async :as async :refer [<! >! chan put! close!]]))
  #?(:cljs (:require-macros [superv.async :refer [go-try- <?-]]
                            [clojure.core.async :refer [go]]))
  #?(:clj
     (:import [clojure.lang Keyword PersistentArrayMap]
              [datahike.db HistoricalDB AsOfDB SinceDB FilteredDB]
              [datahike.impl.entity Entity])))

(defn transact! [connection arg-map]
  (let [arg (cond
              (map? arg-map)      (if (contains? arg-map :tx-data)
                                    arg-map
                                    (log/raise "Bad argument to transact, map missing key :tx-data."
                                               {:error         :transact/syntax
                                                :argument-keys (keys arg-map)}))
              (or (vector? arg-map)
                  (seq? arg-map)) {:tx-data arg-map}
              :else               (log/raise "Bad argument to transact, expected map, vector or sequence."
                                             {:error         :transact/syntax
                                              :argument-type (type arg-map)}))]
    (dw/transact! connection arg)))

(defn transact [connection arg-map]
  #?(:clj
     @(transact! connection arg-map)
     :cljs (throw (ex-info "Synchronous transact not supported in ClojureScript, use transact! instead."
                           {:error :transact/sync-not-supported}))))

;; necessary to support initial-tx shorthand, which really should have been avoided
(defn create-database [& args]
  #?(:clj
     (let [config @(apply dw/create-database args)]
       (when-let [txs (:initial-tx config)]
         (let [conn (dc/connect config)]
           (transact conn txs)
           (dc/release conn)))
       config)
     :cljs
     (go
       (let [config (<! (apply dw/create-database args))]
         (when-let [txs (:initial-tx config)]
           (let [conn (<! (dc/connect config {:sync? false}))]
             (<! (transact! conn txs))
             (dc/release conn)))
         config))))

(defn delete-database [& args]
  #?(:clj @(apply dw/delete-database args)
     :cljs (apply dw/delete-database args)))

(defn database-exists? [config]
  #?(:clj  @(writing/database-exists? config)
     :cljs (writing/database-exists? config)))

(defmulti datoms
  (fn
    ([_db arg-map]
     (type arg-map))
    ([_db index & _components]
     (type index))))

(defmethod datoms PersistentArrayMap
  [db {:keys [index components]}]
  (dbi/datoms db index components))

(defmethod datoms Keyword
  [db index & components]
  (if (nil? components)
    (dbi/datoms db index [])
    (dbi/datoms db index components)))

(defmulti seek-datoms
  (fn
    ([_db arg-map]
     (type arg-map))
    ([_db index & _components]
     (type index))))

(defmethod seek-datoms PersistentArrayMap
  [db {:keys [index components]}]
  (dbi/seek-datoms db index components))

(defmethod seek-datoms Keyword
  [db index & components]
  (if (nil? components)
    (dbi/seek-datoms db index [])
    (dbi/seek-datoms db index components)))

(defn with
  ([db arg-map]
   (let [tx-data (if (:tx-data arg-map) (:tx-data arg-map) arg-map)
         tx-meta (if (:tx-meta arg-map) (:tx-meta arg-map) nil)]
     (with db tx-data tx-meta)))
  ([db tx-data tx-meta]
   (if (dcore/is-filtered db)
     (log/raise "Filtered DB cannot be modified" {:error :transaction/filtered})
     (dbt/transact-tx-data (db/map->TxReport
                            {:db-before db
                             :db-after  db
                             :tx-data   []
                             :tempids   {}
                             :tx-meta   tx-meta}) tx-data))))

(defn db-with [db tx-data]
  (:db-after (with db tx-data)))

(defn db [conn]
  @conn)

(defn since [db time-point]
  (if (dbi/-temporal-index? db)
    (SinceDB. db time-point)
    (log/raise "since is only allowed on temporal indexed databases." {:config (dbi/-config db)})))

(defn as-of [db time-point]
  (if (dbi/-temporal-index? db)
    (if (int? time-point)
      (if (<= const/tx0 time-point)
        (AsOfDB. db time-point)
        (log/raise (str "Invalid transaction ID. Must be bigger than " const/tx0 ".")
                   {:time-point time-point}))
      (AsOfDB. db time-point))
    (log/raise "as-of is only allowed on temporal indexed databases." {:config (dbi/-config db)})))

(defn history [db]
  (if (dbi/-temporal-index? db)
    (HistoricalDB. db)
    (log/raise "history is only allowed on temporal indexed databases." {:config (dbi/-config db)})))

(defn- vt-meta-attrs [db]
  (if (:attribute-refs? (dbi/-config db))
    {:vf (dbi/-ref-for db :db.valid/from)
     :vt (dbi/-ref-for db :db.valid/to)}
    {:vf :db.valid/from
     :vt :db.valid/to}))

(defn- mk-vt-pred
  "Build a `d/filter` predicate `(fn [db datom])` that keeps datoms
   whose asserting tx's vt-window contains `at`. The pred maintains
   an internal `tx-id → bool` HashMap cache so each unique tx-id
   triggers only one EAVT seek — same asymptotic cost as AsOfDB's
   `filter-txInstant` dedup, just a different mechanism (HashMap
   probe rather than transducer-distinct). Txes whose tx-entity has
   no `:db.valid/from` are treated as `_valid_from = -∞` — i.e.
   non-vt-aware data passes through unchanged."
  [^java.util.Date at]
  (let [cache (java.util.concurrent.ConcurrentHashMap.)]
    (fn vt-pred [db ^datahike.datom.Datom d]
      ;; (datom-tx d) returns the absolute tx-id (retractions store it
      ;; negated). Always-positive — safe to pass to entity lookups.
      (let [tx-id (datahike.datom/datom-tx d)
            cached (.get cache tx-id)]
        (if (some? cached)
          cached
          (let [{:keys [vf vt]} (vt-meta-attrs db)
                tx-datoms (dbi/-datoms db :eavt [tx-id] (dbi/-search-context db))
                vf-val (some (fn [^datahike.datom.Datom td]
                               (when (= vf (.-a td)) (.-v td))) tx-datoms)
                vt-val (some (fn [^datahike.datom.Datom td]
                               (when (= vt (.-a td)) (.-v td))) tx-datoms)
                ok? (and (or (nil? vf-val) (not (.after ^java.util.Date vf-val at)))
                         (or (nil? vt-val) (.after ^java.util.Date vt-val at)))]
            (.put cache tx-id ok?)
            ok?))))))

(defn valid-at
  "Filter `db` to datoms whose asserting tx's valid-time window
   contains `time-point`. Returns a `FilteredDB` whose predicate
   reads the tx-entity's `:db.valid/from` / `:db.valid/to` and admits
   a datom iff `vf <= time-point < vt` (open-ended `vt = nil` is
   treated as unbounded; missing `vf` is treated as `-∞`).

   Unlike `as-of`/`since`/`history` (tx-time axes), valid-at is a
   valid-time axis: the *time something was true in the modelled
   world* rather than *the tx that recorded it*. Composes cleanly
   with the tx-time wrappers — e.g.
     `(d/valid-at (d/as-of db tx-time) vt-inst)`
   yields the snapshot at tx-time `tx-time`, further filtered to
   datoms whose vt-window contains `vt-inst`.

   The returned db also carries `:datahike/valid-at` on its meta so
   vt-aware secondary indices (implementing `IValidTimeAware`) push
   the filter into `-search-at-vt` for native pushdown — the
   FilteredDB predicate is then the fallback for paths that don't
   route through a secondary index.

   `time-point` is a `java.util.Date`; `nil` only clears the
   `:datahike/valid-at` marker (so vt-aware secondary indices stop
   routing) — `FilteredDB` predicates are AND-composed and cannot be
   surgically removed, so any active filter remains in effect. To
   truly drop the filter, start from the original unwrapped db."
  [db time-point]
  (if (nil? time-point)
    (vary-meta db dissoc :datahike/valid-at)
    (-> (dcore/filter db (mk-vt-pred time-point))
        (vary-meta assoc :datahike/valid-at time-point))))

(defn index-range [db {:keys [attrid start end]}]
  (dbi/index-range db attrid start end))

(defn schema [db]
  (reduce-kv
   (fn [m k v]
     (cond
       (and (keyword? k)
            (not (or (ds/entity-spec-attr? k)
                     (ds/schema-attr? k)
                     (ds/sys-ident? k)))) (update m k #(merge % v))
       (number? k)                        (update m v #(merge % {:db/id k}))
       :else                              m))
   {}
   (dbi/-schema db)))

(defn reverse-schema [db]
  (reduce-kv
   (fn [m k v]
     (let [attrs (->> v
                      (remove #(or (ds/entity-spec-attr? %)
                                   (ds/sys-ident? %)
                                   (ds/schema-attr? %)))
                      (into #{}))]
       (if (empty? attrs)
         m
         (assoc m k attrs))))
   {}
   (dbi/-rschema db)))

;; ---------------------------------------------------------------------------
;; Versioning Operations

(defn branches [conn]
  #?(:clj  (dv/branches conn {:sync? true})
     :cljs (dv/branches conn {:sync? false})))

(defn branch! [conn from new-branch]
  #?(:clj  (dv/branch! conn from new-branch {:sync? true})
     :cljs (dv/branch! conn from new-branch {:sync? false})))

(defn delete-branch! [conn branch]
  #?(:clj  (dv/delete-branch! conn branch {:sync? true})
     :cljs (dv/delete-branch! conn branch {:sync? false})))

(defn force-branch! [db branch parents]
  #?(:clj  (dv/force-branch! db branch parents {:sync? true})
     :cljs (dv/force-branch! db branch parents {:sync? false})))

(defn merge-db
  ([conn parents tx-data]
   (merge-db conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   #?(:clj  (dv/merge! conn parents tx-data tx-meta)
      :cljs (throw (ex-info "Synchronous merge not supported in ClojureScript, use merge-db! instead."
                            {:error :merge/sync-not-supported})))))

(defn merge-db!
  ([conn parents tx-data]
   (merge-db! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (dv/merge-async! conn parents tx-data tx-meta)))

(defn commit-id [db]
  (dv/commit-id db))

(defn parent-commit-ids [db]
  (dv/parent-commit-ids db))

(defn commit-as-db [conn-or-store cid]
  #?(:clj  (dv/commit-as-db conn-or-store cid {:sync? true})
     :cljs (dv/commit-as-db conn-or-store cid {:sync? false})))

(defn branch-as-db [conn-or-store branch]
  #?(:clj  (dv/branch-as-db conn-or-store branch {:sync? true})
     :cljs (dv/branch-as-db conn-or-store branch {:sync? false})))
