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
            [datahike.bitemporal.predicate :as bp.pred]
            [replikativ.logging :as log]
            #?(:cljs [clojure.core.async :as async :refer [<! >! chan put! close!]]))
  #?(:cljs (:require-macros [superv.async :refer [go-try- <?-]]
                            [clojure.core.async :refer [go]]))
  #?(:clj
     (:import [datahike.db HistoricalDB AsOfDB SinceDB FilteredDB]
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

;; Dispatch on the ARG SHAPE (map vs index+components), not the concrete map
;; class: a small map literal is PersistentArrayMap on the JVM but can be
;; PersistentHashMap on cljs, so the old `(type arg-map)`-keyed dispatch silently
;; missed the arg-map form there. `map?` is value-level and cross-platform.
(defmulti datoms
  (fn
    ([_db arg-map] (if (map? arg-map) ::arg-map ::index))
    ([_db _index & _components] ::index)))

(defmethod datoms ::arg-map
  [db {:keys [index components]}]
  (dbi/datoms db index components))

(defmethod datoms ::index
  [db index & components]
  (if (nil? components)
    (dbi/datoms db index [])
    (dbi/datoms db index components)))

(defmulti seek-datoms
  (fn
    ([_db arg-map] (if (map? arg-map) ::arg-map ::index))
    ([_db _index & _components] ::index)))

(defmethod seek-datoms ::arg-map
  [db {:keys [index components]}]
  (dbi/seek-datoms db index components))

(defmulti rseek-datoms
  (fn
    ([_db arg-map] (if (map? arg-map) ::arg-map ::index))
    ([_db _index & _components] ::index)))

(defmethod rseek-datoms ::arg-map
  [db {:keys [index components]}]
  (dbi/rseek-datoms db index components))

(defmethod rseek-datoms ::index
  [db index & components]
  (if (nil? components)
    (dbi/rseek-datoms db index [])
    (dbi/rseek-datoms db index components)))

(defmethod seek-datoms ::index
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

(defn as-of
  "Snapshot `db` at tx-time `time-point` (a `java.util.Date` or
   tx-id long). Returns an `AsOfDB` wrapper whose reads filter to
   datoms asserted by txes ≤ `time-point`. Composes with
   `d/history` and `d/valid-at`.

   Composition with `d/valid-at`: wrap `d/as-of` FIRST, then
   `d/valid-at` outermost — e.g. `(d/valid-at (d/as-of db t) v)`.
   The supersession check inside `d/valid-at`'s predicate captures
   whatever db it was passed; if you wrap `d/as-of` outside, the
   captured db has no tx-time bound and the supersession scan reads
   future txes, producing incorrect results. This wrapper throws
   when invoked on a `d/valid-at`-wrapped db to surface the
   inversion at call-site rather than silently."
  [db time-point]
  (when (some? (:datahike/valid-at (meta db)))
    (log/raise (str "Cannot wrap d/as-of around a db already filtered by "
                    "d/valid-at — the supersession check would not see the "
                    "tx-time bound. Compose as (d/valid-at (d/as-of db t) v) "
                    "instead.")
               {:error :temporal/wrap-order
                :inner-marker :datahike/valid-at
                :outer-wrapper 'as-of}))
  (if (dbi/-temporal-index? db)
    (if (int? time-point)
      (if (<= const/tx0 time-point)
        (AsOfDB. db time-point)
        (log/raise (str "Invalid transaction ID. Must be bigger than " const/tx0 ".")
                   {:time-point time-point}))
      (AsOfDB. db time-point))
    (log/raise "as-of is only allowed on temporal indexed databases." {:config (dbi/-config db)})))

(defn history [db]
  (cond
    ;; History of a history is just the history — return as-is rather than
    ;; double-wrapping. Besides being the correct idempotent semantics, the
    ;; redundant HistoricalDB-around-HistoricalDB pushes queries onto the
    ;; legacy lookup path on cljs (the fused temporal path needs the origin's
    ;; :eavt to be a real PersistentSortedSet, which a nested wrapper isn't).
    (instance? HistoricalDB db) db
    (dbi/-temporal-index? db) (HistoricalDB. db)
    :else (log/raise "history is only allowed on temporal indexed databases." {:config (dbi/-config db)})))

;; `vt-meta-attrs`, `tx-covers-at?`, `find-eav-winner`, `mk-vt-pred`,
;; `mk-vt-overlap-pred` and `mk-vt-during-pred` live in the leaf
;; namespace `datahike.bitemporal.predicate` so that vt-aware secondary
;; indices can statically require them without closing a cycle back
;; through `api.impl` → `query` → `secondary`.

(defn valid-at
  "Snapshot `db` at valid-time `time-point` with **supersession**
   semantics: among historical events about the same `(e, a, v)`, the
   tx with the greatest tx-id whose vt-window covers `time-point`
   wins. Earlier assertions whose vt-window also covers
   `time-point` are filtered out as superseded.

   This implements bitemporal supersession at the read layer: a
   back-correction that asserts a new value with an overlapping
   vt-window supersedes the prior claim at every vt-point ≥ the
   correction's vt-from, without rewriting history. Composes with
   `d/history` (recommended — exposes the events the supersession
   algorithm needs) and `d/as-of` (bounds the supersession horizon
   to a given tx-time).

   **Composition order matters.** Wrap `d/as-of` first, then
   `d/valid-at` outermost: `(d/valid-at (d/as-of db t) v)`. The
   supersession check captures whatever db it was passed; wrapping
   `d/as-of` *outside* leaves the inner predicate with an unbounded
   db and the supersession scan reads future txes. `d/as-of` will
   throw if invoked on a vt-marked db to catch this mistake at
   call-site.

   The result is a `FilteredDB` whose predicate enforces supersession
   per-datom on every read path (`d/q`, `d/datoms`, `d/pull`,
   `d/entity`, `d/seek-datoms`, `d/index-range`).

   Returns the unwrapped db when `time-point` is `nil`, after
   clearing any `:datahike/valid-at` meta marker. Carries
   `:datahike/valid-at <time-point>` on meta so vt-aware secondary
   indices (`IValidTimeAware`) can push the filter into a native
   `-search-at-vt` — those secondaries (e.g. stratum vt-mode) are
   the fast path; this FilteredDB predicate is the
   semantically-correct fallback.

   Non-vt-aware data (txes without `:db.valid/from`) is treated as
   having an open-ended `[-∞, ∞)` window — it remains visible at
   every valid-time. Mixing vt-aware and non-vt-aware data therefore
   degrades gracefully (the non-vt facts pass through).

   Performance note: the supersession check costs one EAVT scan per
   unique `(e, a, v)` triple in the result set, cached for the
   lifetime of the wrapper. For high-throughput vt queries, route
   through a vt-aware secondary index."
  [db time-point]
  (if (nil? time-point)
    (vary-meta db dissoc :datahike/valid-at)
    (-> (dcore/filter db (bp.pred/mk-vt-pred time-point))
        (vary-meta assoc :datahike/valid-at time-point))))

(defn valid-between
  "Filter `db` to datoms whose asserting tx's vt-window *overlaps*
   `[from, to)`. SQL:2011 `FOR VALID_TIME BETWEEN from AND to`
   maps to this. Both endpoints are `java.util.Date`s.

   Carries `:datahike/valid-between [from to]` on the returned db
   for vt-aware secondary-index pushdown.

   Passing `nil` for either endpoint clears the marker only and
   does not narrow the FilteredDB predicate — to truly drop the
   filter start from the unwrapped db."
  [db from to]
  (if (or (nil? from) (nil? to))
    (vary-meta db dissoc :datahike/valid-between)
    (-> (dcore/filter db (bp.pred/mk-vt-overlap-pred from to))
        (vary-meta assoc :datahike/valid-between [from to]))))

(defn valid-during
  "Filter `db` to datoms whose asserting tx's vt-window is *fully
   contained* in `[from, to)`. Stricter than `valid-between`:
   tx-windows that merely overlap the query window but extend past
   either endpoint are excluded. Useful for 'find all corrections
   whose effective period was wholly within Q2 2024' style queries.

   Carries `:datahike/valid-during [from to]` on the returned db."
  [db from to]
  (if (or (nil? from) (nil? to))
    (vary-meta db dissoc :datahike/valid-during)
    (-> (dcore/filter db (bp.pred/mk-vt-during-pred from to))
        (vary-meta assoc :datahike/valid-during [from to]))))

(defn valid-all
  "Clear any active valid-time marker so the db sees its full
   vt-history. Equivalent to passing `nil` to `valid-at` /
   `valid-between` / `valid-during`: it strips the meta markers so
   vt-aware secondary indices stop routing, but does not unwrap a
   FilteredDB if one is already in place. Idempotent."
  [db]
  (vary-meta db dissoc
             :datahike/valid-at
             :datahike/valid-between
             :datahike/valid-during))

(defn index-range [db {:keys [attrid start end]}]
  (dbi/index-range db attrid start end))

(defn schema [db]
  (reduce-kv
   (fn [m k v]
     (cond
       (and (keyword? k)
            (not (or (ds/entity-spec-attr? k)
                     (ds/schema-attr? k)
                     (ds/secondary-index-attr? k)
                     (ds/reference-attr? k)
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
                                   (ds/secondary-index-attr? %)
                                   (ds/reference-attr? %)
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
