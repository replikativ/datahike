(ns datahike.bitemporal.predicate
  "Valid-time predicate factories — the semantically-correct read-side
   filter for `d/valid-at` / `d/valid-between` / `d/valid-during`.

   Lifted out of `datahike.api.impl` to a leaf namespace so vt-aware
   secondary indices (`datahike.index.secondary` and downstream impls
   like stratum) can require this directly without creating a cycle
   back through the heavyweight `api.impl` → `query` → `writing` chain.

   The factories return per-call predicate closures with their own
   per-predicate memoisation caches; see
   `datahike.bitemporal.platform/mutable-map` for the JVM/CLJS caching
   story and `mk-vt-pred`'s docstring for the supersession algorithm."
  (:require [datahike.db.interface :as dbi]
            [datahike.bitemporal.platform :as bp]
            [datahike.datom]))

(defn vt-meta-attrs
  "On an attribute-refs DB the schema attrs are stored as ref-eids;
   resolve them up-front so the predicate's inner loop compares ints,
   not keywords."
  [db]
  (if (:attribute-refs? (dbi/-config db))
    {:vf (dbi/-ref-for db :db.valid/from)
     :vt (dbi/-ref-for db :db.valid/to)}
    {:vf :db.valid/from
     :vt :db.valid/to}))

;; ---------------------------------------------------------------------------
;; Pure window algebra — the single source of truth shared by the per-datom
;; predicates below and the async engine's pure-pred rebuilds
;; (execute/prepare-vt-wrappers-step). Missing vf = -inf, missing vt = +inf;
;; :during requires both endpoints.

(defn window-covers-at? [vf-val vt-val at]
  (and (or (nil? vf-val) (not (bp/date-after? vf-val at)))
       (or (nil? vt-val) (bp/date-after? vt-val at))))

(defn window-overlaps? [vf-val vt-val from to]
  (and (or (nil? vf-val) (bp/date-before? vf-val to))
       (or (nil? vt-val) (bp/date-after? vt-val from))))

(defn window-during? [vf-val vt-val from to]
  (and (some? vf-val) (some? vt-val)
       (not (bp/date-before? vf-val from))
       (not (bp/date-after? vt-val to))))

(defn- tx-covers-at?
  "Does the tx-entity `tx-id`'s vt-window contain `at`? Reads
   `:db.valid/from` / `:db.valid/to` off the tx-entity. Missing
   `:db.valid/from` is treated as -∞; missing `:db.valid/to` as +∞,
   so non-vt-aware data passes through unchanged. Memoized via
   `cover-cache` (tx-id → Boolean) — a `bp/mutable-map`."
  [db tx-id at cover-cache]
  (let [cached (bp/mget cover-cache tx-id)]
    (if (some? cached)
      cached
      (let [{:keys [vf vt]} (vt-meta-attrs db)
            tx-datoms (dbi/-datoms db :eavt [tx-id] (dbi/-search-context db))
            vf-val (some (fn [^datahike.datom.Datom td]
                           (when (= vf (.-a td)) (.-v td))) tx-datoms)
            vt-val (some (fn [^datahike.datom.Datom td]
                           (when (= vt (.-a td)) (.-v td))) tx-datoms)
            ok? (window-covers-at? vf-val vt-val at)]
        (bp/mput! cover-cache tx-id ok?)
        ok?))))

(defn- find-eav-winner
  "Among all historical datoms about `(e, a, v)`, return the one whose
   tx-id is the greatest AND whose tx-vt-window contains `at` — the
   'supersession winner' for this fact at this valid-time. Returns
   `nil` if no candidate. Implements bitemporal supersession at query time: the
   topmost event at `(qvt, current-sys-time)` is the one whose tx-id
   is highest among those covering `qvt`. Memoized via
   `winner-cache` ([e a v] → Datom or ::miss)."
  [db e a v at cover-cache winner-cache]
  (let [k [e a v]
        cached (bp/mget winner-cache k)]
    (cond
      (= cached ::miss) nil
      (some? cached)    cached
      :else
      (let [datoms (dbi/-datoms db :eavt [e a v] (dbi/-search-context db))
            winner (reduce
                    (fn [w ^datahike.datom.Datom d]
                      (if (tx-covers-at? db (datahike.datom/datom-tx d) at cover-cache)
                        (if (or (nil? w)
                                (> (datahike.datom/datom-tx d)
                                   (datahike.datom/datom-tx ^datahike.datom.Datom w)))
                          d w)
                        w))
                    nil datoms)]
        (bp/mput! winner-cache k (or winner ::miss))
        winner))))

(defn mk-vt-pred
  "Build a `d/filter` predicate `(fn [db datom])` implementing
   supersession-aware valid-time filtering at point `at`. Algorithm
   implements bitemporal supersession at read time:

     for each datom D = (e, a, v, tx, op):
       1. require tx's vt-window covers `at`;
       2. among ALL historical datoms about (e, a, v), find the one
          whose tx is the greatest AND whose tx-vt covers `at` — the
          'winner';
       3. admit D iff D is the winner.

   On current-view (no history) the (e, a, v) scan returns the single
   live datom; the algorithm degrades to today's single-axis
   filter. On `(d/history db)` it gives back-correction-correct
   semantics: a later tx with overlapping vt supersedes an earlier
   one at the relevant vt-points.

   Two caches:
     * `cover-cache` (tx-id → Boolean): one EAVT seek per unique tx.
     * `winner-cache` ([e a v] → Datom): one EAVT seek per unique fact.

   The fast path for high-throughput vt queries is the
   `:datahike/valid-at` meta marker → `IValidTimeAware/-search-at-vt`
   on a vt-aware secondary index (e.g. stratum), which precomputes
   the polygon at write time. This predicate is the
   semantically-correct fallback for paths that don't route through
   a secondary."
  [at]
  (let [cover-cache  (bp/mutable-map)
        winner-cache (bp/mutable-map)]
    (with-meta
      (fn vt-pred [db ^datahike.datom.Datom d]
      ;; (datom-tx d) is always-positive (retractions store it negated).
        (when (tx-covers-at? db (datahike.datom/datom-tx d) at cover-cache)
          (when-let [^datahike.datom.Datom w
                     (find-eav-winner db (.-e d) (.-a d) (.-v d) at
                                      cover-cache winner-cache)]
            (= (datahike.datom/datom-tx d)
               (datahike.datom/datom-tx w)))))
      ;; explicit with-meta: reader ^{} meta on a tail-position fn literal
      ;; miscompiles on cljs (with_meta around a statement-positioned fn)
      {:datahike/vt-spec {:kind :at :at at}})))

(defn mk-vt-overlap-pred
  "Pred that admits a datom iff its tx's vt-window *overlaps*
   `[from, to)`. Open-ended `vt = nil` is treated as `+∞`; missing
   `vf` is `-∞`. The overlap condition is `(vf < to) AND (vt > from)`."
  [from to]
  (let [cache (bp/mutable-map)]
    (with-meta
      (fn vt-overlap-pred [db ^datahike.datom.Datom d]
        (let [tx-id (datahike.datom/datom-tx d)
              cached (bp/mget cache tx-id)]
          (if (some? cached)
            cached
            (let [{:keys [vf vt]} (vt-meta-attrs db)
                  tx-datoms (dbi/-datoms db :eavt [tx-id] (dbi/-search-context db))
                  vf-val (some (fn [^datahike.datom.Datom td]
                                 (when (= vf (.-a td)) (.-v td))) tx-datoms)
                  vt-val (some (fn [^datahike.datom.Datom td]
                                 (when (= vt (.-a td)) (.-v td))) tx-datoms)
                  ok? (window-overlaps? vf-val vt-val from to)]
              (bp/mput! cache tx-id ok?)
              ok?))))
      {:datahike/vt-spec {:kind :overlap :from from :to to}})))

(defn mk-vt-during-pred
  "Pred that admits a datom iff its tx's vt-window is *fully
   contained* in `[from, to)`. Strict containment: missing `vf` or
   `vt` fail (an unbounded interval can't be fully contained in a
   bounded one)."
  [from to]
  (let [cache (bp/mutable-map)]
    (with-meta
      (fn vt-during-pred [db ^datahike.datom.Datom d]
        (let [tx-id (datahike.datom/datom-tx d)
              cached (bp/mget cache tx-id)]
          (if (some? cached)
            cached
            (let [{:keys [vf vt]} (vt-meta-attrs db)
                  tx-datoms (dbi/-datoms db :eavt [tx-id] (dbi/-search-context db))
                  vf-val (some (fn [^datahike.datom.Datom td]
                                 (when (= vf (.-a td)) (.-v td))) tx-datoms)
                  vt-val (some (fn [^datahike.datom.Datom td]
                                 (when (= vt (.-a td)) (.-v td))) tx-datoms)
                  ok? (window-during? vf-val vt-val from to)]
              (bp/mput! cache tx-id ok?)
              ok?))))
      {:datahike/vt-spec {:kind :during :from from :to to}})))
