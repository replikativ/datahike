(ns datahike.query.execute
  "Execution engine for query plans.
   Supports fused scan+merge for entity groups, hash-probe value joins,
   anti-merge NOT, and direct-to-HashSet output."
  (:require
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :as datom :refer [datom]]
   [datahike.db.interface :as dbi]
   [datahike.db.utils :as dbu]
   [datahike.index.interface :as di]
   [datahike.query.analyze :as analyze]
   [datahike.query.plan :as plan]
   [datahike.query.relation :as rel]
   #?(:clj [datahike.index.secondary :as sec])
   #?(:clj [datahike.index.entity-set :as es])
   #?(:clj [datahike.query :as legacy])
   #?(:cljs [org.replikativ.persistent-sorted-set :as psset])
   #?(:cljs [org.replikativ.persistent-sorted-set.btset :as btset :refer [BTSet]])
   [datahike.db :as db #?@(:cljs [:refer [AsOfDB SinceDB HistoricalDB FilteredDB]])]
   [replikativ.logging :as log])
  #?(:cljs (:require-macros [datahike.query.execute :refer [scan-filter scan-filter-temporal emit-tuple]]))
  #?(:clj (:import [datahike.datom Datom]
                   [datahike.db AsOfDB SinceDB HistoricalDB FilteredDB]
                   [org.replikativ.persistent_sorted_set
                    PersistentSortedSet
                    PersistentSortedSet$ForwardCursor])))

#?(:clj (set! *warn-on-reflection* true))

#?(:clj (def ^:private ^Class object-array-class (Class/forName "[Ljava.lang.Object;")))

#?(:clj (import 'datahike.query.relation.Relation))

(declare execute-plan)

;; ---------------------------------------------------------------------------
;; Cross-platform helpers

(defn- pss-instance?
  "Check if x is a PersistentSortedSet (CLJ) or BTSet (CLJS)."
  [x]
  #?(:clj  (instance? PersistentSortedSet x)
     :cljs (instance? BTSet x)))

(defn- pss-lookup-ge
  "Cross-platform lookupGE: first element >= key."
  [pss key]
  #?(:clj  (.lookupGE ^PersistentSortedSet pss key)
     :cljs (btset/lookup-ge pss key nil {:sync? true})))

(defn- make-result-list
  "Create a mutable list for collecting results."
  [capacity]
  #?(:clj  (java.util.ArrayList. (int capacity))
     :cljs #js []))

(defn- result-list-add [list x]
  #?(:clj  (.add ^java.util.ArrayList list x)
     :cljs (.push list x)))

(defn- result-list-size [list]
  #?(:clj  (.size ^java.util.ArrayList list)
     :cljs (.-length list)))

(defn- result-list-get [list i]
  #?(:clj  (.get ^java.util.ArrayList list (int i))
     :cljs (aget list i)))

(defn- make-probe-set
  "Create a mutable set for hash-probe joins."
  [capacity]
  #?(:clj  (java.util.HashSet. (int capacity))
     :cljs (js/Set.)))

(defn- probe-set-add [s x]
  #?(:clj  (.add ^java.util.HashSet s x)
     :cljs (.add s x)))

(defn- probe-set-contains? [s x]
  #?(:clj  (.contains ^java.util.HashSet s x)
     :cljs (.has s x)))

(defn- probe-set-size [s]
  #?(:clj  (.size ^java.util.HashSet s)
     :cljs (.-size s)))

(def ^:private ^:const probe-driven-threshold
  "Use probe-driven AVET seeks when probe-set-size * threshold < scan-size.
   Empirically determined: AVET seek overhead ~50μs, scan+filter ~20ns/datom,
   so break-even at K ≈ N/2500. Using 2500 as threshold."
  2500)

#?(:clj
   (defn- probe-driven-iterable
     "Build an Iterable that seeks an index for each probe-set value using a
      single ForwardCursor. Sorted probe values ensure forward-only cursor
      movement. The iterator yields all datoms matching any probe value.

      probe-field: 0 = entity position (seeks EAVT), 2 = value position (seeks AVET).
      Caller should pass nil for probe-set since filtering is baked into the seeks."
     ^Iterable [^PersistentSortedSet pss resolved-a ^java.util.HashSet probe-set probe-field]
     (let [sorted-vals (sort (vec (.toArray probe-set)))
           ^objects val-arr (object-array sorted-vals)
           n-vals (alength val-arr)
           by-entity? (== (int probe-field) 0)]
       (reify Iterable
         (iterator [_]
           (let [cursor (.forwardCursor pss)
                 vi (volatile! (int 0))
                 current (volatile! nil)
                 done (volatile! false)
                 seek-fn (fn [v]
                           (.seekGE ^PersistentSortedSet$ForwardCursor cursor
                                    (if by-entity?
                                      (datom (long v) resolved-a nil tx0)
                                      (datom e0 resolved-a v tx0))))
                 match-fn (fn [^Datom d v]
                            (if by-entity?
                              (and (== (.-e d) (long v)) (= (.-a d) resolved-a))
                              (and (= (.-a d) resolved-a) (= (.-v d) v))))]
             ;; Seek to first value
             (when (pos? n-vals)
               (vreset! current (seek-fn (aget val-arr 0))))
             (reify java.util.Iterator
               (hasNext [_]
                 (if @done
                   false
                   (loop []
                     (let [^Datom d @current
                           vi-val (int @vi)]
                       (cond
                         (nil? d)
                         (let [next-vi (inc vi-val)]
                           (if (>= next-vi n-vals)
                             (do (vreset! done true) false)
                             (do (vreset! vi next-vi)
                                 (vreset! current (seek-fn (aget val-arr next-vi)))
                                 (recur))))

                         (not (match-fn d (aget val-arr vi-val)))
                         (let [next-vi (inc vi-val)]
                           (if (>= next-vi n-vals)
                             (do (vreset! done true) false)
                             (do (vreset! vi next-vi)
                                 (vreset! current (seek-fn (aget val-arr next-vi)))
                                 (recur))))

                         :else true)))))
               (next [_]
                 (let [d @current]
                   (vreset! current (.next ^PersistentSortedSet$ForwardCursor cursor))
                   d)))))))))

(defn- adopt-vector
  "Create a PersistentVector from an object array without copying."
  [^objects arr]
  #?(:clj  (clojure.lang.PersistentVector/adopt arr)
     :cljs (vec arr)))

;; ---------------------------------------------------------------------------
;; Fast Java comparators (avoid Clojure Var indirection in hot path)

#?(:clj
   (def ^:private ^java.util.Comparator fast-cmp-ea
     "EA-only comparator as a direct Java Comparator.
   Avoids Clojure Var.getRawRoot overhead that cmp-datoms-ea incurs
   (~1.6x faster per seekGE call in profiling)."
     (reify java.util.Comparator
       (compare [_ d1 d2]
         (let [^Datom d1 d1 ^Datom d2 d2
               c (Long/compare (.-e d1) (.-e d2))]
           (if (zero? c)
             (.compareTo ^Comparable (.-a d1) (.-a d2))
             (int c)))))))

;; ---------------------------------------------------------------------------
;; Helpers

(defn- resolve-attr [db a]
  (if (and (:attribute-refs? (dbi/-config db)) (keyword? a))
    (dbi/-ref-for db a)
    a))

(defn- val-eq?
  "Value equality that handles byte arrays (which don't support Clojure =)."
  [a b]
  #?(:clj (if (and (bytes? a) (bytes? b))
            (java.util.Arrays/equals ^bytes a ^bytes b)
            (= a b))
     :cljs (= a b)))

(defmacro ^:private merge-datom-match?
  "Inline predicate: does merge datom d match entity+attr+value+shared-var constraints?
   Expands in place for zero-overhead in hot loops. Type-hints d and scan-d internally."
  [d eid ra vg? vgv check-v? check-tx? scan-d]
  (let [d# (with-meta d {:tag 'datahike.datom.Datom})
        sd# (with-meta scan-d {:tag 'datahike.datom.Datom})]
    `(and (== (.-e ~d#) ~eid) (= (.-a ~d#) ~ra)
          (or (not ~vg?) (val-eq? (.-v ~d#) ~vgv))
          (or (not ~check-v?) (val-eq? (.-v ~d#) (.-v ~sd#)))
          (or (not ~check-tx?) (= (datom/datom-tx ~d#) (datom/datom-tx ~sd#))))))

(defmacro ^:private temporal-merge-datom-match?
  "Like merge-datom-match? but also checks temporal-tx-filter and added-filter.
   Type-hints d and scan-d internally."
  [d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter]
  (let [d# (with-meta d {:tag 'datahike.datom.Datom})
        sd# (with-meta scan-d {:tag 'datahike.datom.Datom})]
    `(and (== (.-e ~d#) ~eid) (= (.-a ~d#) ~ra)
          (or (not ~vg?) (val-eq? (.-v ~d#) ~vgv))
          (or (nil? ~temporal-tx-filter) (~temporal-tx-filter ~d#))
          (or (nil? ~added-filter) (= (datom/datom-added ~d#) ~added-filter))
          (or (not ~check-v?) (val-eq? (.-v ~d#) (.-v ~sd#)))
          (or (not ~check-tx?) (= (datom/datom-tx ~d#) (datom/datom-tx ~sd#))))))

(defn- build-ground-filter
  "Build a filter for ground components not covered by the index bounds."
  [clause index]
  (let [[e a v tx added] clause
        filters
        (cond-> []
          (and (not (nil? v)) (not (symbol? v)) (not= :avet index))
          (conj (fn [^Datom d] (val-eq? (.-v d) v)))
          (and (not (nil? e)) (not (symbol? e)) (number? e) (not= :eavt index))
          (conj (let [le #?(:clj (long e) :cljs e)] (fn [^Datom d] (= (.-e d) le))))
          ;; Ground tx: filter datoms by specific transaction (important for temporal queries)
          (and (not (nil? tx)) (not (symbol? tx)) (number? tx))
          (conj (let [ltx #?(:clj (long tx) :cljs tx)] (fn [^Datom d] (= (datom/datom-tx d) ltx))))
          ;; Ground added: filter by assertion/retraction (important for temporal queries)
          (and (some? added) (not (symbol? added)) (boolean? added))
          (conj (fn [^Datom d] (= (datom/datom-added d) added))))]
    (when (seq filters)
      (fn [^Datom d] (every? #(% d) filters)))))

(defn- build-strict-filter
  "Build a post-filter for strict inequalities (> <) on a datom field."
  [strict-preds datom-field-idx]
  (when (seq strict-preds)
    (fn [^Datom d]
      (let [dv (case (int datom-field-idx) 0 (.-e d) 1 (.-a d) 2 (.-v d) 3 (.-tx d))]
        (every? (fn [{:keys [op const-val]}]
                  (case op > (> (compare dv const-val) 0) < (< (compare dv const-val) 0) true))
                strict-preds)))))

(defn- compute-slice-bounds
  "Compute [from-datom to-datom] for an index slice given clause, index, and pushdown."
  [clause index pushdown-bounds resolved-a resolved-e]
  (case index
    :eavt [(datom (or resolved-e e0) resolved-a nil tx0)
           (datom (or resolved-e emax) resolved-a nil txmax)]
    :aevt [(datom e0 resolved-a nil tx0)
           (datom emax resolved-a nil txmax)]
    :avet (let [v (get clause 2)
                from-v (or (:from-v pushdown-bounds)
                           (when (and (some? v) (not (analyze/free-var? v))) v))
                to-v (or (:to-v pushdown-bounds)
                         (when (and (some? v) (not (analyze/free-var? v))) v))]
            [(datom e0 resolved-a from-v tx0)
             (datom emax resolved-a to-v txmax)])))

;; ---------------------------------------------------------------------------
;; Pattern scan → Relation (for legacy-compatible path)

(defn- execute-pattern-scan [db op]
  (let [{:keys [clause index pushdown-preds]} op
        [e a v tx] clause
        resolved-a (when (and (some? a) (not (symbol? a))) (resolve-attr db a))
        resolved-e (when (and (some? e) (not (symbol? e)) (number? e)) #?(:clj (long e) :cljs e))
        pushdown-bounds (when (seq pushdown-preds) (plan/pushdown-to-bounds pushdown-preds))
        [from-datom to-datom] (compute-slice-bounds clause index pushdown-bounds resolved-a resolved-e)
        db-index (get db index)
        datoms (di/-slice db-index from-datom to-datom index)
        var-map (rel/var-mapping clause (range))
        ground-filter (build-ground-filter clause index)
        scan-var-map (rel/var-mapping clause (range))
        strict-filter (when-let [strict (:strict-preds pushdown-bounds)]
                        (let [pushdown-var (first (keep :var pushdown-preds))]
                          (when pushdown-var
                            (build-strict-filter strict (get scan-var-map pushdown-var)))))
        tuples (into []
                     (comp (if ground-filter (filter ground-filter) identity)
                           (if strict-filter (filter strict-filter) identity)
                           (map (fn [^Datom d] [(.-e d) (.-a d) (.-v d) (.-tx d) true])))
                     datoms)]
    (rel/->Relation var-map tuples)))

;; ---------------------------------------------------------------------------
;; Fused scan+merge execution (single entity group, direct to output)
;;
;; This is the hot path for Q1-Q4 style queries.
;; For each scan datom: lookupGE per merge op, short-circuit on miss.
;;
;; Branch-once-then-loop: the top-level cond selects the loop variant once,
;; then each branch has its own complete iteration with filter/merge/emit
;; inlined. This gives the JIT a stable monomorphic loop body per branch
;; while keeping everything in one defn- (so closures stay inlineable).

;; Named constants for find-source projection slots.
;; Negative values = scan datom fields, -10 = const from :in binding.
;; Non-negative: merge-v at index, +1000 = merge-e, +2000 = merge-a, etc.
(def ^:private ^:const find-src-scan-e -1)
(def ^:private ^:const find-src-scan-a -2)
(def ^:private ^:const find-src-scan-v -3)
(def ^:private ^:const find-src-scan-tx -4)
(def ^:private ^:const find-src-scan-added -5)
(def ^:private ^:const find-src-const -10)
(def ^:private ^:const find-src-merge-v-base 0)
(def ^:private ^:const find-src-merge-e-base 1000)
(def ^:private ^:const find-src-merge-a-base 2000)
(def ^:private ^:const find-src-merge-tx-base 3000)
(def ^:private ^:const find-src-merge-added-base 4000)

;; Macros for filter/emit to avoid duplication across loop branches.
;; These expand inline, so the JIT sees them as part of the same method.

(defmacro ^:private scan-filter
  "Expand inline filter checks for a scan datom. Yields true if the datom
   passes all filters (ground, strict, probe). All filter vars must be in
   scope at expansion site."
  [scan-d ground-filter strict-filter probe-set probe-datom-field]
  (let [d (with-meta scan-d {:tag 'datahike.datom.Datom})]
    `(and (or (nil? ~ground-filter) (~ground-filter ~d))
          (or (nil? ~strict-filter) (~strict-filter ~d))
          (or (nil? ~probe-set)
              (probe-set-contains? ~probe-set
                                   (case (int ~probe-datom-field)
                                     0 (.-e ~d) 1 (.-a ~d)
                                     2 (.-v ~d) 3 (.-tx ~d)
                                     (.-v ~d)))))))

(defmacro ^:private scan-filter-temporal
  "Like scan-filter but with additional temporal-tx-filter check."
  [scan-d ground-filter strict-filter probe-set probe-datom-field temporal-tx-filter]
  (let [d (with-meta scan-d {:tag 'datahike.datom.Datom})]
    `(and (or (nil? ~temporal-tx-filter) (~temporal-tx-filter ~d))
          (or (nil? ~ground-filter) (~ground-filter ~d))
          (or (nil? ~strict-filter) (~strict-filter ~d))
          (or (nil? ~probe-set)
              (probe-set-contains? ~probe-set
                                   (case (int ~probe-datom-field)
                                     0 (.-e ~d) 1 (.-a ~d)
                                     2 (.-v ~d) 3 (datom/datom-tx ~d)
                                     (.-v ~d)))))))

(defmacro ^:private emit-tuple
  "Expand inline result emission for a scan datom. All projection vars
   (find-source, const-vals, merge-datoms, etc.) must be in scope."
  [scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
   n-find find-source const-vals result-list]
  (let [d (with-meta scan-d {:tag 'datahike.datom.Datom})
        md (with-meta (gensym "md") {:tag 'datahike.datom.Datom})]
    `(do
       (when ~collect-set
         (let [val# (if (neg? ~collect-merge-idx)
                      (case (int ~collect-datom-field)
                        0 (.-e ~d) 1 (.-a ~d)
                        2 (.-v ~d) 3 (datom/datom-tx ~d) (.-v ~d))
                      (let [~md (aget ~merge-datoms ~collect-merge-idx)]
                        (.-v ~md)))]
           (probe-set-add ~collect-set val#)))
       (when (pos? ~n-find)
         (let [out# #?(:clj (object-array ~n-find) :cljs (make-array ~n-find))]
           (dotimes [fi# ~n-find]
             (let [src# (aget ~find-source fi#)]
               (aset out# fi#
                     (cond
                       (== src# find-src-const) (aget ~const-vals fi#)
                       (== src# find-src-scan-e) (.-e ~d)
                       (== src# find-src-scan-a) (.-a ~d)
                       (== src# find-src-scan-v) (.-v ~d)
                       (== src# find-src-scan-tx) (datom/datom-tx ~d)
                       (== src# find-src-scan-added) (datom/datom-added ~d)
                       (< src# find-src-merge-e-base) (let [~md (aget ~merge-datoms src#)] (.-v ~md))
                       (< src# find-src-merge-a-base) (let [~md (aget ~merge-datoms (- src# find-src-merge-e-base))] (.-e ~md))
                       (< src# find-src-merge-tx-base) (let [~md (aget ~merge-datoms (- src# find-src-merge-a-base))] (.-a ~md))
                       (< src# find-src-merge-added-base) (let [~md (aget ~merge-datoms (- src# find-src-merge-tx-base))] (datom/datom-tx ~md))
                       :else (let [~md (aget ~merge-datoms (- src# find-src-merge-added-base))] (datom/datom-added ~md))))))
           (result-list-add ~result-list out#))))))

;; ---------------------------------------------------------------------------
;; Temporal query support helpers

(defn- resolve-date-to-tx-id
  "Resolve a Date time-point to the max numeric tx-id where :db/txInstant <= date.
   Scans temporal AEVT for :db/txInstant datoms on the origin DB (plain DB).
   Returns tx0 if no transactions exist at or before the given date."
  [origin-db date]
  (let [txInstant-attr (if (:attribute-refs? (dbi/-config origin-db))
                         (dbi/-ref-for origin-db :db/txInstant)
                         :db/txInstant)
        all-instants (dbi/-datoms origin-db :aevt [txInstant-attr] dbu/temporal-context)
        matching (filter (fn [^Datom d]
                           #?(:clj  (<= (compare (.-v d) date) 0)
                              :cljs (<= (.getTime (.-v d)) (.getTime date))))
                         all-instants)]
    (if (seq matching)
      (long (.-e ^Datom (last matching)))
      (long tx0))))

(defn- temporal-info
  "Extract temporal metadata from a DB wrapper.
   Date-based time-points are resolved to numeric tx-ids via AVET lookup on the origin DB.
   Returns nil for regular DBs."
  [db]
  (cond
    (instance? HistoricalDB db)
    {:type :historical :origin-db (dbi/-origin db)}

    (instance? AsOfDB db)
    (let [tp (dbi/-time-point db)
          origin-db (dbi/-origin db)
          numeric-tp (if (number? tp) (long tp) (resolve-date-to-tx-id origin-db tp))]
      {:type :as-of :origin-db origin-db :time-point numeric-tp})

    (instance? SinceDB db)
    (let [tp (dbi/-time-point db)
          origin-db (dbi/-origin db)
          numeric-tp (if (number? tp) (long tp) (resolve-date-to-tx-id origin-db tp))]
      {:type :since :origin-db origin-db :time-point numeric-tp})

    :else nil))

(defn- build-temporal-tx-filter
  "Build a tx filter function for as-of/since temporal queries.
   Returns nil for non-temporal or historical queries."
  [temporal]
  (when temporal
    (let [tp (long (or (:time-point temporal) 0))]
      (case (:type temporal)
        :since (fn [^Datom d] (> (datom/datom-tx d) tp))
        :as-of (fn [^Datom d] (<= (datom/datom-tx d) tp))
        nil))))

#?(:clj
   (defn- fast-merge-scan
     "Eagerly merge two PSS slices (current + temporal) into an ArrayList.
      Handles nil slices (di/-slice returns nil when no datoms match).
      For card-one history attrs with scan-attr, skips current index entirely."
     ^java.util.ArrayList [^PersistentSortedSet pss-a from to
                           ^PersistentSortedSet pss-b
                           index-type db scan-attr]
     (let [keep-history? (dbi/-keep-history? db)
           scan-attr-current-ok? (if scan-attr
                                   (or (not keep-history?)
                                       (dbu/no-history? db scan-attr)
                                       (dbu/multival? db scan-attr))
                                   true)]
       (if (and scan-attr (not scan-attr-current-ok?))
         ;; Fast path: card-one history attr — skip current entirely
         (let [slice-b (di/-slice pss-b from to index-type)]
           (if slice-b
             (let [^java.util.Iterator iter-b (.iterator ^Iterable slice-b)
                   result (java.util.ArrayList. 4096)]
               (while (.hasNext iter-b) (.add result (.next iter-b)))
               result)
             (java.util.ArrayList. 0)))
         ;; General path: merge both iterators
         (let [slice-a (di/-slice pss-a from to index-type)
               slice-b (di/-slice pss-b from to index-type)]
           (cond
             (and (nil? slice-a) (nil? slice-b))
             (java.util.ArrayList. 0)

             (nil? slice-b)
             (let [^java.util.Iterator iter-a (.iterator ^Iterable slice-a)
                   result (java.util.ArrayList. 4096)]
               (while (.hasNext iter-a)
                 (let [^Datom d (.next iter-a)]
                   (when (or (not keep-history?)
                             scan-attr
                             (dbu/no-history? db (.-a d))
                             (dbu/multival? db (.-a d)))
                     (.add result d))))
               result)

             (nil? slice-a)
             (let [^java.util.Iterator iter-b (.iterator ^Iterable slice-b)
                   result (java.util.ArrayList. 4096)]
               (while (.hasNext iter-b) (.add result (.next iter-b)))
               result)

             :else
             (let [cmp (.comparator pss-a)
                   ^java.util.Iterator iter-a (.iterator ^Iterable slice-a)
                   ^java.util.Iterator iter-b (.iterator ^Iterable slice-b)
                   result (java.util.ArrayList. 4096)]
               (letfn [(current-ok? [^Datom d]
                         (or scan-attr
                             (not keep-history?)
                             (dbu/no-history? db (.-a d))
                             (dbu/multival? db (.-a d))))
                       (next-ok-a []
                         (loop [d (when (.hasNext iter-a) (.next iter-a))]
                           (cond (nil? d) nil
                                 (current-ok? d) d
                                 (.hasNext iter-a) (recur (.next iter-a))
                                 :else nil)))]
                 (loop [a (next-ok-a)
                        b (when (.hasNext iter-b) (.next iter-b))]
                   (cond
                     (and (nil? a) (nil? b)) result
                     (nil? a) (do (.add result b)
                                  (while (.hasNext iter-b) (.add result (.next iter-b)))
                                  result)
                     (nil? b) (do (.add result a)
                                  (while (.hasNext iter-a)
                                    (let [d (.next iter-a)]
                                      (when (current-ok? d) (.add result d))))
                                  result)
                     :else
                     (let [c (.compare ^java.util.Comparator cmp a b)]
                       (cond
                         (< c 0) (do (.add result a) (recur (next-ok-a) b))
                         (> c 0) (do (.add result b)
                                     (recur a (when (.hasNext iter-b) (.next iter-b))))
                         :else   (do (.add result a) ;; dedup: keep first
                                     (recur (next-ok-a)
                                            (when (.hasNext iter-b) (.next iter-b))))))))))))))))

(defn- temporal-merge-slice
  "Get merged datoms for [eid attr] from current+temporal indexes.
   For historical: merge via distinct-datoms (all versions).
   For as-of/since: merge + post-process-datoms (time filter + assemble)."
  [origin-db from-d to-d temporal-type temporal-tx-filter db]
  (let [current-slice (di/-slice (:eavt origin-db) from-d to-d :eavt)]
    (case temporal-type
      :historical
      (let [temporal-index (:temporal-eavt origin-db)]
        (if temporal-index
          (dbu/distinct-datoms origin-db :eavt
                               current-slice
                               (di/-slice temporal-index from-d to-d :eavt))
          current-slice))

      (:as-of :since)
      (let [temporal-index (:temporal-eavt origin-db)
            merged (if temporal-index
                     (dbu/distinct-datoms origin-db :eavt
                                          current-slice
                                          (di/-slice temporal-index from-d to-d :eavt))
                     current-slice)
            ctx (dbi/-search-context db)]
        (db/post-process-datoms merged origin-db ctx))

      ;; regular DB
      current-slice)))

(defn- build-scan-slice
  "Build the scan slice for execute-group-direct / execute-fused-scan-rel.
   For temporal DBs, merges current + temporal indexes.
   db is the temporal wrapper (needed for search-context), origin-db is the unwrapped DB."
  [db db-index from-datom to-datom index temporal origin-db resolved-a]
  (if-not temporal
    (di/-slice db-index from-datom to-datom index)
    (let [temporal-type (:type temporal)
          as-of-at-max-tx? (and (= temporal-type :as-of)
                                (= (long (:time-point temporal))
                                   (long (:max-tx origin-db))))
          temporal-index-key (keyword (str "temporal-" (name index)))]
      (case temporal-type
        :historical
        (let [temporal-index (get origin-db temporal-index-key)]
          (if temporal-index
            #?(:clj (fast-merge-scan db-index from-datom to-datom
                                     temporal-index index origin-db resolved-a)
               :cljs (dbu/distinct-datoms origin-db index
                                          (di/-slice db-index from-datom to-datom index)
                                          (di/-slice temporal-index from-datom to-datom index)))
            (di/-slice db-index from-datom to-datom index)))

        (:as-of :since)
        (if as-of-at-max-tx?
          (di/-slice db-index from-datom to-datom index)
          (let [temporal-index (get origin-db temporal-index-key)]
            (if temporal-index
              (let [merged (dbu/distinct-datoms origin-db index
                                                (di/-slice db-index from-datom to-datom index)
                                                (di/-slice temporal-index from-datom to-datom index))
                    ctx (dbi/-search-context db)]
                (db/post-process-datoms merged origin-db ctx))
              (di/-slice db-index from-datom to-datom index))))

        ;; default: regular slice
        (di/-slice db-index from-datom to-datom index)))))

;; ---------------------------------------------------------------------------
;; Path functions — each small enough for JIT C2/Graal compilation.
;; The macros scan-filter and emit-tuple expand inline within each path.

(defn- execute-scan-only
  "Path 1: Scan without merges (e.g. Q1). Smallest possible loop."
  [slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n]
  (let [^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (let [^Datom scan-d (.next iter)]
             (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
               (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                           n-find find-source const-vals result-list)))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
           (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                       n-find find-source const-vals result-list))))))

(defn- execute-card-many-merge
  "Path 2: Card-many recursive cross-product merge.
   merge-ctx is [merge-attrs merge-v-ground merge-v-vals merge-anti
                 merge-card-many merge-check-scan-v merge-check-scan-tx merge-cursors]."
  [db eavt-pss slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n n-merges merge-ctx]
  (let [^objects merge-attrs (aget ^objects merge-ctx 0)
        ^objects merge-v-ground (aget ^objects merge-ctx 1)
        ^objects merge-v-vals (aget ^objects merge-ctx 2)
        ^objects merge-anti (aget ^objects merge-ctx 3)
        ^objects merge-card-many (aget ^objects merge-ctx 4)
        ^objects merge-check-scan-v (aget ^objects merge-ctx 5)
        ^objects merge-check-scan-tx (aget ^objects merge-ctx 6)
        ^objects merge-cursors (aget ^objects merge-ctx 7)
        ^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (let [^Datom scan-d (.next iter)]
             (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
               (let [eid (.-e scan-d)]
                 (letfn [(process-merges [mi]
                           (if (>= mi n-merges)
                             (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                                         n-find find-source const-vals result-list)
                             (let [anti? (aget merge-anti mi)
                                   ra (aget merge-attrs mi)
                                   vg? (aget merge-v-ground mi)
                                   vgv (aget merge-v-vals mi)
                                   card-many? (aget merge-card-many mi)
                                   check-v? (aget merge-check-scan-v mi)
                                   check-tx? (aget merge-check-scan-tx mi)]
                               (if card-many?
                                 (let [from-d (datom eid ra (when vg? vgv) tx0)
                                       to-d (datom eid ra (when vg? vgv) txmax)
                                       mslice (di/-slice (:eavt db) from-d to-d :eavt)]
                                   (if anti?
                                     (when (not-any? (fn [^Datom d] (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d)) mslice)
                                       (process-merges (inc mi)))
                                     (doseq [^Datom d mslice]
                                       (when (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d)
                                         (aset merge-datoms mi d)
                                         (process-merges (inc mi)))))))
                               (let [probe (datom eid ra vgv tx0)
                                     ^Datom d (if merge-cursors
                                                (.seekGE ^PersistentSortedSet$ForwardCursor
                                                 (aget merge-cursors mi) probe)
                                                (.lookupGE ^PersistentSortedSet eavt-pss probe))
                                     found? (and d (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d))]
                                 (if anti?
                                   (when (not found?)
                                     (process-merges (inc mi)))
                                   (when found?
                                     (aset merge-datoms mi d)
                                     (process-merges (inc mi))))))))]
                   (process-merges 0)))))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
           (let [eid (.-e ^Datom scan-d)]
             (letfn [(process-merges [mi]
                       (if (>= mi n-merges)
                         (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                                     n-find find-source const-vals result-list)
                         (let [anti? (aget merge-anti mi)
                               ra (aget merge-attrs mi)
                               vg? (aget merge-v-ground mi)
                               vgv (aget merge-v-vals mi)
                               card-many? (aget merge-card-many mi)
                               check-v? (aget merge-check-scan-v mi)
                               check-tx? (aget merge-check-scan-tx mi)]
                           (if card-many?
                             (let [from-d (datom eid ra (when vg? vgv) tx0)
                                   to-d (datom eid ra (when vg? vgv) txmax)
                                   mslice (di/-slice (:eavt db) from-d to-d :eavt)]
                               (if anti?
                                 (when (not-any? (fn [^Datom d] (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d)) mslice)
                                   (process-merges (inc mi)))
                                 (doseq [^Datom d mslice]
                                   (when (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d)
                                     (aset merge-datoms mi d)
                                     (process-merges (inc mi))))))
                             (let [probe (datom eid ra vgv tx0)
                                   ^Datom d (pss-lookup-ge eavt-pss probe)
                                   found? (and d (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d))]
                               (if anti?
                                 (when (not found?)
                                   (process-merges (inc mi)))
                                 (when found?
                                   (aset merge-datoms mi d)
                                   (process-merges (inc mi)))))))))]
               (process-merges 0))))))))

#?(:clj
   (defmacro ^:private sorted-merge-inner-loop
     "Inner sorted-merge loop body parameterized by attr type.
      Expects these symbols bound in enclosing scope: sorted-cursor, sorted-order,
      merge-v-ground, merge-v-vals, merge-check-scan-v, merge-check-scan-tx,
      merge-datoms, collect-set, collect-datom-field, collect-merge-idx,
      n-find, find-source, const-vals, result-list, n-merges, scan-d, eid.
      Only the differing attr-comparison forms are passed as parameters."
     [get-first-attr-expr make-probe-expr
      get-cur-a-expr get-target-a-expr
      match-cond less-cond]
     `(let [~'first-attr ~get-first-attr-expr
            ~'probe ~make-probe-expr
            ~(with-meta 'd {:tag 'datahike.datom.Datom})
            (.seekGE ~(with-meta 'sorted-cursor {:tag 'org.replikativ.persistent_sorted_set.PersistentSortedSet$ForwardCursor}) ~'probe)]
        (when (and ~'d (== (.-e ~'d) ~'eid))
          (let [~'ok? (loop [~'si (int 0)
                             ~(with-meta 'cur-d {:tag 'datahike.datom.Datom}) ~'d]
                        (if (>= ~'si ~'n-merges)
                          true
                          (if (or (nil? ~'cur-d) (not (== (.-e ~'cur-d) ~'eid)))
                            false
                            (let [~'cur-a ~get-cur-a-expr
                                  ~'target-a ~get-target-a-expr]
                              (cond
                                ~match-cond
                                (let [~'orig-mi (aget ~(with-meta 'sorted-order {:tag 'ints}) ~'si)
                                      ~'vg? (aget ~'merge-v-ground ~'orig-mi)
                                      ~'vgv (aget ~'merge-v-vals ~'orig-mi)]
                                  (if (and (or (not ~'vg?) (val-eq? (.-v ~'cur-d) ~'vgv))
                                           (or (not (aget ~'merge-check-scan-v ~'orig-mi)) (val-eq? (.-v ~'cur-d) (.-v ~(with-meta 'scan-d {:tag 'datahike.datom.Datom}))))
                                           (or (not (aget ~'merge-check-scan-tx ~'orig-mi)) (= (datom/datom-tx ~'cur-d) (datom/datom-tx ~(with-meta 'scan-d {:tag 'datahike.datom.Datom})))))
                                    (do (aset ~'merge-datoms ~'orig-mi ~'cur-d)
                                        (recur (unchecked-inc-int ~'si)
                                               (.next ~(with-meta 'sorted-cursor {:tag 'org.replikativ.persistent_sorted_set.PersistentSortedSet$ForwardCursor}))))
                                    (recur ~'si (.next ~(with-meta 'sorted-cursor {:tag 'org.replikativ.persistent_sorted_set.PersistentSortedSet$ForwardCursor})))))

                                ~less-cond
                                (recur ~'si (.next ~(with-meta 'sorted-cursor {:tag 'org.replikativ.persistent_sorted_set.PersistentSortedSet$ForwardCursor})))

                                :else false)))))]
            (when ~'ok?
              (emit-tuple ~'scan-d ~'collect-set ~'collect-datom-field ~'collect-merge-idx ~'merge-datoms
                          ~'n-find ~'find-source ~'const-vals ~'result-list)))))))

#?(:clj
   (defn- execute-sorted-merge
     "Path 3: Sorted single-cursor merge (CLJ only, the hot path for Q2-Q4).
      Uses one ForwardCursor with EA comparator, seekGE per entity to first attr,
      then next() through sorted merge attrs.
      sorted-ctx is [sorted-cursor sorted-order sorted-attrs-long sorted-attrs-obj
                     merge-v-ground merge-v-vals merge-check-scan-v merge-check-scan-tx]."
     [slice ground-filter strict-filter
      probe-set probe-datom-field
      collect-set collect-datom-field collect-merge-idx
      merge-datoms n-find find-source const-vals
      result-list max-n n-merges attr-refs? sorted-ctx]
     (let [sorted-cursor (aget ^objects sorted-ctx 0)
           ^ints sorted-order (aget ^objects sorted-ctx 1)
           ^longs sorted-attrs-long (aget ^objects sorted-ctx 2)
           ^objects sorted-attrs-obj (aget ^objects sorted-ctx 3)
           ^objects merge-v-ground (aget ^objects sorted-ctx 4)
           ^objects merge-v-vals (aget ^objects sorted-ctx 5)
           ^objects merge-check-scan-v (aget ^objects sorted-ctx 6)
           ^objects merge-check-scan-tx (aget ^objects sorted-ctx 7)
           ^objects merge-datoms merge-datoms
           ^ints find-source find-source
           ^objects const-vals const-vals]
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (if attr-refs?
         ;; Attribute-refs path: long comparison
           (while (and (.hasNext iter)
                       (or (neg? max-n) (< (result-list-size result-list) max-n)))
             (let [^Datom scan-d (.next iter)]
               (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
                 (let [eid (.-e scan-d)]
                   (sorted-merge-inner-loop
                    (aget ^longs sorted-attrs-long 0)
                    (Datom. eid first-attr nil tx0 0)
                    (long (.-a ^Datom cur-d))
                    (aget ^longs sorted-attrs-long si)
                    (== cur-a target-a)
                    (< cur-a target-a))))))
         ;; Keyword attrs path: compare-based comparison
           (while (and (.hasNext iter)
                       (or (neg? max-n) (< (result-list-size result-list) max-n)))
             (let [^Datom scan-d (.next iter)]
               (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
                 (let [eid (.-e scan-d)]
                   (sorted-merge-inner-loop
                    (aget sorted-attrs-obj 0)
                    (datom eid first-attr nil tx0)
                    (.-a ^Datom cur-d)
                    (aget sorted-attrs-obj si)
                    (zero? (compare cur-a target-a))
                    (neg? (compare cur-a target-a))))))))))))

(defn- execute-per-cursor-merge
  "Path 4: Per-cursor or lookupGE merge (fallback for anti-merges, non-sorted).
   merge-ctx is [merge-attrs merge-v-ground merge-v-vals merge-anti merge-cursors
                 merge-check-scan-v merge-check-scan-tx]."
  [eavt-pss slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n n-merges merge-ctx]
  (let [^objects merge-attrs (aget ^objects merge-ctx 0)
        ^objects merge-v-ground (aget ^objects merge-ctx 1)
        ^objects merge-v-vals (aget ^objects merge-ctx 2)
        ^objects merge-anti (aget ^objects merge-ctx 3)
        ^objects merge-cursors (aget ^objects merge-ctx 4)
        ^objects merge-check-scan-v (aget ^objects merge-ctx 5)
        ^objects merge-check-scan-tx (aget ^objects merge-ctx 6)
        ^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (let [^Datom scan-d (.next iter)]
             (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
               (let [eid (.-e scan-d)
                     ok? (loop [mi (int 0) ok? true]
                           (if (or (not ok?) (>= mi n-merges))
                             ok?
                             (let [anti? (aget merge-anti mi)
                                   ra (aget merge-attrs mi)
                                   vg? (aget merge-v-ground mi)
                                   vgv (aget merge-v-vals mi)
                                   probe (datom eid ra vgv tx0)
                                   ^Datom d (if merge-cursors
                                              (.seekGE ^PersistentSortedSet$ForwardCursor
                                               (aget merge-cursors mi) probe)
                                              (.lookupGE ^PersistentSortedSet eavt-pss probe))
                                   found? (and d
                                               (== (.-e d) eid)
                                               (= (.-a d) ra)
                                               (or (not vg?) (val-eq? (.-v d) vgv))
                                               (or (not (aget merge-check-scan-v mi)) (val-eq? (.-v d) (.-v scan-d)))
                                               (or (not (aget merge-check-scan-tx mi)) (= (datom/datom-tx d) (datom/datom-tx scan-d))))]
                               (if anti?
                                 (recur (unchecked-inc-int mi) (not found?))
                                 (if found?
                                   (do (aset merge-datoms mi d)
                                       (recur (unchecked-inc-int mi) true))
                                   (recur (unchecked-inc-int mi) false))))))]
                 (when ok?
                   (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                               n-find find-source const-vals result-list)))))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
           (let [eid (.-e ^Datom scan-d)
                 ok? (loop [mi (int 0) ok? true]
                       (if (or (not ok?) (>= mi n-merges))
                         ok?
                         (let [anti? (aget merge-anti mi)
                               ra (aget merge-attrs mi)
                               vg? (aget merge-v-ground mi)
                               vgv (aget merge-v-vals mi)
                               probe (datom eid ra vgv tx0)
                               ^Datom d (pss-lookup-ge eavt-pss probe)
                               found? (and d
                                           (== (.-e d) eid)
                                           (= (.-a d) ra)
                                           (or (not vg?) (val-eq? (.-v d) vgv))
                                           (or (not (aget merge-check-scan-v mi)) (val-eq? (.-v d) (.-v scan-d)))
                                           (or (not (aget merge-check-scan-tx mi)) (= (datom/datom-tx d) (datom/datom-tx scan-d))))]
                           (if anti?
                             (recur (inc mi) (not found?))
                             (if found?
                               (do (aset merge-datoms mi d)
                                   (recur (inc mi) true))
                               (recur (inc mi) false))))))]
             (when ok?
               (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                           n-find find-source const-vals result-list))))))))

;; ---------------------------------------------------------------------------
;; Shared dispatcher helpers (cold path — called once per query group).

(defn- build-merge-attrs
  "Build merge-attrs array: resolved attribute for each merge op."
  [resolve-db merge-ops]
  (to-array (mapv (fn [op]
                    (let [ma (second (:clause op))]
                      (when (and (some? ma) (not (symbol? ma)))
                        (resolve-attr resolve-db ma))))
                  merge-ops)))

(defn- build-common-merge-arrays
  "Build merge arrays shared between temporal and non-temporal dispatchers.
   Returns [merge-v-ground merge-v-vals merge-anti merge-check-scan-v merge-check-scan-tx]."
  [merge-ops scan-clause]
  [(to-array (mapv (fn [op]
                     (let [mv (get (:clause op) 2)]
                       (boolean (and (some? mv) (not (symbol? mv))))))
                   merge-ops))
   (to-array (mapv (fn [op]
                     (let [mv (get (:clause op) 2)]
                       (when (and (some? mv) (not (symbol? mv))) mv)))
                   merge-ops))
   (to-array (mapv #(boolean (:anti? %)) merge-ops))
   (to-array (mapv (fn [op]
                     (let [mv (get (:clause op) 2)
                           sv (get scan-clause 2)]
                       (boolean (and (analyze/free-var? mv) (analyze/free-var? sv) (= mv sv)))))
                   merge-ops))
   (to-array (mapv (fn [op]
                     (let [mtx (get (:clause op) 3)
                           stx (get scan-clause 3)]
                       (boolean (and (analyze/free-var? mtx) (analyze/free-var? stx) (= mtx stx)))))
                   merge-ops))])

(defn- build-find-source-array
  "Build find-source projection array mapping find-vars to datom field positions.
   Mutates const-vals to store constant values."
  [find-vars consts clause merge-clauses n-find ^objects const-vals]
  (#?(:clj int-array :cljs to-array)
   (map-indexed
    (fn [fi fvar]
      (if (and consts (contains? consts fvar))
        (do (aset const-vals fi (get consts fvar))
            find-src-const)
        (or
         (some (fn [[i x]]
                 (when (= x fvar)
                   (case (int i)
                     0 find-src-scan-e 1 find-src-scan-a
                     2 find-src-scan-v 3 find-src-scan-tx
                     4 find-src-scan-added nil)))
               (map-indexed vector clause))
         (some (fn [[mi mc]]
                 (some (fn [[i x]]
                         (when (= x fvar)
                           (case (int i)
                             0 (+ mi find-src-merge-e-base) 1 (+ mi find-src-merge-a-base)
                             2 (+ mi find-src-merge-v-base) 3 (+ mi find-src-merge-tx-base)
                             4 (+ mi find-src-merge-added-base) nil)))
                       (map-indexed vector mc)))
               (map-indexed vector merge-clauses))
         0)))
    find-vars)))

;; ---------------------------------------------------------------------------
;; Dispatcher — setup + dispatch to the appropriate path function.

(defn- execute-group-direct
  "Execute an entity-group (or single pattern-scan) fused, writing results
   directly into a result list. Computes shared setup, then dispatches to
   a path-specific function (each small enough for JIT C2/Graal compilation)."
  [db scan-op merge-ops find-vars consts
   result-list
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   max-results
   & {:keys [scan-estimate pipeline]}]
  (let [{:keys [clause index pushdown-preds]} scan-op
        [e a v tx] clause
        resolved-a (when (and (some? a) (not (symbol? a))) (resolve-attr db a))
        resolved-e (when (and (some? e) (not (symbol? e)) (number? e)) #?(:clj (long e) :cljs e))
        pushdown-bounds (when (seq pushdown-preds) (plan/pushdown-to-bounds pushdown-preds))
        [from-datom to-datom] (compute-slice-bounds clause index pushdown-bounds resolved-a resolved-e)
        db-index (get db index)
        ground-filter (build-ground-filter clause index)
        strict-filter (when-let [strict (:strict-preds pushdown-bounds)]
                        (let [scan-var-map (rel/var-mapping clause (range))
                              pushdown-var (first (keep :var pushdown-preds))]
                          (when pushdown-var
                            (build-strict-filter strict (get scan-var-map pushdown-var)))))

        eavt-pss (:eavt db)
        n-merges (count merge-ops)

        ;; Pre-extract merge info into arrays for fast inner loop
        ^objects merge-attrs (build-merge-attrs db merge-ops)
        [merge-v-ground* merge-v-vals* merge-anti*
         merge-check-scan-v* merge-check-scan-tx*] (build-common-merge-arrays merge-ops clause)
        ^objects merge-v-ground merge-v-ground*
        ^objects merge-v-vals merge-v-vals*
        ^objects merge-anti merge-anti*
        ^objects merge-check-scan-v merge-check-scan-v*
        ^objects merge-check-scan-tx merge-check-scan-tx*
        merge-card-many (to-array (mapv (fn [op]
                                          (not (get-in op [:schema-info :card-one?] true)))
                                        merge-ops))
        has-card-many? (some true? (seq merge-card-many))

        _ (assert pipeline "Plans must have :pipeline annotation")
        use-cursors? (:use-cursors? pipeline)
        attr-refs? (:attr-refs? pipeline)
        fused-path (:fused-path pipeline)

        ;; Build find-source projection
        merge-clauses (mapv :clause merge-ops)
        n-find (count find-vars)
        const-vals #?(:clj (object-array n-find) :cljs (make-array n-find))
        find-source (build-find-source-array find-vars consts clause merge-clauses n-find const-vals)

        merge-datoms #?(:clj (object-array n-merges) :cljs (make-array n-merges))
        ;; Probe-driven AVET optimization: when the probe-set is small relative
        ;; to the scan size and the probed field is the value position, use targeted
        ;; AVET seeks instead of scanning all datoms and filtering.
        use-probe-driven? #?(:clj (let [probe-field (int probe-datom-field)]
                                    (and probe-set
                                         resolved-a
                                         (or (= probe-field 0) (= probe-field 2)) ;; entity or value position
                                         (if (= probe-field 2) (some? (:avet db)) true) ;; value needs AVET; entity always has EAVT
                                         (let [est (long (or scan-estimate (di/-count db-index)))]
                                           (and (pos? est)
                                                (< (* (long (probe-set-size probe-set)) (long probe-driven-threshold))
                                                   est)))))
                             :cljs false)
        slice (if use-probe-driven?
                (let [pf (int probe-datom-field)]
                  (probe-driven-iterable
                   (if (= pf 2) (:avet db) (:eavt db))
                   resolved-a probe-set pf))
                (di/-slice db-index from-datom to-datom index))
        ;; When probe-driven, filtering is baked into the seeks — nil out probe-set
        probe-set (if use-probe-driven? nil probe-set)
        max-n (int (or max-results -1))]

    ;; Dispatch to path-specific function via fused-path keyword
    (case fused-path
      :scan-only
      (execute-scan-only slice ground-filter strict-filter
                         probe-set probe-datom-field
                         collect-set collect-datom-field collect-merge-idx
                         merge-datoms n-find find-source const-vals
                         result-list max-n)

      :card-many-merge
      (let [merge-cursors #?(:clj (when use-cursors?
                                    (let [cursors (object-array n-merges)]
                                      (dotimes [i n-merges]
                                        (aset cursors i
                                              (if (aget merge-v-ground i)
                                                (.forwardCursor ^PersistentSortedSet eavt-pss)
                                                (.forwardCursor ^PersistentSortedSet eavt-pss ^java.util.Comparator fast-cmp-ea))))
                                      cursors))
                             :cljs nil)
            merge-ctx (object-array [merge-attrs merge-v-ground merge-v-vals merge-anti
                                     merge-card-many merge-check-scan-v merge-check-scan-tx merge-cursors])]
        (execute-card-many-merge db eavt-pss slice ground-filter strict-filter
                                 probe-set probe-datom-field
                                 collect-set collect-datom-field collect-merge-idx
                                 merge-datoms n-find find-source const-vals
                                 result-list max-n n-merges merge-ctx))

      :sorted-merge
      #?(:clj
         (let [sorted-order (let [indexed (mapv (fn [i] [(aget merge-attrs i) i]) (range n-merges))
                                  sorted (sort-by first compare indexed)]
                              (int-array (mapv second sorted)))
               sorted-attrs-long (when attr-refs?
                                   (let [arr (long-array n-merges)]
                                     (dotimes [si n-merges]
                                       (aset arr si (long (aget merge-attrs (aget sorted-order si)))))
                                     arr))
               sorted-attrs-obj (when (not attr-refs?)
                                  (let [arr (object-array n-merges)]
                                    (dotimes [si n-merges]
                                      (aset arr si (aget merge-attrs (aget sorted-order si))))
                                    arr))
               sorted-cursor (.forwardCursor ^PersistentSortedSet eavt-pss ^java.util.Comparator fast-cmp-ea)]
           (execute-sorted-merge slice ground-filter strict-filter
                                 probe-set probe-datom-field
                                 collect-set collect-datom-field collect-merge-idx
                                 merge-datoms n-find find-source const-vals
                                 result-list max-n n-merges attr-refs?
                                 (object-array [sorted-cursor sorted-order
                                                sorted-attrs-long sorted-attrs-obj
                                                merge-v-ground merge-v-vals
                                                merge-check-scan-v merge-check-scan-tx])))
         :cljs nil)

      :per-cursor-merge
      (let [merge-cursors #?(:clj (when use-cursors?
                                    (let [cursors (object-array n-merges)]
                                      (dotimes [i n-merges]
                                        (aset cursors i
                                              (if (aget merge-v-ground i)
                                                (.forwardCursor ^PersistentSortedSet eavt-pss)
                                                (.forwardCursor ^PersistentSortedSet eavt-pss ^java.util.Comparator fast-cmp-ea))))
                                      cursors))
                             :cljs nil)]
        (execute-per-cursor-merge eavt-pss slice ground-filter strict-filter
                                  probe-set probe-datom-field
                                  collect-set collect-datom-field collect-merge-idx
                                  merge-datoms n-find find-source const-vals
                                  result-list max-n n-merges
                                  (object-array [merge-attrs merge-v-ground merge-v-vals merge-anti merge-cursors
                                                 merge-check-scan-v merge-check-scan-tx]))))

    result-list))

(defn- execute-temporal-scan-only
  "Temporal path 1: scan without merges."
  [slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n temporal-tx-filter scan-added-val]
  (let [^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (let [^Datom scan-d (.next iter)]
             (when (and (scan-filter-temporal scan-d ground-filter strict-filter probe-set probe-datom-field temporal-tx-filter)
                        (or (nil? scan-added-val) (= (datom/datom-added scan-d) scan-added-val)))
               (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                           n-find find-source const-vals result-list)))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (when (and (scan-filter-temporal scan-d ground-filter strict-filter probe-set probe-datom-field temporal-tx-filter)
                    (or (nil? scan-added-val) (= (datom/datom-added scan-d) scan-added-val)))
           (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                       n-find find-source const-vals result-list))))))

(defn- execute-temporal-merge
  "Temporal path 2: merge with card-many/card-one, anti-merge, cursor cache.
   temporal-ctx packs temporal-specific arrays to stay under 20-param limit."
  [db eavt-pss slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n n-merges temporal-ctx]
  (let [^objects merge-attrs (aget ^objects temporal-ctx 0)
        ^objects merge-v-ground (aget ^objects temporal-ctx 1)
        ^objects merge-v-vals (aget ^objects temporal-ctx 2)
        ^objects merge-anti (aget ^objects temporal-ctx 3)
        ^objects merge-card-many (aget ^objects temporal-ctx 4)
        ^objects merge-added-filter (aget ^objects temporal-ctx 5)
        ^objects merge-check-scan-v (aget ^objects temporal-ctx 6)
        ^objects merge-check-scan-tx (aget ^objects temporal-ctx 7)
        ^objects merge-temporal-only (aget ^objects temporal-ctx 8)
        ^objects merge-cursor-cache (aget ^objects temporal-ctx 9)
        temporal-eavt-pss (aget ^objects temporal-ctx 10)
        ^objects temporal-cursors (aget ^objects temporal-ctx 11)
        temporal-type (aget ^objects temporal-ctx 12)
        temporal-tx-filter (aget ^objects temporal-ctx 13)
        scan-added-val (aget ^objects temporal-ctx 14)
        origin-db (aget ^objects temporal-ctx 15)
        ^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (let [^Datom scan-d (.next iter)]
             (when (and (scan-filter-temporal scan-d ground-filter strict-filter probe-set probe-datom-field temporal-tx-filter)
                        (or (nil? scan-added-val) (= (datom/datom-added scan-d) scan-added-val)))
               (let [eid (.-e scan-d)]
                 (letfn [(process-merges [mi]
                           (if (>= mi n-merges)
                             (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                                         n-find find-source const-vals result-list)
                             (let [anti? (aget merge-anti mi)
                                   ra (aget merge-attrs mi)
                                   vg? (aget merge-v-ground mi)
                                   vgv (aget merge-v-vals mi)
                                   card-many? (aget merge-card-many mi)
                                   added-filter (aget merge-added-filter mi)]
                               (if card-many?
                                 (let [temporal-only? (aget merge-temporal-only mi)]
                                   (if (and temporal-only? temporal-cursors (aget temporal-cursors mi)
                                            anti?)
                                   ;; Fast path: ForwardCursor on temporal index (anti-merge only).
                                     (let [^longs cache-eid-arr (aget merge-cursor-cache mi)
                                           cached-eid (aget cache-eid-arr 0)]
                                       (if (== cached-eid (long eid))
                                         (let [cached (aget merge-datoms mi)]
                                           (when (nil? cached) (process-merges (inc mi))))
                                         (let [probe (datom eid ra (when vg? vgv) tx0)
                                               ^PersistentSortedSet$ForwardCursor cur (aget temporal-cursors mi)
                                               ^Datom d (.seekGE cur probe)
                                               found (volatile! nil)]
                                           (do (aset cache-eid-arr 0 (long eid))
                                               (let [check-v? (aget merge-check-scan-v mi)
                                                     check-tx? (aget merge-check-scan-tx mi)
                                                     match (loop [^Datom md d]
                                                             (cond
                                                               (or (nil? md) (not (== (.-e md) eid)) (not (= (.-a md) ra)))
                                                               nil
                                                               (and (or (not vg?) (val-eq? (.-v md) vgv))
                                                                    (or (nil? added-filter) (= (datom/datom-added md) added-filter))
                                                                    (or (not check-v?) (val-eq? (.-v md) (.-v scan-d)))
                                                                    (or (not check-tx?) (= (datom/datom-tx md) (datom/datom-tx scan-d))))
                                                               md
                                                               :else (recur (.next cur))))]
                                                 (if match
                                                   (aset merge-datoms mi match)
                                                   (do (aset merge-datoms mi nil)
                                                       (process-merges (inc mi)))))))))
                                   ;; General path: slice-based merge
                                     (let [from-d (datom eid ra (when vg? vgv) tx0)
                                           to-d (datom eid ra (when vg? vgv) txmax)
                                           mslice (if (aget merge-temporal-only mi)
                                                    (di/-slice temporal-eavt-pss from-d to-d :eavt)
                                                    (temporal-merge-slice origin-db from-d to-d temporal-type temporal-tx-filter db))
                                           check-v? (aget merge-check-scan-v mi)
                                           check-tx? (aget merge-check-scan-tx mi)]
                                       (if anti?
                                         (when (not-any? (fn [^Datom d] (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)) mslice)
                                           (process-merges (inc mi)))
                                         (doseq [^Datom d mslice]
                                           (when (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)
                                             (aset merge-datoms mi d)
                                             (process-merges (inc mi))))))))
                               ;; Card-one merge
                                 (if (nil? temporal-type)
                                   (let [probe (datom eid ra vgv tx0)
                                         ^Datom d (.lookupGE ^PersistentSortedSet eavt-pss probe)
                                         check-v? (aget merge-check-scan-v mi)
                                         check-tx? (aget merge-check-scan-tx mi)
                                         found? (and d (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter))]
                                     (if anti?
                                       (when (not found?) (process-merges (inc mi)))
                                       (when found?
                                         (aset merge-datoms mi d)
                                         (process-merges (inc mi)))))
                                 ;; Temporal card-one: for as-of/since, try direct lookupGE
                                 ;; on current EAVT (avoids lazy-seq merge overhead per entity).
                                   (if (and (not= temporal-type :historical) temporal-tx-filter)
                                   ;; Fast path: lookupGE on current EAVT, check tx filter
                                     (let [probe (datom eid ra vgv tx0)
                                           ^Datom d (.lookupGE ^PersistentSortedSet eavt-pss probe)
                                           check-v? (aget merge-check-scan-v mi)
                                           check-tx? (aget merge-check-scan-tx mi)
                                           found? (and d (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter))]
                                       (if found?
                                         (if anti?
                                           nil ;; anti + found → skip
                                           (do (aset merge-datoms mi d)
                                               (process-merges (inc mi))))
                                       ;; Not in current or tx too new — check temporal index
                                         (let [temporal-eavt (:temporal-eavt origin-db)]
                                           (if temporal-eavt
                                             (let [^Datom td (.lookupGE ^PersistentSortedSet temporal-eavt probe)
                                                   found-t? (and td (temporal-merge-datom-match? td eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter))]
                                               (if found-t?
                                                 (if anti? nil
                                                     (do (aset merge-datoms mi td)
                                                         (process-merges (inc mi))))
                                                 (when anti? (process-merges (inc mi)))))
                                             (when anti? (process-merges (inc mi)))))))
                                   ;; Historical: full temporal-merge-slice (needs all versions)
                                     (let [from-d (datom eid ra (when vg? vgv) tx0)
                                           to-d (datom eid ra (when vg? vgv) txmax)
                                           mslice (temporal-merge-slice origin-db from-d to-d temporal-type temporal-tx-filter db)
                                           check-v? (aget merge-check-scan-v mi)
                                           check-tx? (aget merge-check-scan-tx mi)]
                                       (if anti?
                                         (when (not-any? (fn [^Datom d] (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)) mslice)
                                           (process-merges (inc mi)))
                                         (when-let [^Datom d (some (fn [^Datom d]
                                                                     (when (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter) d))
                                                                   mslice)]
                                           (aset merge-datoms mi d)
                                           (process-merges (inc mi)))))))))))]
                   (process-merges 0)))))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (when (and (scan-filter-temporal scan-d ground-filter strict-filter probe-set probe-datom-field temporal-tx-filter)
                    (or (nil? scan-added-val) (= (datom/datom-added scan-d) scan-added-val)))
           (let [eid (.-e ^Datom scan-d)]
             (letfn [(process-merges [mi]
                       (if (>= mi n-merges)
                         (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                                     n-find find-source const-vals result-list)
                         (let [anti? (aget merge-anti mi)
                               ra (aget merge-attrs mi)
                               vg? (aget merge-v-ground mi)
                               vgv (aget merge-v-vals mi)
                               card-many? (aget merge-card-many mi)
                               added-filter (aget merge-added-filter mi)]
                           (if card-many?
                             (let [from-d (datom eid ra (when vg? vgv) tx0)
                                   to-d (datom eid ra (when vg? vgv) txmax)
                                   temporal-only? (aget merge-temporal-only mi)
                                   mslice (if temporal-only?
                                            (di/-slice temporal-eavt-pss from-d to-d :eavt)
                                            (temporal-merge-slice origin-db from-d to-d temporal-type temporal-tx-filter db))
                                   check-v? (aget merge-check-scan-v mi)
                                   check-tx? (aget merge-check-scan-tx mi)]
                               (if anti?
                                 (when (not-any? (fn [^Datom d] (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)) mslice)
                                   (process-merges (inc mi)))
                                 (doseq [^Datom d mslice]
                                   (when (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)
                                     (aset merge-datoms mi d) (process-merges (inc mi))))))
                             (let [probe (datom eid ra vgv tx0)
                                   ^Datom d (pss-lookup-ge eavt-pss probe)
                                   check-v? (aget merge-check-scan-v mi)
                                   check-tx? (aget merge-check-scan-tx mi)
                                   found? (and d (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter))]
                               (if anti?
                                 (when (not found?) (process-merges (inc mi)))
                                 (when found?
                                   (aset merge-datoms mi d) (process-merges (inc mi)))))))))]
               (process-merges 0))))))))

(defn- execute-group-direct-temporal
  "Temporal variant of execute-group-direct for history/as-of/since queries.
   Dispatcher: computes setup, then calls execute-temporal-scan-only or execute-temporal-merge."
  [db scan-op merge-ops find-vars consts
   result-list
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   max-results
   temporal
   & {:keys [scan-estimate]}]
  (let [{:keys [clause index pushdown-preds]} scan-op
        [e a v tx] clause
        origin-db (:origin-db temporal)
        resolved-a (when (and (some? a) (not (symbol? a))) (resolve-attr origin-db a))
        resolved-e (when (and (some? e) (not (symbol? e)) (number? e)) #?(:clj (long e) :cljs e))
        pushdown-bounds (when (seq pushdown-preds) (plan/pushdown-to-bounds pushdown-preds))
        [from-datom to-datom] (compute-slice-bounds clause index pushdown-bounds resolved-a resolved-e)
        db-index (get origin-db index)
        ground-filter (build-ground-filter clause index)
        strict-filter (when-let [strict (:strict-preds pushdown-bounds)]
                        (let [scan-var-map (rel/var-mapping clause (range))
                              pushdown-var (first (keep :var pushdown-preds))]
                          (when pushdown-var
                            (build-strict-filter strict (get scan-var-map pushdown-var)))))
        temporal-type (:type temporal)
        temporal-tx-filter (build-temporal-tx-filter temporal)
        scan-added-val (let [added (get clause 4)]
                         (when (and (some? added) (not (symbol? added)) (boolean? added))
                           added))
        eavt-pss (:eavt origin-db)
        n-merges (count merge-ops)
        merge-attrs (build-merge-attrs origin-db merge-ops)
        [merge-v-ground* merge-v-vals* merge-anti*
         merge-check-scan-v* merge-check-scan-tx*] (build-common-merge-arrays merge-ops clause)
        ^objects merge-v-ground merge-v-ground*
        ^objects merge-v-vals merge-v-vals*
        ^objects merge-anti merge-anti*
        ^objects merge-check-scan-v merge-check-scan-v*
        ^objects merge-check-scan-tx merge-check-scan-tx*
        merge-card-many (to-array (mapv (fn [op]
                                          (or (= temporal-type :historical)
                                              (not (get-in op [:schema-info :card-one?] true))))
                                        merge-ops))
        merge-added-filter (to-array (mapv (fn [op]
                                             (let [added (get (:clause op) 4)]
                                               (when (and (some? added) (not (symbol? added)) (boolean? added))
                                                 added)))
                                           merge-ops))
        ;; Anti-merge shared-variable checks: when the merge clause shares a variable
        ;; with the scan clause at the value (pos 2) or tx (pos 3) position, the anti-merge
        ;; must verify the merge datom matches the scan datom at those positions.
        ;; Without this, NOT [?e :friend ?f ?tx false] would incorrectly match retractions
        ;; at a different tx than the scan datom.
        ;; For historical card-one attrs, temporal index is self-sufficient — skip current
        temporal-eavt-pss (when (= temporal-type :historical) (:temporal-eavt origin-db))
        merge-temporal-only (to-array (mapv (fn [op]
                                              (and (= temporal-type :historical)
                                                   (some? temporal-eavt-pss)
                                                   (get-in op [:schema-info :card-one?] true)
                                                   (not (dbu/no-history? origin-db
                                                                         (let [ma (second (:clause op))]
                                                                           (when (and (some? ma) (not (symbol? ma)))
                                                                             (resolve-attr origin-db ma)))))))
                                            merge-ops))
        ;; Per-merge ForwardCursors on temporal index (CLJ only).
        ;; Each merge gets its own independent cursor so merges can be processed
        ;; in plan order without requiring EA attribute ordering. A single shared
        ;; cursor fails when merge0=:name, merge1=:age because :age < :name in
        ;; EA order and the forward-only cursor can't seek backward.
        temporal-cursors
        #?(:clj (when (and temporal-eavt-pss (some true? (seq merge-temporal-only)))
                  (let [cursors (object-array n-merges)]
                    (dotimes [i n-merges]
                      (when (aget merge-temporal-only i)
                        (aset cursors i
                              (.forwardCursor ^PersistentSortedSet temporal-eavt-pss
                                              ^java.util.Comparator fast-cmp-ea))))
                    cursors))
           :cljs nil)
        merge-clauses (mapv :clause merge-ops)
        n-find (count find-vars)
        const-vals #?(:clj (object-array n-find) :cljs (make-array n-find))
        find-source (build-find-source-array find-vars consts clause merge-clauses n-find const-vals)
        merge-datoms #?(:clj (object-array n-merges) :cljs (make-array n-merges))
        ;; Per-merge cache for ForwardCursor: when the same entity appears in
        ;; consecutive scan datoms (common in historical mode), reuse the cached
        ;; merge results instead of re-seeking the forward-only cursor.
        ;; Each entry: [cached-eid cached-datoms-or-nil]
        merge-cursor-cache #?(:clj (object-array n-merges) :cljs nil)
        _ #?(:clj (dotimes [i n-merges] (aset merge-cursor-cache i (long-array 1 -1))) :cljs nil)
        use-probe-driven? #?(:clj (let [probe-field (int probe-datom-field)]
                                    (and probe-set
                                         resolved-a
                                         (or (= probe-field 0) (= probe-field 2))
                                         (if (= probe-field 2) (some? (:avet origin-db)) true)
                                         (let [est (long (or scan-estimate (di/-count db-index)))]
                                           (and (pos? est)
                                                (< (* (long (probe-set-size probe-set)) (long probe-driven-threshold))
                                                   est)))))
                             :cljs false)
        slice (if use-probe-driven?
                ;; Build concatenated temporal slices per probe value
                (let [pf (int probe-datom-field)
                      sorted-vals (sort (vec (.toArray ^java.util.HashSet probe-set)))
                      result (java.util.ArrayList.)]
                  (doseq [v sorted-vals]
                    (let [[from to] (if (== pf 0)
                                      [(datom (long v) resolved-a nil tx0)
                                       (datom (long v) resolved-a nil txmax)]
                                      [(datom e0 resolved-a v tx0)
                                       (datom emax resolved-a v txmax)])
                          sub-slice (build-scan-slice db
                                                      (if (== pf 2) (:avet origin-db) db-index)
                                                      from to
                                                      (if (== pf 2) :avet index)
                                                      temporal origin-db resolved-a)]
                      (when sub-slice
                        (let [iter (.iterator ^Iterable sub-slice)]
                          (while (.hasNext iter)
                            (.add result (.next iter)))))))
                  result)
                (build-scan-slice db db-index from-datom to-datom index
                                  temporal origin-db resolved-a))
        probe-set (if use-probe-driven? nil probe-set)
        max-n (int (or max-results -1))]

    (if (zero? n-merges)
      (execute-temporal-scan-only slice ground-filter strict-filter
                                  probe-set probe-datom-field
                                  collect-set collect-datom-field collect-merge-idx
                                  merge-datoms n-find find-source const-vals
                                  result-list max-n temporal-tx-filter scan-added-val)
      (execute-temporal-merge db eavt-pss slice ground-filter strict-filter
                              probe-set probe-datom-field
                              collect-set collect-datom-field collect-merge-idx
                              merge-datoms n-find find-source const-vals
                              result-list max-n n-merges
                              (object-array [merge-attrs merge-v-ground merge-v-vals merge-anti
                                             merge-card-many merge-added-filter
                                             merge-check-scan-v merge-check-scan-tx
                                             merge-temporal-only merge-cursor-cache
                                             temporal-eavt-pss temporal-cursors
                                             temporal-type temporal-tx-filter
                                             scan-added-val origin-db])))

    result-list))

;; Direct-to-output execution (main fast path)

(defn- entity-group-scan-op
  "Extract the scan-op from a plan op (entity-group or plain pattern-scan)."
  [op]
  (if (= :entity-group (:op op))
    (:scan-op op)
    op))

(defn- entity-group-merge-ops
  "Extract merge-ops from a plan op (entity-group or plain pattern-scan)."
  [op]
  (if (= :entity-group (:op op))
    (:merge-ops op)
    []))

(defn- find-probe-info
  "For a consumer entity-group, determine which datom field of its scan-op
   corresponds to the shared probe variable, and which merge-op index (if any)
   in the producer group produces it."
  [consumer-op producer-op probe-vars]
  (when (seq probe-vars)
    (let [probe-var (first probe-vars) ;; For now: single probe var
          ;; Where is probe-var in consumer's scan clause?
          consumer-clause (:clause (entity-group-scan-op consumer-op))
          consumer-scan-field (some (fn [[i x]]
                                      (when (= x probe-var) i))
                                    (map-indexed vector consumer-clause))
          ;; Where is probe-var produced in the producer?
          ;; Check producer's scan clause first, then merge clauses
          producer-scan-clause (:clause (entity-group-scan-op producer-op))
          producer-merge-clauses (mapv :clause (entity-group-merge-ops producer-op))
          producer-source
          (or (some (fn [[i x]]
                      (when (= x probe-var)
                        {:merge-idx -1 :datom-field i}))
                    (map-indexed vector producer-scan-clause))
              (some (fn [[mi mc]]
                      (some (fn [[i x]]
                              (when (= x probe-var)
                                {:merge-idx mi :datom-field i}))
                            (map-indexed vector mc)))
                    (map-indexed vector producer-merge-clauses)))]
      (when (and consumer-scan-field producer-source)
        {:probe-var probe-var
         :consumer-scan-field consumer-scan-field
         :producer-merge-idx (:merge-idx producer-source)
         :producer-datom-field (:datom-field producer-source)}))))

(defn- group-provides-var?
  "Check if a group's scan+merge clauses contain the given variable."
  [group-op var-sym]
  (let [scan-clause (:clause (entity-group-scan-op group-op))
        merge-clauses (mapv :clause (entity-group-merge-ops group-op))]
    (or (some #{var-sym} scan-clause)
        (some (fn [mc] (some #{var-sym} mc)) merge-clauses))))

(defn- can-direct-fuse?
  "Check if a plan can use the direct-to-HashSet execution path.
   Delegates structural checks (op types, post-op eligibility, source exclusion)
   to plan/structurally-fusable?, then adds runtime-specific checks:
   - ALL find-vars resolvable from groups, function outputs, or consts
   - Multi-group: probe vars in consumer scan, find-vars from consumer group"
  [plan find-vars consts]
  (let [ops (:ops plan)
        groups (filterv #(#{:entity-group :pattern-scan} (:op %)) ops)]
    (and ;; Structural eligibility (shared with plan-time pre-check)
     (plan/structurally-fusable? ops)
         ;; Find-var coverage: groups + function outputs + consts
     (let [group-vars (into #{} (mapcat :vars) groups)
           fn-ops (filterv #(= :function (:op %)) ops)
           fn-output-vars (into #{} (mapcat (fn [op]
                                              (let [input-vars (into #{} (filter analyze/free-var?) (:args op))]
                                                (remove input-vars (:vars op)))))
                                fn-ops)
           all-available-vars (into group-vars fn-output-vars)]
       (every? #(or (and consts (contains? consts %))
                    (contains? all-available-vars %))
               find-vars))
         ;; Multi-group: probe vars and find-var coverage from consumer
     (every? (fn [[gi {:keys [producer-idx probe-vars]}]]
               (let [consumer-g (nth groups gi)
                     producer-g (nth groups producer-idx)]
                 (and (some? (find-probe-info consumer-g producer-g probe-vars))
                          ;; All find-vars must be resolvable from the consumer group or consts
                      (every? #(or (and consts (contains? consts %))
                                   (group-provides-var? consumer-g %))
                              find-vars))))
             (:group-joins plan)))))

;; ---------------------------------------------------------------------------
;; Post-processing on fused result tuples
;;
;; When the plan has predicates or functions whose vars are all provided by
;; groups, we emit wide tuples (all group vars), post-process in-place on the
;; ArrayList, then project down to find-vars. This avoids the Relation engine.

(defn- resolve-pred-fn
  "Resolve a predicate or function symbol to a callable.
   Handles built-ins, clj-core built-ins, regular vars, and Java interop methods."
  [f]
  (or (get @(requiring-resolve 'datahike.query/built-ins) f)
      (get @(requiring-resolve 'datahike.query/clj-core-built-ins) f)
      #?(:clj (some-> (resolve f) deref)
         :cljs nil)
      #?(:clj ((requiring-resolve 'datahike.query/resolve-method) f)
         :cljs nil)))

(defn- resolve-pred-fns
  "Resolve all predicate/function symbols up front, failing fast for unknowns.
   Returns a vector of resolved callables parallel to the input ops."
  [ops]
  (mapv (fn [op]
          (let [f (resolve-pred-fn (:fn-sym op))]
            (when-not f
              (throw (ex-info (str "Unknown predicate/function '" (:fn-sym op)
                                   " in " (:clause op))
                              {:error :query/where
                               :form (:clause op)
                               :var (:fn-sym op)})))
            f))
        ops))

(defn- post-filter-preds
  "Filter result-list in-place by evaluating predicate ops.
   var-index maps var-sym → position in the wide tuple Object[].
   Removes tuples that don't satisfy all predicates.
   Exception handling matches the Relation engine (filter-by-pred):
   only ClassCastException and IllegalArgumentException are caught."
  [^java.util.ArrayList result-list pred-ops var-index]
  (let [resolved (resolve-pred-fns pred-ops)
        n (.size result-list)]
    (loop [read-i (int 0)
           write-i (int 0)]
      (if (< read-i n)
        (let [^objects tuple (.get result-list read-i)
              pass? (loop [pi 0]
                      (if (>= pi (count pred-ops))
                        true
                        (let [f (nth resolved pi)
                              args (:args (nth pred-ops pi))
                              argv (mapv (fn [a]
                                           (if (and (symbol? a) (analyze/free-var? a))
                                             (aget tuple (int (get var-index a)))
                                             a))
                                         args)]
                          (if (try (apply f argv)
                                   #?(:clj (catch ClassCastException _ false))
                                   #?(:clj (catch IllegalArgumentException _ false)
                                      :cljs (catch :default _ false)))
                            (recur (inc pi))
                            false))))]
          (if pass?
            (do (when (not= read-i write-i)
                  (.set result-list write-i tuple))
                (recur (unchecked-inc-int read-i) (unchecked-inc-int write-i)))
            (recur (unchecked-inc-int read-i) write-i)))
        ;; Trim removed entries
        (when (< write-i n)
          (loop [i (dec n)]
            (when (>= i write-i)
              (.remove result-list i)
              (recur (dec i)))))))))

(defn- binding-vars
  "Extract the binding variable(s) from a binding form as a flat vector.
   Handles scalar (?x), tuple ([?x ?y]), and nested forms."
  [binding-form]
  (cond
    (symbol? binding-form) [binding-form]
    (vector? binding-form) (vec (flatten binding-form))
    :else []))

(defn- post-apply-fns
  "Extend tuples in result-list by evaluating function ops.
   For new vars: adds columns to each tuple.
   For already-bound vars: acts as a filter (keeps tuple only when function result = existing value).
   Returns updated var-index with new var positions.
   No exception catching — matches the Relation engine (bind-by-fn) which lets
   exceptions propagate and only treats nil return as tuple filter."
  [^java.util.ArrayList result-list fn-ops var-index]
  (let [resolved (resolve-pred-fns fn-ops)]
    (reduce
     (fn [vi [fn-op f]]
       (let [args (:args fn-op)
             bind-form (:binding fn-op)
             bvars (binding-vars bind-form)
             ;; Separate already-bound vars from new vars
             already-bound (filterv #(and (symbol? %) (analyze/free-var? %) (contains? vi %)) bvars)
             ;; New vars get appended to tuple
             new-vi (reduce (fn [m bv]
                              (if (and (symbol? bv) (analyze/free-var? bv)
                                       (not (contains? m bv)))
                                (assoc m bv (count m))
                                m))
                            vi bvars)
             n (.size result-list)]
         (loop [read-i (int 0)
                write-i (int 0)]
           (if (< read-i n)
             (let [^objects tuple (.get result-list read-i)
                   argv (mapv (fn [a]
                                (if (and (symbol? a) (analyze/free-var? a))
                                  (aget tuple (int (get vi a)))
                                  a))
                              args)
                   val (apply f argv)]
               (if (some? val)
                 ;; Check already-bound vars: function result must match existing value
                 (if (every? (fn [bv]
                               (= val (aget tuple (int (get vi bv)))))
                             already-bound)
                   (let [;; Extend tuple with new value(s) for unbound vars
                         old-len (alength tuple)
                         new-len (count new-vi)
                         new-tuple (if (> new-len old-len)
                                     (let [t #?(:clj (object-array new-len) :cljs (make-array new-len))]
                                       (System/arraycopy tuple 0 t 0 old-len)
                                       t)
                                     tuple)]
                     ;; Set only NEW bound var values (already-bound vars keep their value)
                     (doseq [bv bvars
                             :when (and (symbol? bv) (analyze/free-var? bv)
                                        (not (contains? vi bv)))]
                       (aset new-tuple (int (get new-vi bv)) val))
                     (.set result-list write-i new-tuple)
                     (recur (unchecked-inc-int read-i) (unchecked-inc-int write-i)))
                   ;; Already-bound var mismatch — filter out tuple
                   (recur (unchecked-inc-int read-i) write-i))
                 ;; Function returned nil — skip tuple
                 (recur (unchecked-inc-int read-i) write-i)))
             ;; Trim removed entries
             (when (< write-i n)
               (loop [i (dec n)]
                 (when (>= i write-i)
                   (.remove result-list i)
                   (recur (dec i)))))))
         new-vi))
     var-index (map vector fn-ops resolved))))

(defn- project-tuples
  "Project wide tuples down to find-vars only.
   Modifies result-list in-place, replacing each tuple with a narrower one."
  [^java.util.ArrayList result-list find-vars var-index consts]
  (let [n-find (count find-vars)
        ;; Build projection: for each find-var, its index in the wide tuple (or nil if const)
        proj (mapv (fn [fv] (if (and consts (contains? consts fv))
                              [:const (get consts fv)]
                              [:var (get var-index fv)]))
                   find-vars)
        n (.size result-list)]
    (dotimes [i n]
      (let [^objects wide (.get result-list i)
            narrow #?(:clj (object-array n-find) :cljs (make-array n-find))]
        (dotimes [fi n-find]
          (let [[kind v] (nth proj fi)]
            (aset narrow fi (if (= kind :const) v (aget wide (int v))))))
        (.set result-list i narrow)))))

;; ---------------------------------------------------------------------------
;; Single-pass EAVT scan (prototype for max throughput)
;;
;; Instead of scan+seekGE merges, iterate ALL datoms in EAVT once.
;; Entity datoms are contiguous in EAVT. For each entity, collect values
;; for target attributes. If all required attributes present, emit tuple.
;; Zero comparator dispatch — just raw field access + long comparison.

(defn execute-plan-direct
  "Execute a fully-fusable plan directly to a PersistentHashSet.
   Supports single-group and multi-group (value join via hash-probe) plans.
   max-results: when non-nil, stop after collecting this many results (for offset+limit).
   consts: map of var-sym → constant value for scalar :in bindings.
   Returns the HashSet, or nil if the plan can't be executed directly."
  [plan db find-vars max-results consts]
  (let [ops (:ops plan)
        temporal (temporal-info db)]
    (when (and (not (:has-passthrough? plan))
               ;; Fast structural pre-check from IR pipeline (when available)
               (if (contains? plan :structurally-fusable?)
                 (:structurally-fusable? plan)
                 true)
               (can-direct-fuse? plan find-vars consts))
      (let [group-joins (:group-joins plan)
            groups (filterv #(#{:entity-group :pattern-scan} (:op %)) ops)
            pred-ops (filterv #(= :predicate (:op %)) ops)
            fn-ops (filterv #(= :function (:op %)) ops)
            has-post-ops? (or (seq pred-ops) (seq fn-ops))
            ;; When post-ops exist, emit ALL group vars (wide tuples) so
            ;; predicates/functions can reference any var. Project afterwards.
            all-group-vars (when has-post-ops?
                             (vec (distinct (mapcat :vars groups))))
            emit-vars (if has-post-ops? all-group-vars find-vars)
            n-groups (count groups)
            result-list (make-result-list 4000)]

        (if (= 1 n-groups)
          ;; Single group — fused scan+merge
          (let [g (first groups)
                scan-op (entity-group-scan-op g)
                merge-ops (entity-group-merge-ops g)]
            (if temporal
              (execute-group-direct-temporal db scan-op merge-ops emit-vars consts
                                             result-list nil 0 nil 0 -1
                                             max-results temporal)
              (execute-group-direct db scan-op merge-ops emit-vars consts
                                    result-list nil 0 nil 0 -1
                                    max-results
                                    :pipeline (:pipeline g))))

          ;; Multi-group — hash-probe value join
          ;; Execute groups in order, build probe-sets between them
          (loop [gi 0
                 probe-sets {}] ;; {group-idx → HashSet of join-var values}
            (if (>= gi n-groups)
              nil ;; done
              (let [g (nth groups gi)
                    join-info (get group-joins gi)
                    scan-op (entity-group-scan-op g)
                    merge-ops (entity-group-merge-ops g)]

                (if join-info
                  ;; Consumer group: use probe-set from producer
                  (let [{:keys [producer-idx probe-vars]} join-info
                        producer-g (nth groups producer-idx)
                        pinfo (find-probe-info g producer-g probe-vars)
                        probe-set (get probe-sets producer-idx)]
                    (when (and pinfo probe-set)
                      (if temporal
                        (execute-group-direct-temporal db scan-op merge-ops emit-vars consts
                                                       result-list probe-set
                                                       (int (:consumer-scan-field pinfo))
                                                       nil 0 -1
                                                       max-results temporal
                                                       :scan-estimate (:estimated-card g))
                        (execute-group-direct db scan-op merge-ops emit-vars consts
                                              result-list probe-set
                                              (int (:consumer-scan-field pinfo))
                                              nil 0 -1
                                              max-results
                                              :scan-estimate (:estimated-card g)
                                              :pipeline (:pipeline g))))
                    (recur (inc gi) probe-sets))

                  ;; Producer group: collect join-var values for downstream consumers
                  (let [;; Check if any downstream group needs a probe from us
                        downstream-info (some (fn [[consumer-gi info]]
                                                (when (= (:producer-idx info) gi)
                                                  {:consumer-gi consumer-gi
                                                   :probe-vars (:probe-vars info)}))
                                              group-joins)
                        collect-set (when downstream-info
                                      (make-probe-set 4000))
                        ;; Determine which field to collect
                        collect-field-info (when downstream-info
                                             (let [consumer-g (nth groups (:consumer-gi downstream-info))
                                                   pinfo (find-probe-info consumer-g g (:probe-vars downstream-info))]
                                               pinfo))]

                    ;; Execute producer group — empty find-vars, only collect
                    ;; No max-results for producer (needs full collection for join)
                    (if temporal
                      (execute-group-direct-temporal db scan-op merge-ops [] consts
                                                     result-list nil 0
                                                     collect-set
                                                     (int (or (:producer-datom-field collect-field-info) 0))
                                                     (int (or (:producer-merge-idx collect-field-info) -1))
                                                     nil temporal)
                      (execute-group-direct db scan-op merge-ops [] consts
                                            result-list nil 0
                                            collect-set
                                            (int (or (:producer-datom-field collect-field-info) 0))
                                            (int (or (:producer-merge-idx collect-field-info) -1))
                                            nil
                                            :pipeline (:pipeline g)))
                    (recur (inc gi)
                           (if collect-set
                             (assoc probe-sets gi collect-set)
                             probe-sets))))))))
        ;; Post-processing: apply predicates, functions, then project to find-vars
        (when has-post-ops?
          (let [var-index (into {} (map-indexed (fn [i v] [v i])) all-group-vars)]
            (when (seq pred-ops)
              (post-filter-preds result-list pred-ops var-index))
            (let [var-index (if (seq fn-ops)
                              (post-apply-fns result-list fn-ops var-index)
                              var-index)]
              (project-tuples result-list find-vars var-index consts))))
        ;; Convert result-list → final result with appropriate dedup strategy
        (let [has-card-many-dupes?
              (some (fn [g]
                      (let [mops (entity-group-merge-ops g)]
                        (or (some (fn [op] (not (get-in op [:schema-info :card-one?] true))) mops)
                            ;; Entity var not in find-vars → different entities can produce same tuple
                            (let [e-var (first (:clause (entity-group-scan-op g)))]
                              (not (some #{e-var} find-vars))))))
                    groups)
              is-historical? (= :historical (when temporal (:type temporal)))
              dedup-strategy (cond
                               has-card-many-dupes? :hash
                               is-historical? :adjacent
                               :else nil)]
          #?(:clj
             (case dedup-strategy
               :hash
               (let [n (result-list-size result-list)]
                 (persistent!
                  (loop [i (int 0) s (transient #{})]
                    (if (< i n)
                      (recur (unchecked-inc-int i)
                             (conj! s (adopt-vector (result-list-get result-list i))))
                      s))))

               :adjacent
               ;; Adjacent dedup: history card-one duplicates are adjacent in scan order.
               ;; Returns PHS for consistent set behavior and iteration order.
               (let [n (result-list-size result-list)]
                 (persistent!
                  (loop [i (int 0)
                         ^objects prev nil
                         s (transient #{})]
                    (if (< i n)
                      (let [^objects cur (result-list-get result-list i)]
                        (if (or (nil? prev)
                                (not (java.util.Arrays/equals prev cur)))
                          (recur (unchecked-inc-int i) cur
                                 (conj! s (adopt-vector cur)))
                          (recur (unchecked-inc-int i) cur s)))
                      s))))

               ;; nil — Fast path: no duplicates, use QueryResult
               (let [n (result-list-size result-list)
                     out (object-array n)]
                 (loop [i (int 0)]
                   (when (< i n)
                     (aset out i (adopt-vector (result-list-get result-list i)))
                     (recur (unchecked-inc-int i))))
                 (datahike.java.QueryResult. out n)))
             :cljs
             (let [n (result-list-size result-list)]
               (persistent!
                (loop [i 0 s (transient #{})]
                  (if (< i n)
                    (recur (inc i) (conj! s (adopt-vector (result-list-get result-list i))))
                    s))))))))))

(defn execute-plan-direct-rel
  "Execute a fusable plan using the fast direct path, but return a Relation
   instead of a HashSet. This bridges the gap: queries that need aggregates,
   pull, :with, or other post-processing still get the fused scan speed
   instead of the slow per-clause Relation pipeline.
   Returns nil if the plan can't be fused."
  [plan db]
  (let [ops (:ops plan)]
    (when (and (not (:has-passthrough? plan))
               ;; direct-rel only handles pure group plans — no predicates/functions
               ;; (unlike execute-plan-direct which has post-filter machinery)
               (seq ops)
               (every? #(#{:entity-group :pattern-scan} (:op %)) ops)
               (not-any? :source ops))
      ;; Collect ALL vars produced by all groups
      (let [groups (filterv #(#{:entity-group :pattern-scan} (:op %)) ops)
            all-vars (vec (distinct (mapcat :vars groups)))
            ;; For single-group plans only (multi-group needs more work)
            _ (when (> (count groups) 1)
                (throw (ex-info "multi-group direct-rel not yet supported" {})))]
        (when (= 1 (count groups))
          (let [g (first groups)
                scan-op (entity-group-scan-op g)
                merge-ops (entity-group-merge-ops g)
                result-list (make-result-list 4000)]
            (let [ti (temporal-info db)]
              (if ti
                (execute-group-direct-temporal db scan-op merge-ops all-vars nil
                                               result-list nil 0 nil 0 -1 nil ti)
                (execute-group-direct db scan-op merge-ops all-vars nil
                                      result-list nil 0 nil 0 -1 nil
                                      :pipeline (:pipeline g))))
            ;; Convert to Relation: attrs = {var-sym → column-index}, tuples = vec of Object[]
            (let [attrs (into {} (map-indexed (fn [i v] [v i]) all-vars))
                  tuples #?(:clj (vec (.toArray ^java.util.ArrayList result-list))
                            :cljs (vec result-list))]
              (rel/->Relation attrs tuples))))))))

(defn- lookup-attrs-for-clauses
  "Compute the set of variable symbols that may hold lookup refs,
   for binding rel/*lookup-attrs* during collapse-rels in multi-source queries.
   Includes entity and tx position vars (always entity-id typed) and
   ref-typed value position vars."
  [db clause merge-ops]
  (let [add-clause-vars
        (fn [s [e a v tx]]
          (cond-> s
            (and (symbol? e) (analyze/free-var? e)) (conj e)
            (and (some? tx) (symbol? tx) (analyze/free-var? tx)) (conj tx)
            (and (symbol? v) (analyze/free-var? v)
                 (keyword? a)
                 (dbu/ref? db a)) (conj v)))]
    (reduce (fn [s mop] (add-clause-vars s (:clause mop)))
            (add-clause-vars #{} clause)
            merge-ops)))

;; ---------------------------------------------------------------------------
;; Relation-based execution (fallback path for predicates, functions, etc.)

(defn- execute-fused-scan-rel
  "Execute an entity-group as a fused scan, returning a Relation."
  [db scan-op merge-ops context]
  (let [{:keys [clause index pushdown-preds]} scan-op
        [e a v tx] clause
        resolved-a (when (and (some? a) (not (symbol? a))) (resolve-attr db a))
        resolved-e (when (and (some? e) (not (symbol? e)) (number? e)) #?(:clj (long e) :cljs e))
        pushdown-bounds (when (seq pushdown-preds) (plan/pushdown-to-bounds pushdown-preds))
        [from-datom to-datom] (compute-slice-bounds clause index pushdown-bounds resolved-a resolved-e)
        db-index (get db index)
        ground-filter (build-ground-filter clause index)
        scan-var-map (rel/var-mapping clause (range))
        strict-filter (when-let [strict (:strict-preds pushdown-bounds)]
                        (let [pv (first (keep :var pushdown-preds))]
                          (when pv (build-strict-filter strict (get scan-var-map pv)))))
        eavt-pss (:eavt db)
        n-merges (count merge-ops)
        merge-attrs (to-array (mapv (fn [op] (let [ma (second (:clause op))]
                                               (when (and (some? ma) (not (symbol? ma)))
                                                 (resolve-attr db ma))))
                                    merge-ops))
        merge-v-ground (to-array (mapv (fn [op] (let [mv (get (:clause op) 2)]
                                                  (boolean (and (some? mv) (not (symbol? mv))))))
                                       merge-ops))
        merge-v-vals (to-array (mapv (fn [op] (let [mv (get (:clause op) 2)]
                                                (when (and (some? mv) (not (symbol? mv))) mv)))
                                     merge-ops))
        merge-anti (to-array (mapv #(boolean (:anti? %)) merge-ops))
        ;; Anti-merge shared-variable checks (same as temporal variant)
        merge-check-scan-v (to-array (mapv (fn [op]
                                             (let [mv (get (:clause op) 2)
                                                   sv (get clause 2)]
                                               (boolean (and (analyze/free-var? mv) (analyze/free-var? sv)
                                                             (= mv sv)))))
                                           merge-ops))
        merge-check-scan-tx (to-array (mapv (fn [op]
                                              (let [mtx (get (:clause op) 3)
                                                    stx (get clause 3)]
                                                (boolean (and (analyze/free-var? mtx) (analyze/free-var? stx)
                                                              (= mtx stx)))))
                                            merge-ops))

        e-var (first clause)
        base-attrs (reduce-kv (fn [m k v]
                                (if (and (symbol? k) (analyze/free-var? k))
                                  (assoc m k v) m))
                              {} scan-var-map)
        out-attrs (let [idx (volatile! 4)]
                    (reduce (fn [attrs mop]
                              (let [mvar-map (rel/var-mapping (:clause mop) (range))]
                                (reduce-kv (fn [a var-sym _]
                                             (if (or (not (symbol? var-sym))
                                                     (not (analyze/free-var? var-sym))
                                                     (contains? a var-sym))
                                               a
                                               (assoc a var-sym (vswap! idx inc))))
                                           attrs mvar-map)))
                            base-attrs merge-ops))

        ;; Check for entity-filter from upstream secondary index searches
        entity-filter (get (:entity-filters context) e-var)

        ;; Short-circuit: empty entity-filter means no results possible
        empty-filter? (and entity-filter
                           #?(:clj (zero? (es/entity-bitset-cardinality entity-filter))
                              :cljs false))]
    (if empty-filter?
      (let [out-rel (rel/->Relation out-attrs [])
            merged (rel/collapse-rels (:rels context) out-rel)]
        [(-> context (assoc :rels merged) (assoc :unique-results? true))
         (inc (count merge-ops))])
      (let [;; SIP: extract probe values from context relations for selective seeks.
            ;; Check both entity-var and value-var of the scan clause.
            sip-probe
            #?(:clj
               (when (and resolved-a (seq (:rels context)))
                 (let [v-var (get clause 2)
                       extract-probe-values
                       (fn [rel col-idx]
                         (let [tuples (:tuples rel)
                               n (if (instance? java.util.Collection tuples)
                                   (.size ^java.util.Collection tuples)
                                   (count tuples))]
                           (when (and (pos? n)
                                      (< (* (long n) (long probe-driven-threshold))
                                         (long (di/-count db-index))))
                             (let [hs (java.util.HashSet.)]
                               (doseq [t tuples]
                                 (let [v (cond
                                           (instance? clojure.lang.Indexed t) (.nth ^clojure.lang.Indexed t col-idx)
                                           (sequential? t) (nth t col-idx)
                                           :else (get t col-idx))]
                                   (when (some? v) (.add hs v))))
                               (when (pos? (.size hs)) hs)))))
                       ;; Check entity variable
                       e-probe (when (and (symbol? e) (analyze/free-var? e))
                                 (some (fn [rel]
                                         (when-let [col-idx (get (:attrs rel) e)]
                                           (when-let [vs (extract-probe-values rel col-idx)]
                                             {:field 0 :values vs})))
                                       (:rels context)))
                       ;; Check value variable (needs AVET + attribute must be indexed)
                       v-probe (when (and (not e-probe)
                                          (symbol? v-var) (analyze/free-var? v-var)
                                          (some? (:avet db))
                                          resolved-a
                                          (dbu/indexing? db resolved-a))
                                 (some (fn [rel]
                                         (when-let [col-idx (get (:attrs rel) v-var)]
                                           (when-let [vs (extract-probe-values rel col-idx)]
                                             {:field 2 :values vs})))
                                       (:rels context)))]
                   (or e-probe v-probe)))
               :cljs nil)
            filtered-datoms (cond->> (if sip-probe
                                       (probe-driven-iterable
                                        (if (== (:field sip-probe) 2) (:avet db) (:eavt db))
                                        resolved-a (:values sip-probe) (:field sip-probe))
                                       (di/-slice db-index from-datom to-datom index))
                              ground-filter (filter ground-filter)
                              strict-filter (filter strict-filter))

            merge-card-many (to-array (mapv (fn [op]
                                              (not (get-in op [:schema-info :card-one?] true)))
                                            merge-ops))
            acc (make-result-list 2000)
            _ (letfn [(process-merges [scan-d eid mi tuple]
                        (if (>= mi n-merges)
                          (result-list-add acc tuple)
                          (let [anti? (aget merge-anti mi)
                                ra (aget merge-attrs mi)
                                vg? (aget merge-v-ground mi)
                                vgv (aget merge-v-vals mi)
                                card-many? (aget merge-card-many mi)
                                has-v-var? (let [mv (get (:clause (nth merge-ops mi)) 2)]
                                             (and (symbol? mv) (analyze/free-var? mv)))
                                has-tx-var? (let [mtx (get (:clause (nth merge-ops mi)) 3)]
                                              (and (some? mtx) (symbol? mtx) (analyze/free-var? mtx)))]
                            (if card-many?
                          ;; Card-many: iterate ALL matching datoms
                              (let [from-d (datom eid ra (when vg? vgv) tx0)
                                    to-d (datom eid ra (when vg? vgv) txmax)
                                    slice (di/-slice (:eavt db) from-d to-d :eavt)]
                                (if anti?
                                  (let [check-v? (aget merge-check-scan-v mi)
                                        check-tx? (aget merge-check-scan-tx mi)]
                                    (when (not-any? (fn [^Datom d] (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d)) slice)
                                      (process-merges scan-d eid (inc mi) tuple)))
                                  (let [check-v? (aget merge-check-scan-v mi)
                                        check-tx? (aget merge-check-scan-tx mi)]
                                    (doseq [^Datom d slice]
                                      (when (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d)
                                        (process-merges scan-d eid (inc mi)
                                                        (cond-> tuple
                                                          has-v-var? (conj (.-v d))
                                                          has-tx-var? (conj (.-tx d)))))))))
                          ;; Card-one: single lookupGE
                              (let [^Datom d (pss-lookup-ge eavt-pss (datom eid ra vgv tx0))
                                    check-v? (aget merge-check-scan-v mi)
                                    check-tx? (aget merge-check-scan-tx mi)
                                    found? (and d (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d))]
                                (if anti?
                                  (when (not found?)
                                    (process-merges scan-d eid (inc mi) tuple))
                                  (when found?
                                    (process-merges scan-d eid (inc mi)
                                                    (cond-> tuple
                                                      has-v-var? (conj (.-v d))
                                                      has-tx-var? (conj (.-tx d)))))))))))]
                (run! (fn [^Datom scan-d]
                        (when (or (nil? entity-filter)
                                  #?(:clj (es/entity-bitset-contains? entity-filter (.-e scan-d))
                                     :cljs true))
                          (process-merges scan-d (.-e scan-d) (int 0)
                                          [(.-e scan-d) (.-a scan-d) (.-v scan-d) (.-tx scan-d) true])))
                      filtered-datoms))]
        (let [out-rel (rel/->Relation out-attrs #?(:clj (vec (.toArray ^java.util.ArrayList acc))
                                                   :cljs (vec acc)))
              ;; Bind *implicit-source* and *lookup-attrs* so that hash-join inside
              ;; collapse-rels can resolve lookup refs in input relations against
              ;; the correct source db (critical for multi-source queries).
              merged (binding [rel/*implicit-source* db
                               rel/*lookup-attrs* (lookup-attrs-for-clauses db clause merge-ops)]
                       (rel/collapse-rels (:rels context) out-rel))]
          [(-> context
               (assoc :rels merged)
               (assoc :unique-results? true))
           (inc (count merge-ops))])))))

;; ---------------------------------------------------------------------------
;; OR / NOT execution (Relation-based fallback)

(defn- execute-or [db op ctx]
  (let [or-vars (:vars op)
        branch-rels (mapv (fn [sub-plan]
                            (let [branch-ctx (assoc ctx :rels (:rels ctx))
                                  result-ctx (execute-plan sub-plan branch-ctx db)]
                              (when result-ctx
                                (let [joined (reduce rel/hash-join (:rels result-ctx))]
                                  ;; Project to OR's visible vars — critical for rules where
                                  ;; branches introduce auto-generated temp vars
                                  (rel/limit-rel joined or-vars)))))
                          (:branches op))
        valid (filterv some? branch-rels)
        union (when (seq valid) (reduce rel/sum-rel valid))]
    (if union (update ctx :rels rel/collapse-rels union) ctx)))

(defn- execute-or-join [db op ctx]
  (let [join-vars (:join-vars op)
        limited-ctx (rel/limit-context ctx join-vars)
        branch-rels (mapv (fn [sub-plan]
                            (let [result-ctx (execute-plan sub-plan limited-ctx db)]
                              (when result-ctx
                                (let [joined (reduce rel/hash-join (:rels result-ctx))]
                                  (rel/limit-rel joined join-vars)))))
                          (:branches op))
        valid (filterv some? branch-rels)
        union (when (seq valid) (reduce rel/sum-rel valid))]
    (if union (update ctx :rels rel/collapse-rels union) ctx)))

(defn- execute-not [db op ctx]
  (if (empty? (:rels ctx))
    ctx
    (let [join-rel (reduce rel/hash-join (:rels ctx))
          neg-ctx (execute-plan (:sub-plan op) (assoc ctx :rels [join-rel]) db)
          neg-join (when (and neg-ctx (seq (:rels neg-ctx)))
                     (reduce rel/hash-join (:rels neg-ctx)))
          result (if neg-join (rel/subtract-rel join-rel neg-join) join-rel)]
      (assoc ctx :rels [result]))))

(defn- execute-not-join [db op ctx]
  (if (empty? (:rels ctx))
    ctx
    (let [join-vars (:join-vars op)
          join-rel (reduce rel/hash-join (:rels ctx))
          limited-ctx (rel/limit-context (assoc ctx :rels [join-rel]) join-vars)
          neg-ctx (execute-plan (:sub-plan op) limited-ctx db)
          neg-ctx (when neg-ctx (rel/limit-context neg-ctx join-vars))
          neg-join (when (and neg-ctx (seq (:rels neg-ctx)))
                     (reduce rel/hash-join (:rels neg-ctx)))
          result (if neg-join (rel/subtract-rel join-rel neg-join) join-rel)]
      (assoc ctx :rels [result]))))

;; ---------------------------------------------------------------------------
;; Recursive rule execution (semi-naive fixpoint with clause versions)

#?(:clj
   (deftype ArrayWrapper [^objects a ^int h]
     Object
     (hashCode [_] h)
     (equals [_ that]
       (and (instance? ArrayWrapper that)
            (java.util.Arrays/equals a ^objects (.-a ^ArrayWrapper that))))))

(defn- rel-dedup-into!
  "Add tuples from rel (projected to head-vars) into seen-set.
   Returns a new Relation containing only the NEW tuples (not already in seen).
   Mutates seen-set."
  [rel head-vars ^java.util.HashSet seen-set]
  (let [attrs (:attrs rel)
        indices #?(:clj (int-array (mapv #(get attrs %) head-vars))
                   :cljs (mapv #(get attrs %) head-vars))
        n-vars #?(:clj (alength indices) :cljs (count indices))
        tuples (:tuples rel)
        delta #?(:clj (java.util.ArrayList.) :cljs #js [])]
    (doseq [tuple tuples]
      (let [projected #?(:clj (let [arr (object-array n-vars)]
                                (dotimes [j n-vars]
                                  (let [idx (aget indices j)]
                                    (aset arr j (if (instance? object-array-class tuple)
                                                  (aget ^objects tuple idx)
                                                  (nth tuple idx)))))
                                arr)
                         :cljs (let [arr (make-array n-vars)]
                                 (dotimes [j n-vars]
                                   (aset arr j (aget tuple (nth indices j))))
                                 arr))
            wrapper #?(:clj (ArrayWrapper. projected (java.util.Arrays/hashCode projected))
                       :cljs projected)]
        (when #?(:clj (.add seen-set wrapper)
                 :cljs (not (.has seen-set (str (vec projected)))))
          #?(:cljs (.add seen-set (str (vec projected))))
          #?(:clj (.add delta projected)
             :cljs (.push delta projected)))))
    (let [new-attrs (zipmap head-vars (range))
          new-tuples #?(:clj (vec delta) :cljs (vec delta))]
      (rel/->Relation new-attrs new-tuples))))

(defn- rel-from-tuples
  "Create a Relation from raw projected tuples (object arrays) with given var names."
  [vars tuples]
  (rel/->Relation (zipmap vars (range)) tuples))

(defn- execute-branch-plans
  "Execute a list of branch plans (sub-plans), union results, project to output-vars.
   Returns a Relation."
  [db plans ctx output-vars]
  (let [branch-rels
        (into []
              (keep (fn [plan]
                      (let [result-ctx (execute-plan plan ctx db)]
                        (when (and result-ctx (seq (:rels result-ctx)))
                          (let [joined (reduce rel/hash-join (:rels result-ctx))]
                            (rel/limit-rel joined output-vars))))))
              plans)]
    (if (seq branch-rels)
      (reduce rel/sum-rel branch-rels)
      (rel/->Relation (zipmap output-vars (range)) []))))

(defn- compute-magic-info
  "Detect ground call-args for a recursive rule and compute magic set parameters.
   Returns nil if magic sets are not applicable (no ground args, mutual recursion,
   or non-binary rule). When applicable, returns:
   {:ground-positions {pos value}, :head-vars [...], :propagation-pos int}"
  [call-args head-vars scc-rule-names]
  ;; Only apply magic sets to single-rule SCCs with binary head vars
  (when (and (= 1 (count scc-rule-names))
             (= 2 (count head-vars)))
    (let [ground-positions (into {}
                                 (keep-indexed (fn [i arg]
                                                 (when-not (analyze/free-var? arg)
                                                   [i arg])))
                                 call-args)]
      (when (= 1 (count ground-positions))
        (let [ground-pos (ffirst ground-positions)
              prop-pos (if (= ground-pos 0) 1 0)]
          {:ground-positions ground-positions
           :propagation-pos prop-pos})))))

(defn- inject-magic-relation
  "Add a magic demand relation to the context that constrains head-var at
   ground-pos to only values in the demand set."
  [ctx head-vars ground-pos ^java.util.HashSet demand-set]
  (let [demand-var (nth head-vars ground-pos)
        demand-tuples #?(:clj (let [result (java.util.ArrayList. (.size demand-set))
                                    iter (.iterator demand-set)]
                                (while (.hasNext iter)
                                  (let [arr (object-array 1)]
                                    (aset arr 0 (.next iter))
                                    (.add result arr)))
                                (vec result))
                         :cljs (let [result #js []]
                                 (.forEach demand-set
                                           (fn [v]
                                             (let [arr (make-array 1)]
                                               (aset arr 0 v)
                                               (.push result arr))))
                                 (vec result)))
        demand-rel (rel/->Relation {demand-var 0} demand-tuples)]
    (update ctx :rels rel/collapse-rels demand-rel)))

(defn- delta-driven-expand
  "For the recursive clause [?x :attr ?t] (follows-DELTA ?t ?y):
   Instead of scanning all edges and hash-joining with delta,
   iterate delta tuples and do reverse index lookups.
   For each (?t_val, ?y_val) in delta, find all ?x where [?x :attr ?t_val]
   using AVET index, producing (?x, ?y_val) output tuples.

   head-vars: [?x ?y] — the rule head variables
   attr: the :follows attribute keyword/id
   delta-rel: Relation with delta tuples from previous iteration
   join-pos: position in delta tuples that joins with the index (the ?t position)
   output-pos: position in delta tuples that propagates to output (the ?y position)"
  [db head-vars attr delta-rel join-pos output-pos]
  #?(:clj
     (when (and attr (seq (:tuples delta-rel)))
       (let [avet-pss (:avet db)
             result (java.util.ArrayList.)
             tuples (:tuples delta-rel)]
         (doseq [tuple tuples]
           (let [t-val (if (instance? object-array-class tuple)
                         (aget ^objects tuple (int join-pos))
                         (nth tuple join-pos))
                 y-val (if (instance? object-array-class tuple)
                         (aget ^objects tuple (int output-pos))
                         (nth tuple output-pos))
                 ;; Reverse lookup: find all entities where :attr = t-val
                 from-datom (datom e0 attr t-val tx0)
                 to-datom (datom emax attr t-val txmax)
                 slice (di/-slice avet-pss from-datom to-datom :avet)]
             (doseq [^Datom sd slice]
               (when (and (= (.-a sd) attr) (= (.-v sd) t-val))
                 (let [arr (object-array 2)
                       eid (.-e sd)]
                   (aset arr 0 (Long/valueOf eid))   ;; ?x = entity that follows t-val
                   (aset arr 1 y-val)      ;; ?y = propagated from delta
                   (.add result arr))))))
         (when (pos? (.size result))
           (rel/->Relation (zipmap head-vars (range)) (vec result)))))
     :cljs nil))

(defn- magic-base-scan
  "Directly scan edges for entities in the demand set using EAVT point lookups.
   Only scans entities not yet in the scanned set (if provided).
   Much faster than re-executing the full base branch plan for large graphs.
   Only works for simple binary base patterns like [?x :attr ?y]."
  [db head-vars attr demand-set scanned-set]
  #?(:clj
     (when attr
       (let [eavt-pss (:eavt db)
             result (java.util.ArrayList.)
             iter (.iterator ^java.util.HashSet demand-set)]
         ;; Point-lookup each NEW entity's edges (skip already-scanned)
         (while (.hasNext iter)
           (let [e (.next iter)]
             (when (or (nil? scanned-set)
                       (.add ^java.util.HashSet scanned-set e))
               (let [from-datom (datom (if (number? e) (long e) e) attr nil tx0)
                     to-datom (datom (if (number? e) (long e) e) attr nil txmax)
                     slice (di/-slice eavt-pss from-datom to-datom :eavt)]
                 (doseq [^Datom sd slice]
                   (when (and (= (.-e sd) e) (= (.-a sd) attr))
                     (let [arr (object-array 2)
                           eid (.-e sd)]
                       (aset arr 0 (Long/valueOf eid))
                       (aset arr 1 (.-v sd))
                       (.add result arr))))))))
         (when (pos? (.size result))
           (rel/->Relation (zipmap head-vars (range)) (vec result)))))
     :cljs nil))

(defn- extract-demand-values
  "Extract values from delta tuples at the propagation position.
   Adds them to the demand set. Returns true if new values were added."
  [delta-rel prop-pos ^java.util.HashSet demand-set]
  (let [tuples (:tuples delta-rel)
        initial-size (.size demand-set)]
    (doseq [tuple tuples]
      (let [v #?(:clj (if (instance? object-array-class tuple)
                        (aget ^objects tuple (int prop-pos))
                        (nth tuple prop-pos))
                 :cljs (aget tuple prop-pos))]
        (.add demand-set v)))
    (> (.size demand-set) initial-size)))

(defn- execute-recursive-rule
  "Execute a recursive rule via semi-naive fixpoint with clause versions.
   Uses Soufflé-style delta iteration:
   1. Execute base branches for ALL SCC rules → initial 'recent' tuples
   2. Loop: execute clause versions with delta/main accumulators for ALL rules
   3. Terminate when no new tuples produced across any rule, or iteration > 100.
   4. Post-filter: apply constant call-args to restrict final result.

   Magic set optimization: when the called rule has ground arguments, a demand
   set is maintained to restrict DB scans to only reachable entities. This avoids
   computing the full transitive closure when only a subset is needed.

   For mutual recursion (multi-rule SCC), all rules share the fixpoint loop.
   Each rule has its own accumulator."
  [db op ctx]
  (let [{:keys [scc-rule-plans scc-rule-names call-args head-vars rule-name
                base-scan-attr]} op
        max-iterations 100]
    (if (nil? scc-rule-plans)
      ;; No pre-built plans — fall back to legacy
      (let [clause (:clause op)]
        (binding [rel/*implicit-source* (get (:sources ctx) '$)]
          (#?(:clj legacy/solve-rule :cljs (rel/get-legacy-fn :solve-rule)) ctx clause)))
      ;; Semi-naive fixpoint over ALL SCC rules
      ;; Uses mutable HashSet for deduplication (avoids PersistentVector allocation)
      (let [;; Magic set detection — only for single-rule SCCs with binary head vars
            ;; and at least one ground call-arg
            magic-info (compute-magic-info call-args head-vars scc-rule-names)
            magic-demand (when magic-info
                           (let [hs #?(:clj (java.util.HashSet. 64) :cljs (js/Set.))
                                 ground-pos (ffirst (:ground-positions magic-info))
                                 ground-val (get (:ground-positions magic-info) ground-pos)]
                             (.add hs ground-val)
                             hs))
            ;; Track which demand entities have already been base-scanned
            magic-scanned (when magic-info
                            #?(:clj (java.util.HashSet. 64) :cljs (js/Set.)))
            magic-ground-pos (when magic-info (ffirst (:ground-positions magic-info)))
            magic-prop-pos (when magic-info (:propagation-pos magic-info))
            ;; Create per-rule seen-sets for deduplication across iterations
            seen-sets (into {} (map (fn [rn]
                                      [rn #?(:clj (java.util.HashSet. 256)
                                             :cljs (js/Set.))]))
                            scc-rule-names)
            ;; Execute base branches for each SCC rule
            ;; With magic sets: use demand-driven base scan (point lookups only)
            rule-states
            (into {}
                  (map (fn [rn]
                         (let [{:keys [head-vars base-plans]} (get scc-rule-plans rn)
                               base-rel (if (and magic-demand base-scan-attr (= rn rule-name))
                                 ;; Magic: direct EAVT point lookups for demand entities
                                          (or (magic-base-scan db head-vars base-scan-attr magic-demand magic-scanned)
                                              (rel/->Relation (zipmap head-vars (range)) []))
                                 ;; Normal: full base branch plan execution
                                          (execute-branch-plans db base-plans ctx head-vars))
                               delta-rel (rel-dedup-into! base-rel head-vars (get seen-sets rn))]
                  ;; Propagate magic demand from base results
                           (when (and magic-demand (= rn rule-name))
                             (extract-demand-values delta-rel magic-prop-pos magic-demand))
                           [rn {:head-vars head-vars
                                :main-rel delta-rel
                                :delta-rel delta-rel}])))
                  scc-rule-names)
            ;; Main fixpoint loop — Relations throughout, no vector conversion
            ;; With magic sets: re-scan base edges for newly demanded entities each
            ;; iteration (direct EAVT point lookups, not full branch plan re-execution).
            ;; Explosion guard: if demand grows beyond threshold after 3 iterations,
            ;; abandon magic and switch to normal fixpoint.
            magic-explosion-threshold 1000
            final-states
            (loop [states rule-states
                   iteration 0
                   use-magic? (boolean magic-demand)]
              (let [any-delta? (some (fn [[_ s]] (seq (:tuples (:delta-rel s)))) states)]
                (if (or (not any-delta?) (>= iteration max-iterations))
                  states
                  ;; Check for magic explosion
                  (let [use-magic? (and use-magic?
                                        (or (<= iteration 2)
                                            (< #?(:clj (.size ^java.util.HashSet magic-demand)
                                                  :cljs (.-size magic-demand))
                                               magic-explosion-threshold)))
                        ;; Build accumulators for ALL SCC rules
                        acc-map
                        (into {}
                              (map (fn [[rn s]]
                                     (let [hv (:head-vars s)]
                                       [rn {:main (:main-rel s)
                                            :delta (:delta-rel s)
                                            :output-vars hv}])))
                              states)
                        base-aug-ctx (assoc ctx :rule-accumulators acc-map)
                        ;; With magic: inject demand relation to constrain recursive scans
                        aug-ctx (if use-magic?
                                  (inject-magic-relation base-aug-ctx head-vars
                                                         magic-ground-pos magic-demand)
                                  base-aug-ctx)
                        ;; Execute rec clause versions, dedup via seen-set
                        new-states
                        (into {}
                              (map (fn [rn]
                                     (let [{:keys [head-vars rec-clause-versions]} (get scc-rule-plans rn)
                                  ;; With magic: scan newly demanded entities AND run
                                  ;; recursive branches (constrained by demand relation).
                                           magic-base-rel
                                           (when (and use-magic? base-scan-attr (= rn rule-name))
                                             (magic-base-scan db head-vars base-scan-attr
                                                              magic-demand magic-scanned))
                                  ;; Execute recursive clause versions
                                  ;; Optimization: delta-driven expansion for simple binary rules
                                  ;; Instead of full index scan + hash-join, iterate delta tuples
                                  ;; and do reverse AVET lookups — O(delta) instead of O(all-edges)
                                           delta-state (get states rn)
                                           delta-size (count (:tuples (:delta-rel delta-state)))
                                  ;; Use delta-driven when delta is small enough that
                                  ;; point lookups beat full scan + hash-join.
                                  ;; Heuristic: delta-driven wins when delta is very small.
                                  ;; Each AVET lookup costs ~20µs vs scan-all at ~0.3µs/edge.
                                  ;; For 1K edges, break-even is ~15 lookups. Use threshold 16.
                                  ;; Only delta-driven when ALL rec clause versions have
                                  ;; a DB pattern (entity-group/pattern-scan). Pure rule-lookup
                                  ;; clauses (e.g., symmetric rule (follow ?e2 ?e1)) don't
                                  ;; have DB patterns — delta-driven would skip them entirely.
                                           rec-has-db-pattern?
                                           (every? (fn [cv-plan]
                                                     (some #(#{:entity-group :pattern-scan} (:op %))
                                                           (:ops cv-plan)))
                                                   rec-clause-versions)
                                           use-delta-driven? (and base-scan-attr
                                                                  rec-has-db-pattern?
                                                                  (= rn rule-name)
                                                                  (= 1 (count scc-rule-names))
                                                                  (= 2 (count head-vars))
                                                                  (pos? delta-size)
                                                                  (< delta-size 16)
                                                         ;; AVET reverse lookup requires indexed attr
                                                                  (dbu/indexing? db base-scan-attr))
                                           rec-rel (if use-delta-driven?
                                            ;; Delta-driven: for each delta(?t,?y), AVET lookup ?x
                                                     (or (delta-driven-expand db head-vars base-scan-attr
                                                                              (:delta-rel delta-state)
                                                                              0 1)
                                                         (rel/->Relation (zipmap head-vars (range)) []))
                                            ;; Normal: full branch plan execution
                                                     (execute-branch-plans db rec-clause-versions aug-ctx head-vars))
                                  ;; Union base and rec results
                                           new-rel (if (and magic-base-rel (seq (:tuples magic-base-rel)))
                                                     (rel/sum-rel magic-base-rel rec-rel)
                                                     rec-rel)
                                  ;; Fused dedup: only new tuples not in seen-set
                                           delta-rel (rel-dedup-into! new-rel head-vars (get seen-sets rn))
                                  ;; Propagate magic demand from new results
                                           _ (when (and magic-demand (= rn rule-name))
                                               (extract-demand-values delta-rel magic-prop-pos magic-demand))
                                  ;; Accumulate main by summing with delta
                                           old-main (:main-rel (get states rn))
                                           new-main (if (seq (:tuples delta-rel))
                                                      (rel/sum-rel old-main delta-rel)
                                                      old-main)]
                                       [rn {:head-vars head-vars
                                            :main-rel new-main
                                            :delta-rel delta-rel}])))
                              scc-rule-names)]
                    (recur new-states (inc iteration) use-magic?)))))
            ;; Extract result for the called rule
            called-state (get final-states rule-name)
            main-rel (:main-rel called-state)
            ;; Post-filter: restrict to tuples matching constant call-args
            filtered-tuples
            (if (every? analyze/free-var? call-args)
              (:tuples main-rel)
              (let [const-filters (keep-indexed
                                   (fn [i arg]
                                     (when-not (analyze/free-var? arg)
                                       [i arg]))
                                   call-args)]
                (filterv (fn [tuple]
                           (every? (fn [[i expected]]
                                     (= #?(:clj (if (instance? object-array-class tuple)
                                                  (aget ^objects tuple (int i))
                                                  (nth tuple i))
                                           :cljs (aget tuple i))
                                        expected))
                                   const-filters))
                         (:tuples main-rel))))
            ;; Project to only the output vars (free vars from call-args)
            output-vars (vec (filter analyze/free-var? call-args))
            output-indices #?(:clj (int-array (mapv (fn [ov]
                                                      (.indexOf ^java.util.List (vec call-args) ov))
                                                    output-vars))
                              :cljs (mapv (fn [ov]
                                            (.indexOf (to-array call-args) ov))
                                          output-vars))
            ;; Project and dedup final output
            output-seen #?(:clj (java.util.HashSet. (count filtered-tuples))
                           :cljs (js/Set.))
            projected-tuples
            (let [n-out #?(:clj (alength output-indices) :cljs (count output-indices))
                  result #?(:clj (java.util.ArrayList.) :cljs #js [])]
              (doseq [tuple filtered-tuples]
                (let [arr #?(:clj (let [a (object-array n-out)
                                        oa? (instance? object-array-class tuple)]
                                    (dotimes [j n-out]
                                      (let [idx (aget output-indices j)]
                                        (aset a j (if oa? (aget ^objects tuple idx) (nth tuple idx)))))
                                    a)
                             :cljs (let [a (make-array n-out)]
                                     (dotimes [j n-out]
                                       (aset a j (aget tuple (nth output-indices j))))
                                     a))
                      wrapper #?(:clj (ArrayWrapper. arr (java.util.Arrays/hashCode arr))
                                 :cljs (str (vec arr)))]
                  (when #?(:clj (.add output-seen wrapper)
                           :cljs (not (.has output-seen wrapper)))
                    #?(:cljs (.add output-seen wrapper))
                    #?(:clj (.add result arr)
                       :cljs (.push result arr)))))
              #?(:clj (vec result) :cljs (vec result)))
            final-rel (rel/->Relation (zipmap output-vars (range)) projected-tuples)]
        (update ctx :rels rel/collapse-rels final-rel)))))

;; ---------------------------------------------------------------------------
;; ---------------------------------------------------------------------------
;; External engine execution

#?(:clj
   (defn- solver-result-keys
     "Build a mapping from binding vars to result map keys based on query-spec structure.
      Handles aggregate (:group + :agg), window (:window with :as), and generic specs."
     [query-spec binding-vars]
     (cond
       ;; Aggregate: result keys = group cols + agg function names
       (and (map? query-spec) (or (:group query-spec) (:agg query-spec)))
       (let [group-keys (vec (or (:group query-spec) []))
             agg-keys (mapv (fn [agg-spec]
                              (if (sequential? agg-spec)
                                (first agg-spec) ;; :avg, :sum, :count, :min, :max, etc.
                                agg-spec))
                            (or (:agg query-spec) []))
             result-key-order (into group-keys agg-keys)]
         (into {} (map vector binding-vars result-key-order)))

       ;; Window: input cols pass through, window output has :as key
       (and (map? query-spec) (:window query-spec))
       (let [window-specs (:window query-spec)
             ;; Window output keys from :as fields
             window-as-keys (set (keep :as window-specs))
             ;; Binding vars that match a window :as are output cols
             ;; All others are pass-through input cols (name-based)
             ]
         (into {} (map (fn [v] [v (keyword (subs (name v) 1))]) binding-vars)))

       ;; Fallback: keyword from var name
       :else
       (into {} (map (fn [v] [v (keyword (subs (name v) 1))]) binding-vars)))))

#?(:clj
   (defn- execute-external-engine
     "Execute an external engine op and merge results into context.
      Dispatches on :mode — :filter, :retrieval, or :solver."
     [db op ctx]
     (let [mode (:mode op)
           idx-ident (:idx-ident op)
           idx (when idx-ident (get-in db [:secondary-indices idx-ident]))
           fn-sym (:fn-sym op)
           args (:args op)
           binding-form (:binding op)
           binding-vars (if (and (sequential? binding-form)
                                 (sequential? (first binding-form)))
                          (first binding-form)
                          [binding-form])]
       (case mode
         ;; Filter: produce EntityBitSet, create single-column relation of entity IDs.
         ;; Also stores the bitmap in :entity-filters for downstream entity-group optimization.
         :filter
         (if idx
           (let [;; Build query-spec from args (skip the index ident arg)
                 query-args (vec (drop 1 args))
                 resolved-args query-args
                 ;; Build query-spec — the engine function knows its own format
                 ;; For now, call the resolved function with args to get query-spec
                 resolved-fn (when (and (symbol? fn-sym) (namespace fn-sym))
                               (some-> (resolve fn-sym) deref))
                 result-bs (if resolved-fn
                             ;; Call the function which should return results
                             ;; For filter mode, we use sec/-search
                             (sec/-search idx
                                          {:query (first resolved-args)
                                           :field (second resolved-args)}
                                          nil)
                             (es/entity-bitset))
                 ;; Create relation from entity IDs
                 entity-var (first binding-vars)
                 eids (es/entity-bitset-seq result-bs)
                 tuples (mapv (fn [eid] [eid]) eids)
                 rel (rel/->Relation
                      {entity-var 0}
                      (set tuples))]
             (-> ctx
                 (update :rels rel/collapse-rels rel)
                 ;; Store EntityBitSet for downstream entity-group scan optimization
                 (update :entity-filters (fnil assoc {}) entity-var result-bs)))
           ;; No index found — fall back to regular function execution
           (#?(:clj legacy/bind-by-fn :cljs (rel/get-legacy-fn :bind-by-fn)) ctx (:clause op)))

         ;; Retrieval: EntityBitSet + extra columns (score, distance)
         :retrieval
         (if idx
           (let [query-args (vec (drop 1 args))
                 results (sec/-slice-ordered idx
                                             {:query (first query-args)
                                              :field (second query-args)}
                                             nil nil nil nil)
                 ;; Map binding vars to column indices
                 attrs (into {} (map-indexed (fn [i v] [v i]) binding-vars))
                 ;; Build tuples from results
                 tuples (set (mapv (fn [r]
                                     (mapv (fn [v]
                                             (cond
                                               (= v (first binding-vars)) (:entity-id r)
                                               :else (get r (keyword (name v)))))
                                           binding-vars))
                                   results))
                 rel (rel/->Relation attrs tuples)]
             (update ctx :rels rel/collapse-rels rel))
           (#?(:clj legacy/bind-by-fn :cljs (rel/get-legacy-fn :bind-by-fn)) ctx (:clause op)))

         ;; Solver: extract input, call function, merge output
         :solver
         (let [resolved-fn (when (and (symbol? fn-sym) (namespace fn-sym))
                             (some-> (resolve fn-sym) deref))
               query-spec (first (remove analyze/free-var? args))]
           (if resolved-fn
             ;; Derive input vars: all context vars referenced by the query-spec
             (let [all-ctx-vars (into #{} (mapcat (comp keys :attrs)) (:rels ctx))
                   ;; Extract all column keywords referenced in the query-spec
                   spec-cols (when (map? query-spec)
                               (into (set (concat (:group query-spec)
                                                  (keep (fn [agg-spec]
                                                          (when (and (sequential? agg-spec)
                                                                     (> (count agg-spec) 1))
                                                            (last agg-spec)))
                                                        (:agg query-spec))
                                                  (mapcat (fn [w]
                                                            (concat (:partition-by w)
                                                                    (map first (:order w))))
                                                          (:window query-spec))
                                                  (map first (:order query-spec))))
                                     ;; Also include :select if present
                                     (when (:select query-spec) (:select query-spec))))
                   ;; Map spec cols to var symbols: :dept → ?dept
                   spec-vars (set (map #(symbol (str "?" (name %))) spec-cols))
                   ;; Input vars = all context vars needed by query-spec OR binding vars
                   input-vars (filterv #(contains? all-ctx-vars %)
                                       (distinct (concat (sort spec-vars)
                                                         binding-vars)))
                   ;; Convention: ?dept → :dept column name
                   var->col-name (into {} (map (fn [v] [v (keyword (subs (name v) 1))]) input-vars))
                   ;; Find the relation containing these vars
                   input-rels (filter (fn [rel]
                                        (some #(contains? (:attrs rel) %) input-vars))
                                      (:rels ctx))
                   input-rel (first input-rels)
                   ;; Project input vars in stable order
                   ordered-input-vars (sort-by #(get (:attrs input-rel) %) input-vars)
                   col-names (mapv #(get var->col-name %) ordered-input-vars)
                   var-indices (mapv #(get (:attrs input-rel) %) ordered-input-vars)
                   projected-tuples (mapv (fn [tuple]
                                            (mapv (fn [idx]
                                                    (if (sequential? tuple)
                                                      (nth tuple idx)
                                                      (get tuple idx)))
                                                  var-indices))
                                          (vec (:tuples input-rel)))
                   ;; Call the solver function
                   results (resolved-fn query-spec projected-tuples col-names)
                   ;; Build result key mapping from query-spec structure
                   result-keys (solver-result-keys query-spec binding-vars)
                   result-attrs (into {} (map-indexed (fn [i v] [v i]) binding-vars))
                   result-tuples (set (mapv (fn [r]
                                              (mapv (fn [v]
                                                      (get r (get result-keys v)))
                                                    binding-vars))
                                            results))
                   rel (rel/->Relation result-attrs result-tuples)]
               (update ctx :rels rel/collapse-rels rel))
             ;; Can't resolve — error, don't silently fall back
             (throw (ex-info (str "Cannot resolve external engine function: " fn-sym)
                             {:fn-sym fn-sym :clause (:clause op)}))))))))

;; ---------------------------------------------------------------------------
;; Adaptive execution loop (Relation-based)

(def ^:private ^:const replan-threshold 10.0)

(defn- ctx-total-tuples
  "Estimate current total tuple count across all relations in context."
  [ctx]
  (reduce (fn [acc rel] (max acc (count (:tuples rel)))) 0 (:rels ctx)))

(defn- should-replan?
  "Check if we should replan based on actual vs estimated cardinality.
   Returns true if the ratio exceeds the threshold."
  [actual-card estimated-card]
  (when (and actual-card estimated-card
             (pos? estimated-card) (pos? actual-card))
    (let [ratio (/ (double actual-card) (double estimated-card))]
      (or (> ratio replan-threshold)
          (< ratio (/ 1.0 replan-threshold))))))

(defn execute-plan
  "Execute a query plan with adaptive replanning.
   After each op, compares actual cardinality to estimate. If the ratio
   exceeds replan-threshold (10x), re-orders remaining ops.
   Takes and returns a query context."
  [plan context db]
  (if (:has-passthrough? plan)
    nil
    (let [replan-fn plan/replan]
      (loop [ctx context
             plan plan
             idx 0]
        (if (>= idx (count (:ops plan)))
          ctx
          (let [op (nth (:ops plan) idx)
                estimated-card (:estimated-card op)
                ;; Resolve the db for this op — may differ from default $ for multi-source queries
                op-db (if-let [src (:source op)]
                        (or (get (:sources ctx) src)
                            (do (log/warn :datahike/source-not-found "Source not found in query context, using default db"
                                          {:source src :available (set (keys (:sources ctx)))})
                                db))
                        db)]
            (case (:op op)
              ;; Entity group — fused scan+merges (only works with concrete DB sources
              ;; that have PersistentSortedSet indexes — temporal DBs like SinceDB/AsOfDB
              ;; filter via context, not index structure, so fused scan would bypass filters)
              :entity-group
              (if (pss-instance? (:eavt op-db))
                (let [[ctx' consumed] (execute-fused-scan-rel op-db (:scan-op op) (:merge-ops op) ctx)
                      actual-card (ctx-total-tuples ctx')
                      plan' (if (and (> (- (count (:ops plan)) idx) 2)
                                     (should-replan? actual-card estimated-card))
                              (replan-fn plan idx actual-card op-db)
                              plan)]
                  (recur ctx' plan' (inc idx)))
                ;; Temporal/non-standard DB — fall back to per-clause legacy lookup.
                ;; lookup-batch-search takes [source context orig-pattern resolved-pattern];
                ;; passing clause twice is intentional (no resolution needed in fallback).
                (let [scan-clause (:clause (:scan-op op))
                      merge-clauses (mapv :clause
                                          (remove :anti? (:merge-ops op)))
                      anti-clauses (mapv :clause
                                         (filter :anti? (:merge-ops op)))
                      all-clauses (into [scan-clause] merge-clauses)
                      ctx' (binding [rel/*implicit-source* op-db]
                             (reduce (fn [c clause]
                                       (#?(:clj legacy/lookup-batch-search :cljs (rel/get-legacy-fn :lookup-batch-search)) op-db c clause clause))
                                     ctx all-clauses))
                      ;; Apply anti-merges: look up each anti clause and subtract its matches
                      ctx' (binding [rel/*implicit-source* op-db]
                             (reduce (fn [c anti-clause]
                                       (let [join-rel (reduce rel/hash-join (:rels c))
                                             neg-ctx (#?(:clj legacy/lookup-batch-search :cljs (rel/get-legacy-fn :lookup-batch-search))
                                                      op-db (assoc c :rels [join-rel]) anti-clause anti-clause)
                                             neg-join (when (and neg-ctx (seq (:rels neg-ctx)))
                                                        (reduce rel/hash-join (:rels neg-ctx)))
                                             result (if neg-join (rel/subtract-rel join-rel neg-join) join-rel)]
                                         (assoc c :rels [result])))
                                     ctx' anti-clauses))]
                  (recur ctx' plan (inc idx))))

              ;; Single pattern scan
              :pattern-scan
              (if (and op-db (not (dbu/db? op-db)))
                ;; Non-db source (e.g. collection $b) — use legacy lookup
                ;; lookup-pattern-coll takes [source orig-pattern resolved-pattern];
                ;; passing clause twice — no resolution needed in fallback.
                (let [new-rel (#?(:clj legacy/lookup-pattern-coll :cljs (rel/get-legacy-fn :lookup-pattern-coll)) op-db (:clause op) (:clause op))
                      ctx' (binding [rel/*implicit-source* op-db
                                     rel/*lookup-attrs* (lookup-attrs-for-clauses op-db (:clause op) nil)]
                             (update ctx :rels rel/collapse-rels new-rel))]
                  (recur ctx' plan (inc idx)))
                (if (not (pss-instance? (:eavt op-db)))
                  ;; Temporal/non-standard DB — use legacy lookup with search context.
                  ;; lookup-batch-search takes [source context orig-pattern resolved-pattern];
                  ;; passing clause twice — no resolution needed in fallback.
                  (let [ctx' (binding [rel/*implicit-source* op-db]
                               (#?(:clj legacy/lookup-batch-search :cljs (rel/get-legacy-fn :lookup-batch-search)) op-db ctx (:clause op) (:clause op)))]
                    (recur ctx' plan (inc idx)))
                ;; DB source — use fused scan or single pattern scan
                  (let [;; Check if next ops form an ad-hoc fusable group
                        all-ops (:ops plan)
                        e-var (first (:clause op))
                        op-source (:source op)
                        fusable (when (and (= :scan (:join-method op)) (empty? (:rels ctx)))
                                  (loop [j (inc idx) fused []]
                                    (if (>= j (count all-ops))
                                      fused
                                      (let [next-op (nth all-ops j)]
                                        (if (and (= :pattern-scan (:op next-op))
                                                 (= :lookup (:join-method next-op))
                                                 (= e-var (first (:clause next-op)))
                                               ;; Only fuse ops on the same source
                                                 (= op-source (:source next-op)))
                                          (recur (inc j) (conj fused next-op))
                                          fused)))))]
                    (if (seq fusable)
                      (let [[ctx' consumed] (execute-fused-scan-rel op-db op fusable ctx)
                            actual-card (ctx-total-tuples ctx')
                            plan' (if (and (> (- (count (:ops plan)) (+ idx consumed)) 1)
                                           (should-replan? actual-card estimated-card))
                                    (replan-fn plan (+ idx (dec consumed)) actual-card op-db)
                                    plan)]
                        (recur ctx' plan' (long (+ idx consumed))))
                      (let [new-rel (execute-pattern-scan op-db op)
                            ctx' (binding [rel/*implicit-source* op-db
                                           rel/*lookup-attrs* (lookup-attrs-for-clauses op-db (:clause op) nil)]
                                   (update ctx :rels rel/collapse-rels new-rel))
                            actual-card (count (:tuples new-rel))
                            plan' (if (and (> (- (count (:ops plan)) idx) 2)
                                           (should-replan? actual-card estimated-card))
                                    (replan-fn plan idx actual-card op-db)
                                    plan)]
                        (recur ctx' plan' (inc idx)))))))

              :predicate
              (recur (#?(:clj legacy/filter-by-pred :cljs (rel/get-legacy-fn :filter-by-pred)) ctx (:clause op)) plan (inc idx))

              :function
              (recur (#?(:clj legacy/bind-by-fn :cljs (rel/get-legacy-fn :bind-by-fn)) ctx (:clause op)) plan (inc idx))

              :or (recur (execute-or op-db op ctx) plan (inc idx))
              :or-join (recur (execute-or-join op-db op ctx) plan (inc idx))
              :not (recur (execute-not op-db op ctx) plan (inc idx))
              :not-join (recur (execute-not-join op-db op ctx) plan (inc idx))

              :rule-lookup
              (let [acc-map (get-in ctx [:rule-accumulators (:rule-name op)])
                    acc-rel (when acc-map
                              (get acc-map (:mode op)))
                    ;; Map accumulator output-vars to call-args vars
                    mapped-rel (when acc-rel
                                 (let [acc-vars (:output-vars acc-map)
                                       call-args (:call-args op)
                                       ;; Build attr mapping: call-arg-var → position in acc-rel
                                       new-attrs (into {}
                                                       (keep (fn [[acc-var call-var]]
                                                               (when (analyze/free-var? call-var)
                                                                 [call-var (get (:attrs acc-rel) acc-var)])))
                                                       (map vector acc-vars call-args))]
                                   (rel/->Relation new-attrs (:tuples acc-rel))))]
                (recur (if mapped-rel
                         (update ctx :rels rel/collapse-rels mapped-rel)
                         ctx)
                       plan (inc idx)))

              :recursive-rule
              (recur (execute-recursive-rule op-db op ctx) plan (inc idx))

              :external-engine
              (recur (#?(:clj execute-external-engine
                         :cljs (fn [_ _ c] c)) op-db op ctx) plan (inc idx))

              ;; Unknown op — skip
              (recur ctx plan (inc idx)))))))))

;; ---------------------------------------------------------------------------
;; Columnar aggregate execution (JVM only)
;;
;; Instead of materializing tuples (Object[] → PersistentVector → HashSet),
;; writes scan+merge results directly into typed column arrays and delegates
;; aggregation to an external columnar engine (e.g. stratum).
;;
;; For a query like [:find ?d (avg ?s) (count ?e) :where [?e :dept ?d] [?e :salary ?s]]
;; this produces {":dept" String[], ":salary" long[]} and calls the engine's
;; group-by + aggregate, returning result tuples directly.

#?(:clj
   (defn execute-columnar-aggregate
     "Execute a single-group fusable plan, writing results directly into typed
   column arrays. Returns a vector of result tuples (vectors), or nil if
   the plan can't be executed this way.

   aggregate-fn: (fn [column-map group-cols agg-specs]) → seq of result tuples
     column-map: {col-keyword typed-array}
     group-cols: [keyword ...] — columns to group by
     agg-specs:  [[agg-op col-keyword] ...] — e.g. [[:avg :salary] [:count]]

   find-elements: parsed find elements (Variable, Aggregate)
   plan, db: as usual"
     [plan db find-elements aggregate-fn]
     (let [ops (:ops plan)]
       (when (and (not (:has-passthrough? plan))
                  (seq ops)
                  (every? #(#{:entity-group :pattern-scan} (:op %)) ops)
                  (not-any? :source ops)
                  (= 1 (count (filterv #(#{:entity-group :pattern-scan} (:op %)) ops))))
         (let [groups (filterv #(#{:entity-group :pattern-scan} (:op %)) ops)
               g (first groups)
               scan-op (entity-group-scan-op g)
               merge-ops (entity-group-merge-ops g)
            ;; Collect all vars from the plan
               all-vars (into [] (:vars g))

            ;; First pass: execute the fused scan to get Object[] tuples
            ;; (reuses the proven execute-group-direct infrastructure)
               result-list (make-result-list 4000)]
           (execute-group-direct db scan-op merge-ops all-vars nil
                                 result-list nil 0 nil 0 -1 nil
                                 :pipeline (:pipeline g))
           (let [n (.size ^java.util.ArrayList result-list)]
             (when (pos? n)
               (let [attrs (into {} (map-indexed (fn [i v] [v i]) all-vars))

                  ;; Map find-elements to column info
                  ;; Group-by columns: Variable elements → column keyword + index in tuple
                  ;; Aggregate columns: Aggregate elements → agg-op + column keyword + index
                     group-cols (vec (keep-indexed
                                      (fn [_fi fe]
                                        (when-not (instance? datalog.parser.type.Aggregate fe)
                                          (let [var-sym (.-symbol ^datalog.parser.type.Variable fe)
                                                col-idx (get attrs var-sym)]
                                            (when col-idx
                                              {:var var-sym
                                               :col-key (keyword (name var-sym))
                                               :col-idx col-idx}))))
                                      find-elements))
                     agg-cols (vec (keep-indexed
                                    (fn [_fi fe]
                                      (when (instance? datalog.parser.type.Aggregate fe)
                                        (let [agg-sym (.-symbol ^datalog.parser.type.PlainSymbol (.-fn ^datalog.parser.type.Aggregate fe))
                                              agg-args (.-args ^datalog.parser.type.Aggregate fe)
                                            ;; The last arg is the variable being aggregated
                                            ;; (count has no explicit var — counts rows)
                                              agg-var (when (and (seq agg-args)
                                                                 (instance? datalog.parser.type.Variable (last agg-args)))
                                                        (.-symbol ^datalog.parser.type.Variable (last agg-args)))
                                              col-idx (when agg-var (get attrs agg-var))]
                                          {:agg-op (keyword (name agg-sym))
                                           :var agg-var
                                           :col-key (when agg-var (keyword (name agg-var)))
                                           :col-idx col-idx
                                           :find-idx _fi})))
                                    find-elements))

                  ;; Extract typed columns from Object[] tuples
                  ;; Detect types from first tuple, then extract into typed arrays
                     first-tuple ^objects (.get ^java.util.ArrayList result-list 0)

                     extract-column
                     (fn [col-idx]
                       (let [sample (aget first-tuple col-idx)]
                         (cond
                           (instance? Long sample)
                           (let [arr (long-array n)]
                             (dotimes [i n]
                               (aset arr i (long (aget ^objects (.get ^java.util.ArrayList result-list i) col-idx))))
                             arr)

                           (instance? Double sample)
                           (let [arr (double-array n)]
                             (dotimes [i n]
                               (aset arr i (double (aget ^objects (.get ^java.util.ArrayList result-list i) col-idx))))
                             arr)

                           (instance? String sample)
                           (let [arr ^"[Ljava.lang.String;" (make-array String n)]
                             (dotimes [i n]
                               (aset arr i ^String (aget ^objects (.get ^java.util.ArrayList result-list i) col-idx)))
                             arr)

                           :else
                        ;; Generic object array for other types (keywords, etc.)
                           (let [arr (object-array n)]
                             (dotimes [i n]
                               (aset arr i (aget ^objects (.get ^java.util.ArrayList result-list i) col-idx)))
                             arr))))

                  ;; Build column map for the aggregate engine
                     column-map (into {}
                                      (concat
                                       (map (fn [{:keys [col-key col-idx]}]
                                              [col-key (extract-column col-idx)])
                                            group-cols)
                                       (keep (fn [{:keys [col-key col-idx]}]
                                               (when col-idx
                                                 [col-key (extract-column col-idx)]))
                                             agg-cols)))

                     group-keys (mapv :col-key group-cols)
                     agg-specs (mapv (fn [{:keys [agg-op col-key]}]
                                       (if col-key
                                         [agg-op col-key]
                                         [agg-op])) ;; e.g. [:count] with no column
                                     agg-cols)]

              ;; Call the external aggregate engine
                 (aggregate-fn column-map group-keys agg-specs)))))))))

