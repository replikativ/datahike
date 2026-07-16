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
  #?(:cljs (:require-macros [datahike.query.execute :refer [scan-filter scan-filter-temporal emit-tuple check-cancel!]]))
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
;; Mid-query cancellation.
;;
;; `cancel` is an optional IDeref (typically a Volatile) threaded through the
;; query context. When its stored value is truthy, the next `check-cancel!`
;; throws an ex-info with :datahike/canceled true.
;;
;; Datahike itself is protocol-agnostic — the raised ex-info carries
;; :datahike/canceled only. Adapter layers (pgwire, client drivers) map
;; this to their own error codes at the boundary.
;;
;; Cost model: when `cancel` is nil (non-pgwire callers), the nil guard
;; short-circuits before any deref — a single ifnull + predicted-not-taken
;; branch per check, far below measurement noise.

(defmacro check-cancel!
  "Throw :datahike/canceled if `cancel` (a Volatile or Atom, or nil) holds a
   truthy value. No-op on nil. Hot-path safe: callers should bind the value
   from `(:cancel ctx)` to a local *outside* any tight loop, then pass the
   local to this macro inside the loop.

   On the JVM, expands to a direct `.deref` via IDeref (monomorphic when the
   local is always a Volatile). On CLJS, uses `deref` (protocol dispatch);
   cancel in CLJS is not currently exercised in the hot path."
  [cancel]
  (if (:ns &env)
    ;; CLJS expansion
    `(when-let [c# ~cancel]
       (when (cljs.core/deref c#)
         (throw (ex-info "query canceled"
                         {:datahike/canceled true}))))
    ;; CLJ expansion — direct .deref via IDeref hint
    `(when-let [^clojure.lang.IDeref c# ~cancel]
       (when (.deref c#)
         (throw (ex-info "query canceled"
                         {:datahike/canceled true}))))))

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

(defn- result-list-set [list i v]
  #?(:clj  (.set ^java.util.ArrayList list (int i) v)
     :cljs (aset list i v)))

(defn- result-list-trim
  "Remove all entries from index `from` to end."
  [list from]
  #?(:clj  (loop [i (dec (result-list-size list))]
             (when (>= i from)
               (.remove ^java.util.ArrayList list (int i))
               (recur (dec i))))
     :cljs (.splice list from)))

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

;; Probe-map: HashMap<probe-value → ArrayList<Object[]>> for producer find-var propagation.
;; Each key is a probe-value (join-var), each value is a list of producer find-var tuples.

(defn- make-probe-map
  "Create a mutable map for hash-probe joins with value propagation."
  [capacity]
  #?(:clj  (java.util.HashMap. (int capacity))
     :cljs (js/Map.)))

(defn- probe-map-add
  "Add a producer find-var tuple to the probe-map under the given probe-value.
   Supports multiple producer tuples per probe-value (card-many joins)."
  [m k v]
  #?(:clj  (let [^java.util.ArrayList existing (.get ^java.util.HashMap m k)]
             (if existing
               (.add existing v)
               (.put ^java.util.HashMap m k (doto (java.util.ArrayList. 4) (.add v)))))
     :cljs (let [existing (.get m k)]
             (if existing
               (.push existing v)
               (.set m k #js [v])))))

(defn- probe-map-get
  "Get the list of producer find-var tuples for a probe-value, or nil."
  [m k]
  #?(:clj  (.get ^java.util.HashMap m k)
     :cljs (.get m k)))

(defn- probe-map->set
  "Extract keys from a probe-map into a probe-set for consumer scan filtering."
  [m]
  #?(:clj  (java.util.HashSet. (.keySet ^java.util.HashMap m))
     :cljs (let [s (js/Set.)]
             (.forEach m (fn [_v k] (.add s k)))
             s)))

(defn- probe-map-entry-size [entry]
  #?(:clj  (.size ^java.util.ArrayList entry)
     :cljs (.-length entry)))

(defn- probe-map-entry-get [entry i]
  #?(:clj  (.get ^java.util.ArrayList entry (int i))
     :cljs (aget entry i)))

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
  "Create a PersistentVector from an object array.

   `clojure.lang.PersistentVector/adopt` is fast (zero-copy) but only
   correct when `arr.length <= 32`. It constructs the vector with
   `root = EMPTY_NODE` and the data in the tail, which is the
   PersistentVector internal layout for short vectors. For arrays
   longer than 32 the result is silently corrupt: `cnt > 32` but
   `tailoff() = cnt-32 > 0`, and any `arrayFor(i)` for i < tailoff
   walks `EMPTY_NODE.array` and NPEs on the first level.

   Real-world repro: SELECT against a 33+-column table from pgwire
   (Odoo's res_partner has 34 columns). The corrupt row crashes at
   the first `seq`/`nth`/`take` with `Cannot read field \"array\"
   because \"node\" is null`.

   `LazilyPersistentVector/createOwning` does the right dispatch:
   the cheap adopt for length ≤ 32, and `PersistentVector/create`
   (transient-build, valid tree) for longer arrays. Still no copy
   in the short path."
  [^objects arr]
  #?(:clj  (clojure.lang.LazilyPersistentVector/createOwning arr)
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

#?(:clj
   (defmacro ^:private merge-datom-match?
     "Inline predicate: does merge datom d match entity+attr+value+shared-var constraints?
      Expands in place for zero-overhead in hot loops. Type-hints d and scan-d internally."
     [d eid ra vg? vgv check-v? check-tx? scan-d]
     (let [d# (with-meta d {:tag 'datahike.datom.Datom})
           sd# (with-meta scan-d {:tag 'datahike.datom.Datom})]
       `(and (== (.-e ~d#) ~eid) (= (.-a ~d#) ~ra)
             (or (not ~vg?) (val-eq? (.-v ~d#) ~vgv))
             (or (not ~check-v?) (val-eq? (.-v ~d#) (.-v ~sd#)))
             (or (not ~check-tx?) (= (datom/datom-tx ~d#) (datom/datom-tx ~sd#)))))))

#?(:cljs
   (defn- merge-datom-match?
     "Predicate: does merge datom d match entity+attr+value+shared-var constraints?"
     [d eid ra vg? vgv check-v? check-tx? scan-d]
     (and (== (.-e d) eid) (= (.-a d) ra)
          (or (not vg?) (val-eq? (.-v d) vgv))
          (or (not check-v?) (val-eq? (.-v d) (.-v scan-d)))
          (or (not check-tx?) (= (datom/datom-tx d) (datom/datom-tx scan-d))))))

#?(:clj
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
             (or (not ~check-tx?) (= (datom/datom-tx ~d#) (datom/datom-tx ~sd#)))))))

#?(:cljs
   (defn- temporal-merge-datom-match?
     "Like merge-datom-match? but also checks temporal-tx-filter and added-filter."
     [d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter]
     (and (== (.-e d) eid) (= (.-a d) ra)
          (or (not vg?) (val-eq? (.-v d) vgv))
          (or (nil? temporal-tx-filter) (temporal-tx-filter d))
          (or (nil? added-filter) (= (datom/datom-added d) added-filter))
          (or (not check-v?) (val-eq? (.-v d) (.-v scan-d)))
          (or (not check-tx?) (= (datom/datom-tx d) (datom/datom-tx scan-d))))))

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
        (every? (fn [{:keys [op const-val] :as pred}]
                  (case op
                    > (> (compare dv const-val) 0)
                    < (< (compare dv const-val) 0)
                    not= (not= dv const-val)
                    (throw (ex-info "Unhandled op in build-strict-filter — every op pushdown-to-bounds writes into :strict-preds must have a case arm here"
                                    {:op op :pred pred}))))
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

#?(:clj
   (defn- pattern-probe-set
     "Build a probe HashSet from `rels` for a scan clause's bound entity- or
      value-var, when restricting the scan by it would help (the bound set is
      smaller than the scan slice `scan-n`). Returns
      {:field 0|2 :values HashSet :seekable? bool} or nil — :field 0 = entity
      (EAVT-seekable), 2 = value (AVET-seekable only if the attr is indexed)."
     [db clause resolved-a rels scan-n]
     (when (seq rels)
       (let [scan-n (long scan-n)
             [e _ v] clause
             extract (fn [rel col-idx]
                       (let [tuples (:tuples rel)
                             n (if (instance? java.util.Collection tuples)
                                 (.size ^java.util.Collection tuples)
                                 (count tuples))]
                         (when (pos? n)
                           ;; Gate on the DISTINCT value count, not the raw tuple
                           ;; count: a large intermediate rel from a deep join can
                           ;; have many rows but few distinct values for this var
                           ;; (e.g. ?b across a 219k-row hop result). Build the set
                           ;; with an early-exit once it reaches scan-n — at that
                           ;; point it isn't selective, so fall back to a full scan.
                           ;; Bounds build cost to O(min(n, until scan-n distinct)).
                           (let [hs (java.util.HashSet.)]
                             (reduce (fn [_ t]
                                       (let [val (cond
                                                   (instance? clojure.lang.Indexed t) (.nth ^clojure.lang.Indexed t col-idx)
                                                   (sequential? t) (nth t col-idx)
                                                   :else (get t col-idx))]
                                         (when (some? val) (.add hs val)))
                                       (if (>= (.size hs) scan-n) (reduced nil) nil))
                                     nil tuples)
                             (when (and (pos? (.size hs)) (< (.size hs) scan-n)) hs)))))
             e-probe (when (and (symbol? e) (analyze/free-var? e))
                       (some (fn [rel]
                               (when-let [ci (get (:attrs rel) e)]
                                 (when-let [hs (extract rel ci)]
                                   {:field 0 :values hs :seekable? true})))
                             rels))
             v-probe (when (and (not e-probe) resolved-a
                                (symbol? v) (analyze/free-var? v))
                       (some (fn [rel]
                               (when-let [ci (get (:attrs rel) v)]
                                 (when-let [hs (extract rel ci)]
                                   {:field 2 :values hs
                                    :seekable? (boolean (and (:avet db) (dbu/indexing? db resolved-a)))})))
                             rels))]
         (or e-probe v-probe)))))

#?(:clj
   (defn- var-attr-avet-pairs
     "Distinct (attr, value) pairs bound in `rels` for a VARIABLE-attribute
      clause `[?e ?a ?v]` (both ?a and ?v logic vars), returned only when a
      per-pair AVET point-seek beats a full scan: one rel binds BOTH ?a and ?v,
      every distinct attr is AVET-indexed, and the pair count is selective
      (K * probe-driven-threshold < scan-n). nil → caller falls back to
      scan/filter. Result-preserving: the seeks only restrict to the bound
      (attr, value) set the downstream join enforces anyway."
     [db clause rels scan-n]
     (let [[_ a v] clause
           scan-n (long scan-n)
           cell (fn [t i] (cond (instance? clojure.lang.Indexed t) (.nth ^clojure.lang.Indexed t (int i))
                                (sequential? t) (nth t i)
                                :else (get t i)))]
       (some (fn [rel]
               (let [ai (get (:attrs rel) a)
                     vi (get (:attrs rel) v)]
                 (when (and ai vi)
                   (let [seen (java.util.HashSet.)
                         aborted (reduce (fn [_ t]
                                           (let [pa (cell t ai) pv (cell t vi)]
                                             (when (and (some? pa) (some? pv)) (.add seen [pa pv])))
                                           (if (>= (.size seen) scan-n) (reduced true) nil))
                                         nil (:tuples rel))]
                     (when (and (not (true? aborted)) (pos? (.size seen)))
                       (let [ps (vec seen)]
                         (when (and (< (* (long (count ps)) (long probe-driven-threshold)) scan-n)
                                    (every? (fn [[pa _]] (dbu/indexing? db pa)) ps))
                           ps)))))))
             rels))))

(defn- scan-datoms
  "Datoms for a scan clause, driven by the currently-bound `rels` (sideways
   information passing) when beneficial — the single retrieval seam shared by
   every scan site. Three regimes, chosen from K=#bound-values vs N=scan slice
   size (`scan-n`, the pattern's estimated attribute cardinality):

     seek   index point-seeks per bound value   (K*threshold < N, and seekable)
     filter full slice, drop non-matching datoms (K < N)            [semi-join]
     scan   full index slice                     (no useful probe)

   Result-preserving: seek/filter only restrict the scan to the bound
   entity/value set, which the downstream join enforces anyway — so this never
   changes results, only how many datoms (and downstream merge lookups) are
   touched."
  [db clause index pushdown-preds rels scan-n]
  (let [[e a v] clause
        resolved-a (when (and (some? a) (not (symbol? a))) (resolve-attr db a))
        resolved-e (when (and (some? e) (not (symbol? e)) (number? e)) #?(:clj (long e) :cljs e))
        pushdown-bounds (when (seq pushdown-preds) (plan/pushdown-to-bounds pushdown-preds))
        [from to] (compute-slice-bounds clause index pushdown-bounds resolved-a resolved-e)
        db-index (get db index)
        scan-n (long (or scan-n (di/-count db-index)))
        full (fn [] (di/-slice db-index from to index))]
    #?(:clj
       (if-let [avet-pairs (when (and (:avet db)
                                      (nil? resolved-a)
                                      (symbol? a) (analyze/free-var? a)
                                      (symbol? v) (analyze/free-var? v))
                             (var-attr-avet-pairs db clause rels scan-n))]
         ;; Variable-attribute pattern [?e ?a ?v] with ?a and ?v both bound
         ;; upstream (e.g. a reference-driven join whose attribute comes from the
         ;; data): AVET point-seek per (attr, value) pair instead of full-scanning
         ;; EAVT and filtering. O(pairs * log n) vs O(all datoms).
         (mapcat (fn [[pa pv]]
                   (di/-slice (:avet db) (datom e0 pa pv tx0) (datom emax pa pv txmax) :avet))
                 avet-pairs)
         (if-let [probe (pattern-probe-set db clause resolved-a rels scan-n)]
           (let [k     (.size ^java.util.HashSet (:values probe))
                 field (int (:field probe))
                 seek-index (if (== field 2) (:avet db) (:eavt db))]
           ;; Seeks build (entity attr nil)/(e0 attr value) probe datoms and use
           ;; a PersistentSortedSet ForwardCursor, so they need (a) a PSS index
           ;; and (b) a RESOLVED, non-nil attribute — a variable-attribute
           ;; pattern [?e ?a ?v] has resolved-a=nil, which would feed nil into
           ;; the no-nil-check attr comparator (cmp-attr-quick → NPE). In both
           ;; cases the index-agnostic filter regime below is correct instead.
             (if (and (:seekable? probe)
                      (some? resolved-a)
                      (pss-instance? seek-index)
                      (< (* (long k) (long probe-driven-threshold)) scan-n))
               (probe-driven-iterable seek-index resolved-a (:values probe) field)
               (let [^java.util.HashSet hs (:values probe)]
                 (filter (fn [^Datom d]
                           (.contains hs (if (== field 0) (.-e d) (.-v d))))
                         (full)))))
           (full)))
       :cljs (full))))

(defn- execute-pattern-scan [db op cancel rels]
  (let [{:keys [clause index pushdown-preds]} op
        pushdown-bounds (when (seq pushdown-preds) (plan/pushdown-to-bounds pushdown-preds))
        datoms (scan-datoms db clause index pushdown-preds rels (:estimated-card op))
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
                           (map (fn [^Datom d]
                                  (check-cancel! cancel)
                                  ;; 5th component is the datom's actual added flag, NOT a
                                  ;; hardcoded true — history/temporal scans emit retraction
                                  ;; datoms whose op-var (e.g. [?e :age ?a ?t ?op]) must bind
                                  ;; false. Mirrors the fused path's find-src-scan-added slot.
                                  #?(:clj  [(.-e d) (.-a d) (.-v d) (.-tx d) (datom/datom-added d)]
                                     :cljs (let [t (make-array 5)]
                                             (aset t 0 (.-e d))
                                             (aset t 1 (.-a d))
                                             (aset t 2 (.-v d))
                                             (aset t 3 (.-tx d))
                                             (aset t 4 (datom/datom-added d))
                                             t)))))
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

(def ^:private date-tx-id-cache
  "Memoizes Date→tx-id resolution. `temporal-info` is recomputed per plan-op, so a
   single date-based `(d/as-of db <Date>)` query would otherwise re-scan the whole
   `:db/txInstant` log once per op (~Nx). Keyed on the origin-db's store id +
   branch + `:max-tx` (which together identify an immutable db state, so the cached
   tx-id can never be stale) plus the Date. Bounded; dropped wholesale when large."
  (atom {}))

(defn- date-tx-id-cache-key [origin-db date]
  (let [cfg (dbi/-config origin-db)
        sid (get-in cfg [:store :id])
        mt  (:max-tx origin-db)]
    (when (and sid mt) [sid (:branch cfg) mt date])))

(defn- resolve-date-to-tx-id*
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

(defn- resolve-date-to-tx-id
  "Resolve a Date time-point to the max numeric tx-id where :db/txInstant <= date.
   Scans AEVT for :db/txInstant datoms on the origin DB. Returns tx0 if none.
   Memoized per (origin-db :commit-id, date) — see `date-tx-id-cache`."
  [origin-db date]
  (let [k   (date-tx-id-cache-key origin-db date)
        hit (when k (get @date-tx-id-cache k))]
    (if (some? hit)
      hit
      (let [v (resolve-date-to-tx-id* origin-db date)]
        (when k
          (swap! date-tx-id-cache (fn [m] (assoc (if (> (count m) 2048) {} m) k v))))
        v))))

(defn- temporal-info
  "Extract temporal metadata from a DB wrapper.
   Date-based time-points are resolved to numeric tx-ids via AVET lookup on the origin DB.
   Returns nil for regular DBs.

   Recurses through `FilteredDB` so a composition like
   `(d/valid-at (d/as-of db t) inst)` — FilteredDB wrapping AsOfDB —
   still reports `:as-of` to the planner. The FilteredDB's predicate
   stays on the outer `db`'s search-context and fires via
   `post-process-datoms` alongside the AsOf timepred."
  [db]
  (cond
    (instance? FilteredDB db)
    (temporal-info (.-unfiltered-db ^FilteredDB db))

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

(defn- maybe-post-process
  "Pipe `slice` through `post-process-datoms` iff the wrapper `db`'s
   search-context carries an xform-after (set by FilteredDB or any
   future wrapper) or a timepred (legacy temporal path). When neither
   is set the slice is returned unchanged — `post-process-datoms`'s
   `:else` branch short-circuits, so this guard avoids the
   `(into [] xform datoms)` realization cost on plain dbs.

   This is what lifts FilteredDB's predicate into the planner's hot
   scan: without this hook the planner short-circuited filtered dbs
   into raw slices, silently dropping the predicate. `d/valid-at`
   rides on the same hook via a `(d/filter db vt-pred)` wrap."
  [slice db origin-db]
  (let [ctx (dbi/-search-context db)]
    (if (or (dbi/context-xform ctx) (dbi/context-time-pred ctx))
      (db/post-process-datoms slice origin-db ctx)
      slice)))

(defn- temporal-merge-slice
  "Get merged datoms for [eid attr] from current+temporal indexes.
   For historical: merge via distinct-datoms (all versions).
   For as-of/since: merge + post-process-datoms (time filter + assemble).
   For regular DB: pass through `maybe-post-process` so FilteredDB
   (and `d/valid-at`, which is `(d/filter db vt-pred)` underneath)
   actually fires."
  [origin-db from-d to-d temporal-type temporal-tx-filter db]
  (let [current-slice (di/-slice (:eavt origin-db) from-d to-d :eavt)]
    (case temporal-type
      :historical
      (let [temporal-index (:temporal-eavt origin-db)
            merged (dbu/distinct-datoms origin-db :eavt
                                        current-slice
                                        (di/-slice temporal-index from-d to-d :eavt))]
        ;; Same xform-after lift as the as-of/since branch — keeps
        ;; FilteredDB-around-HistoricalDB working.
        (maybe-post-process merged db origin-db))

      (:as-of :since)
      (let [temporal-index (:temporal-eavt origin-db)
            merged (dbu/distinct-datoms origin-db :eavt
                                        current-slice
                                        (di/-slice temporal-index from-d to-d :eavt))
            ctx (dbi/-search-context db)]
        (db/post-process-datoms merged origin-db ctx))

      ;; regular DB
      (maybe-post-process current-slice db origin-db))))

(defn- visible-eavt-datom
  "Find the visible temporal EAVT datom for one card-one merge key.
   `temporal-merge-slice` already assembles the [eid ra] history down to the
   single visible version (or none) for as-of, so we just return the datom that
   also satisfies the scan-relative merge constraints (self-join value/tx checks)."
  [origin-db db temporal-type temporal-eavt
   eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter]
  (when temporal-eavt
    (let [from-d (datom eid ra (when vg? vgv) tx0)
          to-d (datom eid ra (when vg? vgv) txmax)
          slice (temporal-merge-slice origin-db from-d to-d temporal-type temporal-tx-filter db)]
      (some (fn [^Datom td]
              (when (temporal-merge-datom-match? td eid ra vg? vgv check-v? check-tx? scan-d
                                                 temporal-tx-filter added-filter)
                td))
            slice))))

(defn- build-scan-slice
  "Build the scan slice for execute-group-direct / execute-fused-scan-rel.
   For temporal DBs, merges current + temporal indexes.
   db is the temporal wrapper (needed for search-context), origin-db is the unwrapped DB."
  [db db-index from-datom to-datom index temporal origin-db resolved-a]
  (if-not temporal
    ;; Regular DB hot path: still let FilteredDB / d/valid-at fire via
    ;; the wrapper's xform-after on the search-context. No-op when
    ;; neither xform nor timepred is set (vast majority of queries).
    (maybe-post-process (di/-slice db-index from-datom to-datom index) db origin-db)
    (let [temporal-type (:type temporal)
          as-of-at-max-tx? (and (= temporal-type :as-of)
                                (= (long (:time-point temporal))
                                   (long (:max-tx origin-db))))
          temporal-index-key (keyword (str "temporal-" (name index)))]
      (case temporal-type
        :historical
        (let [temporal-index (get origin-db temporal-index-key)
              raw #?(:clj (fast-merge-scan db-index from-datom to-datom
                                           temporal-index index origin-db resolved-a)
                     :cljs (dbu/distinct-datoms origin-db index
                                                (di/-slice db-index from-datom to-datom index)
                                                (di/-slice temporal-index from-datom to-datom index)))]
          ;; Wrapper (e.g. FilteredDB-around-HistoricalDB for valid-at on history)
          ;; still gets its xform-after applied.
          (maybe-post-process raw db origin-db))

        (:as-of :since)
        (if as-of-at-max-tx?
          (maybe-post-process (di/-slice db-index from-datom to-datom index) db origin-db)
          (let [temporal-index (get origin-db temporal-index-key)
                merged (dbu/distinct-datoms origin-db index
                                            (di/-slice db-index from-datom to-datom index)
                                            (di/-slice temporal-index from-datom to-datom index))
                ctx (dbi/-search-context db)]
            (db/post-process-datoms merged origin-db ctx)))

        ;; default: regular slice — still apply xform-after if set
        (maybe-post-process (di/-slice db-index from-datom to-datom index) db origin-db)))))

;; ---------------------------------------------------------------------------
;; Path functions — each small enough for JIT C2/Graal compilation.
;; The macros scan-filter and emit-tuple expand inline within each path.

(defn- execute-scan-only
  "Path 1: Scan without merges (e.g. Q1). Smallest possible loop.
   `cancel` is an IDeref (typically Volatile) or nil; checked per iteration
   via the nil-guarded `check-cancel!` macro."
  [slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n cancel]
  (let [^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (check-cancel! cancel)
           (let [^Datom scan-d (.next iter)]
             (when (scan-filter scan-d ground-filter strict-filter probe-set probe-datom-field)
               (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                           n-find find-source const-vals result-list)))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (check-cancel! cancel)
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
   result-list max-n n-merges merge-ctx cancel]
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
           (check-cancel! cancel)
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
         (check-cancel! cancel)
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
      result-list max-n n-merges attr-refs? sorted-ctx cancel]
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
             (check-cancel! cancel)
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
             (check-cancel! cancel)
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
                 merge-check-scan-v merge-check-scan-tx merge-optional merge-defaults]."
  [eavt-pss slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n n-merges merge-ctx cancel]
  (let [^objects merge-attrs (aget ^objects merge-ctx 0)
        ^objects merge-v-ground (aget ^objects merge-ctx 1)
        ^objects merge-v-vals (aget ^objects merge-ctx 2)
        ^objects merge-anti (aget ^objects merge-ctx 3)
        ^objects merge-cursors (aget ^objects merge-ctx 4)
        ^objects merge-check-scan-v (aget ^objects merge-ctx 5)
        ^objects merge-check-scan-tx (aget ^objects merge-ctx 6)
        ^objects merge-optional (when (> (alength ^objects merge-ctx) 7) (aget ^objects merge-ctx 7))
        ^objects merge-defaults (when (> (alength ^objects merge-ctx) 8) (aget ^objects merge-ctx 8))
        ^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (check-cancel! cancel)
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
                                   ;; Not found: check if this merge is optional (get-else)
                                   (if (and merge-optional (aget merge-optional mi))
                                     ;; Optional: create synthetic datom with default value
                                     (do (aset merge-datoms mi
                                               (datom eid ra (aget merge-defaults mi) tx0))
                                         (recur (unchecked-inc-int mi) true))
                                     ;; Regular: short-circuit (skip entity)
                                     (recur (unchecked-inc-int mi) false)))))))]
                 (when ok?
                   (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                               n-find find-source const-vals result-list)))))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (check-cancel! cancel)
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
                               ;; Not found: check optional
                               (if (and merge-optional (aget merge-optional mi))
                                 (do (aset merge-datoms mi
                                           (datom eid ra (aget merge-defaults mi) tx0))
                                     (recur (inc mi) true))
                                 (recur (inc mi) false)))))))]
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

(defn- execute-temporal-scan-only
  "Temporal path 1: scan without merges."
  [slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n temporal-tx-filter scan-added-val cancel]
  (let [^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (check-cancel! cancel)
           (let [^Datom scan-d (.next iter)]
             (when (and (scan-filter-temporal scan-d ground-filter strict-filter probe-set probe-datom-field temporal-tx-filter)
                        (or (nil? scan-added-val) (= (datom/datom-added scan-d) scan-added-val)))
               (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                           n-find find-source const-vals result-list)))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (check-cancel! cancel)
         (when (and (scan-filter-temporal scan-d ground-filter strict-filter probe-set probe-datom-field temporal-tx-filter)
                    (or (nil? scan-added-val) (= (datom/datom-added scan-d) scan-added-val)))
           (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                       n-find find-source const-vals result-list))))))

(defn- fast-eligible?
  "True iff a temporal merge is the exact shape the cursor fast path handles:
   a single, probe-less (fully unbound), card-many, temporal-only, NON-anti,
   NON-optional merge with a pre-built ForwardCursor. Everything else (probe-bound
   as-of, multi-merge, anti, get-else/optional, no cursor) delegates to the slow
   path unchanged."
  [n-merges temporal-ctx probe-set]
  (and (== (int n-merges) 1)
       (nil? probe-set)
       (let [^objects tc temporal-ctx
             ^objects merge-anti (aget tc 3)
             ^objects merge-card-many (aget tc 4)
             ^objects merge-temporal-only (aget tc 8)
             ^objects temporal-cursors (aget tc 11)
             ^objects merge-optional (when (> (alength tc) 16) (aget tc 16))]
         (and (aget merge-card-many 0)
              (aget merge-temporal-only 0)
              (not (aget merge-anti 0))
              (or (nil? merge-optional) (not (aget merge-optional 0)))
              (some? temporal-cursors)
              (some? (aget temporal-cursors 0))))))

#?(:clj
   (defn- execute-temporal-merge-fast
     "Fast path for a single card-many temporal-only non-anti non-optional merge
      with a forward cursor (already created by execute-group-direct). Replaces the
      ~N root-anchored per-entity -slice calls (one per scanned datom) with ONE
      monotonically advancing ForwardCursor: the scan emits entities in ascending
      `e` and temporal-eavt is EAVT-sorted, so seekGE never re-seeks from root.

      Peek-ahead handles the history cartesian: a singleton entity (next scan datom
      is a different eid) emits directly while walking the cursor (single touch); a
      repeated entity (next scan datom is the same eid — multiple name × age
      versions) materializes the matched datoms into a small replay buffer once,
      then replays it for each repeat without moving the cursor."
     [eavt-pss slice ground-filter strict-filter
      probe-set probe-datom-field
      collect-set collect-datom-field collect-merge-idx
      merge-datoms n-find find-source const-vals
      result-list max-n temporal-ctx cancel]
     (let [^objects merge-attrs (aget ^objects temporal-ctx 0)
           ^objects merge-v-ground (aget ^objects temporal-ctx 1)
           ^objects merge-v-vals (aget ^objects temporal-ctx 2)
           ^objects merge-added-filter (aget ^objects temporal-ctx 5)
           ^objects merge-check-scan-v (aget ^objects temporal-ctx 6)
           ^objects merge-check-scan-tx (aget ^objects temporal-ctx 7)
           ^objects temporal-cursors (aget ^objects temporal-ctx 11)
           temporal-tx-filter (aget ^objects temporal-ctx 13)
           scan-added-val (aget ^objects temporal-ctx 14)
           ^objects merge-datoms merge-datoms
           ^ints find-source find-source
           ^objects const-vals const-vals
           ra (aget merge-attrs 0)
           vg? (aget merge-v-ground 0)
           vgv (aget merge-v-vals 0)
           check-v? (aget merge-check-scan-v 0)
           check-tx? (aget merge-check-scan-tx 0)
           added-filter (aget merge-added-filter 0)
           ^PersistentSortedSet$ForwardCursor cur (aget temporal-cursors 0)
           buf (java.util.ArrayList.)]
       (when-let [^java.util.Iterator it (some-> ^Iterable slice .iterator)]
         (loop [^Datom cur-d (when (.hasNext it) (.next it))
                buffer-eid -1]
           (when (and cur-d (or (neg? max-n) (< (result-list-size result-list) max-n)))
             (check-cancel! cancel)
             (let [^Datom nxt-d (when (.hasNext it) (.next it))
                   next-eid (if nxt-d (.-e nxt-d) -1)]
               (if (and (scan-filter-temporal cur-d ground-filter strict-filter probe-set probe-datom-field temporal-tx-filter)
                        (or (nil? scan-added-val) (= (datom/datom-added cur-d) scan-added-val)))
                 (let [eid (.-e cur-d)
                       scan-d cur-d]
                   (if (== eid buffer-eid)
                     ;; replay buffer (same entity, cursor already consumed)
                     (do (dotimes [bi (.size buf)]
                           (aset merge-datoms 0 ^Datom (.get buf bi))
                           (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                                       n-find find-source const-vals result-list))
                         (recur nxt-d buffer-eid))
                     ;; advance cursor to this entity
                     (let [probe (datom eid ra (when vg? vgv) tx0)
                           ^Datom d0 (.seekGE cur probe)]
                       (if (== next-eid eid)
                         ;; repeats follow -> materialize buffer, emit
                         (do (.clear buf)
                             (loop [^Datom md d0]
                               (when (and md (== (.-e md) eid) (= (.-a md) ra))
                                 (when (temporal-merge-datom-match? md eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)
                                   (.add buf md))
                                 (recur (.next cur))))
                             (dotimes [bi (.size buf)]
                               (aset merge-datoms 0 ^Datom (.get buf bi))
                               (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                                           n-find find-source const-vals result-list))
                             (recur nxt-d eid))
                         ;; singleton -> direct emit (single touch)
                         (do (loop [^Datom md d0]
                               (when (and md (== (.-e md) eid) (= (.-a md) ra))
                                 (when (temporal-merge-datom-match? md eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)
                                   (aset merge-datoms 0 md)
                                   (emit-tuple scan-d collect-set collect-datom-field collect-merge-idx merge-datoms
                                               n-find find-source const-vals result-list))
                                 (recur (.next cur))))
                             (recur nxt-d -1))))))
                 ;; scan datom filtered out
                 (recur nxt-d buffer-eid)))))))))

(declare execute-temporal-merge-slow)

(defn- execute-temporal-merge
  "Temporal path 2 dispatcher: route the probe-less single card-many temporal-only
   merge (the fully-unbound history/as-of entity-group, e.g. [?e :name ?n][?e :age ?a])
   through the one-pass ForwardCursor fast path; every other shape runs the slow
   path unchanged."
  [db eavt-pss slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n n-merges temporal-ctx cancel]
  (if #?(:clj (fast-eligible? n-merges temporal-ctx probe-set) :cljs false)
    #?(:clj (execute-temporal-merge-fast eavt-pss slice ground-filter strict-filter
                                         probe-set probe-datom-field
                                         collect-set collect-datom-field collect-merge-idx
                                         merge-datoms n-find find-source const-vals
                                         result-list max-n temporal-ctx cancel)
       :cljs nil)
    (execute-temporal-merge-slow db eavt-pss slice ground-filter strict-filter
                                 probe-set probe-datom-field
                                 collect-set collect-datom-field collect-merge-idx
                                 merge-datoms n-find find-source const-vals
                                 result-list max-n n-merges temporal-ctx cancel)))

(defn- execute-temporal-merge-slow
  "Temporal path 2: merge with card-many/card-one, anti-merge, cursor cache.
   temporal-ctx packs temporal-specific arrays to stay under 20-param limit."
  [db eavt-pss slice ground-filter strict-filter
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   merge-datoms n-find find-source const-vals
   result-list max-n n-merges temporal-ctx cancel]
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
        ;; Optional merge arrays (added for get-else on temporal DBs).
        ;; Nil-safe so callers built before this slot was added still work.
        ^objects merge-optional (when (> (alength ^objects temporal-ctx) 16)
                                  (aget ^objects temporal-ctx 16))
        ^objects merge-defaults (when (> (alength ^objects temporal-ctx) 17)
                                  (aget ^objects temporal-ctx 17))
        ^objects merge-datoms merge-datoms
        ^ints find-source find-source
        ^objects const-vals const-vals]
    #?(:clj
       (when-let [iter (some-> ^Iterable slice .iterator)]
         (while (and (.hasNext iter)
                     (or (neg? max-n) (< (result-list-size result-list) max-n)))
           (check-cancel! cancel)
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
                                         (let [matched? (volatile! false)]
                                           (doseq [^Datom d mslice]
                                             (when (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)
                                               (vreset! matched? true)
                                               (aset merge-datoms mi d)
                                               (process-merges (inc mi))))
                                           ;; Optional merge (get-else): emit the default-valued
                                           ;; datom when no version matched. :historical forces
                                           ;; every merge card-many, so a card-one get-else (e.g.
                                           ;; the valid-at :db.valid/to default) lands here.
                                           (when (and (not @matched?) merge-optional (aget merge-optional mi))
                                             (aset merge-datoms mi (datom eid ra (aget merge-defaults mi) tx0))
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
                                       (cond
                                         found?
                                         (do (aset merge-datoms mi d)
                                             (process-merges (inc mi)))
                                         ;; Optional merge (get-else): emit synthetic
                                         ;; default-valued datom on miss.
                                         (and merge-optional (aget merge-optional mi))
                                         (do (aset merge-datoms mi
                                                   (datom eid ra (aget merge-defaults mi) tx0))
                                             (process-merges (inc mi))))))
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
                                         (if-let [found-t (when (= temporal-type :as-of)
                                                            (visible-eavt-datom origin-db db temporal-type (:temporal-eavt origin-db)
                                                                                eid ra vg? vgv check-v? check-tx? scan-d
                                                                                temporal-tx-filter added-filter))]
                                           (if anti? nil
                                               (do (aset merge-datoms mi found-t)
                                                   (process-merges (inc mi))))
                                           (cond
                                             anti? (process-merges (inc mi))
                                             ;; Optional merge: emit default on miss
                                             (and merge-optional (aget merge-optional mi))
                                             (do (aset merge-datoms mi
                                                       (datom eid ra (aget merge-defaults mi) tx0))
                                                 (process-merges (inc mi)))))))
                                   ;; Historical: full temporal-merge-slice (needs all versions)
                                     (let [from-d (datom eid ra (when vg? vgv) tx0)
                                           to-d (datom eid ra (when vg? vgv) txmax)
                                           mslice (temporal-merge-slice origin-db from-d to-d temporal-type temporal-tx-filter db)
                                           check-v? (aget merge-check-scan-v mi)
                                           check-tx? (aget merge-check-scan-tx mi)]
                                       (if anti?
                                         (when (not-any? (fn [^Datom d] (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)) mslice)
                                           (process-merges (inc mi)))
                                         (if-let [^Datom d (some (fn [^Datom d]
                                                                   (when (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter) d))
                                                                 mslice)]
                                           (do (aset merge-datoms mi d)
                                               (process-merges (inc mi)))
                                           ;; Optional merge: emit default when no version matched
                                           (when (and merge-optional (aget merge-optional mi))
                                             (aset merge-datoms mi
                                                   (datom eid ra (aget merge-defaults mi) tx0))
                                             (process-merges (inc mi))))))))))))]
                   (process-merges 0)))))))
       :cljs
       (doseq [scan-d slice
               :while (or (neg? max-n) (< (result-list-size result-list) max-n))]
         (check-cancel! cancel)
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
                                 (let [matched? (volatile! false)]
                                   (doseq [^Datom d mslice]
                                     (when (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter)
                                       (vreset! matched? true)
                                       (aset merge-datoms mi d) (process-merges (inc mi))))
                                   ;; Optional merge (get-else): emit default when no version
                                   ;; matched (:historical forces card-many, so card-one
                                   ;; get-else like valid-at :db.valid/to lands here).
                                   (when (and (not @matched?) merge-optional (aget merge-optional mi))
                                     (aset merge-datoms mi (datom eid ra (aget merge-defaults mi) tx0))
                                     (process-merges (inc mi))))))
                             (let [probe (datom eid ra vgv tx0)
                                   ^Datom d (pss-lookup-ge eavt-pss probe)
                                   check-v? (aget merge-check-scan-v mi)
                                   check-tx? (aget merge-check-scan-tx mi)
                                   found-d (or (when (and d (temporal-merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d temporal-tx-filter added-filter))
                                                 d)
                                               (when (= temporal-type :as-of)
                                                 (visible-eavt-datom origin-db db temporal-type (:temporal-eavt origin-db)
                                                                     eid ra vg? vgv check-v? check-tx? scan-d
                                                                     temporal-tx-filter added-filter)))]
                               (if anti?
                                 (when (not found-d) (process-merges (inc mi)))
                                 (cond
                                   found-d
                                   (do (aset merge-datoms mi found-d) (process-merges (inc mi)))
                                   ;; Optional merge: emit synthetic default datom on miss
                                   (and merge-optional (aget merge-optional mi))
                                   (do (aset merge-datoms mi
                                             (datom eid ra (aget merge-defaults mi) tx0))
                                       (process-merges (inc mi))))))))))]
               (process-merges 0))))))))

;; ---------------------------------------------------------------------------
;; Dispatcher — setup + dispatch to the appropriate path function.

(defn- execute-group-direct
  "Execute an entity-group (or single pattern-scan) fused, writing results
   directly into a result list. Computes shared setup, then dispatches to
   a path-specific function (each small enough for JIT C2/Graal compilation).
   When temporal is non-nil, uses temporal-aware scan slicing and loop functions.
   `:cancel` (optional) is an IDeref/Volatile checked per iteration in the
   path function's inner loop — nil when caller doesn't need cancellation."
  [db scan-op merge-ops find-vars consts
   result-list
   probe-set probe-datom-field
   collect-set collect-datom-field collect-merge-idx
   max-results
   & {:keys [scan-estimate pipeline temporal cancel]}]
  (let [{:keys [clause index pushdown-preds]} scan-op
        [e a v tx] clause
        ;; For temporal queries, resolve against the unwrapped origin-db
        origin-db (when temporal (:origin-db temporal))
        index-db (or origin-db db)
        resolved-a (when (and (some? a) (not (symbol? a))) (resolve-attr index-db a))
        resolved-e (when (and (some? e) (not (symbol? e)) (number? e)) #?(:clj (long e) :cljs e))
        pushdown-bounds (when (seq pushdown-preds) (plan/pushdown-to-bounds pushdown-preds))
        [from-datom to-datom] (compute-slice-bounds clause index pushdown-bounds resolved-a resolved-e)
        db-index (get index-db index)
        ground-filter (build-ground-filter clause index)
        strict-filter (when-let [strict (:strict-preds pushdown-bounds)]
                        (let [scan-var-map (rel/var-mapping clause (range))
                              pushdown-var (first (keep :var pushdown-preds))]
                          (when pushdown-var
                            (build-strict-filter strict (get scan-var-map pushdown-var)))))

        ;; Temporal-specific setup
        temporal-type (when temporal (:type temporal))
        temporal-tx-filter (when temporal (build-temporal-tx-filter temporal))
        scan-added-val (when temporal
                         (let [added (get clause 4)]
                           (when (and (some? added) (not (symbol? added)) (boolean? added))
                             added)))

        eavt-pss (:eavt index-db)
        n-merges (count merge-ops)

        ;; Pre-extract merge info into arrays for fast inner loop
        ^objects merge-attrs (build-merge-attrs index-db merge-ops)
        [merge-v-ground* merge-v-vals* merge-anti*
         merge-check-scan-v* merge-check-scan-tx*] (build-common-merge-arrays merge-ops clause)
        ^objects merge-v-ground merge-v-ground*
        ^objects merge-v-vals merge-v-vals*
        ^objects merge-anti merge-anti*
        ^objects merge-check-scan-v merge-check-scan-v*
        ^objects merge-check-scan-tx merge-check-scan-tx*
        merge-card-many (to-array (mapv (fn [op]
                                          ;; History normally forces every merge card-many so all
                                          ;; versions surface. `get-else` (`:optional?`) is a
                                          ;; single-valued function, though — like the legacy
                                          ;; `-get-else` (`(first (search …))`) it yields exactly one
                                          ;; value (or the default) per entity, regardless of
                                          ;; attribute cardinality or temporal type — so it never
                                          ;; takes the card-many path.
                                          (and (not (:optional? op))
                                               (or (= temporal-type :historical)
                                                   (not (get-in op [:schema-info :card-one?] true)))))
                                        merge-ops))
        ;; Optional merge arrays (for get-else optimization)
        ^objects merge-optional (to-array (mapv #(boolean (:optional? %)) merge-ops))
        ^objects merge-defaults (to-array (mapv :default-value merge-ops))

        ;; Temporal-only merge arrays (nil when non-temporal)
        merge-added-filter (when temporal
                             (to-array (mapv (fn [op]
                                               (let [added (get (:clause op) 4)]
                                                 (when (and (some? added) (not (symbol? added)) (boolean? added))
                                                   added)))
                                             merge-ops)))
        temporal-eavt-pss (when (= temporal-type :historical) (:temporal-eavt index-db))
        merge-temporal-only (when temporal
                              (to-array (mapv (fn [op]
                                                (and (= temporal-type :historical)
                                                     (some? temporal-eavt-pss)
                                                     (get-in op [:schema-info :card-one?] true)
                                                     (not (dbu/no-history? index-db
                                                                           (let [ma (second (:clause op))]
                                                                             (when (and (some? ma) (not (symbol? ma)))
                                                                               (resolve-attr index-db ma)))))))
                                              merge-ops)))
        temporal-cursors
        #?(:clj (when (and temporal-eavt-pss merge-temporal-only (some true? (seq merge-temporal-only)))
                  (let [cursors (object-array n-merges)]
                    (dotimes [i n-merges]
                      (when (aget merge-temporal-only i)
                        (aset cursors i
                              (.forwardCursor ^PersistentSortedSet temporal-eavt-pss
                                              ^java.util.Comparator fast-cmp-ea))))
                    cursors))
           :cljs nil)
        merge-cursor-cache (when temporal
                             #?(:clj (let [cache (object-array n-merges)]
                                       (dotimes [i n-merges] (aset cache i (long-array 1 -1)))
                                       cache)
                                :cljs nil))

        ;; Non-temporal pipeline annotation
        _ (when-not temporal (assert pipeline "Plans must have :pipeline annotation"))
        use-cursors? (when pipeline (:use-cursors? pipeline))
        attr-refs? (when pipeline (:attr-refs? pipeline))
        fused-path (when pipeline (:fused-path pipeline))

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
                                         ;; Value-position seeks read the AVET index, which only
                                         ;; contains :db/index / :db/unique attributes. For a
                                         ;; non-indexed attr the AVET slice is empty, so gate on
                                         ;; indexing? (matching scan-datoms' :seekable?) — otherwise
                                         ;; fall through to the db-index scan + probe-set filter.
                                         (if (= probe-field 2)
                                           (and (some? (:avet index-db)) (dbu/indexing? index-db resolved-a))
                                           true) ;; entity position always has EAVT
                                         ;; For a TEMPORAL scan the non-probe path materializes the
                                         ;; whole current+temporal history of the attribute — far
                                         ;; costlier than per-value seeks — so prefer probe-driven
                                         ;; whenever the probe set is smaller than the attribute, not
                                         ;; just below the seek break-even for cheap current-DB slices.
                                         (let [est (long (or scan-estimate (di/-count db-index)))]
                                           (and (pos? est)
                                                (if temporal
                                                  (< (long (probe-set-size probe-set)) est)
                                                  (< (* (long (probe-set-size probe-set)) (long probe-driven-threshold))
                                                     est))))))
                             :cljs false)
        slice (if use-probe-driven?
                (if temporal
                  ;; Temporal probe-driven: build concatenated temporal slices per probe value
                  #?(:clj
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
                                                           (if (== pf 2) (:avet index-db) db-index)
                                                           from to
                                                           (if (== pf 2) :avet index)
                                                           temporal index-db resolved-a)]
                           (when sub-slice
                             (let [iter (.iterator ^Iterable sub-slice)]
                               (while (.hasNext iter)
                                 (.add result (.next iter)))))))
                       result)
                     :cljs nil)
                  ;; Non-temporal probe-driven. JVM-only fast path (ForwardCursor);
                  ;; never taken on cljs (use-probe-driven? is :cljs false above), so
                  ;; :cljs nil just keeps the dead branch compilable (no undeclared-var).
                  #?(:clj
                     (let [pf (int probe-datom-field)]
                       (probe-driven-iterable
                        (if (= pf 2) (:avet index-db) (:eavt index-db))
                        resolved-a probe-set pf))
                     :cljs nil))
                (if temporal
                  (build-scan-slice db db-index from-datom to-datom index
                                    temporal index-db resolved-a)
                  (di/-slice db-index from-datom to-datom index)))
        ;; When probe-driven, filtering is baked into the seeks — nil out probe-set
        probe-set (if use-probe-driven? nil probe-set)
        max-n (int (or max-results -1))]

    (if temporal
      ;; Temporal dispatch: scan-only or general temporal merge
      (if (zero? n-merges)
        (execute-temporal-scan-only slice ground-filter strict-filter
                                    probe-set probe-datom-field
                                    collect-set collect-datom-field collect-merge-idx
                                    merge-datoms n-find find-source const-vals
                                    result-list max-n temporal-tx-filter scan-added-val cancel)
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
                                               scan-added-val origin-db
                                               merge-optional merge-defaults])
                                cancel))

      ;; Non-temporal dispatch via fused-path keyword
      (case fused-path
        :scan-only
        (execute-scan-only slice ground-filter strict-filter
                           probe-set probe-datom-field
                           collect-set collect-datom-field collect-merge-idx
                           merge-datoms n-find find-source const-vals
                           result-list max-n cancel)

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
                                   result-list max-n n-merges merge-ctx cancel))

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
                                                  merge-check-scan-v merge-check-scan-tx])
                                   cancel))
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
                                                   merge-check-scan-v merge-check-scan-tx
                                                   merge-optional merge-defaults])
                                    cancel))))

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

(defn can-direct-fuse?
  "Check if a plan can use the direct-to-HashSet execution path.
   Public: query/explain mirrors the execution dispatch with it.
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
           ;; Output vars come from the binding form ONLY. (:vars op) minus
           ;; input args would also count vars merely MENTIONED in an arg —
           ;; e.g. the lexically-scoped ?p inside a nested subquery literal
           ;; [(q [:find ?p …] $) ?v] — wrongly satisfying find-var coverage
           ;; for a var this plan never binds.
           fn-output-vars (into #{} (mapcat (fn [op]
                                              (filter analyze/free-var?
                                                      (analyze/extract-vars (:binding op)))))
                                fn-ops)
           all-available-vars (into group-vars fn-output-vars)]
       (every? #(or (and consts (contains? consts %))
                    (contains? all-available-vars %))
               find-vars))
         ;; Multi-group: probe vars resolvable; find-vars from any group or consts
     (every? (fn [[gi {:keys [producer-idx probe-vars]}]]
               (let [consumer-g (nth groups gi)
                     producer-g (nth groups producer-idx)]
                 (and (some? (find-probe-info consumer-g producer-g probe-vars))
                      ;; Find-vars must be resolvable from consumer, producer, or consts
                      (every? #(or (and consts (contains? consts %))
                                   (group-provides-var? consumer-g %)
                                   (group-provides-var? producer-g %))
                              find-vars))))
             (:group-joins plan)))))

;; ---------------------------------------------------------------------------
;; Post-processing on fused result tuples
;;
;; When the plan has predicates or functions whose vars are all provided by
;; groups, we emit wide tuples (all group vars), post-process in-place on the
;; ArrayList, then project down to find-vars. This avoids the Relation engine.

;; CLJS can't use requiring-resolve due to circular deps with datahike.query.
;; Instead, datahike.query registers its built-ins here at load time.
#?(:cljs (defonce ^:private registered-built-ins (atom nil)))
#?(:cljs (defonce ^:private registered-clj-core-built-ins (atom nil)))

#?(:cljs
   (defn register-built-ins!
     "Called by datahike.query at load time to register built-in predicates/functions."
     [bi cci]
     (reset! registered-built-ins bi)
     (reset! registered-clj-core-built-ins cci)))

(defn- resolve-pred-fn
  "Resolve a predicate or function symbol to a callable.
   Handles built-ins, clj-core built-ins, regular vars, and Java interop methods."
  [f]
  (or (get #?(:clj @(requiring-resolve 'datahike.query/built-ins)
              :cljs @registered-built-ins) f)
      (get #?(:clj @(requiring-resolve 'datahike.query/clj-core-built-ins)
              :cljs @registered-clj-core-built-ins) f)
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

(defn- bind-by-fn-strict
  "Delegate a function clause to the legacy engine's bind-by-fn, raising when
   it returns nil.

   nil is bind-by-fn's retry-later signal: the legacy interpreter's main loop
   (tools/resolve-clauses) re-queues the clause and raises \"Cannot resolve
   any more clauses\" once no clause can make progress. The planner executes
   its plan in a single linear pass with no retry queue, so a nil here means
   the clause's input vars are unbound and can never become bound — the query
   is unresolvable. Continuing instead of raising has produced two distinct
   silent-wrong-results bugs: #814's `(or next-ctx ctx)` guard silently
   DROPPED the clause, and #815's removal of that guard (on the false premise
   that ordering guarantees runnability — order-plan-ops' fallback branches
   emit unrunnable ops by design) let nil WIPE the whole context. Raising the
   same error as the base engine keeps the two engines in agreement on
   unresolvable queries and turns any future ordering bug into a loud error
   instead of wrong data."
  [ctx clause]
  (or (#?(:clj legacy/bind-by-fn :cljs (rel/get-legacy-fn :bind-by-fn)) ctx clause)
      (log/raise "Cannot resolve any more clauses"
                 {:error :query/where :clauses [clause]})))

(defn- post-filter-preds
  "Filter result-list in-place by evaluating predicate ops.
   var-index maps var-sym → position in the wide tuple Object[].
   Removes tuples that don't satisfy all predicates.
   Exception handling matches the Relation engine (filter-by-pred):
   only ClassCastException and IllegalArgumentException are caught."
  [result-list pred-ops var-index]
  (let [resolved (resolve-pred-fns pred-ops)
        n (result-list-size result-list)]
    (loop [read-i (int 0)
           write-i (int 0)]
      (if (< read-i n)
        (let [^objects tuple (result-list-get result-list read-i)
              pass? (loop [pi 0]
                      (if (>= pi (count pred-ops))
                        true
                        (let [f (nth resolved pi)
                              args (:args (nth pred-ops pi))
                              argv (mapv (fn [a]
                                           (cond
                                             (and (symbol? a) (analyze/free-var? a))
                                             (aget tuple (int (get var-index a)))
                                             ;; (quote x) → constant x, matching -call-fn
                                             (analyze/quote-form? a) (second a)
                                             :else a))
                                         args)]
                          (if (try (apply f argv)
                                   #?(:clj (catch ClassCastException _ false))
                                   #?(:clj (catch IllegalArgumentException _ false)
                                      :cljs (catch :default _ false)))
                            (recur (inc pi))
                            false))))]
          (if pass?
            (do (when (not= read-i write-i)
                  (result-list-set result-list write-i tuple))
                (recur (unchecked-inc-int read-i) (unchecked-inc-int write-i)))
            (recur (unchecked-inc-int read-i) write-i)))
        ;; Trim removed entries
        (when (< write-i n)
          (result-list-trim result-list write-i))))))

(defn- post-filter-not-joins
  "Apply NOT-JOIN anti-probe filtering on result-list.
   For each NOT-JOIN op, executes its sub-plan to get exclusion values,
   builds a HashSet of join-var tuples, and removes matching result tuples."
  [result-list not-join-ops var-index db]
  (doseq [nj-op not-join-ops]
    (let [join-vars (:join-vars nj-op)
          sub-plan (:sub-plan nj-op)
          ;; Execute the NOT-JOIN's sub-plan via the Relation engine
          neg-ctx (execute-plan sub-plan {:rels [] :sources {}} db)
          neg-rel (when (and neg-ctx (seq (:rels neg-ctx)))
                    (reduce rel/hash-join (:rels neg-ctx)))]
      (when (and neg-rel (pos? (count (:tuples neg-rel))))
        ;; Build exclusion set from the negation result
        (let [neg-attrs (:attrs neg-rel)
              jv-vec (vec join-vars)
              n-jv (count jv-vec)
              neg-key (fn [tuple]
                        (if (= 1 n-jv)
                          (get tuple (get neg-attrs (first jv-vec)))
                          (mapv #(get tuple (get neg-attrs %)) jv-vec)))
              ;; Exclusion set. JVM keeps the fast java.util.HashSet (value equality
              ;; via .equals/.hashCode, incl. vector keys); cljs uses a Clojure set —
              ;; js/Set would key vectors by REFERENCE and never match (and HashSet is
              ;; undeclared there). Correct on both; cljs is the acceptable slower side.
              excl-set #?(:clj (let [^java.util.HashSet hs (java.util.HashSet.)]
                                 (doseq [tuple (:tuples neg-rel)] (.add hs (neg-key tuple)))
                                 hs)
                          :cljs (persistent!
                                 (reduce (fn [s tuple] (conj! s (neg-key tuple)))
                                         (transient #{}) (:tuples neg-rel))))]
          ;; Filter result-list: remove tuples whose join-var values are in the exclusion set
          (let [jv-indices (mapv #(int (get var-index %)) jv-vec)
                n (result-list-size result-list)]
            (loop [read-i (int 0) write-i (int 0)]
              (if (< read-i n)
                (let [^objects tuple (result-list-get result-list read-i)
                      probe (if (= 1 n-jv)
                              (aget tuple (int (first jv-indices)))
                              (mapv #(aget tuple (int %)) jv-indices))
                      excluded? #?(:clj (.contains ^java.util.HashSet excl-set probe)
                                   :cljs (contains? excl-set probe))]
                  (if excluded?
                    (recur (unchecked-inc-int read-i) write-i)
                    (do (when (not= read-i write-i)
                          (result-list-set result-list write-i tuple))
                        (recur (unchecked-inc-int read-i) (unchecked-inc-int write-i)))))
                (when (< write-i n)
                  (result-list-trim result-list write-i))))))))))

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
  [result-list fn-ops var-index]
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
             n (result-list-size result-list)]
         (loop [read-i (int 0)
                write-i (int 0)]
           (if (< read-i n)
             (let [^objects tuple (result-list-get result-list read-i)
                   argv (mapv (fn [a]
                                (cond
                                  (and (symbol? a) (analyze/free-var? a))
                                  (aget tuple (int (get vi a)))
                                  ;; (quote x) → constant x, matching -call-fn
                                  (analyze/quote-form? a) (second a)
                                  :else a))
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
                         ^objects new-tuple
                         (if (> new-len old-len)
                           (let [t #?(:clj (object-array new-len) :cljs (make-array new-len))]
                             #?(:clj  (System/arraycopy tuple 0 t 0 old-len)
                                :cljs (dotimes [j old-len] (aset t j (aget tuple j))))
                             t)
                           tuple)]
                     ;; Set only NEW bound var values (already-bound vars keep their value)
                     (doseq [bv bvars
                             :when (and (symbol? bv) (analyze/free-var? bv)
                                        (not (contains? vi bv)))]
                       (aset new-tuple (int (get new-vi bv)) val))
                     (result-list-set result-list write-i new-tuple)
                     (recur (unchecked-inc-int read-i) (unchecked-inc-int write-i)))
                   ;; Already-bound var mismatch — filter out tuple
                   (recur (unchecked-inc-int read-i) write-i))
                 ;; Function returned nil — skip tuple
                 (recur (unchecked-inc-int read-i) write-i)))
             ;; Trim removed entries
             (when (< write-i n)
               (result-list-trim result-list write-i))))
         new-vi))
     var-index (map vector fn-ops resolved))))

(defn- project-tuples
  "Project wide tuples down to find-vars only.
   Modifies result-list in-place, replacing each tuple with a narrower one."
  [result-list find-vars var-index consts]
  (let [n-find (count find-vars)
        ;; Build projection: for each find-var, its index in the wide tuple (or nil if const)
        proj (mapv (fn [fv] (if (and consts (contains? consts fv))
                              [:const (get consts fv)]
                              [:var (get var-index fv)]))
                   find-vars)
        n (result-list-size result-list)]
    (dotimes [i n]
      (let [^objects wide (result-list-get result-list i)
            ^objects narrow #?(:clj (object-array n-find) :cljs (make-array n-find))]
        (dotimes [fi n-find]
          (let [[kind v] (nth proj fi)]
            (aset narrow fi (if (= kind :const) v (aget wide (int v))))))
        (result-list-set result-list i narrow)))))

;; ---------------------------------------------------------------------------
;; Single-pass EAVT scan (prototype for max throughput)
;;
;; Instead of scan+seekGE merges, iterate ALL datoms in EAVT once.
;; Entity datoms are contiguous in EAVT. For each entity, collect values
;; for target attributes. If all required attributes present, emit tuple.
;; Zero comparator dispatch — just raw field access + long comparison.

(defn- apply-attached-preds
  "Apply a group's attached predicates in-place on the result-list.
   The group emitted wide tuples (all group-vars). This filters by the predicates,
   then projects down to target-vars."
  [result-list attached-preds group-vars target-vars consts]
  (let [var-index (into {} (map-indexed (fn [i v] [v i])) group-vars)]
    (post-filter-preds result-list attached-preds var-index)
    (when (not= group-vars target-vars)
      (project-tuples result-list target-vars var-index consts))))

(defn- group-emit-vars
  "Determine what vars a group should emit. If the group has attached-preds,
   emit all group vars (wide tuples) so predicates can reference any var.
   Otherwise emit the caller's target vars."
  [group target-vars]
  (if (seq (:attached-preds group))
    (vec (or (:output-vars group) (:vars group)))
    target-vars))

(defn execute-plan-direct
  "Execute a fully-fusable plan directly to a PersistentHashSet.
   Supports single-group and multi-group (value join via hash-probe) plans.
   max-results: when non-nil, stop after collecting this many results (for offset+limit).
   consts: map of var-sym → constant value for scalar :in bindings.
   cancel: optional IDeref/Volatile; when its value is truthy the query
     raises :datahike/canceled at the next check point.
   Returns the HashSet, or nil if the plan can't be executed directly."
  [plan db find-vars max-results consts cancel]
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
            not-join-ops (filterv #(= :not-join (:op %)) ops)
            has-post-ops? (or (seq pred-ops) (seq fn-ops) (seq not-join-ops))
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
                merge-ops (entity-group-merge-ops g)
                g-attached (:attached-preds g)
                g-emit (if (seq g-attached)
                         (group-emit-vars g emit-vars)
                         emit-vars)]
            (execute-group-direct db scan-op merge-ops g-emit consts
                                  result-list nil 0 nil 0 -1
                                  max-results
                                  :temporal temporal :pipeline (:pipeline g)
                                  :cancel cancel)
            (when (seq g-attached)
              (apply-attached-preds result-list g-attached
                                    (vec (or (:output-vars g) (:vars g)))
                                    emit-vars consts)))

          ;; Multi-group — hash-probe value join
          ;; Execute groups in order, build probe-sets between them.
          ;; When producer has find-vars, use probe-map to propagate values.
          ;;
          ;; Probe collections are keyed by [producer-idx probe-var] (not by
          ;; producer-idx alone): a single producer can have multiple
          ;; downstream consumers that probe on DIFFERENT variables, and
          ;; each needs its own probe-map. The pre-fix code keyed by
          ;; producer-idx, so the second consumer's lookup found the
          ;; first consumer's map and silently produced wrong joins
          ;; whenever the two probe-vars ranged over the same value
          ;; domain. See planner-bugs/cross-product-test.cljc Bug B.
          (let [producer-idxs (into #{} (map :producer-idx) (vals group-joins))
                extra-preds (into [] (comp (keep-indexed
                                            (fn [i g] (when (contains? producer-idxs i)
                                                        (:attached-preds g))))
                                           cat)
                                  groups)
                pred-ops (into pred-ops extra-preds)
                has-post-ops? (or (seq pred-ops) (seq fn-ops))
                all-group-vars (when has-post-ops?
                                 (vec (distinct (mapcat :vars groups))))
                emit-vars (if has-post-ops? all-group-vars find-vars)]
            (loop [gi 0
                   probe-sets    {} ;; {[producer-idx probe-var] → HashSet of join-var values}
                   probe-maps    {} ;; {[producer-idx probe-var] → {:map HashMap :p-all-vars [...]}}
                   ;; Consumer-group indices already materialized by the
                   ;; multi-consumer producer step. Each entry's own loop
                   ;; iteration becomes a no-op (the producer step
                   ;; produced the joined result-list inline).
                   consumed-cgi #{}]
              (if (>= gi n-groups)
                nil ;; done
                (let [g (nth groups gi)
                      join-info (get group-joins gi)
                      scan-op (entity-group-scan-op g)
                      merge-ops (entity-group-merge-ops g)]

                  (cond
                    ;; Multi-consumer producer's downstream — already handled
                    ;; inline at the producer step; nothing to do here.
                    (contains? consumed-cgi gi)
                    (recur (inc gi) probe-sets probe-maps consumed-cgi)

                    join-info
                    ;; Consumer group: use probe-set/map from producer keyed
                    ;; on this consumer's own probe-var.
                    (let [{:keys [producer-idx probe-vars]} join-info
                          producer-g (nth groups producer-idx)
                          pinfo (find-probe-info g producer-g probe-vars)
                          probe-var (:probe-var pinfo)
                          probe-key [producer-idx probe-var]
                          probe-set (get probe-sets probe-key)
                          pmap (get probe-maps probe-key)
                          c-attached (when-not (contains? producer-idxs gi)
                                       (:attached-preds g))
                          ;; Consumer emits all its vars (wide) so we can extract probe-var for map lookup
                          c-all-vars (vec (or (:output-vars g) (:vars g)))
                          c-emit (if (or pmap (seq c-attached) has-post-ops?)
                                   c-all-vars
                                   emit-vars)]
                      (when (and pinfo probe-set)
                        (execute-group-direct db scan-op merge-ops c-emit consts
                                              result-list probe-set
                                              (int (:consumer-scan-field pinfo))
                                              nil 0 -1
                                              max-results
                                              :temporal temporal
                                              :scan-estimate (:estimated-card g)
                                              :pipeline (:pipeline g)
                                              :cancel cancel)
                        ;; Apply consumer attached-preds (if any)
                        (when (seq c-attached)
                          (apply-attached-preds result-list c-attached c-all-vars c-all-vars consts))
                        ;; Combine consumer tuples with producer values from probe-map
                        (when pmap
                          (let [probe-var (:probe-var pinfo)
                                c-var-index (into {} (map-indexed (fn [i v] [v i])) c-all-vars)
                                c-probe-idx (int (get c-var-index probe-var))
                                the-map (:map pmap)
                                p-all-vars (:p-all-vars pmap)
                                target-vars (if has-post-ops?
                                              ;; Wide: must match all-group-vars layout for post-processing
                                              all-group-vars
                                              find-vars)
                                p-var-index (into {} (map-indexed (fn [i v] [v i])) p-all-vars)
                                n-target (count target-vars)
                                ;; Build combination plan: for each target var, [source index]
                                combo-plan (mapv (fn [tv]
                                                   (cond
                                                     (and consts (contains? consts tv)) [:const (get consts tv)]
                                                     (contains? c-var-index tv) [:consumer (get c-var-index tv)]
                                                     (contains? p-var-index tv) [:producer (get p-var-index tv)]
                                                     :else [:const nil]))
                                                 target-vars)
                                n-consumer (result-list-size result-list)
                                combined (make-result-list (* 2 n-consumer))]
                            (dotimes [ci n-consumer]
                              (let [^objects c-tuple (result-list-get result-list ci)
                                    probe-val (aget c-tuple c-probe-idx)
                                    p-entries (probe-map-get the-map probe-val)]
                                (when p-entries
                                  (dotimes [pi (probe-map-entry-size p-entries)]
                                    (let [^objects p-tuple (probe-map-entry-get p-entries pi)
                                          ^objects out #?(:clj (object-array n-target) :cljs (make-array n-target))]
                                      (dotimes [ti n-target]
                                        (let [[src idx] (nth combo-plan ti)]
                                          (case src
                                            :consumer (aset out ti (aget c-tuple (int idx)))
                                            :producer (aset out ti (aget p-tuple (int idx)))
                                            :const    (aset out ti idx))))
                                      (result-list-add combined out))))))
                            ;; Replace result-list contents with combined results
                            #?(:clj  (do (.clear ^java.util.ArrayList result-list)
                                         (.addAll ^java.util.ArrayList result-list ^java.util.ArrayList combined))
                               :cljs (do (.splice result-list 0)
                                         (dotimes [i (.-length combined)]
                                           (.push result-list (aget combined i))))))))
                      (recur (inc gi) probe-sets probe-maps consumed-cgi))

                    :else
                  ;; Producer group: collect join-var values for downstream consumers.
                  ;;
                  ;; A producer may have MULTIPLE downstream consumers probing
                  ;; on different vars (e.g. an edge entity-group with both src
                  ;; and tgt refs feeding two vertex consumers, one keyed on
                  ;; ?v1 = :src, the other on ?v2 = :tgt). Build one probe
                  ;; collection per unique consumer probe-var, all keyed by
                  ;; [producer-idx probe-var] in the threaded state.
                    (let [downstream-consumers
                          (vec
                           (keep (fn [[consumer-gi info]]
                                   (when (= (:producer-idx info) gi)
                                     (let [consumer-g (nth groups consumer-gi)
                                           pinfo (find-probe-info consumer-g g (:probe-vars info))]
                                       (when pinfo
                                         {:consumer-gi consumer-gi
                                          :probe-vars  (:probe-vars info)
                                          :pinfo       pinfo
                                          :consumer-g  consumer-g}))))
                                 group-joins))
                          ;; Deduplicate by the probe-var (a single producer/var
                          ;; combination only needs one probe collection even
                          ;; if multiple consumers share it).
                          unique-probes
                          (vec (vals
                                (reduce (fn [m d]
                                          (let [pv (get-in d [:pinfo :probe-var])]
                                            (cond-> m
                                              (not (contains? m pv)) (assoc pv d))))
                                        {}
                                        downstream-consumers)))
                          ;; producer-has-find-vars? if ANY downstream consumer
                          ;; is missing a find-var the producer supplies — we
                          ;; need to keep the producer's tuples to combine
                          ;; them in. If multiple probe-vars feed different
                          ;; consumers we also need the producer tuples
                          ;; retained to build each probe-map.
                          producer-has-find-vars?
                          (and (seq downstream-consumers)
                               (some (fn [{c-g :consumer-g}]
                                       (some (fn [fv]
                                               (and (not (and consts (contains? consts fv)))
                                                    (group-provides-var? g fv)
                                                    (not (group-provides-var? c-g fv))))
                                             find-vars))
                                     downstream-consumers))
                          use-new-path?
                          (or producer-has-find-vars?
                              (> (count unique-probes) 1))]

                      (cond
                        ;; MULTI-CONSUMER PATH: a single producer has two-or-more
                        ;; distinct probe-vars flowing to downstream consumers.
                        ;; We materialize the producer's wide tuples once,
                        ;; compute each consumer's accepted probe-value set, then
                        ;; filter the producer tuples by ALL constraints
                        ;; simultaneously. Marking the consumer-gis as consumed
                        ;; makes their main-loop iterations no-ops.
                        (> (count unique-probes) 1)
                        (let [p-all-vars (vec (or (:output-vars g) (:vars g)))
                              p-attached (:attached-preds g)
                              ;; 1. Producer wide tuples.
                              _ (execute-group-direct db scan-op merge-ops p-all-vars consts
                                                      result-list nil 0 nil 0 -1 nil
                                                      :temporal temporal :pipeline (:pipeline g)
                                                      :cancel cancel)
                              p-var-index (into {} (map-indexed (fn [i v] [v i])) p-all-vars)
                              _ (when (seq p-attached)
                                  (post-filter-preds result-list (vec p-attached) p-var-index))
                              n-producer (result-list-size result-list)
                              ;; 2. For each unique probe-var: run the consumer scan
                              ;; against the producer's probe-set to get accepted
                              ;; values. We do this by reusing the producer's
                              ;; probe-set as the filter for the consumer scan.
                              ;; The accepted set is the set of values the
                              ;; consumer's scan emits for the probe-var.
                              probe-info-by-var
                              (reduce
                               (fn [acc {:keys [pinfo consumer-g consumer-gi]}]
                                 (let [probe-var (:probe-var pinfo)
                                       probe-idx (int (get p-var-index probe-var))
                                       producer-set (let [s (make-probe-set 64)]
                                                      (dotimes [i n-producer]
                                                        (let [^objects t (result-list-get result-list i)]
                                                          (probe-set-add s (aget t probe-idx))))
                                                      s)
                                       c-scan-op (entity-group-scan-op consumer-g)
                                       c-merge-ops (entity-group-merge-ops consumer-g)
                                       c-attached (when-not (contains? producer-idxs consumer-gi)
                                                    (:attached-preds consumer-g))
                                       c-all-vars (vec (or (:output-vars consumer-g) (:vars consumer-g)))
                                       ;; Scratch list for consumer scan results.
                                       c-list (make-result-list 256)]
                                   (execute-group-direct db c-scan-op c-merge-ops c-all-vars consts
                                                         c-list producer-set
                                                         (int (:consumer-scan-field pinfo))
                                                         nil 0 -1 nil
                                                         :temporal temporal
                                                         :scan-estimate (:estimated-card consumer-g)
                                                         :pipeline (:pipeline consumer-g)
                                                         :cancel cancel)
                                   (when (seq c-attached)
                                     (apply-attached-preds c-list c-attached c-all-vars c-all-vars consts))
                                   ;; Extract the probe-var values that the
                                   ;; consumer scan accepted.
                                   (let [c-var-index (into {} (map-indexed (fn [i v] [v i])) c-all-vars)
                                         c-probe-idx (int (get c-var-index probe-var))
                                         accepted (make-probe-set (result-list-size c-list))]
                                     (dotimes [i (result-list-size c-list)]
                                       (let [^objects ct (result-list-get c-list i)]
                                         (probe-set-add accepted (aget ct c-probe-idx))))
                                     (assoc acc probe-var
                                            {:probe-idx probe-idx
                                             :accepted  accepted}))))
                               {}
                               unique-probes)
                              ;; 3. Filter producer tuples: keep only those whose
                              ;; values at each probe-var are in the corresponding
                              ;; accepted set. Then project to emit-vars.
                              filtered (make-result-list n-producer)
                              probe-checks (vec (vals probe-info-by-var))
                              target-vars  (if has-post-ops? all-group-vars find-vars)
                              n-target     (count target-vars)
                              combo-plan   (mapv (fn [tv]
                                                   (cond
                                                     (and consts (contains? consts tv)) [:const (get consts tv)]
                                                     (contains? p-var-index tv) [:producer (get p-var-index tv)]
                                                     :else [:const nil]))
                                                 target-vars)
                              _ (dotimes [i n-producer]
                                  (let [^objects t (result-list-get result-list i)
                                        ok? (every? (fn [{:keys [probe-idx accepted]}]
                                                      (probe-set-contains? accepted (aget t (int probe-idx))))
                                                    probe-checks)]
                                    (when ok?
                                      (let [^objects out #?(:clj (object-array n-target) :cljs (make-array n-target))]
                                        (dotimes [ti n-target]
                                          (let [[src idx] (nth combo-plan ti)]
                                            (case src
                                              :producer (aset out ti (aget t (int idx)))
                                              :const    (aset out ti idx))))
                                        (result-list-add filtered out)))))]
                          ;; Replace result-list with the filtered+projected tuples.
                          #?(:clj  (do (.clear ^java.util.ArrayList result-list)
                                       (.addAll ^java.util.ArrayList result-list ^java.util.ArrayList filtered))
                             :cljs (do (.splice result-list 0)
                                       (dotimes [i (.-length filtered)]
                                         (.push result-list (aget filtered i)))))
                          ;; Mark all downstream consumers as consumed so the main
                          ;; loop skips them.
                          (recur (inc gi)
                                 probe-sets probe-maps
                                 (into consumed-cgi (map :consumer-gi) downstream-consumers)))

                        ;; SINGLE-CONSUMER NEW PATH: producer has find-vars not
                        ;; in the (single) consumer — keep producer tuples by
                        ;; building a probe-map for value propagation.
                        producer-has-find-vars?
                        (let [p-all-vars (vec (or (:output-vars g) (:vars g)))
                              p-attached (:attached-preds g)]
                          (execute-group-direct db scan-op merge-ops p-all-vars consts
                                                result-list nil 0 nil 0 -1 nil
                                                :temporal temporal :pipeline (:pipeline g)
                                                :cancel cancel)
                          (when (seq p-attached)
                            (let [var-idx (into {} (map-indexed (fn [i v] [v i])) p-all-vars)]
                              (post-filter-preds result-list (vec p-attached) var-idx)))
                          (let [p-var-index (into {} (map-indexed (fn [i v] [v i])) p-all-vars)
                                {:keys [pinfo]} (first unique-probes)
                                probe-var (:probe-var pinfo)
                                probe-idx (int (get p-var-index probe-var))
                                n-producer (result-list-size result-list)
                                pmap (make-probe-map n-producer)]
                            (dotimes [i n-producer]
                              (let [^objects tuple (result-list-get result-list i)
                                    probe-val (aget tuple probe-idx)]
                                (probe-map-add pmap probe-val tuple)))
                            #?(:clj  (.clear ^java.util.ArrayList result-list)
                               :cljs (.splice result-list 0))
                            (recur (inc gi)
                                   (assoc probe-sets [gi probe-var] (probe-map->set pmap))
                                   (assoc probe-maps [gi probe-var]
                                          {:map pmap :p-all-vars p-all-vars})
                                   consumed-cgi)))

                        ;; SINGLE-CONSUMER EXISTING PATH: collect-only scan;
                        ;; producer's vars not needed in output.
                        :else
                        (let [{:keys [pinfo] :as one} (first downstream-consumers)
                              probe-var (:probe-var pinfo)
                              collect-set (when one (make-probe-set 4000))]
                          (execute-group-direct db scan-op merge-ops [] consts
                                                result-list nil 0
                                                collect-set
                                                (int (or (:producer-datom-field pinfo) 0))
                                                (int (or (:producer-merge-idx pinfo) -1))
                                                nil
                                                :temporal temporal :pipeline (:pipeline g)
                                                :cancel cancel)
                          (recur (inc gi)
                                 (if collect-set
                                   (assoc probe-sets [gi probe-var] collect-set)
                                   probe-sets)
                                 probe-maps
                                 consumed-cgi))))))))
            ;; Post-processing for multi-group: use potentially augmented pred-ops
            (when has-post-ops?
              (let [var-index (into {} (map-indexed (fn [i v] [v i])) all-group-vars)]
                (when (seq pred-ops)
                  (post-filter-preds result-list pred-ops var-index))
                (let [var-index (if (seq fn-ops)
                                  (post-apply-fns result-list fn-ops var-index)
                                  var-index)]
                  (project-tuples result-list find-vars var-index consts))))))
        ;; Post-processing: apply predicates, functions, NOT-JOINs, then project
        ;; (single-group standalone post-ops only — multi-group handled above)
        (when (and (= 1 n-groups) has-post-ops?)
          (let [var-index (into {} (map-indexed (fn [i v] [v i])) all-group-vars)]
            (when (seq pred-ops)
              (post-filter-preds result-list pred-ops var-index))
            (when (seq not-join-ops)
              (post-filter-not-joins result-list not-join-ops var-index db))
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
   cancel: optional IDeref; see execute-plan-direct.
   Returns nil if the plan can't be fused."
  [plan db cancel]
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
              (execute-group-direct db scan-op merge-ops all-vars nil
                                    result-list nil 0 nil 0 -1 nil
                                    :temporal ti :pipeline (:pipeline g)
                                    :cancel cancel))
            ;; Apply attached predicates (group-level filters from Step 4c)
            (when-let [attached (seq (:attached-preds g))]
              (let [var-index (into {} (map-indexed (fn [i v] [v i])) all-vars)]
                (post-filter-preds result-list (vec attached) var-index)))
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
  (let [cancel (:cancel context)
        {:keys [clause index pushdown-preds]} scan-op
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
      (let [;; SIP retrieval: drive the scan from the currently-bound context
            ;; relations (index seek / semi-join filter / full scan) via the
            ;; shared `scan-datoms` seam. The merge below runs once per emitted
            ;; scan datom, so restricting the scan to the bound entities/values
            ;; is what keeps an :in-bound join O(bound) rather than O(attribute).
            filtered-datoms (cond->> (scan-datoms db clause index pushdown-preds
                                                  (:rels context) (:estimated-card scan-op))
                              ground-filter (filter ground-filter)
                              strict-filter (filter strict-filter))

            ;; `get-else` (`:optional?`) is single-valued (see execute-group-direct)
            ;; — never card-many, regardless of the attribute's cardinality.
            merge-card-many (to-array (mapv (fn [op]
                                              (and (not (:optional? op))
                                                   (not (get-in op [:schema-info :card-one?] true))))
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
                                    found? (and d (merge-datom-match? d eid ra vg? vgv check-v? check-tx? scan-d))
                                    merge-op (nth merge-ops mi)
                                    optional? (:optional? merge-op)]
                                (cond
                                  anti?
                                  (when (not found?)
                                    (process-merges scan-d eid (inc mi) tuple))

                                  found?
                                  (process-merges scan-d eid (inc mi)
                                                  (cond-> tuple
                                                    has-v-var? (conj (.-v d))
                                                    has-tx-var? (conj (.-tx d))))

                                  ;; Optional merge (get-else): produce default value on miss
                                  optional?
                                  (process-merges scan-d eid (inc mi)
                                                  (cond-> tuple
                                                    has-v-var? (conj (:default-value merge-op))
                                                    has-tx-var? (conj 0)))))))))]
                (run! (fn [^Datom scan-d]
                        (check-cancel! cancel)
                        (when (or (nil? entity-filter)
                                  #?(:clj (es/entity-bitset-contains? entity-filter (.-e scan-d))
                                     :cljs true))
                          (process-merges scan-d (.-e scan-d) (int 0)
                                          [(.-e scan-d) (.-a scan-d) (.-v scan-d) (.-tx scan-d) (datom/datom-added scan-d)])))
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

(defn- execute-temporal-group-rel
  "Temporal entity-group / pattern-scan kept on the FUSED fast path. Runs the
   temporal-aware `execute-group-direct` (current+temporal merge, inline tx /
   added / retraction resolution, per-value temporal probe seeks) driven by a
   SIP probe-set from the bound context rels, returning [ctx' consumed] like
   `execute-fused-scan-rel`. Avoids the legacy per-clause `lookup-batch-search`
   fallback that materializes the whole intermediate join then runs one
   temporal-search over it (the cross-product blowup). `temporal` is a non-nil
   `(temporal-info db)`; its :origin-db carries the PersistentSortedSet indexes.

   Cross-platform: its callees (execute-group-direct, the temporal merge path) are
   cljc; only the result-list drain differs (JVM ArrayList .toArray vs the cljs
   #js [] dual, as elsewhere in this ns)."
  [db op context temporal]
  (let [scan-op     (entity-group-scan-op op)
        merge-ops   (entity-group-merge-ops op)
        scan-clause (:clause scan-op)
        index-db    (or (:origin-db temporal) db)
        a           (second scan-clause)
        resolved-a  (when (and (some? a) (not (symbol? a))) (resolve-attr index-db a))
        scan-n      (or (:estimated-card scan-op)
                        (di/-count (get index-db (:index scan-op))))
        ;; SIP probe-set is a JVM-only fast-path optimization (HashSet + ForwardCursor
        ;; seeks). On cljs fall back to a full temporal scan — slower but identical
        ;; results, since the probe only restricts the scan to already-bound values.
        probe       #?(:clj  (pattern-probe-set index-db scan-clause resolved-a (:rels context) scan-n)
                       :cljs nil)
        ;; Project to ALL free vars across the scan + merge clauses (mirroring
        ;; execute-fused-scan-rel's out-attrs), NOT (:vars op). (:vars op) is the
        ;; group's declared interface and can omit the entity var (e.g. ?c when it
        ;; is not in :find), but downstream ops — get-else / predicates / further
        ;; joins — reference those clause vars, so the fused tuples must carry them.
        find-vars   (vec (distinct (filter #(and (symbol? %) (analyze/free-var? %))
                                           (concat scan-clause
                                                   (mapcat :clause merge-ops)))))
        result-list (make-result-list 4000)]
    (execute-group-direct db scan-op merge-ops find-vars nil
                          result-list
                          (when probe (:values probe)) (if probe (int (:field probe)) (int 0))
                          nil 0 -1 nil
                          :temporal temporal :pipeline (:pipeline op)
                          :cancel (:cancel context))
    (let [attrs   (into {} (map-indexed (fn [i v] [v i]) find-vars))
          out-rel (rel/->Relation attrs #?(:clj  (vec (.toArray ^java.util.ArrayList result-list))
                                           :cljs (vec result-list)))
          merged  (binding [rel/*implicit-source* db
                            rel/*lookup-attrs* (lookup-attrs-for-clauses db scan-clause merge-ops)]
                    (rel/collapse-rels (:rels context) out-rel))]
      [(-> context (assoc :rels merged) (assoc :unique-results? true))
       (inc (count merge-ops))])))

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
  ;; Magic-set optimization is JVM-only — the magic-base-scan / demand-set machinery
  ;; (java.util.HashSet, ArrayList) has :cljs nil bodies, so activating it on cljs
  ;; collapses recursive rules to an EMPTY rel (silently incomplete results). Return
  ;; nil on cljs so they take the correct (slower) full base-branch fixpoint instead.
  #?(:cljs nil
     :clj
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
              :propagation-pos prop-pos}))))))

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
       ;; Resolve AVET through any temporal wrapper (see magic-base-scan): on a
       ;; temporal DB the :avet lives on the origin-db and the slice needs the
       ;; current+temporal merge + time-point post-process via build-scan-slice.
       (let [ti (temporal-info db)
             index-db (if ti (:origin-db ti) db)
             avet-pss (:avet index-db)
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
                 slice (build-scan-slice db avet-pss from-datom to-datom :avet ti index-db attr)]
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
       ;; Resolve the EAVT index through any temporal wrapper: AsOfDB/SinceDB/
       ;; HistoricalDB carry no :eavt of their own (it lives on the origin-db),
       ;; and the slice must merge current+temporal history and post-process by
       ;; the time-point. build-scan-slice is the one seam that does this; it is a
       ;; plain passthrough slice for a regular DB (ti = nil).
       (let [ti (temporal-info db)
             index-db (if ti (:origin-db ti) db)
             eavt-pss (:eavt index-db)
             result (java.util.ArrayList.)
             iter (.iterator ^java.util.HashSet demand-set)]
         ;; Point-lookup each NEW entity's edges (skip already-scanned)
         (while (.hasNext iter)
           (let [e (.next iter)]
             (when (or (nil? scanned-set)
                       (.add ^java.util.HashSet scanned-set e))
               (let [from-datom (datom (if (number? e) (long e) e) attr nil tx0)
                     to-datom (datom (if (number? e) (long e) e) attr nil txmax)
                     slice (build-scan-slice db eavt-pss from-datom to-datom :eavt ti index-db attr)]
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

(defn- magic-base-scan-general
  "Demand-restricted base evaluation for the magic-set fixpoint, correct for ANY
   base shape — value-join (`[?a :id ?x][?c :id ?x]`), multi-branch, or non-ref
   edge — unlike the single-ref-edge `magic-base-scan` point-lookup (which feeds a
   demand entity into the EAVT entity slot and maps [entity value] onto the head
   vars, breaking whenever the propagated head var is not the scanned attr's value).

   Computes the NEW demand entities (those not yet in `scanned-set`), marks them
   scanned, then runs ALL base branch plans with the ground head var constrained to
   that batch via `inject-magic-relation`. Range-restricted datalog guarantees every
   base branch binds both head vars, so the injected demand relation always shares
   the ground var and hash-joins (never a Cartesian product) — the same mechanism
   the loop already uses to demand-restrict the recursive clause versions (aug-ctx).
   Keeps the base O(demand). Returns a Relation or nil when there is nothing new."
  [db base-plans head-vars ground-pos ^java.util.HashSet demand-set ^java.util.HashSet scanned-set ctx]
  #?(:clj
     (let [new-batch (java.util.ArrayList.)
           it (.iterator demand-set)]
       ;; Select the new-entity batch BEFORE marking, so an entity that appears in
       ;; two branches (or twice in one) is still scanned exactly once and never
       ;; marked-before-emitted.
       (while (.hasNext it)
         (let [e (.next it)]
           (when (and scanned-set (not (.contains scanned-set e)))
             (.add new-batch e))))
       (when (pos? (.size new-batch))
         (doseq [e new-batch] (.add scanned-set e))
         (let [demand' (java.util.HashSet. new-batch)
               ctx' (inject-magic-relation ctx head-vars ground-pos demand')]
           (execute-branch-plans db base-plans ctx' head-vars))))
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
   3. Terminate when no new tuples produced across any rule (fixpoint).
   4. Post-filter: apply constant call-args to restrict final result.

   Magic set optimization: when the called rule has ground arguments, a demand
   set is maintained to restrict DB scans to only reachable entities. This avoids
   computing the full transitive closure when only a subset is needed.

   For mutual recursion (multi-rule SCC), all rules share the fixpoint loop.
   Each rule has its own accumulator."
  [db op ctx]
  (let [{:keys [scc-rule-plans scc-rule-names call-args head-vars rule-name
                base-scan-attr]} op]
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
                               base-rel (cond
                                  ;; Magic, single ref-edge base: EAVT point lookups for demand entities
                                          (and magic-demand base-scan-attr (= rn rule-name))
                                          (or (magic-base-scan db head-vars base-scan-attr magic-demand magic-scanned)
                                              (rel/->Relation (zipmap head-vars (range)) []))
                                  ;; Magic, any other base shape: demand-restricted base branches
                                          (and magic-demand (= rn rule-name))
                                          (or (magic-base-scan-general db base-plans head-vars magic-ground-pos
                                                                       magic-demand magic-scanned ctx)
                                              (rel/->Relation (zipmap head-vars (range)) []))
                                  ;; No magic: full base branch plan execution
                                          :else
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
                (if (not any-delta?)
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
                                     (let [{:keys [head-vars rec-clause-versions base-plans]} (get scc-rule-plans rn)
                                  ;; With magic: scan newly demanded entities AND run
                                  ;; recursive branches (constrained by demand relation).
                                           magic-base-rel
                                           (cond
                                  ;; Single ref-edge fast path
                                             (and use-magic? base-scan-attr (= rn rule-name))
                                             (magic-base-scan db head-vars base-scan-attr
                                                              magic-demand magic-scanned)
                                  ;; General demand-restricted base for newly demanded entities
                                             (and use-magic? (= rn rule-name))
                                             (magic-base-scan-general db base-plans head-vars magic-ground-pos
                                                                      magic-demand magic-scanned ctx)
                                  ;; Magic abandoned by the explosion guard (use-magic? now false)
                                  ;; on the general path: the recursive step below runs
                                  ;; unrestricted, so backfill the FULL base once per round
                                  ;; (dedup makes it idempotent) so reflexive / leaf base
                                  ;; facts exist for every entity the closure can reach.
                                             (and (not use-magic?) magic-demand (not base-scan-attr) (= rn rule-name))
                                             (execute-branch-plans db base-plans ctx head-vars))
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
                                  ;; delta-driven-expand emits `[entity-id,
                                  ;; propagated-y]` per AVET hit. That's correct
                                  ;; only for simple transitive closure shapes
                                  ;; — `(rule ?x ?y) [?x :attr ?prev-y]
                                  ;; (rule ?prev-y ?z)` and the like, where the
                                  ;; recursive body is exhausted by the
                                  ;; reverse-index step. Any `:function` (e.g.
                                  ;; arithmetic on a counter, get-else
                                  ;; column-extraction), `:predicate`
                                  ;; (filtering on a get-else output), or
                                  ;; `:attached-preds` on the entity-group is
                                  ;; SILENTLY SKIPPED by the shortcut, producing
                                  ;; wrong tuples (eg `[entity-id, 0]` instead
                                  ;; of `[node-id, depth+1]` for a tree-walk
                                  ;; CTE). Only fire the optimization when every
                                  ;; recursive-clause version is reducible to
                                  ;; `:rule-lookup` + one `:entity-group` /
                                  ;; `:pattern-scan` with no attached predicates.
                                           rec-shape-simple?
                                           (every? (fn [cv-plan]
                                                     (every? (fn [op]
                                                               (case (:op op)
                                                                 (:rule-lookup :pattern-scan)
                                                                 true
                                                                 :entity-group
                                                                 (empty? (:attached-preds op))
                                                                 false))
                                                             (:ops cv-plan)))
                                                   rec-clause-versions)
                                           ;; Delta-driven expansion is JVM-only
                                           ;; (delta-driven-expand has a :cljs nil
                                           ;; body); false on cljs → the correct
                                           ;; non-delta recursive scan below.
                                           use-delta-driven? (and #?(:cljs false :clj base-scan-attr)
                                                                  rec-has-db-pattern?
                                                                  rec-shape-simple?
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
                             ;; Call the function which should return results.
                             ;; `search-with-vt` reads the db's `:datahike/valid-at`
                             ;; marker (set by `d/valid-at`) and routes through
                             ;; `-search-at-vt` for vt-aware indices.
                             (sec/search-with-vt db idx
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
           (bind-by-fn-strict ctx (:clause op)))

         ;; Retrieval: EntityBitSet + extra columns (score, distance)
         :retrieval
         (if idx
           (let [query-args (vec (drop 1 args))
                 results (sec/slice-ordered-with-vt db idx
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
           (bind-by-fn-strict ctx (:clause op)))

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

(defn- ctx-var-cards
  "{var → distinct-value count} over the materialized relations in `ctx`. Seeds
   the probe-triggered re-order so a function's input-row estimate reflects the
   ACTUAL bound cardinalities (not the card-1 default), and so every already-
   bound var is in the re-order's runnability seed."
  [ctx]
  (persistent!
   (reduce (fn [m rel]
             (let [tuples (:tuples rel)]
               (reduce-kv (fn [m v idx]
                            (assoc! m v (count (into #{} (map #(nth % idx)) tuples))))
                          m (:attrs rel))))
           (transient {}) (:rels ctx))))

(defn- probe-card
  "MEASURE the relation cardinality running `op` would produce against the
   already-materialized `ctx` — exact, not estimated. For an index-backed
   pattern-scan this is a single SIP read (restricted to the bound probe-set),
   so a row-reducing join returns its small true size and a row-expanding join
   its true blow-up. Pure read over immutable indexes; the result is discarded.
   Returns nil for op types we don't probe."
  [op-db op ctx]
  (case (:op op)
    :pattern-scan (count (:tuples (execute-pattern-scan op-db op (:cancel ctx) (:rels ctx))))
    :entity-group (ctx-total-tuples (first (execute-fused-scan-rel op-db (:scan-op op) (:merge-ops op) ctx)))
    nil))

(defn- probe-candidate?
  "Can `op` be run early to (potentially) change a function's input? It must be
   an index-backed producer sharing a free var with `in-vars`, on the default
   source, whose own required vars are already bound."
  [op in-vars bound]
  (and (#{:pattern-scan :entity-group} (:op op))
       (not (:source op))
       (some in-vars (:vars op))
       (let [[req pol] (plan/op-required-vars op)]
         (case pol
           :none true
           :all  (every? bound req)
           :any  (boolean (some bound req))
           true))))

(defn- ctx-rows-over
  "Rows the function would iterate over `in-vars` = the size of the production it
   runs against = the product of the (disjoint) ctx relations that hold any of
   those vars. Using the product (not the first/any rel) is essential: a graph
   arg like ?g sits in its own 1-tuple rel, so picking that rel alone would make
   every reducing join look like an expander and wrongly hoist the function."
  [ctx in-vars]
  (max 1 (long (->> (:rels ctx)
                    (filter (fn [rel] (some in-vars (keys (:attrs rel)))))
                    (map (fn [rel] (count (:tuples rel))))
                    (reduce * 1)))))

(defn- hoist-expensive-fn
  "The runtime HALF of the single cost model: static `op-cost` correctly defers an
   expensive function behind row-reducing joins and cheaper functions, but it also
   over-defers it behind an input-EXPANDING join (which static stats can't tell
   from a reducing one). Before running group `op` at `idx`, if a pending
   downstream expensive function is ready and `op` would EXPAND its input
   (measured rows > the function's current input rows), pull that function ahead
   of `op` so it runs on the smaller pre-expansion relation. Row-reducing groups
   are left ahead of the function — the beneficial order. Returns a reordered plan
   or nil. Gated on :exec-cost-fn + a direct shared input var, so MUTUAL-style
   deferral behind a cheaper function's OUTPUT filter is untouched."
  [plan idx ctx db op]
  (when (and (#{:pattern-scan :entity-group} (:op op)) (pss-instance? (:eavt db)))
    (let [ops (:ops plan)
          rest-ops (subvec ops idx)
          bound (into #{} (mapcat (comp keys :attrs)) (:rels ctx))
          ready-expensive-fn?
          (fn [o] (and (= :function (:op o)) (:exec-cost-fn o) (not (:probed? o)) (nil? (:source o))
                       (let [[req pol] (plan/op-required-vars o)]
                         (case pol :none true :all (every? bound req)
                               :any (boolean (some bound req)) true))))
          cand-fns (filter (fn [o]
                             (and (ready-expensive-fn? o)
                                  (some (set (plan/args-free-vars (:args o))) (:vars op))))
                           (rest rest-ops))]
      (when (seq cand-fns)
        (when-let [op-rows (probe-card db op ctx)]
          (let [hoistable (filter (fn [f]
                                    (> (long op-rows)
                                       (long (ctx-rows-over ctx (set (plan/args-free-vars (:args f)))))))
                                  cand-fns)]
            (when (seq hoistable)
              (let [vc (ctx-var-cards ctx)
                    f (apply min-key #(plan/op-cost % bound vc) hoistable)
                    rest' (into [(assoc f :probed? true)]
                                (filterv #(not (identical? % f)) rest-ops))]
                (assoc plan :ops (into (subvec ops 0 idx) rest'))))))))))

(defn execute-plan
  "Execute a query plan with adaptive replanning.
   After each op, compares actual cardinality to estimate. If the ratio
   exceeds replan-threshold (10x), re-orders remaining ops.

   Expensive functions (those carrying :datahike/cost via :exec-cost-fn) are
   additionally PROBE-SUNK: before running one, the deferred index-backed joins
   on its input are measured against the live relation; a join that REDUCES the
   input is re-ordered ahead of the function (so it runs on fewer rows), while
   one that EXPANDS it stays behind. This resolves the row-reducing-vs-expanding
   join ambiguity that no static cost model can (correlation/skew is invisible
   to per-attribute stats) — see plan/op-required-vars and the cost oracle.
   Takes and returns a query context."
  [plan context db]
  (if (:has-passthrough? plan)
    nil
    (let [replan-fn plan/replan
          ;; Re-promote attached-preds to standalone ops for the Relation engine.
          ;; Each group is followed by its attached predicate ops so var deps are met.
          plan (let [ops (:ops plan)
                     has-attached? (some #(seq (:attached-preds %)) ops)]
                 (if has-attached?
                   (assoc plan :ops
                          (into [] (mapcat (fn [op]
                                             (if (seq (:attached-preds op))
                                               (cons (dissoc op :attached-preds)
                                                     (:attached-preds op))
                                               [op])))
                                ops))
                   plan))]
      (loop [ctx context
             plan plan
             idx 0]
        (if (>= idx (count (:ops plan)))
          ctx
          (let [_ (check-cancel! (:cancel ctx))
                op (nth (:ops plan) idx)
                estimated-card (:estimated-card op)
                ;; Resolve the db for this op — may differ from default $ for multi-source queries
                op-db (if-let [src (:source op)]
                        (or (get (:sources ctx) src)
                            (do (log/warn :datahike/source-not-found "Source not found in query context, using default db"
                                          {:source src :available (set (keys (:sources ctx)))})
                                db))
                        db)]
            ;; Runtime corrector for the one thing static cost can't know: pull an
            ;; expensive function ahead of a group that would EXPAND its input.
            (if-let [plan' (hoist-expensive-fn plan idx ctx op-db op)]
              (recur ctx plan' idx)
              (case (:op op)
              ;; Entity group — fused scan+merges (only works with concrete DB sources
              ;; that have PersistentSortedSet indexes — temporal DBs like SinceDB/AsOfDB
              ;; filter via context, not index structure, so fused scan would bypass filters)
                :entity-group
                (let [ti (temporal-info op-db)]
                  (cond
                ;; Single temporal wrapper over a PSS-backed origin — stay on the
                ;; FUSED path (temporal-aware execute-group-direct + SIP probe)
                ;; rather than the per-clause legacy lookup-batch-search fallback.
                    (and (some? ti) (pss-instance? (:eavt (:origin-db ti))))
                    (let [[ctx' _] (execute-temporal-group-rel op-db op ctx ti)
                          actual-card (ctx-total-tuples ctx')
                      ;; Re-plan cardinality estimation reads index subtree counts
                      ;; (estimate-pattern → -has-subtree-counts?), which live on the
                      ;; origin's PSS indexes — the temporal wrapper has none. Estimate
                      ;; against the origin (filtering only changes counts, not the
                      ;; relative ordering the re-plan decides); execution still uses op-db.
                          plan' (if (and (> (- (count (:ops plan)) idx) 2)
                                         (should-replan? actual-card estimated-card))
                                  (replan-fn plan idx actual-card (:origin-db ti))
                                  plan)]
                      (recur ctx' plan' (inc idx)))

                    (pss-instance? (:eavt op-db))
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
                ;;
                ;; Optional merges (LOptionalScan from get-else) require left-outer
                ;; semantics with a default-on-miss. lookup-batch-search is inner-join,
                ;; so we route optional merges through bind-by-fn (which evaluates
                ;; -get-else per row, mirroring the legacy engine).
                    :else
                    (let [scan-clause (:clause (:scan-op op))
                          scan-evar (first scan-clause)
                          all-merge-ops (:merge-ops op)
                          regular-merge-clauses (mapv :clause
                                                      (filter (fn [m] (not (or (:anti? m) (:optional? m))))
                                                              all-merge-ops))
                          optional-merge-ops (filterv :optional? all-merge-ops)
                          anti-clauses (mapv :clause (filter :anti? all-merge-ops))
                      ;; Run scan with full ctx so upstream constants/constraints
                      ;; (e.g. const-bound safe-vars, lookup-refs in v) feed into
                      ;; the scan's substitution.
                          ctx-after-scan (binding [rel/*implicit-source* op-db]
                                           (#?(:clj legacy/lookup-batch-search :cljs (rel/get-legacy-fn :lookup-batch-search))
                                            op-db ctx scan-clause scan-clause))
                      ;; For each merge clause, trim ctx to rels connected to the
                      ;; scan entity-var. Independent upstream rels (no shared var
                      ;; with scan-derived rels) would otherwise cause
                      ;; lookup-batch-search to substitute the cartesian of all
                      ;; bound rels — a blowup when upstream and scan-rel are
                      ;; both large but disjoint. After lookup we re-add the
                      ;; aside rels; collapse-rels hash-joins on shared vars
                      ;; (e.g. the merge-introduced v-var) — semantically
                      ;; equivalent to cartesian-then-filter, but avoids the
                      ;; quadratic substitution.
                          contains-scan-evar? (fn [r] (contains? (set (keys (:attrs r))) scan-evar))
                          ctx' (binding [rel/*implicit-source* op-db]
                                 (reduce (fn [c clause]
                                           (let [{scan-rels true other-rels false}
                                                 (group-by contains-scan-evar? (:rels c))
                                                 trimmed-ctx (assoc c :rels (vec scan-rels))
                                                 after-merge (#?(:clj legacy/lookup-batch-search :cljs (rel/get-legacy-fn :lookup-batch-search))
                                                              op-db trimmed-ctx clause clause)
                                                 final-rels (reduce rel/collapse-rels
                                                                    (:rels after-merge)
                                                                    (or other-rels []))]
                                             (assoc c :rels final-rels)))
                                         ctx-after-scan regular-merge-clauses))
                      ;; Optional merges via per-row bind-by-fn(get-else). Synthetic
                      ;; clause is [e-var attr bind-var] (attr already resolved to eid
                      ;; in :attribute-refs? mode by logical.cljc).
                          ctx' (binding [rel/*implicit-source* op-db]
                                 (reduce (fn [c opt-merge]
                                           (let [[e-var attr bind-var] (:clause opt-merge)
                                                 default-val (:default-value opt-merge)
                                                 ;; seq defaults were quote-unwrapped at IR
                                                 ;; construction (logical/LOptionalScan); re-wrap
                                                 ;; so -call-fn round-trips and check-fn-args
                                                 ;; accepts the synthetic clause.
                                                 default-arg (if (seq? default-val) (list 'quote default-val) default-val)
                                                 fn-clause [(list 'get-else '$ e-var attr default-arg) bind-var]]
                                             (bind-by-fn-strict c fn-clause)))
                                         ctx' optional-merge-ops))
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
                                         ctx' anti-clauses))
                      ;; Apply pushed-down predicates as post-filters. The
                      ;; planner records each push-down predicate's original
                      ;; clause in the scan-op's :pushdown-preds list and adds
                      ;; the clause to the plan's :consumed-preds set, so the
                      ;; clause-level predicate isn't emitted as its own
                      ;; :predicate op. The fused PSS scan path applies these
                      ;; via slice bounds + strict-filter inside
                      ;; execute-fused-scan-rel; the temporal fallback uses
                      ;; lookup-batch-search which doesn't honor :pushdown-preds,
                      ;; so without a post-filter the predicate is silently
                      ;; dropped — surfaced in jobtech daynotes tests as rows
                      ;; matching ?inst < ?from-version that should have been
                      ;; filtered out.
                          pushdown-pred-clauses
                          (concat (mapv :pred-clause (:pushdown-preds (:scan-op op)))
                                  (mapcat #(mapv :pred-clause (:pushdown-preds %)) all-merge-ops))
                          ctx' (binding [rel/*implicit-source* op-db]
                                 (reduce (fn [c pred-clause]
                                           (#?(:clj legacy/filter-by-pred :cljs (rel/get-legacy-fn :filter-by-pred))
                                            c pred-clause))
                                         ctx' (filter some? pushdown-pred-clauses)))]
                      (recur ctx' plan (inc idx)))))

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
                    (let [ti (temporal-info op-db)
                          scan-attr (second (:clause op))]
                      (if (and (some? ti)
                               (pss-instance? (:eavt (:origin-db ti)))
                               (not (:optional? op))
                               (some? scan-attr) (not (symbol? scan-attr)))
                    ;; Temporal DB with a PSS-backed origin and a concrete-attr,
                    ;; non-optional pattern: stay on the FUSED seek path (per bound
                    ;; entity/value temporal index seeks via execute-group-direct +
                    ;; build-scan-slice) instead of the per-clause legacy
                    ;; lookup-batch-search, which cannot seek on a temporal wrapper
                    ;; and full-scans the attribute every call (catastrophic inside
                    ;; a recursive-rule fixpoint). Optional (get-else) and
                    ;; variable-attribute patterns stay on legacy below.
                        (let [[ctx' _] (execute-temporal-group-rel op-db op ctx ti)]
                          (recur ctx' plan (inc idx)))
                  ;; Temporal/non-standard DB — use legacy lookup with search context.
                  ;; lookup-batch-search takes [source context orig-pattern resolved-pattern];
                  ;; passing clause twice — no resolution needed in fallback.
                  ;; If this op is a standalone LOptionalScan-derived pattern-scan
                  ;; (get-else where the entity isn't shared with another scan in
                  ;; the same scope, e.g. `?e` only appears inside OR branches),
                  ;; the legacy lookup-batch-search would inner-join and drop
                  ;; entities lacking the attribute. Route through bind-by-fn for
                  ;; left-outer-with-default semantics, matching the legacy
                  ;; engine's behavior for [(get-else …) ?v].
                        (let [ctx' (binding [rel/*implicit-source* op-db]
                                     (if (:optional? op)
                                       (let [[e-var attr bind-var] (:clause op)
                                             default-val (:default-value op)
                                             ;; re-wrap seq defaults — see the optional-merge
                                             ;; site above.
                                             default-arg (if (seq? default-val) (list 'quote default-val) default-val)
                                             fn-clause [(list 'get-else '$ e-var attr default-arg) bind-var]]
                                         (bind-by-fn-strict ctx fn-clause))
                                       (#?(:clj legacy/lookup-batch-search :cljs (rel/get-legacy-fn :lookup-batch-search)) op-db ctx (:clause op) (:clause op))))
                        ;; Re-apply pushed-down predicates as a post-filter.
                        ;; lookup-batch-search doesn't honor :pushdown-preds and
                        ;; the planner has already consumed the clause-level
                        ;; predicate (:consumed-preds), so without this filter
                        ;; the predicate is silently dropped. Symmetric with
                        ;; the entity-group branch above.
                              ctx' (binding [rel/*implicit-source* op-db]
                                     (reduce (fn [c pred-clause]
                                               (#?(:clj legacy/filter-by-pred :cljs (rel/get-legacy-fn :filter-by-pred))
                                                c pred-clause))
                                             ctx' (filter some? (mapv :pred-clause (:pushdown-preds op)))))]
                          (recur ctx' plan (inc idx)))))
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
                        (let [new-rel (execute-pattern-scan op-db op (:cancel ctx) (:rels ctx))
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
                (recur (#?(:clj legacy/filter-by-pred
                           :cljs (rel/get-legacy-fn :filter-by-pred))
                        ctx (:clause op))
                       plan (inc idx))

                :function
                (let [run-fn (fn [c o] (bind-by-fn-strict c (:clause o)))
                      in-vars (set (plan/args-free-vars (:args op)))
                      rest-ops (subvec (:ops plan) (inc idx))
                    ;; Only probe when it can pay off: the function is expensive
                    ;; (:exec-cost-fn), not already probed, on a PSS default
                    ;; source, AND some deferred index-backed op shares its input.
                      probe? (and (:exec-cost-fn op)
                                  (not (:probed? op))
                                  (nil? (:source op))
                                  (pss-instance? (:eavt op-db))
                                  (some #(and (#{:pattern-scan :entity-group} (:op %))
                                              (some in-vars (:vars %)))
                                        rest-ops))]
                  (if-not probe?
                    (recur (run-fn ctx op) plan (inc idx))
                    (let [cur-cards (ctx-var-cards ctx)
                          bound (set (keys cur-cards))
                        ;; Measure each candidate's true join size and inject it
                        ;; as the ordering cost (group-effective-card reads
                        ;; :scan-card first, then :estimated-card; op-output-cards
                        ;; reads :output-var-cards).
                          rest' (mapv (fn [o]
                                        (if (probe-candidate? o in-vars bound)
                                          (if-let [c (probe-card op-db o ctx)]
                                            (assoc o :scan-card c :estimated-card c
                                                   :output-var-cards (zipmap (:vars o) (repeat c)))
                                            o)
                                          o))
                                      rest-ops)
                        ;; Re-order the function together with the remaining ops,
                        ;; seeded with the live per-var cardinalities (MAP seed so
                        ;; the function's input-rows estimate is real; db=nil so
                        ;; the injected measured cards aren't re-derived away).
                          to-order (into [(assoc op :probed? true)] rest')
                          reordered (plan/order-plan-ops to-order cur-cards nil)
                          moved? (not= :function (:op (first reordered)))]
                      (if moved?
                      ;; A reducing join sank ahead of the function — splice the
                      ;; new order in and re-run from the function's slot.
                        (recur ctx
                               (assoc plan :ops (into (subvec (:ops plan) 0 idx) reordered))
                               idx)
                      ;; Nothing reduces the input (or only expansions remain) —
                      ;; run the function now; mark probed so we don't re-probe.
                        (recur (run-fn ctx op) plan (inc idx))))))

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

              ;; Unknown op — a plan/IR bug. Raise instead of skipping: silently
              ;; advancing past an op drops that clause's constraint from the
              ;; result (same silent-degradation class as the #814/#815 guard
              ;; history — see bind-by-fn-strict).
                (log/raise "Unknown plan operation"
                           {:error :query/plan :op (:op op) :clause (:clause op)})))))))))

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
   cancel: optional IDeref; see execute-plan-direct.
   plan, db: as usual"
     [plan db find-elements aggregate-fn cancel]
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
                                 :pipeline (:pipeline g)
                                 :cancel cancel)
           ;; Apply attached predicates (post-scan filters from pushdown)
           ;; These are predicates that were attached to the group during planning
           ;; but not pushed into index bounds — they filter result tuples.
           (when-let [attached (seq (:attached-preds g))]
             (apply-attached-preds result-list attached
                                   (vec (or (:output-vars g) (:vars g)))
                                   all-vars nil))
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
