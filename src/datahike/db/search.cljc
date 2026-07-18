(ns datahike.db.search
  (:require
   #?(:clj [clojure.core.cache.wrapped :as cw]
      :cljs [cljs.cache.wrapped :as cw])
   [datahike.array :refer [a=]]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :refer [datom datom-tx datom-added type-hint-datom]]
   [datahike.db.utils :as dbu]
   [datahike.index :as di]
   [datahike.lru :refer [lru-datom-cache-factory]]
   [datahike.tools :refer [match-vector]]
   [environ.core :refer [env]]
   #?(:cljs [is.simm.partial-cps.async :as pca :refer-macros [async]])
   #?(:cljs [org.replikativ.persistent-sorted-set.btset :as btset])
   [replikativ.logging :as log])
  #?(:cljs (:require-macros [datahike.db.search :refer [lookup-strategy]]))
  #?(:clj (:import [datahike.datom Datom])))

(def db-caches (cw/lru-cache-factory {} :threshold (:datahike-max-db-caches env 5)))

(defn- db-snapshot-key
  "Identity of a DB snapshot for the search cache. The additive :hash alone
   is collision-prone and is shared across stores and branches; scoping the
   key to store + branch + snapshot counters means two databases can never
   share cached datoms (cf. execute's date-tx-id-cache, which established
   this key shape)."
  [db]
  (let [config (:config db)]
    [(:store config) (:branch config) (:hash db) (:max-tx db) (:max-eid db)]))

(defn memoize-for
  "Per-DB-snapshot memoization of index searches, sized by the
   `:search-cache-size` config value (0 disables).

   History: introduced in 2021 gated on `:cache-size`; #503 (Nov 2022)
   renamed the config key to `:search-cache-size` without updating this
   reader, so the cache was silently dead for three years — the knob stayed
   spec'd, documented and env-configurable while `(:cache-size config)` was
   always nil. Gate and threshold now read the real config key; a test in
   datahike.test.search-cache-test pins the wiring so a rename cannot
   silently kill it again."
  [db key f]
  (let [cache-size (long (or (:search-cache-size (:config db)) 0))]
    (if (or (zero? cache-size)
            (zero? (:hash db))) ;; empty db
      (f)
      (let [db-cache (cw/lookup-or-miss db-caches
                                        (db-snapshot-key db)
                                        (fn [_] (lru-datom-cache-factory {} :threshold cache-size)))]
        (cw/lookup-or-miss db-cache key (fn [_] (f)))))))

(defn validate-pattern
  "Checks if database pattern is valid"
  [db pattern can-have-vars]
  (let [bound-var? (if can-have-vars symbol? (fn [_] false))
        [e a v tx added?] pattern]

    (when-not (or (number? e)
                  (nil? e)
                  (bound-var? e)
                  (and (vector? e) (= 2 (count e))))
      (log/raise "Bad format for entity-id in pattern, must be a number, nil or vector of two elements."
                 {:error :search/pattern :e e :pattern pattern}))

    (when-not (or (number? a)
                  (keyword? a)
                  (bound-var? a)
                  (nil? a))
      (log/raise "Bad format for attribute in pattern, must be a number, nil or a keyword."
                 {:error :search/pattern :a a :pattern pattern}))

    ;; Value validation: allow vectors of any length for tuple attributes
    (when-not (or (not (vector? v))
                  (nil? v)
                  (bound-var? v)
                  ;; Allow 2-element vectors for lookup refs (when attr is ref type)
                  (and (vector? v) (= 2 (count v)))
                  ;; Allow any-length vectors for tuple values
                  (and (vector? v) a (dbu/tuple? db a)))
      (log/raise "Bad format for value in pattern, must be a scalar, nil or a vector of two elements."
                 {:error :search/pattern :v v :pattern pattern}))

    (when-not (or (nil? tx)
                  (bound-var? tx)
                  (number? tx))
      (log/raise "Bad format for transaction ID in pattern, must be a number or nil."
                 {:error :search/pattern :tx tx :pattern pattern}))

    (when-not (or (nil? added?)
                  (boolean? added?)
                  (bound-var? added?))
      (log/raise "Bad format for added? in pattern, must be a boolean value or nil."
                 {:error :search/pattern :added? added? :pattern pattern}))))

(defn short-hand->strat-symbol [x]
  (case x
    1 :substitute
    f :filter
    _ nil
    :substitute :substitute
    :filter :filter
    nil nil))

(defn datom-expr [[esym asym vsym tsym]
                  [e-strat a-strat v-strat t-strat]
                  e-bound
                  tx-bound]
  (let [subst (fn [expr strategy bound]
                (case strategy
                  :substitute expr
                  bound))]
    `(datom ~(subst esym e-strat e-bound)
            ~(subst asym a-strat nil)
            ~(subst vsym v-strat nil)
            ~(subst tsym t-strat tx-bound))))

(defn lookup-strategy-sub [index-key eavt-symbols eavt-strats]
  (let [[_ _ v-strat t-strat] eavt-strats
        [_ _ v-sym t-sym] eavt-symbols
        strat-set (set eavt-strats)
        has-substitution (contains? strat-set :substitute)
        index-expr (symbol index-key)

        lower-datom (datom-expr eavt-symbols eavt-strats 'e0 'tx0)
        upper-datom (datom-expr eavt-symbols eavt-strats 'emax 'txmax)

        ;; Either get all datoms or a subset where some values in the
        ;; datom are fixed.
        lookup-expr (if has-substitution
                      `(di/-slice ~index-expr
                                  ~lower-datom
                                  ~upper-datom
                                  ~index-key)
                      `(di/-all ~index-expr))

        ;; Symbol type-hinted as Datom.
        dexpr (type-hint-datom (gensym))

        ;; Equalities used for filtering (in conjunction)
        equalities (remove nil? [(when (= :filter v-strat)
                                   `(a= ~v-sym (.-v ~dexpr)))
                                 (when (= :filter t-strat)
                                   `(= ~t-sym (datom-tx ~dexpr)))])
        added (gensym)]
    `{:index-key ~index-key
      :strategy-vec ~(vec eavt-strats)
      :lookup-fn (fn [~index-expr ~eavt-symbols]
                   ~(if (seq equalities)
                      `(filter (fn [~dexpr] (and ~@equalities)) ~lookup-expr)
                      lookup-expr))
      :backend-fn (fn [~index-expr]
                    (fn [~@eavt-symbols ~added]
                      ~lookup-expr))}))

(defmacro lookup-strategy [index-key & eavt-strats]
  {:pre [(keyword? index-key)]}
  (let [pattern-symbols '[e a v tx]]
    (lookup-strategy-sub index-key
                         pattern-symbols
                         (map short-hand->strat-symbol eavt-strats))))

(defn- get-search-strategy-impl [e a v t i]
  (match-vector [e a v t i]
                [e a v t *] (lookup-strategy :eavt 1 1 1 1)
                [e a v _ *] (lookup-strategy :eavt 1 1 1 _)
                [e a _ t *] (lookup-strategy :eavt 1 1 _ f)
                [e a _ _ *] (lookup-strategy :eavt 1 1 _ _)
                [e _ v t *] (lookup-strategy :eavt 1 _ f f)
                [e _ v _ *] (lookup-strategy :eavt 1 _ f _)
                [e _ _ t *] (lookup-strategy :eavt 1 _ _ f)
                [e _ _ _ *] (lookup-strategy :eavt 1 _ _ _)
                [_ a v t i] (lookup-strategy :avet _ 1 1 f)
                [_ a v t _] (lookup-strategy :aevt _ 1 f f)
                [_ a v _ i] (lookup-strategy :avet _ 1 1 _)
                [_ a v _ _] (lookup-strategy :aevt _ 1 f _)
                [_ a _ t *] (lookup-strategy :aevt _ 1 _ f)
                [_ a _ _ *] (lookup-strategy :aevt _ 1 _ _)
                [_ _ v t *] (lookup-strategy :eavt _ _ f f)
                [_ _ v _ *] (lookup-strategy :eavt _ _ f _)
                [_ _ _ t *] (lookup-strategy :eavt _ _ _ f)
                [_ _ _ _ *] (lookup-strategy :eavt _ _ _ _)))

(defn empty-lookup-fn
  ([_db-index [_e _a _v _tx]]
   [])
  ([_db-index [_e _a _v _tx] _batch-fn]
   []))

(defn- get-search-strategy [db pattern indexed? temporal-db?]
  (validate-pattern db pattern true)
  (let [[e a v tx added?] pattern]
    (when-not (and (not temporal-db?) (false? added?))
      (get-search-strategy-impl
       (boolean e)
       (boolean a)
       (some? v)
       (boolean tx)
       (boolean indexed?)))))

(defn- resolve-db-index [strategy index-map]
  (when strategy
    (assoc strategy
           :db-index
           (get index-map (:index-key strategy)))))

(defn- indexing-for-pattern? [db pattern]
  (let [[_ a _ _] pattern]
    (dbu/indexing? db a)))

(defn search-strategy-with-index [db pattern temporal?]
  (-> (get-search-strategy db
                           pattern
                           (indexing-for-pattern? db pattern)
                           temporal?)
      (resolve-db-index (if temporal?
                          {:eavt (:temporal-eavt db)
                           :aevt (:temporal-aevt db)
                           :avet (:temporal-avet db)}
                          db))))

(defn search-current-indices
  ([db pattern]
   (memoize-for
    db [:search pattern]
    #(if-let [{:keys [db-index lookup-fn]}
              (search-strategy-with-index db pattern false)]
       (lookup-fn db-index pattern)
       [])))

  ;; For batches
  ([db pattern batch-fn]
   (if-let [{:keys [db-index strategy-vec backend-fn]}
            (search-strategy-with-index db pattern false)]
     (batch-fn strategy-vec (backend-fn db-index) identity)
     [])))

#?(:cljs
   (defn- achunk-step
     "One chunk-granular step over a PSS AsyncSeq (or a realized coll served
      as instantly-resolving pseudo-chunks). Async-only local copy of the
      query engine's chunk consumption — db.search must not require the
      engine (that would drag the planner into the write path)."
     [s]
     (cond
       (nil? s) (async nil)
       (not (instance? btset/AsyncSeq s))
       (async
        (when-let [sq (seq s)]
          (if (chunked-seq? sq)
            (let [ch (chunk-first sq)]
              [(.-arr ch) (.-off ch) (.-end ch) (chunk-next sq)])
            [(array (first sq)) 0 1 (next sq)])))
       :else (btset/achunk-next s))))

#?(:cljs
   (defn arealize-slice
     "Async-only slice realization into a vector; pred (nil = keep all) is
      applied synchronously per datom."
     [index from to index-type pred]
     (async
      (let [s0 (pca/await (di/-slice index from to index-type {:sync? false}))]
        (loop [s s0 acc (transient [])]
          (if-some [step (pca/await (achunk-step s))]
            (let [ks (nth step 0)
                  start (nth step 1)
                  end (nth step 2)
                  nxt (nth step 3)]
              (recur nxt
                     (loop [i start a acc]
                       (if (< i end)
                         (let [d (aget ks i)]
                           (recur (inc i) (if (or (nil? pred) (pred d)) (conj! a d) a)))
                         a))))
            (persistent! acc)))))))

#?(:cljs
   (defn search-current-step
     "Async mirror of (search-current-indices db pattern): the SAME strategy
      selection (validate-pattern raises included), bounds and filters
      derived from the declarative :strategy-vec, realized via awaited
      chunk consumption. Deliberately UNMEMOIZED — the transaction path
      searches the evolving transient, whose state must never be cached."
     [db pattern]
     (async
      (if-let [{:keys [index-key strategy-vec db-index]}
               (search-strategy-with-index db pattern false)]
        (let [[e a v tx] pattern
              [es as vs ts] strategy-vec
              from (datom (if (= :substitute es) e e0)
                          (when (= :substitute as) a)
                          (when (= :substitute vs) v)
                          (if (= :substitute ts) tx tx0))
              to (datom (if (= :substitute es) e emax)
                        (when (= :substitute as) a)
                        (when (= :substitute vs) v)
                        (if (= :substitute ts) tx txmax))
              pred (cond
                     (and (= :filter vs) (= :filter ts))
                     (fn [d] (and (a= v (.-v d)) (= tx (datom-tx d))))
                     (= :filter vs) (fn [d] (a= v (.-v d)))
                     (= :filter ts) (fn [d] (= tx (datom-tx d)))
                     :else nil)]
          (pca/await (arealize-slice db-index from to index-key pred)))
        []))))

#?(:cljs
   (defn datoms-components-step
     "Async prefix-components read on a PLAIN db record (base-context
      semantics — exactly what the transaction path's dbi/datoms calls see)."
     [db index-type cs]
     (arealize-slice (get db index-type)
                     (dbu/components->pattern db index-type cs e0 tx0)
                     (dbu/components->pattern db index-type cs emax txmax)
                     index-type nil)))

#?(:cljs
   (defn entid-step
     "Async dbu/entid: identical validation raises; avet probes awaited."
     [db eid]
     (async
      (cond
        (dbu/numeric-entid? eid) eid

        (sequential? eid)
        (let [[attr value] eid]
          (cond
            (not= (count eid) 2)
            (log/raise "Lookup ref should contain 2 elements: " eid
                       {:error :lookup-ref/syntax, :entity-id eid})
            (not (dbu/is-attr? db attr :db/unique))
            (log/raise "Lookup ref attribute should be marked as :db/unique: " eid
                       {:error :lookup-ref/unique, :entity-id eid})
            (nil? value)
            nil
            :else
            (:e (first (pca/await (datoms-components-step db :avet [attr value]))))))

        (array? eid)
        (pca/await (entid-step db (array-seq eid)))

        (keyword? eid)
        (:e (first (pca/await (datoms-components-step db :avet [:db/ident eid]))))

        :else
        (log/raise "Expected number or lookup ref for entity id, got " eid
                   {:error :entity-id/syntax, :entity-id eid})))))

#?(:cljs
   (defn entid-strict-step
     [db eid]
     (async
      (or (pca/await (entid-step db eid))
          (log/raise "Nothing found for entity id " eid
                     {:error :entity-id/missing
                      :entity-id eid})))))

(defn added? [[_ _ _ _ added]]
  added)

(defn filter-by-added
  ([pattern]
   (case (added? pattern)
     true (filter datom-added)
     false (remove datom-added)
     identity))
  ([pattern result]
   (case (added? pattern)
     true (filter datom-added result)
     false (remove datom-added result)
     result)))

(defn search-temporal-indices
  ([db pattern]
   (validate-pattern db pattern false)
   (memoize-for db [:temporal-search pattern]
                #(if-let [{:keys [db-index lookup-fn]}
                          (search-strategy-with-index
                           db pattern true)]
                   (let [result (lookup-fn db-index pattern)]
                     (filter-by-added pattern result))
                   [])))
  ([db pattern batch-fn]
   (validate-pattern db pattern true)
   (if-let [{:keys [db-index strategy-vec backend-fn]}
            (search-strategy-with-index
             db pattern true)]
     (batch-fn strategy-vec (backend-fn db-index)
               (filter-by-added pattern))
     [])))

(defn index-type [db pattern]
  (let [idx (indexing-for-pattern? db pattern)
        a (:index-key (get-search-strategy db pattern idx false))
        b (:index-key (get-search-strategy db pattern idx true))]
    (assert (or (nil? a) (keyword? a)))
    (assert (or (nil? b) (keyword? b)))
    (when (= a b)
      a)))

(defn temporal-search
  ([db pattern]
   (validate-pattern db pattern false)
   (dbu/distinct-datoms
    db
    (index-type db pattern)
    (search-current-indices db pattern)
    (search-temporal-indices db pattern)))
  ([db pattern batch-fn]
   (validate-pattern db pattern true)
   (dbu/distinct-datoms
    db
    (index-type db pattern)
    (search-current-indices db pattern batch-fn)
    (search-temporal-indices db pattern batch-fn))))

(defn temporal-seek-datoms [db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (dbu/components->pattern db index-type cs e0 tx0)
        to (datom emax nil nil txmax)]
    (dbu/distinct-datoms db
                         index-type
                         (di/-slice index from to index-type)
                         (di/-slice temporal-index from to index-type))))

(defn temporal-rseek-datoms
  "Reverse seek over history dbs: datoms <= cs, descending to the index
   beginning. FULLY LAZY — the symmetric counterpart of
   `temporal-seek-datoms`: reverse slices of the current + temporal
   indexes (`-rslice`, from the seek point down) merged descending via
   `distinct-datoms-desc`. `(take n …)` restores only the nodes on the
   seek path plus the n consumed datoms."
  [db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (dbu/components->pattern db index-type cs emax txmax)
        to (datom e0 nil nil tx0)]
    (dbu/distinct-datoms-desc db
                              index-type
                              (di/-rslice index from to index-type)
                              (di/-rslice temporal-index from to index-type))))

(defn temporal-index-range [db current-db attr start end]
  (when-not (dbu/indexing? db attr)
    (log/raise "Attribute" attr "should be marked as :db/index true" {}))
  (dbu/validate-attr attr (list '-index-range 'db attr start end) db)
  (let [from (dbu/resolve-datom current-db nil attr start nil e0 tx0)
        to (dbu/resolve-datom current-db nil attr end nil emax txmax)]
    (dbu/distinct-datoms db
                         :avet
                         (di/-slice (:avet db) from to :avet)
                         (di/-slice (:temporal-avet db) from to :avet))))


