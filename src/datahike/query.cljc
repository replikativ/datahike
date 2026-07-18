(ns ^:no-doc datahike.query
  #?(:cljs (:require-macros [datahike.query :refer [basic-index-selector make-vec-lookup-ref-replacer some-of substitution-expansion]]))
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [clojure.set :as set]
   [clojure.string :as str]
   [is.simm.partial-cps.async :as pca :refer [async async+sync]]
   [datahike.query.estimate :as estimate]
   [clojure.walk :as walk]
   [datahike.datom :as datom]
   [datahike.db.interface :as dbi]
   [datahike.db.utils :as dbu]
   [datahike.index.interface :as di]
   [datahike.array :refer [wrap-comparable]]
   [datahike.impl.entity :as de]
   [datahike.lru]
   [datahike.pull-api :as dpa]
   [datahike.query-stats :as dqs]
   [datahike.tools :as dt]
   [datalog.parser :refer [parse]]
   [datalog.parser.impl :as dpi]
   [datalog.parser.impl.proto :as dpip]
   [datalog.parser.pull :as dpp]
   #?(:cljs [datalog.parser.type :refer [Aggregate BindColl BindIgnore BindScalar BindTuple Constant
                                         FindColl FindRel FindScalar FindTuple PlainSymbol Pull
                                         RulesVar SrcVar Variable]])
   [datahike.constants :as const]
   [datahike.query.relation :as rel]
   [datahike.query.plan :as plan]
   [datahike.query.analyze :as analyze]
   #?(:clj [datahike.query.logical :as logical])
   #?(:clj [datahike.query.lower :as lower])
   #?(:cljs [datahike.db :refer [DB AsOfDB SinceDB HistoricalDB FilteredDB]])
   #?(:cljs [datahike.query.execute :as execute])
   #?(:cljs [datahike.query.logical :as logical])
   #?(:cljs [datahike.query.lower :as lower])
   #?(:clj [datahike.index.entity-set :as es])
   #?(:clj [datahike.index.secondary :as sec])
   ;; NOTE: datahike.index.secondary.stratum is loaded lazily via requiring-resolve
   ;; to keep stratum as an optional dependency
   [org.replikativ.persistent-sorted-set.arrays :as da]
   [replikativ.logging :as log])
  (:refer-clojure :exclude [seqable?])

  #?(:clj (:import [clojure.lang Reflector Seqable]
                   [datahike.datom Datom]
                   [datahike.db DB AsOfDB SinceDB HistoricalDB FilteredDB]
                   [datahike.query.relation Relation]
                   [datalog.parser.type Aggregate BindColl BindIgnore BindScalar BindTuple Constant
                    FindColl FindRel FindScalar FindTuple PlainSymbol Pull
                    RulesVar SrcVar Variable]
                   [java.lang.reflect Method]
                   [java.util Date Map HashSet HashSet])))

#?(:clj (set! *warn-on-reflection* true))

;; ----------------------------------------------------------------------------

(def ^:const lru-cache-size 100)

(def ^:dynamic *disable-planner*
  "When true, bypass the query planner and run every query through the relational
   (base) engine. The planner is the DEFAULT: it handles eligible queries and falls
   back to the relational engine for the rest (multi-source disjoint joins, nested
   temporal wrappers, stats), so the base engine is a permanent fallback, not legacy.
   Set DATAHIKE_QUERY_PLANNER=false to disable the planner globally."
  #?(:clj  (= "false" (System/getenv "DATAHIKE_QUERY_PLANNER"))
     :cljs false))

(def ^:dynamic *query-result-cache?*
  "When true (default), query results are cached by [query args db-snapshot].
   Bind to false for benchmarking raw query execution."
  true)

(declare -collect -resolve-clause resolve-clause raw-q)

;; Records

;; `cancel`: optional clojure.lang.Volatile holding a boolean. When truthy,
;; query execution raises :datahike/canceled at the next check point. `nil`
;; means no cancellation (zero cost in the hot path). Protocol adapters
;; (pgwire, etc.) map :datahike/canceled to their own error codes at the
;; boundary — datahike core stays protocol-agnostic.
(defrecord Context [rels sources rules consts settings cancel])
(defrecord StatContext [rels sources rules consts stats settings cancel])

;; Relation record is defined in datahike.query.relation
;; Imported via :refer [->Relation] and (:import [datahike.query.relation Relation])

;; Main functions

(declare built-in-rule-names auto-inject-built-in-rules)

(defn normalize-q-input
  "Turns input to q into a map with :query and :args fields.
   Also normalizes the query into a map representation."
  [query-input arg-inputs]
  (let [query (-> query-input
                  (#(or (and (map? %) (:query %)) %))
                  (#(if (string? %) (edn/read-string %) %))
                  (#(if (= 'quote (first %)) (second %) %))
                  (#(if (sequential? %) (dpi/query->map %) %)))
        args (if (and (map? query-input) (contains? query-input :args))
               (do (when (seq arg-inputs)
                     (log/warn :datahike/query-input-ignored {:query query}))
                   (:args query-input))
               arg-inputs)
        extra-ks [:offset :limit :order-by :stats? :count-fns? :settings :cancel :sync?]]
    (-> (cond-> {:query (apply dissoc query extra-ks)
                 :args args}
          (map? query-input)
          (merge (select-keys query-input extra-ks)))
        auto-inject-built-in-rules)))

(declare raw-q* raw-q-async *profile?*)

(defn q
  "Executes a datalog query. The arg-map form accepts `:sync? false`
   (ClojureScript): the query runs on the async engine and a partial-cps
   async expression is returned — invoke it with (expr resolve reject) or
   await it in an async block; warm stores resolve on the calling stack,
   cold async-only stores stream every read."
  [query & inputs]
  (let [qm (normalize-q-input query inputs)]
    (if (false? (:sync? qm))
      (raw-q-async qm)
      (raw-q (dissoc qm :sync?)))))

(defn raw-q-async
  "Asynchronous raw query entry (ClojureScript only): runs the raw-q* dual
   spine in async mode, returning a partial-cps async expression yielding
   the result. Bypasses the result cache (v1 — sync q caches settled
   results). Snapshots the dynamic controls exactly like raw-q."
  [query-map]
  #?(:clj (throw (ex-info "q-async is ClojureScript-only — on the JVM use q (virtual threads make an async engine unnecessary)"
                          {:error :storage/async-unsupported}))
     :cljs (raw-q* (assoc query-map
                          :sync? false
                          :disable-planner? *disable-planner*
                          :profile? *profile?*))))

(defn q-async
  "Asynchronous query (ClojureScript): same inputs as `q`, returns a
   partial-cps async expression — invoke it with (expr resolve reject), or
   await it inside an async block. Warm stores resolve on the calling stack
   (the trampoline's sync-completion property); cold async-only stores
   stream EVERY read — planning included, across all find specs, rules,
   wrapper stacks (temporal, filtered, nested) and pull. Remaining sync
   islands (query-stats, collection-only sources, db-reading filter
   predicates, tx functions) surface the decorated
   :storage/sync-read-unavailable fault cleanly. Equivalent to the public
   arg-map form (q {:query .. :args .. :sync? false})."
  [query & inputs]
  (raw-q-async (normalize-q-input query inputs)))

(defn query-stats [query & inputs]
  (-> query
      (normalize-q-input inputs)
      (assoc :stats? true)
      q))

;; ---------------------------------------------------------------------------
;; Query explain — human-readable plan visualization

#?(:clj (declare ^:private format-plan-ops))

#?(:clj
   (defn- format-op
     "Format a single plan op as a human-readable string with indentation."
     [op indent]
     (let [pad (apply str (repeat indent "  "))
           card (when-let [c (:estimated-card op)] (format " (est. %s rows)" c))]
       (case (:op op)
         :pattern-scan
         (str pad "SCAN " (name (or (:index op) :unknown))
              " " (pr-str (:clause op))
              (when-let [si (:schema-info op)]
                (str " [" (when (:ref? si) "ref ")
                     (if (:card-one? si) "card-one" "card-many")
                     (when (:unique? si) " unique")
                     "]"))
              card
              (when (seq (:pushdown-preds op))
                (str "\n" pad "  PUSHDOWN " (pr-str (mapv :clause (:pushdown-preds op))))))

         :entity-group
         (let [scan (:scan-op op)
               merges (:merge-ops op)
               lines [(str pad "ENTITY-GROUP on " (:entity-var op) card)
                      (str pad "  scan: " (pr-str (:clause scan))
                           " [" (name (or (:index scan) :unknown)) "]"
                           (when-let [si (:schema-info scan)]
                             (str " " (if (:card-one? si) "card-one" "card-many")
                                  (when (:unique? si) " unique")
                                  (when (:ref? si) " ref")))
                           (when-let [c (:estimated-card scan)]
                             (format " est=%d" (long c))))]]
           (str (clojure.string/join "\n" lines)
                (when (seq merges)
                  (str "\n" (clojure.string/join "\n"
                                                 (map-indexed
                                                  (fn [i m]
                                                    (str pad "  merge[" i "]: " (pr-str (:clause m))
                                                         (when-let [si (:schema-info m)]
                                                           (str " [" (if (:card-one? si) "card-one" "card-many")
                                                                (when (:ref? si) " ref") "]"))
                                                         (when (:anti? m) " [ANTI]")
                                                         (when-let [c (:estimated-card m)]
                                                           (format " est=%d" (long c)))))
                                                  merges))))
                (when (seq (:pushdown-preds scan))
                  (str "\n" pad "  pushdown: "
                       (pr-str (mapv :clause (:pushdown-preds scan)))))))

         :predicate
         (str pad "FILTER " (pr-str (:clause op))
              (when-let [s (:estimated-selectivity op)]
                (format " (sel=%.2f)" (double s))))

         :function
         (str pad "BIND " (pr-str (:clause op)))

         :or
         (str pad "OR" card "\n"
              (clojure.string/join "\n"
                                   (map-indexed
                                    (fn [i branch]
                                      (str pad "  branch[" i "]:\n"
                                           (format-plan-ops (:ops branch) (+ indent 2))))
                                    (:branches op))))

         :or-join
         (str pad "OR-JOIN " (pr-str (:join-vars op)) card "\n"
              (clojure.string/join "\n"
                                   (map-indexed
                                    (fn [i branch]
                                      (str pad "  branch[" i "]:\n"
                                           (format-plan-ops (:ops branch) (+ indent 2))))
                                    (:branches op))))

         :not
         (str pad "NOT\n"
              (format-plan-ops (:ops (:sub-plan op)) (+ indent 1)))

         :not-join
         (str pad "NOT-JOIN " (pr-str (:join-vars op)) "\n"
              (format-plan-ops (:ops (:sub-plan op)) (+ indent 1)))

         :recursive-rule
         (let [{:keys [rule-name call-args head-vars scc-rule-names scc-rule-plans]} op
               mutual? (> (count scc-rule-names) 1)]
           (str pad "RECURSIVE-RULE " rule-name
                " call-args=" (pr-str call-args)
                (when head-vars (str " head-vars=" (pr-str head-vars)))
                (when mutual? (str " [MUTUAL SCC: " (pr-str scc-rule-names) "]"))
                "\n"
                (clojure.string/join "\n"
                                     (mapcat
                                      (fn [rn]
                                        (let [{:keys [head-vars base-plans rec-clause-versions]}
                                              (get scc-rule-plans rn)]
                                          (concat
                                           [(str pad "  " rn " head-vars=" (pr-str head-vars))]
                                           [(str pad "    base-plans: " (count base-plans))]
                                           (map-indexed
                                            (fn [i bp]
                                              (str pad "      [" i "]:\n"
                                                   (format-plan-ops (:ops bp) (+ indent 4))))
                                            base-plans)
                                           [(str pad "    clause-versions: " (count rec-clause-versions))]
                                           (map-indexed
                                            (fn [i cv]
                                              (str pad "      [" i "]:\n"
                                                   (format-plan-ops (:ops cv) (+ indent 4))))
                                            rec-clause-versions))))
                                      scc-rule-names))))

         :rule-lookup
         (str pad "RULE-LOOKUP " (:rule-name op)
              " " (pr-str (:call-args op))
              " mode=" (name (:mode op))
              card)

         :passthrough
         (str pad "PASSTHROUGH (legacy) " (pr-str (:clause op)))

         ;; default
         (str pad (name (:op op)) " " (pr-str (:clause op)) card)))))

#?(:clj
   (defn- format-plan-ops [ops indent]
     (clojure.string/join "\n"
                          (map #(format-op % indent) ops))))

;; explain is defined later in the file (after memoized-parse-query, Context, etc.)

;; Utilities

(defn distinct-tuples
  "Remove duplicates just like `distinct` but with the difference that it only works on values on which `vec` can be applied and two different objects are considered equal if and only if their results after `vec` has been applied are equal. This means that two different Java arrays are considered equal if and only if their elements are equal."
  ([tuples]
   (into [] (distinct-tuples) tuples))
  ([]
   (let [step ((distinct) (fn [_ _] true))]
     (filter #(step false (vec %))))))

(defn seqable?
  #?@(:clj [^Boolean [x]]
      :cljs [^boolean [x]])
  (and (not (string? x))
       #?(:cljs (or (cljs.core/seqable? x)
                    (da/array? x))
          :clj (or (seq? x)
                   (instance? Seqable x)
                   (nil? x)
                   (instance? Iterable x)
                   (da/array? x)
                   (instance? Map x)))))

(defn intersect-keys [attrs1 attrs2]
  (set/intersection (set (keys attrs1))
                    (set (keys attrs2))))

(defn concatv [& xs]
  (into [] cat xs))

(defn same-keys? [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))
       (every? #(contains? b %) (keys a))))

(defn- looks-like? [pattern form]
  (cond
    (= '_ pattern)
    true
    (= '[*] pattern)
    (sequential? form)
    (symbol? pattern)
    (= form pattern)
    (sequential? pattern)
    (if (= (last pattern) '*)
      (and (sequential? form)
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (map vector (butlast pattern) form)))
      (and (sequential? form)
           (= (count form) (count pattern))
           (every? (fn [[pattern-el form-el]] (looks-like? pattern-el form-el))
                   (map vector pattern form))))
    :else ;; (predicate? pattern)
    (pattern form)))

(defn source? [sym]
  (and (symbol? sym)
       (= \$ (first (name sym)))))

(defn free-var? [sym]
  (and (symbol? sym)
       (= \? (first (name sym)))))

(defn attr? [form]
  (or (keyword? form) (string? form)))

(defn lookup-ref? [form]
  ;; Using looks-like? here is quite inefficient.
  (and (vector? form)
       (= 2 (count form))
       (attr? (first form))))

(defn entid? [x]  ;; See `dbu/entid for all forms that are accepted
  (or (attr? x)
      (lookup-ref? x)
      (dbu/numeric-entid? x)
      (keyword? x)))

;; Relation algebra
(defn join-tuples [t1 #?(:cljs idxs1
                         :clj ^{:tag "[[Ljava.lang.Object;"} idxs1)
                   t2 #?(:cljs idxs2
                         :clj ^{:tag "[[Ljava.lang.Object;"} idxs2)]
  (let [l1 (alength idxs1)
        l2 (alength idxs2)
        res (da/make-array (+ l1 l2))]
    (dotimes [i l1]
      (aset res i (get t1 (aget idxs1 i))))
    (dotimes [i l2]
      (aset res (+ l1 i) (get t2 (aget idxs2 i))))
    res))

(defn sum-rel [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b]
    (cond
      (= attrs-a attrs-b)
      (rel/->Relation attrs-a (into (vec tuples-a) tuples-b))

      (not (same-keys? attrs-a attrs-b))
      (log/raise "Can't sum relations with different attrs: " attrs-a " and " attrs-b
                 {:error :query/where})

      (every? number? (vals attrs-a))                       ;; can’t conj into BTSetIter
      (let [idxb->idxa (vec (for [[sym idx-b] attrs-b]
                              [idx-b (attrs-a sym)]))
            tlen (->> (vals attrs-a) (reduce max) (inc))
            tuples' (persistent!
                     (reduce
                      (fn [acc tuple-b]
                        (let [tuple' (da/make-array tlen)]
                          (doseq [[idx-b idx-a] idxb->idxa]
                            (aset tuple' idx-a (get tuple-b idx-b)))
                          (conj! acc tuple')))
                      (transient (vec tuples-a))
                      tuples-b))]
        (rel/->Relation attrs-a tuples'))

      :else
      (let [all-attrs (zipmap (keys (merge attrs-a attrs-b)) (range))]
        (-> (rel/->Relation all-attrs [])
            (sum-rel a)
            (sum-rel b))))))

(defn simplify-rel [rel]
  (rel/->Relation (:attrs rel) (distinct-tuples (:tuples rel))))

(defn prod-rel
  ([] (rel/->Relation {} [(da/make-array 0)]))
  ([rel1 rel2]
   (let [attrs1 (keys (:attrs rel1))
         attrs2 (keys (:attrs rel2))
         idxs1 (to-array (map (:attrs rel1) attrs1))
         idxs2 (to-array (map (:attrs rel2) attrs2))]
     (rel/->Relation
      (zipmap (concat attrs1 attrs2) (range))
      (persistent!
       (reduce
        (fn [acc t1]
          (reduce (fn [acc t2]
                    (conj! acc (join-tuples t1 idxs1 t2 idxs2)))
                  acc (:tuples rel2)))
        (transient []) (:tuples rel1)))))))

;; built-ins

(defn- translate-for [db a]
  (if (and (-> db dbi/-config :attribute-refs?) (keyword? a))
    (dbi/-ref-for db a)
    a))

(defn- -differ? [& xs]
  (let [l (count xs)]
    (not= (take (/ l 2) xs) (drop (/ l 2) xs))))

(defn- cached-first-datom
  "Consult the async prefetch cache (dt/*db-fn-cache*) for the first
   visible datom of ground [e translated-attr]. Returns the datom,
   :datahike.tools/absent (probed, attribute absent), or nil (not probed
   / no cache — caller falls through to the direct index read)."
  [e ta]
  (when-some [c dt/*db-fn-cache*]
    (get c [e ta])))

(defn- -get-else
  [db e a else-val]
  (when (nil? else-val)
    (log/raise "get-else: nil default value is not supported" {:error :query/where}))
  (let [ta (translate-for db a)
        hit (cached-first-datom e ta)]
    (cond
      (= :datahike.tools/absent hit) else-val
      (some? hit) (:v hit)
      :else (if-some [datom (first (dbi/search db [e ta]))]
              (:v datom)
              else-val))))

(defn- -get-some
  [db e & as]
  (reduce
   (fn [_ a]
     (let [ta (translate-for db a)
           hit (cached-first-datom e ta)
           datom (cond
                   (= :datahike.tools/absent hit) nil
                   (some? hit) hit
                   :else (first (dbi/search db [e ta])))]
       (when-some [datom datom]
         (let [a-ident (if (keyword? (:a datom))
                         (:a datom)
                         (dbi/ident-for db (:a datom) :error-on-missing))]
           (reduced [a-ident (:v datom)])))))
   nil
   as))

(defn- -missing?
  [db e a]
  ;; forward-keyword attrs can be answered from the prefetch cache (presence
  ;; of a visible datom ≡ entity get non-nil); anything else — reverse refs,
  ;; :db/id — keeps the entity path.
  (let [hit (when (and (keyword? a) (not (str/starts-with? (name a) "_")))
              (cached-first-datom e (translate-for db a)))]
    (cond
      (= :datahike.tools/absent hit) true
      (some? hit) false
      :else (nil? (get (de/entity db e) a)))))

(defn- and-fn [& args]
  (reduce (fn [_a b]
            (if b b (reduced b))) true args))

(defn- or-fn [& args]
  (reduce (fn [_a b]
            (if b (reduced b) b)) nil args))

(defprotocol CollectionOrder
  (-strictly-decreasing? [x more])
  (-decreasing? [x more])
  (-strictly-increasing? [x more])
  (-increasing? [x more]))

(extend-protocol CollectionOrder

  #?(:clj Number :cljs number)
  (-strictly-decreasing? [x more] (apply < x more))
  (-decreasing? [x more] (apply <= x more))
  (-strictly-increasing? [x more] (apply > x more))
  (-increasing? [x more] (apply >= x more))

  #?(:clj Date :cljs js/Date)
  (-strictly-decreasing? [x more] #?(:clj (reduce (fn [res [d1 d2]] (if (.before ^Date d1 ^Date d2)
                                                                      res
                                                                      (reduced false)))
                                                  true (map vector (cons x more) more))
                                     :cljs (apply < x more)))
  (-decreasing? [x more] #?(:clj (reduce (fn [res [d1 d2]] (if (.after ^Date d1 ^Date d2)
                                                             (reduced false)
                                                             res))
                                         true (map vector (cons x more) more))
                            :cljs (apply <= x more)))
  (-strictly-increasing? [x more] #?(:clj (reduce (fn [res [d1 d2]] (if (.after ^Date d1 ^Date d2)
                                                                      res
                                                                      (reduced false)))
                                                  true (map vector (cons x more) more))
                                     :cljs (apply > x more)))
  (-increasing? [x more] #?(:clj (reduce (fn [res [d1 d2]] (if (.before ^Date d1 ^Date d2)
                                                             (reduced false)
                                                             res))
                                         true (map vector (cons x more) more))
                            :cljs (apply >= x more)))

  #?(:clj Object :cljs object) ;; default
  (-strictly-decreasing? [x more] (reduce (fn [res [v1 s2]] (if (neg? (compare  v1 s2))
                                                              res
                                                              (reduced false)))
                                          true (map vector (cons x more) more)))

  (-decreasing? [x more] (reduce (fn [res [v1 v2]] (if (pos? (compare v1 v2))
                                                     (reduced false)
                                                     res))
                                 true (map vector (cons x more) more)))

  (-strictly-increasing? [x more] (reduce (fn [res [v1 v2]] (if (pos? (compare v1 v2))
                                                              res
                                                              (reduced false)))
                                          true (map vector (cons x more) more)))

  (-increasing? [x more] (reduce (fn [res [v1 v2]] (if (neg? (compare v1 v2))
                                                     (reduced false)
                                                     res))
                                 true (map vector (cons x more) more))))

(defn- lesser? [& args]
  (-strictly-decreasing? (first args) (rest args)))

(defn- lesser-equal? [& args]
  (-decreasing? (first args) (rest args)))

(defn- greater? [& args]
  (-strictly-increasing? (first args) (rest args)))

(defn- greater-equal? [& args]
  (-increasing? (first args) (rest args)))

(defn -min
  ([coll] (reduce (fn [acc x]
                    (if (neg? (compare x acc))
                      x acc))
                  (first coll) (next coll)))
  ([n coll]
   (vec
    (reduce (fn [acc x]
              (cond
                (< (count acc) n)
                (sort compare (conj acc x))
                (neg? (compare x (last acc)))
                (sort compare (conj (butlast acc) x))
                :else acc))
            [] coll))))

(defn -max
  ([coll] (reduce (fn [acc x]
                    (if (pos? (compare x acc))
                      x acc))
                  (first coll) (next coll)))
  ([n coll]
   (vec
    (reduce (fn [acc x]
              (cond
                (< (count acc) n)
                (sort compare (conj acc x))
                (pos? (compare x (first acc)))
                (sort compare (conj (next acc) x))
                :else acc))
            [] coll))))

(def built-ins {'=          =, '== ==, 'not= not=, '!= not=, '< lesser?, '> greater?, '<= lesser-equal?, '>= greater-equal?, '+ +, '- -
                '*          *, '/ /, 'quot quot, 'rem rem, 'mod mod, 'inc inc, 'dec dec, 'max -max, 'min -min
                'zero?      zero?, 'pos? pos?, 'neg? neg?, 'even? even?, 'odd? odd?, 'compare compare
                'rand       rand, 'rand-int rand-int
                'true?      true?, 'false? false?, 'nil? nil?, 'some? some?, 'not not, 'and and-fn, 'or or-fn
                'complement complement, 'identical? identical?
                'identity   identity, 'meta meta, 'name name, 'namespace namespace, 'type type
                'vector     vector, 'list list, 'set set, 'hash-map hash-map, 'array-map array-map
                'count      count, 'range range, 'not-empty not-empty, 'empty? empty, 'contains? contains?
                'str        str, 'pr-str pr-str, 'print-str print-str, 'println-str println-str, 'prn-str prn-str, 'subs subs
                're-find    re-find, 're-matches re-matches, 're-seq re-seq
                '-differ?   -differ?, 'get-else -get-else, 'get-some -get-some, 'missing? -missing?, 'ground identity, 'before? lesser?, 'after? greater?
                'tuple vector, 'untuple identity
                'q q
                'datahike.query/q q
                #'datahike.query/q q
                ;; Namespaced fns the cljs engine cannot resolve dynamically
                ;; (resolve-sym is JVM-only): portable clojure.string built-ins
                ;; keyed by full symbol, exactly like 'datahike.query/q above.
                ;; On the JVM these shadow resolve-sym with the same vars —
                ;; behavior-neutral.
                'clojure.string/upper-case str/upper-case
                'clojure.string/lower-case str/lower-case
                'clojure.string/capitalize str/capitalize
                'clojure.string/starts-with? str/starts-with?
                'clojure.string/ends-with? str/ends-with?
                'clojure.string/includes? str/includes?
                'clojure.string/blank? str/blank?
                'clojure.string/join str/join
                'clojure.string/split str/split
                'clojure.string/trim str/trim
                'clojure.string/replace str/replace
                'datahike.api/q q})

(def clj-core-built-ins
  #?(:clj
     (dissoc (ns-publics 'clojure.core)
             'eval)
     :cljs {}))

;; Register built-ins with execute module for CLJS (breaks circular dep)
#?(:cljs (execute/register-built-ins! built-ins clj-core-built-ins))

(def built-in-aggregates
  (letfn [(sum [coll] (reduce + 0 coll))
          (avg [coll] (/ (sum coll) (count coll)))
          (median
            [coll]
            (let [terms (sort coll)
                  size (count coll)
                  med (bit-shift-right size 1)]
              (cond-> (nth terms med)
                (even? size)
                (-> (+ (nth terms (dec med)))
                    (/ 2)))))
          (variance
            [coll]
            (let [mean (avg coll)
                  sum (sum (for [x coll
                                 :let [delta (- x mean)]]
                             (* delta delta)))]
              (/ sum (count coll))))
          (stddev
            [coll]
            (#?(:cljs js/Math.sqrt :clj Math/sqrt) (variance coll)))]
    {'avg            avg
     'median         median
     'variance       variance
     'stddev         stddev
     'distinct       set
     'min            -min
     'max            -max
     'sum            sum
     'rand           (fn
                       ([coll] (rand-nth coll))
                       ([n coll] (vec (repeatedly n #(rand-nth coll)))))
     'sample         (fn [n coll]
                       (vec (take n (shuffle coll))))
     'count          count
     'count-distinct (fn [coll] (count (distinct coll)))}))

(defn parse-rules [rules]
  (let [rules (if (string? rules) (edn/read-string rules) rules)] ;; for datahike.js interop
    (group-by ffirst rules)))

;; ============================================================================
;; Built-in bitemporal rules
;; ============================================================================
;;
;; Four rules that consumers can use directly in `:where` without
;; passing `:in $ %` rules. They operate over the system attrs
;; `:db.valid/from` / `:db.valid/to` (commit 1 of bitemporal-v1) and
;; degrade gracefully when `:db.valid/to` is absent (treated as +∞).
;;
;; The rules expand to:
;;   1. an AVET pattern on `:db.valid/from`,
;;   2. a `get-else` for `:db.valid/to` with a `9999-12-31` default,
;;   3. two `.compareTo` predicates against the user-provided time-points.
;;
;; *Performance note*: place the rule call EARLY in `:where`. Even
;; with the planner, clause order dominates — the rule's AVET-seek
;; pattern needs to run before downstream patterns that join on the
;; same ?tx. (A future planner pass will auto-reorder; today the
;; convention is "vt-predicate first".)

(def built-in-rules
  "Bitemporal rules pre-installed on every Context. User-supplied
   rules (via `:in $ %`) override built-ins on name collision.

   The `<` `<=` `>` `>=` predicates dispatch on Date via the
   `CollectionOrder` protocol, so the rules can use them directly —
   no `.compareTo` boilerplate."
  (parse-rules
   '[;; valid-at ?tx ?at — half-open membership: vf <= at < vt
     [(valid-at ?tx ?at)
      [?tx :db.valid/from ?vf]
      [(get-else $ ?tx :db.valid/to #inst "9999-12-31T23:59:59.999-00:00") ?vt]
      [(<= ?vf ?at)]
      [(> ?vt ?at)]]
     ;; valid-between ?tx ?from ?to — tx's window intersects [?from, ?to)
     ;; non-empty intersection iff vf < to AND vt > from
     [(valid-between ?tx ?from ?to)
      [?tx :db.valid/from ?vf]
      [(get-else $ ?tx :db.valid/to #inst "9999-12-31T23:59:59.999-00:00") ?vt]
      [(< ?vf ?to)]
      [(> ?vt ?from)]]
     ;; valid-during ?tx ?from ?to — tx's window is CONTAINED in [?from, ?to)
     [(valid-during ?tx ?from ?to)
      [?tx :db.valid/from ?vf]
      [(get-else $ ?tx :db.valid/to #inst "9999-12-31T23:59:59.999-00:00") ?vt]
      [(>= ?vf ?from)]
      [(<= ?vt ?to)]]
     ;; period-overlaps? — Allen-relation alias for valid-between
     [(period-overlaps? ?tx ?from ?to)
      (valid-between ?tx ?from ?to)]

     ;; ----- Allen interval predicates (4-arg, generic) ---------------------
     ;; Each takes two half-open intervals `[?af, ?at)` and `[?bf, ?bt)`
     ;; and asserts the Allen relation. Generic over any orderable type
     ;; the `<` / `<=` / `>` / `>=` predicates dispatch on (java.util.Date
     ;; via CollectionOrder, java.lang.Long, etc.). Use for
     ;; application-domain interval comparisons (lease vs contract dates,
     ;; not just the bitemporal axis). Mirrors stratum's 4-arg SQL
     ;; functions (OVERLAPS, EQUALS_PERIOD, CONTAINS_PERIOD, …).
     [(interval-overlaps? ?af ?at ?bf ?bt)
      [(< ?af ?bt)]
      [(< ?bf ?at)]]
     [(interval-equals? ?af ?at ?bf ?bt)
      [(= ?af ?bf)]
      [(= ?at ?bt)]]
     ;; A contains B: A.from <= B.from AND A.to >= B.to
     [(interval-contains? ?af ?at ?bf ?bt)
      [(<= ?af ?bf)]
      [(>= ?at ?bt)]]
     ;; A strictly contains B: A.from < B.from AND A.to > B.to
     [(interval-strictly-contains? ?af ?at ?bf ?bt)
      [(< ?af ?bf)]
      [(> ?at ?bt)]]
     ;; A precedes B: A's end at-or-before B's start (touching allowed)
     [(interval-precedes? ?af ?at ?bf ?bt)
      [(<= ?at ?bf)]]
     [(interval-strictly-precedes? ?af ?at ?bf ?bt)
      [(< ?at ?bf)]]
     ;; A immediately precedes B (A meets B): A.end == B.from
     [(interval-immediately-precedes? ?af ?at ?bf ?bt)
      [(= ?at ?bf)]]
     ;; A succeeds B: A's start at-or-after B's end (touching allowed)
     [(interval-succeeds? ?af ?at ?bf ?bt)
      [(>= ?af ?bt)]]
     [(interval-strictly-succeeds? ?af ?at ?bf ?bt)
      [(> ?af ?bt)]]
     ;; A immediately succeeds B: A.from == B.to
     [(interval-immediately-succeeds? ?af ?at ?bf ?bt)
      [(= ?af ?bt)]]
     ;; A meets B — alias for interval-immediately-precedes?
     [(interval-meets? ?af ?at ?bf ?bt)
      (interval-immediately-precedes? ?af ?at ?bf ?bt)]
     ;; ----- Allen "anchored-start" / "anchored-end" pairs --------------------
     ;; Allen 1983 names that complete the canonical 13. The 11 above cover
     ;; before/after, meets/met-by (via aliases), overlaps/overlapped-by,
     ;; equals, during/contains. The four below cover starts/started-by and
     ;; finishes/finished-by.
     ;; A starts B: A.from == B.from AND A.to < B.to
     [(interval-starts? ?af ?at ?bf ?bt)
      [(= ?af ?bf)]
      [(< ?at ?bt)]]
     ;; A started-by B: A.from == B.from AND A.to > B.to (inverse of starts?)
     [(interval-started-by? ?af ?at ?bf ?bt)
      [(= ?af ?bf)]
      [(> ?at ?bt)]]
     ;; A finishes B: A.to == B.to AND A.from > B.from
     [(interval-finishes? ?af ?at ?bf ?bt)
      [(= ?at ?bt)]
      [(> ?af ?bf)]]
     ;; A finished-by B: A.to == B.to AND A.from < B.from (inverse of finishes?)
     [(interval-finished-by? ?af ?at ?bf ?bt)
      [(= ?at ?bt)]
      [(< ?af ?bf)]]]))

(def ^:private built-in-rule-names
  "Set of rule-head symbols recognised as built-ins. Used by
   `auto-inject-built-in-rules` to detect when a user's query needs
   `%` injected into `:in`."
  (set (keys built-in-rules)))

(defn- where-uses-built-in-rule?
  "True iff any clause in `where-clauses` is a rule invocation whose
   head names a built-in rule. Rule invocations are SEQs (not vectors)
   whose head is a non-special-form symbol."
  [where-clauses]
  (boolean
   (some (fn [clause]
           (and (seq? clause)
                (not (vector? clause))
                (symbol? (first clause))
                (contains? built-in-rule-names (first clause))))
         where-clauses)))

(defn- auto-inject-built-in-rules
  "If the query uses a built-in rule (`valid-at` etc.) but the user
   did NOT declare `%` in `:in`, synthesise the `%` binding and pass
   `[]` as the corresponding arg — `resolve-in`'s `update :rules
   merge` then leaves built-ins available (since Context's `:rules`
   slot is pre-seeded with `built-in-rules`). Transparent to the
   caller."
  [{:keys [query args] :as input}]
  (let [where (:where query)]
    (if (and (where-uses-built-in-rule? where)
             ;; `%` inside `#(...)` is the anonymous fn's first param, so
             ;; `#(= '% %)` would compare `'%` to itself on every call;
             ;; use an explicit fn to test for the literal `%` symbol.
             (not (some (fn [x] (= '% x)) (:in query))))
      (let [in (or (:in query) '[$])
            new-in (vec (conj (vec in) '%))
            ;; Build the flat rule-defs vec from the parsed map.
            built-in-defs (vec (mapcat val built-in-rules))]
        (-> input
            (assoc-in [:query :in] new-in)
            ;; `args` may be a list (from `& varargs` in `q`); `conj` on
            ;; a list prepends — vectorize first so we always append.
            (update :args (fn [a] (conj (vec (or a [])) built-in-defs)))))
      input)))

(defn empty-rel [binding]
  (let [vars (->> (dpi/collect-vars-distinct binding)
                  (map :symbol))]
    (rel/->Relation (zipmap vars (range)) [])))

(defprotocol IBinding
  (in->rel [binding value]))

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (prod-rel))

  BindScalar
  (in->rel [binding value]
    (rel/->Relation {(get-in binding [:variable :symbol]) 0} [(into-array [value])]))

  BindColl
  (in->rel [binding coll]
    (cond
      (not (seqable? coll))
      (log/raise "Cannot bind value " coll " to collection " (dpi/get-source binding)
                 {:error :query/binding, :value coll, :binding (dpi/get-source binding)})
      (empty? coll)
      (empty-rel binding)
      :else
      (->> coll
           (map #(in->rel (:binding binding) %))
           (reduce sum-rel))))

  BindTuple
  (in->rel [binding coll]
    (cond
      (not (seqable? coll))
      (log/raise "Cannot bind value " coll " to tuple " (dpi/get-source binding)
                 {:error :query/binding, :value coll, :binding (dpi/get-source binding)})
      (< (count coll) (count (:bindings binding)))
      (log/raise "Not enough elements in a collection " coll " to bind tuple " (dpi/get-source binding)
                 {:error :query/binding, :value coll, :binding (dpi/get-source binding)})
      :else
      (reduce prod-rel
              (map #(in->rel %1 %2) (:bindings binding) coll)))))

(defn resolve-in [context [binding value]]
  (cond
    (and (instance? BindScalar binding)
         (instance? SrcVar (:variable binding)))
    (update context :sources assoc (get-in binding [:variable :symbol]) value)
    (and (instance? BindScalar binding)
         (instance? RulesVar (:variable binding)))
    ;; Merge user-supplied rules over the built-ins so consumers can
    ;; redefine `valid-at` etc. if they need to.
    (update context :rules merge (parse-rules value))
    (and (instance? BindScalar binding)
         (instance? Variable (:variable binding)))
    (assoc-in context [:consts (get-in binding [:variable :symbol])] value)
    #_(instance? BindColl binding)                          ;; TODO: later
    :else
    (update context :rels conj (in->rel binding value))))

(defn resolve-ins [context bindings values]
  (reduce resolve-in context (zipmap bindings values)))

;; Join environment: {:source db :lookup-attrs #{..}} threaded explicitly
;; (formerly the dynamic vars rel/*implicit-source* / rel/*lookup-attrs*,
;; whose per-tuple reads inside getter-fn closures cannot survive a
;; partial-cps suspension on cljs). The relational algebra lives in
;; datahike.query.relation; `:join-source` on the context mirrors exactly
;; the old *implicit-source* binding discipline.

(defn- join-env [context]
  {:source (:join-source context)})

(defn var-mapping [pattern indices]
  (->> (map vector pattern indices)
       (filter (fn [[s _]] (free-var? s)))
       (into {})))

(defn map-consts [context orig-pattern datoms]
  (let [;; Create a map from free var to index
        ;; for the positions in the pattern
        attr->idx (var-mapping orig-pattern (range))
        idx->const (reduce-kv (fn [m k v]
                                (if-let [c (k (:consts context))]
                                  (if (= c (get (first datoms) v)) ;; All datoms have the same format and the same value at position v
                                    m ;; -> avoid unnecessary translations
                                    (assoc m v c))
                                  m))
                              {}
                              attr->idx)]
    (when (seq idx->const)
      (rel/->Relation attr->idx (map #(reduce (fn [datom [k v]]
                                                (assoc datom k v))
                                              (vec (seq %))
                                              idx->const)
                                     datoms)))))

(defn replace-symbols-by-nil [pattern]
  (mapv #(if (symbol? %) nil %) pattern))

(defn resolve-pattern-eid [db search-pattern]
  (let [first-p (first search-pattern)]
    (if (and (some? first-p)
             (not (symbol? first-p)))
      (when-let [eid (dbu/entid db first-p)]
        (assoc search-pattern 0 eid))
      search-pattern)))

(defn relation-from-datoms-xform []
  (comp (map #?(:cljs (fn [datom]
                        (if (and (some? datom)
                                 (or (instance? datahike.datom/Datom datom)
                                     (.-e datom)))
                          (let [e (.-e ^datahike.datom.Datom datom)
                                a (.-a ^datahike.datom.Datom datom)
                                a-kw (keyword (.-fqn ^clj a))  ; Extract from Keyword object
                                v (.-v ^datahike.datom.Datom datom)]
                            ;; tx is sign-encoded with the added flag (retraction => negative
                            ;; raw .-tx). Project the ABS tx (datom-tx) and read added from the
                            ;; sign (datom-added) — using raw .-tx would give retractions a
                            ;; negative ?t that no [?t :db/txInstant] join could ever match.
                            #js [e a-kw v (datom/datom-tx datom) (datom/datom-added datom)])  ; JS array for goog.array/get
                          datom))
                :clj (fn [[e a v tx added?]]
                       [e a v tx added?])))
        (distinct-tuples)))

(defn relation-from-datoms [context orig-pattern datoms]
  (or (map-consts context orig-pattern datoms)
      #?(:cljs (let [converted (mapv (fn [d]
                                       (if (and (some? d)
                                                (or (instance? datahike.datom/Datom d)
                                                    (.-e d)))
                                         (let [a (.-a ^datahike.datom.Datom d)
                                               a-kw (keyword (.-fqn ^clj a))]
                                           ;; abs tx + sign-derived added — see
                                           ;; relation-from-datoms-xform.
                                           #js [(.-e ^datahike.datom.Datom d)
                                                a-kw
                                                (.-v ^datahike.datom.Datom d)
                                                (datom/datom-tx d)
                                                (datom/datom-added d)])
                                         d))
                                     datoms)]
                 (rel/->Relation (var-mapping orig-pattern (range))
                                 converted))
         :clj (rel/->Relation (var-mapping orig-pattern (range))
                              datoms))))

(defn matches-pattern? [pattern tuple]
  (loop [tuple tuple
         pattern pattern]
    (if (and tuple pattern)
      (let [t (first tuple)
            p (first pattern)]
        (if (or (symbol? p) (= t p))
          (recur (next tuple) (next pattern))
          false))
      true)))

(defn lookup-pattern-coll [coll pattern orig-pattern]
  (let [attr->idx (var-mapping orig-pattern (range))
        data (filter #(matches-pattern? pattern %) coll)]
    (rel/->Relation attr->idx (mapv to-array data))))            ;; FIXME to-array

(defn- rel-with-attr [context sym]
  (some #(when (contains? (:attrs %) sym) %) (:rels context)))

(defn- context-resolve-val [context sym]
  (if-let [replacement (get (:consts context) sym)]
    replacement
    (when-some [rel (rel-with-attr context sym)]
      (when-some [tuple (first (:tuples rel))]
        (get tuple ((:attrs rel) sym))))))

(defn- rel-contains-attrs? [rel attrs]
  (some #(contains? (:attrs rel) %) attrs))

(defn- rel-prod-by-attrs [context attrs]
  (let [rels (filter #(rel-contains-attrs? % attrs) (:rels context))
        production (reduce prod-rel rels)]
    [(update context :rels #(remove (set rels) %)) production]))

(defn -call-fn [context rel f args]
  (let [sources (:sources context)
        attrs (:attrs rel)
        len (count args)
        static-args (da/make-array len)
        tuples-args (da/make-array len)]
    (dotimes [i len]
      (let [arg (nth args i)]
        (if (symbol? arg)
          (if-let [const (get (:consts context) arg)]
            (da/aset static-args i const)
            (if-some [source (get sources arg)]
              (da/aset static-args i source)
              (da/aset tuples-args i (get attrs arg))))
          ;; (quote x) args unwrap to their constant — the only seq form
          ;; allowed by analyze/check-fn-args. All other non-symbol args
          ;; pass verbatim (vectors/maps/sets are data literals).
          (da/aset static-args i (if (analyze/quote-form? arg) (second arg) arg)))))
    (fn [tuple]
      ;; TODO raise if not all args are bound
      (dotimes [i len]
        (when-some [tuple-idx (aget tuples-args i)]
          (let [v (get tuple tuple-idx)]
            (da/aset static-args i v))))
      (apply f static-args))))

(defn- resolve-sym [#?(:clj sym :cljs _)]
  #?(:cljs nil
     :clj (when (namespace sym)
            (when-some [v (resolve sym)] @v))))

#?(:clj (def ^:private find-method
          (memoize
           (fn find-method-impl [^Class this-class method-name args-classes]
             (or (->> this-class
                      .getMethods
                      (some (fn [^Method method]
                              (when (and (= method-name (.getName method))
                                         (= (count args-classes)
                                            (.getParameterCount method))
                                         (every? true? (map #(Reflector/paramArgTypeMatch %1 %2)
                                                            (.getParameterTypes method)
                                                            args-classes)))
                                method))))
                 (throw (ex-info (str (.getName this-class) "."
                                      method-name "("
                                      (str/join "," (map #(.getName ^Class %) args-classes))
                                      ") not found")
                                 {:this-class this-class
                                  :method-name method-name
                                  :args-classes args-classes})))))))

(defn- resolve-method [#?(:clj method-sym :cljs _)]
  #?(:cljs nil
     :clj (let [method-str (name method-sym)]
            (when (= \. (.charAt method-str 0))
              (let [method-name (subs method-str 1)]
                (fn [this & args]
                  (let [^Method method (find-method (class this) method-name (mapv class args))]
                    (Reflector/prepRet (.getReturnType method) (.invoke method this (into-array Object args))))))))))

(defn filter-by-pred [context clause]
  (let [[[f & args]] clause
        _ (analyze/check-fn-args clause args)
        pred (or (get built-ins f)
                 (get clj-core-built-ins f)
                 (context-resolve-val context f)
                 (resolve-sym f)
                 (resolve-method f)
                 (when (nil? (rel-with-attr context f))
                   (log/raise "Unknown predicate '" f " in " clause
                              {:error :query/where, :form clause, :var f})))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        new-rel (if pred
                  (let [tuple-pred (-call-fn context production pred args)
                        safe-pred (fn [tuple]
                                    ;; absorbs user-pred TYPE errors only — a
                                    ;; storage fault or cancellation must escape
                                    ;; (dropped tuples would be cached as truth)
                                    (try (tuple-pred tuple)
                                         #?(:clj (catch ClassCastException _ false)
                                            :cljs (catch :default e
                                                    (if (dt/rethrowable? e) (throw e) false)))
                                         #?(:clj (catch IllegalArgumentException _ false))))]
                    (update production :tuples #(filter safe-pred %)))
                  (assoc production :tuples []))]
    (update context :rels conj new-rel)))

(defn bind-by-fn [context clause]
  (let [[[f & args] out] clause
        _ (analyze/check-fn-args clause args)
        binding (dpi/parse-binding out)
        fun (or (get built-ins f)
                (get clj-core-built-ins f)
                (context-resolve-val context f)
                (resolve-sym f)
                (resolve-method f)
                (when (nil? (rel-with-attr context f))
                  (log/raise "Unknown function '" f " in " clause
                             {:error :query/where, :form clause, :var f})))
        attrs (filter symbol? args)
        [context production] (rel-prod-by-attrs context attrs)
        symbols-with-values (into #{}
                                  (mapcat keys)
                                  [(:attrs production)
                                   (:consts context)
                                   (:sources context)])]
      ;; Currently, we can only evaluate this clause if all variables
      ;; in the function call are bound. If not, we return nil which
      ;; is handled by `datahike.tools/resolve-clauses`.
    (when (every? symbols-with-values attrs)
      ;; Engine-level function-invocation count (atom-free): the fn is applied
      ;; once per production tuple, so invocations = (count (:tuples production)).
      ;; Accumulated into the threaded context by fn-symbol; surfaced as result
      ;; metadata. Gated on :count-fns? so normal queries pay nothing.
      (let [context (cond-> context
                      (:count-fns? context)
                      (update :fn-counts (fn [m] (update (or m {}) f (fnil + 0)
                                                         (count (:tuples production))))))
            new-rel (if fun
                      (let [tuple-fn (-call-fn context production fun args)
                            rels (for [tuple (:tuples production)
                                       :let [val (tuple-fn tuple)]
                                       :when (not (nil? val))]
                                   (prod-rel (rel/->Relation (:attrs production) [tuple])
                                             (in->rel binding val)))]
                        (if (empty? rels)
                          (prod-rel production (empty-rel binding))
                          (reduce sum-rel rels)))
                      (prod-rel (assoc production :tuples []) (empty-rel binding)))
            idx->const (reduce-kv (fn [m k v]
                                    (if-let [c (k (:consts context))]
                                      (assoc m v c) ;; different value at v for each tuple
                                      m))
                                  {}
                                  (:attrs new-rel))]
        (if (empty? (:tuples new-rel))
          (update context :rels rel/collapse-rels new-rel (join-env context))
          (-> context ;; filter output binding
              (update :rels rel/collapse-rels
                      (update new-rel
                              :tuples
                              #(filter (fn [tuple]
                                         (every? (fn [[ind c]]
                                                   (= c (get tuple ind)))
                                                 idx->const))
                                       %))
                      (join-env context))))))))

;;; RULES

(defn rule? [context clause]
  (and (sequential? clause)
       (contains? (:rules context)
                  (if (source? (first clause))
                    (second clause)
                    (first clause)))))

(def rule-seqid (atom 0))

#?(:clj
   (defmacro some-of
     ([] nil)
     ([x] x)
     ([x & more]
      `(let [x# ~x] (if (nil? x#) (some-of ~@more) x#)))))

(defn expand-rule [clause context _used-args]
  (let [[rule & call-args] clause
        seqid (swap! rule-seqid inc)
        branches (get (:rules context) rule)
        call-args-new (map #(if (free-var? %) % (symbol (str "?__auto__" %2)))
                           call-args
                           (range))
        consts (->> (map vector call-args-new call-args)
                    (filter (fn [[new old]] (not= new old)))
                    (into {}))]
    [(for [branch branches
           :let [[[_ & rule-args] & clauses] branch
                 replacements (zipmap rule-args call-args-new)]]
       (walk/postwalk
        #(if (free-var? %)
           (some-of
            (replacements %)
            (symbol (str (name %) "__auto__" seqid)))
           %)
        clauses))
     consts]))

(defn remove-pairs [xs ys]
  (let [pairs (->> (map vector xs ys)
                   (remove (fn [[x y]] (= x y))))]
    [(map first pairs)
     (map second pairs)]))

(defn rule-gen-guards [rule-clause used-args]
  (let [[rule & call-args] rule-clause
        prev-call-args (get used-args rule)]
    (for [prev-args prev-call-args
          :let [[call-args prev-args] (remove-pairs call-args prev-args)]]
      [(concat ['-differ?] call-args prev-args)])))

(defn walk-collect [form pred]
  (let [res (atom [])]
    (walk/postwalk #(do (when (pred %) (swap! res conj %)) %) form)
    @res))

(defn collect-vars [clause]
  (set (walk-collect clause free-var?)))

(defn split-guards [clauses guards]
  (let [bound-vars (collect-vars clauses)
        pred (fn [[[_ & vars]]] (every? bound-vars vars))]
    [(filter pred guards)
     (remove pred guards)]))

(defn solve-rule [context clause]
  (let [final-attrs (filter free-var? clause)
        final-attrs-map (zipmap final-attrs (range))
        stats? (:stats context)
        ;;         clause-cache    (atom {}) ;; TODO
        solve (fn [prefix-context clause clauses]
                (if stats?
                  (dqs/update-ctx-with-stats prefix-context clause
                                             (fn [ctx]
                                               (let [tmp-context (dt/resolve-clauses
                                                                  -resolve-clause
                                                                  (assoc ctx :stats [])
                                                                  clauses)]
                                                 (assoc tmp-context
                                                        :stats (:stats ctx)
                                                        :tmp-stats {:type :solve
                                                                    :clauses clauses
                                                                    :branches (:stats tmp-context)}))))
                  (dt/resolve-clauses -resolve-clause prefix-context clauses)))
        empty-rels? (fn [ctx]
                      (some #(empty? (:tuples %)) (:rels ctx)))]
    (loop [stack (list {:prefix-clauses []
                        :prefix-context context
                        :clauses [clause]
                        :used-args {}
                        :pending-guards {}
                        :clause clause})
           rel (rel/->Relation final-attrs-map [])
           tmp-stats []]
      (if-some [frame (first stack)]
        (let [[clauses [rule-clause & next-clauses]] (split-with #(not (rule? context %)) (:clauses frame))]
          (if (nil? rule-clause)

            ;; no rules -> expand, collect, sum
            (let [prefix-context (solve (:prefix-context frame) (:clause frame) clauses)
                  tuples (-collect prefix-context final-attrs)
                  new-rel (rel/->Relation final-attrs-map tuples)
                  new-stats (conj tmp-stats (last (:stats prefix-context)))]
              (recur (next stack) (sum-rel rel new-rel) new-stats))

            ;; has rule -> add guards -> check if dead -> expand rule -> push to stack, recur
            (let [[rule & call-args] rule-clause
                  guards (rule-gen-guards rule-clause (:used-args frame))
                  [active-gs pending-gs] (split-guards (concat (:prefix-clauses frame) clauses)
                                                       (concat guards (:pending-guards frame)))]
              (if (some #(= % '[(-differ?)]) active-gs)     ;; trivial always false case like [(not= [?a ?b] [?a ?b])]

                ;; this branch has no data, just drop it from stack
                (recur (next stack) rel tmp-stats)

                (let [prefix-clauses (concat clauses active-gs)
                      prefix-context (solve (:prefix-context frame) (:clause frame) prefix-clauses)
                      new-stats (conj tmp-stats (last (:stats prefix-context)))]
                  (if (empty-rels? prefix-context)

                    ;; this branch has no data, just drop it from stack
                    (recur (next stack) rel new-stats)

                    ;; need to expand rule to branches
                    (let [used-args (assoc (:used-args frame) rule
                                           (conj (get (:used-args frame) rule []) call-args))
                          [branches rule-consts] (expand-rule rule-clause context used-args)]
                      (recur (concat
                              (for [branch branches]
                                {:prefix-clauses prefix-clauses
                                 :prefix-context (update prefix-context :consts merge rule-consts)
                                 :clauses (concatv branch next-clauses)
                                 :used-args used-args
                                 :pending-guards pending-gs
                                 :clause branch})
                              (next stack))
                             rel
                             new-stats))))))))
        (cond-> (update context :rels rel/collapse-rels rel (join-env context))
          stats? (assoc :tmp-stats {:type :rule
                                    :branches tmp-stats}))))))

(defn resolve-pattern-lookup-entity-id [source e error-code]
  (cond
    (dbu/numeric-entid? e) e
    (or (lookup-ref? e) (attr? e)) (dbu/entid-strict source e error-code)
                                        ;(entid? e) e
    (keyword? e) e
    (symbol? e) e
    :else (or error-code (log/raise "Invalid entid" {:error :entity-id/syntax :entity-id e}))))

(defn resolve-pattern-lookup-refs
  "Translate pattern entries before using pattern for database search"
  ([source pattern] (resolve-pattern-lookup-refs source pattern nil))
  ([source pattern error-code]
   (if (dbu/db? source)
     (dt/with-destructured-vector pattern
       e (resolve-pattern-lookup-entity-id source e error-code)
       a (if (and (:attribute-refs? (dbi/-config source)) (keyword? a))
           (dbi/-ref-for source a)
           a)
       v (if (and v (attr? a) (dbu/ref? source a) (or (lookup-ref? v) (attr? v)))
           (dbu/entid-strict source v error-code)
           v)
       tx (if (lookup-ref? tx)
            (dbu/entid-strict source tx error-code)
            tx)
       added added)
     pattern)))

(defn good-lookup-refs? [pattern]
  (if (coll? pattern)
    (not-any? #(= % ::error) pattern)
    (not= ::error pattern)))

(defn resolve-pattern-lookup-refs-or-nil
  "This function works just like `resolve-pattern-lookup-refs` but if there is an error it returns `nil` instead of throwing an exception. This is used to reject patterns with variables substituted for invalid values.

  For instance, take the query

  (d/q '[:find ?e
      :in $ [?e ...]
      :where [?e :friend 3]]
     db [1 2 3 \"A\"])

  in the test `datahike.test.lookup-refs-test/test-lookup-refs-query`.

  According to this query, the variable `?e` can be either `1`, `2`, `3` or `\"A\"`
  but \"A\" is not a valid entity.

  The query engine will evaluate the pattern `[?e :friend 3]`. For the strategies
  `identity` and `select-simple`, no substitution will be performed in this pattern.
  Instead, they will ask for all tuples from the database and then filter them, so
  the fact that `?e` can be bound to an impossible entity id `\"A\"` is not a problem.

  But with the strategy `select-all`, the substituted pattern will become 

  [\"A\" :friend 3]

  and consequently, the `result` below will take the value `[::error :friend 3]`.
  The unit test is currently written to simply ignore illegal illegal entity ids
  such as \"A\" and therefore, we handle that by letting this function return nil
  in those cases.
  "
  [source pattern]
  (let [result (resolve-pattern-lookup-refs source pattern ::error)]
    (when (good-lookup-refs? result)
      result)))

(defn dynamic-lookup-attrs [source pattern]
  (let [[e a v tx] pattern]
    (cond-> #{}
      (free-var? e) (conj e)
      (free-var? tx) (conj tx)
      (and
       (free-var? v)
       (not (free-var? a))
       (dbu/ref? source a)) (conj v))))

(defn limit-rel [rel vars]
  (when-some [attrs' (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs')))

(defn limit-context [context vars]
  (assoc context
         :rels (->> (:rels context)
                    (keep #(limit-rel % vars)))))

(defn- ctx-bound-vars [context]
  (set (concat (mapcat #(keys (:attrs %)) (:rels context))
               (keys (:consts context)))))

(defn all-bound?
  "True iff every var in `vars` is currently bound in `context`."
  [context vars]
  (set/subset? (set vars) (ctx-bound-vars context)))

(defn some-bound?
  "True iff at least one var in `vars` is currently bound in `context`."
  [context vars]
  (boolean (seq (set/intersection (set vars) (ctx-bound-vars context)))))

(defn check-all-bound [context vars form]
  (let [bound (ctx-bound-vars context)]
    (when-not (set/subset? vars bound)
      (let [missing (set/difference (set vars) bound)]
        (log/raise "Insufficient bindings: " missing " not bound in " form
                   {:error :query/where
                    :form form
                    :vars missing})))))

(defn check-some-bound [context vars form]
  (when (empty? (set/intersection vars (ctx-bound-vars context)))
    (log/raise "Insufficient bindings: none of " vars " is bound in " form
               {:error :query/where
                :form form})))

(defn resolve-context [context clauses]
  (dt/resolve-clauses resolve-clause context clauses))

(def rel-product-unit (rel/->Relation {} [[]]))

(defn bound-symbol-map
  "Given a sequential collection of relations, return a map where every key is a symbol of a variable and every value is a map with the keys `:relation-index` and `:tuple-element-index`. The key `:relation-index` is associated with the index of the relation where the variable occurs and the key `:tuple-element-index` is associated with the index of the location in the clause where the symbol occurs."
  [rels]
  (into {} (for [[rel-index rel] (map-indexed vector rels)
                 [sym tup-index] (:attrs rel)]
             [sym {:relation-index rel-index
                   :tuple-element-index tup-index}])))

(defn normalize-pattern
  "Takes a pattern and returns a new pattern with exactly five elements, filling in any missing ones with nil."
  [[e a v tx added?]]
  [e a v tx added?])

(defn replace-unbound-symbols-by-nil [bsm pattern]
  (normalize-pattern
   (mapv #(when-not (and (symbol? %) (not (contains? bsm %)))
            %)
         pattern)))

(defn search-index-mapping
  "Returns a sequence of maps with index-information for a subset of e, a, v, tx. The `strategy-vec` argument is a vector of four elements corresponding to e, a, v, tx respectively. Every such element can be either `:substitute`, `:filter` or `nil` depending on how the corresponding element in the pattern should be used. The `clean-pattern` is argument is a vector with the elements corresponding to e, a, v, tx. The argument `selected-strategy-symbol` can be either `:substitute`, `:filter` or `nil` and is used to filter e, a, v, tx based on the value of `:strategy-vec`."
  [{:keys [strategy-vec clean-pattern bsm]}
   selected-strategy-symbol]
  {:pre [(= 4 (count strategy-vec))]}
  (let [pattern (normalize-pattern clean-pattern)]
    (for [[pattern-element-index
           pattern-var
           strategy-symbol] (map vector (range) pattern strategy-vec)
          :when (= selected-strategy-symbol strategy-symbol)
          :let [m (bsm pattern-var)]
          :when m]
      (assoc m :pattern-element-index pattern-element-index))))

(defn substitution-relation-indices
  "Returns the set of indices of relations that have symbols that are substituted for actual values in the pattern before index lookup."
  [context]
  (into #{}
        (map :relation-index)
        (search-index-mapping context :substitute)))

(defn filtering-relation-indices
  "Returns the set of indices of relations that have symbols that will be used for filtering the datoms returned from the inex lookup."
  [context subst-inds]
  (into #{}
        (comp (map :relation-index)
              (remove subst-inds))
        (search-index-mapping context :filter)))

(defn index-feature-extractor
  "Given a set of indices referring to elements in a sequential container such as a datom or vector, construct a function that returns a value computed from such a sequential container such that two different values returned from that function are equal if and only if their corresponding values at those indices are equal. Optionally takes a function that can remap the selected elements."
  ([inds include-empty?]
   (index-feature-extractor inds include-empty? (fn [_ x] x)))
  ([inds include-empty? replacer]
   (let [first-index (first inds)]
     (case (count inds)
       0 (when include-empty?
           (fn
             ([] [nil])
             ([_] nil)))
       1 (fn
           ([] [first-index])
           ([x] (wrap-comparable (replacer first-index (nth x first-index)))))
       (fn
         ([] inds)
         ([x]
          (mapv #(wrap-comparable (replacer % (nth x %))) inds)))))))

(defn extend-predicate1 [predicate feature-extractor ref-feature]
  (if (nil? feature-extractor)
    predicate
    (if predicate
      (fn [datom]
        (let [feature (feature-extractor datom)]
          (if (= ref-feature feature)
            (predicate datom)
            false)))
      (fn [datom]
        (= ref-feature (feature-extractor datom))))))

(defn predicate-from-set [s]
  (case (count s)
    0 (fn [_] false)
    1 (let [y (first s)]
        (fn [x] (= x y)))
    (fn [x] (contains? s x))))

(defn extend-predicate [predicate feature-extractor features]
  {:pre [#?(:clj (or (set? features) (instance? HashSet features))
            :cljs (set? features))]}
  (let [this-pred (predicate-from-set features)]
    (if (nil? feature-extractor)
      predicate
      (if predicate
        (fn
          ([] (conj (predicate) [(feature-extractor) features]))
          ([datom]
           (let [feature (feature-extractor datom)]
             (if (this-pred feature)
               (predicate datom)
               false))))
        (fn
          ([] [(feature-extractor) features])
          ([datom]
           (this-pred (feature-extractor datom))))))))

(defn resolve-pattern-lookup-ref-at-index
  [source clean-attribute pattern-index pattern-value error-code]
  (let [a clean-attribute]
    (case (int pattern-index)
      0 (resolve-pattern-lookup-entity-id source pattern-value error-code)
      1 (if (and (:attribute-refs? (dbi/-config source)) (keyword? pattern-value))
          (dbi/-ref-for source pattern-value)
          pattern-value)
      2 (if (and pattern-value
                 (attr? a)
                 (dbu/ref? source a)
                 (or (lookup-ref? pattern-value) (attr? pattern-value)))
          (dbu/entid-strict source pattern-value error-code)
          pattern-value)
      3 (if (lookup-ref? pattern-value)
          (dbu/entid-strict source pattern-value error-code)
          pattern-value)
      4 pattern-value
      ;; Indices 0-4 are the datom positions (e, a, v, tx, added). A larger
      ;; index reaches here only when the caller passes a `:tuple-element-index`
      ;; — a var's position in a WIDE relation tuple (e.g. a multi-source query
      ;; whose driving relation joined several entity-groups) — rather than a
      ;; pattern position. Such a position is not a datom slot, so it can never
      ;; be a resolvable pattern lookup-ref: return the value unchanged.
      ;; (Previously this threw "No matching clause: N", crashing the legacy
      ;; engine on variable-attribute cross-source patterns.)
      pattern-value)))

(defn lookup-ref-replacer
  ([context] (lookup-ref-replacer context ::error))
  ([{:keys [source clean-pattern]} error-value]
   (let [[_ attribute _ _] clean-pattern]
     (if source
       (if (dbu/db? source)
         (fn [index pattern-value]
           (resolve-pattern-lookup-ref-at-index source
                                                attribute
                                                index
                                                pattern-value
                                                error-value))
         (fn [_i x] x))
       (fn [_ x] x)))))

(defn- generate-substitution-xform-code [pred-expr
                                         datom-predicate-symbol
                                         filter-feature-symbol
                                         pmask
                                         substituted-pattern-and-filter-feature-pairs]
  (let [pattern-symbols (repeatedly 5 gensym)
        substitution-value-vector (gensym "substitution-value-vector")]
    `(fn [step#]
       (fn
         ([] (step#))
         ([dst-one#] (step# dst-one#))

         ;; This is a higher-arity step function.
         ([dst# ~@pattern-symbols ~datom-predicate-symbol]

          ;; This generates the code that substitutes some of the
          ;; incomping values by values from the relation and calls
          ;; the next step function in the transducer chain.
          (reduce
           (fn [dst-inner# [~substitution-value-vector ~filter-feature-symbol]]
             (step# dst-inner#
                    ~@(map (fn [i sym]
                             (if (nil? i)
                               sym
                               `(nth ~substitution-value-vector ~i)))
                           pmask
                           pattern-symbols)
                    ~pred-expr))
           dst#
           ~substituted-pattern-and-filter-feature-pairs))))))

(defmacro substitution-expansion [substitution-pattern-element-inds
                                  filter-feature-extractor
                                  substituted-pattern-and-filter-feature-pairs]
  (let [datom-predicate-symbol (gensym)
        filter-feature-symbol (gensym)]

    ;; This code generates a tree of `if`-forms for all ordered subsets of
    ;; the sequence `(range 5)` that `substitution-pattern-element-inds`.
    ;; can take. At each leaf of the tree, code is generated for that particular
    ;; subset.
    (dt/range-subset-tree
     5
     substitution-pattern-element-inds

     ;; This function is called at each leaf of the tree.
     ;; `pmast` is a boolean sequence
     (fn [_pinds pmask]

       ;; `branch-expr` is a function that generates the actual
       ;; code given a predicate expression.
       (let [branch-expr (fn [pred-expr]

                           ;; This is the code for the transducer.
                           (generate-substitution-xform-code
                            pred-expr
                            datom-predicate-symbol
                            filter-feature-symbol
                            pmask
                            substituted-pattern-and-filter-feature-pairs))]

         ;; Generate different code depending on whether or not there is a
         ;; `filt-extractor`, meaning that the resulting datoms have to be
         ;; filtered.
         `(if (nil? ~filter-feature-extractor)
            ~(branch-expr datom-predicate-symbol)
            ~(branch-expr `(extend-predicate1 ~datom-predicate-symbol
                                              ~filter-feature-extractor
                                              ~filter-feature-symbol))))))))

#_(instantiate-substitution-xform substitution-pattern-element-inds
                                  filter-feature-extractor
                                  substituted-pattern-and-filter-feature-pairs)

(defn instantiate-substitution-xform [substitution-pattern-element-inds
                                      filter-feature-extractor
                                      substituted-pattern-and-filter-feature-pairs]

  ;; Returns a transducer based on the indices in `substitution-pattern-element-inds`
  (substitution-expansion substitution-pattern-element-inds
                          filter-feature-extractor
                          substituted-pattern-and-filter-feature-pairs))

;; The performance improvement of using this macro has been measured,
;; see comment in single-substition-xform.
(defmacro make-vec-lookup-ref-replacer [range-length]
  (let [inds (gensym)
        replacer (gensym)
        tuple (gensym)
        ex-sym# (if (get-in &env [:ns])
                  'js/Error
                  Exception)]
    `(fn tree-fn# [~replacer ~inds]
       ~(dt/range-subset-tree
         range-length inds
         (fn replacer-fn# [pinds _mask]
           `(fn [~tuple]
              (try
                ~(mapv (fn [index i] `(~replacer ~index (nth ~tuple ~i)))
                       pinds
                       (range))
                (catch ~ex-sym# e#
                  (if (datahike.tools/rethrowable? e#) (throw e#) nil)))))))))

(def vec-lookup-ref-replacer (make-vec-lookup-ref-replacer 5))

;; The performance improvement of using this macro has been measured,
;; see comment in single-substition-xform.
(defmacro basic-index-selector [max-length]
  (let [inds (gensym)

        obj (gensym)]
    `(fn [~inds]
       (case (count ~inds)
         ~@(mapcat (fn [length]
                     (let [index-symbols (vec (repeatedly length gensym))]
                       [length `(let [~index-symbols ~inds]
                                  (fn [~obj]
                                    ~(mapv
                                      (fn [sym] `(nth ~obj ~sym))
                                      index-symbols)))]))
                   (range (inc max-length)))))))

(def make-basic-index-selector (basic-index-selector 5))

(defn single-substitution-xform
  "Returns a transducer that substitutes the symbols for a single relation."
  [search-context
   relation-index
   substituted-vars-per-relation
   filtered-vars-per-relation]
  (let [;; This function maps the value at a pattern at a certain index to
        ;; a new value where the lookup-ref has been replaced. If there is an
        ;; error, it returns the `::error` value.
        lrr (lookup-ref-replacer search-context)

        tuples (:tuples (nth (:rels search-context) relation-index))
        substituted-vars (substituted-vars-per-relation relation-index)
        filtered-vars (filtered-vars-per-relation relation-index)
        pattern-substitution-inds (map :tuple-element-index substituted-vars)
        pattern-filter-inds (map :tuple-element-index filtered-vars)

        ;; The filter feature-extractor reads a var's value from the RELATION
        ;; TUPLE (via `:tuple-element-index`) but `lrr`
        ;; (resolve-pattern-lookup-ref-at-index) resolves lookup-refs by the
        ;; var's PATTERN position (e/a/v/tx/added). On a wide multi-source
        ;; relation the two indices differ, so the raw tuple index would pick
        ;; the wrong resolution case (and, past index 4, formerly crashed).
        ;; Translate tuple-index → pattern-index before resolving.
        tuple-idx->pattern-idx (into {} (map (juxt :tuple-element-index :pattern-element-index))
                                     filtered-vars)
        lrr-by-tuple-idx (fn [tuple-idx v]
                           (lrr (get tuple-idx->pattern-idx tuple-idx tuple-idx) v))

        ;; This function returns a unique feature for the values at
        ;; `pattern-filter-inds` given a pattern.
        feature-extractor (index-feature-extractor pattern-filter-inds
                                                   true
                                                   lrr-by-tuple-idx)

        ;; These are the indices of the locations in the pattern that will be substituted
        ;; with values from the tuples in this relation.
        substitution-pattern-element-inds (map :pattern-element-index substituted-vars)

        ;; This function maps the value at a pattern at a certain index to
        ;; a new value where the lookup-ref has been replaced. If there is an error,
        ;; an exception is thrown.
        lrr-ex (lookup-ref-replacer search-context nil)

        ;; This constructs a new pattern given a tuple of values that will be inserted
        ;; at the `substitution-pattern-element-inds`.
        ;;
        ;; Precomputing this function moves some work out of the loop
        ;; and contributes to about 1½ seconds reduction in
        ;; https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/experimental/datahike-benchmark/
        pattern-from-tuple (vec-lookup-ref-replacer lrr-ex substitution-pattern-element-inds)

        ;; This is a function that simply picks out a subset of the elements from a sequential
        ;; collection, at the indices `pattern-subsitution-inds`.
        ;;
        ;; Precomputing this function moves some work out of the loop
        ;; and contributes to about 2 seconds reduction in
        ;; https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/experimental/datahike-benchmark/
        select-pattern-substitution-inds (make-basic-index-selector pattern-substitution-inds)

        ;; This is a list of pairs such that:
        ;;
        ;; * The first element is a pattern where variables for this relation have been substituted.
        ;; * The second element is a feature used for filtering the datoms after querying the backend
        ;;
        ;; Using a transducer here (with a transient vector under the hood)
        ;; is about ½ second faster than a doseq-loop that accumulates to
        ;; an ArrayList in the benchmark
        ;; https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/experimental/datahike-benchmark/
        ;; In other words, there is no use writing imperative code here
        ;; with Java mutable collections.
        substituted-pattern-and-filter-feature-pairs
        (into []
              (keep
               (fn [tuple]
                 (let [feature (feature-extractor tuple)]
                   (when (good-lookup-refs? feature)
                     (when-let [k (-> tuple
                                      select-pattern-substitution-inds
                                      pattern-from-tuple)]
                       [k feature])))))
              tuples)

        filter-feature-extractor (index-feature-extractor
                                  (map :pattern-element-index filtered-vars)
                                  false
                                  lrr)]

    ;; This expression will produce an `xform` that performs the substitutions for
    ;; this relation.
    (instantiate-substitution-xform substitution-pattern-element-inds
                                    filter-feature-extractor
                                    substituted-pattern-and-filter-feature-pairs)))

(defn search-context? [x]
  (assert (map? x))
  (let [{:keys [bsm clean-pattern rels strategy-vec]} x]
    (assert bsm)
    (assert clean-pattern)
    (assert rels)
    (assert strategy-vec))
  true)

(defn compute-per-rel-map [search-context rel-inds strat-symbol]
  {:pre [(search-context? search-context)]}
  (->> strat-symbol
       (search-index-mapping search-context)
       (filter (comp rel-inds :relation-index))
       (group-by :relation-index)))

(defn clean-pattern-before-substitution [pattern subst-map]
  (let [subst-pattern-positions (into #{}
                                      (comp cat (map :pattern-element-index))
                                      (vals subst-map))]
    (into []
          (map-indexed (fn [i x]
                         (cond
                           (subst-pattern-positions i) x
                           (symbol? x) nil
                           :else x)))
          pattern)))

(defn initialization-and-substitution-xform
  "Returns a transducer that performs all subsitutions possible given the relations with indices `rel-inds`."
  [search-context substituted-relation-inds]
  {:pre [(map? search-context)
         (set? substituted-relation-inds)]}
  (let [;; We refer to relations by their index in the vector in the context.
        substituted-vars-per-relation (compute-per-rel-map search-context
                                                           substituted-relation-inds
                                                           :substitute)

        filtered-vars-per-relation (compute-per-rel-map search-context
                                                        substituted-relation-inds
                                                        :filter)

        all-substitutions-xform (apply comp
                                       (map (fn [relation-index]
                                              (single-substitution-xform
                                               search-context
                                               relation-index
                                               substituted-vars-per-relation
                                               filtered-vars-per-relation))
                                            substituted-relation-inds))
        init-coll [[;; This is the initial pattern
                    (clean-pattern-before-substitution
                     (:clean-pattern search-context)
                     substituted-vars-per-relation)

                    ;; This is the initial predicate (nil because there is no predicate)
                    nil]]]
    [init-coll all-substitutions-xform]))

(defn datom-filter-predicate [filtered-relation-inds search-context]
  (let [filtered-vars-per-relation (compute-per-rel-map search-context filtered-relation-inds :filter)
        rels (:rels search-context)]
    (reduce (fn [predicate [relation-index filtered-vars]]
              (let [tuples (:tuples (nth rels relation-index))
                    pos-inds (map :pattern-element-index filtered-vars)
                    tup-inds (map :tuple-element-index filtered-vars)
                    tuple-feature-extractor (index-feature-extractor tup-inds true)
                    features (into #{}
                                   (map tuple-feature-extractor)
                                   tuples)
                    datom-feature-extractor
                    (index-feature-extractor pos-inds false)]
                (extend-predicate predicate
                                  datom-feature-extractor
                                  features)))
            nil
            filtered-vars-per-relation)))

(defn filter-from-predicate [pred]
  (if pred
    (filter pred)
    identity))

(defn backend-xform [backend-fn]
  (fn [step]
    (fn
      ([] (step))
      ([dst] (step dst))
      ([dst e a v tx added? datom-predicate]
       (let [inner-step (if datom-predicate
                          (fn [dst datom]
                            (if (datom-predicate datom)
                              (step dst datom)
                              dst))
                          step)
             datoms (try
                      (backend-fn e a v tx added?)
                      (catch #?(:clj Exception :cljs js/Error) e
                        (throw e)))]
         (reduce inner-step
                 dst
                 datoms))))))

(defn extend-predicate-for-pattern-constants
  [predicate {:keys [strategy-vec clean-pattern] :as search-context}]
  (let [inds (for [[i strategy pattern-value] (mapv vector (range)
                                                    strategy-vec
                                                    clean-pattern)
                   :when (= :filter strategy)
                   :when (and (some? pattern-value)
                              (not (symbol? pattern-value)))]
               i)
        extractor (index-feature-extractor
                   inds
                   false
                   (lookup-ref-replacer search-context))]
    (if extractor
      (extend-predicate predicate extractor #{(extractor clean-pattern)})
      predicate)))

(defn unpack6 [step]
  (fn
    ([] (step))
    ([dst] (step dst))
    ([dst [[e a v tx added?] filt]]
     (step dst e a v tx added? filt))))

(defn search-batch-fn
  "This function constructs a \"strategy function\" that gets called by `dbi/-batch-search.`"
  [search-context]
  (fn [strategy-vec backend-fn datom-xform]
    (let [search-context (merge search-context {:strategy-vec strategy-vec
                                                :backend-fn backend-fn})

          ;; Relations with indices `substituted-relation-inds` are used for substituting variables
          ;; in the pattern.
          substituted-relation-inds (substitution-relation-indices search-context)

          ;; Relations with indices `filtered-relation-inds` are used for filtering the datoms
          ;; returned by the search backend.
          filtered-relation-inds (filtering-relation-indices search-context substituted-relation-inds)

          [init-coll substitution-xform] (initialization-and-substitution-xform
                                          search-context
                                          substituted-relation-inds)

          filter-xform (-> filtered-relation-inds
                           (datom-filter-predicate search-context)
                           (extend-predicate-for-pattern-constants search-context)
                           filter-from-predicate)

          ;; This transduction will take the initial pattern,
          ;; perform all variable substitutions for all combinations
          ;; of relations and then look up the datoms in the index.
          ;; Finally, the datoms will be filtered for the variables
          ;; that were not substituted.
          result (into []

                       ;; From the output of `unpack6`
                       ;; to the input of `backend-xform`
                       ;; the transducers are higher-arity. That is,
                       ;; instead of calling `(step acc [[e a v tx added?] pred])`,
                       ;; they call `(step acc e a v tx added? pred)`. This avoids
                       ;; the allocation of short-lived vectors and speeds up the
                       ;; process by about 0.4 seconds in
                       ;; https://gitlab.com/arbetsformedlingen/taxonomy-dev/backend/experimental/datahike-benchmark/

                       (comp

                        ;; Unpack the pattern as arguments to the next step function.
                        unpack6

                        ;; Substitute variables with values
                        ;; from tuples in the relations and accumulate the filter predicate.
                        substitution-xform

                        ;; Perform the lookup in the search backend.
                        (backend-xform backend-fn)

                        ;; Filter the datoms returned from the search backend.
                        filter-xform

                        ;; Apply the provided datom-xform on the returned datoms
                        datom-xform)
                       init-coll)]
      result)))

(defn- scalar-value?
  "Returns true if v is a scalar value suitable for index lookup."
  [v]
  (or (number? v) (string? v) (keyword? v)
      (boolean? v) (inst? v) (uuid? v)))

(defn- fast-lookup-type
  "Returns :ea for ground [e a ?v], :av for ground [?e a v] patterns that
   can use di/-lookup for a single point lookup instead of the full batch
   search pipeline. Only matches base DB type and cardinality-one attributes."
  [source pattern1]
  (when (and (instance? #?(:clj DB :cljs datahike.db/DB) source)
             (>= (count pattern1) 3))
    (let [[e a v] pattern1
          tx (get pattern1 3)
          a-ground? (or (keyword? a) (number? a))
          tx-free? (or (nil? tx) (symbol? tx))]
      (cond
        ;; [e a ?v] — lookup on EAVT with EA comparator
        (and (number? e) a-ground? (symbol? v) tx-free?
             (not (dbu/multival? source a)))
        :ea

        ;; [?e a v] — lookup on AVET with AV comparator
        ;; v must be a scalar, attr must be :db/unique (guarantees single result)
        (and (symbol? e) a-ground? tx-free?
             (scalar-value? v)
             (:avet source)
             (dbu/is-attr? source a :db/unique))
        :av))))

(defn- fast-ground-lookup
  "Fast path for ground point-lookup patterns.
   Uses di/-lookup with a partial comparator instead of the full batch
   search transduction pipeline. ~3x faster for point lookups.
   pattern0 is the pre-resolution pattern (may contain lookup refs),
   pattern1 is the resolved pattern (entity ids, attr refs)."
  [lookup-type source context orig-pattern pattern0 pattern1]
  (let [var-map (var-mapping orig-pattern (range))
        [e0 a0 v0] pattern0
        found (case lookup-type
                :ea (let [[e a] pattern1]
                      (di/-lookup (:eavt source)
                                  (datom/datom (long e) a nil 0)
                                  (datom/index-type->cmp-replace :eavt)))
                :av (let [[_ a v] pattern1]
                      (di/-lookup (:avet source)
                                  (datom/datom 0 a v 0)
                                  datom/cmp-datoms-av-only)))
        tuples (if found
                 (let [d found]
                   (case lookup-type
                     :ea [#?(:cljs (da/array e0 a0 (.-v ^Datom d) (.-tx ^Datom d) true)
                             :clj [e0 a0 (.-v ^Datom d) (.-tx ^Datom d) true])]
                     :av [#?(:cljs (da/array (.-e ^Datom d) a0 v0 (.-tx ^Datom d) true)
                             :clj [(.-e ^Datom d) a0 v0 (.-tx ^Datom d) true])]))
                 [])
        new-rel (rel/->Relation var-map tuples)]
    (cond-> (update context :rels rel/collapse-rels new-rel (join-env context))
      (:stats context) (assoc :tmp-stats {:type :lookup}))))

(defn lookup-batch-search [source context orig-pattern pattern1]
  (let [new-rel (if (dbu/db? source)
                  (let [rels (vec (:rels context))
                        bsm (bound-symbol-map rels)
                        clean-pattern (->> pattern1
                                           (replace-unbound-symbols-by-nil bsm)
                                           (resolve-pattern-eid source))
                        search-context {:source source
                                        :bsm bsm
                                        :clean-pattern clean-pattern
                                        :rels rels}

                        datoms (if clean-pattern

                                 ;; Make the call to the search backend
                                 (dbi/batch-search
                                  source clean-pattern
                                  (search-batch-fn search-context)
                                  (relation-from-datoms-xform))

                                 [])

                        new-rel (relation-from-datoms
                                 context orig-pattern datoms)]
                    new-rel)
                  (lookup-pattern-coll source pattern1 orig-pattern))]

    ;; The env is needed for `collapse-rels` / `hash-join` / `getter-fn`:
    ;; lookup-attrs enables lookup-ref eid resolution on the join columns.
    (let [env {:source (:join-source context)
               :lookup-attrs (when (satisfies? dbi/IDB source)
                               (dynamic-lookup-attrs source pattern1))}]
      (cond-> (update context :rels rel/collapse-rels new-rel env)
        (:stats context) (assoc :tmp-stats {:type :lookup})))))

(defn -resolve-clause*
  ([context clause]
   (-resolve-clause* context clause clause))
  ([context clause orig-clause]
   (condp looks-like? clause
     [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
     ;; Defer if any input var isn't bound yet — the iterative resolver
     ;; (datahike.tools/resolve-clauses) will retry once binders fire.
     ;; If the var is never bound, the resolver raises "Cannot resolve any
     ;; more clauses" with the full pending list, which is more useful
     ;; than a misleading single-clause error from this site.
     (let [vars (filter free-var? (first clause))]
       (when (all-bound? context vars)
         (filter-by-pred context clause)))

     [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
     (bind-by-fn context clause)

     [source? '*] ;; source + anything
     (let [[source-sym & rest] clause
           ;; :join-source mirrors the old *implicit-source* BINDING: scoped to
           ;; this clause only — restore on the escaping context, else later
           ;; plain patterns would resolve against this clause's source.
           ctx' (-resolve-clause (assoc context :join-source (get (:sources context) source-sym))
                                 rest clause)]
       (cond-> ctx' (map? ctx') (assoc :join-source (:join-source context))))

     '[or *] ;; (or ...)
     (let [[_ & branches] clause
           context' (assoc context :stats [])
           contexts (mapv #(resolve-clause context' %) branches)
           env (join-env context)
           sum-rel (->> contexts
                        (map #(reduce (fn [a b] (rel/hash-join a b env)) (:rels %)))
                        (reduce sum-rel))]
       (cond-> (assoc context :rels [sum-rel])
         (:stats context) (assoc :tmp-stats {:type :or
                                             :branches (mapv :stats contexts)})))

     '[or-join [[*] *] *] ;; (or-join [[req-vars] vars] ...)
     (let [[_ [req-vars & vars] & branches] clause]
       (when (all-bound? context req-vars)
         (recur context (list* 'or-join (concat req-vars vars) branches) clause)))

     '[or-join [*] *] ;; (or-join [vars] ...)
     ;; TODO required vars
     (let [[_ vars & branches] clause
           vars (set vars)
           join-context (-> context
                            (assoc :stats [])
                            (limit-context vars))
           contexts (map #(-> join-context
                              (resolve-clause %)
                              (limit-context vars))
                         branches)
           env (join-env context)
           sum-rel (->> contexts
                        (map #(reduce (fn [a b] (rel/hash-join a b env)) (:rels %)))
                        (reduce sum-rel))]
       (cond-> (update context :rels rel/collapse-rels sum-rel env)
         (:stats context) (assoc :tmp-stats {:type :or-join
                                             :branches (mapv #(-> % :stats first) contexts)})))

     '[and *] ;; (and ...)
     (let [[_ & clauses] clause]
       (if (:stats context)
         (let [and-context (-> context
                               (assoc :stats [])
                               (resolve-context clauses))]
           (assoc and-context
                  :tmp-stats {:type :and
                              :branches (:stats and-context)}
                  :stats (:stats context)))
         (resolve-context context clauses)))

     '[not *] ;; (not ...)
     (let [[_ & clauses] clause
           negation-vars (collect-vars clauses)]
       (when (some-bound? context negation-vars)
         (let [env (join-env context)
               join-rel (reduce (fn [a b] (rel/hash-join a b env)) (:rels context))
               negation-context (-> context
                                    (assoc :rels [join-rel])
                                    (assoc :stats [])
                                    (resolve-context clauses))
               negation-join-rel (reduce (fn [a b] (rel/hash-join a b env)) (:rels negation-context))
               negation (rel/subtract-rel join-rel negation-join-rel env)]
           (cond-> (assoc context :rels [negation])
             (:stats context) (assoc :tmp-stats {:type :not
                                                 :branches (:stats negation-context)})))))

     '[not-join [*] *] ;; (not-join [vars] ...)
     (let [[_ vars & clauses] clause]
       (when (all-bound? context vars)
         (let [env (join-env context)
               join-rel (reduce (fn [a b] (rel/hash-join a b env)) (:rels context))
               negation-context (-> context
                                    (assoc :rels [join-rel])
                                    (assoc :stats [])
                                    (limit-context vars)
                                    (resolve-context clauses)
                                    (limit-context vars))
               negation-join-rel (reduce (fn [a b] (rel/hash-join a b env)) (:rels negation-context))
               negation (rel/subtract-rel join-rel negation-join-rel env)]
           (cond-> (assoc context :rels [negation])
             (:stats context) (assoc :tmp-stats {:type :not
                                                 :branches (:stats negation-context)})))))

     '[*] ;; pattern
     (let [source (:join-source context)
           pattern0 (replace (:consts context) clause)
           pattern1 (resolve-pattern-lookup-refs source pattern0)
           lt (fast-lookup-type source pattern1)]
       (if lt
         (fast-ground-lookup lt source context clause pattern0 pattern1)
         (lookup-batch-search source context clause pattern1))))))

(defn -resolve-clause
  ([context clause]
   (-resolve-clause context clause clause))
  ([context clause orig-clause]
   (dqs/update-ctx-with-stats context orig-clause
                              (fn [context]
                                (-resolve-clause* context clause orig-clause)))))

(defn resolve-clause [context clause]
  (if (rule? context clause)
    (if (source? (first clause))
      (let [ctx' (resolve-clause (assoc context :join-source (get (:sources context) (first clause)))
                                 (next clause))]
        ;; binding-scope semantics: restore the enclosing source on escape
        (cond-> ctx' (map? ctx') (assoc :join-source (:join-source context))))
      (dqs/update-ctx-with-stats context clause
                                 (fn [context] (solve-rule context clause))))
    (-resolve-clause context clause)))

(defn -q [context clauses]
  (dt/resolve-clauses resolve-clause
                      (assoc context :join-source (get (:sources context) '$))
                      clauses))

(defn -collect
  ([context symbols]
   (let [rels (:rels context)
         start-array (to-array (map #(get (:consts context) %) symbols))]
     (-collect [start-array] rels symbols)))
  ([acc rels symbols]
   (if-some [rel (first rels)]
     (if (empty? (:tuples rel))
       ;; A zero-tuple relation means this conjunct has no satisfying bindings,
       ;; so the whole conjunctive result is empty — annihilate. This must run
       ;; before the keep-attrs test below: a const-only / no-find-var clause
       ;; (e.g. a predicate over `:in` constants like `[(= ?e 999)]`) produces
       ;; an empty-attrs eliminating relation that would otherwise be skipped,
       ;; letting the const-seeded accumulator survive unfiltered (issue #848).
       []
       (let [keep-attrs (select-keys (:attrs rel) symbols)]
         (if (empty? keep-attrs)
           (recur acc (next rels) symbols)
           (let [copy-map (to-array (map #(get keep-attrs %) symbols))
                 len (count symbols)]
             (recur (for [#?(:cljs t1
                             :clj ^{:tag "[[Ljava.lang.Object;"} t1) acc
                          t2 (:tuples rel)]
                      (let [res (aclone t1)]
                        (dotimes [i len]
                          (when-some [idx (aget copy-map i)]
                            (aset res i (get t2 idx))))
                        res))
                    (next rels)
                    symbols)))))
     acc)))

(defprotocol IContextResolve
  (-context-resolve [var context]))

(extend-protocol IContextResolve
  Variable
  (-context-resolve [var context]
    (context-resolve-val context (.-symbol var)))
  SrcVar
  (-context-resolve [var context]
    (get-in context [:sources (.-symbol var)]))
  PlainSymbol
  (-context-resolve [var _]
    (or (get built-in-aggregates (.-symbol var))
        (resolve-sym (.-symbol var))))
  Constant
  (-context-resolve [var _]
    (.-value var)))

(defn -aggregate [find-elements context tuples]
  (mapv (fn [element fixed-value i]
          (if (instance? Aggregate element)
            (let [f (-context-resolve (:fn element) context)
                  args (map #(-context-resolve % context) (butlast (:args element)))
                  vals (map #(nth % i) tuples)]
              (apply f (concat args [vals])))
            fixed-value))
        find-elements
        (first tuples)
        (range)))

(defn- idxs-of [pred coll]
  (->> (map #(when (pred %1) %2) coll (range))
       (remove nil?)))

(defn aggregate [find-elements context resultset]
  (let [group-idxs (idxs-of (complement #(instance? Aggregate %)) find-elements)
        group-fn (fn [tuple]
                   (map #(nth tuple %) group-idxs))
        grouped (group-by group-fn resultset)]
    (for [[_ tuples] grouped]
      (-aggregate find-elements context tuples))))

(defprotocol IPostProcess
  (-post-process [find tuples]))

(extend-protocol IPostProcess
  FindRel
  (-post-process [_ tuples]
    (if (seq? tuples) (vec tuples) tuples))
  FindColl
  (-post-process [_ tuples]
    (into [] (map first) tuples))
  FindScalar
  (-post-process [_ tuples]
    (ffirst tuples))
  FindTuple
  (-post-process [_ tuples]
    (first tuples)))

(defn- pull
  "Materialize Pull find elements over the result tuples.
   cljs dual: sync? false yields an async expression; the per-entity
   pull-spec walks are awaited (eager loops — the lazy seqs of the sync
   shape would hide the reads from the CPS transform)."
  ([find-elements context resultset] (pull find-elements context resultset true))
  ([find-elements context resultset sync?]
   (async+sync sync?
               (let [resolved (mapv (fn [find]
                                      (when (instance? Pull find)
                                        [(-context-resolve (:source find) context)
                                         (dpp/parse-pull
                                          (-context-resolve (:pattern find) context))]))
                                    find-elements)]
                 (loop [ts (seq resultset) acc []]
                   (if ts
                     (let [tuple (first ts)
                           row (loop [i 0 racc []]
                                 (if (< i (count resolved))
                                   (let [env (nth resolved i)
                                         el (nth tuple i)
                                         v (if env
                                             (let [[src spec] env]
                                               (pca/await (dpa/pull-spec src spec [el] false sync?)))
                                             el)]
                                     (recur (inc i) (conj racc v)))
                                   racc))]
                       (recur (next ts) (conj acc row)))
                     acc))))))

(def ^:private query-cache (volatile! (datahike.lru/lru lru-cache-size)))

;; Plan cache: keyed by [where-clauses bound-vars rules-keys schema-hash]
;; The plan structure (index selection, merge ordering) is stable across
;; transactions as long as the schema hasn't changed.
(def ^:private plan-cache (volatile! (datahike.lru/lru lru-cache-size)))

;; ---------------------------------------------------------------------------
;; Query result cache with structural sharing across transactions
;;
;; Each DB snapshot (identified by max-tx) has a persistent map of cached results.
;; When a transaction creates a new DB, the new DB inherits the parent's cache
;; minus entries whose attribute-deps overlap with the transaction's modified attrs.
;; This gives O(k) cache propagation where k = invalidated entries.
;;
;; Cache structure: ConcurrentHashMap<Long(max-tx), PersistentHashMap<cache-key, {:result r :attrs deps}>>
;; Cache key: [query non-db-args]
;; ---------------------------------------------------------------------------
;; Query result cache
;;
;; Global LRU cache keyed by DB identity [hash max-tx max-eid].
;; Portable (atom + persistent maps), no metadata, survives serialization.
;; Inspectable: @datahike.query/query-result-cache
;;
;; Structure: LRU { db-key -> { cache-key -> {:result r :attrs #{...}} } }
;;
;; Propagation: on transaction, surviving entries (whose attr deps don't
;; overlap with modified-attrs) are copied from parent db-key to child db-key
;; via dissoc for structural sharing.

(def ^:dynamic *query-cache-size*
  "Maximum number of DB snapshots retained in the query result cache.
   Set DATAHIKE_QUERY_CACHE_SIZE env var or call set-query-cache-size! to change.
   Default: 64."
  (let [env-val #?(:clj (System/getenv "DATAHIKE_QUERY_CACHE_SIZE") :cljs nil)]
    (if env-val
      (let [n #?(:clj (Long/parseLong env-val) :cljs (js/parseInt env-val))]
        (if (pos? n) n 64))
      64)))

(def ^:dynamic *query-cache-weight-limit*
  "Maximum total number of cached result tuples retained across all DB snapshots
   in the query result cache. 0 disables the budget, leaving only the
   snapshot-count cap *query-cache-size*. Set DATAHIKE_QUERY_CACHE_WEIGHT_LIMIT
   env var or call set-query-cache-weight-limit! to change. Default: 1000000."
  (let [env-val #?(:clj (System/getenv "DATAHIKE_QUERY_CACHE_WEIGHT_LIMIT") :cljs nil)]
    (if env-val
      (let [n #?(:clj (Long/parseLong env-val) :cljs (js/parseInt env-val))]
        (if (nat-int? n) n 1000000))
      1000000)))

(defn- result-weight
  "Cached-tuple weight of one query result. Relation / collection finds
   return countable collections (weight = tuple count); scalar-find (`.`)
   and single-tuple / single-pull finds return a non-collection value
   that weighs 1. Computed once at cache-put time and stored on the entry
   as :weight, so the weigh path never calls `count` on a raw result
   (a scalar `:result` would otherwise throw)."
  [result]
  (cond
    (nil? result)     0
    (counted? result) (count result)
    #?@(:clj [(instance? java.util.Collection result) (count result)])
    (coll? result)    (count result)
    :else             1))

(defn- bucket-weight
  "Total cached-tuple weight of one DB-snapshot bucket — sums the
   per-entry :weight precomputed at cache-put time (see `result-weight`)."
  [bucket]
  (reduce-kv (fn [acc _ entry] (+ acc (:weight entry 0))) 0 bucket))

(defonce ^{:doc "Global weighted LRU query result cache. Keys are [hash max-tx max-eid]
   identifying a DB snapshot. Values are maps of {cache-key -> {:result r :attrs #{...}}}.
   Bounded by *query-cache-size* snapshots and *query-cache-weight-limit* total tuples.
   Inspect with @datahike.query/query-result-cache."}
  query-result-cache
  (atom (datahike.lru/weighted-lru *query-cache-size* *query-cache-weight-limit* bucket-weight)))

(defn set-query-cache-size!
  "Set the maximum number of DB snapshots retained in the query result cache.
   Takes effect immediately by replacing the cache with a new empty LRU of the given size."
  [n]
  {:pre [(pos-int? n)]}
  #?(:clj (alter-var-root #'*query-cache-size* (constantly n))
     :cljs (set! *query-cache-size* n))
  (reset! query-result-cache
          (datahike.lru/weighted-lru n *query-cache-weight-limit* bucket-weight))
  n)

(defn clear-query-cache!
  "Clear all entries from the query result cache."
  []
  (reset! query-result-cache
          (datahike.lru/weighted-lru *query-cache-size* *query-cache-weight-limit* bucket-weight)))

(defn set-query-cache-weight-limit!
  "Set the maximum total number of cached result tuples across all DB snapshots.
   0 disables the weight budget. Takes effect immediately by replacing the cache."
  [n]
  {:pre [(nat-int? n)]}
  #?(:clj (alter-var-root #'*query-cache-weight-limit* (constantly n))
     :cljs (set! *query-cache-weight-limit* n))
  (reset! query-result-cache
          (datahike.lru/weighted-lru *query-cache-size* n bucket-weight))
  n)

(defn- db-cache-key
  "Compute the cache identity key for a DB snapshot."
  [db]
  [(:hash db) (:max-tx db) (:max-eid db)])

(defn- extract-query-attr-deps
  "Extract the set of attributes referenced in where-clauses.
   Returns a set of keywords/symbols, or :all if deps cannot be determined.
   Uses a work-queue loop so :all short-circuits cleanly without nested reduces."
  [where-clauses]
  (loop [clauses where-clauses
         attrs   #{}]
    (if (empty? clauses)
      attrs
      (let [[clause & rest-clauses] clauses]
        (cond
          ;; Data pattern: [?e :attr ?v] or with src-var [$ ?e :attr ?v]
          (and (vector? clause) (not (vector? (first clause))))
          (let [f (first clause)
                src? (and (symbol? f) (clojure.string/starts-with? (name f) "$"))
                a (if src? (nth clause 2 nil) (second clause))]
            (if (symbol? a)
              ;; Variable in attribute position — query touches all attributes
              :all
              (recur rest-clauses
                     (if (some? a)
                       (conj attrs a)
                       attrs))))

          ;; Predicate/function clause [(> ?a 50)] — deps come from binding
          ;; pattern clauses, safe to skip.
          (and (sequential? clause) (vector? (first clause)))
          (recur rest-clauses attrs)

          ;; or / not / and / or-join / not-join — add sub-clauses to work queue
          (and (sequential? clause) (symbol? (first clause)))
          (let [op-name (name (first clause))]
            (case op-name
              ("or" "and" "not")
              (recur (into rest-clauses (rest clause)) attrs)

              ("or-join" "not-join")
              ;; Skip the binding-var vector (second element)
              (recur (into rest-clauses (rest (rest clause))) attrs)

              ;; Unknown: rule call, get-else, etc. — cannot determine attr deps
              :all))

          ;; Unknown shape — skip
          :else (recur rest-clauses attrs))))))

(defn- extract-find-pull-attr-deps
  "Extract the set of attributes referenced in pull patterns within :find.
   Returns a set of keywords, or :all for wildcard pulls or unresolvable patterns."
  [find-clause]
  (let [find-elements (dpip/find-elements find-clause)]
    (reduce
     (fn [attrs el]
       (if (= attrs :all)
         (reduced :all)
         (if (instance? Pull el)
           (let [pattern (:pattern el)]
             (if-not (sequential? pattern)
                ;; Pattern is a variable bound via :in — cannot determine attrs
               (reduced :all)
               (let [spec (dpp/parse-pull pattern)]
                 (if (:wildcard? spec)
                   (reduced :all)
                   (into attrs (keys (:attrs spec)))))))
           attrs)))
     #{}
     find-elements)))

(defn- merge-attr-deps
  "Merge two attr-dep sets. :all dominates."
  [a b]
  (cond
    (= a :all) :all
    (= b :all) :all
    :else (into a b)))

(defn- result-cache-get
  "Look up a cached query result for the given DB."
  [db cache-key]
  (let [dk (db-cache-key db)]
    (when-let [snapshot-cache (get @query-result-cache dk)]
      (when-let [entry (get snapshot-cache cache-key)]
        ;; Touch LRU atomically — re-assoc current value so LRU updates generation.
        ;; Use swap! which retries on CAS conflict, reading fresh state each retry.
        (swap! query-result-cache
               (fn [lru]
                 (if-let [current (get lru dk)]
                   (assoc lru dk current)
                   lru)))
        entry))))

(defn- result-cache-put!
  "Store a query result in the cache for the given DB."
  [db cache-key result attr-deps]
  (let [dk (db-cache-key db)]
    (swap! query-result-cache
           (fn [lru]
             (let [existing (or (get lru dk) {})]
               (assoc lru dk (assoc existing cache-key
                                    {:result result :attrs attr-deps
                                     :weight (result-weight result)})))))))

(defn propagate-query-cache
  "Propagate query result cache from parent DB to child DB after a transaction.
   Entries whose attribute deps overlap with modified-attrs are evicted.
   Uses dissoc for structural sharing — child cache shares most of parent's
   persistent map when few entries are invalidated.
   Parent cache is NOT removed — parent DB may still be queried (e.g. d/with).
   Called from datahike.writing/complete-db-update and datahike.core/with."
  [parent-db child-db modified-attrs]
  (let [parent-key (db-cache-key parent-db)
        child-key  (db-cache-key child-db)]
    (when-not (= parent-key child-key)
      ;; Only propagate if we have user-visible modified attrs for selective invalidation.
      ;; If modified-attrs is empty or only has system attrs (e.g. purge ops where
      ;; tx-data doesn't list affected user attrs), skip propagation entirely —
      ;; the DB changed but we can't determine which queries are safe to keep.
      (let [user-attrs (disj modified-attrs :db/txInstant)]
        (when (seq user-attrs)
          (let [parent-entries (get @query-result-cache parent-key)]
            (when (seq parent-entries)
              (let [child-entries (reduce-kv
                                   (fn [m k {:keys [attrs]}]
                                     (if (or (= attrs :all)
                                             (some user-attrs attrs))
                                       (dissoc m k)
                                       m))
                                   parent-entries
                                   parent-entries)]
                (when (seq child-entries)
                  (swap! query-result-cache assoc child-key child-entries))))))))))

(defn memoized-parse-query [q]
  (if-some [cached (get @query-cache q nil)]
    cached
    (let [qp (parse q)]
      (vswap! query-cache assoc q qp)
      qp)))

(defn convert-to-return-maps [{:keys [mapping-type mapping-keys]} resultset]
  (let [mapping-keys (map #(get % :mapping-key) mapping-keys)
        convert-fn (fn [mkeys]
                     (mapv #(zipmap mkeys %) resultset))]
    (condp = mapping-type
      :keys (convert-fn (map keyword mapping-keys))
      :strs (convert-fn (map str mapping-keys))
      :syms (convert-fn (map symbol mapping-keys)))))

(defn collect [context symbols]
  (->> (-collect context symbols)
       (map vec)))

(def default-settings {})

(defn- context-bound-vars
  "Extract the set of variables already bound in context relations (from :in bindings)."
  [context]
  (into #{} (mapcat (comp keys :attrs)) (:rels context)))

(defn- in-card-seed
  "Value-independent {var → card} cardinality seed for :in bindings, derived
   from binding SHAPE alone (so it is safe to fold into the plan cache key).
   Collection/relation bindings ([?x ...], [[?a ?b]]) bind many rows; tuple and
   scalar bindings bind one. Routes through plan/source-cards so the planner has
   a single produce-side definition of how each cardinality source seeds the
   join-graph estimate. `qin` is the parsed :in vector of binding records."
  [qin]
  (reduce (fn [m b]
            (let [shape (cond (instance? BindColl b)  :collection
                              (instance? BindTuple b) :tuple
                              :else                   :scalar)
                  vars (map :symbol (dpi/collect-vars-distinct b))]
              (merge m (plan/source-cards {:kind :input :shape shape :vars vars}))))
          {} qin))

(defn- substitute-consts
  "Replace const-bound variables in where clauses with their values.
   E.g., [?e :name ?name] with consts {?name \"Ivan\"} → [?e :name \"Ivan\"]."
  [where-clauses consts]
  (if (empty? consts)
    where-clauses
    (mapv (fn [clause]
            (if (and (vector? clause)
                     (not (vector? (first clause)))) ;; not a pred/fn clause
              (mapv (fn [x]
                      (if (and (symbol? x) (contains? consts x))
                        (get consts x)
                        x))
                    clause)
              clause))
          where-clauses)))

(defn- substitute-consts-with-lookup-refs
  "Like substitute-consts but also resolves lookup refs in pattern positions.
   Used by the query planner which needs patterns normalized before planning.
   Lookup refs like [:name \"Ivan\"] in e/v positions are resolved to entity IDs.
   For multi-source queries, pass sources so each source-prefixed clause resolves
   against its own db."
  ([db where-clauses consts] (substitute-consts-with-lookup-refs db where-clauses consts nil))
  ([db where-clauses consts sources]
   (letfn [(resolve-clause
             ([clause] (resolve-clause db clause))
             ([resolve-db clause]
              (cond
              ;; Source-prefixed clauses ($source pattern...) — must be checked BEFORE
              ;; data patterns because [$1 ?e :attr ?v] is also a vector.
                (and (sequential? clause) (symbol? (first clause))
                     (let [s (name (first clause))]
                       (= \$ (first s))))
                (let [src-sym (first clause)
                      src-db (if sources (get sources src-sym resolve-db) resolve-db)
                      inner-pattern (vec (rest clause))
                      resolved-inner (resolve-clause src-db inner-pattern)]
                  (cons src-sym resolved-inner))

              ;; Data pattern: [e a v ...]
                (and (vector? clause) (not (sequential? (first clause))))
                (let [had-consts? (some #(and (symbol? %) (contains? consts %)) clause)
                      substituted (if had-consts?
                                    (mapv (fn [x]
                                            (if (and (symbol? x) (contains? consts x))
                                              (get consts x)
                                              x))
                                          clause)
                                    clause)
                    ;; Use lenient resolution when consts were substituted (values might be invalid),
                    ;; UNLESS the entity position is a lookup ref — those should throw on missing entities.
                      has-lookup-ref-entity? (and had-consts?
                                                  (let [e (first substituted)]
                                                    (and (sequential? e) (= 2 (count e)) (keyword? (first e)))))
                      resolved (if (or (not had-consts?) has-lookup-ref-entity?)
                                 (resolve-pattern-lookup-refs resolve-db substituted)
                                 (resolve-pattern-lookup-refs-or-nil resolve-db substituted))]
                  (or resolved substituted))

              ;; Data pattern where first elem is a lookup ref
              ;; e.g., [[:name "Ivan"] :age ?v]
                (and (vector? clause) (vector? (first clause))
                     (= 2 (count (first clause))))
              ;; Inline lookup refs use strict resolution (should throw on missing entities)
                (resolve-pattern-lookup-refs resolve-db clause)

              ;; (not ...) / (not-join [...] ...)
                (and (sequential? clause) (symbol? (first clause))
                     (#{'not 'not-join} (first clause)))
                (if (= 'not (first clause))
                  (cons 'not (mapv (partial resolve-clause resolve-db) (rest clause)))
                  (let [[_ join-vars & body] clause]
                    (list* 'not-join join-vars (mapv (partial resolve-clause resolve-db) body))))

              ;; (or ...) / (or-join [...] ...)
                (and (sequential? clause) (symbol? (first clause))
                     (#{'or 'or-join} (first clause)))
                (if (= 'or (first clause))
                  (cons 'or (map (fn [branch]
                                   (if (and (sequential? branch) (sequential? (first branch)))
                                     (mapv (partial resolve-clause resolve-db) branch)
                                     (resolve-clause resolve-db branch)))
                                 (rest clause)))
                  (let [[_ join-vars & branches] clause]
                    (list* 'or-join join-vars
                           (map (fn [branch]
                                  (if (and (sequential? branch) (sequential? (first branch)))
                                    (mapv (partial resolve-clause resolve-db) branch)
                                    (resolve-clause resolve-db branch)))
                                branches))))

              ;; (and ...)
                (and (sequential? clause) (= 'and (first clause)))
                (cons 'and (mapv (partial resolve-clause resolve-db) (rest clause)))

              ;; Source-prefixed: already handled at top of cond

              ;; Rule calls: (rule-name ?arg1 ?arg2 ...) — substitute scalar consts in args.
              ;; Only substitute data values (numbers, strings, keywords, booleans),
              ;; NOT function references (IFn), since those are used as higher-order args
              ;; in rules like (match ?pred ?x ?y) and must resolve at execution time.
                (and (sequential? clause) (not (vector? clause))
                     (symbol? (first clause))
                   ;; Not a known special form
                     (not (#{'not 'not-join 'or 'or-join 'and} (first clause)))
                   ;; Not source-prefixed
                     (not (and (string? (name (first clause)))
                               (= \$ (first (name (first clause)))))))
                (let [rule-name (first clause)
                      args (rest clause)
                      scalar? (fn [v]
                                (or (number? v) (string? v) (keyword? v)
                                    (boolean? v) (nil? v) (uuid? v)
                                    (inst? v)))
                      substituted-args (map (fn [x]
                                              (if (and (symbol? x)
                                                       (contains? consts x)
                                                       (scalar? (get consts x)))
                                                (get consts x)
                                                x))
                                            args)]
                  (apply list rule-name substituted-args))

              ;; Anything else (predicates, functions): substitute consts in data args only.
              ;; Don't substitute the function/predicate name (position 0 of the call form)
              ;; since context-resolve-val already handles consts lookup at execution time.
              ;; Only substitute data-position args (non-function variables) so the plan
              ;; can use ground values for index selection.
                :else
                (if (and (vector? clause) (not (vector? (first clause))))
                  (mapv (fn [x]
                          (cond
                            (and (symbol? x) (contains? consts x))
                            (get consts x)
                          ;; Recurse into predicate/function call lists like (> ?s ?min_s)
                          ;; but don't substitute the fn name (first element)
                            (sequential? x)
                            (let [substituted (map-indexed
                                               (fn [i y]
                                                 (if (and (pos? i)
                                                          (symbol? y)
                                                          (contains? consts y))
                                                   (get consts y)
                                                   y))
                                               x)]
                              (if (list? x) (apply list substituted) (vec substituted)))
                            :else x))
                        clause)
                  clause))))]
     (mapv resolve-clause where-clauses))))

(defn- has-lookup-ref-bindings?
  "Check if any :in binding contains lookup refs (sequential values like [:name \"Ivan\"]).
   These need entity-id resolution before the query planner can join them."
  [context-in]
  (some (fn [rel]
          (when-let [tuple (first (:tuples rel))]
            (some (fn [[_sym idx]]
                    (let [v (if (da/array? tuple)
                              (aget ^objects tuple idx)
                              (get tuple idx))]
                      (and (sequential? v)
                           (= 2 (count v))
                           (keyword? (first v)))))
                  (:attrs rel))))
        (:rels context-in)))

(defn- resolve-lookup-ref-bindings
  "Resolve lookup-ref values in :in binding relations to entity IDs for joining.
   Returns [context-in' reverse-map] where reverse-map is
   {var-sym {entity-id original-lookup-ref, ...}} for restoring output.
   Returns [context-in nil] when no lookup-refs are present (common fast path)."
  [db context-in]
  (if-not (has-lookup-ref-bindings? context-in)
    [context-in nil]
    (let [reverse-map (volatile! {})
          context-in'
          (update context-in :rels
                  (fn [rels]
                    (mapv (fn [rel]
                            (let [tuples (:tuples rel)]
                              (if (empty? tuples)
                                rel
                                (assoc rel :tuples
                                       (mapv (fn [tuple]
                                               (reduce-kv
                                                (fn [t sym idx]
                                                  (let [v (if (da/array? t)
                                                            (aget ^objects t idx)
                                                            (get t idx))]
                                                    (if (and (sequential? v)
                                                             (= 2 (count v))
                                                             (keyword? (first v)))
                                                      (let [eid (dbu/entid db v)]
                                                         ;; Track reverse mapping for this var
                                                        (vswap! reverse-map update sym assoc eid v)
                                                        (if (da/array? t)
                                                           ;; Copy to Object[] to avoid ArrayStoreException
                                                           ;; (typed arrays like PersistentVector[] can't hold Long)
                                                          #?(:clj
                                                             (let [^objects new-arr (object-array (alength ^objects t))]
                                                               (System/arraycopy ^objects t 0 new-arr 0 (alength ^objects t))
                                                               (aset new-arr (int idx) eid)
                                                               new-arr)
                                                             :cljs
                                                             (let [new-arr (.slice t)]
                                                               (aset new-arr idx eid)
                                                               new-arr))
                                                          (assoc t idx eid)))
                                                      t)))
                                                tuple
                                                (:attrs rel)))
                                             tuples)))))
                          rels)))]
      [context-in' @reverse-map])))

(defn- create-plan-via-ir
  "Build a plan using the IR pipeline: logical IR → lowering.
   `in-cards` is the value-independent :in cardinality seed (see in-card-seed)."
  [db clauses bound-vars rules in-cards]
  (let [logical (logical/build-logical-plan db clauses bound-vars rules)
        plan (lower/lower logical db rules in-cards)]
    plan))

#?(:clj
   (defn- key-has-bigdec?
     "Read-only scan: does `x` (a cache-key fragment) contain a BigDecimal?
   Recurses only plain Clojure collections; records and opaque values
   (Datom, DB, fns, …) are treated as leaves so we never call unsupported
   ops on them. Lets `scale-sensitive-key` skip the (allocating) rebuild for
   the overwhelmingly common BigDecimal-free key. CLJ only — ClojureScript
   has no BigDecimal."
     [x]
     (cond
       (instance? java.math.BigDecimal x) true
       (record? x)                        false
       (map? x)  (reduce-kv (fn [_ k v] (if (or (key-has-bigdec? k) (key-has-bigdec? v))
                                          (reduced true) false))
                            false x)
       ;; indexed loop for vectors (cache keys are vectors of vectors) and a
       ;; reducing scan for sets/seqs — both avoid allocating a lazy seq per node.
       (vector? x) (let [n (count x)]
                     (loop [i 0]
                       (cond (= i n) false
                             (key-has-bigdec? (nth x i)) true
                             :else (recur (unchecked-inc i)))))
       (or (set? x) (seq? x)) (reduce (fn [_ e] (if (key-has-bigdec? e) (reduced true) false))
                                      false x)
       :else                              false)))

(defn scale-sensitive-key
  "Canonicalize a value for use as (part of) a cache key.

   Clojure's `=` and `hash` for BigDecimal are scale-INSENSITIVE
   (compareTo-based): `(= 1.50M 1.500M)` => true and their hashes are equal,
   so they collide as map/vector keys (e.g. `(get {1.50M :a} 1.500M)` => :a).
   But query plans embed the substituted constant and query results carry the
   value's scale (`1.50M` and `1.500M` print differently), so two
   numerically-equal but differently-scaled values MUST map to DIFFERENT cache
   keys — otherwise whichever scale is cached first wins and later queries get
   the wrong scale. See clojure.org/guides/equality.

   Replaces every BigDecimal with a scale-sensitive surrogate
   `[::bigdec <unscaled BigInteger> <scale int>]`, recursing only plain Clojure
   collections (records and opaque values like Datom/DB are left as-is).
   Returns `x` itself when it contains no BigDecimal — no allocation on the
   common path.

   No-op in ClojureScript: JS has no BigDecimal (only doubles), so the
   scale-insensitivity collision cannot arise there."
  [x]
  #?(:cljs x
     :clj
     (if-not (key-has-bigdec? x)
       x
       (letfn [(walk [v]
                 (cond
                   (instance? java.math.BigDecimal v)
                   [::bigdec (.unscaledValue ^java.math.BigDecimal v) (.scale ^java.math.BigDecimal v)]
                   (record? v) v
                   (map? v)    (into (empty v) (map (fn [e] [(walk (key e)) (walk (val e))])) v)
                   (vector? v) (mapv walk v)
                   (set? v)    (into (empty v) (map walk) v)
                   (seq? v)    (doall (map walk v))
                   :else       v))]
         (walk x)))))

(defn get-or-create-plan
  "Get a cached query plan or create a new one. Plans are cached by
   [clauses bound-vars rules-keys in-cards schema-hash] since the plan structure
   (index selection, merge ordering) depends on query shape and schema,
   not on the actual data. `in-cards` (shape-derived, value-independent) is in
   the key only to separate tuple from relation :in bindings (see
   get-or-create-plan body) — it does not make the plan data-dependent.

   `clauses` may embed substituted constants (substitute-consts-with-lookup-refs),
   so the key is run through `scale-sensitive-key` to keep BigDecimals of
   different scale distinct (Clojure `=`/`hash` would otherwise collapse them)."
  [db clauses bound-vars rules in-cards]
  (let [schema-hash (hash (dbi/-schema db))
        ;; `in-cards` is part of the key: it is value-independent (shape-only),
        ;; but it distinguishes bindings the bound-var SET cannot — e.g. a tuple
        ;; [?a ?b] (#{?a ?b}, card 1) from a relation [[?a ?b]] (#{?a ?b}, many)
        ;; — which would otherwise collide on identical clauses + bound-vars.
        cache-key (scale-sensitive-key [clauses bound-vars (when rules rules)
                                        (not-empty in-cards) schema-hash])]
    (if-some [cached (get @plan-cache cache-key nil)]
      cached
      (let [plan (create-plan-via-ir db clauses bound-vars rules in-cards)]
        (vswap! plan-cache assoc cache-key plan)
        plan))))

(defn get-or-create-plan-step
  "Dual-mode plan acquisition. sync? true delegates to get-or-create-plan
   (the only JVM mode). sync? false (cljs) makes PLANNING itself work with
   zero warmup on a cold async-only store: on a plan-cache miss it runs a
   RECORD pass (estimator reads log their requests and return fallbacks;
   that plan is discarded and never cached), fetches every recorded read
   concurrently, then builds the real plan purely against the memo."
  [db clauses bound-vars rules in-cards sync?]
  #?(:clj
     (do (when (false? sync?)
           (throw (ex-info "async planning is not supported on the JVM"
                           {:error :storage/async-unsupported})))
         (get-or-create-plan db clauses bound-vars rules in-cards))
     :cljs
     (if sync?
       (get-or-create-plan db clauses bound-vars rules in-cards)
       (async
        (let [schema-hash (hash (dbi/-schema db))
              cache-key (scale-sensitive-key [clauses bound-vars (when rules rules)
                                              (not-empty in-cards) schema-hash])]
          (if-some [cached (get @plan-cache cache-key nil)]
            cached
            (let [memo (atom {:recording? true :values {} :requests {}})
                  mdb (assoc db estimate/memo-key memo)
                  _ (create-plan-via-ir mdb clauses bound-vars rules in-cards)
                  _ (pca/await (estimate/fetch-estimate-requests-step memo))
                  plan (create-plan-via-ir mdb clauses bound-vars rules in-cards)]
              (vswap! plan-cache assoc cache-key plan)
              plan)))))))

(def ^:dynamic *profile?* false)

(defn- parse-order-by
  "Parse :order-by into a vector of [index direction] pairs.
   Supports: ?var, [?var], [?var :desc], [?var :asc ?var2 :desc], column-idx.
   Returns nil if no ordering, or [[idx :asc] [idx :desc] ...] with find-var indices."
  [order-by find-elements]
  (when order-by
    (let [find-vars (mapv (fn [e]
                            (cond
                              (instance? Variable e) (.-symbol ^Variable e)
                              (instance? Aggregate e) nil
                              :else nil))
                          find-elements)
          resolve-idx (fn [v]
                        (cond
                          (nat-int? v) (do (when (>= v (count find-vars))
                                             (throw (ex-info (str ":order-by column index " v " out of bounds, :find has " (count find-vars) " elements")
                                                             {:index v :find-count (count find-vars)})))
                                           v)
                          (symbol? v) (let [idx (let [fv find-vars n (count fv)]
                                                  (loop [i 0]
                                                    (cond (>= i n) -1
                                                          (= (nth fv i) v) i
                                                          :else (recur (inc i)))))]
                                        (when (neg? idx)
                                          (throw (ex-info (str ":order-by variable " v " not found in :find " find-vars)
                                                          {:var v :find-vars find-vars})))
                                        idx)
                          :else (throw (ex-info (str ":order-by element must be a symbol or column index, got: " (pr-str v))
                                                {:element v}))))
          ;; Normalize to vector
          spec (cond
                 (symbol? order-by) [[order-by :asc]]
                 (nat-int? order-by) [[order-by :asc]]
                 (vector? order-by) (loop [items (seq order-by)
                                           result []]
                                      (if-not items
                                        result
                                        (let [item (first items)]
                                          (cond
                                            (#{:asc :desc} item)
                                            (throw (ex-info ":order-by cannot start with direction" {:spec order-by}))

                                            (or (symbol? item) (nat-int? item))
                                            (let [nxt (second items)]
                                              (if (#{:asc :desc} nxt)
                                                (recur (nnext items) (conj result [item nxt]))
                                                (recur (next items) (conj result [item :asc]))))

                                            :else
                                            (throw (ex-info (str "Invalid :order-by element: " (pr-str item)) {:spec order-by}))))))
                 :else (throw (ex-info (str "Invalid :order-by format: " (pr-str order-by)) {:order-by order-by})))]
      (mapv (fn [[v dir]] [(resolve-idx v) dir]) spec))))

(defn- order-comparator
  "Build a Comparator from parsed order spec [[idx :asc] [idx :desc] ...]."
  ^java.util.Comparator [order-spec]
  (let [n (count order-spec)]
    (if (== n 1)
      ;; Fast path: single key
      (let [[idx dir] (first order-spec)]
        (if (= dir :asc)
          (fn [a b] (compare (nth a idx) (nth b idx)))
          (fn [a b] (compare (nth b idx) (nth a idx)))))
      ;; Multi-key
      (fn [a b]
        (loop [i 0]
          (if (>= i n)
            0
            (let [[idx dir] (nth order-spec i)
                  c (if (= dir :asc)
                      (compare (nth a idx) (nth b idx))
                      (compare (nth b idx) (nth a idx)))]
              (if (zero? c)
                (recur (inc i))
                c))))))))

(defn- apply-order-by
  "Sort a result set by the given order spec. Returns a vector (not a set)
   since ordering is meaningful. Applies offset/limit after sorting.
   Datalog results are already deduplicated, so no set conversion needed."
  [results order-spec offset limit]
  (let [sorted (sort (order-comparator order-spec) results)]
    (cond->> sorted
      offset (drop offset)
      (and limit (pos? limit)) (take limit)
      true vec)))

(declare planner-eligible-db? planner-origin-db connected-components)

(defn explain
  "Returns a human-readable string explaining the query plan.
   Takes the same arguments as `q`. Shows the execution path the dispatcher
   would take (legacy / cartesian-split / direct / columnar-aggregate /
   relation), index selection, scan/merge ordering, recursive rule structure,
   and estimated cardinalities.

   The plan shown is the SAME cached plan execution uses (get-or-create-plan),
   and the database source is resolved exactly like execution: `$` if present,
   else the first planner-eligible source. Caveats explain cannot see:
   a warm query result cache returns without executing at all, and the
   columnar/secondary-index paths depend on a runtime probe.

   Usage:
     (explain '[:find ?e2 :in $ ?e1 % :where (follow ?e1 ?e2)]
              db 1
              '[[(follow ?e1 ?e2) [?e1 :follow ?e2]]
                [(follow ?e1 ?e2) [?e1 :follow ?t] (follow ?t ?e2)]])"
  [query & inputs]
  #?(:clj
     (let [{:keys [query args]} (normalize-q-input query inputs)
           {:keys [qfind qin]} (memoized-parse-query query)
           context-in (-> (Context. [] {} built-in-rules {} default-settings nil)
                          (resolve-ins qin args))
           ;; SAME source resolution as raw-q*: `$` if present, else the first
           ;; planner-eligible db source. (Previously hardcoded '$, which
           ;; NPE'd for queries whose only source is named, e.g. :in $data.)
           db (let [sources (:sources context-in)]
                (or (get sources '$)
                    (some (fn [[_k v]] (when (and (dbu/db? v) (planner-eligible-db? v)) v))
                          sources)))
           use-planner? (and (some? db) (dbu/db? db) (planner-eligible-db? db)
                             (some? (planner-origin-db db)))
           find-vars (mapv #(.-symbol ^Variable %) (filter #(instance? Variable %) (dpip/find-elements qfind)))
           bound-vars (context-bound-vars context-in)
           rules (not-empty (:rules context-in))
           header (fn [engine path]
                    (str "=== Query Plan ===\n"
                         "find: " (pr-str find-vars) "\n"
                         "bound: " (pr-str bound-vars) "\n"
                         (when rules
                           (str "rules: " (pr-str (vec (keys rules))) "\n"))
                         (when-let [c (:consts context-in)]
                           (when (seq c)
                             (str "consts: " (pr-str c) "\n")))
                         "engine: " engine "\n"
                         (when path (str "path: " path "\n"))
                         "---\n"))]
       (if-not use-planner?
         (str (header "legacy (relational)" nil)
              (cond
                (nil? db) "no planner-eligible database source — clauses resolve via the relational fixpoint loop\n"
                (nil? (planner-origin-db db)) "nested temporal wrapper (e.g. (history (as-of …))) — planner not applicable\n"
                :else "database not planner-eligible\n"))
         (let [;; Temporal wrappers (HistoricalDB/AsOfDB/SinceDB) carry no own
               ;; :eavt/:avet indexes; plan against the origin, mirroring
               ;; execute-plan's (planner-origin-db …).
               plan-db (planner-origin-db db)
               clauses (substitute-consts-with-lookup-refs db (:where query) (:consts context-in))
               ;; The SAME cached plan execution will use — create-plan-via-ir
               ;; here could diverge from a previously cached plan.
               plan (get-or-create-plan plan-db clauses bound-vars rules (in-card-seed qin))
               find-elements (dpip/find-elements qfind)
               has-aggs? (some #(instance? Aggregate %) find-elements)
               has-pull? (some #(instance? Pull %) find-elements)
               find-rel? (instance? FindRel qfind)
               no-in-rels? (empty? (:rels context-in))
               split? (and find-rel? (not (:with query)) (not has-aggs?) (not has-pull?)
                           (> (count (:components (connected-components
                                                   (:where query)
                                                   (set (keys (:consts context-in)))
                                                   find-vars)))
                              1))
               direct? (and find-rel? (not (:with query)) (not has-aggs?) (not has-pull?)
                            no-in-rels?
                            (let [cdf (requiring-resolve 'datahike.query.execute/can-direct-fuse?)]
                              (boolean (cdf plan find-vars (:consts context-in)))))
               columnar? (and has-aggs? find-rel? (not (:with query)) (not has-pull?) no-in-rels?)
               path (cond
                      split?    "cartesian-split — disjoint components run as sub-queries and merge"
                      direct?   "direct — fused scans write straight to the result set"
                      columnar? "columnar-aggregate if a secondary/columnar engine accepts (runtime probe); else relation"
                      :else     "relation — execute-plan over relations")]
           (str (header "planned" path) (format-plan-ops (:ops plan) 0) "\n"))))
     :cljs (throw (ex-info "explain is not supported in ClojureScript" {}))))

;; ---------------------------------------------------------------------------
;; Query execution paths — split into small functions for JIT optimization

(defn- apply-result-transforms
  "Apply offset/limit/order-by/return-maps to a result set."
  [result order-spec offset limit qreturnmaps]
  (let [result (cond
                 order-spec
                 (apply-order-by result order-spec offset limit)

                 (or offset (and limit (pos? limit)))
                 (into #{}
                       (comp (if offset (drop offset) identity)
                             (if (and limit (pos? limit)) (take limit) identity))
                       result)

                 :else result)]
    (cond->> result
      qreturnmaps (convert-to-return-maps qreturnmaps))))

(defn- plan-sub-ops
  "Flatten plan ops into leaf sub-ops (expanding entity-groups)."
  [plan]
  (mapcat (fn [op]
            (if (= :entity-group (:op op))
              (cons (:scan-op op) (:merge-ops op))
              [op]))
          (:ops plan)))

#?(:clj
   (def ^:private pred-sym->stratum-op
     "Map from Clojure predicate symbols to stratum WHERE operators."
     {'> :>, '< :<, '>= :>=, '<= :<=, '= :=, 'not= :!=,
      'clojure.core/> :>, 'clojure.core/< :<, 'clojure.core/>= :>=,
      'clojure.core/<= :<=, 'clojure.core/= :=, 'clojure.core/not= :!=}))

#?(:clj
   (defn- execute-filter-pattern-via-pss
     "Execute a filter pattern (with v-ground) via PSS to collect entity IDs.
      Returns a RoaringBitmap of matching entity IDs."
     [db sub-op]
     (let [attr (or (:attr sub-op) (get-in sub-op [:schema-info :attr]))
           v-ground (:v-ground sub-op)
           datoms (di/-slice (get db :aevt)
                             (dbu/components->pattern db :aevt [attr v-ground] const/e0 const/tx0)
                             (dbu/components->pattern db :aevt [attr v-ground] const/emax const/txmax)
                             :aevt)
           bs (es/entity-bitset)]
       (doseq [^datahike.datom.Datom d datoms]
         (es/entity-bitset-add! bs (.-e d)))
       bs)))

#?(:clj
   (defn- resolve-stratum-fn
     "Lazily resolve a var from datahike.index.secondary.stratum and deref it.
      Returns the value (fn or data) or nil if stratum is not on the classpath."
     [sym]
     (try
       (when-let [v (requiring-resolve (symbol "datahike.index.secondary.stratum" (name sym)))]
         @v)
       (catch Exception _ nil))))

#?(:clj
   (defn- try-secondary-index-aggregate
     "Path 1: Push aggregate directly to a secondary index (e.g. stratum).
      Supports partial coverage: uncovered filter patterns are resolved via PSS
      to produce a RoaringBitmap entity-filter. Predicates on covered columns are
      translated to stratum WHERE clauses.
      Returns result tuples or nil if not applicable."
     [db plan find-elements]
     (try
       (let [sec-indices (:secondary-indices db)]
         (when (seq sec-indices)
           (let [ref->ident (:ref-ident-map db)
                 resolve-attr (fn [a] (if (and ref->ident (number? a))
                                        (get ref->ident a a)
                                        a))
                 sub-ops (plan-sub-ops plan)
                 all-attrs (into #{}
                                 (comp (keep (fn [op]
                                               (or (:attr op)
                                                   (get-in op [:schema-info :attr]))))
                                       (map resolve-attr))
                                 sub-ops)
                 ;; Find best IColumnarAggregate index — prefer full coverage, accept partial
                 sec-agg-protocol sec/IColumnarAggregate
                 indexed-attrs-fn sec/-indexed-attrs
                 agg-indices (keep (fn [[_idx-ident idx]]
                                     (when (satisfies? sec-agg-protocol idx)
                                       (let [indexed (indexed-attrs-fn idx)
                                             covered (clojure.set/intersection all-attrs indexed)]
                                         {:idx idx :indexed indexed :covered covered
                                          :coverage-ratio (if (empty? all-attrs) 0
                                                              (/ (count covered) (count all-attrs)))})))
                                   sec-indices)
                 ;; Pick the index with most coverage (>0)
                 best-idx-info (when (seq agg-indices)
                                 (apply max-key :coverage-ratio agg-indices))
                 best-idx (when (and best-idx-info (pos? (:coverage-ratio best-idx-info)))
                            (:idx best-idx-info))
                 indexed-attrs (when best-idx (:indexed best-idx-info))
                 stratum-compat? (when best-idx
                                   (resolve-stratum-fn 'stratum-compatible-aggs?))
                 agg-ops (into [] (keep (fn [fe]
                                          (when (instance? Aggregate fe)
                                            [(keyword (name (.-symbol ^PlainSymbol (.-fn ^Aggregate fe))))])))
                               find-elements)]
             (when (and best-idx stratum-compat? (stratum-compat? agg-ops))
               ;; Determine which sub-ops provide values needed by :find vs which are filter-only
               (let [col-agg-fn sec/-columnar-aggregate
                     stratum-agg-ops (resolve-stratum-fn 'stratum-agg-ops)
                     attr-col-key (resolve-stratum-fn 'attr-col-key)
                     var->col (into {}
                                    (keep (fn [sub-op]
                                            (let [clause (:clause sub-op)
                                                  v-sym (when (and (sequential? clause) (>= (count clause) 3))
                                                          (nth clause 2))
                                                  a-raw (when (and (sequential? clause) (>= (count clause) 2))
                                                          (nth clause 1))]
                                              (when (and v-sym (symbol? v-sym) a-raw (not (symbol? a-raw)))
                                                [v-sym (attr-col-key (resolve-attr a-raw))]))))
                                    sub-ops)
                     entity-vars (into #{}
                                       (keep (fn [op]
                                               (when (= :entity-group (:op op))
                                                 (:entity-var op))))
                                       (:ops plan))
                     var->col (into var->col (map (fn [ev] [ev :eid])) entity-vars)
                     ;; Check that all :find vars map to columns in the index
                     find-vars (into #{}
                                     (keep (fn [fe]
                                             (cond
                                               (instance? Variable fe) (.-symbol ^Variable fe)
                                               (instance? Aggregate fe)
                                               (let [agg-args (.-args ^Aggregate fe)]
                                                 (when (and (seq agg-args) (instance? Variable (last agg-args)))
                                                   (.-symbol ^Variable (last agg-args)))))))
                                     find-elements)
                     find-col-attrs (into #{}
                                          (keep (fn [v]
                                                  (let [col (get var->col v)]
                                                    (when (and col (not= col :eid))
                                                      ;; Find the attr for this col-key
                                                      (some (fn [sub-op]
                                                              (let [a-raw (or (:attr sub-op) (get-in sub-op [:schema-info :attr]))]
                                                                (when (and a-raw (= col (attr-col-key (resolve-attr a-raw))))
                                                                  (resolve-attr a-raw))))
                                                            sub-ops)))))
                                          find-vars)
                     ;; All value-providing columns must be in the index
                     all-find-attrs-covered? (every? indexed-attrs find-col-attrs)]
                 (when all-find-attrs-covered?
                   ;; Split sub-ops: covered (in index) vs uncovered (need PSS)
                   (let [covered-ops (filterv (fn [sub-op]
                                                (let [a (resolve-attr (or (:attr sub-op) (get-in sub-op [:schema-info :attr])))]
                                                  (or (nil? a) (contains? indexed-attrs a))))
                                              sub-ops)
                         uncovered-filter-ops (filterv (fn [sub-op]
                                                         (let [a (resolve-attr (or (:attr sub-op) (get-in sub-op [:schema-info :attr])))]
                                                           (and a
                                                                (not (contains? indexed-attrs a))
                                                                (:v-ground sub-op))))
                                                       sub-ops)
                         ;; Execute uncovered filter patterns via PSS → RoaringBitmap
                         entity-filter (when (seq uncovered-filter-ops)
                                         (reduce (fn [acc sub-op]
                                                   (let [bs (execute-filter-pattern-via-pss db sub-op)]
                                                     (if acc
                                                       (es/entity-bitset-and acc bs)
                                                       bs)))
                                                 nil
                                                 uncovered-filter-ops))
                         group-keys (vec (keep (fn [fe]
                                                 (when-not (instance? Aggregate fe)
                                                   (get var->col (.-symbol ^Variable fe))))
                                               find-elements))
                         stratum-aggs (vec (keep (fn [fe]
                                                   (when (instance? Aggregate fe)
                                                     (let [agg-sym (keyword (name (.-symbol ^PlainSymbol (.-fn ^Aggregate fe))))
                                                           op (get stratum-agg-ops agg-sym)
                                                           agg-args (.-args ^Aggregate fe)
                                                           agg-col (when (and (seq agg-args)
                                                                              (instance? Variable (last agg-args))
                                                                              (not= agg-sym :count))
                                                                     (get var->col (.-symbol ^Variable (last agg-args))))]
                                                       (if agg-col [op agg-col] [op]))))
                                                 find-elements))
                         ;; WHERE: covered v-ground equality constraints
                         ;; Convert keyword ground values to strings to match stratum's
                         ;; dict-encoded column storage (keywords stored via str)
                         where-equality (vec (keep (fn [sub-op]
                                                     (when-let [v-ground (:v-ground sub-op)]
                                                       (let [a (resolve-attr (or (:attr sub-op) (get-in sub-op [:schema-info :attr])))]
                                                         (when (contains? indexed-attrs a)
                                                           [:= (attr-col-key a) (if (keyword? v-ground) (str v-ground) v-ground)]))))
                                                   sub-ops))
                         ;; WHERE: translate predicates on covered columns.
                         ;; Collect both top-level predicate ops AND attached-preds
                         ;; on scan/entity-group ops (predicates that were attached
                         ;; during pushdown and are no longer standalone ops).
                         pred-ops (into (vec (filter #(= :predicate (:op %)) (:ops plan)))
                                        (mapcat (fn [op]
                                                  (when-let [ap (:attached-preds op)]
                                                    (filter #(= :predicate (:op %)) ap)))
                                                (:ops plan)))
                         where-predicates (vec (keep (fn [pred-op]
                                                       (let [fn-sym (:fn-sym pred-op)
                                                             stratum-op (get pred-sym->stratum-op fn-sym)
                                                             args (:args pred-op)]
                                                         (when (and stratum-op (= 2 (count args)))
                                                           (let [[a b] args
                                                                 ;; One must be a var mapped to a covered column, other a constant
                                                                 ;; Returns [effective-op col-key const-val] or nil
                                                                 result
                                                                 (cond
                                                                   ;; a=var, b=const → normal direction
                                                                   (and (symbol? a) (not (symbol? b))
                                                                        (get var->col a)
                                                                        (let [col (get var->col a)]
                                                                          (some (fn [sub-op]
                                                                                  (let [attr (resolve-attr (or (:attr sub-op) (get-in sub-op [:schema-info :attr])))]
                                                                                    (when (and attr (= col (attr-col-key attr))
                                                                                               (contains? indexed-attrs attr))
                                                                                      true)))
                                                                                sub-ops)))
                                                                   [stratum-op (get var->col a) b]

                                                                   ;; b=var, a=const → flip operator direction
                                                                   (and (symbol? b) (not (symbol? a))
                                                                        (get var->col b)
                                                                        (some (fn [sub-op]
                                                                                (let [attr (resolve-attr (or (:attr sub-op) (get-in sub-op [:schema-info :attr])))]
                                                                                  (when (and attr (= (get var->col b) (attr-col-key attr))
                                                                                             (contains? indexed-attrs attr))
                                                                                    true)))
                                                                              sub-ops))
                                                                   (let [flipped (case stratum-op
                                                                                   :> :<, :< :>, :>= :<=, :<= :>=
                                                                                   stratum-op)]
                                                                     [flipped (get var->col b) a])

                                                                   :else nil)]
                                                             result))))
                                                     pred-ops))
                         ;; Guard: if any predicate couldn't be translated, bail out
                         ;; (otherwise the predicate is silently dropped → wrong results)
                         _ (when (and (seq pred-ops) (< (count where-predicates) (count pred-ops)))
                             (throw (ex-info "Not all predicates translatable to stratum WHERE"
                                             {:pred-count (count pred-ops)
                                              :translated (count where-predicates)})))
                         where-constraints (into where-equality where-predicates)
                         query-spec (cond-> {:agg stratum-aggs}
                                      (seq group-keys) (assoc :group group-keys)
                                      (seq where-constraints) (assoc :where where-constraints))
                         result-maps (col-agg-fn best-idx query-spec entity-filter)
                         col-agg-adapter (resolve-stratum-fn 'columnar-aggregate-from-maps)]
                     (col-agg-adapter result-maps group-keys stratum-aggs find-elements))))))))
       (catch Exception e
         (when (dt/rethrowable? e) (throw e))
         (log/warn "secondary-idx-agg not applicable:" (.getMessage e) (pr-str (type e)))
         nil))))

#?(:clj
   (defn- try-columnar-aggregate
     "Path 2: Aggregate via PSS scan + column extraction → stratum.
      Returns result tuples or nil if not applicable."
     [plan db find-elements cancel]
     (try
       (let [agg-ops (vec (keep (fn [fe]
                                  (when (instance? Aggregate fe)
                                    [(keyword (name (.-symbol ^PlainSymbol (.-fn ^Aggregate fe))))]))
                                find-elements))]
         (when-let [compat-fn (resolve-stratum-fn 'stratum-compatible-aggs?)]
           (when (compat-fn agg-ops)
             (when-let [col-agg-fn (resolve-stratum-fn 'columnar-aggregate)]
               (let [exec-col-agg (requiring-resolve 'datahike.query.execute/execute-columnar-aggregate)]
                 (exec-col-agg plan db find-elements
                               (fn [column-map group-keys agg-specs]
                                 (col-agg-fn column-map group-keys agg-specs find-elements))
                               cancel))))))
       (catch Exception e
         (when (dt/rethrowable? e) (throw e))
         (log/debug "columnar-aggregate not applicable:" (.getMessage e))
         nil))))

;; ============================================================================
;; Component partitioning — Cartesian-product detection
;; ============================================================================
;;
;; A query whose where-clauses form two-or-more sub-sets sharing no free
;; variables is semantically a Cartesian product of those sub-queries.
;; The planner+executor assume a connected join graph, so we detect this
;; case at the dispatch level and recursively delegate each component to
;; the existing pipeline, Cartesian-merging the results.
;;
;; Connectivity rule
;;   - Free vars in NON-predicate clauses are unioned together (every
;;     free var in the clause joins every other free var).
;;   - Pure :predicate clauses do not contribute to unions. After all
;;     joiners are processed, a predicate whose vars span more than one
;;     component becomes a post-Cartesian filter.
;;   - Externally bound vars (:in bindings) are constants — they do not
;;     connect clauses.
;;   - Anonymous vars (symbols starting with _) are not free vars and do
;;     not contribute.

(defn- clause-meaningful-vars
  "Free vars in a classified clause that participate in connectivity.
   Excludes externally bound vars (treated as constants)."
  [classified bound-vars]
  (let [vs (:vars classified)]
    (if (seq bound-vars)
      (into #{} (remove bound-vars) vs)
      vs)))

(defn- joiner-clause?
  "True if this clause contributes its free vars to the connectivity
   union. Pure predicates do not — they apply as filters and can span
   components without merging them."
  [classified]
  (not= :predicate (:type classified)))

(defn- uf-find
  "Path-compressed find on a parent map. Returns [updated-parent root]."
  [parent x]
  (loop [trail [] x x]
    (let [p (get parent x x)]
      (if (= p x)
        [(reduce (fn [m n] (assoc m n x)) parent trail) x]
        (recur (conj trail x) p)))))

(defn- uf-union
  "Union two elements in a parent map."
  [parent a b]
  (let [[p1 ra] (uf-find parent a)
        [p2 rb] (uf-find p1 b)]
    (if (= ra rb) p2 (assoc p2 ra rb))))

(defn- connected-components
  "Partition where-clauses into connected components by shared free vars.

   Returns {:components [{:clauses [...], :find-vars [...]}, ...]
            :post-filters [predicate-clause ...]}.

   `bound-vars` is the set of externally bound vars (from :in).
   `find-vars` is the original ordered seq of result-projection vars.

   Component order is stable: components appear in the order their
   first source clause appears. Each component's :find-vars preserves
   the original find order, filtered to vars produced by that
   component's clauses."
  [clauses bound-vars find-vars]
  (let [classified  (mapv analyze/classify-clause clauses)
        clause-vars (mapv #(clause-meaningful-vars % bound-vars) classified)
        all-vars    (into #{} cat clause-vars)
        ;; Step 1: union vars within each joiner clause.
        uf (reduce
            (fn [uf [ci vs]]
              (if (and (joiner-clause? ci) (> (count vs) 1))
                (let [[v0 & rest-vs] (vec vs)]
                  (reduce (fn [p v] (uf-union p v0 v)) uf rest-vs))
                uf))
            (into {} (map (fn [v] [v v])) all-vars)
            (map vector classified clause-vars))
        ;; Step 2: assign each clause a representative root (or a
        ;; sentinel for clauses with no meaningful vars — those become
        ;; global gates attached to the first component).
        no-var-root   ::no-vars
        clause-roots  (mapv
                       (fn [vs]
                         (if (empty? vs)
                           no-var-root
                           (second (uf-find uf (first vs)))))
                       clause-vars)
        ;; Step 3: identify post-filter predicates (those that touch >1 root).
        post-filter? (mapv
                      (fn [ci vs]
                        (and (= :predicate (:type ci))
                             (> (count (into #{} (map #(second (uf-find uf %))) vs)) 1)))
                      classified clause-vars)
        ;; Step 4: build components in source order. Component roots are
        ;; encountered in first-clause order (skipping post-filters).
        roots-in-order (vec (distinct
                             (keep-indexed
                              (fn [i root]
                                (when-not (nth post-filter? i) root))
                              clause-roots)))
        ;; Special handling: no-var clauses (e.g. fully-bound patterns)
        ;; attach to the first non-sentinel component if any exist.
        primary-root  (first (remove #(= no-var-root %) roots-in-order))
        components
        (mapv
         (fn [root]
           (let [own-idxs (vec (keep-indexed
                                (fn [i r]
                                  (when (and (not (nth post-filter? i))
                                             (or (= r root)
                                                 (and (= primary-root root)
                                                      (= r no-var-root))))
                                    i))
                                clause-roots))
                 cs (mapv #(nth clauses %) own-idxs)
                 vs (into #{} (mapcat #(nth clause-vars %)) own-idxs)
                 fvs (vec (filter vs find-vars))]
             {:clauses cs :vars vs :find-vars fvs}))
         (remove #(= no-var-root %) roots-in-order))
        ;; If there are ONLY no-var clauses (no real components), keep
        ;; them as one degenerate component so the user's query still runs.
        components (if (and (empty? components) (some #(= no-var-root %) roots-in-order))
                     [{:clauses clauses :vars #{} :find-vars (vec find-vars)}]
                     components)
        post-filters (vec (keep-indexed
                           (fn [i c] (when (nth post-filter? i) c))
                           clauses))]
    {:components components
     :post-filters post-filters}))

;; ============================================================================
;; Cartesian merge and post-filter evaluation
;; ============================================================================

(defn- cartesian-product-seq
  "Lazy seq of all combinations across N collections. Each result is a
   vector of one element per input collection. Empty input → singleton
   empty combination; any empty collection → empty seq."
  [colls]
  (if (empty? colls)
    (list [])
    (for [x (first colls)
          xs (cartesian-product-seq (rest colls))]
      (into [x] xs))))

(defn- cartesian-merge
  "Build wide result tuples in `target-vars` order from a vector of
   `{:tuples seq-of-vectors :vars [var ...]}` component results.

   For each combination of one tuple per component, project to
   target-vars by looking up each target-var in the per-component
   variable layout."
  [component-results target-vars]
  (let [;; var → [component-idx position-in-component]
        var-locator (into {}
                          (for [[ci {:keys [vars]}] (map-indexed vector component-results)
                                [pos v] (map-indexed vector vars)]
                            [v [ci pos]]))
        component-tuples (mapv :tuples component-results)]
    (if (some empty? component-tuples)
      #{}
      (into #{}
            (map (fn [combo]
                   (mapv (fn [v]
                           (let [[ci pos] (var-locator v)]
                             (nth (nth combo ci) pos)))
                         target-vars)))
            (cartesian-product-seq component-tuples)))))

(defn- resolve-pred-symbol
  "Resolve a predicate symbol used in a post-filter clause.

   In CLJ, user-defined predicates that aren't built-in are looked up
   via runtime resolution. In CLJS, `resolve` is compile-time only, so
   we restrict to the built-ins maps; user-defined cross-component
   filter predicates are CLJ-only and `eval-post-filter` raises a
   targeted error if encountered."
  [sym]
  (or (get built-ins sym)
      (get clj-core-built-ins sym)
      #?(:clj  (when (symbol? sym)
                 (some-> (clojure.core/resolve sym) deref))
         :cljs nil)))

(defn- eval-post-filter
  "Apply a single predicate post-filter to a set of wide tuples.
   `pred-clause` is [(fn-sym arg ...)]; each arg is either a free var
   (looked up in `var->idx`) or a constant value.
   `consts` is the {:in var → value} binding map: when the predicate
   position is itself a variable bound there (e.g. `:in $ ?pred` +
   `[(?pred ?a)]`), it resolves to the supplied function value."
  [tuples var->idx pred-clause consts]
  ;; No tuples to test → nothing to resolve or apply. Short-circuits the
  ;; case where an upstream component matched nothing (so the predicate,
  ;; whose function may be unresolvable, is never actually invoked).
  (if (empty? tuples)
    tuples
    (let [call    (first pred-clause)
          fn-sym  (first call)
          args    (rest call)
          pred-fn (or (when (and (symbol? fn-sym)
                                 (analyze/free-var? fn-sym)
                                 (contains? consts fn-sym))
                        (get consts fn-sym))
                      (resolve-pred-symbol fn-sym))]
      (when-not pred-fn
        (throw (ex-info (str "Cannot resolve predicate in cross-component post-filter: " fn-sym
                             #?(:cljs
                                " (CLJS-only limitation: user-defined predicate functions are not resolvable at runtime — use a built-in comparison or restructure the query to avoid the cross-component span)"
                                :clj nil))
                        {:clause pred-clause})))
      (let [arg-readers (mapv (fn [a]
                                (if (and (symbol? a)
                                         (analyze/free-var? a))
                                  (let [idx (get var->idx a)]
                                    (when (nil? idx)
                                      (throw (ex-info (str "Post-filter references unknown var: " a)
                                                      {:clause pred-clause})))
                                    (fn [t] (nth t idx)))
                                  (constantly a)))
                              args)]
        (into #{} (filter (fn [t]
                            (apply pred-fn (map #(% t) arg-readers))))
              tuples)))))

(defn- apply-post-filters
  "Apply each post-filter clause in sequence to the merged tuple set.
   `consts` is the {:in var → value} binding map, threaded to
   `eval-post-filter` so a predicate passed as an :in parameter resolves."
  [tuples target-vars post-filters consts]
  (if (empty? post-filters)
    tuples
    (let [var->idx (into {} (map-indexed (fn [i v] [v i])) target-vars)]
      (reduce (fn [ts pf] (eval-post-filter ts var->idx pf consts))
              tuples
              post-filters))))

;; ============================================================================

(defn- planner-eligible-db?
  "Check if db is eligible for the query planner.
   Accepts regular DB, all temporal wrappers (AsOfDB, SinceDB, HistoricalDB),
   and FilteredDB over an eligible db (its execution routes through the
   contextual per-clause reads, which apply the filter predicate — the fused
   raw-index kernels are gated off for filtered dbs).
   Date-based time-points are resolved to tx-ids at execution time via AVET lookup."
  [db]
  (or (instance? DB db)
      (and (or (instance? SinceDB db)
               (instance? AsOfDB db)
               (instance? HistoricalDB db))
           (planner-eligible-db? (dbi/-origin db)))
      (and (instance? FilteredDB db)
           (planner-eligible-db? (.-unfiltered-db ^FilteredDB db)))))

(defn- planner-origin-db
  "The innermost plain DB of an arbitrary wrapper stack — temporal wrappers
   and FilteredDB recurse (planning sees the origin's stats; execution
   applies the composed visibility via the normalized temporal-info).
   Returns nil only for stacks not bottoming out on a plain DB."
  [db]
  (cond
    (instance? DB db) db
    (instance? FilteredDB db) (planner-origin-db (.-unfiltered-db ^FilteredDB db))
    (or (instance? SinceDB db) (instance? AsOfDB db) (instance? HistoricalDB db))
    (planner-origin-db (dbi/-origin db))
    :else nil))

(defn- execute-planned-direct
  "Direct HashSet path: write tuples directly, no Relations.
   Returns result set or nil if not eligible."
  [plan db qfind find-elements context-in query stats? qreturnmaps sync?]
  (async+sync sync?
              (let [direct-eligible? (and (instance? FindRel qfind)
                                          (not stats?)
                              ;; the fused HashSet path applies fns via post-apply-fns,
                              ;; which doesn't accumulate :fn-counts — route counting
                              ;; queries through the relation path (bind-by-fn) instead.
                                          (not (:count-fns? context-in))
                                          (not qreturnmaps)
                                          (not (:with query))
                                          (not-any? #(instance? Aggregate %) find-elements)
                                          (not-any? #(instance? Pull %) find-elements)
                                          (empty? (:rels context-in)))]
                (when direct-eligible?
      ;; requiring-resolve is cheap after first call: just a ns-map lookup via resolve,
      ;; since the namespace is already loaded. No need to cache the resolved var.
                  (let [exec-direct #?(:clj (requiring-resolve 'datahike.query.execute/execute-plan-direct)
                                       :cljs execute/execute-plan-direct)
                        find-var-syms (mapv (fn [^Variable el] (.-symbol el)) (:elements qfind))]
                    (pca/await (exec-direct plan db find-var-syms nil (:consts context-in) (:cancel context-in) sync?)))))))

(defn- post-process-result
  "Shared post-processing pipeline for both planned-relation and legacy paths.
   Applies :with truncation, aggregation, pull, post-process, return-maps,
   ordering, offset/limit, and stats wrapping.
   cljs dual: sync? false yields an async expression — the pull step's index
   walks are awaited; everything else is pure."
  ([deduped context-in context-out query qfind find-elements
    result-arity order-spec offset limit stats? qreturnmaps]
   (post-process-result deduped context-in context-out query qfind find-elements
                        result-arity order-spec offset limit stats? qreturnmaps true))
  ([deduped context-in context-out query qfind find-elements
    result-arity order-spec offset limit stats? qreturnmaps sync?]
   (async+sync sync?
               (let [pre (cond->> deduped
                           (:with query)                                 (mapv #(subvec % 0 result-arity))
                           (some #(instance? Aggregate %) find-elements) (aggregate find-elements context-in))
                     pulled (if (some #(instance? Pull %) find-elements)
                              (pca/await (pull find-elements context-in pre sync?))
                              pre)]
                 (cond->> pulled
                   true                                          (-post-process qfind)
                   qreturnmaps                                   (convert-to-return-maps qreturnmaps)
                   order-spec                                    (#(apply-order-by % order-spec offset limit))
                   (and (not order-spec) (or offset (and limit (pos? limit))))
                   (into #{}
                         (comp (if offset (drop offset) identity)
                               (if (and limit (pos? limit)) (take limit) identity)))
                   stats?                                        (#(-> context-out
                                                                       (dissoc :rels :sources :settings :cancel :join-source)
                                                                       (update :rules
                                                                               (fn [rs]
                                                                  ;; Subtract built-ins; stats only show user-supplied rules.
                                                                                 (apply dissoc rs (keys built-in-rules))))
                                                                       (assoc :ret % :query query))))))))

(defn- with-fn-counts
  "Surface the engine-collected {fn-sym → invocations} map (accumulated in the
   threaded context by bind-by-fn under :count-fns?) as :fn-counts metadata on
   `result`, when present and the result supports metadata."
  [context-out result]
  (if-let [fc (:fn-counts context-out)]
    (cond-> result (coll? result) (vary-meta assoc :fn-counts fc))
    result))

(defn- execute-planned-relation
  "Relation path: fused scan → collect → dedup → aggregate/pull.
   Returns final result."
  [plan db qfind find-elements context-in query all-vars
   result-arity lookup-ref-reverse-map order-spec offset limit
   stats? qreturnmaps sync?]
  ;; ONE dual body — only the direct-rel probe is awaited; the execute-plan
  ;; fallback stays synchronous until B2 (its cold reads fault, by design).
  (async+sync sync?
              (let [exec-direct-rel #?(:clj (requiring-resolve 'datahike.query.execute/execute-plan-direct-rel)
                                       :cljs execute/execute-plan-direct-rel)
        ;; Only take the direct-rel fast path when there are NO input relations.
        ;; That path executes the plan from scratch and `collapse-rels`-joins the
        ;; result with the :in rels AFTERWARD, which defeats sideways-information-
        ;; passing: a pattern whose value/entity var is bound by an :in binding
        ;; would scan its whole attribute instead of probing the bound values.
        ;; execute-plan threads context-in into execute-fused-scan-rel, whose SIP
        ;; extracts a probe-set from the input rels and uses AVET/EAVT seeks
        ;; (jobtech batched lookups: 43ms -> 0.26ms at small batch sizes). This
        ;; mirrors the `(empty? (:rels context-in))` gate already in
        ;; execute-planned-direct.
                    fused-rel (when (and (empty? (:rels context-in))
                             ;; fused direct-rel reads raw indexes — a FilteredDB
                             ;; must go through execute-plan's contextual reads
                                         (not (instance? FilteredDB db)))
                                (try (pca/await (exec-direct-rel plan db (:cancel context-in) sync?))
                                     (catch #?(:clj Exception :cljs :default) e
                           ;; storage faults / cancellation must escape — a
                           ;; silent fallback would re-execute (and mask the
                           ;; fault from the async machinery)
                                       (when (dt/rethrowable? e) (throw e))
                                       (log/debug "fused-scan-rel not applicable:" #?(:clj (.getMessage ^Exception e) :cljs (str e)))
                                       nil)))
                    context-out (if fused-rel
                                  (update context-in :rels rel/collapse-rels fused-rel (join-env context-in))
                                  (pca/await
                                   (#?(:clj (requiring-resolve 'datahike.query.execute/execute-plan)
                                       :cljs execute/execute-plan) plan context-in db sync?)))
                    resultset (collect context-out all-vars)
                    deduped (if (and (not order-spec)
                                     (:unique-results? context-out)
                                     (not offset)
                                     (or (nil? limit) (neg? limit)))
                              (set resultset)
                              (into #{} resultset))
                    deduped (if lookup-ref-reverse-map
                              (let [var-vec (vec all-vars)
                                    idx-maps (keep-indexed
                                              (fn [i v]
                                                (when-let [rm (get lookup-ref-reverse-map v)]
                                                  [i rm]))
                                              var-vec)]
                                (if (seq idx-maps)
                                  (into #{}
                                        (map (fn [tuple]
                                               (reduce (fn [t [idx rm]]
                                                         (let [eid (nth t idx)]
                                                           (if-let [orig (get rm eid)]
                                                             (assoc t idx orig)
                                                             t)))
                                                       tuple
                                                       idx-maps)))
                                        deduped)
                                  deduped))
                              deduped)]
                (with-fn-counts context-out
                  (pca/await
                   (post-process-result deduped context-in context-out query qfind find-elements
                                        result-arity order-spec offset limit stats? qreturnmaps sync?))))))

(defn- execute-legacy
  "Legacy engine path: -q → collect → dedup → aggregate/pull."
  [context-in query qfind find-elements all-vars result-arity
   order-spec offset limit stats? qreturnmaps]
  (let [context-out (-q context-in (:where query))
        resultset (collect context-out all-vars)
        deduped (into #{} resultset)]
    (with-fn-counts context-out
      (post-process-result deduped context-in context-out query qfind find-elements
                           result-arity order-spec offset limit stats? qreturnmaps))))

(defn- raw-q* [{:keys [query args offset limit order-by stats? count-fns? settings cancel
                       disable-planner? profile? sync?]
                :or {sync? true} :as _query-map}]
  ;; ONE dual spine (v1 scope): only the direct fused path awaits — plan-time
  ;; reads (estimates, lookup-refs, date resolution), the relation fallback,
  ;; the legacy engine and cartesian-split sub-queries stay synchronous. On a
  ;; WARM store every shape therefore works in async mode (sync-completion on
  ;; the trampoline); on a COLD async-only store, fused shapes stream and the
  ;; rest surface the decorated :storage/sync-read-unavailable fault.
  (async+sync sync?
              (let [t0 (when profile? #?(:clj (System/nanoTime) :cljs (* 1000 (.getTime (js/Date.)))))
                    settings (merge default-settings settings)
                    {:keys [qfind qwith qreturnmaps qin]} (memoized-parse-query query)
                    t1 (when profile? #?(:clj (System/nanoTime) :cljs (* 1000 (.getTime (js/Date.)))))
                    context-in (-> (if stats?
                                     (StatContext. [] {} built-in-rules {} [] settings cancel)
                                     (Context. [] {} built-in-rules {} settings cancel))
                                   (resolve-ins qin args)
                       ;; When set, bind-by-fn accumulates {fn-sym → invocations}
                       ;; into the threaded context :fn-counts; surfaced as result
                       ;; metadata. (Extra defrecord keys persist via the extmap.)
                                   (cond-> count-fns? (assoc :count-fns? true)))
                    t2 (when profile? #?(:clj (System/nanoTime) :cljs (* 1000 (.getTime (js/Date.)))))

                    all-vars      (concat (dpi/find-vars qfind) (map :symbol qwith))
                    find-elements (dpip/find-elements qfind)
                    result-arity  (count find-elements)
                    order-spec    (when (and order-by (instance? FindRel qfind))
                                    (parse-order-by order-by find-elements))

        ;; async: rewrite Date-based as-of/since wrappers to numeric time-points
        ;; (one awaited txInstant scan each) so temporal filtering stays on the
        ;; pure txpred path instead of the per-tx txInstant reads that fault cold
                    context-in #?(:clj context-in
                                  :cljs (if sync?
                                          context-in
                                          (let [srcs (:sources context-in)
                                                srcs' (loop [ks (seq (keys srcs)) m srcs]
                                                        (if ks
                                                          (let [k (first ks)
                                                                v (get m k)
                                                                v' (pca/await (execute/normalize-date-wrappers-step v))
                                                                v' (pca/await (execute/prepare-vt-wrappers-step v'))]
                                                            (recur (next ks) (if (identical? v v') m (assoc m k v'))))
                                                          m))]
                                            (if (identical? srcs' srcs)
                                              context-in
                                              (assoc context-in :sources srcs')))))
        ;; Find the primary db for planning: $ if available, else first eligible source
                    primary-db (let [sources (:sources context-in)]
                                 (or (get sources '$)
                                     (some (fn [[_k v]] (when (and (dbu/db? v) (planner-eligible-db? v)) v))
                                           sources)))
                    multi-source? (> (count (:sources context-in)) 1)
                    use-planner? (and (not disable-planner?)
                                      (not stats?)
                                      (some? primary-db)
                                      (dbu/db? primary-db)
                                      (planner-eligible-db? primary-db))
        ;; async: prefetch lookup-ref / ident resolution so the (unchanged)
        ;; substitution code below runs read-free against dt/*entid-cache*.
        ;; The cache VALUE may live across awaits; the BINDINGS below are
        ;; sync-scoped only.
                    entid-cache #?(:clj nil
                                   :cljs (when (and (false? sync?) use-planner? (not multi-source?))
                                           (pca/await (execute/prefetch-entids-step
                                                       primary-db (:rels context-in)
                                                       (:consts context-in) (:where query)))))
                    [context-in lookup-ref-reverse-map]
                    (if use-planner?
          ;; For multi-source, don't pre-resolve lookup refs in :in bindings —
          ;; they may resolve to different entity IDs per source. The relation path's
          ;; lookup-batch-search handles per-source resolution at match time.
                      (if multi-source?
                        [context-in nil]
                        #?(:clj (resolve-lookup-ref-bindings primary-db context-in)
                           :cljs (if entid-cache
                                   (binding [dt/*entid-cache* entid-cache]
                                     (resolve-lookup-ref-bindings primary-db context-in))
                                   (resolve-lookup-ref-bindings primary-db context-in))))
                      [context-in nil])]

                (if (and limit (zero? limit))
                  #{}

      ;; Cartesian-product detection: if the where-clauses split into
      ;; two-or-more components sharing no free vars, the executor's
      ;; assumption of a connected join graph is violated. Recursively
      ;; run each component as its own query and Cartesian-merge.
      ;;
      ;; The check is gated on the planner being eligible at all — when
      ;; we're heading for the legacy engine anyway, the existing
      ;; relation pipeline handles disjoint components correctly via
      ;; collapse-rels / -collect, so no special-casing needed.
                  (if-let [split-result
                           (when (and use-planner?
                                      (planner-origin-db primary-db)
                                      (not (instance? FilteredDB primary-db))
                                      (instance? FindRel qfind)
                                      (not qreturnmaps)
                                      (not (:with query))
                                      (not-any? #(instance? Aggregate %) find-elements)
                                      (not-any? #(instance? Pull %) find-elements))
                             (let [find-var-syms (mapv (fn [^Variable el] (.-symbol el))
                                                       (:elements qfind))
                       ;; Only scalar :in bindings (stored in :consts) are
                       ;; constants. Collection / tuple bindings end up in
                       ;; :rels — those are JOIN dimensions across patterns,
                       ;; not constants. Excluding them would incorrectly
                       ;; treat e.g. `[?e ...]` as disconnecting two
                       ;; patterns that share ?e.
                                   in-bound-vars (set (keys (:consts context-in)))
                                   {:keys [components post-filters]}
                                   (connected-components (:where query) in-bound-vars find-var-syms)]
                               (when (> (count components) 1)
                     ;; Recursively run each component as its own query.
                     ;; Sub-queries with one component will fall through to
                     ;; the existing dispatch below (no further split).
                     ;;
                     ;; Post-filter clauses may reference vars NOT in the
                     ;; user's :find — we extend each sub-query's :find
                     ;; with any post-filter vars it provides, so the
                     ;; merged wide tuples carry every value the
                     ;; post-filter evaluator needs. After filtering, we
                     ;; project back down to the user's find layout.
                                 (let [post-filter-vars (into #{}
                                                              (mapcat (comp :vars analyze/classify-clause))
                                                              post-filters)
                                       extended-find
                                       (mapv (fn [{:keys [vars find-vars]}]
                                               (let [pf-here (filter vars post-filter-vars)
                                                     seen (into #{} find-vars)
                                                     ef (vec (concat find-vars (remove seen pf-here)))]
                                     ;; A component that projects NO var is a pure
                                     ;; existence/liveness constraint (e.g. `[?e _ _]`
                                     ;; sharing no var with the rest of the query). Running
                                     ;; it with `:find []` returns #{} even when the
                                     ;; component is satisfiable, which would wrongly
                                     ;; collapse the whole cartesian-merge to #{}. Give it
                                     ;; one of its own free vars so the sub-query yields
                                     ;; ≥1 row iff the component matches; the extra var is
                                     ;; dropped by the final projection to the user's
                                     ;; find-vars.
                                                 (if (and (empty? ef) (seq vars))
                                                   [(first vars)]
                                                   ef)))
                                             components)
                                       sub-results
                                       (mapv
                                        (fn [{:keys [clauses]} sub-find]
                                          (let [sub-q (-> query
                                                          (assoc :find sub-find)
                                                          (assoc :where (vec clauses)))
                                                sub-input (-> _query-map
                                                              (assoc :query sub-q)
                                                  ;; sub-queries run synchronously
                                                  ;; even under q-async (v1)
                                                              (assoc :sync? true)
                                                  ;; offset/limit/order-by apply to
                                                  ;; the merged result only.
                                                              (dissoc :offset :limit :order-by)
                                                  ;; stats? applies to the top-level
                                                  ;; query only.
                                                              (dissoc :stats?))]
                                            {:tuples (raw-q* sub-input)
                                             :vars   sub-find}))
                                        components extended-find)
                                       wide-vars (vec (mapcat :vars sub-results))
                                       merged    (cartesian-merge sub-results wide-vars)
                                       filtered  (apply-post-filters merged wide-vars post-filters
                                                                     (:consts context-in))
                           ;; Project wide tuples back to the user's find-var order.
                                       wide->find-idxs (let [idx (into {} (map-indexed (fn [i v] [v i])) wide-vars)]
                                                         (mapv idx find-var-syms))
                                       projected (into #{} (map (fn [t] (mapv #(nth t %) wide->find-idxs))) filtered)]
                                   (apply-result-transforms projected order-spec offset limit qreturnmaps)))))]
                    split-result

                    (if (and use-planner?
               ;; Wrapper stacks (nested temporal, filtered-over-temporal) fold
               ;; into the normalized temporal-info; only stacks not bottoming
               ;; out on a plain DB fall back to legacy
                             (planner-origin-db primary-db))
                      (let [db primary-db
              ;; For temporal wrappers, use origin-db for plan creation (schema, index stats)
                            plan-db (planner-origin-db db)
              ;; FilteredDB executes via the contextual per-clause reads (the
              ;; predicate applies through -search); the fused raw-index paths
              ;; below are gated off for it.
                            filtered? (instance? FilteredDB db)
                            bound-vars (context-bound-vars context-in)
              ;; Use the actual db (not origin) for lookup-ref resolution — temporal
              ;; DBs (history) can resolve retracted entities that origin-db can't.
              ;; For multi-source, pass sources so each clause resolves against its source db.
                            clauses #?(:clj (substitute-consts-with-lookup-refs db (:where query) (:consts context-in)
                                                                                (when multi-source? (:sources context-in)))
                                       :cljs (if entid-cache
                                               (binding [dt/*entid-cache* entid-cache]
                                                 (substitute-consts-with-lookup-refs db (:where query) (:consts context-in)
                                                                                     (when multi-source? (:sources context-in))))
                                               (substitute-consts-with-lookup-refs db (:where query) (:consts context-in)
                                                                                   (when multi-source? (:sources context-in)))))
                            rules (not-empty (:rules context-in))
                            plan (pca/await (get-or-create-plan-step
                                             plan-db clauses bound-vars rules (in-card-seed qin) sync?))]

          ;; Try paths in order of preference:
          ;; 1. Direct HashSet (non-aggregate simple queries)
                        (if-let [direct-result (when-not filtered?
                                                 (pca/await (execute-planned-direct
                                                             plan db qfind find-elements context-in query stats? qreturnmaps sync?)))]
                          (let [result (apply-result-transforms direct-result order-spec offset limit qreturnmaps)]
                            #?(:clj
                               (when profile?
                                 (let [t3 (System/nanoTime)]
                                   (println (format "parse=%.3f resolve=%.3f direct=%.3f total=%.3f ms"
                                                    (/ (- t1 t0) 1e6) (/ (- t2 t1) 1e6) (/ (- t3 t2) 1e6)
                                                    (/ (- t3 t0) 1e6))))))
                            result)

            ;; 2. Columnar aggregate (secondary index or PSS scan)
                          (let [ta (when profile? #?(:clj (System/nanoTime) :cljs 0))
                                has-aggs? (some #(instance? Aggregate %) find-elements)
                                columnar-eligible? (and has-aggs?
                                                        (not filtered?)
                                                        (instance? FindRel qfind)
                                                        (not (:with query))
                                                        (not (some #(instance? Pull %) find-elements))
                                                        (empty? (:rels context-in))
                                                        (not lookup-ref-reverse-map))
                                tb (when profile? #?(:clj (System/nanoTime) :cljs 0))
                                columnar-result
                                (when columnar-eligible?
                                  #?(:clj (or (try-secondary-index-aggregate db plan find-elements)
                                              (try-columnar-aggregate plan db find-elements (:cancel context-in)))
                                     :cljs nil))
                                tc (when profile? #?(:clj (System/nanoTime) :cljs 0))]
                            (if columnar-result
                              (let [result (-post-process qfind columnar-result)
                                    result (apply-result-transforms result order-spec offset limit qreturnmaps)]
                                #?(:clj
                                   (when profile?
                                     (let [t3 (System/nanoTime)]
                                       (println (format "parse=%.3f resolve=%.3f plan=%.3f elig=%.3f sec-idx=%.3f post=%.3f total=%.3f ms"
                                                        (/ (- t1 t0) 1e6) (/ (- t2 t1) 1e6) (/ (- ta t2) 1e6)
                                                        (/ (- tb ta) 1e6) (/ (- tc tb) 1e6) (/ (- t3 tc) 1e6)
                                                        (/ (- t3 t0) 1e6))))))
                                result)

                ;; 3. Standard Relation path
                              (let [result (pca/await
                                            (execute-planned-relation
                                             plan db qfind find-elements context-in query all-vars
                                             result-arity lookup-ref-reverse-map order-spec offset limit
                                             stats? qreturnmaps sync?))]
                                #?(:clj
                                   (when profile?
                                     (let [t3 (System/nanoTime)]
                                       (println (format "parse=%.3f resolve=%.3f relation=%.3f total=%.3f ms"
                                                        (/ (- t1 t0) 1e6) (/ (- t2 t1) 1e6) (/ (- t3 t2) 1e6)
                                                        (/ (- t3 t0) 1e6))))))
                                result)))))

        ;; Legacy engine
                      (let [result (execute-legacy context-in query qfind find-elements all-vars result-arity
                                                   order-spec offset limit stats? qreturnmaps)]
                        #?(:clj
                           (when profile?
                             (let [t3 (System/nanoTime)]
                               (println (format "parse=%.3f resolve=%.3f legacy=%.3f total=%.3f ms"
                                                (/ (- t1 t0) 1e6) (/ (- t2 t1) 1e6) (/ (- t3 t2) 1e6)
                                                (/ (- t3 t0) 1e6))))))
                        result)))))))

#?(:clj
   (defn- try-secondary-index-aggregate-fast
     "Fast-path: check if query is a simple aggregate that a secondary index can handle.
      Called before raw-q* to avoid the large method body's JIT deoptimization.
      Returns result or nil.
      Note: the nested when/when-let forms correctly propagate the result — each
      returns nil when its condition is false, or the body's last expression when true.
      The innermost when-let binds the result and returns (-post-process qfind result)."
     [{:keys [query args offset limit order-by stats? disable-planner?]}]
     (when (and (not disable-planner?)
                (not stats?)
                (not order-by)
                (not offset)
                (not (and limit (pos? limit)))
                (= 1 (count args)))
       (let [db (first args)]
         (when (and (dbu/db? db) (instance? DB db)
                    (seq (:secondary-indices db)))
           (let [{:keys [qfind qwith qreturnmaps qin]} (memoized-parse-query query)]
             (when (and (instance? FindRel qfind)
                        (not qwith)
                        (not qreturnmaps)
                        (<= (count (:bindings qin)) 1))
               (let [find-elements (dpip/find-elements qfind)]
                 (when (and (some #(instance? Aggregate %) find-elements)
                            (not (some #(instance? Pull %) find-elements)))
                   (let [context-in (-> (Context. [] {} built-in-rules {} (merge default-settings nil) nil)
                                        (resolve-ins qin args))
                         clauses (substitute-consts-with-lookup-refs db (:where query) (:consts context-in))
                         bound-vars (context-bound-vars context-in)
                         plan (get-or-create-plan db clauses bound-vars nil (in-card-seed qin))]
                     (when (and (empty? (:rels context-in))
                                (seq (:ops plan)))
                       (when-let [result (try-secondary-index-aggregate db plan find-elements)]
                         (-post-process qfind result)))))))))))))

(defn raw-q [{:keys [query args stats? count-fns? offset limit order-by] :as query-map}]
  (let [;; snapshot the dynamic controls ONCE at query entry: partial-cps
        ;; conveys no bindings across suspension on cljs, so the executable
        ;; values travel in the query-map, not in the binding frame
        query-map (assoc query-map
                         :disable-planner? *disable-planner*
                         :profile? *profile?*)
        disable-planner? (:disable-planner? query-map)
        uncached (fn []
                   #?(:clj (or (try-secondary-index-aggregate-fast query-map)
                               (raw-q* query-map))
                      :cljs (raw-q* query-map)))]
    (if (or (not *query-result-cache?*)
            stats?
            count-fns?                         ;; counting needs re-execution; a cache hit wouldn't re-run the fn
            #?(:clj *profile?* :cljs false))
      (uncached)
      ;; Try result cache
      (let [db (first args)
            cacheable? (and (dbu/db? db) (instance? DB db))]
        (if-not cacheable?
          (uncached)
          (let [non-db-args (vec (rest args))
                ;; scale-sensitive-key: BigDecimal args/consts of equal value but
                ;; different scale (1.50M vs 1.500M) are `=` with equal hash in
                ;; Clojure, so they'd share a result-cache entry and return the
                ;; first-cached scale. Keep them distinct.
                cache-key (scale-sensitive-key
                           [query non-db-args offset limit order-by disable-planner?])
                entry (result-cache-get db cache-key)]
            (if entry
              (:result entry)
              (let [result (uncached)
                    where-deps (extract-query-attr-deps (:where query))
                    find-deps  (extract-find-pull-attr-deps
                                (:qfind (memoized-parse-query query)))
                    attr-deps  (merge-attr-deps where-deps find-deps)]
                (result-cache-put! db cache-key result attr-deps)
                result))))))))

;; ---------------------------------------------------------------------------
;; Register legacy functions for CLJS execute.cljc (breaks circular dep)
;; In CLJ, execute.cljc requires datahike.query directly.
;; In CLJS, execute.cljc uses rel/get-legacy-fn to access these.
(rel/register-legacy-fns!
 {:solve-rule         solve-rule
  :filter-by-pred     filter-by-pred
  :bind-by-fn         bind-by-fn
  :lookup-batch-search lookup-batch-search
  :lookup-pattern-coll lookup-pattern-coll})
