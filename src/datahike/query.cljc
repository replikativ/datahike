(ns ^:no-doc datahike.query
  #?(:cljs (:require-macros [datahike.query :refer [basic-index-selector make-vec-lookup-ref-replacer some-of substitution-expansion]]))
  (:require
   [#?(:cljs cljs.reader :clj clojure.edn) :as edn]
   [clojure.set :as set]
   #?(:clj [clojure.string :as str])
   [clojure.walk :as walk]
   [datahike.db.interface :as dbi]
   [datahike.db.utils :as dbu]
   [datahike.array :refer [wrap-comparable]]
   [datahike.impl.entity :as de]
   [datahike.lru]
   [datahike.middleware.query]
   [datahike.pull-api :as dpa]
   [datahike.query-stats :as dqs]
   [datahike.middleware.utils :as middleware-utils]
   [datahike.tools :as dt]
   [datalog.parser :refer [parse]]
   [datalog.parser.impl :as dpi]
   [datalog.parser.impl.proto :as dpip]
   [datalog.parser.pull :as dpp]
   #?(:cljs [datalog.parser.type :refer [Aggregate BindColl BindIgnore BindScalar BindTuple Constant
                                         FindColl FindRel FindScalar FindTuple PlainSymbol Pull
                                         RulesVar SrcVar Variable]])
   [me.tonsky.persistent-sorted-set.arrays :as da]
   [taoensso.timbre :as log])
  (:refer-clojure :exclude [seqable?])

  #?(:clj (:import [clojure.lang Reflector Seqable]
                   [datalog.parser.type Aggregate BindColl BindIgnore BindScalar BindTuple Constant
                    FindColl FindRel FindScalar FindTuple PlainSymbol Pull
                    RulesVar SrcVar Variable]
                   [java.lang.reflect Method]
                   [java.util Date Map HashSet HashSet])))

#?(:clj (set! *warn-on-reflection* true))

;; ----------------------------------------------------------------------------

(def ^:const lru-cache-size 100)

(declare -collect -resolve-clause resolve-clause raw-q)

;; Records

(defrecord Context [rels sources rules consts settings])
(defrecord StatContext [rels sources rules consts stats settings])

;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples:
;;    [ #js [1 "Ivan" 5 14] ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(defrecord Relation [attrs tuples])

;; Main functions

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
                     (log/warn (str "Query-map '" query "' already defines query input."
                                    " Additional arguments to q will be ignored!")))
                   (:args query-input))
               arg-inputs)
        extra-ks [:offset :limit :stats? :settings]]
    (cond-> {:query (apply dissoc query extra-ks)
             :args args}
      (map? query-input)
      (merge (select-keys query-input extra-ks)))))

(defn q [query & inputs]
  (let [{:keys [args] :as query-map} (normalize-q-input query inputs)]
    (if-let [middleware (when (dbu/db? (first args))
                          (get-in (dbi/-config (first args)) [:middleware :query]))]
      (let [q-with-middleware (middleware-utils/apply-middlewares middleware raw-q)]
        (q-with-middleware query-map))
      (raw-q query-map))))

(defn query-stats [query & inputs]
  (-> query
      (normalize-q-input inputs)
      (assoc :stats? true)
      q))

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
      (aset res i (#?(:cljs da/aget :clj get) t1 (aget idxs1 i)))) ;; FIXME aget
    (dotimes [i l2]
      (aset res (+ l1 i) (#?(:cljs da/aget :clj get) t2 (aget idxs2 i)))) ;; FIXME aget
    res))

(defn sum-rel [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b]
    (cond
      (= attrs-a attrs-b)
      (Relation. attrs-a (into (vec tuples-a) tuples-b))

      (not (same-keys? attrs-a attrs-b))
      (dt/raise "Can't sum relations with different attrs: " attrs-a " and " attrs-b
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
                            (aset tuple' idx-a (#?(:cljs da/aget :clj get) tuple-b idx-b)))
                          (conj! acc tuple')))
                      (transient (vec tuples-a))
                      tuples-b))]
        (Relation. attrs-a tuples'))

      :else
      (let [all-attrs (zipmap (keys (merge attrs-a attrs-b)) (range))]
        (-> (Relation. all-attrs [])
            (sum-rel a)
            (sum-rel b))))))

(defn simplify-rel [rel]
  (Relation. (:attrs rel) (distinct-tuples (:tuples rel))))

(defn prod-rel
  ([] (Relation. {} [(da/make-array 0)]))
  ([rel1 rel2]
   (let [attrs1 (keys (:attrs rel1))
         attrs2 (keys (:attrs rel2))
         idxs1 (to-array (map (:attrs rel1) attrs1))
         idxs2 (to-array (map (:attrs rel2) attrs2))]
     (Relation.
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

(defn- -get-else
  [db e a else-val]
  (when (nil? else-val)
    (dt/raise "get-else: nil default value is not supported" {:error :query/where}))
  (if-some [datom (first (dbi/search db [e (translate-for db a)]))]
    (:v datom)
    else-val))

(defn- -get-some
  [db e & as]
  (reduce
   (fn [_ a]
     (when-some [datom (first (dbi/search db [e (translate-for db a)]))]
       (let [a-ident (if (keyword? (:a datom))
                       (:a datom)
                       (dbi/-ident-for db (:a datom)))]
         (reduced [a-ident (:v datom)]))))
   nil
   as))

(defn- -missing?
  [db e a]
  (nil? (get (de/entity db e) a)))

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
                #'datahike.query/q q})

(def clj-core-built-ins
  #?(:clj
     (dissoc (ns-publics 'clojure.core)
             'eval)
     :cljs {}))

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

(defn empty-rel [binding]
  (let [vars (->> (dpi/collect-vars-distinct binding)
                  (map :symbol))]
    (Relation. (zipmap vars (range)) [])))

(defprotocol IBinding
  (in->rel [binding value]))

(extend-protocol IBinding
  BindIgnore
  (in->rel [_ _]
    (prod-rel))

  BindScalar
  (in->rel [binding value]
    (Relation. {(get-in binding [:variable :symbol]) 0} [(into-array [value])]))

  BindColl
  (in->rel [binding coll]
    (cond
      (not (seqable? coll))
      (dt/raise "Cannot bind value " coll " to collection " (dpi/get-source binding)
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
      (dt/raise "Cannot bind value " coll " to tuple " (dpi/get-source binding)
                {:error :query/binding, :value coll, :binding (dpi/get-source binding)})
      (< (count coll) (count (:bindings binding)))
      (dt/raise "Not enough elements in a collection " coll " to bind tuple " (dpi/get-source binding)
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
    (assoc context :rules (parse-rules value))
    (and (instance? BindScalar binding)
         (instance? Variable (:variable binding)))
    (assoc-in context [:consts (get-in binding [:variable :symbol])] value)
    #_(instance? BindColl binding)                          ;; TODO: later
    :else
    (update context :rels conj (in->rel binding value))))

(defn resolve-ins [context bindings values]
  (reduce resolve-in context (zipmap bindings values)))

(def ^{:dynamic true
       :doc "List of symbols in current pattern that might potentially be resolved to refs"}
  *lookup-attrs* nil)

(def ^{:dynamic true
       :doc "Default pattern source. Lookup refs, patterns, rules will be resolved with it"}
  *implicit-source* nil)

(defn getter-fn [attrs attr]
  (let [idx (attrs attr)]
    (if (contains? *lookup-attrs* attr)
      (fn [tuple]
        (let [eid (#?(:cljs da/aget :clj get) tuple idx)]
          (cond
            (number? eid) eid                               ;; quick path to avoid fn call
            (sequential? eid) (dbu/entid *implicit-source* eid)
            (da/array? eid) (dbu/entid *implicit-source* eid)
            :else eid)))
      (fn [tuple]
        (#?(:cljs da/aget :clj get) tuple idx)))))

(defn tuple-key-fn [getters]
  (if (== (count getters) 1)
    (first getters)
    (let [getters (to-array getters)]
      (fn [tuple]
        (list* #?(:cljs (.map getters #(% tuple))
                  :clj (to-array (map #(% tuple) getters))))))))

(defn hash-attrs [key-fn tuples]
  ;; Equivalent to group-by except that it uses a list instead of a vector.
  (loop [tuples tuples
         hash-table (transient {})]
    (if-some [tuple (first tuples)]
      (let [key (key-fn tuple)]
        (recur (next tuples)
               (assoc! hash-table key (conj (get hash-table key '()) tuple))))
      (persistent! hash-table))))

(defn hash-join [rel1 rel2]
  (let [tuples1      (:tuples rel1)
        tuples2      (:tuples rel2)
        attrs1       (:attrs rel1)
        attrs2       (:attrs rel2)
        common-attrs (vec (intersect-keys (:attrs rel1) (:attrs rel2)))
        common-gtrs1 (map #(getter-fn attrs1 %) common-attrs)
        common-gtrs2 (map #(getter-fn attrs2 %) common-attrs)
        keep-attrs1  (keys attrs1)
        keep-attrs2  (vec (set/difference (set (keys attrs2)) (set (keys attrs1))))
        keep-idxs1   (to-array (map attrs1 keep-attrs1))
        keep-idxs2   (to-array (map attrs2 keep-attrs2))
        key-fn1      (tuple-key-fn common-gtrs1)
        key-fn2      (tuple-key-fn common-gtrs2)]
    (if (< (count tuples1) (count tuples2))
      (let [hash       (hash-attrs key-fn1 tuples1)
            new-tuples (->>
                        (reduce (fn [acc tuple2]
                                  (let [key (key-fn2 tuple2)]
                                    (if-some [tuples1 (get hash key)]
                                      (reduce (fn [acc tuple1]
                                                (conj! acc (join-tuples tuple1 keep-idxs1 tuple2 keep-idxs2)))
                                              acc tuples1)
                                      acc)))
                                (transient []) tuples2)
                        (persistent!))]
        (Relation. (zipmap (concat keep-attrs1 keep-attrs2) (range))
                   new-tuples))
      (let [hash       (hash-attrs key-fn2 tuples2)
            new-tuples (->>
                        (reduce (fn [acc tuple1]
                                  (let [key (key-fn1 tuple1)]
                                    (if-some [tuples2 (get hash key)]
                                      (reduce (fn [acc tuple2]
                                                (conj! acc (join-tuples tuple1 keep-idxs1 tuple2 keep-idxs2)))
                                              acc tuples2)
                                      acc)))
                                (transient []) tuples1)
                        (persistent!))]
        (Relation. (zipmap (concat keep-attrs1 keep-attrs2) (range))
                   new-tuples)))))

(defn subtract-rel [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b
        attrs (intersect-keys attrs-a attrs-b)
        getters-b (map #(getter-fn attrs-b %) attrs)
        key-fn-b (tuple-key-fn getters-b)
        hash (hash-attrs key-fn-b tuples-b)
        getters-a (map #(getter-fn attrs-a %) attrs)
        key-fn-a (tuple-key-fn getters-a)]
    (assoc a
           :tuples (filterv #(nil? (hash (key-fn-a %))) tuples-a))))

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
      (Relation. attr->idx (map #(reduce (fn [datom [k v]]
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
  (comp (map (fn [[e a v tx added?]]
               [e a v tx added?]))
        (distinct-tuples)))

(defn relation-from-datoms [context orig-pattern datoms]
  (or (map-consts context orig-pattern datoms)
      (Relation. (var-mapping orig-pattern
                              (range))
                 datoms)))

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
    (Relation. attr->idx (mapv to-array data))))            ;; FIXME to-array

(defn collapse-rels [rels new-rel]
  (loop [rels rels
         new-rel new-rel
         acc []]
    (if-some [rel (first rels)]
      (if (not-empty (intersect-keys (:attrs new-rel) (:attrs rel)))
        (recur (next rels) (hash-join rel new-rel) acc)
        (recur (next rels) new-rel (conj acc rel)))
      (conj acc new-rel))))

(defn- rel-with-attr [context sym]
  (some #(when (contains? (:attrs %) sym) %) (:rels context)))

(defn- context-resolve-val [context sym]
  (if-let [replacement (get (:consts context) sym)]
    replacement
    (when-some [rel (rel-with-attr context sym)]
      (when-some [tuple (first (:tuples rel))]
        (#?(:cljs da/aget :clj get) tuple ((:attrs rel) sym))))))

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
          (da/aset static-args i arg))))
    (fn [tuple]
      ;; TODO raise if not all args are bound
      (dotimes [i len]
        (when-some [tuple-idx (aget tuples-args i)]
          (let [v (#?(:cljs da/aget :clj get) tuple tuple-idx)]
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
        pred (or (get built-ins f)
                 (get clj-core-built-ins f)
                 (context-resolve-val context f)
                 (resolve-sym f)
                 (resolve-method f)
                 (when (nil? (rel-with-attr context f))
                   (dt/raise "Unknown predicate '" f " in " clause
                             {:error :query/where, :form clause, :var f})))
        [context production] (rel-prod-by-attrs context (filter symbol? args))
        new-rel (if pred
                  (let [tuple-pred (-call-fn context production pred args)]
                    (update production :tuples #(filter tuple-pred %)))
                  (assoc production :tuples []))]
    (update context :rels conj new-rel)))

(defn bind-by-fn [context clause]
  (let [[[f & args] out] clause
        binding (dpi/parse-binding out)
        fun (or (get built-ins f)
                (get clj-core-built-ins f)
                (context-resolve-val context f)
                (resolve-sym f)
                (resolve-method f)
                (when (nil? (rel-with-attr context f))
                  (dt/raise "Unknown function '" f " in " clause
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
      (let [new-rel (if fun
                      (let [tuple-fn (-call-fn context production fun args)
                            rels (for [tuple (:tuples production)
                                       :let [val (tuple-fn tuple)]
                                       :when (not (nil? val))]
                                   (prod-rel (Relation. (:attrs production) [tuple])
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
          (update context :rels collapse-rels new-rel)
          (-> context ;; filter output binding
              (update :rels collapse-rels
                      (update new-rel
                              :tuples
                              #(filter (fn [tuple]
                                         (every? (fn [[ind c]]
                                                   (= c (get tuple ind)))
                                                 idx->const))
                                       %)))))))))

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
           rel (Relation. final-attrs-map [])
           tmp-stats []]
      (if-some [frame (first stack)]
        (let [[clauses [rule-clause & next-clauses]] (split-with #(not (rule? context %)) (:clauses frame))]
          (if (nil? rule-clause)

            ;; no rules -> expand, collect, sum
            (let [prefix-context (solve (:prefix-context frame) (:clause frame) clauses)
                  tuples (-collect prefix-context final-attrs)
                  new-rel (Relation. final-attrs-map tuples)
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
        (cond-> (update context :rels collapse-rels rel)
          stats? (assoc :tmp-stats {:type :rule
                                    :branches tmp-stats}))))))

(defn resolve-pattern-lookup-entity-id [source e error-code]
  (cond
    (dbu/numeric-entid? e) e
    (or (lookup-ref? e) (attr? e)) (dbu/entid-strict source e error-code)
                                        ;(entid? e) e
    (keyword? e) e
    (symbol? e) e
    :else (or error-code (dt/raise "Invalid entid" {:error :entity-id/syntax :entity-id e}))))

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

(defn check-all-bound [context vars form]
  (let [bound (set (concat (mapcat #(keys (:attrs %)) (:rels context))
                           (keys (:consts context))))]
    (when-not (set/subset? vars bound)
      (let [missing (set/difference (set vars) bound)]
        (dt/raise "Insufficient bindings: " missing " not bound in " form
                  {:error :query/where
                   :form form
                   :vars missing})))))

(defn check-some-bound [context vars form]
  (let [bound (set (concat (mapcat #(keys (:attrs %)) (:rels context))
                           (keys (:consts context))))]
    (when (empty? (set/intersection vars bound))
      (dt/raise "Insufficient bindings: none of " vars " is bound in " form
                {:error :query/where
                 :form form}))))

(defn resolve-context [context clauses]
  (dt/resolve-clauses resolve-clause context clauses))

(defn tuple-var-mapper [rel]
  (let [attrs (:attrs rel)
        key-fn-pairs (into []
                           (map (juxt identity (partial getter-fn attrs)))
                           (keys attrs))]
    (fn [tuple]
      (into {}
            (map (fn [[k f]] [k (f tuple)]))
            key-fn-pairs))))

(def rel-product-unit (Relation. {} [[]]))

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
      4 pattern-value)))

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
                (catch ~ex-sym# e# nil))))))))

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

        ;; This function returns a unique feature for the values at
        ;; `pattern-filter-inds` given a pattern.
        feature-extractor (index-feature-extractor pattern-filter-inds
                                                   true
                                                   lrr)

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

    ;; This binding is needed for `collapse-rels` to work, and more specifically,
    ;; `hash-join` to work, that in turn depends on `getter-fn`.
    (binding [*lookup-attrs* (if (satisfies? dbi/IDB source)
                               (dynamic-lookup-attrs source pattern1)
                               *lookup-attrs*)]
      (cond-> (update context :rels collapse-rels new-rel)
        (:stats context) (assoc :tmp-stats {:type :lookup})))))

(defn -resolve-clause*
  ([context clause]
   (-resolve-clause* context clause clause))
  ([context clause orig-clause]
   (condp looks-like? clause
     [[symbol? '*]] ;; predicate [(pred ?a ?b ?c)]
     (do (check-all-bound context (identity (filter free-var? (first clause))) orig-clause)
         (filter-by-pred context clause))

     [[symbol? '*] '_] ;; function [(fn ?a ?b) ?res]
     (bind-by-fn context clause)

     [source? '*] ;; source + anything
     (let [[source-sym & rest] clause]
       (binding [*implicit-source* (get (:sources context) source-sym)]
         (-resolve-clause context rest clause)))

     '[or *] ;; (or ...)
     (let [[_ & branches] clause
           context' (assoc context :stats [])
           contexts (mapv #(resolve-clause context' %) branches)
           sum-rel (->> contexts
                        (map #(reduce hash-join (:rels %)))
                        (reduce sum-rel))]
       (cond-> (assoc context :rels [sum-rel])
         (:stats context) (assoc :tmp-stats {:type :or
                                             :branches (mapv :stats contexts)})))

     '[or-join [[*] *] *] ;; (or-join [[req-vars] vars] ...)
     (let [[_ [req-vars & vars] & branches] clause]
       (check-all-bound context req-vars orig-clause)
       (recur context (list* 'or-join (concat req-vars vars) branches) clause))

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
           sum-rel (->> contexts
                        (map #(reduce hash-join (:rels %)))
                        (reduce sum-rel))]
       (cond-> (update context :rels collapse-rels sum-rel)
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
           negation-vars (collect-vars clauses)
           _ (check-some-bound context negation-vars orig-clause)
           join-rel (reduce hash-join (:rels context))
           negation-context (-> context
                                (assoc :rels [join-rel])
                                (assoc :stats [])
                                (resolve-context clauses))
           negation-join-rel (reduce hash-join (:rels negation-context))
           negation (subtract-rel join-rel negation-join-rel)]
       (cond-> (assoc context :rels [negation])
         (:stats context) (assoc :tmp-stats {:type :not
                                             :branches (:stats negation-context)})))

     '[not-join [*] *] ;; (not-join [vars] ...)
     (let [[_ vars & clauses] clause
           _ (check-all-bound context vars orig-clause)
           join-rel (reduce hash-join (:rels context))
           negation-context (-> context
                                (assoc :rels [join-rel])
                                (assoc :stats [])
                                (limit-context vars)
                                (resolve-context clauses)
                                (limit-context vars))
           negation-join-rel (reduce hash-join (:rels negation-context))
           negation (subtract-rel join-rel negation-join-rel)]
       (cond-> (assoc context :rels [negation])
         (:stats context) (assoc :tmp-stats {:type :not
                                             :branches (:stats negation-context)})))

     '[*] ;; pattern
     (let [source *implicit-source*
           pattern0 (replace (:consts context) clause)
           pattern1 (resolve-pattern-lookup-refs source pattern0)]
       (lookup-batch-search source context clause pattern1)))))

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
      (binding [*implicit-source* (get (:sources context) (first clause))]
        (resolve-clause context (next clause)))
      (dqs/update-ctx-with-stats context clause
                                 (fn [context] (solve-rule context clause))))
    (-resolve-clause context clause)))

(defn -q [context clauses]
  (binding [*implicit-source* (get (:sources context) '$)]
    (dt/resolve-clauses resolve-clause context clauses)))

(defn -collect
  ([context symbols]
   (let [rels (:rels context)
         start-array (to-array (map #(get (:consts context) %) symbols))]
     (-collect [start-array] rels symbols)))
  ([acc rels symbols]
   (if-some [rel (first rels)]
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
                          (aset res i (#?(:cljs da/aget :clj get) t2 idx))))
                      res))
                  (next rels)
                  symbols))))
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

(defn- pull [find-elements context resultset]
  (let [resolved (for [find find-elements]
                   (when (instance? Pull find)
                     [(-context-resolve (:source find) context)
                      (dpp/parse-pull
                       (-context-resolve (:pattern find) context))]))]
    (for [tuple resultset]
      (mapv (fn [env el]
              (if env
                (let [[src spec] env]
                  (dpa/pull-spec src spec [el] false))
                el))
            resolved
            tuple))))

(def ^:private query-cache (volatile! (datahike.lru/lru lru-cache-size)))

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

(defn raw-q [{:keys [query args offset limit stats? settings] :as _query-map}]
  (let [settings (merge default-settings settings)
        {:keys [qfind
                qwith
                qreturnmaps
                qin]} (memoized-parse-query query)
        context-in    (-> (if stats?
                            (StatContext. [] {} {} {} [] settings)
                            (Context. [] {} {} {} settings))
                          (resolve-ins qin args))
        ;; TODO utilize parser

        all-vars      (concat (dpi/find-vars qfind) (map :symbol qwith))
        context-out   (-q context-in (:where query))
        resultset     (collect context-out all-vars)
        find-elements (dpip/find-elements qfind)
        result-arity  (count find-elements)]
    (cond->> (into #{}
                   (comp (distinct)
                         (if offset
                           (drop offset)
                           identity)
                         (if (or (nil? limit) (neg? limit))
                           identity
                           (take limit)))
                   resultset)
      (:with query)                                 (mapv #(subvec % 0 result-arity))
      (some #(instance? Aggregate %) find-elements) (aggregate find-elements context-in)
      (some #(instance? Pull %) find-elements)      (pull find-elements context-in)
      true                                          (-post-process qfind)
      qreturnmaps                                   (convert-to-return-maps qreturnmaps)
      stats?                                        (#(-> context-out
                                                          (dissoc :rels :sources :settings)
                                                          (assoc :ret %
                                                                 :query query))))))
