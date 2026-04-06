(ns datahike.query.relation
  "Relation record and relational algebra utilities.
   Extracted from datahike.query to break circular dependencies
   between datahike.query and datahike.query.execute."
  (:require
   [clojure.set :as set]
   [datahike.db.utils :as dbu]
   [datahike.tools :as dt]
   [replikativ.logging :as log]
   [org.replikativ.persistent-sorted-set.arrays :as da]))

#?(:clj (set! *warn-on-reflection* true))

;; ---------------------------------------------------------------------------
;; Relation record

;; attrs:
;;    {?e 0, ?v 1} or {?e2 "a", ?age "v"}
;; tuples:
;;    [ #js [1 "Ivan" 5 14] ... ]
;; or [ (Datom. 2 "Oleg" 1 55) ... ]
(defrecord Relation [attrs tuples])

;; ---------------------------------------------------------------------------
;; Shared predicates

(defn free-var? [sym]
  (and (symbol? sym)
       (= \? (first (name sym)))))

;; ---------------------------------------------------------------------------
;; Set utilities

(defn intersect-keys [attrs1 attrs2]
  (set/intersection (set (keys attrs1))
                    (set (keys attrs2))))

(defn same-keys? [a b]
  (and (= (count a) (count b))
       (every? #(contains? b %) (keys a))))

;; ---------------------------------------------------------------------------
;; Dynamic vars

(def ^{:dynamic true
       :doc "List of symbols in current pattern that might potentially be resolved to refs"}
  *lookup-attrs* nil)

(def ^{:dynamic true
       :doc "Default pattern source. Lookup refs, patterns, rules will be resolved with it"}
  *implicit-source* nil)

;; ---------------------------------------------------------------------------
;; Tuple operations

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

;; ---------------------------------------------------------------------------
;; Relational algebra

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

(defn sum-rel [a b]
  (let [{attrs-a :attrs, tuples-a :tuples} a
        {attrs-b :attrs, tuples-b :tuples} b]
    (cond
      (= attrs-a attrs-b)
      (Relation. attrs-a (into (vec tuples-a) tuples-b))

      (not (same-keys? attrs-a attrs-b))
      (log/raise "Can't sum relations with different attrs: " attrs-a " and " attrs-b
                 {:error :query/where})

      (every? number? (vals attrs-a))                       ;; can't conj into BTSetIter
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

(defn collapse-rels [rels new-rel]
  (loop [rels rels
         new-rel new-rel
         acc []]
    (if-some [rel (first rels)]
      (if (not-empty (intersect-keys (:attrs new-rel) (:attrs rel)))
        (recur (next rels) (hash-join rel new-rel) acc)
        (recur (next rels) new-rel (conj acc rel)))
      (conj acc new-rel))))

;; ---------------------------------------------------------------------------
;; Pattern / variable utilities

(defn var-mapping [pattern indices]
  (->> (map vector pattern indices)
       (filter (fn [[s _]] (free-var? s)))
       (into {})))

;; ---------------------------------------------------------------------------
;; Context utilities

(defn limit-rel [rel vars]
  (when-some [attrs' (not-empty (select-keys (:attrs rel) vars))]
    (assoc rel :attrs attrs')))

(defn limit-context [context vars]
  (assoc context
         :rels (->> (:rels context)
                    (keep #(limit-rel % vars)))))

;; ---------------------------------------------------------------------------
;; Late-binding registry for query engine functions.
;; Breaks circular dependency: execute.cljc needs some functions from query.cljc,
;; but query.cljc needs to call execute.cljc. In CLJ this is solved via
;; requiring-resolve; in CLJS we register the functions here at load time.

(defonce ^:private legacy-fns (atom {}))

(defn register-legacy-fns!
  "Register query engine functions that execute.cljc needs.
   Called by datahike.query at load time."
  [fns-map]
  (reset! legacy-fns fns-map))

(defn get-legacy-fn
  "Retrieve a registered legacy function by key."
  [k]
  (or (get @legacy-fns k)
      (throw (ex-info (str "Legacy function not registered: " k
                           ". Ensure datahike.query is loaded.")
                      {:key k}))))
