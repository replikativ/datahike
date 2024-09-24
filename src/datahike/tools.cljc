(ns ^:no-doc datahike.tools
  (:require
   [superv.async :refer [throw-if-exception-]]
   #?(:clj [clojure.java.io :as io])
   [taoensso.timbre :as log])
  #?(:clj (:import [java.util Properties UUID Date]
                   [java.net InetAddress])))

(defn combine-hashes [x y]
  #?(:clj  (clojure.lang.Util/hashCombine x y)
     :cljs (hash-combine x y)))

(defn -match-vector-class [x]
  (case x
    _ :negative
    * :any
    :positive))

(defn -match-vector [path pattern-pos pattern-size pattern-symbols pairs]
  (cond
    (< pattern-pos pattern-size)
    (let [groups (group-by (comp -match-vector-class #(nth % pattern-pos) first) pairs)
          sub (fn [p pairs] (-match-vector (conj path p)
                                           (inc pattern-pos)
                                           pattern-size
                                           pattern-symbols
                                           pairs))]
      (if (= [:any] (keys groups))
        (sub '* (:any groups))
        `(if ~(nth pattern-symbols pattern-pos)
           ~(sub 1 (mapcat groups [:positive :any]))
           ~(sub '_ (mapcat groups [:negative :any])))))

    (not= 1 (count pairs)) (throw (ex-info "There should be exactly one expression at leaf"
                                           {:path path}))
    :else (-> pairs first second)))

(defmacro match-vector [input-vector & pattern-expr-pairs]
  {:pre [(sequential? pattern-expr-pairs)
         (even? (count pattern-expr-pairs))]}
  (let [pairs (partition 2 pattern-expr-pairs)
        patterns (map first pairs)
        _ (assert (every? sequential? patterns))
        pattern-sizes (into #{} (map count) patterns)
        _ (assert (= 1 (count pattern-sizes)))
        pattern-size (first pattern-sizes)
        symbols (repeatedly pattern-size gensym)]
    `(let [[~@symbols] ~input-vector]
       ~(-match-vector [] 0 pattern-size symbols pairs))))

(defn ^:dynamic get-date []
  #?(:clj (Date.)
     :cljs (js/Date.)))

(defn ^:dynamic get-time-ms []
  #?(:clj (.getTime (Date.))
     :cljs (.getTime (js/Date.))))

(defmacro raise
  "Logging an error and throwing an exception with message and structured data.
   Arguments:
   - Any number of strings that describe the error
   - Last argument is a map of data that helps understanding the source of the error"
  [& fragments]
  (let [msgs (butlast fragments)
        data (last fragments)]
    (list `(log/log! :error :p ~fragments ~{:?line (:line (meta &form))})
          `(throw #?(:clj  (ex-info (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data)
                     :cljs (error (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data))))))

; (throwable-promise) derived from (promise) in clojure/core.clj.
; *   Clojure
; *   Copyright (c) Rich Hickey. All rights reserved.
; *   The use and distribution terms for this software are covered by the
; *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
; *   which can be found in the file epl-v10.html at the root of this distribution.
; *   By using this software in any fashion, you are agreeing to be bound by
; * 	 the terms of this license.
; *   You must not remove this notice, or any other, from this software.
(defn throwable-promise
  "Returns a promise object that can be read with deref/@, and set,
  once only, with deliver. Calls to deref/@ prior to delivery will
  block, unless the variant of deref with timeout is used. All
  subsequent derefs will return the same delivered value without
  blocking. Exceptions delivered to the promise will throw on deref."
  []
  (let [d (java.util.concurrent.CountDownLatch. 1)
        v (atom d)]
    (reify
      clojure.lang.IDeref
      (deref [_] (.await d) (throw-if-exception- @v))
      clojure.lang.IBlockingDeref
      (deref
        [_ timeout-ms timeout-val]
        (if (.await d timeout-ms java.util.concurrent.TimeUnit/MILLISECONDS)
          (throw-if-exception- @v)
          timeout-val))
      clojure.lang.IPending
      (isRealized [this]
        (zero? (.getCount d)))
      clojure.lang.IFn
      (invoke
        [this x]
        (when (and (pos? (.getCount d))
                   (compare-and-set! v d x))
          (.countDown d)
          this)))))

(defn get-version
  "Retrieves the current version of a dependency. Thanks to https://stackoverflow.com/a/33070806/10978897"
  [dep]
  #?(:clj
     (let [path (str "META-INF/maven/" (or (namespace dep) (name dep))
                     "/" (name dep) "/pom.properties")
           props (io/resource path)]
       (when props
         (with-open [stream (io/input-stream props)]
           (let [props (doto (Properties.) (.load stream))]
             (.getProperty props "version")))))
     :cljs
     "JavaScript"))

(def datahike-version (or (get-version 'io.replikativ/datahike) "DEVELOPMENT"))

(def hitchhiker-tree-version (get-version 'io.replikativ/hitchhiker-tree))

(def persistent-set-version (get-version 'persistent-sorted-set/persistent-sorted-set))

(def konserve-version (get-version 'io.replikativ/konserve))

(defn meta-data []
  {:datahike/version datahike-version
   :konserve/version konserve-version
   :hitchhiker.tree/version hitchhiker-tree-version
   :persistent.set/version persistent-set-version
   :datahike/id (UUID/randomUUID)
   :datahike/created-at (Date.)})

(defn deep-merge
  "Recursively merges maps together. If all the maps supplied have nested maps
  under the same keys, these nested maps are merged. Otherwise the value is
  overwritten, as in `clojure.core/merge`.

  Copied from weavejester/medley 1.3.0"
  {:arglists '([& maps])
   :added    "1.1.0"}
  ([])
  ([a] a)
  ([a b]
   (when (or a b)
     (letfn [(merge-entry [m e]
               (let [k  (key e)
                     v' (val e)]
                 (if (contains? m k)
                   (assoc m k (let [v (get m k)]
                                (if (and (map? v) (map? v'))
                                  (deep-merge v v')
                                  v')))
                   (assoc m k v'))))]
       (reduce merge-entry (or a {}) (seq b)))))
  ([a b & more]
   (reduce deep-merge (or a {}) (cons b more))))

(defn timed [f]
  (let [now #?(:clj #(. System (nanoTime))
               :cljs #(* 1000 (. (js/Date.) (getTime))))
        start (now)
        result (f)
        end (now)
        t (/ (double (- end start))
             1000000.0)]
    {:res result
     :t t}))

(defn get-hostname []
  #?(:clj (.getHostAddress (InetAddress/getLocalHost))
     :cljs (raise "Not supported yet." {:type :hostname-not-supported-yet})))

(def datahike-logo (slurp (io/resource "datahike-logo.txt")))

(defmacro with-destructured-vector [v & var-expr-pairs]
  {:pre [(even? (count var-expr-pairs))]}
  (let [pairs (partition 2 var-expr-pairs)
        vars (mapv first pairs)
        vsym (gensym)
        nsym (gensym)
        generate (fn generate [acc pairs]
                   (let [i (count acc)]
                     `(if (<= ~nsym ~i)
                        ~acc
                        ~(if (empty? pairs)
                           `(throw (ex-info "Pattern mismatch"
                                            {:input ~vsym
                                             :pattern (quote ~var-expr-pairs)}))
                           (let [[[_ expr] & pairs] pairs
                                 g (gensym)]
                             `(let [~g ~expr]
                                ~(generate (conj acc g) pairs)))))))]
    `(let [~vsym ~v
           ~nsym (count ~vsym)
           ~vars ~vsym]
       ~(generate [] pairs))))

(defn- reduce-clauses
  [resolver context clauses]
  (loop [context context
         clauses clauses
         failed-clauses []]
    (if (empty? clauses)
      [context failed-clauses]
      (let [[clause & clauses] clauses]
        (if-let [next-context (resolver context clause)]
          (recur next-context clauses failed-clauses)
          (recur context clauses (conj failed-clauses clause)))))))

(defn resolve-clauses [resolver context clauses]
  (if (empty? clauses)
    context
    (let [[context failed-clauses] (reduce-clauses resolver
                                                   context
                                                   clauses)]
      (if (= (count failed-clauses)
             (count clauses))
        (raise "Cannot resolve any more clauses"
               {:clauses clauses})
        (recur resolver context failed-clauses)))))

(defn group-by-step
  "Create a step function to use with `transduce` for grouping values"
  [f]
  (fn
    ([] (transient {}))
    ([dst] (persistent! dst))
    ([dst x]
     (let [k (f x)]
       (assoc! dst k (conj (get dst k []) x))))))

(defn range-subset-tree
  "This function generates code for a decision tree that for an input expression `input` that has to represent a sequence of growing integers that is a subset of the integers in the sequence `(range length-length)`. Every leaf in the decision tree corresponds to one of the 2^range-length possible subsequences and the `branch-visitor-fn` is called at every leaf with the first argument being the subsequence and the second argument being a mask."
  ([range-length input branch-visitor-fn]
   (if (symbol? input)
     (range-subset-tree range-length
                        input
                        branch-visitor-fn
                        0
                        []
                        (vec (repeat range-length nil)))
     (let [sym (gensym)]
       `(let [~sym ~input]
          ~(range-subset-tree range-length sym branch-visitor-fn)))))
  ([range-length input-symbol branch-visitor-fn at acc-inds mask]
   {:pre [(number? range-length)
          (symbol? input-symbol)
          (ifn? branch-visitor-fn)
          (number? at)
          (vector? acc-inds)]}
   (if (= range-length at)
     (branch-visitor-fn acc-inds mask)
     `(if (empty? ~input-symbol)
        ~(branch-visitor-fn acc-inds mask)
        (if (= ~at (first ~input-symbol))
          (let [~input-symbol (rest ~input-symbol)]
            ~(range-subset-tree range-length
                                input-symbol
                                branch-visitor-fn
                                (inc at)
                                (conj acc-inds at)
                                (assoc mask at (count acc-inds))))
          ~(range-subset-tree range-length
                              input-symbol
                              branch-visitor-fn
                              (inc at)
                              acc-inds
                              mask))))))

