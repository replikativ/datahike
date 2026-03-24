(ns datahike.query.analyze
  "Clause analysis for the query planner.
   Classifies clauses, extracts variable dependencies, and detects
   pushable predicates."
  (:require
   [datahike.db.utils :as dbu]
   [datahike.db.interface :as dbi]))

#?(:clj (set! *warn-on-reflection* true))

;; ---------------------------------------------------------------------------
;; Variable extraction

(defn free-var?
  "A free variable is a symbol starting with '?'"
  [x]
  (and (symbol? x)
       (= \? (first (name x)))))

(defn extract-vars
  "Extract all free variables from a clause form."
  [form]
  (cond
    (free-var? form) #{form}
    (sequential? form) (into #{} (mapcat extract-vars) form)
    :else #{}))

;; ---------------------------------------------------------------------------
;; Clause classification

(defn- source? [sym]
  (and (symbol? sym)
       #?(:clj (.startsWith (name sym) "$")
          :cljs (= (subs (name sym) 0 1) "$"))))

(defn classify-clause
  "Classify a where clause into its type. Returns a map with :type and
   clause-specific fields."
  [clause]
  (cond
    ;; [[pred ?x ?y]] — predicate
    (and (sequential? clause)
         (= 1 (count clause))
         (sequential? (first clause))
         (symbol? (ffirst clause)))
    {:type :predicate
     :clause clause
     :fn-sym (ffirst clause)
     :args (rest (first clause))
     :vars (extract-vars (first clause))}

    ;; [[fn ?x ?y] ?result] — function binding
    (and (sequential? clause)
         (= 2 (count clause))
         (sequential? (first clause))
         (symbol? (ffirst clause)))
    {:type :function
     :clause clause
     :fn-sym (ffirst clause)
     :args (rest (first clause))
     :binding (second clause)
     :vars (extract-vars clause)}

    ;; (or ...)
    (and (sequential? clause)
         (= 'or (first clause)))
    {:type :or
     :clause clause
     :branches (rest clause)
     :vars (extract-vars (rest clause))}

    ;; (or-join [...] ...)
    (and (sequential? clause)
         (= 'or-join (first clause)))
    {:type :or-join
     :clause clause
     :join-vars (second clause)
     :branches (drop 2 clause)
     :vars (extract-vars clause)}

    ;; (not ...)
    (and (sequential? clause)
         (= 'not (first clause)))
    {:type :not
     :clause clause
     :sub-clauses (rest clause)
     :vars (extract-vars (rest clause))}

    ;; (not-join [...] ...)
    (and (sequential? clause)
         (= 'not-join (first clause)))
    {:type :not-join
     :clause clause
     :join-vars (second clause)
     :sub-clauses (drop 2 clause)
     :vars (extract-vars clause)}

    ;; (and ...)
    (and (sequential? clause)
         (= 'and (first clause)))
    {:type :and
     :clause clause
     :sub-clauses (rest clause)
     :vars (extract-vars (rest clause))}

    ;; [$source ...] — source-prefixed clause
    (and (sequential? clause)
         (source? (first clause)))
    {:type :source-prefix
     :clause clause
     :source-sym (first clause)
     :inner-clause (vec (rest clause))
     :vars (extract-vars (rest clause))}

    ;; [?e :attr ?v] — data pattern
    (and (sequential? clause)
         (not (sequential? (first clause))))
    {:type :pattern
     :clause clause
     :pattern (vec clause)
     :vars (extract-vars clause)
     :e (first clause)
     :a (second clause)
     :v (get (vec clause) 2)
     :tx (get (vec clause) 3)}

    :else
    {:type :unknown
     :clause clause
     :vars (extract-vars clause)}))

;; ---------------------------------------------------------------------------
;; Predicate pushdown detection

(def range-ops
  "Operators that can be pushed into index scan bounds."
  #{'> '< '>= '<= '= '== 'not=})

(defn- pushable-pred?
  "Returns pushdown info if predicate clause can be pushed into a pattern's
   index scan. A predicate is pushable when:
   - It's a binary comparison: (op ?var const) or (op const ?var)
   - op is a range operator
   - ?var appears as the value variable of a pattern with an indexed attribute
   Returns nil if not pushable."
  [pred-info pattern-infos bound-vars]
  (let [args (:args pred-info)
        op (:fn-sym pred-info)]
    (when (and (contains? range-ops op)
               (= 2 (count args)))
      (let [[arg1 arg2] args
            ;; Determine which arg is the variable and which is the constant
            [var-sym const-val flipped?]
            (cond
              (and (free-var? arg1)
                   (not (free-var? arg2)))
              [arg1 arg2 false]

              (and (free-var? arg2)
                   (not (free-var? arg1)))
              [arg2 arg1 true]

              ;; One var is bound from earlier, treat as const
              (and (free-var? arg1)
                   (free-var? arg2)
                   (contains? bound-vars arg1)
                   (not (contains? bound-vars arg2)))
              [arg2 arg1 true]

              (and (free-var? arg1)
                   (free-var? arg2)
                   (contains? bound-vars arg2)
                   (not (contains? bound-vars arg1)))
              [arg1 arg2 false]

              :else nil)]
        (when var-sym
          ;; Find pattern clauses where var-sym is the value variable
          (some (fn [pi]
                  (when (and (= :pattern (:type pi))
                             (= var-sym (:v pi))
                             (not (free-var? (:a pi)))) ;; attribute must be ground
                    {:pred-clause (:clause pred-info)
                     :pattern-clause (:clause pi)
                     :var var-sym
                     :op (if flipped?
                           ;; Flip operator when constant was first arg
                           (case op > '< < '> >= '<= <= '>= op)
                           op)
                     :const-val const-val}))
                pattern-infos))))))

(defn detect-pushdown
  "Analyze all clauses and detect which predicates can be pushed into
   pattern scans. Returns a map:
   {:pushdowns {pattern-clause → [{:op :const-val :var}...]}
    :consumed #{consumed-predicate-clauses}}"
  [classified-clauses bound-vars]
  (let [patterns (filterv #(= :pattern (:type %)) classified-clauses)
        predicates (filterv #(= :predicate (:type %)) classified-clauses)]
    (reduce
     (fn [acc pred-info]
       (if-let [push (pushable-pred? pred-info patterns bound-vars)]
         (-> acc
             (update-in [:pushdowns (:pattern-clause push)]
                        (fnil conj [])
                        (select-keys push [:op :const-val :var :pred-clause]))
             (update :consumed (fnil conj #{}) (:pred-clause push)))
         acc))
     {:pushdowns {} :consumed #{}}
     predicates)))

;; ---------------------------------------------------------------------------
;; Schema introspection helpers

(defn pattern-schema-info
  "For a pattern clause with a ground attribute, extract schema information
   from the database. Returns map with :cardinality, :indexed?, :unique?, :ref?"
  [db pattern-info]
  (let [a (:a pattern-info)]
    (when (and (some? a) (not (symbol? a)))
      {:attr a
       :card-one? (not (dbu/multival? db a))
       :indexed? (dbu/indexing? db a)
       :unique? (dbu/is-attr? db a :db/unique)
       :ref? (dbu/ref? db a)})))
