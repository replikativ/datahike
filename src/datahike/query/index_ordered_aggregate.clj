(ns datahike.query.index-ordered-aggregate
  "Primary-index aggregate execution for ordered unique-key/foreign-key joins."
  (:require
   [datahike.array :as da]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :as datom :refer [datom]]
   [datahike.db.interface :as dbi]
   [datahike.index.interface :as di]
   [datahike.query.analyze :as analyze])
  (:import
   [datahike.datom Datom]
   [datahike.db DB]
   [datalog.parser.type Aggregate PlainSymbol Variable]
   [java.util HashMap HashSet]))

(set! *warn-on-reflection* true)

(def ^:dynamic *enabled*
  "Bind false to force the ordinary Relation aggregate path."
  true)

(defn- group-pattern-ops [group]
  (if (= :entity-group (:op group))
    (into [(:scan-op group)] (:merge-ops group))
    [group]))

(defn- group-scan-op [group]
  (if (= :entity-group (:op group)) (:scan-op group) group))

(defn- join-value-op [group join-var]
  (first
   (filter (fn [op]
             (let [[_e a v _tx] (:clause op)]
               (and (= v join-var)
                    (or (keyword? a) (integer? a))
                    (get-in op [:schema-info :indexed?])
                    (get-in op [:schema-info :card-one?] true)
                    (not (:anti? op))
                    (not (:optional? op)))))
           (group-pattern-ops group))))

(defn- resolve-attr [db a]
  (if (and (:attribute-refs? (dbi/-config db)) (keyword? a))
    (dbi/ref-for db a :no-match)
    a))

(defn- index-attr-datoms [db op index]
  (let [attr (resolve-attr db (second (:clause op)))
        from (datom e0 attr nil tx0)
        to (datom emax attr nil txmax)]
    (di/-slice (get db index) from to index)))

(defn- entity-value-index
  ^HashMap [db op]
  (let [^HashMap out (HashMap.)]
    (doseq [^Datom d (index-attr-datoms db op :aevt)]
      (.put out (.-e d) (.-v d)))
    out))

(defn- simple-predicate-value [fn-sym a b]
  (case (symbol (name fn-sym))
    > (> a b)
    >= (>= a b)
    < (< a b)
    <= (<= a b)
    = (da/a= a b)
    == (== a b)
    not= (not (da/a= a b))
    false))

(defn- row-value [e key e-var join-var value-indexes x]
  (cond
    (= x e-var) e
    (= x join-var) key
    (contains? value-indexes x) (.get ^HashMap (get value-indexes x) e)
    :else x))

(defn- row-pass? [e key e-var join-var value-indexes predicates]
  (and (every? (fn [[_ ^HashMap values]] (.containsKey values e))
               value-indexes)
       (every? (fn [{:keys [fn-sym args]}]
                 (let [[a b] args]
                   (simple-predicate-value
                    fn-sym
                    (row-value e key e-var join-var value-indexes a)
                    (row-value e key e-var join-var value-indexes b))))
               predicates)))

(defn- cost-effective?
  [producer-group consumer-group producer-join-op consumer-join-op]
  (let [producer-card (double (or (:estimated-card producer-join-op) 0))
        consumer-card (double (or (:estimated-card consumer-join-op) 0))
        producer-scan-op (group-scan-op producer-group)
        consumer-scan-op (group-scan-op consumer-group)
        producer-scan (double (or (:scan-card producer-scan-op)
                                  (:estimated-card producer-scan-op)
                                  0))
        consumer-scan (double (or (:scan-card consumer-scan-op)
                                  (:estimated-card consumer-scan-op)
                                  0))]
    (and (>= producer-card 1000.0)
         (pos? consumer-card)
         ;; Tiny producers are better served by probe-driven AVET seeks.
         (>= (/ producer-card consumer-card) 0.05)
         ;; Do not replace a selective driving index with two full key scans.
         ;; The controlled filter/hash-to-merge crossover was about 20%.
         (>= producer-scan (* 0.20 producer-card))
         (>= consumer-scan (* 0.20 consumer-card)))))

(defn- supported-pattern? [op]
  (let [[e a v tx] (:clause op)]
    (and (nil? (:source op))
         (symbol? e) (analyze/free-var? e)
         (or (keyword? a) (integer? a))
         (symbol? v) (analyze/free-var? v)
         (nil? tx)
         (get-in op [:schema-info :card-one?] true)
         (not (:anti? op))
         (not (:optional? op)))))

(defn- independent-group? [ops]
  (let [entity-vars (mapv (comp first :clause) ops)
        value-vars (mapv #(nth (:clause %) 2) ops)]
    (and (apply = entity-vars)
         (= (count value-vars) (count (distinct value-vars))))))

(def ^:private supported-predicates
  #{'> '>= '< '<= '= '== 'not=
    'clojure.core/> 'clojure.core/>=
    'clojure.core/< 'clojure.core/<=
    'clojure.core/= 'clojure.core/==
    'clojure.core/not=})

(defn- supported-predicate? [available-vars {:keys [fn-sym args]}]
  (and (contains? supported-predicates fn-sym)
       (= 2 (count args))
       (every? #(or (not (symbol? %)) (contains? available-vars %)) args)
       ;; Bound input vars have already been folded to constants. Restrict this
       ;; path to a single row var and one constant so evaluation stays exact.
       (= 1 (count (filter symbol? args)))))

(defn- available-vars [ops]
  (set (mapcat (fn [op]
                 (let [[e _a v] (:clause op)] [e v]))
               ops)))

(defn- value-indexes [db ops join-op]
  (into {}
        (map (fn [op]
               [(nth (:clause op) 2) (entity-value-index db op)]))
        (remove #(identical? % join-op) ops)))

(defn execute
  "Return aggregate result tuples for the supported ordered join shape, or nil.

  The supported subset is intentionally conservative: two card-one entity
  groups, one indexed shared value, a unique producer join attribute,
  producer-side grouping columns, one `(count ?consumer-entity)`, and simple
  constant predicates. Both join attributes are scanned in AVET order; other
  columns are indexed by entity once; predicates run before equal-key runs are
  counted, so the intermediate join relation is never materialized."
  [plan db find-elements cancel]
  (when *enabled*
    (let [groups (filterv #(#{:entity-group :pattern-scan} (:op %)) (:ops plan))
          joins (:group-joins plan)
          aggregates (filterv #(instance? Aggregate %) find-elements)]
      (when (and (instance? DB db)
                 (every? #(#{:entity-group :pattern-scan} (:op %)) (:ops plan))
                 (not-any? :source (:ops plan))
                 (= 2 (count groups))
                 (= 1 (count joins))
                 (contains? joins 1)
                 (= 1 (count aggregates))
                 (= (count find-elements)
                    (+ 1 (count (filter #(instance? Variable %) find-elements)))))
        (let [{:keys [producer-idx probe-vars]} (get joins 1)
              join-var (first probe-vars)
              producer-group (nth groups 0)
              consumer-group (nth groups 1)
              producer-ops (group-pattern-ops producer-group)
              consumer-ops (group-pattern-ops consumer-group)
              producer-join-op (join-value-op producer-group join-var)
              consumer-join-op (join-value-op consumer-group join-var)
              producer-e (first (:clause producer-join-op))
              consumer-e (first (:clause consumer-join-op))
              ^Aggregate aggregate (first aggregates)
              aggregate-fn (.-symbol ^PlainSymbol (.-fn aggregate))
              aggregate-args (.-args aggregate)
              count-var (when (and (= 1 (count aggregate-args))
                                   (instance? Variable (first aggregate-args)))
                          (.-symbol ^Variable (first aggregate-args)))
              group-vars (mapv #(.-symbol ^Variable %)
                               (filter #(instance? Variable %) find-elements))
              producer-vars (set (or (:output-vars producer-group)
                                     (:vars producer-group)))
              producer-preds (vec (:attached-preds producer-group))
              consumer-preds (vec (:attached-preds consumer-group))]
          (when (and (zero? producer-idx)
                     (= 1 (count probe-vars))
                     producer-join-op consumer-join-op
                     (get-in producer-join-op [:schema-info :unique?])
                     (= 'count aggregate-fn)
                     (= consumer-e count-var)
                     (every? producer-vars group-vars)
                     (every? supported-pattern? (concat producer-ops consumer-ops))
                     (independent-group? producer-ops)
                     (independent-group? consumer-ops)
                     (every? #(supported-predicate? (available-vars producer-ops) %)
                             producer-preds)
                     (every? #(supported-predicate? (available-vars consumer-ops) %)
                             consumer-preds)
                     (cost-effective? producer-group consumer-group
                                      producer-join-op consumer-join-op))
            (let [producer-value-indexes (value-indexes db producer-ops producer-join-op)
                  consumer-value-indexes (value-indexes db consumer-ops consumer-join-op)
                  producer-datoms (vec (index-attr-datoms db producer-join-op :avet))
                  consumer-datoms (vec (index-attr-datoms db consumer-join-op :avet))
                  pn (count producer-datoms)
                  cn (count consumer-datoms)
                  ^HashSet out (HashSet.)]
              (loop [pi 0 ci 0]
                (if (or (>= pi pn) (>= ci cn))
                  (vec out)
                  (let [_ (when (and cancel @cancel)
                            (throw (ex-info "query canceled" {:datahike/canceled true})))
                        ^Datom pd (nth producer-datoms pi)
                        ^Datom cd (nth consumer-datoms ci)
                        pk (.-v pd)
                        ck (.-v cd)
                        cmp (long (datom/compare-value pk ck))]
                    (cond
                      (neg? cmp) (recur (inc pi) ci)
                      (pos? cmp) (recur pi (inc ci))
                      :else
                      (let [pi' (long
                                 (loop [i (inc pi)]
                                   (if (and (< i pn)
                                            (zero? (datom/compare-value
                                                    (.-v ^Datom (nth producer-datoms i))
                                                    pk)))
                                     (recur (inc i))
                                     i)))
                            [ci' n]
                            (loop [i ci n 0]
                              (if (and (< i cn)
                                       (zero? (datom/compare-value
                                               (.-v ^Datom (nth consumer-datoms i))
                                               ck)))
                                (let [^Datom d (nth consumer-datoms i)]
                                  (recur (inc i)
                                         (if (row-pass?
                                              (.-e d) ck consumer-e join-var
                                              consumer-value-indexes consumer-preds)
                                           (inc n)
                                           n)))
                                [i n]))]
                        ;; A unique producer AVET must have one datom per key.
                        (assert (= 1 (- pi' pi)))
                        (when (and (pos? n)
                                   (row-pass? (.-e pd) pk producer-e join-var
                                              producer-value-indexes producer-preds))
                          (.add out
                                (mapv (fn [element]
                                        (if (instance? Aggregate element)
                                          n
                                          (row-value
                                           (.-e pd) pk producer-e join-var
                                           producer-value-indexes
                                           (.-symbol ^Variable element))))
                                      find-elements)))
                        (recur pi' (long ci'))))))))))))))
