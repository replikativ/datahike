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

(def ^:private max-dense-sum-eid
  ;; A dense primitive table avoids per-row boxing in the hot sum fold. Above
  ;; this bound its worst-case allocation would exceed 40 MB, so sparse or very
  ;; large databases conservatively stay on the Relation path.
  5000000)

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

(defn- sum-cost-effective? [left-join-op right-join-op]
  ;; The sum path replaces a materialized Cartesian join with a direct index
  ;; fold. At small cardinalities the Relation path is already cheap; keep the
  ;; runtime probe focused on joins large enough to amortize its tables.
  (>= (+ (long (or (:estimated-card left-join-op) 0))
         (long (or (:estimated-card right-join-op) 0)))
      1000))

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

(defn- group-predicates [group]
  (->> (concat
        (map #(select-keys % [:fn-sym :args]) (:attached-preds group))
        (for [op (group-pattern-ops group)
              pushdown (:pushdown-preds op)
              :let [call (first (:pred-clause pushdown))]
              :when (seq call)]
          {:fn-sym (first call) :args (rest call)}))
       (distinct)
       (vec)))

(defn- value-indexes [db ops join-op]
  (into {}
        (map (fn [op]
               [(nth (:clause op) 2) (entity-value-index db op)]))
        (remove #(identical? % join-op) ops)))

(defn- run-end [datoms n from key]
  (long
   (loop [i (inc from)]
     (if (and (< i n)
              (zero? (datom/compare-value (.-v ^Datom (nth datoms i)) key)))
       (recur (inc i))
       i))))

(defn- execute-count
  [db find-elements cancel
   producer-group consumer-group producer-ops consumer-ops
   producer-join-op consumer-join-op join-var]
  (let [producer-e (first (:clause producer-join-op))
        consumer-e (first (:clause consumer-join-op))
        producer-preds (group-predicates producer-group)
        consumer-preds (group-predicates consumer-group)
        producer-value-indexes (value-indexes db producer-ops producer-join-op)
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
            (neg? cmp) (recur (long (inc pi)) (long ci))
            (pos? cmp) (recur (long pi) (long (inc ci)))
            :else
            (let [pi' (run-end producer-datoms pn pi pk)
                  ci' (run-end consumer-datoms cn ci ck)
                  n (loop [i ci n 0]
                      (if (== i ci')
                        n
                        (let [^Datom d (nth consumer-datoms i)]
                          (recur (inc i)
                                 (if (row-pass? (.-e d) ck consumer-e join-var
                                                consumer-value-indexes consumer-preds)
                                   (inc n)
                                   n)))))]
              ;; Count grouping is only admitted for a schema-unique producer.
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
              (recur (long pi') (long ci')))))))))

(defn- execute-sum
  [db cancel left-group right-group left-ops right-ops
   left-join-op right-join-op join-var sum-var sum-side]
  (let [sum-group (if (zero? sum-side) left-group right-group)
        other-group (if (zero? sum-side) right-group left-group)
        sum-ops (if (zero? sum-side) left-ops right-ops)
        other-ops (if (zero? sum-side) right-ops left-ops)
        sum-join-op (if (zero? sum-side) left-join-op right-join-op)
        other-join-op (if (zero? sum-side) right-join-op left-join-op)
        sum-e (first (:clause sum-join-op))
        other-e (first (:clause other-join-op))
        sum-op (first (filter #(= sum-var (nth (:clause %) 2)) sum-ops))
        sum-preds (group-predicates sum-group)
        other-preds (group-predicates other-group)
        ;; Only additional predicate columns need entity lookup maps.
        sum-value-indexes (value-indexes db (remove #(identical? % sum-op) sum-ops)
                                         sum-join-op)
        other-value-indexes (value-indexes db other-ops other-join-op)
        ^HashMap other-counts (HashMap.)
        ^longs entity-multiplicities (long-array (inc (long (:max-eid db))))]
    ;; Collapse the non-summed run to key -> multiplicity. This preserves the
    ;; producer x consumer bag implied by :with without materializing pairs.
    (doseq [^Datom d (index-attr-datoms db other-join-op :avet)]
      (when (and cancel @cancel)
        (throw (ex-info "query canceled" {:datahike/canceled true})))
      (let [key (.-v d)]
        (when (row-pass? (.-e d) key other-e join-var
                         other-value-indexes other-preds)
          (.put other-counts key (inc (long (or (.get other-counts key) 0)))))))
    ;; AVET is substantially cheaper than materializing an AEVT entity map.
    ;; Retain only entities whose join key exists on the other side, together
    ;; with the exact Cartesian multiplicity implied by :with.
    (doseq [^Datom d (index-attr-datoms db sum-join-op :avet)]
      (when (and cancel @cancel)
        (throw (ex-info "query canceled" {:datahike/canceled true})))
      (let [e (.-e d)
            key (.-v d)
            n (long (or (.get other-counts key) 0))]
        (when (and (pos? n)
                   (row-pass? e key sum-e join-var sum-value-indexes sum-preds))
          (aset-long entity-multiplicities (int e) n))))
    (let [[matched? total]
          (reduce
           (fn [[matched? total] ^Datom d]
             (when (and cancel @cancel)
               (throw (ex-info "query canceled" {:datahike/canceled true})))
             (let [n (aget entity-multiplicities (int (.-e d)))]
               (if (zero? n)
                 [matched? total]
                 [true (loop [i n acc total]
                         (if (zero? i)
                           acc
                           (recur (dec i) (+ acc (.-v d)))))])))
           [false (identity 0)]
           (index-attr-datoms db sum-op :aevt))]
      (if matched? [[total]] []))))

(defn execute
  "Return aggregate result tuples for the supported ordered join shape, or nil.

  The supported subset is intentionally conservative: two card-one entity
  groups, one indexed shared value, simple constant predicates, and either a
  producer-grouped `(count ?consumer-entity)` over a unique key or a scalar
  `(sum ?value)` whose `:with` variables prove pair multiplicity. Count walks
  equal AVET runs; sum folds a direct aggregate-column scan against join-key
  multiplicities. Neither path materializes the intermediate join relation."
  [plan db find-elements with-elements cancel]
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
              left-group (nth groups 0)
              right-group (nth groups 1)
              left-ops (group-pattern-ops left-group)
              right-ops (group-pattern-ops right-group)
              left-join-op (join-value-op left-group join-var)
              right-join-op (join-value-op right-group join-var)
              left-e (first (:clause left-join-op))
              right-e (first (:clause right-join-op))
              ^Aggregate aggregate (first aggregates)
              aggregate-fn (.-symbol ^PlainSymbol (.-fn aggregate))
              aggregate-args (.-args aggregate)
              aggregate-var (when (and (= 1 (count aggregate-args))
                                       (instance? Variable (first aggregate-args)))
                              (.-symbol ^Variable (first aggregate-args)))
              group-vars (mapv #(.-symbol ^Variable %)
                               (filter #(instance? Variable %) find-elements))
              with-vars (set (map #(.-symbol ^Variable %) with-elements))
              left-vars (available-vars left-ops)
              right-vars (available-vars right-ops)
              sum-side (cond
                         (and (contains? left-vars aggregate-var)
                              (not (contains? right-vars aggregate-var))) 0
                         (and (contains? right-vars aggregate-var)
                              (not (contains? left-vars aggregate-var))) 1
                         :else nil)
              sum-e (if (zero? (or sum-side -1)) left-e right-e)
              other-e (if (zero? (or sum-side -1)) right-e left-e)
              other-join-op (if (zero? (or sum-side -1)) right-join-op left-join-op)
              sum-group (if (zero? (or sum-side -1)) left-group right-group)
              sum-ops (if (zero? (or sum-side -1)) left-ops right-ops)
              sum-op (first (filter #(= aggregate-var (nth (:clause %) 2)) sum-ops))
              count-supported?
              (and (= 'count aggregate-fn)
                   (= right-e aggregate-var)
                   (empty? with-vars)
                   (get-in left-join-op [:schema-info :unique?])
                   (every? (set (or (:output-vars left-group)
                                    (:vars left-group)))
                           group-vars)
                   (cost-effective? left-group right-group left-join-op right-join-op))
              sum-supported?
              (and (= 'sum aggregate-fn)
                   (some? sum-side)
                   sum-op
                   (= 1 (count find-elements))
                   (contains? with-vars sum-e)
                   (or (get-in other-join-op [:schema-info :unique?])
                       (contains? with-vars other-e))
                   (every? #{left-e right-e} with-vars)
                   (<= (long (:max-eid db)) max-dense-sum-eid)
                   (not-any? #(some #{aggregate-var} (:args %)) (group-predicates sum-group))
                   (sum-cost-effective? left-join-op right-join-op))]
          (when (and (zero? producer-idx)
                     (= 1 (count probe-vars))
                     left-join-op right-join-op
                     (or count-supported? sum-supported?)
                     (every? supported-pattern? (concat left-ops right-ops))
                     (independent-group? left-ops)
                     (independent-group? right-ops)
                     (every? #(supported-predicate? left-vars %)
                             (group-predicates left-group))
                     (every? #(supported-predicate? right-vars %)
                             (group-predicates right-group)))
            (if count-supported?
              (execute-count db find-elements cancel
                             left-group right-group left-ops right-ops
                             left-join-op right-join-op join-var)
              (execute-sum db cancel left-group right-group left-ops right-ops
                           left-join-op right-join-op join-var aggregate-var sum-side))))))))
