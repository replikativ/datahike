(ns datahike.test.generative-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.datom :as dd])
  (:import [java.util UUID Date]
           [java.io File]))

(def entity-offset 1000)

(defn example-strings [prefix count]
  (mapv #(str prefix %) (range count)))

(defn example-entities [offset count]
  (mapv #(+ offset %) (range count)))

;; This is a set of parameters to configure the generation of datoms
(def setup0 {;; This is the space of available entity ids to be used for entities in the database
             :entities (example-entities entity-offset 30)

             ;; Here we specify the characteristics of attributes
             :attribs {:concept/attrib0 {:cardinality :db.cardinality/many
                                         :type :db.type/ref}
                       :concept/attrib2 {:cardinality :db.cardinality/one
                                         :type :db.type/ref}
                       :concept/pref-label {:cardinality :db.cardinality/one
                                            :type :db.type/string
                                            :values (example-strings "pref" 30)}
                       :concept/alt-label {:cardinality :db.cardinality/many
                                           :type :db.type/string
                                           :values (example-strings "alt" 30)}}

             ;; Here we specify different strategies used to populate the sequence
             ;; of datoms. Each strategy has a weight to determine how probable it is
             :actions {:add 4      ;; Add an entirely new datom, irrespective of what existed before.
                       :remove 2   ;; Remove a datom added in a previous transaction
                       :tweak 1    ;; Take a datom of a previous transaction and add a new one that has been modified.
                       :combine 1  ;; Take two datoms from previous transactions and mix them.
                       :transact 1 ;; Emit a new transaction.
                       }

             ;; The entity id offset for the schema datoms
             :schema-offset 100

             ;; The entity id offset for the transaction datoms
             :tx-offset 100000

             ;; The time offset where the transactions start.
             :tx-epoch-ms 1771581153000

             ;; A parameter that controls the random distribution
             ;; of the duration from one transaction to the next transaction.
             :tx-timestep-T-ms (* 24 3600 1000)})

(defn ref-attrib?
  "Look up an attribute and return true iff it is a reference to an entity"
  [attribs a]
  (= :db.type/ref (get-in attribs [a :type])))

(def built-in-attribs {:db/txInstant {:cardinality :db.cardinality/one}})

(defn built-in-datom? [[_ a]]
  (contains? built-in-attribs a))

(defn sample-transaction-timestep
  "Generate the duration from one transaction to the next: Given a
uniformly distributed number y in [0, 1], solve for x the following
equation: e^(-x/T) = y"
  [T]
  (* -1 T (Math/log (Math/random))))

(defn weighted-sample
  "Given a map, or pairs of [element weight], pick one element with a probability proportional to its weight."
  [pairs]
  (let [sum (transduce (map second) + pairs)]
    (loop [at (* sum (Math/random))
           [[k w] & pairs] pairs]
      (if (<= at w)
        k
        (recur (- at w) pairs)))))

(defn schema-datoms
  "Produce the datoms that define the schema of a setup"
  [{:keys [attribs schema-offset tx-offset]}]
  (let [tx (- tx-offset 1)]
    (into []
          (comp (map-indexed (fn [i0 [attrib {:keys [cardinality type]}]]
                               (let [i (+ i0 schema-offset)]
                                 [[i :db/ident attrib tx true]
                                  [i :db/valueType type tx true]
                                  [i :db/cardinality cardinality tx true]])))
                cat)
          attribs)))

(defn generate-datom-by-attrib
  "Given a setup and an attribute, generate a valid datom. Populate with values `tx-id` and `add` as last elements."
  [{:keys [entities attribs]}
   attrib tx-id
   add]
  (let [{:keys [type values]} (attribs attrib)]
    [(rand-nth entities)
     attrib
     (case type
       :db.type/ref (rand-nth entities)
       :db.type/string (rand-nth values)
       (throw (ex-info "Unknown type" {:attrib attrib :attribs attribs})))
     tx-id
     add]))

(defn generate-datom
  "Generate a datom according to setup and populate with `tx-id` and `add` as last elements."
  [{:keys [attribs] :as setup} tx-id add]
  (let [attrib (rand-nth (keys attribs))]
    (generate-datom-by-attrib setup attrib tx-id add)))

(defn- matches
  "Helper for `predicate-from-pattern` that treats `nil` as a wildcard."
  [x0 x]
  (or (nil? x0)
      (= x0 x)))

(defn predicate-from-pattern
  "Returns a predicate function for a pattern"
  [[e0 a0 v0 tx0]]
  (fn [[e a v tx]]
    (and (matches e0 e)
         (matches a0 a)
         (matches v0 v)
         (matches tx0 tx))))

(defn remove-pattern
  "Filter a set of datoms by removing those matching a pattern."
  [datom-set pattern]
  (into #{}
        (remove (predicate-from-pattern pattern))
        datom-set))

(defn lookup-attribute
  "Look up an attribute `a` from `attribs` or from the built-in attributes."
  [attribs a]
  (or (attribs a)
      (built-in-attribs a)
      (throw (ex-info (format "Attribute not found: %s" (pr-str a))
                      {:attribs attribs
                       :attrib a}))))

(defn accumulate-datom
  "Given a setup, a set `datom-set` to accumulate to and a `datom`, accumulate the datom."
  [{:keys [attribs]} datom-set [e a v tx added :as datom]]
  {:pre [(set? datom-set)
         (vector? datom)
         (boolean? added)]}
  (if added
    (let [{:keys [cardinality]} (lookup-attribute attribs a)
          datom-to-add [e a v tx]]
      (case cardinality
        :db.cardinality/many (if (some (predicate-from-pattern [e a v]) datom-set)
                               datom-set
                               (-> datom-set (remove-pattern [e a v]) (conj datom-to-add)))
        :db.cardinality/one (-> datom-set (remove-pattern [e a nil]) (conj datom-to-add))))
    (remove-pattern datom-set [e a v nil])))

(defn accumulate-datoms
  "Accumulate multiple datoms to `datom-set`."
  [setup datom-set datoms]
  (reduce (fn [datom-set datom] (accumulate-datom setup datom-set datom))
          datom-set
          datoms))

(defn combine-two-datoms
  "Create a new datom by randomly mixing the entity and value parts."
  [a b tx]
  {:pre [(vector? a)
         (vector? b)
         (<= 4 (count a))
         (<= 4 (count b))]}
  (let [i (rand-nth [0 2])]
    (assoc a
           i (nth b i)
           3 tx
           4 true)))

(defn combine-random-datoms
  "Helper to `sample-datoms-impl`. Sample two random datoms of the same attribute, mix their elements and return a vector with the single element being that datom. Return nil if failure."
  [datoms tx]
  (when (seq datoms)
    (when-let [group (->> (group-by second datoms)
                          vals
                          (filter #(<= 2 (count %)))
                          seq)]
      (when-let [[a b] (->> group
                            shuffle
                            first
                            shuffle
                            seq)]
        [(combine-two-datoms a b tx)]))))

(defn- datom-equivalence-fn
  "This function creates another function such that there must not be two different datoms in the same transaction for which that function returns the same value. This is to avoid ambiguous (maybe ill-defined) situations where, for example, we might add and delete the same datom in the same transaction. Datahike may have a strategy to deal with this, but it might be an implementation detail. Not sure."
  [attribs]
  (fn [[e a v]]
    (case (:cardinality (lookup-attribute attribs a))
      :db.cardinality/many [e a v]
      :db.cardinality/one [e a])))

(defn- sample-datoms-impl
  "Helper for `sample-datoms`. This function creates a sequence of datoms and tracks which datoms have been created so far in order to generate real-looking data."
  [{:keys [actions tx-timestep-T-ms attribs] :as setup}
   current-set
   transacted-set
   current-tx-eav
   tx
   tx-epoch-ms]
  {:pre [(number? tx)]}
  (let [equiv-fn (datom-equivalence-fn attribs)
        action (weighted-sample actions)
        step (fn step
               ([result] (step tx tx-epoch-ms result))
               ([tx+ tx-epoch-ms result0]
                {:pre [(number? tx)
                       (number? tx-epoch-ms)
                       (or (nil? result0) (sequential? result0))]}
                (let [result (into []
                                   (remove (comp current-tx-eav equiv-fn))
                                   result0)
                      current-set (accumulate-datoms setup current-set result)
                      emit-tx? (not= tx tx+)]
                  (concat
                   result
                   (lazy-seq
                    (sample-datoms-impl
                     setup
                     current-set
                     (if emit-tx? current-set transacted-set)
                     (into (if emit-tx? #{} current-tx-eav)
                           (map equiv-fn)
                           result)
                     tx+
                     tx-epoch-ms))))))]
    (case action
      :transact (step (inc tx)
                      (+ tx-epoch-ms (long (sample-transaction-timestep tx-timestep-T-ms)))
                      [[tx :db/txInstant (Date. tx-epoch-ms) tx true]])
      :add (step [(generate-datom setup tx true)])
      :combine (step (combine-random-datoms current-set tx))
      (step (when-let [s (seq (into [] (remove built-in-datom?) transacted-set))]
              (let [[e a v :as datom] (rand-nth s)]
                (case action
                  :remove [[e a v tx false]]
                  :tweak [(combine-two-datoms
                           (generate-datom-by-attrib
                            setup a tx true)
                           datom
                           tx)])))))))

(defn sample-datoms
  "Generate an infinite sequence of random datoms sampled according to the setup."
  [{:keys [tx-offset tx-epoch-ms] :as setup}]
  (sample-datoms-impl setup #{} #{} #{} tx-offset tx-epoch-ms))

(defn id-or-temp-id [id-map id]
  (id-map id (str "tmp" id)))

(defn unwrap-temp-id
  "Remove the `tmp` prefix from a generated temp id, so that for example `tmp119` results in integer `119`."
  [tempid]
  {:post [(number? %)]}
  (when-let [[_ id] (re-matches #"^tmp(\d+)$" tempid)]
    (Long/parseLong id)))

(defn transact-datoms
  "Given a setup, a connection and datoms to be transacted, transact the datoms and return a map from input entity ids to assigned entity ids."
  [{:keys [attribs]} conn datoms]
  {:pre [(map? attribs)]}
  (transduce (partition-by (fn [[_ _ _ tx]] tx))
             (fn
               ([id-map] id-map)
               ([id-map transaction]
                (let [[[_ _ _ tx]] transaction
                      list-forms (for [[e a v _ added] transaction
                                       :when (not (built-in-attribs a))]
                                   [(if added :db/add :db/retract)
                                    (id-or-temp-id id-map e)
                                    a
                                    (if (ref-attrib? attribs a)
                                      (id-or-temp-id id-map v)
                                      v)])
                      {:keys [tempids] :as result} (d/transact conn {:tx-data (vec list-forms)})]
                  (into (assoc id-map tx (:db/current-tx tempids))
                        (keep (fn [[tmpid assigned-id]]
                                (when (string? tmpid)
                                  [(unwrap-temp-id tmpid) assigned-id])))
                        tempids))))
             {}
             datoms))

(defn map-ids
  "Given a setup, a map of assigned ids from transactions and datoms that were transacted, map to assigned ids."
  ([setup id-map ident->entity datoms]
   (map-ids setup id-map ident->entity datoms false))
  ([{:keys [attribs]} id-map ident->entity datoms allow-incomplete-mapping?]
   (mapv (fn [[e a v tx added :as datom]]
           {:pre [(keyword? a)]}
           [(or (id-map e)
                (when allow-incomplete-mapping? e)
                (throw (ex-info "Missing id at e" {:datom datom})))
            (ident->entity a a)
            (if (ref-attrib? attribs a)
              (or (id-map v)
                  (when allow-incomplete-mapping? v)
                  (throw (ex-info "Missing id at v" {:datom datom})))
              v)
            (or (id-map tx)
                (when allow-incomplete-mapping? tx)
                (throw (ex-info "Missing id at tx" {:datom datom})))
            (or added (< (count datom) 5))])
         datoms)))

(defn ident->entity-map [datoms]
  (into {}
        (keep (fn [[e a v]]
                (when (= a :db/ident)
                  [v e])))
        datoms))

(def indexes [{:current :eavt
               :temporal :temporal-eavt}
              {:current :avet
               :temporal :temporal-avet}
              {:current :aevt
               :temporal :temporal-aevt}])

(def queries-to-test [{:make-query (fn [[_e a v]]
                                     (conj '[:find ?e
                                             :in $
                                             :where]
                                           ['?e a v]))
                       :free-inds [0]}
                      {:make-query (fn [[e a _v]]
                                     (conj '[:find ?v
                                             :in $
                                             :where]
                                           [e a '?v]))
                       :free-inds [2]}])

(defn transaction-entity-ids [datoms]
  (sort
   (into []
         (keep (fn [[e a _ _ added]]
                 (when (and added (= a :db/txInstant)) e)))
         datoms)))

(defn frac-nth [coll frac]
  (let [i (Math/round (* frac (dec (count coll))))]
    (nth coll i)))

(deftest basic-test
  (is (= #{} (remove-pattern #{} [1 :a 2 3])))
  (is (= #{} (remove-pattern #{[1 :a 2 3]} [1 :a 2 3])))
  
  (is (= #{[1 :a 2 3]} (remove-pattern #{[1 :a 2 3]} [2 :a 2 3])))
  (is (= #{[1 :a 2 3]} (remove-pattern #{[1 :a 2 3]} [1 :b 2 3])))
  (is (= #{[1 :a 2 3]} (remove-pattern #{[1 :a 2 3]} [1 :a 200 3])))
  (is (= #{[1 :a 2 3]} (remove-pattern #{[1 :a 2 3]} [1 :a 2 3000])))
  
  (is (= #{} (remove-pattern #{[1 :a 2 3]} [nil :a 2 3])))
  (is (= #{} (remove-pattern #{[1 :a 2 3]} [1 nil 2 3])))
  (is (= #{} (remove-pattern #{[1 :a 2 3]} [1 :a nil 3])))
  (is (= #{} (remove-pattern #{[1 :a 2 3]} [1 :a 2 nil])))
  (let [s0 (accumulate-datom setup0 #{} [3 :concept/attrib0 4 0 true])
        s1 (accumulate-datom setup0 s0 [5 :concept/attrib0 4 0 true])
        s2 (accumulate-datom setup0 s1 [7 :concept/attrib2 8 0 true])]
    (is (= #{[3 :concept/attrib0 4 0]} s0))
    (is (= #{[5 :concept/attrib0 4 0] [3 :concept/attrib0 4 0]} s1))
    (is (= #{[5 :concept/attrib0 4 0]
             [7 :concept/attrib2 8 0]
             [3 :concept/attrib0 4 0]}
           s2))))

(defn partition-select-tx
  "Generate partitions of datoms based on the txs that we are interested in."
  [datoms tx-to-select]
  (let [datoms (vec datoms)
        [_ _ _ last-tx] (peek datoms)
        tx-to-select-set (set tx-to-select)]
    (->> datoms
         (into {-1 0
                last-tx (count datoms)}
               (comp (map-indexed (fn [i [_ _ _ tx _]]
                                    (when (tx-to-select-set tx)
                                      [tx (inc i)])))
                     (remove nil?)))
         (sort-by second)
         (partition 2 1)
         (into [] (map (fn [[[_lower-tx lower-index]
                             [upper-tx upper-index]]]
                         [upper-tx (subvec datoms lower-index upper-index)]))))))

(defn accumulate-partitioned-datoms
  "Accumulate partitions of datoms and whenever we have accumulated a partition, associate it into a map for the id of the transaction. Return the final set of datoms and the map."
  [setup partitions]
  (reduce (fn [[acc tx->acc] [tx partition]]
            (let [acc (accumulate-datoms setup acc partition)]
              [acc (assoc tx->acc tx acc)]))
          [#{} {}]
          partitions))

(defn perform-generative-test [cfg setup datom-count relative-time-pts]
  (let [attribs (:attribs setup)

        ;; Generate two sets of datoms:
        ;; Datoms that should be transacted and a second set of unrelated datoms
        ;; for negative testing.
        {:keys [datoms neg-candidate-datoms] :as random-test-data}
        (into {} (for [k [:datoms :neg-candidate-datoms]]
                   [k (into [] (take datom-count) (sample-datoms setup))]))

        temp-file (File/createTempFile "sample_datoms" ".edn")
        
        _ (spit temp-file (pr-str random-test-data))
        _ (println "Random test data saved to" temp-file)
        
        ;; Assemble all datoms to be transacted along with
        ;; their schema derived from the setup.
        all-datoms (into (schema-datoms setup) datoms)]
    (d/create-database cfg)
    (try
      (let [conn (d/connect cfg)

            ;; Here we populate the database defined by `cfg`
            ;; using all the datoms. The return value, `old->assigned`,
            ;; is a map from the entity ids used by all-datoms to the entity
            ;; ids assigned by Datahike when it transacts them.
            old->assigned (transact-datoms setup conn all-datoms)

            ;; Apply the `old->assigned` mapping to the entity ids of `all-datoms`.
            all-mapped-datoms (map-ids setup old->assigned {} all-datoms)

            ;; The same as for all-mapped-datoms except that we omit
            ;; mapping the schema datoms.
            mapped-datoms (map-ids setup old->assigned {} datoms)

            ;; Build a map from keyword to entity id that lets us map the attribute
            ;; keywords to their corresponding entity ids
            ident->entity (ident->entity-map all-mapped-datoms)

            ;; A sequence of all distinct transaction entity ids.
            tx-ids (transaction-entity-ids mapped-datoms)

            ;; A sequence of the entity ids that we will provide to as-of for testing
            ;; historical databases.
            tx-ids-to-test (for [f relative-time-pts] (frac-nth tx-ids f))

            ;; Split the ordered sequence of datoms into parts at every point
            ;; where a transaction specified by tx-ids-to-test has been completed.
            ;; Also add a part for the remaining datoms up to the last datom.
            ;; Return pairs of transaction ids and datoms up to that transaction
            ;; from the previous transaction.
            partitioned-datoms (partition-select-tx mapped-datoms tx-ids-to-test)


            ;; Given the partitions, return the datoms expected to exist in the final
            ;; current state of the database, and a map that maps the transaction id
            ;; to the set of datoms expected to be contained in the as-of-database
            ;; of that transaction.
            [positive-datom-examples tx->acc-datoms]
            (accumulate-partitioned-datoms setup partitioned-datoms)


            ;; Construct a set of all eav-triplets for the datoms expected to exist
            ;; in the current database ...
            current-eav (into #{} (map (fn [[e a v]] [e a v])) positive-datom-examples)

            ;; ... and use that set to filter away datoms so that we obtain a set
            ;; of negative examples that are guaranteed to not exist in the current database.
            negative-datom-examples (into []
                                          (comp cat 
                                                (remove (fn [[e a v]] (contains? current-eav [e a v]))))
                                          [mapped-datoms
                                           (map-ids setup old->assigned {} neg-candidate-datoms true)])

            ;; Assemble a flat list of pairs, where the first element of each pair is a boolean
            ;; that is true if and only if the second element of the pair is a datom that
            ;; exists in the current database.
            datoms-to-test (for [[exists datom-set] [[true positive-datom-examples]
                                                     [false negative-datom-examples]]
                                 datom datom-set]
                             [exists datom])
            
            db (d/db conn)]

        (testing (format "Verify existence of %d datoms and the non-existence of %d datoms in all indices"
                         (count positive-datom-examples)
                         (count negative-datom-examples))
          (doseq [ ;; Iterate over every datom whose existence we are testing
                  [exists [e a-key v tx :as datom]] datoms-to-test
                  :when (not (built-in-datom? datom))

                  ;; Iterate over every current index
                  {:keys [current]} indexes

                  :let [attrib-data (attribs a-key)
                        a-index (ident->entity a-key)
                        db-index (get db current)
                        datom-obj (dd/datom e a-index v tx)]

                  ;; It seems like AVET needs special treatment and that it is
                  ;; only meaningful to test datoms whose values are references.
                  :when (or (not= :avet current) (= :db.type/ref (:type attrib-data)))]
            (is (number? a-index))

            ;; The index should contain the datom
            (is (= exists (contains? db-index datom-obj)))))
        
        (testing "Check that the indexes are sorted"
          (doseq [ ;; Iterate over the indexes (:eavt :avet :aevt)
                  current-and-temporal indexes
                  :let [index (:current current-and-temporal)
                        cmp (dd/index-type->cmp-quick index true)]
                  [a b] (into [] (partition 2 1 (get db index)))]
            ;; The relation between any consecutive pair of datoms is <=
            (is (not (pos? (cmp a b))))))

        
        (testing "Simple queries against current and past indexes"
          (doseq [ ;; Iterate over the databases (current and past ones), along with
                  ;; their respective expected datoms
                  [_tx db-to-test existing-datoms] (into [[:current db positive-datom-examples]]
                                                         (map (fn [tx]
                                                                [tx
                                                                 (d/as-of db tx)
                                                                 (tx->acc-datoms tx)]))
                                                         tx-ids-to-test)
                  ;; Iterate over datoms that we are going to queery
                  [_exists [e a-key v :as datom]] datoms-to-test
                  :when (not (built-in-datom? datom))

                  ;; Iterate over the queries to test
                  {:keys [make-query free-inds]} queries-to-test

                  :let [ ;; Populate the query with constant values from the datom
                        query (make-query datom)

                        ;; Run the query
                        result (set (d/q query db-to-test))

                        ;; Compute the expected result from the query
                        pattern (reduce (fn [pattern i] (assoc pattern i nil))
                                        [e a-key v]
                                        free-inds)
                        expected (into #{}
                                       (comp (filter (predicate-from-pattern pattern))
                                             (map #(mapv (partial nth %) free-inds)))
                                       existing-datoms)]]
            (is (= expected result)))))
      (finally
        (d/delete-database cfg)))))

(deftest generative-test
  (perform-generative-test {:attribute-refs? true
                            :index :datahike.index/persistent-set
                            :keep-history true
                            :name (str "gendb" (rand-int 9999999))
                            :schema-flexibility :write
                            :store {:backend :memory
                                    :id (UUID/randomUUID)}}
                           setup0
                           1000
                           [0.3 0.5 0.7]))
