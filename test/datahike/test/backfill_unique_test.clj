(ns datahike.test.backfill-unique-test
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.backfill.unique :as unique]
            [datahike.backfill.effects :as effects]
            [datahike.datom :as dd]
            [datahike.db :as db]
            [datahike.index.interface :as di]))

(defn- check-result [f]
  (try (f) :valid
       (catch clojure.lang.ExceptionInfo e
         (if (= :transact/schema (:error (ex-data e))) :duplicate (throw e)))))

(defn- capturing [database]
  (-> database
      (dissoc :avet-build-effects)
      (assoc :avet-build {:id (random-uuid)
                          :attrs (into #{:value :other} (get-in database [:rschema :db/index]))})))

(defn- base [refs?]
  (d/db-with (db/empty-db nil {:index :datahike.index/persistent-set :attribute-refs? refs? :keep-history? true
                               :schema-flexibility :write})
             [{:db/ident :value :db/valueType :db.type/long :db/index true
               :db/cardinality :db.cardinality/one}
              {:db/ident :other :db/valueType :db.type/long :db/index true
               :db/cardinality :db.cardinality/one}]))

(deftest full-checks-use-current-not-historical-values
  (doseq [refs? [false true]]
    (let [initial (d/db-with (base refs?) [{:db/id 10000 :value 1}
                                           {:db/id 10001 :value 2}])
          duplicate (d/db-with initial [[:db/add 10001 :value 1]])
          resolved (d/db-with duplicate [[:db/add 10000 :value 3]])]
      (is (nil? (unique/validate! initial (:avet initial) #{:value})))
      (is (thrown? clojure.lang.ExceptionInfo
                   (unique/validate! duplicate (:avet duplicate) #{:value})))
      (is (nil? (unique/validate! resolved (:avet resolved) #{:value}))))))

(deftest delta-checks-run-after-complete-transaction
  (doseq [refs? [false true]]
    (let [initial (assoc (d/db-with (base refs?) [{:db/id 10000 :value 1}
                                                  {:db/id 10001 :value 2}])
                         :avet-build {:id (random-uuid) :attrs #{:value :other}})
          duplicate (d/db-with initial [[:db/add 10001 :value 1]])
          swapped (d/db-with initial [[:db/add 10000 :value 2]
                                      [:db/add 10001 :value 1]])
          unrelated (d/db-with initial [{:db/id 10000 :other 9}
                                        {:db/id 10001 :other 9}])]
      (is (thrown? clojure.lang.ExceptionInfo
                   (unique/validate-effects! duplicate (:avet duplicate) #{:value}
                                             (:avet-build-effects duplicate))))
      (is (nil? (unique/validate-effects! swapped (:avet swapped) #{:value}
                                          (:avet-build-effects swapped))))
      (is (nil? (unique/validate-effects! unrelated (:avet unrelated) #{:value}
                                          (:avet-build-effects unrelated)))))))

(deftest native-array-and-tuple-value-semantics
  (doseq [refs? [false true]
          [label properties first-value equal-value other-value]
          [[:bytes {}
            (byte-array [1 2]) (byte-array [1 2]) (byte-array [1 3])]
           [:floats {}
            (float-array [1.0 2.0]) (float-array [1.0 2.0]) (float-array [1.0 3.0])]
           [:doubles {}
            (double-array [1.0 2.0]) (double-array [1.0 2.0]) (double-array [1.0 3.0])]
           [:tuple {:db/valueType :db.type/tuple :db/tupleTypes [:db.type/long :db.type/string]}
            [1 "a"] [1 "a"] [1 "b"]]
           [:array-tuple {}
            [(byte-array [1 2]) 1] [(byte-array [1 2]) 1] [(byte-array [1 3]) 1]]]
          :when (or (not refs?) (= label :tuple))]
    (testing (str label " refs=" refs?)
      (let [database (if (= label :tuple)
                       (d/db-with (db/empty-db nil {:index :datahike.index/persistent-set :attribute-refs? refs? :keep-history? true
                                                    :schema-flexibility :write})
                                  [(merge {:db/ident :value :db/index true
                                           :db/cardinality :db.cardinality/one} properties)])
                       ;; Native array ordering is independent of the schema
                       ;; parser. These values use the supported flexible schema
                       ;; path; array value-type refs are not system entities.
                       (db/empty-db {:value {:db/index true :db/cardinality :db.cardinality/one}}
                                    {:index :datahike.index/persistent-set
                                     :keep-history? true :schema-flexibility :read}))
            initial (capturing (d/db-with database [{:db/id 10000 :value first-value}
                                                    {:db/id 10001 :value other-value}]))
            duplicate (d/db-with initial [[:db/add 10001 :value equal-value]])]
        (is (zero? (dd/compare-value first-value equal-value)))
        (is (= :valid (check-result #(unique/validate! initial (:avet initial) #{:value}))))
        (is (= :duplicate (check-result #(unique/validate! duplicate (:avet duplicate) #{:value}))))
        (is (= :duplicate (check-result #(unique/validate-effects! duplicate (:avet duplicate) #{:value}
                                                                   (:avet-build-effects duplicate)))))))))

(deftest delta-and-full-validation-agree-after-complete-random-transactions
  (doseq [refs? [false true]]
    (let [rng (java.util.Random. 701)
          initial (capturing (d/db-with (base refs?)
                                        (mapv #(hash-map :db/id (+ 10000 %) :value %) (range 6))))]
      (loop [database initial iteration 0]
        (when (< iteration 75)
          (let [tx (vec (repeatedly (inc (.nextInt rng 4))
                                    (fn [] [(if (zero? (.nextInt rng 4)) :db/retract :db/add)
                                            (+ 10000 (.nextInt rng 6))
                                            (if (zero? (.nextInt rng 5)) :other :value)
                                            (long (.nextInt rng 9))])))
                database (dissoc database :avet-build-effects)
                after (d/db-with database tx)
                operations (:avet-build-effects after)
                replayed (reduce effects/replay {:current (:avet database)
                                                 :temporal (:temporal-avet database)} operations)
                full (check-result #(unique/validate! after (:current replayed) #{:value}))
                delta (check-result #(unique/validate-effects! after (:current replayed) #{:value} operations))]
            (is (= (mapv #(vec (seq %)) (:avet after))
                   (mapv #(vec (seq %)) (:current replayed))))
            (is (= full delta) (str "complete transaction " iteration " " tx))
            ;; Delta validation assumes the previous candidate was checked.
            ;; Rejected candidates are discarded, never used as the next base.
            (recur (if (= :valid full) after database) (inc iteration))))))))

(deftest delta-validation-only-seeks-touched-values
  (let [database (base false)
        changed (dd/datom 10000 :value 7 100)
        unrelated (dd/datom 10000 :other 8 100)
        sought (atom [])
        current (:avet database)]
    (with-redefs [di/-slice (fn [_ from to family]
                              (swap! sought conj [(:a from) (:v from) (:a to) (:v to) family])
                              nil)]
      (is (nil? (unique/validate-effects! database current #{:value}
                                          [[:temporal :remove changed nil]
                                           [:current :upsert unrelated nil]
                                           [:current :insert changed nil]]))))
    (is (= [[:value 7 :value 7 :avet]] @sought)))
  (let [database (base false)
        first-match (dd/datom 10000 :value 7 100)
        second-match (dd/datom 10001 :value 7 100)]
    (with-redefs [di/-slice (fn [& _]
                              (concat [first-match second-match]
                                      (lazy-seq (throw (AssertionError. "Read beyond duplicate pair")))))]
      (is (= :duplicate
             (check-result #(unique/validate-effects! database (:avet database) #{:value}
                                                      [[:current :insert first-match nil]])))))))
