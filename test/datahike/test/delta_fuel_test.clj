(ns datahike.test.delta-fuel-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.query :as q]))

(deftest aggregation-exhausts-at-exactly-the-signed-limit
  (let [rows [[0 :nuts :source -5] [1 :nuts :source -5]
              [2 :nuts :transit 10] [3 :bolts :destination 4]]]
    (dotimes [limit 12]
      (let [receipt (q/metered-delta-query q/delta-sum-query rows limit)]
        (is (= :out-of-fuel (:status receipt)))
        (is (= limit (:used receipt) (reduce + (vals (:events receipt)))))))
    (doseq [source [rows (vec (reverse rows))]]
      (let [result (q/metered-delta-query q/delta-sum-query source 12)]
        (is (= #{[:bolts 4] [:nuts 0]} (set (:rows result))))
        (is (= {:scan 4 :project 4 :aggregate 4} (:events result)))
        (is (= 12 (:used result)))))
    (is (= :ok (:status (q/metered-delta-query q/delta-sum-query [] 0))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (q/metered-delta-query '[:find ?x :where [?x :a]] rows 12)))))

(deftest indexed-plan-agrees-with-datalog
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}]
    (d/create-database cfg)
    (let [c (d/connect cfg)]
      (try
        (d/transact c [{:db/ident :invariant.stock/account-key :db/valueType :db.type/string
                        :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
                       {:db/ident :invariant.stock/account-commodity :db/valueType :db.type/keyword
                        :db/cardinality :db.cardinality/one}])
        (d/transact c [{:invariant.stock/account-key "typed" :invariant.stock/account-commodity :bolts}
                       {:invariant.stock/account-key "general"}])
        (let [rows [[0 "typed" :bolts] [1 "typed" :nuts] [2 "general" :nuts] [3 "unknown" :bolts]]
              db @c
              ref-result (set (q/q q/commodity-match-query db rows))]
          (is (= #{[1] [3]} ref-result))
          (doseq [ordered [rows (vec (reverse rows))]]
            (let [complete (q/metered-commodity-query q/commodity-match-query db ordered 18)]
              (is (= ref-result (:rows complete)))
              (is (= {:posting 4 :seek 7 :datom 5 :compare 2} (:events complete))))
            (doseq [n (range 18)]
              (let [r (q/metered-commodity-query q/commodity-match-query db ordered n)]
                (is (= [:out-of-fuel n n] [(:status r) (:used r) (reduce + (vals (:events r)))]))))))
        (finally (d/release c) (d/delete-database cfg))))))
