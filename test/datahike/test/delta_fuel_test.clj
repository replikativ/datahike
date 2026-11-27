(ns datahike.test.delta-fuel-test
  (:require [clojure.test :refer [deftest is]]
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
