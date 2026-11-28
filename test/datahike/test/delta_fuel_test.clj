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

(deftest point-plan-charges-seeks-and-visible-datoms
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}]
    (d/create-database cfg)
    (let [c (d/connect cfg)]
      (try
        (d/transact c [{:db/ident :example/key :db/valueType :db.type/string
                        :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}])
        (d/transact c [{:example/key "a"}])
        (let [probe (fn [limit key]
                      (q/metered-point-plan @c limit #{} (fn [_ point] (point :avet [:example/key key]))))]
          (is (= [:out-of-fuel {:seek 0 :datom 0}] ((juxt :status :events) (probe 0 "a"))))
          (is (= [:out-of-fuel {:seek 1 :datom 0}] ((juxt :status :events) (probe 1 "a"))))
          (is (= [:ok {:seek 1 :datom 1}] ((juxt :status :events) (probe 2 "a"))))
          (is (= [:ok {:seek 1 :datom 0}] ((juxt :status :events) (probe 1 "b"))))
          (is (thrown? clojure.lang.ExceptionInfo (probe 1 nil)))
          (doseq [[idx prefix] [[:avet [:example/unknown "a"]] [:avet [:example/key]]
                                [:eavt [nil :example/key]] [:aevt [:example/key 1]]]]
            (is (thrown? clojure.lang.ExceptionInfo
                         (q/metered-point-plan @c 1 #{} (fn [_ point] (point idx prefix))))))
          (is (thrown? clojure.lang.ExceptionInfo
                       (q/metered-point-plan @c 1 #{} (fn [charge! _] (charge! :other)))))
          (is (thrown? clojure.lang.ExceptionInfo
                       (q/metered-point-plan @c 1 #{:seek} (fn [_ _] nil)))))
        (finally (d/release c) (d/delete-database cfg))))))
