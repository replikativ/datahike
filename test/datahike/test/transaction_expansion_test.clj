(ns datahike.test.transaction-expansion-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.experimental.transaction :as tx]))

(defn- outcome [db forms limit]
  (try (:tx-data (tx/prepare db forms limit))
       (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))

(deftest ordinary-transaction-expansion
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :sample/id :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                          {:db/ident :sample/child :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one :db/isComponent true}
                          {:db/ident :sample/link :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/one}])
        (let [forms (concat [{:db/id -1 :sample/id 1 :sample/child -2}
                             (into {:db/id -2 :sample/id 2}
                                   (map (fn [n] [(keyword "sample" (str n)) n]) (range 32)))]
                            (map (fn [n] {:sample/link -2 :sample/id (+ 3 n)}) (range 8)))
              {:keys [tempids]} (d/transact conn (vec forms))
              db @conn
              parent (get tempids -1)
              child (get tempids -2)
              redundant [:db/add child :sample/id 2]]
          (let [retry [[:db/add -1 :sample/other "updated"] [:db/add -1 :sample/id 1]]]
            (is (= :transact/expansion-limit (outcome db retry 6)))
            (is (= (:tx-data (d/with db retry)) (outcome db retry 7))))
          (is (= (set (keys (d/with db []))) (set (keys (tx/prepare db [] 0)))))
          (is (= [] (outcome db [] 0)))
          (let [counts (atom [])
                record! #(swap! counts conj %)]
            (is (empty? (:tx-data (tx/prepare db [redundant] 9 record!))))
            (is (thrown? clojure.lang.ExceptionInfo (tx/prepare db [redundant redundant] 1 record!)))
            (is (thrown? clojure.lang.ExceptionInfo (tx/prepare db [42] 9 record!)))
            (is (= [1 1 1] @counts)))
          (is (= [] (outcome db [redundant] 1)))
          (is (= :transact/expansion-limit (outcome db [redundant redundant] 1)))
          (is (= :transact/expansion-limit (outcome db [{:db/id child :sample/id 2}] 1)))
          (doseq [ops [[[:db/retractEntity child]] [[:db.fn/retractEntity parent]]
                       [[:db.fn/retractAttribute parent :sample/child]]
                       [[:db/retract child :sample/id 2]]]]
            (let [ordinary (:tx-data (d/with db ops))]
              (is (= :transact/expansion-limit (outcome db ops 2)))
              (is (= ordinary (outcome db ops 256)))))
          (is (= [] (outcome db [[:db/retract child :sample/id 420]] 2)))
          (is (= :transact/expansion-limit
                 (outcome db [[:db/retract child :sample/id 420]] 1)))
          (is (= 2 (:sample/id (d/entity @conn child)))))
        (finally (d/release conn) (d/delete-database cfg))))))
