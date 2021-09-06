(ns datahike.test.migrate
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datahike.migrate :as dm]
            [datahike.datom :as dd]))

(def tx-data [[:db/add 1 :db/cardinality :db.cardinality/one 536870913 true]
              [:db/add 1 :db/ident :name 536870913 true]
              [:db/add 1 :db/index true 536870913 true]
              [:db/add 1 :db/unique :db.unique/identity 536870913 true]
              [:db/add 1 :db/valueType :db.type/string 536870913 true]
              [:db/add 2 :db/cardinality :db.cardinality/one 536870913 true]
              [:db/add 2 :db/ident :age 536870913 true]
              [:db/add 2 :db/valueType :db.type/long 536870913 true]
              [:db/add 3 :age 25 536870913 true]
              [:db/add 3 :name "Alice" 536870913 true]
              [:db/add 4 :age 35 536870913 true]
              [:db/add 4 :name "Bob" 536870913 true]])

(deftest load-entities-test
  (testing "Test migrate simple datoms"
    (let [source-datoms (->> tx-data
                             (mapv #(-> % rest vec))
                             (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))]
      (let [cfg {:store {:backend :mem
                         :id "target"}}
            _ (d/delete-database cfg)
            _ (d/create-database cfg)
            conn (d/connect cfg)]
        @(d/load-entities conn source-datoms)
        (is (= (into #{} source-datoms)
               (d/q '[:find ?e ?a ?v ?t ?op :where [?e ?a ?v ?t ?op]] @conn)))))))

(deftest coerce-tx-test
  (testing "coerce simple transaction report"
    (let [tx-report {:tx 6,
                     :data
                     [(dd/datom 536870918 :db/txInstant #inst "2021-08-27T12:10:08.954-00:00" 536870918 true)
                      (dd/datom 45 :name "Daisy" 536870918 false)
                      (dd/datom 45 :age 55 536870918 false)]}]
      (is (= {:tx 6
              :data [[536870918 :db/txInstant #inst "2021-08-27T12:10:08.954-00:00" 536870918 true]
                     [45 :name "Daisy" 536870918 false]
                     [45 :age 55 536870918 false]]}
             (dm/coerce-tx tx-report))))))

(defn setup-conn-from-cfg [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))

(deftest closed-circuit-test
  (testing "Exporting and importing a database without ID mapping or configuration change"
    (let [base-cfg {:store {:backend :mem
                            :id "closed-circuit-test"}
                    :attribute-refs? false
                    :schema-flexibility :write
                    :keep-history? true}
          source-cfg (assoc-in base-cfg [:store :id] "closed-circuit-source-test")
          target-cfg (assoc-in base-cfg [:store :id] "closed-circuit-target-test")
          source-txs [[{:db/ident       :name
                        :db/cardinality :db.cardinality/one
                        :db/index       true
                        :db/unique      :db.unique/identity
                        :db/valueType   :db.type/string}
                       {:db/ident       :parents
                        :db/cardinality :db.cardinality/many
                        :db/valueType   :db.type/ref}
                       {:db/ident       :age
                        :db/cardinality :db.cardinality/one
                        :db/valueType   :db.type/long}]
                      [{:name "Alice"
                        :age  25}
                       {:name "Bob"
                        :age 30}]
                      [{:name    "Charlie"
                        :age     5
                        :parents [[:name "Alice"]
                                  [:name "Bob"]]}]
                      [{:db/id [:name "Alice"]
                        :age 26}]
                      [{:name "Daisy"
                        :age 55}]
                      [[:db/retractEntity [:name "Daisy"]]]]
          source-conn (setup-conn-from-cfg source-cfg)
          target-conn (setup-conn-from-cfg target-cfg)
          export-path (str (System/getProperty "java.io.tmpdir") "/closed-circuit-export-test.edn")]
      (doseq [tx source-txs]
        (d/transact source-conn {:tx-data tx}))
      (d/export source-conn export-path {:format :edn})
      (d/import target-conn export-path)
      (let [source-db @source-conn
            target-db @target-conn]
        (is (= (d/datoms source-db :eavt)
               (d/datoms target-db :eavt)))
        (is (= (d/datoms source-db :aevt)
               (d/datoms target-db :aevt)))
        (is (= (d/datoms source-db :avet)
               (d/datoms target-db :avet)))
        (is (= (d/tx-range source-db)
               (d/tx-range target-db)))))))

