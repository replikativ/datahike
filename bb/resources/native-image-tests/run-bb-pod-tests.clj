#!/usr/bin/env bb

(require '[babashka.pods :as pods]
         '[clojure.test :refer [run-tests deftest testing is]])
(import '[java.util Date])

(pods/load-pod "./dthk")

(require '[datahike.pod :as d])

(def config {:keep-history? true,
             :search-cache-size 10000,
             :index :datahike.index/persistent-set,
             :store {:id "inexpensive-red-fox", :backend :mem :scope "test.datahike.io"},
             :store-cache-size 1000,
             :attribute-refs? false,
             :writer {:backend :self},
             :crypto-hash? false,
             :schema-flexibility :read,
             :branch :db})

(deftest pod-workflow

  (testing "delete-database"
    (is (= nil
           (d/delete-database config))))

  (testing "create-database"
    (is (= {:keep-history? true
            :search-cache-size 10000
            :index :datahike.index/persistent-set
            :store {:id "inexpensive-red-fox", :backend :mem :scope "test.datahike.io"}
            :store-cache-size 1000
            :attribute-refs? false
            :writer {:backend :self}
            :crypto-hash? false
            :schema-flexibility :read
            :branch :db}
           (d/create-database config))))

  (testing "database-exists?"
    (is (= true
           (d/database-exists? config))))

  (let [conn (d/connect config)]
    (testing "connect"
      (is (= "conn:2061701040"
             conn)))
    (testing "transact"
      (is (= [:tempids :db-before :db-after :tx-meta :tx-data]
             (keys (d/transact conn [{:name  "Alice", :age   20}
                                     {:name  "Bob", :age   30}
                                     {:name  "Charlie", :age   40}
                                     {:age 15}])))))
    ; (testing "transact with bad arg"
    ;   (is (thrown? clojure.lang.ExceptionInfo
    ;                (keys (d/transact
    ;                       "foo"
    ;                       [{:name  "Alice", :age   20}
    ;                        {:name  "Bob", :age   30}
    ;                        {:name  "Charlie", :age   40}
    ;                        {:age 15}])))))
    (testing "with-db"
      (is (= #{[2 "Bob" 30] [1 "Alice" 20] [3 "Charlie" 40]}
             (d/with-db [db (d/db conn)]
               (d/q {:query '{:find [?e ?n ?a]
                              :where
                              [[?e :name ?n]
                               [?e :age ?a]]}
                     :args [(d/db conn)]})))))
    (testing "release-db"
      (let [db (d/db conn)]
        (is (= {}
               (d/release-db db)))))
    (testing "q"
      (is (= #{[2 "Bob" 30] [1 "Alice" 20] [3 "Charlie" 40]}
             (d/q {:query '{:find [?e ?n ?a]
                            :where
                            [[?e :name ?n]
                             [?e :age ?a]]}
                   :args [(d/db conn)]})
             (d/q '[:find ?e ?n ?a
                    :where
                    [?e :name ?n]
                    [?e :age ?a]]
                  (d/db conn)))))
    (let [timestamp (Date.)]
      (Thread/sleep 1)
      (d/transact conn {:tx-data [{:db/id 3 :age 25}]})
      (d/transact conn [{:name "FOO"  :age "BAR"}])
      (testing "pull"
        (is (= {:db/id 1, :age 20, :name "Alice"}
               (d/pull (d/db conn) '[*] 1))))
      (testing "pull-many"
        (is (= [{:db/id 1, :age 20, :name "Alice"}
                {:db/id 2, :age 30, :name "Bob"}
                {:db/id 3, :age 25, :name "Charlie"}]
               (d/pull-many (d/db conn) '[*] [1 2 3]))))
      (testing "metrics"
        (is (= {:per-attr-counts {:age 5, :name 4, :db/txInstant 3}
                :per-entity-counts {1 2, 2 2, 3 2, 4 1, 5 2, 536870913 1, 536870914 1, 536870915 1}
                :count 12, :avet-count 0, :temporal-count 11, :temporal-avet-count 0}
               (d/metrics (d/db conn)))))
      (testing "as-of tx-id"
        (is (= #{[3 "Charlie" 25] [2 "Bob" 30] [5 "FOO" "BAR"] [1 "Alice" 20]}
               (d/q '[:find ?e ?n ?a
                      :where
                      [?e :name ?n]
                      [?e :age ?a]]
                    (d/as-of (d/db conn) 536870916)))))
      (testing "as-of timestamp"
        (is (= #{[2 "Bob" 30] [1 "Alice" 20] [3 "Charlie" 40]}
               (d/q '[:find ?e ?n ?a
                      :where
                      [?e :name ?n]
                      [?e :age ?a]]
                    (d/as-of (d/db conn) timestamp)))))
      (testing "since tx-id"
        (is (= #{[5 "FOO" "BAR"]}
               (d/q '[:find ?e ?n ?a
                      :where
                      [?e :name ?n]
                      [?e :age ?a]]
                    (d/since (d/db conn) 536870914)))))
      (testing "since timestamp"
        (is (= #{[5 "FOO" "BAR"]}
               (d/q '[:find ?e ?n ?a
                      :where
                      [?e :name ?n]
                      [?e :age ?a]]
                    (d/since (d/db conn) timestamp)))))
      (testing "history"
        (is (= #{[3 "Charlie" 25] [2 "Bob" 30] [5 "FOO" "BAR"] [1 "Alice" 20] [3 "Charlie" 40]}
               (d/q '[:find ?e ?n ?a
                      :where
                      [?e :name ?n]
                      [?e :age ?a]]
                    (d/history (d/db conn))))))
      (testing "datoms"
        (is (= '((1 :age 20 536870913 true))
               (d/datoms (d/db conn) :eavt 1 :age 20)))
        (is (= '((2 :age 30 536870913 true) (2 :name "Bob" 536870913 true))
               (d/datoms (d/db conn) {:index :eavt :components [2]}))))
      (testing "schema"
        (d/transact conn {:tx-data [{:db/ident :name :db/valueType :db.type/string :db/unique :db.unique/identity
                                     :db/index true :db/cardinality :db.cardinality/one}
                                    {:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]})
        (is (= {:name {:db/ident :name, :db/valueType :db.type/string, :db/unique :db.unique/identity
                       :db/index true, :db/cardinality :db.cardinality/one, :db/id 6}
                :age {:db/ident :age, :db/valueType :db.type/long, :db/cardinality :db.cardinality/one, :db/id 7}}
               (d/schema (d/db conn))))
        (testing "entity"
          (is (= {:age "BAR" :name "FOO"}
                 (d/entity (d/db conn) 5))))))))

(defn -main [& _args]
  (let [{:keys [fail error]} (run-tests)]
    (when (and fail error (pos? (+ fail error)))
      (System/exit 1))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
