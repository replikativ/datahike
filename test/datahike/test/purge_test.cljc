(ns datahike.test.purge-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datahike.api :as d]
   [datahike.test.utils :as tu]))

#?(:cljs (def Throwable js/Error))

(def schema-tx [{:db/ident       :name
                 :db/valueType   :db.type/string
                 :db/unique      :db.unique/identity
                 :db/index       true
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :age
                 :db/valueType   :db.type/long
                 :db/cardinality :db.cardinality/one}
                {:name "Alice"
                 :age  25}
                {:name "Bob"
                 :age  35}])

(def cfg-template {:store {:backend :memory
                           :id #uuid "001b0000-0000-0000-0000-00000000001b"}
                   :keep-history? true
                   :schema-flexibility :write
                   :initial-tx schema-tx})

(defn find-age [db name]
  (d/q '[:find ?a . :in $ ?n :where [?e :name ?n] [?e :age ?a]] db name))

(defn find-entity [db name]
  (d/q '[:find (pull ?e [:name :age]) :in $ ?n :where [?e :name ?n]] db name))

(defn find-entities [db]
  (into #{}
        (d/q '[:find [(pull ?e [:name :age]) ...] :where [?e :name _]] db)))

(deftest test-purge
  (let [conn (tu/setup-db (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000001"))]
    (testing "retract datom, data is removed from current db and found in history"
      (let [name "Alice"]
        (d/transact conn [[:db/retract [:name name] :age 25]])
        (are [x y] (= x y)
          true (nil? (find-age @conn name))
          25 (find-age (d/history @conn) name))))
    (testing "purge datom from current index and from history"
      (let [name "Bob"]
        (d/transact conn [[:db/purge [:name name] :age 35]])
        (are [x y] (= x y)
          true (nil? (find-age @conn name))
          true (nil? (find-age (d/history @conn) name)))))
    (testing "purge retracted datom"
      (let [name "Alice"]
        (d/transact conn [[:db/purge [:name name] :age 25]])
        (are [x y] (= x y)
          nil (find-age @conn name)
          nil (find-age (d/history @conn) name))))
    (d/release conn)))

(deftest test-purge-attribute
  (let [conn (tu/setup-db (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000002"))]
    (testing "purge attribute from current index"
      (let [name "Alice"]
        (d/transact conn [[:db.purge/attribute [:name name] :age]])
        (are [x y] (= x y)
          true (nil? (find-age @conn name))
          true (nil? (find-age (d/history @conn) name))
          #{["Alice"] ["Bob"]} (d/q '[:find ?n :where [_ :name ?n]] @conn))))
    (testing "retract attribute from current index and purge from history"
      (let [name "Bob"]
        (testing "retracting from current index"
          (d/transact conn [[:db.fn/retractAttribute [:name name] :age]])
          (are [x y] (= x y)
            true (nil? (find-age @conn name))
            35 (find-age (d/history @conn) name)))
        (testing "purging from history"
          (d/transact conn [[:db.purge/entity [:name name] :age]])
          (are [x y] (= x y)
            true (nil? (find-age @conn name))
            true (nil? (find-age (d/history @conn) name))))))
    (d/release conn)))

(deftest test-purge-entity
  (let [conn (tu/setup-db (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000003"))]
    (testing "purge entity from current index"
      (is (= #{{:name "Alice" :age 25} {:name "Bob" :age 35}} (find-entities @conn)))
      (d/transact conn [[:db.purge/entity [:name "Alice"]]])
      (is (= #{{:name "Bob" :age 35}} (find-entities @conn)))
      (is (= #{{:name "Bob" :age 35}} (find-entities (d/history @conn)))))
    (testing "retract entity from current index and purge from history"
      (let [name "Bob"]
        (testing "retracting from current index"
          (d/transact conn [[:db/retractEntity [:name name]]])
          (is (= #{} (find-entities @conn)))
          (is (= #{{:name "Bob" :age 35}} (find-entities (d/history @conn)))))
        (testing "purging from history"
          (d/transact conn [[:db.purge/entity [:name name]]])
          (is (= #{} (find-entities @conn)))
          (is (= #{} (find-entities (d/history @conn)))))))
    (testing "purge something that is not present in the database"
      (is (thrown-with-msg? Throwable
                            #"Can't find entity with ID \[:name \"Alice\"\] to be purged"
                            (d/transact conn [[:db.purge/entity [:name "Alice"]]]))))
    (d/release conn)))

(deftest test-purge-non-temporal-database
  (let [conn (tu/setup-db (-> (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000004")
                              (assoc :keep-history? false)))]
    (testing "purge data in non temporal database"
      (is (thrown-with-msg? Throwable #"Purge entity is only available in temporal databases\."
                            (d/transact conn [[:db.purge/entity [:name "Alice"]]]))))
    (d/release conn)))

(defn find-ages [db name]
  (d/q '[:find ?a ?op
         :in $ ?n
         :where
         [?e :name ?n]
         [?e :age ?a ?t ?op]]
       db
       name))

(deftest test-history-purge-before
  (let [conn (tu/setup-db (assoc-in cfg-template [:store :id] #uuid "09000000-0000-0000-0000-000000000005"))
        name "Alice"]
    (testing "remove all historical data before date"
      (is (= #{[25 true]}
             (find-ages @conn name)))
      (let [upsert-date (java.util.Date.)]
        (d/transact conn [{:db/id [:name name] :age 30}])
        (is (= #{[30 true]}
               (find-ages @conn name)))
        (is (= #{[25 true] [25 false] [30 true]}
               (find-ages (d/history @conn) name)))
        (d/transact conn [[:db.history.purge/before upsert-date]])
        (is (= #{[30 true]}
               (find-ages @conn name)))
        (is (= #{[25 false] [30 true]}
               (find-ages (d/history @conn) name)))
        (d/transact conn [[:db.history.purge/before (java.util.Date.)]])
        (is (= #{[30 true]}
               (find-ages (d/history @conn) name)))))
    (d/release conn)))
