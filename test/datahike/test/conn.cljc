(ns datahike.test.conn
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datahike.core :as d]
   [datahike.constants :as c]
   [datahike.db :as db]
   [datahike.test.core :as tdc]))

(def user-schema {:aka {:db/cardinality :db.cardinality/many}})
(def user-schema' {:email {:db/unique :db.unique/identity}})

(def non-ref-schema (merge c/non-ref-implicit-schema user-schema))
(def ref-schema (merge c/ref-implicit-schema user-schema))

(def non-ref-datoms #{(d/datom c/e0 :age 17)
                      (d/datom c/e0 :name "Ivan")})

(def ref-datoms #{(d/datom c/u0 :age 17)
                  (d/datom c/u0 :name "Ivan")})

(deftest test-ways-to-create-conn
  (testing "Connections with default storeless config: no attribute references"
    (let [conn (d/create-conn)]
      (is (= #{} (set (d/datoms @conn :eavt))))
      (is (= c/non-ref-implicit-schema (:schema @conn))))

    (let [conn (d/create-conn user-schema)]
      (is (= #{} (set (d/datoms @conn :eavt))))
      (is (= non-ref-schema (:schema @conn))))

    (let [conn (d/conn-from-datoms non-ref-datoms)]
      (is (= non-ref-datoms (set (d/datoms @conn :eavt))))
      (is (= c/non-ref-implicit-schema (:schema @conn))))

    (let [conn (d/conn-from-datoms non-ref-datoms user-schema)]
      (is (= non-ref-datoms (set (d/datoms @conn :eavt))))
      (is (= non-ref-schema (:schema @conn))))

    (let [conn (d/conn-from-db (d/init-db non-ref-datoms))]
      (is (= non-ref-datoms (set (d/datoms @conn :eavt))))
      (is (= c/non-ref-implicit-schema (:schema @conn))))

    (let [conn (d/conn-from-db (d/init-db non-ref-datoms user-schema))]
      (is (= non-ref-datoms (set (d/datoms @conn :eavt))))
      (is (= non-ref-schema (:schema @conn)))))

  (testing "Connections with attribute references"
    (let [conn (d/create-conn nil {:attribute-refs? true})]
      (is (= (set db/ref-datoms) (set (d/datoms @conn :eavt))))
      (is (= c/ref-implicit-schema (:schema @conn))))

    (let [conn (d/create-conn user-schema {:attribute-refs? true})]
      (is (= (set db/ref-datoms) (set (d/datoms @conn :eavt))))
      (is (= ref-schema (:schema @conn))))

    (let [conn (d/conn-from-datoms ref-datoms nil {:attribute-refs? true})]
      (is (= ref-datoms (set (d/datoms @conn :eavt))))
      (is (= c/ref-implicit-schema (:schema @conn))))

    (let [conn (d/conn-from-datoms ref-datoms user-schema {:attribute-refs? true})]
      (is (= ref-datoms (set (d/datoms @conn :eavt))))
      (is (= ref-schema (:schema @conn))))

    (let [conn (d/conn-from-db (d/init-db ref-datoms nil {:attribute-refs? true}))]
      (is (= ref-datoms (set (d/datoms @conn :eavt))))
      (is (= c/ref-implicit-schema (:schema @conn))))

    (let [conn (d/conn-from-db (d/init-db ref-datoms user-schema {:attribute-refs? true}))]
      (is (= ref-datoms (set (d/datoms @conn :eavt))))
      (is (= ref-schema (:schema @conn))))))

(deftest test-reset-conn!
  (testing "Connection resetting with default storeless config: no attribute references"
    (let [conn    (d/conn-from-datoms non-ref-datoms user-schema)
          report  (atom nil)
          _       (d/listen! conn #(reset! report %))
          non-ref-datoms' #{(d/datom c/e0 :age 20)
                            (d/datom c/e0 :sex :male)}
          non-ref-schema' (merge c/non-ref-implicit-schema user-schema')
          db'     (d/init-db non-ref-datoms' user-schema')]
      (d/reset-conn! conn db' :meta)
      (is (= non-ref-datoms' (set (d/datoms @conn :eavt))))
      (is (= non-ref-schema' (:schema @conn)))

      (let [{:keys [db-before db-after tx-data tx-meta]} @report]
        (is (= non-ref-datoms (set (d/datoms db-before :eavt))))
        (is (= non-ref-schema (:schema db-before)))
        (is (= non-ref-datoms' (set (d/datoms db-after :eavt))))
        (is (= non-ref-schema' (:schema db-after)))
        (is (= :meta tx-meta))
        (is (= [[c/e0 :age 17 false]
                [c/e0 :name "Ivan" false]
                [c/e0 :age 20 true]
                [c/e0 :sex :male true]]
               (map (juxt :e :a :v :added) tx-data))))))

  (testing "Connection resetting with attribute references"
    (let [conn    (d/conn-from-datoms ref-datoms user-schema {:attribute-refs? true})
          report  (atom nil)
          _       (d/listen! conn #(reset! report %))
          ref-datoms' #{(d/datom c/u0 :age 20)
                        (d/datom c/u0 :sex :male)}
          ref-schema' (merge c/ref-implicit-schema user-schema')
          db'     (d/init-db ref-datoms' user-schema' {:attribute-refs? true})]
      (d/reset-conn! conn db' :meta)
      (is (= ref-datoms' (set (d/datoms @conn :eavt))))
      (is (= ref-schema' (:schema @conn)))

      (let [{:keys [db-before db-after tx-data tx-meta]} @report]
        (is (= ref-datoms (set (d/datoms db-before :eavt))))
        (is (= ref-schema (:schema db-before)))
        (is (= ref-datoms' (set (d/datoms db-after :eavt))))
        (is (= ref-schema' (:schema db-after)))
        (is (= :meta tx-meta))
        (is (= [[c/u0 :age 17 false]
                [c/u0 :name "Ivan" false]
                [c/u0 :age 20 true]
                [c/u0 :sex :male true]]
               (map (juxt :e :a :v :added) tx-data)))))))
