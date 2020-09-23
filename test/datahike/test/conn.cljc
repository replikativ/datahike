(ns datahike.test.conn
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.constants :as c]
   [datahike.test.core :as tdc]))

(def schema (merge c/old-implicit-schema
                   {:aka {:db/cardinality :db.cardinality/many}}))

;; TODO: Fix tests

(def datoms #{(d/datom tdc/e1 :age  17)
              (d/datom tdc/e1 :name "Ivan")})

#_(deftest test-ways-to-create-conn
  (let [conn (d/create-conn)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= c/old-implicit-schema (:schema @conn))))

  (let [conn (d/create-conn schema)]
    (is (= #{} (set (d/datoms @conn :eavt))))
    (is (= schema (:schema @conn))))

  (let [conn (d/conn-from-datoms datoms)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= c/old-implicit-schema (:schema @conn))))

  (let [conn (d/conn-from-datoms datoms schema)]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= schema (:schema @conn))))

  (let [conn (d/conn-from-db (d/init-db datoms))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= c/old-implicit-schema (:schema @conn))))

  (let [conn (d/conn-from-db (d/init-db datoms schema))]
    (is (= datoms (set (d/datoms @conn :eavt))))
    (is (= schema (:schema @conn)))))

#_(deftest test-reset-conn!
  (let [conn    (d/conn-from-datoms datoms schema)
        report  (atom nil)
        _       (d/listen! conn #(reset! report %))
        datoms' #{(d/datom 1 :age 20)
                  (d/datom 1 :sex :male)}
        schema' (merge c/old-implicit-schema {:email {:db/unique :db.unique/identity}})
        db'     (d/init-db datoms' schema')]
    (d/reset-conn! conn db' :meta)
    (is (= datoms' (set (d/datoms @conn :eavt))))
    (is (= schema' (:schema @conn)))

    (let [{:keys [db-before db-after tx-data tx-meta]} @report]
      (is (= datoms  (set (d/datoms db-before :eavt))))
      (is (= schema  (:schema db-before)))
      (is (= datoms' (set (d/datoms db-after :eavt))))
      (is (= schema' (:schema db-after)))
      (is (= :meta   tx-meta))
      (is (= [[tdc/e1 :age  17     false]
              [tdc/e1 :name "Ivan" false]
              [tdc/e1 :age  20     true]
              [tdc/e1 :sex  :male  true]]
             (map (juxt :e :a :v :added) tx-data))))))
