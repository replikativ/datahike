(ns datahike.test.tx-log
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.constants :as const]
   [datahike.test.utils :as u])
  (:import [clojure.lang ExceptionInfo]))

(def tx1 (inc const/tx0))

(def complete-result [{:tx 1
                       :data
                       [[1 :db/ident :name tx1 true]
                        [1 :db/cardinality :db.cardinality/one tx1 true]
                        [1 :db/index true tx1 true]
                        [1 :db/unique :db.unique/identity tx1 true]
                        [1 :db/valueType :db.type/string tx1 true]
                        [2 :db/ident :parents tx1 true]
                        [2 :db/cardinality :db.cardinality/many tx1 true]
                        [2 :db/valueType :db.type/ref tx1 true]
                        [3 :db/ident :age tx1 true]
                        [3 :db/cardinality :db.cardinality/one tx1 true]
                        [3 :db/valueType :db.type/long tx1 true]]}
                      {:tx 2
                       :data
                       [[4 :name "Alice" (+ tx1 1) true]
                        [4 :age 25 (+ tx1 1) true]
                        [5 :name "Bob" (+ tx1 1) true]
                        [5 :age 30 (+ tx1 1) true]]}
                      {:tx 3
                       :data
                       [[6 :name "Charlie" (+ tx1 2) true]
                        [6 :age 5 (+ tx1 2) true]
                        [6 :parents 4 (+ tx1 2) true]
                        [6 :parents 5 (+ tx1 2) true]]}
                      {:tx 4, :data [[4 :age 26 (+ tx1 3) true]]}
                      {:tx 5
                       :data
                       [[6 :age 5 (+ tx1 4) false]
                        [6 :name "Charlie" (+ tx1 4) false]
                        [6 :parents 4 (+ tx1 4) false]
                        [6 :parents 5 (+ tx1 4) false]]}])

(defn setup-data [conn]
  (let [schema [{:db/ident       :name
                 :db/cardinality :db.cardinality/one
                 :db/index       true
                 :db/unique      :db.unique/identity
                 :db/valueType   :db.type/string}
                {:db/ident       :parents
                 :db/cardinality :db.cardinality/many
                 :db/valueType   :db.type/ref}
                {:db/ident       :age
                 :db/cardinality :db.cardinality/one
                 :db/valueType   :db.type/long}]]
    (d/transact conn {:tx-data schema})

    (d/transact conn {:tx-data [{:name "Alice"
                                 :age  25}
                                {:name "Bob"
                                 :age 30}]})

    (d/transact conn {:tx-data [{:name    "Charlie"
                                 :age     5
                                 :parents [[:name "Alice"]
                                           [:name "Bob"]]}]})))

(deftest test-tx-log-without-attr-refs
  (let [default-cfg (u/with-default-cfg {:store {:backend :mem
                                                 :id "test-tx-range"}
                                         :keep-history? true
                                         :keep-log? true
                                         :attribute-refs? false
                                         :schema-flexibility :write})
        _ (do
            (d/delete-database default-cfg)
            (d/create-database default-cfg))
        conn (d/connect default-cfg)
        clean-tx (fn [tx] (->> tx
                               (remove #(= :db/txInstant (:a %)))
                               (mapv (comp vec seq))))
        cleanup-txs (fn [txs]
                      (mapv (fn [tx] (update tx :data clean-tx)) txs))]
    (setup-data conn)
    (testing "tx-range"
      (testing "after setup"
        (is (= (take 3 complete-result)
               (cleanup-txs (d/tx-range @conn)))))
      (testing "after new transactions"
        (d/transact conn {:tx-data [[:db/add [:name "Alice"] :age 26]]})
        (d/transact conn {:tx-data [[:db/retractEntity [:name "Charlie"]]]})
        (is (= complete-result
               (cleanup-txs (d/tx-range @conn)))))
      (testing "with offset 2 and limit 2"
        (is (= (->> complete-result
                    (drop 1)
                    (take 2))
               (cleanup-txs (d/tx-range @conn {:offset 2
                                               :limit 2})))))
      (testing "with limit beyond tx size"
        (is (= []
               (d/tx-range @conn {:offset 200}))))
      (testing "with negative offset"
        (is (thrown-with-msg? ExceptionInfo #"Only positive offsets allowed."
                              (d/tx-range @conn {:offset -2}))))
      (testing "with offset 3 and no limit"
        (is (= (drop 2 complete-result)
               (cleanup-txs (d/tx-range @conn {:offset 3})))))
      (testing "on database without tx log"
        (let [cfg {:store {:backend :mem
                           :id "test-tx-log-cfg-error"}
                   :keep-history? true
                   :keep-log? false
                   :attribute-refs? true
                   :schema-flexibility :write}
              _ (d/delete-database cfg)
              _ (d/create-database cfg)
              conn (d/connect cfg)
              _ (setup-data conn)]
          (is (thrown-with-msg? ExceptionInfo #"tx-range is only allowed for databases with transaction log. Consider setting \:keep-log\? to true in your configuration."
                                (d/tx-range @conn))))))
    (testing "tx-get"
      (testing "with first transaction"
        (is (= (-> complete-result first :data)
               (-> @conn
                   (d/get-tx tx1)
                   clean-tx))))
      (testing "with last transaction"
        (is (= (-> complete-result last :data)
               (-> @conn
                   (d/get-tx (+ tx1 4))
                   clean-tx)))))

    (testing "purge"
      (let [dirk-entity {:name "Dirk"
                         :age 3
                         :parents [[:name "Alice"] [:name "Bob"]]}
            dirk-datoms [[7 :name "Dirk" 536870918 true]
                         [7 :age 3 536870918 true]
                         [7 :parents 4 536870918 true]
                         [7 :parents 5 536870918 true]]]
        (d/transact conn {:tx-data [dirk-entity]})
        (is (= dirk-datoms
               (-> @conn
                   (d/get-tx (+ tx1 5))
                   clean-tx)))

        (d/transact conn [[:db/purge [:name "Dirk"] :age 2]])
        (is (= dirk-datoms
               (-> @conn
                   (d/get-tx (+ tx1 5))
                   clean-tx)))

        (d/transact conn [[:db/purge [:name "Dirk"] :age 3]])
        (is (= [[7 :name "Dirk" 536870918 true]
                [7 :parents 4 536870918 true]
                [7 :parents 5 536870918 true]]
               (-> @conn
                   (d/get-tx (+ tx1 5))
                   clean-tx)))

        (testing "attribute"
          (d/transact conn [[:db.purge/attribute [:name "Dirk"] :parents]])
          (is (= [[7 :name "Dirk" 536870918 true]]
                 (-> @conn
                     (d/get-tx (+ tx1 5))
                     clean-tx))))

        (testing "entity"
          (d/transact conn [[:db.purge/entity [:name "Dirk"]]])
          (is (= []
                 (-> @conn
                     (d/get-tx (+ tx1 5))
                     clean-tx))))))

    (d/delete-database default-cfg)))

(def attr-ref-result [{:tx 0,
                       :data
                       [[1 :db/ident :db/ident const/tx0 true]
                        [1 :db/valueType :db.type/keyword const/tx0 true]
                        [1 :db/cardinality :db.cardinality/one const/tx0 true]
                        [1
                         :db/doc
                         "An attribute's or specification's identifier"
                         const/tx0
                         true]
                        [1 :db/unique :db.unique/value const/tx0 true]
                        [2 :db/ident :db/valueType const/tx0 true]
                        [2 :db/valueType :db.type/valueType const/tx0 true]
                        [2 :db/cardinality :db.cardinality/one const/tx0 true]
                        [2 :db/doc "An attribute's value type" const/tx0 true]
                        [3 :db/ident :db/cardinality const/tx0 true]
                        [3 :db/valueType :db.type/cardinality const/tx0 true]
                        [3 :db/cardinality :db.cardinality/one const/tx0 true]
                        [3 :db/doc "An attribute's cardinality" const/tx0 true]
                        [4 :db/ident :db/doc const/tx0 true]
                        [4 :db/valueType :db.type/string const/tx0 true]
                        [4 :db/cardinality :db.cardinality/one const/tx0 true]
                        [4 :db/doc "An attribute's documentation" const/tx0 true]
                        [5 :db/ident :db/index const/tx0 true]
                        [5 :db/valueType :db.type/boolean const/tx0 true]
                        [5 :db/cardinality :db.cardinality/one const/tx0 true]
                        [5 :db/doc "An attribute's index selection" const/tx0 true]
                        [6 :db/ident :db/unique const/tx0 true]
                        [6 :db/valueType :db.type/unique const/tx0 true]
                        [6 :db/cardinality :db.cardinality/one const/tx0 true]
                        [6 :db/doc "An attribute's unique selection" const/tx0 true]
                        [7 :db/ident :db/noHistory const/tx0 true]
                        [7 :db/valueType :db.type/boolean const/tx0 true]
                        [7 :db/cardinality :db.cardinality/one const/tx0 true]
                        [7 :db/doc "An attribute's history selection" const/tx0 true]
                        [8 :db/ident :db.install/attribute const/tx0 true]
                        [8 :db/valueType :db.type.install/attribute const/tx0 true]
                        [8 :db/cardinality :db.cardinality/one const/tx0 true]
                        [8 :db/doc "Only for interoperability with Datomic" const/tx0 true]
                        [9 :db/ident :db/txInstant const/tx0 true]
                        [9 :db/valueType :db.type/instant const/tx0 true]
                        [9 :db/cardinality :db.cardinality/one const/tx0 true]
                        [9 :db/doc "A transaction's time-point" const/tx0 true]
                        [9 :db/noHistory true const/tx0 true]
                        [9 :db/index true const/tx0 true]
                        [10 :db/ident :db.cardinality/many const/tx0 true]
                        [11 :db/ident :db.cardinality/one const/tx0 true]
                        [12 :db/ident :db.part/sys const/tx0 true]
                        [13 :db/ident :db.part/tx const/tx0 true]
                        [14 :db/ident :db.part/user const/tx0 true]
                        [15 :db/ident :db.type/bigdec const/tx0 true]
                        [16 :db/ident :db.type/bigint const/tx0 true]
                        [17 :db/ident :db.type/boolean const/tx0 true]
                        [18 :db/ident :db.type/double const/tx0 true]
                        [19 :db/ident :db.type/cardinality const/tx0 true]
                        [20 :db/ident :db.type/float const/tx0 true]
                        [21 :db/ident :db.type/number const/tx0 true]
                        [22 :db/ident :db.type/instant const/tx0 true]
                        [23 :db/ident :db.type/keyword const/tx0 true]
                        [24 :db/ident :db.type/long const/tx0 true]
                        [25 :db/ident :db.type/ref const/tx0 true]
                        [26 :db/ident :db.type/string const/tx0 true]
                        [27 :db/ident :db.type/symbol const/tx0 true]
                        [28 :db/ident :db.type/unique const/tx0 true]
                        [29 :db/ident :db.type/uuid const/tx0 true]
                        [30 :db/ident :db.type/valueType const/tx0 true]
                        [31 :db/ident :db.type.install/attribute const/tx0 true]
                        [32 :db/ident :db.unique/identity const/tx0 true]
                        [33 :db/ident :db.unique/value const/tx0 true]
                        [34 :db/ident :db/isComponent const/tx0 true]
                        [35 :db/ident :db/tupleType const/tx0 true]
                        [36 :db/ident :db/tupleTypes const/tx0 true]
                        [37 :db/ident :db/tupleAttrs const/tx0 true]
                        [38 :db/ident :db.type/tuple const/tx0 true]]}
                      {:tx 1,
                       :data
                       [[39 :db/ident :name (inc const/tx0) true]
                        [39 :db/cardinality :db.cardinality/one (inc const/tx0) true]
                        [39 :db/index true (inc const/tx0) true]
                        [39 :db/unique :db.unique/identity (inc const/tx0) true]
                        [39 :db/valueType :db.type/string (inc const/tx0) true]
                        [40 :db/ident :parents (inc const/tx0) true]
                        [40 :db/cardinality :db.cardinality/many (inc const/tx0) true]
                        [40 :db/valueType :db.type/ref (inc const/tx0) true]
                        [41 :db/ident :age (inc const/tx0) true]
                        [41 :db/cardinality :db.cardinality/one (inc const/tx0) true]
                        [41 :db/valueType :db.type/long (inc const/tx0) true]]}
                      {:tx 2,
                       :data
                       [[42 :name "Alice" (+ const/tx0 2) true]
                        [42 :age 25 (+ const/tx0 2) true]
                        [43 :name "Bob" (+ const/tx0 2) true]
                        [43 :age 30 (+ const/tx0 2) true]]}
                      {:tx 3,
                       :data
                       [[44 :name "Charlie" (+ const/tx0 3) true]
                        [44 :age 5 (+ const/tx0 3) true]
                        [44 :parents 42 (+ const/tx0 3) true]
                        [44 :parents 43 (+ const/tx0 3) true]]}
                      {:tx 4,
                       :data [[42 :age 26 (+ const/tx0 4) true]]}
                      {:tx 5,
                       :data
                       [[44 :name "Charlie" (+ const/tx0 5) false]
                        [44 :parents 42 (+ const/tx0 5) false]
                        [44 :parents 43 (+ const/tx0 5) false]
                        [44 :age 5 (+ const/tx0 5) false]]}])

(deftest test-tx-log-with-attr-refs
  (let [default-cfg (u/with-default-cfg {:store {:backend :mem
                                                 :id "test-tx-range-with-attr-refs"}
                                         :keep-history? true
                                         :keep-log? true
                                         :attribute-refs? true
                                         :schema-flexibility :write})
        _ (do
            (d/delete-database default-cfg)
            (d/create-database default-cfg))
        conn (d/connect default-cfg)
        clean-tx (fn [tx] (->> tx
                               (remove #(= :db/txInstant (:a %)))
                               (mapv (comp vec seq))))
        cleanup-txs (fn [txs]
                      (mapv (fn [tx] (update tx :data clean-tx)) txs))
        complete-result attr-ref-result]
    (setup-data conn)
    (testing "tx-range"
      (testing "after setup"
        (is (= (take 4 complete-result)
               (cleanup-txs (d/tx-range @conn)))))
      (testing "after new transactions"
        (d/transact conn {:tx-data [{:db/id [:name "Alice"] :age 26}]})
        (d/transact conn {:tx-data [[:db/retractEntity [:name "Charlie"]]]})
        (is (= complete-result
               (cleanup-txs (d/tx-range @conn)))))
      (testing "with offset 0 and limit 1"
        (is (= (->> complete-result
                    (take 1))
               (cleanup-txs (d/tx-range @conn {:offset 0
                                               :limit 1})))))
      (testing "with offset 2 and limit 2"
        (is (= (->> complete-result
                    (drop 2)
                    (take 2))
               (cleanup-txs (d/tx-range @conn {:offset 2
                                               :limit 2})))))
      (testing "with offset beyond tx size"
        (is (= []
               (d/tx-range @conn {:offset 200}))))
      (testing "with negative offset"
        (is (thrown-with-msg? ExceptionInfo #"Only positive offsets allowed."
                              (d/tx-range @conn {:offset -2}))))
      (testing "with offset 3 and no limit"
        (is (= (drop 3 complete-result)
               (cleanup-txs (d/tx-range @conn {:offset 3}))))))
    (testing "tx-get"
      (testing "with first transaction"
        (is (= (-> complete-result second :data)
               (-> @conn
                   (d/get-tx tx1)
                   clean-tx))))
      (testing "with last transaction"
        (is (= (-> complete-result last :data)
               (-> @conn
                   (d/get-tx (+ tx1 4))
                   clean-tx)))))
    (testing "purge"
      (let [dirk-entity {:name "Dirk"
                         :age 3
                         :parents [[:name "Alice"] [:name "Bob"]]}
            dirk-datoms [[45 :name "Dirk" 536870918 true]
                         [45 :age 3 536870918 true]
                         [45 :parents 42 536870918 true]
                         [45 :parents 43 536870918 true]]]
        (d/transact conn {:tx-data [dirk-entity]})
        (is (= dirk-datoms
               (-> @conn
                   (d/get-tx (+ tx1 5))
                   clean-tx)))

        (d/transact conn [[:db/purge [:name "Dirk"] 41 2]])
        (is (= dirk-datoms
               (-> @conn
                   (d/get-tx (+ tx1 5))
                   clean-tx)))

        (d/transact conn [[:db/purge [:name "Dirk"] 41 3]])
        (is (= [[45 :name "Dirk" 536870918 true]
                [45 :parents 42 536870918 true]
                [45 :parents 43 536870918 true]]
               (-> @conn
                   (d/get-tx (+ tx1 5))
                   clean-tx)))

        (testing "attribute"
          (d/transact conn [[:db.purge/attribute [:name "Dirk"] 40]])
          (is (= [[45 :name "Dirk" 536870918 true]]
                 (-> @conn
                     (d/get-tx (+ tx1 5))
                     clean-tx))))

        (testing "entity"
          (d/transact conn [[:db.purge/entity [:name "Dirk"]]])
          (is (= []
                 (-> @conn
                     (d/get-tx (+ tx1 5))
                     clean-tx))))))

    (d/delete-database default-cfg)))
