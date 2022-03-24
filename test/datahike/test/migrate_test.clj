(ns datahike.test.migrate-test
  (:require [clojure.test :refer :all]
            [datahike.migrate :as m]
            [datahike.api :as d]))

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

(def tx-meta [[:db/ident :db.install/attribute 536870912 true]
              [:db/doc "An attribute's cardinality" 536870912 true]
              [:db/valueType 28 536870912 true]
              [:db/ident :db.type/instant 536870912 true]
              [:db/ident :db.type/bigint 536870912 true]
              [:db/cardinality 11 536870912 true]
              [:db/ident :db/txInstant 536870912 true]
              [:db/ident :db/isComponent 536870912 true]
              [:db/ident :db/unique 536870912 true]
              [:db/ident :db/valueType 536870912 true]
              [:db/valueType 26 536870912 true]
              [:db/ident :db.type/valueType 536870912 true]
              [:db/ident :db.type/unique 536870912 true]
              [:db/ident :db.part/tx 536870912 true]
              [:db/ident :db/tupleAttrs 536870912 true]
              [:db/doc "An attribute's unique selection" 536870912 true]
              [:db/doc "An attribute's history selection" 536870912 true]
              [:db/txInstant #inst "1970-01-01T00:00:00.000-00:00" 536870912 true]
              [:db/ident :db.type/boolean 536870912 true]
              [:db/valueType 31 536870912 true]
              [:db/ident :db/noHistory 536870912 true]
              [:db/valueType 17 536870912 true]
              [:db/ident :db.type/float 536870912 true]
              [:db/ident :db.part/sys 536870912 true]
              [:db/ident :db/index 536870912 true]
              [:db/noHistory true 536870912 true]
              [:db/ident :db.part/user 536870912 true]
              [:db/ident :db.type/tuple 536870912 true]
              [:db/ident :db/doc 536870912 true]
              [:db/ident :db/tupleType 536870912 true]
              [:db/doc
               "An attribute's or specification's identifier"
               536870912
               true]
              [:db/ident :db.type/uuid 536870912 true]
              [:db/doc "Only for interoperability with Datomic" 536870912 true]
              [:db/ident :db.type/cardinality 536870912 true]
              [:db/valueType 30 536870912 true]
              [:db/ident :db/cardinality 536870912 true]
              [:db/ident :db.cardinality/one 536870912 true]
              [:db/ident :db.unique/identity 536870912 true]
              [:db/valueType 23 536870912 true]
              [:db/ident :db.type/number 536870912 true]
              [:db/unique 33 536870912 true] [:db/ident :db/ident 536870912 true]
              [:db/ident :db.type/ref 536870912 true]
              [:db/ident :db.type/bigdec 536870912 true]
              [:db/ident :db/tupleTypes 536870912 true]
              [:db/ident :db.type/symbol 536870912 true]
              [:db/valueType 22 536870912 true]
              [:db/doc "An attribute's index selection" 536870912 true]
              [:db/doc "A transaction's time-point" 536870912 true]
              [:db/index true 536870912 true]
              [:db/ident :db.type.install/attribute 536870912 true]
              [:db/ident :db.unique/value 536870912 true]
              [:db/valueType 19 536870912 true]
              [:db/ident :db.type/long 536870912 true]
              [:db/ident :db.cardinality/many 536870912 true]
              [:db/ident :db.type/string 536870912 true]
              [:db/ident :db.type/double 536870912 true]
              [:db/doc "An attribute's value type" 536870912 true]
              [:db/doc "An attribute's documentation" 536870912 true]
              [:db/ident :db.type/keyword 536870912 true]])

(deftest export-import-test
  (testing "Test a roundtrip for exporting and importing."
    (let [os (System/getProperty "os.name")
          path (case os
                 "Windows 10" (str (System/getProperty "java.io.tmpdir") "export-db1")
                 "/tmp/export-db1")
          cfg {:store {:backend :file
                       :path path}}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          export-path (case os
                        "Windows 10" (str (System/getProperty "java.io.tmpdir") "eavt-dump")
                        "/tmp/eavt-dump")
          result     (-> (d/transact conn tx-data)
                         :tx-data)
          tx-instant (-> (filter #(= :db/txInstant (second %)) result)
                         first
                         (nth 2))
          tx-string  (with-out-str (pr tx-instant))]

      (m/export-db @conn export-path)

      (is (= (slurp export-path)
             (case os
               "Windows 10"
               "#datahike/Datom [1 :db/cardinality :db.cardinality/one 536870913 true]\r\n#datahike/Datom [1 :db/ident :name 536870913 true]\r\n#datahike/Datom [1 :db/index true 536870913 true]\r\n#datahike/Datom [1 :db/unique :db.unique/identity 536870913 true]\r\n#datahike/Datom [1 :db/valueType :db.type/string 536870913 true]\r\n#datahike/Datom [2 :db/cardinality :db.cardinality/one 536870913 true]\r\n#datahike/Datom [2 :db/ident :age 536870913 true]\r\n#datahike/Datom [2 :db/valueType :db.type/long 536870913 true]\r\n#datahike/Datom [3 :age 25 536870913 true]\r\n#datahike/Datom [3 :name \"Alice\" 536870913 true]\r\n#datahike/Datom [4 :age 35 536870913 true]\r\n#datahike/Datom [4 :name \"Bob\" 536870913 true]\r\n#datahike/Datom [536870913 :db/txInstant " tx-string " 536870913 true]\r\n"
               (str "#datahike/Datom [1 :db/cardinality :db.cardinality/one 536870913 true]\n#datahike/Datom [1 :db/ident :name 536870913 true]\n#datahike/Datom [1 :db/index true 536870913 true]\n#datahike/Datom [1 :db/unique :db.unique/identity 536870913 true]\n#datahike/Datom [1 :db/valueType :db.type/string 536870913 true]\n#datahike/Datom [2 :db/cardinality :db.cardinality/one 536870913 true]\n#datahike/Datom [2 :db/ident :age 536870913 true]\n#datahike/Datom [2 :db/valueType :db.type/long 536870913 true]\n#datahike/Datom [3 :age 25 536870913 true]\n#datahike/Datom [3 :name \"Alice\" 536870913 true]\n#datahike/Datom [4 :age 35 536870913 true]\n#datahike/Datom [4 :name \"Bob\" 536870913 true]\n#datahike/Datom [536870913 :db/txInstant " tx-string " 536870913 true]\n"))))

      (let [import-path (case os
                          "Windows 10" (str (System/getProperty "java.io.tmpdir") "reimport")
                          "/tmp/reimport")
            import-cfg {:store {:backend :file
                                :path import-path}}
            _ (d/delete-database import-cfg)
            _ (d/create-database import-cfg)
            new-conn (d/connect import-cfg)]

        (m/import-db new-conn export-path)

        (is (= (d/q '[:find ?e
                      :where [?e :name]]
                    @new-conn)
               #{[3] [4]}))
        (d/delete-database cfg)))))

(deftest load-entities-test
  (let [default-cfg {:store         {:backend :mem
                                     :id      "load-entities-test-no-attr-refs"}
                     :keep-history? true}]
    (testing "Test migrate simple datoms without attribute refs"
      (let [source-datoms (->> tx-data
                               (mapv #(-> % rest vec))
                               (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))
            cfg           (assoc default-cfg :attribute-refs false)
            _             (d/delete-database cfg)
            _             (d/create-database cfg)
            conn          (d/connect cfg)]
        @(d/load-entities conn source-datoms)
        (is (= (into #{} source-datoms)
               (d/q '[:find ?e ?a ?v ?t ?op :where [?e ?a ?v ?t ?op]] @conn)))))
    (testing "Test migrate simple datoms with attribute refs"
      (let [source-datoms (->> tx-data
                               (mapv #(-> % rest vec))
                               (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))
            cfg           (assoc default-cfg :attribute-refs? true)
            _             (d/delete-database cfg)
            _             (d/create-database cfg)
            conn          (d/connect cfg)]
        @(d/load-entities conn source-datoms)
        (is (= (into #{} (->> source-datoms
                              (mapv (comp vec rest))
                              (concat tx-meta)))
               (d/q '[:find ?a ?v ?t ?op
                      :where
                      [?e ?attr ?v ?t ?op]
                      [?attr :db/ident ?a]] @conn)))))))

(deftest load-entities-history-test
  (testing "Migrate predefined set with historical data"
    (let [schema      [{:db/ident       :name
                        :db/cardinality :db.cardinality/one
                        :db/index       true
                        :db/unique      :db.unique/identity
                        :db/valueType   :db.type/string}]
          source-cfg  {:store              {:backend :mem
                                            :id      "load-entities-history-test-source"}
                       :name               "load-entities-history-test-source"
                       :keep-history?      true
                       :schema-flexibility :write
                       :cache-size         0
                       :attribute-refs?    false}
          source-conn (do
                        (d/delete-database source-cfg)
                        (d/create-database source-cfg)
                        (d/connect source-cfg))
          txs         [schema
                       [{:name "Alice"} {:name "Bob"}]
                       [{:name "Charlie"} {:name "Daisy"}]
                       [[:db/retractEntity [:name "Alice"]]]]
          _           (doseq [tx-data txs]
                        (d/transact source-conn {:tx-data tx-data}))
          export-data (->> (d/datoms (d/history @source-conn) :eavt)
                           (map (comp vec seq))
                           (sort-by #(nth % 3))
                           (into []))
          target-cfg  (-> source-cfg
                          (assoc-in [:store :id] "load-entities-history-test-target")
                          (assoc-in [:name] "load-entities-history-test-target"))
          target-conn (do
                        (d/delete-database target-cfg)
                        (d/create-database target-cfg)
                        (d/connect target-cfg))
          _           @(d/load-entities target-conn export-data)
          current-q   (fn [conn] (d/q
                                  '[:find ?n
                                    :where
                                    [?e :name ?n]]
                                  @conn))
          history-q   (fn [conn] (d/q '[:find ?n ?t ?op
                                        :where
                                        [?e :name ?n ?t ?op]]
                                      (d/history @conn)))]
      (is (= (current-q source-conn)
             (current-q target-conn)))
      (is (= (history-q source-conn)
             (history-q target-conn))))))
