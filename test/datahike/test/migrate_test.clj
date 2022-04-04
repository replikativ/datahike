(ns datahike.test.migrate-test
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datahike.datom :as datom]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]))

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
    (doseq [hist [true false]
            attr-ref [true false]]
      (let [os (System/getProperty "os.name")
            path (case os
                   "Windows 10" (str (System/getProperty "java.io.tmpdir") "export-db1")
                   "/tmp/export-db1")
            cfg {:store {:backend :file
                         :path path}
                 :schema-flexibility :write
                 :keep-history? hist
                 :attribute-refs? attr-ref}
            schema [{:db/ident       :name
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
            os-prefix (case os "Windows 10" (System/getProperty "java.io.tmpdir") "/tmp/")
            export-path (str os-prefix "eavt-dump")
            import-path (str os-prefix "reimport")
            import-cfg (assoc-in cfg [:store :path] import-path)
            conn (utils/setup-db cfg)
            new-conn (utils/setup-db import-cfg)]
        (d/transact conn schema)
        (d/transact conn {:tx-data [{:name "Alice" :age  25}
                                    {:name "Bob" :age  35}
                                    {:name    "Charlie"
                                     :age     5
                                     :parents [[:name "Alice"] [:name "Bob"]]}
                                    {:name "Daisy" :age 20}
                                    {:name "Erhard" :age 20}]})
        (d/transact conn {:tx-data [[:db/retractEntity [:name "Erhard"]]]})
        (m/export-db conn export-path)
        (m/import-db new-conn export-path)
        (is (= (d/datoms (if (:keep-history? cfg) (d/history @conn) @conn) :eavt)
               (filter #(< (:e %) (:max-tx @new-conn))
                       (d/datoms (if (:keep-history? cfg) (d/history @new-conn) @new-conn) :eavt))))
        (d/delete-database cfg)))))

(defn load-entities-test [cfg]
  (testing "Test migrate simple datoms without attribute refs"
    (let [source-datoms (->> tx-data
                             (mapv #(-> % rest vec))
                             (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))
          cfg           (assoc cfg :attribute-refs false)
          conn (utils/setup-db cfg)]
      @(d/load-entities conn source-datoms)
      (is (= (into #{} source-datoms)
             (d/q '[:find ?e ?a ?v ?t ?op :where [?e ?a ?v ?t ?op]] @conn)))))
  (testing "Test migrate simple datoms with attribute refs"
    (let [source-datoms (->> tx-data
                             (mapv #(-> % rest vec))
                             (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))
          cfg           (assoc cfg
                               :attribute-refs? true
                               :schema-flexibility :write)
          conn (utils/setup-db cfg)]
      @(d/load-entities conn source-datoms)
      (is (= (into #{} (->> source-datoms
                            (mapv (comp vec rest))
                            (concat tx-meta)))
             (d/q '[:find ?a ?v ?t ?op
                    :where
                    [?e ?attr ?v ?t ?op]
                    [?attr :db/ident ?a]] @conn))))))

(deftest load-entities-test-hht
  (load-entities-test {:store         {:backend :mem
                                       :id      "load-entities-test-no-attr-refs"}
                       :keep-history? true}))

(deftest load-entities-test-ps
  (load-entities-test {:store         {:backend :mem
                                       :id      "load-entities-test-no-attr-refs"}
                       :index :datahike.index/persistent-set
                       :keep-history? true}))

(defn load-entities-history-test [source-cfg]
  (testing "Migrate predefined set with historical data"
    (let [schema      [{:db/ident       :name
                        :db/cardinality :db.cardinality/one
                        :db/index       true
                        :db/unique      :db.unique/identity
                        :db/valueType   :db.type/string}]
          source-conn (utils/setup-db source-cfg)
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
          target-conn (utils/setup-db target-cfg)
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

(deftest load-entities-history-test-hht
  (load-entities-history-test {:store              {:backend :mem
                                                    :id      "load-entities-history-test-source"}
                               :name               "load-entities-history-test-source"
                               :keep-history?      true
                               :schema-flexibility :write
                               :cache-size         0
                               :attribute-refs?    false}))

(deftest load-entities-history-test-ps
  (load-entities-history-test {:store              {:backend :mem
                                                    :id      "load-entities-history-test-source"}
                               :name               "load-entities-history-test-source"
                               :keep-history?      true
                               :schema-flexibility :write
                               :cache-size         0
                               :index :datahike.index/persistent-set
                               :attribute-refs?    false}))

(deftest test-binary-support
  (doseq [index [:datahike.index/persistent-set :datahike.index/hitchhiker-tree]]
    (let [config {:store {:backend :mem
                          :id "test-export-binary-support"}
                  :schema-flexibility :read
                  :keep-history? false
                  :index index}
          export-path "/tmp/test-export-binary-support"
          conn (utils/setup-db config)
          import-conn (utils/setup-db)]
      (d/transact conn [{:db/id 1, :name "Jiayi", :payload (byte-array [0 2 3])}
                        {:db/id 2, :name "Peter", :payload (byte-array [1 2 3])}])
      (m/export-db conn export-path)
      (m/import-db import-conn export-path)
      (is (utils/all-true? (map #(or (= %1 %2) (utils/all-eq? (nth %1 2) (nth %2 2)))
                                (d/datoms @conn :eavt)
                                (filter #(< (datom/datom-tx %) (:max-tx @import-conn))
                                        (d/datoms @import-conn :eavt))))))))
