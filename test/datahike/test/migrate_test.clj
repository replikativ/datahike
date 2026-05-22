(ns datahike.test.migrate-test
  (:require [clojure.test :refer :all]
            [datahike.api :as d]
            [datahike.datom :as datom]
            [datahike.migrate :as m]
            [datahike.db.utils :as dbu]
            [datahike.test.utils :as utils])
  (:import [java.util UUID]))

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
  (let [os-prefix (case (System/getProperty "os.name")
                    "Windows 10" (System/getProperty "java.io.tmpdir")
                    "/tmp/")
        old-path (str os-prefix "old-db")
        new-path (str os-prefix "new-db")
        export-path (str os-prefix "eavt-dump")
        base-config {:store {:backend :file}
                     :schema-flexibility :write
                     :keep-history? false
                     :attribute-refs? false}
        add-uuid (fn [cfg] (assoc-in cfg [:store :id] (java.util.UUID/randomUUID)))
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
        tx-data {:tx-data [{:name "Alice" :age  25}
                           {:name "Bob" :age  35}
                           {:name    "Charlie"
                            :age     5
                            :parents [[:name "Alice"] [:name "Bob"]]}
                           {:name "Daisy" :age 20}
                           {:name "Erhard" :age 20}]}]

    (testing "Same configuration roundtrip for exporting and importing."
      (doseq [hist [true false]
              attr-ref [true false]]
        (let [cfg (merge base-config
                         {:keep-history? hist
                          :attribute-refs? attr-ref})
              old-cfg (add-uuid (assoc-in cfg [:store :path] (str old-path (utils/get-time))))
              old-conn (utils/setup-db old-cfg)
              new-cfg (add-uuid (assoc-in cfg [:store :path] (str new-path (utils/get-time))))
              new-conn (utils/setup-db new-cfg)]
          (d/transact old-conn schema)
          (d/transact old-conn tx-data)
          (d/transact old-conn {:tx-data [[:db/retractEntity [:name "Erhard"]]]})
          (m/export-db old-conn export-path)
          (m/import-db new-conn export-path)
          (is (= (d/datoms (if (:keep-history? cfg) (d/history @old-conn) @old-conn) :eavt)
                 (filter #(< (:e %) (:max-tx @new-conn))
                         (d/datoms (if (:keep-history? cfg) (d/history @new-conn) @new-conn) :eavt))))
          (d/release old-conn)
          (d/release new-conn)
          (d/delete-database old-cfg)
          (d/delete-database new-cfg))))

    (testing "Export history database, import non-history database"
      (let [old-cfg (-> base-config
                        (assoc-in [:store :path] (str old-path (utils/get-time)))
                        (assoc :keep-history? true)
                        add-uuid)
            old-conn (utils/setup-db old-cfg)
            new-cfg (-> base-config
                        (assoc-in [:store :path] (str new-path (utils/get-time)))
                        (assoc :keep-history? false)
                        add-uuid)
            new-conn (utils/setup-db new-cfg)]
        (d/transact old-conn schema)
        (d/transact old-conn tx-data)
        (m/export-db old-conn export-path)
        (m/import-db new-conn export-path)
        (is (= (d/datoms @old-conn :eavt)
               (d/datoms @new-conn :eavt)))
        (d/release old-conn)
        (d/release new-conn)
        (d/delete-database old-cfg)
        (d/delete-database new-cfg)))

    (testing "Export non-history database, import history database"
      (let [old-cfg (-> base-config
                        (assoc-in [:store :path] (str old-path (utils/get-time)))
                        (assoc :keep-history? false)
                        add-uuid)
            old-conn (utils/setup-db old-cfg)
            new-cfg (-> base-config
                        (assoc-in [:store :path] (str new-path (utils/get-time)))
                        (assoc :keep-history? true)
                        add-uuid)
            new-conn (utils/setup-db new-cfg)]
        (d/transact old-conn schema)
        (d/transact old-conn tx-data)
        (m/export-db old-conn export-path)
        (m/import-db new-conn export-path)
        (is (= (d/datoms @old-conn :eavt)
               (filter #(< (:e %) (:max-tx @new-conn))
                       (d/datoms @new-conn :eavt))))
        (d/release old-conn)
        (d/release new-conn)
        (d/delete-database old-cfg)
        (d/delete-database new-cfg)))))

(deftest load-entities-test
  (testing "Test migrate simple datoms without attribute refs"
    (let [source-datoms (->> tx-data
                             (mapv #(-> % rest vec))
                             (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))
          cfg           {:store         {:backend :memory
                                         :id      #uuid "001d0000-0000-0000-0000-00000000001d"}
                         :keep-history? true
                         :attribute-refs false}
          conn (utils/setup-db cfg)]
      @(d/load-entities conn source-datoms)
      (is (= (into #{} source-datoms)
             (d/q '[:find ?e ?a ?v ?t ?op :where [?e ?a ?v ?t ?op]] @conn)))
      (d/release conn)))
  (testing "Test migrate simple datoms with attribute refs"
    (let [source-datoms (->> tx-data
                             (mapv #(-> % rest vec))
                             (concat [[536870913 :db/txInstant #inst "2020-03-11T14:54:27.979-00:00" 536870913 true]]))
          cfg           {:store         {:backend :memory
                                         :id      #uuid "001e0000-0000-0000-0000-00000000001e"}
                         :keep-history? true
                         :attribute-refs? true
                         :schema-flexibility :write}
          conn (utils/setup-db cfg)]
      @(d/load-entities conn source-datoms)
      (is (= (into #{} (->> source-datoms
                            (mapv (comp vec rest))
                            (concat tx-meta)))
             (d/q '[:find ?a ?v ?t ?op
                    :where
                    [?e ?attr ?v ?t ?op]
                    [?attr :db/ident ?a]] @conn)))
      (d/release conn))))

(deftest load-entities-history-test
  (testing "Migrate predefined set with historical data"
    (let [source-cfg {:store              {:backend :memory
                                           :id      #uuid "001f0000-0000-0000-0000-00000000001f"}
                      :name               "load-entities-history-test-source"
                      :keep-history?      true
                      :schema-flexibility :write
                      :search-cache-size  0
                      :store-cache-size   1
                      :attribute-refs?    false}
          schema      [{:db/ident       :name
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
          datoms-to-export (d/datoms (d/history @source-conn) :eavt)

          ;; The datoms to export must primarily
          ;; be sorted by transaction entity id
          ;; and secondarily so that datoms with attribute `:db/txInstant`
          ;; come before other datoms
          export-data (->> datoms-to-export
                           (map (comp vec seq))
                           (sort-by (fn [[e a v tx]]
                                      [tx

                                       ;; TODO: It seems as if :db/txInstant
                                       ;; datoms must come first. Is this a bug
                                       ;; in load-entities?
                                       (case a
                                         :db/txInstant 0
                                         1)]))
                           (into []))
          target-cfg  (-> source-cfg
                          (assoc-in [:store :id] #uuid "00200000-0000-0000-0000-000000000020")
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
      (is (dbu/distinct-sorted-datoms? :eavt datoms-to-export))
      (is (= (current-q source-conn)
             (current-q target-conn)))
      (is (= (history-q source-conn)
             (history-q target-conn)))
      (d/release source-conn)
      (d/release target-conn))))

(deftest test-binary-support
  (let [config {:store {:backend :memory
                        :id #uuid "00210000-0000-0000-0000-000000000021"}
                :schema-flexibility :read
                :keep-history? false}
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
                                      (d/datoms @import-conn :eavt)))))
    (d/release conn)
    (d/release import-conn)))

(def taxonomy-data0
  [[76 :db/ident :relation/id 13194139533318 true]
   [77 :db/ident :concept/id 13194139533318 true]
   [77 :db/valueType :db.type/string 13194139533318 true]
   [77 :db/unique :db.unique/identity 13194139533318 true]
   [112 :db/valueType :db.type/string 13194139533318 true]
   [113 :db/ident :relation/concept-1 13194139533318 true]
   [113 :db/valueType :db.type/ref 13194139533318 true]
   [114 :db/ident :relation/concept-2 13194139533318 true]
   [114 :db/valueType :db.type/ref 13194139533318 true]
   [124 :db/ident :webhook/url 13194139533318 true]
   [118747255884125
    :relation/concept-1
    92358976749187
    13194139533332
    true]
   [129 :db/valueType :db.type/string 13194139540571 true]
   [149533581525767 :concept/id "qLqq_iDq_Ua4" 13194139543922 true]
   [149533581525768
    :relation/id
    "qLqq_iDq_Ua4:broad-match:haMy_QRx_7vg"
    13194139543923
    true]
   [149533581525768
    :relation/concept-1
    149533581525767
    13194139543923
    true]
   [149533581525788
    :relation/id
    "2x2V_UeL_6ke:related:SpRR_JX9_6o6"
    13194139543944
    true]
   [149533581525788
    :relation/concept-1
    136339441992474
    13194139543944
    true]
   [149533581525788
    :relation/concept-2
    92358976749187
    13194139543944
    true]
   [149533581525788
    :relation/concept-1
    92358976749187
    13194139543945
    true]
   [149533581525768
    :relation/concept-1
    149533581525767
    13194139624849
    false]
   [193514046695117
    :relation/id
    "qLqq_iDq_Ua4:broad-match:haMy_QRx_7vg"
    13194139624850
    true]
   [193514046695117
    :relation/concept-1
    149533581525767
    13194139624850
    true]])

(deftest test-load-taxonomy-data0
  (let [cfg {:attribute-refs? true
             :index :datahike.index/persistent-set
             :keep-history true
             :name (str "jobtech-taxonomy-db" (rand-int 9999999))
             :schema-flexibility :write
             :store {:backend :memory
                     :id (UUID/randomUUID)}}]
    (try
      (d/create-database cfg)
      (let [conn (d/connect cfg)
            _ @(d/load-entities conn taxonomy-data0)
            db (d/db conn)
            raw-eavt-datoms (d/datoms db {:index :eavt :limit -1})

            ident (some (fn [[e a v]]
                          (when (and (= e a) (= :db/ident v)) e))
                        raw-eavt-datoms)
            eid->attrib (into {}
                              (keep (fn [[e a v]]
                                      (when (= a ident)
                                        [e v])))
                              raw-eavt-datoms)
            interesting-attrib? #(not= "db" (namespace %))
            map-attribs (fn [raw-datoms]
                          (into #{}
                                (keep
                                 (fn [[e a v tx added]]
                                   (let [attrib (eid->attrib a)]
                                     (when (interesting-attrib? attrib)
                                       [e attrib v tx added]))))
                                raw-datoms))
            raw-avet-datoms (d/datoms db {:index :avet :limit -1})
            eavt-datoms (map-attribs raw-eavt-datoms)
            avet-datoms (map-attribs raw-avet-datoms)

            extract-test-sample
            (fn [datoms]
              (let [[concept-eid] (keep
                                   (fn [[e a v]]
                                     (when (= [:concept/id "qLqq_iDq_Ua4"]
                                              [a v])
                                       e))
                                   datoms)]
                (into #{}
                      (filter (fn [[_ a v _ added]]
                                (= [:relation/concept-1 concept-eid true]
                                   [a v added])))
                      datoms)))

            ;; Just pick out the datom(s) that we are
            ;; interested in.
            eavt-sample (extract-test-sample eavt-datoms)
            avet-sample (extract-test-sample avet-datoms)]

        ;; The equality (= eavt-datoms avet-datoms) does not
        ;; seem to hold. I am not sure whether this is generally
        ;; expected, or not.

        (is (= 1 (count eavt-sample)))
        (is (= eavt-sample avet-sample))

        (is (= (into #{}
                     (map (fn [[e _ v]] [e v]))
                     eavt-sample)
               (into #{}
                     (d/q '[:find ?r ?c
                            :in $ ?concept-id
                            :where
                            [?c :concept/id ?concept-id]
                            [?r :relation/concept-1 ?c]]
                          db
                          "qLqq_iDq_Ua4")))))
      (finally
        (d/delete-database cfg)))))

