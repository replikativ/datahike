(ns datahike.test.upsert
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   [datahike.core :as d]
   [datahike.api :as api]
   [datahike.db :as db]
   [datahike.test.core :as tdc])
  (:import [java.util UUID]))

#?(:cljs
   (def Throwable js/Error))

(deftest test-upsert
  (let [db (d/db-with (d/empty-db {:name  {:db/unique :db.unique/identity}
                                   :email {:db/unique :db.unique/identity}})
                      [{:db/id 1 :name "Ivan" :email "@1"}
                       {:db/id 2 :name "Petr" :email "@2"}])
        touched (fn [tx e] (into {} (d/touch (d/entity (:db-after tx) e))))
        tempids (fn [tx] (dissoc (:tempids tx) :db/current-tx))]
    (testing "upsert, no tempid"
      (let [tx (d/with db [{:name "Ivan" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 attrs, no tempid"
      (let [tx (d/with db [{:name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert with tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {-1 1}))))

    (testing "upsert with string tempid"
      (let [tx (d/with db [{:db/id "1" :name "Ivan" :age 35}
                           [:db/add "2" :name "Oleg"]
                           [:db/add "2" :email "@2"]])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (touched tx 2)
               {:name "Oleg" :email "@2"}))
        (is (= (tempids tx)
               {"1" 1
                "2" 2}))))

    (testing "upsert by 2 attrs with tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {-1 1}))))

    (testing "upsert to two entities, resolve to same tempid"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}
                           {:db/id -1 :name "Ivan" :age 36}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 36}))
        (is (= (tempids tx)
               {-1 1}))))

    (testing "upsert to two entities, two tempids"
      (let [tx (d/with db [{:db/id -1 :name "Ivan" :age 35}
                           {:db/id -2 :name "Ivan" :age 36}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 36}))
        (is (= (tempids tx)
               {-1 1, -2 1}))))

    (testing "upsert with existing id"
      (let [tx (d/with db [{:db/id 1 :name "Ivan" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 attrs with existing id"
      (let [tx (d/with db [{:db/id 1 :name "Ivan" :email "@1" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@1" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert conficts with existing id"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id 2"
                            (d/with db [{:db/id 2 :name "Ivan" :age 36}]))))

    (testing "upsert conficts with non-existing id"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id 3"
                            (d/with db [{:db/id 3 :name "Ivan" :age 36}]))))

    (testing "upsert by non-existing value resolves as update"
      (let [tx (d/with db [{:name "Ivan" :email "@3" :age 35}])]
        (is (= (touched tx 1)
               {:name "Ivan" :email "@3" :age 35}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert by 2 conflicting fields"
      (is (thrown-with-msg? Throwable #"Conflicting upserts: \[:name \"Ivan\"\] resolves to 1, but \[:email \"@2\"\] resolves to 2"
                            (d/with db [{:name "Ivan" :email "@2" :age 35}]))))

    (testing "upsert over intermediate db"
      (let [tx (d/with db [{:name "Igor" :age 35}
                           {:name "Igor" :age 36}])]
        (is (= (touched tx 3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {}))))

    (testing "upsert over intermediate db, tempids"
      (let [tx (d/with db [{:db/id -1 :name "Igor" :age 35}
                           {:db/id -1 :name "Igor" :age 36}])]
        (is (= (touched tx 3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {-1 3}))))

    (testing "upsert over intermediate db, different tempids"
      (let [tx (d/with db [{:db/id -1 :name "Igor" :age 35}
                           {:db/id -2 :name "Igor" :age 36}])]
        (is (= (touched tx 3)
               {:name "Igor" :age 36}))
        (is (= (tempids tx)
               {-1 3, -2 3}))))

    (testing "upsert and :current-tx conflict"
      (is (thrown-with-msg? Throwable #"Conflicting upsert: \[:name \"Ivan\"\] resolves to 1, but entity already has :db/id \d+"
                            (d/with db [{:db/id :db/current-tx :name "Ivan" :age 35}]))))))

(deftest test-redefining-ids
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [{:db/id -1 :name "Ivan"}]))]
    (let [tx (d/with db [{:db/id -1 :age 35}
                         {:db/id -1 :name "Ivan" :age 36}])]
      (is (= #{[1 :age 36] [1 :name "Ivan"]}
             (tdc/all-datoms (:db-after tx))))
      (is (= {-1 1, :db/current-tx (+ d/tx0 2)}
             (:tempids tx)))))

  (let [db (-> (d/empty-db {:name  {:db/unique :db.unique/identity}})
               (d/db-with [{:db/id -1 :name "Ivan"}
                           {:db/id -2 :name "Oleg"}]))]
    (is (thrown-with-msg? Throwable #"Conflicting upsert: -1 resolves both to 1 and 2"
                          (d/with db [{:db/id -1 :name "Ivan" :age 35}
                                      {:db/id -1 :name "Oleg" :age 36}])))))

;; https://github.com/tonsky/datahike/issues/285
(deftest test-retries-order
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [[:db/add -1 :age 42]
                           [:db/add -2 :likes "Pizza"]
                           [:db/add -1 :name "Bob"]
                           [:db/add -2 :name "Bob"]]))]
    (is (= {:db/id 1, :name "Bob", :likes "Pizza", :age 42}
           (tdc/entity-map db 1))))

  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [[:db/add -1 :age 42]
                           [:db/add -2 :likes "Pizza"]
                           [:db/add -2 :name "Bob"]
                           [:db/add -1 :name "Bob"]]))]
    (is (= {:db/id 2, :name "Bob", :likes "Pizza", :age 42}
           (tdc/entity-map db 2)))))

(deftest test-vector-upsert
  (let [db (-> (d/empty-db {:name {:db/unique :db.unique/identity}})
               (d/db-with [{:db/id -1, :name "Ivan"}]))]
    (are [tx res] (= res (tdc/all-datoms (d/db-with db tx)))
      [[:db/add -1 :name "Ivan"]
       [:db/add -1 :age 12]]
      #{[1 :age 12] [1 :name "Ivan"]}

      [[:db/add -1 :age 12]
       [:db/add -1 :name "Ivan"]]
      #{[1 :age 12] [1 :name "Ivan"]}))

  (let [db (-> (d/empty-db {:name  {:db/unique :db.unique/identity}})
               (d/db-with [[:db/add -1 :name "Ivan"]
                           [:db/add -2 :name "Oleg"]]))]
    (is (thrown-with-msg? Throwable #"Conflicting upsert: -1 resolves both to 1 and 2"
                          (d/with db [[:db/add -1 :name "Ivan"]
                                      [:db/add -1 :age 35]
                                      [:db/add -1 :name "Oleg"]
                                      [:db/add -1 :age 36]])))))

(defn temporal-history-test [cfg]
  (let [schema [{:db/ident       :name
                 :db/cardinality :db.cardinality/one
                 :db/index       true
                 :db/unique      :db.unique/identity
                 :db/valueType   :db.type/string}
                {:db/ident       :age
                 :db/cardinality :db.cardinality/one
                 :db/valueType   :db.type/long}]
        _ (api/delete-database cfg)
        _ (api/create-database cfg)
        conn (api/connect cfg)]
    (testing "inserting a new datom creates an entry in history"
      (api/transact conn {:tx-data schema})
      (api/transact conn {:tx-data [{:name "Alice"
                                     :age  25}]})
      (is (= 1 (count (d/datoms (api/history @conn) :eavt [:name "Alice"] :age)))))

    (testing "inserting the exact same datom"
      (api/transact conn {:tx-data [{:db/id [:name "Alice"]
                                     :age 25}]})
      (testing " does not change the history for any index other than persistent-set"
        (when (not= (:index cfg) :datahike.index/persistent-set)
          (is (= 1 (count (api/datoms (api/history @conn) :eavt [:name "Alice"] :age))))))
      (testing " adds another version of the datom when index is persistent-set"
        (when (= (:index cfg) :datahike.index/persistent-set)
          (is (= 2 (count (api/datoms (api/history @conn) :eavt [:name "Alice"] :age)))))))
    (testing "changing the datom value increases the history with 2 datoms: the retraction datom and the new value."
      (api/transact conn {:tx-data [{:db/id [:name "Alice"]
                                     :age 26}]})
      (is (= 3 (count (api/datoms (api/history @conn) :eavt [:name "Alice"] :age)))))))

(deftest temporal-history-mem-hht
  (let [config {:store {:backend :mem :id "temp-hist-hht"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/hitchhiker-tree}]
    (temporal-history-test config)))

(deftest temporal-history-file
  (let [config {:store {:backend :file :path "/tmp/temp-hist-hht"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/hitchhiker-tree}]
    (temporal-history-test config)))

(deftest temporal-history-file-with-attr-refs
  (let [config {:store {:backend :file :path "/tmp/temp-hist-attr-refs"}
                :schema-flexibility :write
                :keep-history? true
                :attribute-refs? true
                :index :datahike.index/hitchhiker-tree}]
    (temporal-history-test config)))

(deftest temporal-history-mem-set
  (let [config {:store {:backend :mem :id "temp-hist-set"}
                :schema-flexibility :write
                :keep-history? true
                :index :datahike.index/persistent-set}]
    (temporal-history-test config)))

(deftest test-upsert-after-large-coll
  (let [ascii-ish (map char (concat (range 48 58) (range 65 91) (range 97 123)))
        file-cfg {:store {:backend :file
                          :path "/tmp/upsert-large-test"}}
        mem-cfg {:store {:backend :mem
                         :id "upsert-large-test"}}
        _ (if (api/database-exists? file-cfg)
            (do
              (api/delete-database file-cfg)
              (api/create-database file-cfg))
            (api/create-database file-cfg))
        _ (if (api/database-exists? mem-cfg)
            (do
              (api/delete-database mem-cfg)
              (api/create-database mem-cfg))
            (api/create-database mem-cfg))
        file-conn (api/connect file-cfg)
        mem-conn (api/connect mem-cfg)
        initial-active-count 8
        inactive-count 5
        space-taker-count 1000]
    (letfn [(ident-eid [db ident]
              (api/q '[:find ?e .
                       :in $ ?ident
                       :where [?e :db/ident ?ident]]
                     db ident))
            (random-uuid []
              (UUID/randomUUID))
            (random-char []
              (rand-nth ascii-ish))
            (random-string [length]
              (apply str (repeatedly length random-char)))
            (ent-ids [db]
              (api/q '[:find [?e ...]
                       :where
                       [?e :ent/id]]
                     db))
            (active-count [db]
              (api/q '[:find (count ?e) .
                       :where
                       [?e :ent/active? true]]
                     db))
            (init-data [conn]
              (api/transact conn {:tx-data [{:db/ident       :ent/id
                                             :db/valueType   :db.type/uuid
                                             :db/cardinality :db.cardinality/one
                                             :db/unique      :db.unique/identity
                                             :db/doc         "The entity ID."}
                                            {:db/ident       :ent/active?
                                             :db/valueType   :db.type/boolean
                                             :db/cardinality :db.cardinality/one
                                             :db/doc         "Whether the entity is active."}
                                            {:db/ident       :meta/space-taker
                                             :db/valueType   :db.type/string
                                             :db/cardinality :db.cardinality/one
                                             :db/doc         "Takes up some space in the db"}]})

              (api/transact conn {:tx-data (map (fn [_]
                                                  {:ent/id (random-uuid)})
                                                (range initial-active-count))})

              (api/transact conn {:tx-data (map (fn [_]
                                                  {:meta/space-taker (random-string 250)})
                                                (range space-taker-count))})

              (api/transact conn {:tx-data (map (fn [eid]
                                                  {:db/id      eid
                                                   :ent/active? true})
                                                (ent-ids @conn))})

              (api/transact conn {:tx-data (->> (ent-ids @conn)
                                                sort
                                                (take inactive-count)
                                                (map (fn [eid]
                                                       [:db/add eid :ent/active? false])))}))]
      (testing "File upsert"
        (init-data file-conn)
        (let [cached-db @file-conn
              fresh-db @(api/connect file-cfg)
              actual-count (- initial-active-count inactive-count)
              cached-count (active-count cached-db)
              fresh-count (active-count fresh-db)]
          (is (= actual-count cached-count))
          (is (= cached-count fresh-count))))
      (testing "Mem upsert"
        (init-data mem-conn)
        (let [cached-db @mem-conn
              fresh-db @(api/connect mem-cfg)
              actual-count (- initial-active-count inactive-count)
              cached-count (active-count cached-db)
              fresh-count (active-count fresh-db)]
          (is (= actual-count cached-count))
          (is (= cached-count fresh-count)))))))
