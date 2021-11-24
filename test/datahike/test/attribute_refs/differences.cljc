(ns datahike.test.attribute-refs.differences
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj [clojure.test :as t :refer [is deftest testing]])
   [clojure.set :refer [difference]]
   [datahike.api :as d]
   [datahike.constants :as c]
   [datahike.db :as db :refer [ref-datoms]])
  #?(:clj (:import [datahike.datom Datom])))

(def no-ref-cfg
  {:store {:backend :mem :id "attr-no-refs-test.differences"}
   :keep-history? true
   :attribute-refs? false
   :schema-flexibility :write
   :name "attr-no-refs-test"})

(def ref-cfg
  {:store {:backend :mem :id "attr-refs-test.differences"}
   :keep-history? true
   :attribute-refs? true
   :schema-flexibility :write
   :name "attr-refs-test"})

(def name-schema [{:db/ident :name
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string}])

(defn setup-db [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))

(defn tx-instant [db]
  (if (get-in db [:config :attribute-refs?])
    (:db/txInstant (:ident-ref-map db))
    :db/txInstant))

(deftest test-empty-db
  (let [conn (setup-db no-ref-cfg)]
    (testing "Empty EAVT index for keyword DB"
      (is (= nil
             (d/datoms @conn :eavt))))
    (testing "Empty AEVT index for keyword DB"
      (is (= nil
             (d/datoms @conn :aevt))))
    (testing "Empty AVET index for keyword DB"
      (is (= nil
             (d/datoms @conn :avet)))))

  (let [conn (setup-db ref-cfg)]
    (testing "System-datoms in EAVT index for reference DB"
      (is (= (set ref-datoms)
             (set (d/datoms @conn :eavt)))))
    (testing "System-datoms in AEVT index for reference DB"
      (is (= (set ref-datoms)
             (set (d/datoms @conn :aevt)))))
    (testing "System ident and transaction datoms in AVET index for reference DB"
      (let [ref (:ident-ref-map @conn)
            indexed-refs #{(:db/ident ref) (:db/txInstant ref)}]
        (is (= (set (filter (fn [^Datom datom] (contains? indexed-refs (:a datom)))
                            ref-datoms))
               (set (d/datoms @conn :avet))))))))

(deftest test-last-entity-id                                ;; TODO: What is the behavior wanted?
  (let [find-last-entity-id (fn [db]
                              (->> (d/datoms db :eavt)
                                   (remove (fn [datom] (= (tx-instant db) (:a datom))))
                                   (map :e)
                                   (concat [-1])
                                   (apply max)))
        simple-schema [{:db/ident :name
                        :db/cardinality :db.cardinality/one
                        :db/valueType :db.type/string}]]
    (let [conn (setup-db no-ref-cfg)]
      (testing "Last entity id for empty keyword DB"
        (is (= c/e0 (:max-eid @conn)))
        (is (= -1 (find-last-entity-id @conn))))

      (testing "Last entity id for non-empty keyword DB"
        (d/transact conn simple-schema)
        (is (= (+ 1 c/e0) (:max-eid @conn)))
        (is (= (+ 1 c/e0) (find-last-entity-id @conn)))))

    (let [conn (setup-db ref-cfg)]
      (testing "Last entity id for empty reference DB"
        (is (= c/ue0 (:max-eid @conn)))
        (is (= c/ue0 (find-last-entity-id @conn))))

      (testing "Last entity id for non-empty reference DB"
        (d/transact conn simple-schema)
        (is (= (+ 1 c/ue0) (:max-eid @conn)))
        (is (= (+ 1 c/ue0) (find-last-entity-id @conn)))))))

(deftest test-transact-schema
  (testing "Schema for keyword DB"
    (let [conn (setup-db no-ref-cfg)]
      (is (= (:schema @conn)
             c/non-ref-implicit-schema))
      (d/transact conn name-schema)
      (is (= (:schema @conn)
             (merge c/non-ref-implicit-schema {:name (first name-schema)} {1 :name})))))
  (testing "Schema for reference DB"
    (let [conn (setup-db ref-cfg)]
      (is (= (:schema @conn)
             c/ref-implicit-schema))
      (d/transact conn name-schema)
      (is (= (:schema @conn)
             (merge c/ref-implicit-schema
                    {:name (first name-schema)}
                    {(+ 1 c/ue0) :name})))
      (is (contains? (-> (:rschema @conn) :db/ident) :name))
      (is (contains? (-> (:ident-ref-map @conn) keys set) :name))
      (is (contains? (-> (:ref-ident-map @conn) vals set) :name))

      (testing "after reconnection"
        (d/release conn)
        (let [conn2 (d/connect ref-cfg)]
          (is (= (:schema @conn2)
                 (merge c/ref-implicit-schema
                        {:name (first name-schema)}
                        {(+ 1 c/ue0) :name})))
          (is (contains? (-> (:rschema @conn2) :db/ident) :name))
          (is (contains? (-> (:ident-ref-map @conn2) keys set) :name))
          (is (contains? (-> (:ref-ident-map @conn2) vals set) :name)))))))

(deftest test-transact-tempid
  (testing "Tempid resolution for keyword DB"
    (let [conn (setup-db no-ref-cfg)]
      (is (= (:tempids (d/transact conn name-schema))
             {:db/current-tx (+ 1 c/tx0)}))
      (is (= (:tempids (d/transact conn [{:db/id -1 :name "Ivan"}]))
             {-1 (+ 2 c/e0), :db/current-tx (+ 2 c/tx0)}))
      (is (= (:tempids (d/transact conn [[:db/add -2 :name "Petr"]]))
             {-2 (+ 3 c/e0), :db/current-tx (+ 3 c/tx0)}))
      (is (= (:tempids (d/transact conn [{:db/id "Serg" :name "Sergey"}]))
             {"Serg" (+ 4 c/e0), :db/current-tx (+ 4 c/tx0)}))))

  (testing "Tempid resolution for reference DB"
    (let [conn (setup-db ref-cfg)]
      (is (= (:tempids (d/transact conn name-schema))
             {:db/current-tx (+ 1 c/tx0)}))
      (is (= (:tempids (d/transact conn [{:db/id -1 :name "Ivan"}]))
             {-1 (+ 2 c/ue0), :db/current-tx (+ 2 c/tx0)}))
      (is (= (:tempids (d/transact conn [[:db/add -2 (get-in @conn [:ident-ref-map :name]) "Petr"]]))
             {-2 (+ 3 c/ue0), :db/current-tx (+ 3 c/tx0)}))
      (is (= (:tempids (d/transact conn [{:db/id "Serg" :name "Sergey"}]))
             {"Serg" (+ 4 c/ue0), :db/current-tx (+ 4 c/utx0)})))))

(deftest test-system-attr-resolution
  (let [schema [{:db/ident :name
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/string}]
        keyword-attrs (fn [datoms] (->> datoms
                                        (filter (fn [datom] (keyword? (:a datom))))
                                        (map :a)
                                        set))]
    (testing "Do not resolve attributes in keyword DB"
      (let [conn (setup-db no-ref-cfg)
            tx-data (:tx-data (d/transact conn schema))]
        (is (= (keyword-attrs tx-data)
               #{:db/ident :db/cardinality :db/valueType :db/txInstant}))))

    (testing "Resolve attributes in reference DB"
      (let [conn (setup-db ref-cfg)
            tx-data (:tx-data (d/transact conn schema))]
        (is (= (keyword-attrs tx-data)
               #{}))))))

(deftest test-system-enum-resolution
  (let [schema [{:db/ident :name
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/string}]
        unresolved-enums (fn [datoms] (->> datoms
                                           (filter (fn [datom] (keyword? (:v datom))))
                                           (map :v)
                                           set))]
    (testing "Do not resolve enums in keyword DB"
      (let [conn (setup-db no-ref-cfg)
            tx-data (:tx-data (d/transact conn schema))]
        (is (= (unresolved-enums tx-data)
               #{:name :db.cardinality/one :db.type/string}))))

    (testing "Resolve enums in reference DB"
      (let [conn (setup-db ref-cfg)
            tx-data (:tx-data (d/transact conn schema))]
        (is (= (unresolved-enums tx-data)
               #{:name}))))))

(deftest test-indexing
  (let [schema [{:db/ident :name
                 :db/cardinality :db.cardinality/one
                 :db/index true
                 :db/valueType :db.type/string}
                {:db/ident :age
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/long}]
        tx1 [{:name "Alice"
              :age 10}]
        avet-a-v (fn [db] (->> (d/datoms db :avet)
                               (map (fn [datom] [(:a datom) (:v datom)]))
                               (remove (fn [[a _]] (= a (tx-instant db))))
                               set))]
    (testing "Entry in avet index only when indexing true for keyword DB"
      (let [conn (setup-db no-ref-cfg)]
        (d/transact conn schema)
        (is (= (avet-a-v @conn)
               #{[:db/ident :age] [:db/ident :name]}))
        (d/transact conn tx1)
        (is (= (avet-a-v @conn)
               #{[:db/ident :age] [:db/ident :name] [:name "Alice"]}))))

    (testing "Entry in avet index only when indexing true for reference DB"
      (let [conn (setup-db ref-cfg)
            initial-avet (avet-a-v @conn)
            ref (fn [ident] (get-in @conn [:ident-ref-map ident]))]
        (d/transact conn schema)
        (is (= (difference (avet-a-v @conn) initial-avet)
               #{[1 :age] [1 :name]}))
        (d/transact conn tx1)
        (is (= (difference (avet-a-v @conn) initial-avet)
               #{[(ref :name) "Alice"] [1 :age] [1 :name]}))))))

(deftest test-transact-nested-data
  (let [schema [{:db/ident :name
                 :db/cardinality :db.cardinality/one
                 :db/unique :db.unique/identity
                 :db/valueType :db.type/string}
                {:db/ident :sibling
                 :db/cardinality :db.cardinality/many
                 :db/valueType :db.type/ref}]
        find-alices (fn [db] (d/q '[:find ?e :where [?e :name "Alice"]] db))
        find-bobs (fn [db] (d/q '[:find ?e :where [?e :name "Bob"]] db))
        tx1 [{:name "Alice"}]
        tx2 [{:name "Charlie"
              :sibling [{:name "Alice"} {:name "Bob"}]}]]
    (testing "Resolve nesting in keyword DB"
      (let [conn (setup-db no-ref-cfg)]
        (d/transact conn (vec (concat schema tx1)))
        (d/transact conn tx2)
        (is (= 1 (count (find-alices @conn))))
        (is (= 1 (count (find-bobs @conn))))))

    (testing "Resolve nesting in reference DB"
      (let [conn (setup-db ref-cfg)]
        (d/transact conn (vec (concat schema tx1)))
        (d/transact conn tx2)
        (is (= 1 (count (find-alices @conn))))
        (is (= 1 (count (find-bobs @conn))))))))

(deftest test-transact-data-with-keyword-attr
  (testing "Keyword transaction in keyword DB"
    (let [conn (setup-db no-ref-cfg)
          next-eid (inc (:max-eid @conn))]
      (is (not (nil? (d/transact conn [[:db/add next-eid :db/ident :name]]))))))

  (testing "Keyword transaction in reference DB"
    (let [conn (setup-db ref-cfg)
          next-eid (inc (:max-eid @conn))]
      (is (thrown-msg? (str "Bad entity attribute " :db/ident " at " [:db/add next-eid :db/ident :name] ", expected reference number") ;; TODO: ensure to have error thrown
                       (d/transact conn [[:db/add next-eid :db/ident :name]]))))))

(deftest test-transact-data-with-reference-attr
  (testing "Reference transaction in keyword DB"
    (let [conn (setup-db no-ref-cfg)
          next-eid (inc (:max-eid @conn))]
      (is (thrown-msg? (str "Bad entity attribute " 1 " at " [:db/add next-eid 1 :name] ", expected keyword or string")
                       (d/transact conn [[:db/add next-eid 1 :name]])))))

  (testing "Reference transaction in reference DB"
    (let [conn (setup-db ref-cfg)
          next-eid (inc (:max-eid @conn))]
      (is (not (nil? (d/transact conn [[:db/add next-eid 1 :name]])))))))

(deftest test-system-schema-protection
  (let [conn (setup-db ref-cfg)]
    (testing "Transact sequential system schema update"
      (is (thrown-msg? "System schema entity cannot be changed"
                       (d/transact conn [[:db/add 1 1 :name]]))))

    (testing "Transact system schema update as map"
      (is (thrown-msg? "Entity with ID 1 is a system attribute :db/ident and cannot be changed"
                       (d/transact conn [{:db/id 1 :db/ident :name}]))))))

(deftest test-system-attribute-protection
  (testing "Use system keyword for schema in keyword DB"
    (let [conn (setup-db no-ref-cfg)]
      (is (thrown-msg? "Using namespace 'db' for attribute identifiers is not allowed"
                       (d/transact conn [{:db/ident :db/unique}])))))

  (testing "Use system keyword for schema in keyword DB"
    (let [conn (setup-db ref-cfg)]
      (is (thrown-msg? "Using namespace 'db' for attribute identifiers is not allowed"
                       (d/transact conn [{:db/ident :db/unique}]))))))

(deftest test-system-enum-protection
  (testing "Use system keyword for schema in keyword DB"
    (let [conn (setup-db no-ref-cfg)]
      (is (thrown-msg? "Using namespace 'db' for attribute identifiers is not allowed"
                       (d/transact conn [{:db/ident :db.cardinality/many}])))))

  (testing "Use system keyword for schema in keyword DB"
    (let [conn (setup-db ref-cfg)]
      (is (thrown-msg? "Using namespace 'db' for attribute identifiers is not allowed"
                       (d/transact conn [{:db/ident :db.cardinality/many}]))))))

(deftest test-read-schema                                   ;; thrown-msg not working? intended behavior?
  (testing "No error in combination with schema-flexibility read for keyword DB"
    (let [read-no-ref-cfg (assoc no-ref-cfg :schema-flexibility :read)]
      (db/empty-db nil read-no-ref-cfg)
      (db/init-db [] nil read-no-ref-cfg)))

  (testing "Error in combination with schema-flexibility read for reference DB"
    (let [read-ref-cfg (assoc ref-cfg :schema-flexibility :read)]
      (is (thrown-msg? "Attribute references cannot be used with schema-flexibility ':read'."
                       (db/empty-db nil read-ref-cfg)))
      (is (thrown-msg? "Attribute references cannot be used with schema-flexibility ':read'."
                       (db/init-db [] nil read-ref-cfg))))))

(deftest test-query
  (testing "Query keyword translation keyword db"
    (let [conn (setup-db no-ref-cfg)
          schema [{:db/ident :name
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string}]
          ref (fn [ident] (db/-ref-for @conn ident))]
      (d/transact conn schema)
      (d/transact conn [{:name "Alice"}
                        {:name "Bob"}])
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ ?a :where [_ ?a ?n]] @conn :name)))
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ ?a :where [_ ?a ?n]] @conn (ref :name))))
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ :where [_ :name ?n]] @conn)))
      (is (= #{}
             (d/q '[:find ?n :in $ :where [_ ?a ?n] [?a :db/ident :name]] @conn)))
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ ?a :where [_ ?a ?n]] @conn :name)))))

  (testing "Query keyword translation reference db"
    (let [conn (setup-db ref-cfg)
          schema [{:db/ident :name
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string}]
          ref (fn [ident] (db/-ref-for @conn ident))]
      (d/transact conn schema)
      (d/transact conn [{:name "Alice"}
                        {:name "Bob"}])
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ ?a :where [_ ?a ?n]] @conn :name)))
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ ?a :where [_ ?a ?n]] @conn (ref :name))))
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ :where [_ :name ?n]] @conn)))
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ :where [_ ?a ?n] [?a :db/ident :name]] @conn)))
      (is (= #{["Alice"] ["Bob"]}
             (d/q '[:find ?n :in $ ?a :where [_ ?a ?n]] @conn :name))))))

(deftest test-pull-ref-db
  (let [conn (setup-db ref-cfg)
        schema [{:db/ident :name
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/string}
                {:db/ident :aka
                 :db/cardinality :db.cardinality/many
                 :db/valueType :db.type/string}
                {:db/ident :child
                 :db/cardinality :db.cardinality/many
                 :db/valueType :db.type/ref}
                {:db/ident :father
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/ref}]
        _ (d/transact conn schema)
        _ (d/transact conn [{:name "Ivan"
                             :aka ["Devil" "Tupen"]}])
        ivan (ffirst (d/q '[:find ?e :where [?e :name "Ivan"]] @conn))
        _ (d/transact conn [{:name "Matthew"
                             :father ivan}])
        matthew (ffirst (d/q '[:find ?e :where [?e :name "Matthew"]] @conn))]

    (is (= {:name "Ivan" :aka ["Devil" "Tupen"]}
           (d/pull @conn '[:name :aka] ivan)))

    (is (= {:name "Matthew" :father {:db/id ivan} :db/id matthew}
           (d/pull @conn '[:name :father :db/id] matthew)))

    (is (= [{:name "Ivan"} {:name "Matthew"}]
           (d/pull-many @conn '[:name] [ivan matthew])))

    (is (= {:name "Ivan" :_father [{:db/id matthew}]}
           (d/pull @conn '[:name :_father] ivan)))))
