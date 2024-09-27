(ns datahike.test.attribute-refs.differences-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj [clojure.test :as t :refer [is deftest testing]])
   [clojure.set :refer [difference]]
   [datahike.api :as d]
   [datahike.constants :as const]
   [datahike.db :as db :refer [ref-datoms]]
   [datahike.db.interface :as dbi]
   [datahike.test.core-test]
   [datahike.test.utils :refer [get-time]])
  #?(:clj (:import [datahike.datom Datom])))

#?(:cljs (def Throwable js/Error))

(def no-ref-cfg
  {:store {:backend :mem}
   :keep-history? true
   :attribute-refs? false
   :schema-flexibility :write})

(def ref-cfg
  {:store {:backend :mem}
   :keep-history? true
   :attribute-refs? true
   :schema-flexibility :write})

(defn init-cfgs []
  (let [t (get-time)]
    [(assoc-in no-ref-cfg [:store :id] (str t))
     (assoc-in ref-cfg [:store :id] (str (inc t)))]))

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
  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        conn (setup-db no-ref-cfg)]
    (testing "Empty EAVT index for keyword DB"
      (is (= nil
             (d/datoms @conn :eavt))))
    (testing "Empty AEVT index for keyword DB"
      (is (= nil
             (d/datoms @conn :aevt))))
    (testing "Empty AVET index for keyword DB"
      (is (= nil
             (d/datoms @conn :avet))))
    (d/release conn))

  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        conn (setup-db ref-cfg)]
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
               (set (d/datoms @conn :avet))))))
    (d/release conn)))

(deftest test-last-entity-id                                ;; TODO: What is the behavior wanted?
  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        find-last-entity-id (fn [db]
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
        (is (= const/e0 (:max-eid @conn)))
        (is (= -1 (find-last-entity-id @conn))))

      (testing "Last entity id for non-empty keyword DB"
        (d/transact conn simple-schema)
        (is (= (+ 1 const/e0) (:max-eid @conn)))
        (is (= (+ 1 const/e0) (find-last-entity-id @conn))))
      (d/release conn))

    (let [conn (setup-db ref-cfg)]
      (testing "Last entity id for empty reference DB"
        (is (= const/ue0 (:max-eid @conn)))
        (is (= const/ue0 (find-last-entity-id @conn))))

      (testing "Last entity id for non-empty reference DB"
        (d/transact conn simple-schema)
        (is (= (+ 1 const/ue0) (:max-eid @conn)))
        (is (= (+ 1 const/ue0) (find-last-entity-id @conn))))
      (d/release conn))))

(deftest test-transact-schema
  (testing "Schema for keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db no-ref-cfg)]
      (is (= (:schema @conn)
             const/non-ref-implicit-schema))
      (d/transact conn name-schema)
      (is (= (:schema @conn)
             (merge const/non-ref-implicit-schema {:name (first name-schema)} {1 :name})))
      (d/release conn)))
  (testing "Schema for reference DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db ref-cfg)]
      (is (= (:schema @conn)
             const/ref-implicit-schema))
      (d/transact conn name-schema)
      (is (= (:schema @conn)
             (merge const/ref-implicit-schema
                    {:name (first name-schema)}
                    {(+ 1 const/ue0) :name})))
      (is (contains? (-> (:rschema @conn) :db/ident) :name))
      (is (contains? (-> (:ident-ref-map @conn) keys set) :name))
      (is (contains? (-> (:ref-ident-map @conn) vals set) :name))

      (testing "after reconnection"
        (d/release conn)
        (let [conn2 (d/connect ref-cfg)]
          (is (= (:schema @conn2)
                 (merge const/ref-implicit-schema
                        {:name (first name-schema)}
                        {(+ 1 const/ue0) :name})))
          (is (contains? (-> (:rschema @conn2) :db/ident) :name))
          (is (contains? (-> (:ident-ref-map @conn2) keys set) :name))
          (is (contains? (-> (:ref-ident-map @conn2) vals set) :name))
          (d/release conn2))))))

(deftest test-transact-tempid
  (testing "Tempid resolution for keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db no-ref-cfg)]
      (is (= (:tempids (d/transact conn name-schema))
             {:db/current-tx (+ 1 const/tx0)}))
      (is (= (:tempids (d/transact conn [{:db/id -1 :name "Ivan"}]))
             {-1 (+ 2 const/e0), :db/current-tx (+ 2 const/tx0)}))
      (is (= (:tempids (d/transact conn [[:db/add -2 :name "Petr"]]))
             {-2 (+ 3 const/e0), :db/current-tx (+ 3 const/tx0)}))
      (is (= (:tempids (d/transact conn [{:db/id "Serg" :name "Sergey"}]))
             {"Serg" (+ 4 const/e0), :db/current-tx (+ 4 const/tx0)}))
      (d/release conn)))

  (testing "Tempid resolution for reference DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db ref-cfg)]
      (is (= (:tempids (d/transact conn name-schema))
             {:db/current-tx (+ 1 const/tx0)}))
      (is (= (:tempids (d/transact conn [{:db/id -1 :name "Ivan"}]))
             {-1 (+ 2 const/ue0), :db/current-tx (+ 2 const/tx0)}))
      (is (= (:tempids (d/transact conn [[:db/add -2 (get-in @conn [:ident-ref-map :name]) "Petr"]]))
             {-2 (+ 3 const/ue0), :db/current-tx (+ 3 const/tx0)}))
      (is (= (:tempids (d/transact conn [{:db/id "Serg" :name "Sergey"}]))
             {"Serg" (+ 4 const/ue0), :db/current-tx (+ 4 const/utx0)}))
      (d/release conn))))

(deftest test-system-attr-resolution
  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        schema [{:db/ident :name
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
               #{:db/ident :db/cardinality :db/valueType :db/txInstant}))
        (d/release conn)))

    (testing "Resolve attributes in reference DB"
      (let [conn (setup-db ref-cfg)
            tx-data (:tx-data (d/transact conn schema))]
        (is (= (keyword-attrs tx-data)
               #{}))
        (d/release conn)))))

(deftest test-system-enum-resolution
  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        schema [{:db/ident :name
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
               #{:name :db.cardinality/one :db.type/string}))
        (d/release conn)))

    (testing "Resolve enums in reference DB"
      (let [conn (setup-db ref-cfg)
            tx-data (:tx-data (d/transact conn schema))]
        (is (= (unresolved-enums tx-data)
               #{:name}))
        (d/release conn)))))

(deftest test-store-db-id-as-keyword
  (doseq [cfg (init-cfgs)]
    (testing "Do not resolve enums in keyword DB"
      (let [conn (setup-db cfg)]
        (d/transact conn [{:db/ident :attribute-to-use
                           :db/cardinality :db.cardinality/one
                           :db/valueType :db.type/keyword}])
        (d/transact conn [{:attribute-to-use :the-location}
                          {:attribute-to-use :db/id}])
        (is (= #{[:the-location]
                 [:db/id]}
               (d/q '[:find ?v
                      :in $ ?a
                      :where
                      [_ :attribute-to-use ?v]]
                    @conn)))
        (d/release conn)))))

(deftest test-indexing
  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        schema [{:db/ident :name
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
               #{[:db/ident :age] [:db/ident :name] [:name "Alice"]}))
        (d/release conn)))

    (testing "Entry in avet index only when indexing true for reference DB"
      (let [conn (setup-db ref-cfg)
            initial-avet (avet-a-v @conn)
            ref (fn [ident] (get-in @conn [:ident-ref-map ident]))]
        (d/transact conn schema)
        (is (= (difference (avet-a-v @conn) initial-avet)
               #{[1 :age] [1 :name]}))
        (d/transact conn tx1)
        (is (= (difference (avet-a-v @conn) initial-avet)
               #{[(ref :name) "Alice"] [1 :age] [1 :name]}))
        (d/release conn)))))

(deftest test-transact-nested-data
  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        schema [{:db/ident :name
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
        (is (= 1 (count (find-bobs @conn))))
        (d/release conn)))

    (testing "Resolve nesting in reference DB"
      (let [conn (setup-db ref-cfg)]
        (d/transact conn (vec (concat schema tx1)))
        (d/transact conn tx2)
        (is (= 1 (count (find-alices @conn))))
        (is (= 1 (count (find-bobs @conn))))
        (d/release conn)))))

(deftest test-transact-data-with-keyword-attr
  (testing "Keyword transaction in keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db no-ref-cfg)
          next-eid (inc (:max-eid @conn))]
      (is (not (nil? (d/transact conn [[:db/add next-eid :db/ident :name]]))))
      (d/release conn)))

  (testing "Using :db/ident attribute"
    (doseq [cfg (init-cfgs)
            :let [attribute-refs? (:attribute-refs? cfg)
                  conn (setup-db cfg)
                  next-eid (inc (:max-eid @conn))
                  init-datoms (d/datoms
                               @conn
                               {:index :aevt
                                :components [:db/ident]})]]

      ;; Check that the database with attribute-refs? being true
      ;; contains some initial atoms, and otherwise none.
      (is (= (boolean (seq init-datoms))
             (boolean attribute-refs?)))

      ;; Transact a datom for attribute :db/ident. This
      ;; must work no matter the value of :attribute-refs?
      (is (some? (d/transact
                  conn
                  [[:db/add next-eid :db/ident :name]])))

      ;; Check that we can access the datom just transacted
      ;; using d/datoms.
      (let [datoms (d/datoms @conn
                             {:index :aevt
                              :components [:db/ident]})]
        (is (= (inc (count init-datoms))
               (count datoms)))
        (is (some (fn [[_ a v]]
                    (and (= v :name)
                         (or attribute-refs?
                             (= a :db/ident))))
                  datoms)))

      ;; Check for a more specific query.
      (let [[ident-name-datom :as ident-name-datoms]
            (d/datoms @conn
                      {:index :avet
                       :components [:db/ident
                                    :name]})]
        (is (= 1 (count ident-name-datoms)))
        (is (= :name (:v ident-name-datom)))
        (is (or attribute-refs?
                (= :db/ident (:a ident-name-datom)))))
      (d/release conn))))

(deftest test-transact-data-with-reference-attr
  (testing "Reference transaction in keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db no-ref-cfg)
          next-eid (inc (:max-eid @conn))]
      (is (thrown-with-msg? Throwable
                            (re-pattern (str "Bad entity attribute 1"
                                             " at \\[:db/add " next-eid " 1 :name\\],"
                                             " expected keyword or string"))
                            (d/transact conn [[:db/add next-eid 1 :name]])))
      (d/release conn)))

  (testing "Reference transaction in reference DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db ref-cfg)
          next-eid (inc (:max-eid @conn))]
      (is (not (nil? (d/transact conn [[:db/add next-eid 1 :name]]))))
      (d/release conn))))

(deftest test-system-schema-protection
  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        conn (setup-db ref-cfg)]
    (testing "Transact sequential system schema update"
      (is (thrown-with-msg? Throwable
                            #"System schema entity cannot be changed"
                            (d/transact conn [[:db/add 1 1 :name]]))))

    (testing "Transact system schema update as map"
      (is (thrown-with-msg? Throwable
                            #"Entity with ID 1 is a system attribute :db/ident and cannot be changed"
                            (d/transact conn [{:db/id 1 :db/ident :name}]))))
    (d/release conn)))

(deftest test-system-attribute-protection
  (testing "Use system keyword for schema in keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db no-ref-cfg)]
      (is (thrown-with-msg? Throwable #"Using namespace 'db' for attribute identifiers is not allowed"
                            (d/transact conn [{:db/ident :db/unique}])))
      (d/release conn)))

  (testing "Use system keyword for schema in keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db ref-cfg)]
      (is (thrown-with-msg? Throwable #"Using namespace 'db' for attribute identifiers is not allowed"
                            (d/transact conn [{:db/ident :db/unique}])))
      (d/release conn))))

(deftest test-system-enum-protection
  (testing "Use system keyword for schema in keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db no-ref-cfg)]
      (is (thrown-with-msg? Throwable #"Using namespace 'db' for attribute identifiers is not allowed"
                            (d/transact conn [{:db/ident :db.cardinality/many}])))
      (d/release conn)))

  (testing "Use system keyword for schema in keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db ref-cfg)]
      (is (thrown-with-msg? Throwable #"Using namespace 'db' for attribute identifiers is not allowed"
                            (d/transact conn [{:db/ident :db.cardinality/many}])))
      (d/release conn))))

(deftest test-read-schema
  (testing "No error in combination with schema-flexibility read for keyword DB"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          read-no-ref-cfg (assoc no-ref-cfg :schema-flexibility :read)]
      (db/empty-db nil read-no-ref-cfg)
      (db/init-db [] nil read-no-ref-cfg)))

  (testing "Error in combination with schema-flexibility read for reference DB"
    (let [read-ref-cfg (assoc ref-cfg :schema-flexibility :read)]
      (is (thrown-with-msg? Throwable
                            #"Attribute references cannot be used with schema-flexibility ':read'."
                            (db/empty-db nil read-ref-cfg)))
      (is (thrown-with-msg? Throwable
                            #"Attribute references cannot be used with schema-flexibility ':read'."
                            (db/init-db [] nil read-ref-cfg))))))

(deftest test-query
  (testing "Query keyword translation keyword db"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db no-ref-cfg)
          schema [{:db/ident :name
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string}]
          ref (fn [ident] (dbi/-ref-for @conn ident))]
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
             (d/q '[:find ?n :in $ ?a :where [_ ?a ?n]] @conn :name)))
      (d/release conn)))

  (testing "Query keyword translation reference db"
    (let [[no-ref-cfg ref-cfg] (init-cfgs)
          conn (setup-db ref-cfg)
          schema [{:db/ident :name
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/string}]
          ref (fn [ident] (dbi/-ref-for @conn ident))]
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
             (d/q '[:find ?n :in $ ?a :where [_ ?a ?n]] @conn :name)))
      (d/release conn))))

(deftest test-pull-ref-db
  (let [[no-ref-cfg ref-cfg] (init-cfgs)
        conn (setup-db ref-cfg)
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
           (d/pull @conn '[:name :_father] ivan)))
    (d/release conn)))
