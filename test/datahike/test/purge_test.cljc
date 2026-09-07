(ns datahike.test.purge-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datahike.api :as d]
   [datahike.datom :as dd]
   [datahike.db :as db]
   #?(:clj [datahike.tx-preds :as txp])
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

(defn- reference-purge-db [refs?]
  (d/db-with (db/empty-db nil {:attribute-refs? refs?
                               :keep-history? true :schema-flexibility :write})
             [{:db/ident :age :db/valueType :db.type/long
               :db/cardinality :db.cardinality/one}
              {:db/ident :friend :db/valueType :db.type/ref
               :db/cardinality :db.cardinality/one}]))

(defn- attribute-values [database field ident]
  (let [a (if (get-in database [:config :attribute-refs?])
            (get (:ident-ref-map database) ident) ident)]
    (into #{} (comp (filter #(= a (:a %))) (map :v)) (get database field))))

(deftest purge-normalizes-attribute-idents-in-reference-databases
  (doseq [refs? [false true]]
    (let [snapshot (-> (reference-purge-db refs?)
                       (d/db-with [{:db/id 10000 :age 1}])
                       (d/db-with [[:db/add 10000 :age 2]]))]
      (doseq [attr (cond-> [:age] refs? (conj (get (:ident-ref-map snapshot) :age)))]
        (let [value-purged (d/db-with snapshot [[:db/purge 10000 attr 1]])
              attribute-purged (d/db-with snapshot [[:db.purge/attribute 10000 attr]])]
          (is (= #{2} (attribute-values value-purged :eavt :age)))
          (is (= #{2} (attribute-values value-purged :temporal-eavt :age)))
          (is (empty? (attribute-values attribute-purged :eavt :age)))
          (is (empty? (attribute-values attribute-purged :temporal-eavt :age))))))))

(deftest purge-entity-removes-current-and-historical-incoming-references
  (doseq [refs? [false true]]
    (let [snapshot (-> (reference-purge-db refs?)
                       (d/db-with [{:db/id 10000 :age 1}
                                   {:db/id 10002 :age 2}
                                   {:db/id 10001 :friend 10000}
                                   {:db/id 10003 :friend 10000}])
                       (d/db-with [[:db/add 10001 :friend 10002]]))
          purged (d/db-with snapshot [[:db.purge/entity 10000]])]
      (is (= #{10000 10002} (attribute-values snapshot :eavt :friend)))
      (is (= #{10002} (attribute-values purged :eavt :friend)))
      (is (= #{10002} (attribute-values purged :temporal-eavt :friend)))
      (is (empty? (filter #(= 10000 (:e %)) (:eavt purged)))))))

(deftest purge-reference-values-accepts-keyword-and-numeric-attributes
  (doseq [refs? [false true]]
    (let [snapshot (-> (reference-purge-db refs?)
                       (d/db-with [{:db/id 10000 :age 1}
                                   {:db/id 10002 :age 2}
                                   {:db/id 10001 :friend 10000}])
                       (d/db-with [[:db/add 10001 :friend 10002]]))]
      (doseq [attr (cond-> [:friend] refs? (conj (get (:ident-ref-map snapshot) :friend)))]
        (let [purged (d/db-with snapshot [[:db/purge 10001 attr 10000]])]
          (is (= #{10002} (attribute-values purged :eavt :friend)))
          (is (= #{10002} (attribute-values purged :temporal-eavt :friend))))))))

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
        (let [report (d/transact conn [[:db/purge [:name name] :age 35]])]
          (is (not (contains? report :datahike/tx-ops)))
          (is (not (contains? report :tx-ops))))
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

#?(:clj
   (deftest transaction-predicate-sees-expanded-operation-kinds
     (let [store-id #uuid "09000000-0000-0000-0000-000000000006"
           conn (tu/setup-db (assoc-in cfg-template [:store :id] store-id))
           observed (atom nil)]
       (txp/register-tx-pred!
        store-id
        #(reset! observed (:datahike/tx-ops %)))
       (try
         (let [report (d/transact
                       conn
                       [[:db.fn/call
                         (fn [_]
                           [[:db/add [:name "Alice"] :age 26]
                            [:db.purge/entity [:name "Bob"]]])]])]
           (is (= #{:db.fn/call :db/add :db.purge/entity} @observed))
           (is (not (contains? report :datahike/tx-ops)))
           (is (not (contains? report :tx-ops)))
           (is (= 26 (find-age @conn "Alice")))
           (is (empty? (find-entity (d/history @conn) "Bob"))))
         (finally
           (txp/unregister-tx-pred! store-id)
           (d/release conn))))))

(deftest operation-provenance-is-internal-on-empty-and-raw-datom-reports
  (let [conn (tu/setup-db
              (-> cfg-template
                  (assoc-in [:store :id]
                            #uuid "09000000-0000-0000-0000-000000000007")
                  (assoc :keep-history? false)))
        bob-eid (d/q '[:find ?e . :where [?e :name "Bob"]] @conn)
        age (first (d/datoms @conn :eavt bob-eid :age))
        raw-retraction (dd/datom (:e age) (:a age) (:v age)
                                 (inc (:max-tx @conn)) false)]
    (is (not (contains? (d/with @conn []) :datahike/tx-ops)))
    (is (not (contains? (d/with @conn [raw-retraction]) :datahike/tx-ops)))
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
