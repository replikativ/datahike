(ns datahike.test.attribute-refs.schema-values-test
  "Schema-level values under `:attribute-refs?`.

   Under refs a datom holds the attribute's ENTITY id, and three system
   attributes (`:db/valueType`, `:db/cardinality`, `:db/unique`) hold a system
   entity id as their VALUE. Which values are system refs is a property of the
   ATTRIBUTE (`:db/systemAttribRef`), never of the number: a plain long that
   happens to equal a system entity id is still a long.

   Entity specs (`:db.entity/attrs`, `:db.entity/preds`, `:db/ensure`) and the
   map-form attribute rename (`{:db/id aid :db/ident :new}`) must work the same
   as without refs."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.constants :as c]))

(defn- cfg
  ([] (cfg true))
  ([refs?]
   {:store {:backend :memory :id (java.util.UUID/randomUUID)}
    :keep-history? true
    :schema-flexibility :write
    :attribute-refs? refs?}))

(defn- with-conn
  ([f] (with-conn true f))
  ([refs? f]
   (let [cf (cfg refs?)]
     (d/create-database cf)
     (let [conn (d/connect cf)]
       (try (f conn) (finally (d/release conn) (d/delete-database cf)))))))

(defn- root-message [e]
  (loop [x e] (if-let [c (.getCause ^Throwable x)] (recur c) (ex-message x))))

(defn- sys-id [ident]
  (some #(when (= ident (:db/ident %)) (:db/id %)) c/system-schema))

(deftest a-long-that-equals-a-system-entity-id-is-a-long
  (doseq [n [(sys-id :dh.ref/temporal) (sys-id :db.cardinality/many) (sys-id :db.unique/identity)]]
    (testing (str ":db/maxLength " n " (the id of a system entity)")
      (with-conn
        (fn [conn]
          (d/transact conn [{:db/ident :p/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/maxLength n}])
          (is (= n (:db/maxLength (get (:schema @conn) :p/name)))
              "the schema holds the number, not the system ident it collides with")
          (is (= n (:db/maxLength (d/pull @conn '[:db/maxLength] [:db/ident :p/name]))))
          (d/transact conn [{:p/name (apply str (repeat n \a))}])
          (is (thrown? Exception
                       (d/transact conn [{:p/name (apply str (repeat (inc n) \a))}]))
              "and it is enforced as a length"))))))

(def ^:private account-schema
  [{:db/ident :account/email
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :account/balance
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(defn positive-balance? [db eid]
  (pos? (:account/balance (d/entity db eid) 0)))

(deftest entity-specs-under-attribute-refs
  (with-conn
    (fn [conn]
      (d/transact conn account-schema)
      (d/transact conn [{:db/ident :account/guard
                         :db.entity/attrs [:account/email :account/balance]}
                        {:db/ident :account/positive
                         :db.entity/preds ['datahike.test.attribute-refs.schema-values-test/positive-balance?]}])
      (testing "the spec is stored and readable"
        (is (= #{:account/email :account/balance}
               (set (:db.entity/attrs (d/pull @conn '[:db.entity/attrs] [:db/ident :account/guard])))))
        (is (= #{:account/email :account/balance}
               (set (:db.entity/attrs (get (:schema @conn) :account/guard))))))
      (testing ":db.entity/attrs"
        (d/transact conn [{:account/email "a@x" :account/balance 1 :db/ensure :account/guard}])
        (is (= 1 (:account/balance (d/entity @conn [:account/email "a@x"]))))
        (is (re-find #"missing attributes #\{:account/balance\} of spec :account/guard"
                     (try (d/transact conn [{:account/email "b@x" :db/ensure :account/guard}]) ""
                          (catch Exception e (root-message e))))))
      (testing ":db.entity/preds"
        (d/transact conn [{:account/email "c@x" :account/balance 5 :db/ensure :account/positive}])
        (is (re-find #"failed predicates"
                     (try (d/transact conn [{:account/email "d@x" :account/balance -5
                                             :db/ensure :account/positive}]) ""
                          (catch Exception e (root-message e)))))))))

(deftest map-form-rename-under-attribute-refs
  (with-conn
    (fn [conn]
      (d/transact conn [{:db/ident :p/name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      (let [aid (:db/id (d/entity @conn :p/name))]
        (d/transact conn [{:db/id 1000 :p/name "Ann"}])
        (d/transact conn [{:db/id aid :db/ident :p/renamed}])
        (is (= #{[1000 "Ann"]} (d/q '[:find ?e ?v :where [?e :p/renamed ?v]] @conn))
            "existing data answers to the new name")
        (d/transact conn [{:db/id 1001 :p/renamed "Bob"}])
        (is (= #{[1000 "Ann"] [1001 "Bob"]} (d/q '[:find ?e ?v :where [?e :p/renamed ?v]] @conn)))
        (is (= aid (:db/id (d/entity @conn :p/renamed))))))))

(deftest reusing-a-renamed-attributes-old-name-is-not-served-from-cache
  (with-conn
    (fn [conn]
      (d/transact conn [{:db/ident :p/name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      (let [aid (:db/id (d/entity @conn :p/name))
            q '[:find ?e ?v :where [?e :p/name ?v]]]
        (d/transact conn [{:db/id 1000 :p/name "Ann"}])
        (d/transact conn [[:db/add aid :db/ident :p/renamed]])
        ;; The old name still resolves to the entity until it is reused.
        (is (= #{[1000 "Ann"]} (d/q q @conn)))
        (d/transact conn [{:db/ident :p/name :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (is (not= aid (:db/id (d/entity @conn :p/name))))
        (is (= #{} (d/q q @conn))
            "the name now denotes a different, empty attribute")))))

(defn- tx-error [conn tx]
  (try (d/transact conn tx) nil (catch Exception e (root-message e))))

(deftest retracting-one-spec-value-keeps-the-rest
  (doseq [refs? [true false]]
    (testing (str ":attribute-refs? " refs?)
      (with-conn refs?
        (fn [conn]
          (d/transact conn [{:db/ident :a/x :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                            {:db/ident :a/y :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
          (d/transact conn [{:db/ident :a/guard :db.entity/attrs [:a/x :a/y]
                             :db.entity/preds ['clojure.core/some? 'clojure.core/any?]}])
          (d/transact conn [[:db/retract [:db/ident :a/guard] :db.entity/attrs :a/y]
                            [:db/retract [:db/ident :a/guard] :db.entity/preds 'clojure.core/any?]])
          (let [spec (get (:schema @conn) :a/guard)]
            (is (= [:a/x] (vec (:db.entity/attrs spec))))
            (is (= '[clojure.core/some?] (vec (:db.entity/preds spec)))))
          (is (re-find #"missing attributes #\{:a/x\}"
                       (str (tx-error conn [{:a/y "q" :db/ensure :a/guard}])))
              "the remaining required attribute is still enforced"))))))

(deftest ensure-checks-the-entity-not-the-transaction
  (doseq [refs? [true false]]
    (testing (str ":attribute-refs? " refs?)
      (with-conn refs?
        (fn [conn]
          (d/transact conn [{:db/ident :a/x :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                            {:db/ident :a/y :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
          (d/transact conn [{:db/ident :a/guard :db.entity/attrs [:a/x :a/y]}])
          (d/transact conn [{:db/id 5000 :a/x "1" :a/y "2" :db/ensure :a/guard}])
          (is (nil? (tx-error conn [{:db/id 5000 :a/x "3" :db/ensure :a/guard}]))
              "an attribute the entity already has satisfies the spec")
          (is (re-find #"missing attributes #\{:a/y\}"
                       (str (tx-error conn [[:db/retract 5000 :a/y "2"]
                                            {:db/id 5000 :a/x "4" :db/ensure :a/guard}])))
              "a retracted attribute does not"))))))

(deftest map-form-rename-onto-an-existing-ident
  (with-conn
    (fn [conn]
      (d/transact conn [{:db/ident :a/x :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                        {:db/ident :a/y :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
      (is (= "Schema with attribute :a/y already exists"
             (tx-error conn [{:db/id (:db/id (d/entity @conn :a/x)) :db/ident :a/y}]))))))

(deftest a-database-without-entity-spec-attributes-says-why
  (with-conn
    (fn [conn]
      ;; A database created before :db.entity/* joined the system schema:
      ;; its stored ident map has no entry for them.
      (let [old-db (update @conn :ident-ref-map dissoc :db.entity/attrs :db.entity/preds)
            msg (try (d/db-with old-db [{:db/ident :a/guard :db.entity/attrs [:a/x]}]) nil
                     (catch Exception e (root-message e)))]
        (is (re-find #"predates entity specs" (str msg)))
        (is (re-find #":db.entity/attrs" (str msg)))
        (is (re-find #"export-db" (str msg)))))))
