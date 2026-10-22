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

(defn- cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true
   :schema-flexibility :write
   :attribute-refs? true})

(defn- with-conn [f]
  (let [cf (cfg)]
    (d/create-database cf)
    (let [conn (d/connect cf)]
      (try (f conn) (finally (d/release conn) (d/delete-database cf))))))

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
