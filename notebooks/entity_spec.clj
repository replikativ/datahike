(ns datahike.notebooks.entity-spec
  (:require [datahike.api :as d]))

;; setup database with schema and connection
(def schema [{:db/ident :account/email
              :db/valueType :db.type/string
              :db/unique :db.unique/identity
              :db/cardinality :db.cardinality/one}
             {:db/ident :account/holder
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
             {:db/ident :account/balance
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}])

(def cfg {:store {:backend :mem
                  :id "accounts"}
          :name "accounts"
          :schema-flexibility :write
          :keep-history? true
          :initial-tx schema})

(d/delete-database cfg)

(d/create-database cfg)

(def conn (d/connect cfg))

;; define both predicates
(defn is-email? [db eid]
  (if-let [email (:account/email (d/entity db eid))]
    (-> (re-find #"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)" email) empty? not)
    false))

(defn positive-balance? [db eid]
  (if-let [balance (-> (d/entity db eid) :account/balance)]
    (< 0 balance)
    false))

;; add the person spec
(d/transact conn {:tx-data [{:db/ident :person/guard
                             :db.entity/attrs [:account/email :account/holder]
                             :db.entity/preds ['user/is-email?]}]})

(def valid-account {:account/email "emma@datahike.io"
                    :account/holder "Emma"})

;; add a valid person
(d/transact conn {:tx-data [(assoc valid-account :db/ensure :person/guard)]})

;; add with missing holder, observe exception
(d/transact conn {:tx-data [{:account/email "benedikt@datahike.io"
                             :db/ensure :person/guard}]})

;; add with invalid email, observe exception
(d/transact conn {:tx-data [{:account/email "thekla@datahike"
                             :account/holder "Thekla"
                             :db/ensure :person/guard}]})

;; add the balance spec
(d/transact conn {:tx-data [{:db/ident :balance/guard
                             :db.entity/attrs [:account/balance]
                             :db.entity/preds ['user/positive-balance?]}]})

;; add valid balance
(d/transact conn {:tx-data [{:db/id [:account/email (:account/email valid-account)]
                             :account/balance 1000
                             :db/ensure :balance/guard}]})

;; add invalid negative balance, observe exception
(d/transact conn {:tx-data [{:db/id [:account/email (:account/email valid-account)]
                             :account/balance -100
                             :db/ensure :balance/guard}]})
