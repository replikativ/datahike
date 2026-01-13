# Entity Specs

Similar to [Datomic's](https://www.datomic.com/) [entity specs](https://docs.datomic.com/on-prem/schema.html#entity-specs) Datahike supports assertion to ensure properties of transacted entities.

**⚠️ Important:** Predicate validation (`:db.entity/preds`) only works in Clojure, not ClojureScript. Attribute validation (`:db.entity/attrs`) works in both.

In short: you need to transact a `spec` to the database using `:db/ident` as the identifier, with at least either `:db.entity/attrs` as a list of attributes defined in the schema for ensuring required attributes, or `:db.entity/preds` with a list of fully namespaced symbols that refer to predicate functions that you want to assert. The signature of the predicate function should be `[db eid]` with the database value `db` where the transaction has happened and the entity id `eid` to be asserted.


## Example

Imagine you have a database where an account has an email, a holder, and a balance. Each new account should have an `email` and a `holder` as a required attribute and the email should be checked for its standard. 
The next spec should check a new `balance` to not be less than 0.


Let's fire up a REPL:

```clojure
(require '[datahike.api :as d])

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

(def cfg {:store {:backend :memory
                  :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
          :schema-flexibility :write
          :keep-history? true
          :initial-tx schema})

(d/delete-database cfg)

(d/create-database cfg)

(def conn (d/connect cfg))

;; define both predicates (must be fully-qualified symbols matching the namespace)
(defn is-email? [db eid]
  (if-let [email (:account/email (d/entity db eid))]
    (seq (re-find #"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)" email))
    false))

(defn positive-balance? [db eid]
  (if-let [balance (-> (d/entity db eid) :account/balance)]
    (< 0 balance)
    false))

;; add the person spec
;; Note: 'user/is-email? assumes you're in the 'user namespace (default REPL namespace)
;; If running in a different namespace, use that namespace in the symbol
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
```
