(ns datahike.test.entity-spec-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.test.core-test]
   [datahike.api :as d]
   [datahike.query.resolve :as qr]))

#?(:cljs (def Throwable js/Error))

(def cfg-template {:store {:backend :memory
                           :id #uuid "001c0000-0000-0000-0000-00000000001c"}
                   :keep-history? false
                   :schema-flexibility :write})

(def schema-template
  [{:db/ident :account/email
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :account/balance
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(defn setup-db [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))

(deftest test-attribute-assertion
  (let [schema (conj schema-template
                     {:db/ident :account/guard
                      :db.entity/attrs [:account/email :account/balance]})
        valid-account {:account/email "antonia@a.corp"
                       :account/balance 1000}
        invalid-account {:account/email "arthur@b.corp"}
        empty-account {}]
    (letfn [(tx-with-ensure [conn account]
              (d/transact conn [(assoc account :db/ensure :account/guard)]))]
      (testing "with write schema flexibility"
        (let [cfg (-> cfg-template
                      (assoc-in [:store :id] #uuid "a77e0000-0000-0000-0000-000000000001")
                      (assoc :initial-tx schema))
              conn (setup-db cfg)]
          (testing "assert valid account"
            (let [{:keys [db-after]} (tx-with-ensure conn valid-account)]
              (is (= valid-account (d/pull db-after '[:account/email :account/balance] [:account/email (:account/email valid-account)])))))
          (testing "assert invalid account"
            (is (thrown-with-msg? Throwable
                                  #"Entity 5 missing attributes #\{:account/balance\} of spec :account/guard"
                                  (tx-with-ensure conn invalid-account))))
          (testing "assert empty entity"
            (is (thrown-with-msg? Throwable
                                  #"Entity 5 missing attributes #\{:account/balance :account/email\} of spec :account/guard"
                                  (tx-with-ensure conn empty-account))))
          (d/release conn)))
      (testing "with read schema flexibility"
        (let [cfg (-> cfg-template
                      (assoc :schema-flexibility :read)
                      (assoc-in [:store :id] #uuid "a77e0000-0000-0000-0000-000000000002")
                      (assoc :initial-tx schema))
              conn (setup-db cfg)]
          (testing "assert valid account"
            (let [{:keys [db-after]} (tx-with-ensure conn valid-account)]
              (is (= valid-account (d/pull db-after '[:account/email :account/balance] [:account/email (:account/email valid-account)])))))
          (testing "assert invalid account"
            (is (thrown-with-msg? Throwable
                                  #"Entity 5 missing attributes #\{:account/balance\} of spec :account/guard"
                                  (tx-with-ensure conn invalid-account))))
          (testing "assert empty entity"
            (is (thrown-with-msg? Throwable
                                  #"Entity 5 missing attributes #\{:account/balance :account/email\} of spec :account/guard"
                                  (tx-with-ensure conn empty-account))))
          (d/release conn))))))

(defn is-email? [db eid]
  ;; email could not exist
  (if-let [email (:account/email (d/entity db eid))]
    (seq (re-find #"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)" email))
    false))

(defn positive-balance? [db eid]
  ;; balance could not exist
  (if-let [balance (-> (d/entity db eid) :account/balance)]
    (< 0 balance)
    false))

#?(:clj
   (deftest test-predicates-under-the-safe-resolver
     ;; The server runs with the safe resolver set process-wide (with-redefs
     ;; here, since the transaction runs on the writer thread): a predicate
     ;; named by symbol then has to be registered, not loaded from a var.
     (with-redefs [qr/*symbol-resolver* qr/safe-symbol-resolver]
       (let [schema (conj schema-template
                          {:db/ident :account/guard
                           :db.entity/preds ['datahike.test.entity-spec-test/is-email?]})
             cfg (-> cfg-template
                     (assoc-in [:store :id] #uuid "a77e0000-0000-0000-0000-000000000009")
                     (assoc :initial-tx schema))
             conn (setup-db cfg)
             tx (fn [] (d/transact conn {:tx-data [{:account/email "x@a.corp" :account/balance 1
                                                    :db/ensure :account/guard}]}))]
         (testing "an unregistered predicate is refused"
           (is (thrown-with-msg? Throwable #"Unknown entity predicate" (tx))))
         (testing "a registered one runs"
           (qr/register-fn! 'datahike.test.entity-spec-test/is-email? is-email?)
           (try (is (some? (:db-after (tx))))
                (finally (qr/unregister-fn! 'datahike.test.entity-spec-test/is-email?))))
         (d/release conn)))))

(deftest test-predicate-assertion
  (let [schema (conj schema-template
                     {:db/ident :account/guard
                      :db.entity/preds ['datahike.test.entity-spec-test/is-email? 'datahike.test.entity-spec-test/positive-balance?]})
        valid-account {:account/email "greta@a.corp"
                       :account/balance 1000}
        invalid-account {:account/email "georg"
                         :account/balance 500}
        invalid-account-multiple {:account/email "gustav"
                                  :account/balance -500}
        empty-account {}
        cfg (-> cfg-template
                (assoc-in [:store :id] #uuid "a77e0000-0000-0000-0000-000000000003")
                (assoc :initial-tx schema))
        conn (setup-db cfg)]
    (letfn [(tx-with-ensure [account]
              (d/transact conn {:tx-data [(assoc account :db/ensure :account/guard)]}))]
      (testing "assert valid account"
        (let [{:keys [db-after]} (tx-with-ensure valid-account)]
          (is (= valid-account (d/pull db-after '[:account/email :account/balance] [:account/email (:account/email valid-account)])))))
      (testing "assert invalid account with one invalid predicate"
        (is (thrown-with-msg? Throwable
                              #"Entity 5 failed predicates #\{datahike.test.entity-spec-test/is-email\?\} of spec :account/guard"
                              (tx-with-ensure invalid-account))))
      (testing "assert invalid account with mulitple invalid predicates"
        (is (thrown-with-msg? Throwable
                              #"Entity 5 failed predicates #\{datahike.test.entity-spec-test/positive-balance\? datahike.test.entity-spec-test/is-email\?\} of spec :account/guard"
                              (tx-with-ensure invalid-account-multiple))))
      (testing "assert empty account"
        (is (thrown-with-msg? Throwable
                              #"Entity 5 failed predicates #\{datahike.test.entity-spec-test/positive-balance\? datahike.test.entity-spec-test/is-email\?\} of spec :account/guard"
                              (tx-with-ensure empty-account)))))
    (d/release conn)))

(deftest test-attribute-and-predicate-assertion
  (let [schema (conj schema-template
                     {:db/ident :account/guard
                      :db.entity/attrs [:account/email :account/balance]
                      :db.entity/preds ['datahike.test.entity-spec-test/is-email? 'datahike.test.entity-spec-test/positive-balance?]})
        valid-account {:account/email "karla@a.corp"
                       :account/balance 1000}
        invalid-account-attr {:account/email "konstantin@b.corp"}
        invalid-account-pred {:account/email "kaspar"
                              :account/balance 500}
        invalid-account-pred-multiple {:account/email "katharina"
                                       :account/balance -500}
        empty-account {}
        cfg (-> cfg-template
                (assoc-in [:store :id] #uuid "a77e0000-0000-0000-0000-000000000004")
                (assoc :initial-tx schema))
        conn (setup-db cfg)]
    (letfn [(tx-with-ensure [account]
              (d/transact conn {:tx-data [(assoc account :db/ensure :account/guard)]}))]
      (testing "assert valid account"
        (let [{:keys [db-after]} (tx-with-ensure valid-account)]
          (is (= valid-account (d/pull db-after '[:account/email :account/balance] [:account/email (:account/email valid-account)])))))
      (testing "assert invalid account with missing attributes"
        (is (thrown-with-msg? Throwable
                              #"Entity 5 missing attributes #\{:account/balance\} of spec :account/guard"
                              (tx-with-ensure invalid-account-attr))))
      (testing "assert invalid account with one invalid predicate"
        (is (thrown-with-msg? Throwable
                              #"Entity 5 failed predicates #\{datahike.test.entity-spec-test/is-email\?\} of spec :account/guard"
                              (tx-with-ensure invalid-account-pred))))
      (testing "assert invalid account with mulitple invalid predicates"
        (is (thrown-with-msg? Throwable
                              #"Entity 5 failed predicates #\{datahike.test.entity-spec-test/positive-balance\? datahike.test.entity-spec-test/is-email\?\} of spec :account/guard"
                              (tx-with-ensure invalid-account-pred-multiple))))
      (testing "assert empty account with required attributes precidenting over predicates"
        (is (thrown-with-msg? Throwable
                              #"Entity 5 missing attributes #\{:account/balance :account/email\} of spec :account/guard"
                              (tx-with-ensure empty-account)))))
    (d/release conn)))
