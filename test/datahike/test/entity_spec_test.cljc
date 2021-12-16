(ns datahike.test.entity-spec-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing use-fixtures]])
   [datahike.test.core-test]
   [datahike.api :as d]))

(def cfg-template {:store {:backend :mem
                           :id "entity-spec"}
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
                      (assoc-in [:store :id] "attribute-assertion-write")
                      (assoc :initial-tx schema))
              conn (setup-db cfg)]
          (testing "assert valid account"
            (let [{:keys [db-after]} (tx-with-ensure conn valid-account)]
              (is (= valid-account (d/pull db-after '[:account/email :account/balance] [:account/email (:account/email valid-account)])))))
          (testing "assert invalid account"
            (is (thrown-msg?
                 "Entity 5 missing attributes #{:account/balance} of spec :account/guard"
                 (tx-with-ensure conn invalid-account))))
          (testing "assert empty entity"
            (is (thrown-msg?
                 "Entity 5 missing attributes #{:account/balance :account/email} of spec :account/guard"
                 (tx-with-ensure conn empty-account))))))
      (testing "with read schema flexibility"
        (let [cfg (-> cfg-template
                      (assoc :schema-flexibility :read)
                      (assoc-in [:store :id] "attribute-assertion-read")
                      (assoc :initial-tx schema))
              conn (setup-db cfg)]
          (testing "assert valid account"
            (let [{:keys [db-after]} (tx-with-ensure conn valid-account)]
              (is (= valid-account (d/pull db-after '[:account/email :account/balance] [:account/email (:account/email valid-account)])))))
          (testing "assert invalid account"
            (is (thrown-msg?
                 "Entity 5 missing attributes #{:account/balance} of spec :account/guard"
                 (tx-with-ensure conn invalid-account))))
          (testing "assert empty entity"
            (is (thrown-msg?
                 "Entity 5 missing attributes #{:account/balance :account/email} of spec :account/guard"
                 (tx-with-ensure conn empty-account)))))))))

(defn is-email? [db eid]
  ;; email could not exist
  (if-let [email (:account/email (d/entity db eid))]
    (-> (re-find #"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)" email) empty? not)
    false))

(defn positive-balance? [db eid]
  ;; balance could not exist
  (if-let [balance (-> (d/entity db eid) :account/balance)]
    (< 0 balance)
    false))

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
                (assoc-in [:store :id] "predicate-assertion")
                (assoc :initial-tx schema))
        conn (setup-db cfg)]
    (letfn [(tx-with-ensure [account]
              (d/transact conn {:tx-data [(assoc account :db/ensure :account/guard)]}))]
      (testing "assert valid account"
        (let [{:keys [db-after]} (tx-with-ensure valid-account)]
          (is (= valid-account (d/pull db-after '[:account/email :account/balance] [:account/email (:account/email valid-account)])))))
      (testing "assert invalid account with one invalid predicate"
        (is (thrown-msg?
             "Entity 5 failed predicates #{datahike.test.entity-spec-test/is-email?} of spec :account/guard"
             (tx-with-ensure invalid-account))))
      (testing "assert invalid account with mulitple invalid predicates"
        (is (thrown-msg?
             "Entity 5 failed predicates #{datahike.test.entity-spec-test/positive-balance? datahike.test.entity-spec-test/is-email?} of spec :account/guard"
             (tx-with-ensure invalid-account-multiple))))
      (testing "assert empty account"
        (is (thrown-msg?
             "Entity 5 failed predicates #{datahike.test.entity-spec-test/positive-balance? datahike.test.entity-spec-test/is-email?} of spec :account/guard"
             (tx-with-ensure empty-account)))))))

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
                (assoc-in [:store :id] "predicate-attribute-assertion")
                (assoc :initial-tx schema))
        conn (setup-db cfg)]
    (letfn [(tx-with-ensure [account]
              (d/transact conn {:tx-data [(assoc account :db/ensure :account/guard)]}))]
      (testing "assert valid account"
        (let [{:keys [db-after]} (tx-with-ensure valid-account)]
          (is (= valid-account (d/pull db-after '[:account/email :account/balance] [:account/email (:account/email valid-account)])))))
      (testing "assert invalid account with missing attributes"
        (is (thrown-msg?
             "Entity 5 missing attributes #{:account/balance} of spec :account/guard"
             (tx-with-ensure invalid-account-attr))))
      (testing "assert invalid account with one invalid predicate"
        (is (thrown-msg?
             "Entity 5 failed predicates #{datahike.test.entity-spec-test/is-email?} of spec :account/guard"
             (tx-with-ensure invalid-account-pred))))
      (testing "assert invalid account with mulitple invalid predicates"
        (is (thrown-msg?
             "Entity 5 failed predicates #{datahike.test.entity-spec-test/positive-balance? datahike.test.entity-spec-test/is-email?} of spec :account/guard"
             (tx-with-ensure invalid-account-pred-multiple))))
      (testing "assert empty account with required attributes precidenting over predicates"
        (is (thrown-msg?
             "Entity 5 missing attributes #{:account/balance :account/email} of spec :account/guard"
             (tx-with-ensure empty-account)))))))
