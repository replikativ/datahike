(ns datahike.test.ident
  (:require
    [clojure.test :as t :refer [is deftest]]
    [datahike.test.core :as tdc]
    [datahike.core :as d]))

(def db
  (let [db   (d/empty-db {:ref {:db/valueType :db.type/ref}})]
    (d/db-with db [[:db/add tdc/e1 :db/ident :ent1]
                   [:db/add tdc/e2 :db/ident :ent2]
                   [:db/add tdc/e2 :ref tdc/e1]])))

(deftest test-q
  (is (= tdc/e1 (d/q '[:find ?v .
                  :where [:ent2 :ref ?v]] db)))
  (is (= tdc/e2 (d/q '[:find ?f .
                  :where [?f :ref :ent1]] db))))

(deftest test-transact!
  (let [db' (d/db-with db [[:db/add :ent1 :ref :ent2]])]
    (is (= tdc/e2 (-> (d/entity db' :ent1) :ref :db/id)))))

(deftest test-entity
  (is (= {:db/ident :ent1}
         (into {} (d/entity db :ent1)))))

(deftest test-pull
  (is (= {:db/id tdc/e1, :db/ident :ent1}
         (d/pull db '[*] :ent1))))

#_(user/test-var #'test-transact!)
#_(t/test-ns 'datahike.test.ident)
