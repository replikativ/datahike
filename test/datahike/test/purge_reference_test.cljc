(ns datahike.test.purge-reference-test
  (:require #?(:cljs [cljs.test :refer-macros [deftest is]]
               :clj [clojure.test :refer [deftest is]])
            [datahike.api :as d]
            [datahike.db :as db]))

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
