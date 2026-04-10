(ns datahike.test.query-cache-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]))

(defn- with-temp-db
  "Create a temp in-memory db with schema, run f with the connection, then clean up."
  [schema-txs f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :attribute-refs? false}
        _ (d/create-database cfg)
        conn (d/connect cfg)]
    (try
      (d/transact conn schema-txs)
      (f conn)
      (finally
        (d/release conn)
        (d/delete-database cfg)))))

(def ^:private label-schema
  [{:db/ident :c/id
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :c/labels
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}
   {:db/ident :c/note
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}])

(deftest test-pull-only-attr-retract-invalidates-cache
  (testing "Core bug: retract on attr only in pull pattern must invalidate cache"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/labels ["a" "b"]}])
        ;; Populate cache
        (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
               :where [?c :c/id]]
             @conn)
        ;; Retract one label
        (let [tx (d/transact conn [[:db/retract [:c/id "t1"] :c/labels "a"]])]
          (is (= ["b"]
                 (:c/labels (first (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
                                          :where [?c :c/id]]
                                        (:db-after tx)))))))))))

(deftest test-pull-only-attr-assert-invalidates-cache
  (testing "Adding a value to an attr only in pull pattern must show in results"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/labels ["a"]}])
        ;; Populate cache
        (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
               :where [?c :c/id]]
             @conn)
        ;; Add a label
        (let [tx (d/transact conn [{:c/id "t1" :c/labels ["b"]}])]
          (is (= #{"a" "b"}
                 (set (:c/labels (first (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
                                               :where [?c :c/id]]
                                             (:db-after tx))))))))))))

(deftest test-pull-only-attr-update-cardinality-one
  (testing "Updating a cardinality-one attr only in pull must show new value"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/note "old"}])
        ;; Populate cache
        (d/q '[:find [(pull ?c [:c/id :c/note]) ...]
               :where [?c :c/id]]
             @conn)
        ;; Update note
        (let [tx (d/transact conn [{:c/id "t1" :c/note "new"}])]
          (is (= "new"
                 (:c/note (first (d/q '[:find [(pull ?c [:c/id :c/note]) ...]
                                        :where [?c :c/id]]
                                      (:db-after tx)))))))))))

(deftest test-wildcard-pull-invalidates-on-any-change
  (testing "Wildcard pull [*] must invalidate when any attr changes"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/note "old"}])
        ;; Populate cache with wildcard pull
        (d/q '[:find [(pull ?c [*]) ...]
               :where [?c :c/id]]
             @conn)
        ;; Update note
        (let [tx (d/transact conn [{:c/id "t1" :c/note "new"}])]
          (is (= "new"
                 (:c/note (first (d/q '[:find [(pull ?c [*]) ...]
                                        :where [?c :c/id]]
                                      (:db-after tx)))))))))))

(deftest test-variable-pull-pattern-invalidates-on-any-change
  (testing "Variable pull pattern bound via :in must invalidate conservatively"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/note "old"}])
        ;; Populate cache with variable pattern
        (d/q '[:find [(pull ?c ?pattern) ...]
               :in $ ?pattern
               :where [?c :c/id]]
             @conn [:c/id :c/note])
        ;; Update note
        (let [tx (d/transact conn [{:c/id "t1" :c/note "new"}])]
          (is (= "new"
                 (:c/note (first (d/q '[:find [(pull ?c ?pattern) ...]
                                        :in $ ?pattern
                                        :where [?c :c/id]]
                                      (:db-after tx) [:c/id :c/note]))))))))))

(deftest test-where-attr-change-still-invalidates
  (testing "Changes to attrs in :where clauses still invalidate correctly"
    (with-temp-db label-schema
      (fn [conn]
        (d/transact conn [{:c/id "t1" :c/labels ["a" "b"]}
                          {:c/id "t2" :c/labels ["x"]}])
        ;; Query with :c/id in both :where and pull
        (is (= 2 (count (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
                               :where [?c :c/id]]
                             @conn))))
        ;; Retract an entity's :c/id — affects :where clause
        (let [tx (d/transact conn [[:db/retract [:c/id "t2"] :c/id "t2"]])]
          (is (= 1 (count (d/q '[:find [(pull ?c [:c/id :c/labels]) ...]
                                 :where [?c :c/id]]
                               (:db-after tx))))))))))
