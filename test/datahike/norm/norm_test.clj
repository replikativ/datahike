(ns datahike.norm.norm-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as string]
            [datahike.api :as d]
            [datahike.norm.norm :as sut]))

(defn create-test-db []
  (let [id (apply str
                  (for [_i (range 8)]
                    (char (+ (rand 26) 65))))]
    (d/create-database {:store {:backend :mem
                                :id id}})
    (d/connect {:store {:backend :mem
                        :id id}})))

(deftest simple-test
  (let [conn (create-test-db)
        _ (sut/ensure-norms! conn "test/datahike/norm/resources")]
    (is (= #:db{:valueType :db.type/string, :cardinality :db.cardinality/one, :doc "foo", :ident :foo}
           (-> (d/schema (d/db conn))
               :foo
               (dissoc :db/id))))
    (is (= #:db{:valueType :db.type/string, :cardinality :db.cardinality/one, :doc "Simpsons character name", :ident :character/name}
           (-> (d/schema (d/db conn))
               :character/name
               (dissoc :db/id))))
    (is (= #:db{:ident :tx/norm, :valueType :db.type/keyword, :cardinality :db.cardinality/one}
           (-> (d/schema (d/db conn))
               :tx/norm
               (dissoc :db/id))))))

(deftest tx-fn-test
  (let [conn (create-test-db)
        _ (sut/ensure-norms! conn "test/datahike/norm/resources")
        _ (d/transact conn {:tx-data [{:foo "upper-case"}
                                      {:foo "Grossbuchstaben"}]})
        test-fn '(fn [conn]
                   (-> (for [[eid value] (d/q '[:find ?e ?v
                                                :where
                                                [?e :foo ?v]]
                                              (d/db conn))]
                         [:db/add eid
                          :foo (string/upper-case value)])
                       vec))
        test-norm [{:norm :test-norm-1,
                    :tx-fn test-fn}]
        _ (sut/ensure-norms! conn test-norm)]
    (is (= #{["GROSSBUCHSTABEN"] ["UPPER-CASE"]}
           (d/q '[:find ?v
                  :where
                  [_ :foo ?v]]
                (d/db conn))))))

(deftest tx-and-fn-test
  (let [conn (create-test-db)
        _ (sut/ensure-norms! conn "test/datahike/norm/resources")
        _ (d/transact conn {:tx-data [{:character/name "Homer Simpson"}
                                      {:character/name "Marge Simpson"}]})
        margehomer (d/q '[:find [?e ...]
                          :where
                          [?e :character/name]]
                        (d/db conn))
        tx-data [{:db/doc "Simpsons children reference"
                  :db/ident :character/child
                  :db/valueType :db.type/ref
                  :db/cardinality :db.cardinality/many}]
        tx-fn '(fn [conn]
                 (-> (for [[eid] (d/q '[:find ?e
                                        :where
                                        [?e :character/name]
                                        (or-join [?e]
                                                 [?e :character/name "Homer Simpson"]
                                                 [?e :character/name "Marge Simpson"])]
                                      (d/db conn))]
                       {:db/id eid
                        :character/child [{:character/name "Bart Simpson"}
                                          {:character/name "Lisa Simpson"}
                                          {:character/name "Maggie Simpson"}]})
                     vec))
        test-norm [{:norm :test-norm-2
                    :tx-data tx-data
                    :tx-fn tx-fn}]]
    (sut/ensure-norms! conn test-norm)
    (is (= [#:character{:name "Marge Simpson",
                        :child
                        [#:character{:name "Bart Simpson"}
                         #:character{:name "Lisa Simpson"}
                         #:character{:name "Maggie Simpson"}]}
            #:character{:name "Homer Simpson",
                        :child
                        [#:character{:name "Bart Simpson"}
                         #:character{:name "Lisa Simpson"}
                         #:character{:name "Maggie Simpson"}]}]
           (d/pull-many (d/db conn) '[:character/name {:character/child [:character/name]}] margehomer)))))

(defn baz-test-fn-1 [_conn]
  [{:baz "baz"}])

(defn baz-test-fn-2 [conn]
  (-> (for [[eid value] (d/q '[:find ?e ?v
                               :where
                               [?e :baz ?v]]
                             (d/db conn))]
        [:db/add eid
         :baz (-> (string/replace value #" " "_")
                  keyword)])
      vec))

(deftest naming-and-sorting-test
  (let [conn (create-test-db)
        _ (sut/ensure-norms! conn "test/datahike/norm/resources")]
    (testing "updated schema with docstring"
      (is (= "baz"
             (-> (d/schema (d/db conn))
                 :baz
                 :db/doc))))
    (testing "all bazes keywordized"
      (is (= :baz
             (d/q '[:find ?v
                    :where
                    [_ :baz ?v]]
                  (d/db conn)))))))

(comment
  (def conn (create-test-db))
  (def norm-list (sut/read-norm-files! "test/datahike/norm/resources/0001 a3 example.edn"))
  (sut/ensure-norms! conn "test/datahike/norm/resources/0002-a4-example.edn")
  (d/schema (d/db conn))
  (d/datoms (d/db conn) :eavt)
  (d/transact conn [{:baz "baz"}])
  (d/q '[:find ?e ?a ?v
         :where
         [?e ?a ?v]]
       (d/db conn))
  (d/transact conn {:tx-data (vec (concat [{:tx/norm :bazbaz}]
                                          [{:baz "baz"}]))})

  ((eval 'datahike.norm.norm-test/baz-test-fn-1) conn))
