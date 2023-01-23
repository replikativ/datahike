(ns datahike.norm.norm-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.string :as s]
            [datahike.api :as d]
            [datahike.norm :as sut]))

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
        test-fn (fn [conn]
                  (-> (for [[eid value] (d/q '[:find ?e ?v
                                               :where
                                               [?e :foo ?v]]
                                             (d/db conn))]
                        [:db/add eid
                         :foo (s/upper-case value)])
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
        tx-fn (fn [conn]
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

(comment
  (def conn (create-test-db))
  (sut/ensure-norms! conn "test/resources")
  (d/transact conn {:tx-data [{:character/name "Homer Simpson"}
                              {:character/name "Marge Simpson"}]})
  (def margehomer (-> (d/q '[:find [?e ...]
                             :where
                             [?e :character/name]]
                           (d/db conn))))
  (d/transact conn {:tx-data [{:db/doc "Simpsons children reference"
                               :db/ident :character/child
                               :db/valueType :db.type/ref
                               :db/cardinality :db.cardinality/many}]})
  (d/transact conn (-> (for [[eid] (d/q '[:find ?e
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

  (d/pull-many (d/db conn) '[:character/name {:character/child [:character/name]}] margehomer))
