(ns datahike.norm.norm-test
  (:require [clojure.test :refer [deftest is]]
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

(def ensure-norms #'sut/ensure-norms)

(deftest simple-test
  (let [conn (create-test-db)
        _ (ensure-norms conn "test/datahike/norm/resources/001-a1-example.edn")
        _ (ensure-norms conn "test/datahike/norm/resources/002-a2-example.edn")
        schema (d/schema (d/db conn))]
    (is (= #:db{:valueType :db.type/string, :cardinality :db.cardinality/one, :doc "Place of occupation", :ident :character/place-of-occupation}
           (-> (schema :character/place-of-occupation)
               (dissoc :db/id))))
    (is (= #:db{:valueType :db.type/string, :cardinality :db.cardinality/one, :doc "Simpsons character name", :ident :character/name, :db/unique :db.unique/identity}
           (-> (schema :character/name)
               (dissoc :db/id))))
    (is (= #:db{:ident :tx/norm, :valueType :db.type/keyword, :cardinality :db.cardinality/one}
           (-> (schema :tx/norm)
               (dissoc :db/id))))))

(comment
  (def conn (create-test-db))
  (ensure-norms conn "test/datahike/norm/resources/001-a1-example.edn")
  (ensure-norms conn "test/datahike/norm/resources/002-a2-example.edn")
  (def schema (d/schema (d/db conn))))


(defn tx-fn-test-fn [conn]
  (-> (for [[eid value] (d/q '[:find ?e ?v
                               :where
                               [?e :character/place-of-occupation ?v]]
                             (d/db conn))]
        [:db/add eid
         :character/place-of-occupation (string/lower-case value)])
      vec))

(deftest tx-fn-test
  (let [conn (create-test-db)
        _ (ensure-norms conn "test/datahike/norm/resources/001-a1-example.edn")
        _ (ensure-norms conn "test/datahike/norm/resources/002-a2-example.edn")
        _ (d/transact conn {:tx-data [{:character/place-of-occupation "SPRINGFIELD ELEMENTARY SCHOOL"}
                                      {:character/place-of-occupation "SPRINGFIELD NUCLEAR POWER PLANT"}]})
        _ (ensure-norms conn "test/datahike/norm/resources/003-tx-fn-test.edn")]
    (is (= #{["springfield elementary school"] ["springfield nuclear power plant"]}
           (d/q '[:find ?v
                  :where
                  [_ :character/place-of-occupation ?v]]
                (d/db conn))))))

(defn tx-data-and-tx-fn-test-fn [conn]
  (-> (for [[eid]
            (d/q '[:find ?e
                   :where
                   [?e :character/name]
                   (or-join [?e]
                            [?e :character/name "Homer Simpson"]
                            [?e :character/name "Marge Simpson"])]
                 (d/db conn))]
        {:db/id eid
         :character/children [[:character/name "Bart Simpson"]
                              [:character/name "Lisa Simpson"]
                              [:character/name "Maggie Simpson"]]})
      vec))

(deftest tx-data-and-tx-fn-test
  (let [conn (create-test-db)
        _ (ensure-norms conn "test/datahike/norm/resources/001-a1-example.edn")
        _ (ensure-norms conn "test/datahike/norm/resources/002-a2-example.edn")
        _ (ensure-norms conn "test/datahike/norm/resources/003-tx-fn-test.edn")
        _ (d/transact conn {:tx-data [{:character/name "Homer Simpson"}
                                      {:character/name "Marge Simpson"}
                                      {:character/name "Bart Simpson"}
                                      {:character/name "Lisa Simpson"}
                                      {:character/name "Maggie Simpson"}]})
        _ (ensure-norms conn "test/datahike/norm/resources/004-tx-data-and-tx-fn-test.edn")
        margehomer (d/q '[:find [?e ...]
                          :where
                          [?e :character/name]
                          (or-join [?e]
                                   [?e :character/name "Homer Simpson"]
                                   [?e :character/name "Marge Simpson"])]
                        (d/db conn))]
    (is (= [#:character{:name "Homer Simpson",
                        :children
                        [#:character{:name "Bart Simpson"}
                         #:character{:name "Lisa Simpson"}
                         #:character{:name "Maggie Simpson"}]}
            #:character{:name "Marge Simpson",
                        :children
                        [#:character{:name "Bart Simpson"}
                         #:character{:name "Lisa Simpson"}
                         #:character{:name "Maggie Simpson"}]}]
           (d/pull-many (d/db conn) '[:character/name {:character/children [:character/name]}] margehomer)))))

(defn naming-and-sorting-test-fn [conn]
  (-> (for [[eid] (d/q '[:find ?e
                         :where
                         [?e :character/name]
                         (or-join [?e]
                                  [?e :character/name "Bart Simpson"]
                                  [?e :character/name "Lisa Simpson"])]
                       (d/db conn))]
        {:db/id eid
         :character/occupation :student})
      vec))

(deftest naming-and-sorting-test
  (let [conn (create-test-db)
        _ (sut/ensure-norms! conn "test/datahike/norm/resources")
        lisabart (d/q '[:find [?e ...]
                        :where
                        [?e :character/occupation :student]]
                      (d/db conn))]
    (is (= [{:db/id 10,
             :character/name "Bart Simpson",
             :character/occupation :student}
            {:db/id 11,
             :character/name "Lisa Simpson",
             :character/occupation :student}]
           (d/pull-many (d/db conn) '[*] lisabart)))))
