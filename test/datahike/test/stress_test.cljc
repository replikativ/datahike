(ns datahike.test.stress-test
  (:require [datahike.api :as d]
            #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
               :clj  [clojure.test :as t :refer        [is deftest testing]])))

(deftest ^:no-spec stress-test
  (testing "Test lots of parallel reads and writes."
    (let [avet?      true
          num-writes 10000
          num-reads  1000

          schema [{:db/ident       :name
                   :db/cardinality :db.cardinality/one
                   :db/index       true
                   :db/unique      :db.unique/identity
                   :db/valueType   :db.type/string}
                  {:db/ident       :sibling
                   :db/cardinality :db.cardinality/many
                   :db/valueType   :db.type/ref}
                  {:db/ident       :age
                   :db/cardinality :db.cardinality/one
                   :db/index       avet?
                   :db/valueType   :db.type/long}]

          cfg {:store  {:backend :file :path "/tmp/dh-stress"
                        :id #uuid "57e55000-0000-0000-0000-000000000001"
                        :config {:sync-blob? true :in-place? false}}
               :keep-history? false
               :schema-flexibility :read
               :initial-tx []}

          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)

          _ (d/transact conn schema)

          ;; write in parallel and force the transactor to keep flushing
          all-transacts
          (mapv (fn [i]
                  (d/transact! conn {:tx-data [[:db/add (inc i) :age i]]}))
                (shuffle (range num-writes)))]

      ;; read while we are writing
      (dotimes [_ num-reads]
        (d/q '[:find ?e :where [?e :age ?a]]
             @conn))

      ;; wait for ALL transactions, not just the last one
      (run! deref all-transacts)
      (is (= num-writes
             (d/q '[:find (count ?e) .
                    :where
                    [?e :age ?a]]
                  @conn)))
      (d/release conn true))))