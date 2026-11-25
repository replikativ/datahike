(ns datahike.test.writer-queued-transactions-test
  "Transactions queued on an exclusive writer.

   An exclusive writer applies the next queued transaction before the commit of
   the one before it has been published. Each still has to get its own
   transaction id, in the order the caller submitted them: a shared id puts two
   versions of a value into one transaction, and no historical view can
   separate them."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate.fs :as fs]))

(deftest queued-transactions-keep-their-own-ids-in-submission-order
  (doseq [store [{:backend :memory :id (random-uuid)}
                 {:backend :file :id (random-uuid)
                  :path (fs/temp-store-path! "dh-queued-tx-")}]]
    (testing (str (:backend store) " store")
      (let [cfg {:store store
                 :keep-history? true
                 :schema-flexibility :write
                 :writer {:backend :self :writer-ownership :exclusive}}
            _ (d/create-database cfg)
            conn (d/connect cfg)]
        (d/transact conn [{:db/ident :counter/value
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (let [e (get-in (d/transact conn [{:db/id -1 :counter/value 0}]) [:tempids -1])
              ;; submitted without waiting: each overwrites the one before
              pending (mapv #(d/transact! conn [{:db/id e :counter/value %}]) (range 1 21))
              txs (mapv #(get-in @% [:tempids :db/current-tx]) pending)]
          (is (apply < txs) "one id per transaction, ascending in submission order")
          (is (= 20 (:counter/value (d/pull @conn [:counter/value] e)))
              "the last submitted value is the current one")
          (d/release conn)
          (let [conn (d/connect cfg)
                db @conn]
            (testing "after reopening, every version is its own point in history"
              (is (= (mapv vector (range 1 21) txs)
                     (sort (d/q '[:find ?v ?t :in $ ?e
                                  :where [?e :counter/value ?v ?t true] [(pos? ?v)]]
                                (d/history db) e))))
              (is (= (vec (range 1 21))
                     (mapv #(:counter/value (d/pull (d/as-of db %) [:counter/value] e)) txs))))
            (d/release conn)))
        (d/delete-database cfg)))))
