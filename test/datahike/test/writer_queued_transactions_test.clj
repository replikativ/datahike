(ns datahike.test.writer-queued-transactions-test
  "Transactions queued on an exclusive writer.

   An exclusive writer applies the next queued transaction before the commit of
   the one before it has been published. Each still has to get its own
   transaction id, in the order the caller submitted them: a shared id puts two
   versions of a value into one transaction, and no historical view can
   separate them."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate.fs :as fs]
            [datahike.test.utils :as du]
            [datahike.writer :as writer]))

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

(defn- name-id [db]
  (d/q '[:find ?e . :where [?e :name "first"]] db))

(deftest a-prepared-transaction-sees-the-queued-one-before-it
  (let [conn (du/setup-db {:keep-history? true
                           :writer {:backend :self :writer-ownership :exclusive}})
        prepare (writer/prepare-tx-data
                 (fn [db]
                   (let [eid (name-id db)]
                     (when-not eid (throw (ex-info "Preceding insert is absent" {})))
                     [[:db/add eid :name "second"]])))]
    (try
      (let [first-write (d/transact! conn [[:db/add -1 :name "first"]])
            second-write (d/transact! conn prepare)
            first-tx (get-in @first-write [:tempids :db/current-tx])
            second-tx (get-in @second-write [:tempids :db/current-tx])
            eid (name-id (d/as-of @conn first-tx))]
        (is (< first-tx second-tx))
        (is (some? eid))
        (is (= "second" (:name (d/entity (d/as-of @conn second-tx) eid)))))
      (finally (du/teardown-db conn)))))

(deftest preparation-may-stage-speculative-transactions
  (let [cfg {:keep-history? true
             :writer {:backend :self :writer-ownership :exclusive}}
        ;; stages one write on the database it is given, reads the staged
        ;; result, and returns operations derived from it
        staged (fn [db]
                 (let [eid (name-id db)
                       spec (d/db-with db [[:db/add eid :name "staged"]])]
                   [[:db/add eid :name (str (:name (d/entity spec eid)) "-applied")]]))]
    (testing "the preparation hook hands over an immutable database"
      (let [conn (du/setup-db cfg)]
        (try
          (let [first-write (d/transact! conn [[:db/add -1 :name "first"]])
                second-write (d/transact! conn (writer/prepare-tx-data staged))
                eid (get-in @first-write [:tempids -1])]
            @second-write
            (is (= "staged-applied" (:name (d/entity @conn eid)))))
          (finally (du/teardown-db conn)))))
    (testing "a transaction function gets the database mid-transaction and cannot"
      (let [conn (du/setup-db cfg)]
        (try
          (d/transact conn [[:db/add -1 :name "first"]])
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Expected persistent set"
                                (d/transact conn [[:db.fn/call staged]])))
          (finally (du/teardown-db conn)))))))

(deftest preparation-requires-an-exclusive-local-writer
  (let [shared (du/setup-db)]
    (try
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"exclusive local writer"
                            (d/transact! shared (writer/prepare-tx-data (constantly [])))))
      (finally (du/teardown-db shared)))))
