(ns datahike.kabel.writer-test
  "Unit tests for KabelWriter."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.kabel.writer :as kw]
            [datahike.api :as d]
            [datahike.query :as dq]
            [datahike.writing :as dw]
            [datahike.writer :as writer]
            [clojure.core.async :refer [<!! timeout alts!! promise-chan]]))

(def test-peer-id #uuid "10000000-0000-0000-0000-000000000001")
(def test-store-id #uuid "20000000-0000-0000-0000-000000000002")

(deftest test-kabel-writer-construction
  (testing "kabel-writer creates proper instance"
    (let [w (kw/kabel-writer test-peer-id test-store-id nil)]
      (is (instance? datahike.kabel.writer.KabelWriter w))
      (is (= test-peer-id (:peer-id w)))
      (is (= test-store-id (:store-id w)))
      (is (= {} @(:pending-txs w)))
      (is (= 0 @(:current-max-tx w)))
      (is (= #{} @(:listeners w))))))

(deftest test-create-writer-multimethod
  (testing "create-writer :kabel dispatches correctly"
    (let [config {:backend :kabel
                  :peer-id test-peer-id
                  :store-config {:id test-store-id}}
          w (writer/create-writer config nil)]
      (is (instance? datahike.kabel.writer.KabelWriter w))
      (is (= test-peer-id (:peer-id w)))
      (is (= test-store-id (:store-id w))))))

(deftest test-on-sync-update
  (testing "on-sync-update! resolves pending transactions"
    (let [w (kw/kabel-writer test-peer-id test-store-id nil)
          wait-ch (promise-chan)]

      ;; Simulate a pending transaction waiting for max-tx 100
      (swap! (:pending-txs w) assoc ::one
             {:expected-max-tx 100
              :tx-report {:db-after {:max-tx 100}}
              :ch wait-ch})

      ;; Sync update with max-tx 100 should resolve
      (kw/on-sync-update! w 100)

      ;; Check that wait-ch received :synced
      (let [[result ch] (alts!! [wait-ch (timeout 1000)])]
        (is (= wait-ch ch) "wait-ch should receive value")
        (is (= :synced result)))

      ;; current-max-tx should be updated
      (is (= 100 @(:current-max-tx w)))))

  (testing "on-sync-update! resolves multiple pending transactions"
    (let [w (kw/kabel-writer test-peer-id test-store-id nil)
          wait-ch-1 (promise-chan)
          wait-ch-2 (promise-chan)
          wait-ch-3 (promise-chan)]

      ;; Simulate multiple pending transactions
      (swap! (:pending-txs w) assoc
             ::one {:expected-max-tx 100 :tx-report {:db-after {:max-tx 100}} :ch wait-ch-1}
             ::two {:expected-max-tx 101 :tx-report {:db-after {:max-tx 101}} :ch wait-ch-2}
             ::three {:expected-max-tx 105 :tx-report {:db-after {:max-tx 105}} :ch wait-ch-3})

      ;; Sync update with max-tx 103 should resolve 100 and 101, not 105
      (kw/on-sync-update! w 103)

      ;; 100 and 101 should be resolved
      (is (= :synced (<!! wait-ch-1)))
      (is (= :synced (<!! wait-ch-2)))

      ;; 105 should not be resolved yet
      (let [[_ ch] (alts!! [wait-ch-3 (timeout 100)])]
        (is (not= wait-ch-3 ch) "wait-ch-3 should timeout, not receive value"))

      ;; Now sync to 105
      (kw/on-sync-update! w 105)
      (is (= :synced (<!! wait-ch-3))))))

(deftest same-watermark-keeps-every-waiter
  (let [w (kw/kabel-writer test-peer-id test-store-id nil)
        wait-ch-1 (promise-chan)
        wait-ch-2 (promise-chan)]
    ;; A server-side writer batch gives every per-call report the batch's
    ;; final max-tx. Correlation therefore cannot use max-tx as a map key.
    (swap! (:pending-txs w) assoc
           ::one {:expected-max-tx 100 :tx-report {:tx-data [:a]} :ch wait-ch-1}
           ::two {:expected-max-tx 100 :tx-report {:tx-data [:b]} :ch wait-ch-2})
    (kw/on-sync-update! w 100)
    (is (= :synced (<!! wait-ch-1)))
    (is (= :synced (<!! wait-ch-2)))
    (is (= #{::one ::two} (set (keys @(:pending-txs w)))))))

(deftest synced-snapshot-does-not-propagate-an-incomplete-query-cache
  (let [config {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :write}
        _ (d/create-database config)
        conn (d/connect config)
        _ (<!! (d/transact! conn
                            [{:db/ident :cache/a :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}
                             {:db/ident :cache/b :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}
                             {:db/id 1 :cache/a "old-a" :cache/b "old-b"}]))
        old-db @conn
        original-writer (:writer @(:wrapped-atom conn))
        own-report (d/with old-db [[:db/add 1 :cache/a "new-a"]])
        foreign-report (d/with (:db-after own-report)
                               [[:db/add 1 :cache/b "new-b"]])
        final-db (:db-after foreign-report)
        kabel-writer (kw/kabel-writer test-peer-id test-store-id nil)]
    (try
      (dq/clear-query-cache!)
      ;; Seed a result that is unaffected by our pending report but invalidated
      ;; by the foreign transaction folded into the same sync snapshot.
      (is (= "old-b" (d/q '[:find ?v . :where [_ :cache/b ?v]] old-db)))
      (swap! (:pending-txs kabel-writer) assoc ::own
             {:expected-max-tx (get-in own-report [:db-after :max-tx])
              :tx-report own-report
              :ch (promise-chan)})
      (swap! (:wrapped-atom conn) assoc :writer kabel-writer)
      (kw/set-connection! kabel-writer conn)
      (with-redefs [dw/stored->db (fn [_ _] final-db)]
        (kw/on-db-sync! conn ::stored))
      (is (= "new-b" (d/q '[:find ?v . :where [_ :cache/b ?v]] @conn)))
      (finally
        (dq/clear-query-cache!)
        (swap! (:wrapped-atom conn) assoc :writer original-writer)
        (d/release conn)
        (d/delete-database config)))))

(deftest test-listener-management
  (testing "add-listener! and remove-listener!"
    (let [w (kw/kabel-writer test-peer-id test-store-id nil)
          callback-1 (fn [_])
          callback-2 (fn [_])]

      ;; Add listeners
      (kw/add-listener! w callback-1)
      (is (= #{callback-1} @(:listeners w)))

      (kw/add-listener! w callback-2)
      (is (= #{callback-1 callback-2} @(:listeners w)))

      ;; Remove listener
      (kw/remove-listener! w callback-1)
      (is (= #{callback-2} @(:listeners w)))

      (kw/remove-listener! w callback-2)
      (is (= #{} @(:listeners w))))))

(deftest test-shutdown
  (testing "-shutdown cancels pending transactions"
    (let [w (kw/kabel-writer test-peer-id test-store-id nil)
          wait-ch-1 (promise-chan)
          wait-ch-2 (promise-chan)]

      ;; Add pending transactions
      (swap! (:pending-txs w) assoc
             ::one {:expected-max-tx 100 :tx-report {:db-after {:max-tx 100}} :ch wait-ch-1}
             ::two {:expected-max-tx 101 :tx-report {:db-after {:max-tx 101}} :ch wait-ch-2})

      ;; Shutdown
      (let [shutdown-ch (writer/-shutdown w)]
        (is (true? (<!! shutdown-ch))))

      ;; Pending txs should be cleared
      (is (= {} @(:pending-txs w)))

      ;; Wait channels should receive shutdown errors
      (let [result-1 (<!! wait-ch-1)
            result-2 (<!! wait-ch-2)]
        (is (instance? clojure.lang.ExceptionInfo result-1))
        (is (= :writer-shutdown (:type (ex-data result-1))))
        (is (instance? clojure.lang.ExceptionInfo result-2))))))

(deftest test-streaming
  (testing "-streaming? returns true"
    (let [w (kw/kabel-writer test-peer-id test-store-id nil)]
      (is (true? (writer/streaming? w)))
      (is (false? (writer/refresh-on-deref? w))
          "konserve-sync keeps the connection current"))))
