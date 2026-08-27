(ns datahike.kabel.writer-test
  "Unit tests for KabelWriter."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.kabel.writer :as kw]
            [datahike.kabel.handlers :as handlers]
            [datahike.api :as d]
            [datahike.query :as dq]
            [datahike.writing :as dw]
            [datahike.writer :as writer]
            [datahike.connections :as connections]
            [is.simm.distributed-scope :as ds]
            [clojure.core.async :refer [<!! go timeout alts!! promise-chan]]))

(def test-peer-id #uuid "10000000-0000-0000-0000-000000000001")
(def test-store-id #uuid "20000000-0000-0000-0000-000000000002")

(deftest test-kabel-writer-construction
  (testing "kabel-writer creates proper instance"
    (let [w (kw/kabel-writer test-peer-id test-store-id nil)]
      (is (instance? datahike.kabel.writer.KabelWriter w))
      (is (= test-peer-id (:peer-id w)))
      (is (= test-store-id (:store-id w)))
      (is (= :db (:branch w)))
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
      (is (= test-store-id (:store-id w)))
      (is (= :db (:branch w))))))

(deftest branch-aware-store-registry
  (let [original @handlers/store-registry
        main-conn {:wrapped-atom (atom {:config {:branch :db}})}
        fork-conn {:wrapped-atom (atom {:config {:branch :fork}})}]
    (try
      (reset! handlers/store-registry {})
      (handlers/register-connection-for-store! test-store-id main-conn :main-peer)
      (handlers/register-connection-for-store! test-store-id fork-conn :fork-peer)
      (is (identical? main-conn
                      (handlers/get-connection-for-store test-store-id :db)))
      (is (identical? fork-conn
                      (handlers/get-connection-for-store test-store-id :fork)))
      (is (= :fork-peer (handlers/get-peer-for-store test-store-id :fork)))
      (handlers/unregister-connection-for-store! test-store-id :fork)
      (is (nil? (handlers/get-connection-for-store test-store-id :fork)))
      (is (identical? main-conn
                      (handlers/get-connection-for-store test-store-id :db)))
      (finally
        (reset! handlers/store-registry original)))))

(deftest global-dispatch-routes-to-exact-branch
  (let [original @handlers/store-registry
        calls (atom [])
        make-conn (fn [branch]
                    {:wrapped-atom
                     (atom {:config {:branch branch}
                            :writer
                            (reify writer/PWriter
                              (-dispatch! [_ arg-map]
                                (swap! calls conj [branch arg-map])
                                (go {:branch branch}))
                              (-shutdown [_] (go true))
                              (-streaming? [_] true))})})]
    (try
      (reset! handlers/store-registry {})
      (handlers/register-connection-for-store!
       test-store-id (make-conn :db) nil)
      (handlers/register-connection-for-store!
       test-store-id (make-conn :fork) nil)
      (is (= {:branch :fork}
             (<!! (handlers/global-dispatch-handler
                   {:store-id test-store-id
                    :branch :fork
                    :arg-map {:op 'test-op}}))))
      (is (= [[:fork {:op 'test-op}]] @calls))
      (let [error (<!! (handlers/global-dispatch-handler
                        {:store-id test-store-id
                         :branch :missing
                         :arg-map {:op 'test-op}}))]
        (is (= :missing (:branch (ex-data error))))
        (is (= #{:db :fork} (:registered-branches (ex-data error)))))
      (finally
        (reset! handlers/store-registry original)))))

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

;; ---------------------------------------------------------------------------
;; Remote delete must invalidate this process's connection registry
;; ---------------------------------------------------------------------------
;;
;; The delete happens on the remote peer, so nothing local touches the registry.
;; Left alone, a later `d/connect` with the same config returns a connection
;; whose head belongs to the deleted database -- which stayed latent until the
;; optimistic overlay work began dereferencing the old root on a moved head.

(def ^:private delete-store-id #uuid "21000000-0000-0000-0000-000000000021")

(def ^:private delete-config
  {:store  {:backend :memory :id delete-store-id}
   :writer {:backend :kabel :peer-id test-peer-id}})

(deftest successful-remote-delete-invalidates-local-store-connections
  (testing "every branch of the deleted store is released; other stores untouched"
    (let [db-branch      (atom :connected)
          feature-branch (atom :connected)
          other-store    (atom :connected)
          other-store-id #uuid "22000000-0000-0000-0000-000000000022"
          registry (atom {[delete-store-id :db]      {:conn db-branch :count 1}
                          [delete-store-id :feature] {:conn feature-branch :count 1}
                          [other-store-id :db]       {:conn other-store :count 1}})]
      (binding [connections/*connections* registry]
        (with-redefs [ds/invoke-remote (fn [& _] (go {:success true}))]
          (is (= {:success true} @(writer/delete-database delete-config)))))
      (is (= :released @db-branch))
      (is (= :released @feature-branch))
      (is (= {[other-store-id :db] {:conn other-store :count 1}} @registry)))))

(deftest failed-remote-delete-preserves-local-store-connections
  (testing "a failed delete must leave the connection usable"
    (let [conn     (atom :connected)
          registry (atom {[delete-store-id :db] {:conn conn :count 1}})]
      (binding [connections/*connections* registry]
        ;; `throwable-promise` RETHROWS on deref, so the failure surfaces as a
        ;; throw here rather than as a returned value.
        (with-redefs [ds/invoke-remote (fn [& _] (go (ex-info "remote delete failed" {})))]
          (is (thrown-with-msg? Exception #"remote delete failed"
                                @(writer/delete-database delete-config)))))
      (is (= :connected @conn))
      (is (= conn (get-in @registry [[delete-store-id :db] :conn]))))))
