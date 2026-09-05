(ns datahike.kabel.writer-barrier-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.kabel.writer :as kabel]
            [datahike.writer :as writer]
            [kabel.remote :as remote]))

(defn- await [channel]
  (let [[value port] (async/alts!! [channel (async/timeout 10000)])]
    (if (= port channel) value ::timed-out)))

(defn- with-writer [f]
  (let [config {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :read :keep-history? false
                :max-string-length 0}]
    (d/create-database config)
    (let [conn (d/connect config)
          remote (kabel/kabel-writer (random-uuid) (get-in config [:store :id]) nil)]
      (kabel/set-connection! remote conn)
      (try (f remote @conn)
           (finally
             (writer/shutdown remote)
             (d/release conn)
             (d/delete-database config))))))

(deftest remote-barriers-wait-for-sync-without-replacing-report-waiters
  (with-writer
    (fn [remote db]
      (let [db (assoc-in (d/db-with db [{:barrier-test 1}])
                         [:meta :datahike/commit-id] (random-uuid))
            max-tx (:max-tx db)
            report-waiter (async/promise-chan)
            registered (async/promise-chan)
            listeners (atom 0)]
        (swap! (:pending-txs remote) assoc ::report
               {:expected-max-tx max-tx
                :ch report-waiter :tx-report {:db-after db}})
        (kabel/add-listener! remote (fn [_] (swap! listeners inc)))
        (add-watch (:pending-txs remote) ::registered
                   (fn [_ _ _ pending]
                     (when (= 3 (count pending)) (async/put! registered true))))
        (with-redefs [remote/invoke
                      (fn [_ _ _ request]
                        (is (= 'writer-barrier (get-in request [:arg-map :op])))
                        (async/to-chan! [db]))]
          (let [left (writer/dispatch! remote {:op 'writer-barrier :args []})
                right (writer/dispatch! remote {:op 'writer-barrier :args []})]
            (is (= true (await registered)))
            (is (nil? (async/poll! left)))
            (is (nil? (async/poll! right)))
            (kabel/on-sync-update! remote (inc max-tx) (random-uuid))
            (is (nil? (async/poll! left))
                "a higher transaction number on another root is not readiness")
            (is (nil? (async/poll! right)))
            (kabel/on-sync-update! remote max-tx
                                   (get-in db [:meta :datahike/commit-id]))
            (is (= :synced (await report-waiter)))
            (is (= db (await left)))
            (is (= db (await right)))
            (is (= #{::report} (set (keys @(:pending-txs remote)))))
            (is (zero? @listeners))))))))

(deftest remote-barrier-remembers-exact-sync-before-rpc-response
  (with-writer
    (fn [remote db]
      (let [commit-id (random-uuid)
            snapshot (assoc-in db [:meta :datahike/commit-id] commit-id)
            response (async/promise-chan)
            invoked (async/promise-chan)]
        (with-redefs [remote/invoke (fn [& _]
                                      (async/put! invoked true)
                                      response)]
          (let [result (writer/dispatch! remote {:op 'writer-barrier :args []})]
            (is (= true (await invoked)))
            (kabel/on-sync-update! remote (:max-tx db) commit-id)
            (kabel/on-sync-update! remote (inc (:max-tx db)) (random-uuid))
            (async/put! response snapshot)
            (is (= snapshot (await result)))
            (is (empty? @(:pending-txs remote)))))))))

(deftest remote-barrier-preserves-unknown-operation-errors
  (with-writer
    (fn [remote _]
      (let [failure (ex-info "This writer has no operation writer-barrier"
                             {:type :writer/unknown-op :op 'writer-barrier})]
        (with-redefs [remote/invoke (fn [& _] (async/to-chan! [failure]))]
          (is (= (ex-data failure)
                 (ex-data (await (writer/dispatch! remote
                                                   {:op 'writer-barrier :args []}))))))))))

(deftest remote-barrier-recognizes-an-already-synchronized-root
  (with-writer
    (fn [remote db]
      (reset! (:current-max-tx remote) (:max-tx db))
      (with-redefs [remote/invoke (fn [& _] (async/to-chan! [db]))]
        (is (= db (await (writer/dispatch! remote {:op 'writer-barrier :args []}))))
        (is (empty? @(:pending-txs remote)))))))

(deftest remote-barrier-fails-on-shutdown-while-waiting-for-sync
  (with-writer
    (fn [remote db]
      (let [db (assoc-in (d/db-with db [{:barrier-test 1}])
                         [:meta :datahike/commit-id] (random-uuid))
            registered (async/promise-chan)]
        (add-watch (:pending-txs remote) ::registered
                   (fn [_ _ _ pending]
                     (when (seq pending) (async/put! registered true))))
        (with-redefs [remote/invoke (fn [& _] (async/to-chan! [db]))]
          (let [barrier (writer/dispatch! remote {:op 'writer-barrier :args []})]
            (is (= true (await registered)))
            (writer/shutdown remote)
            (is (= :writer-shutdown (:type (ex-data (await barrier)))))
            (is (empty? @(:pending-txs remote)))))))))
