(ns datahike.test.writer-barrier-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.tx-preds :as txp]
            [datahike.writer :as writer]
            [datahike.writing :as writing]))

(defn- with-db [f]
  (let [config {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :read
                :max-string-length 0
                :keep-history? false
                :writer {:backend :self
                         ;; These tests hold commit! open to inspect the local
                         ;; queue boundary. Shared ownership intentionally
                         ;; stops preparing a batch when its queue becomes
                         ;; momentarily empty, so use the local-exclusive path
                         ;; whose speculative chain remains active here.
                         :writer-ownership :exclusive
                         :write-fn-map
                         {'processed (fn [_] (async/to-chan! [true]))
                          'fatal (fn [_ entered release]
                                   (deliver entered true)
                                   @release
                                   (throw (AssertionError. "synthetic processing failure")))}}}]
    (d/create-database config)
    (let [conn (d/connect config)]
      (try (f conn)
           (finally
             (d/release conn)
             (d/delete-database config))))))

(defn- await [channel]
  (let [[value port] (async/alts!! [channel (async/timeout 10000)])]
    (if (= port channel) value ::timed-out)))

(defn- dispatch [conn op args]
  (writer/dispatch! (:writer @(:wrapped-atom conn)) {:op op :args args}))

(deftest ensure-predicate-is-atomic-and-does-not-replace
  (let [store-id (random-uuid)
        f identity
        other (fn [report] report)]
    (try
      (let [start (promise)
            attempts (vec (repeatedly 16
                                      #(future @start
                                               (txp/ensure-tx-pred! store-id :guard f))))
            _ (deliver start true)
            results (mapv deref attempts)]
        (is (= {:installed 1 :present 15} (frequencies results))))
      (is (= :present (txp/ensure-tx-pred! store-id :guard f)))
      (is (= :tx-pred/id-collision
             (:type (try (txp/ensure-tx-pred! store-id :guard other)
                         (catch clojure.lang.ExceptionInfo e (ex-data e))))))
      (is (identical? f (get (txp/tx-preds-for store-id) :guard)))
      (finally (txp/unregister-tx-pred! store-id :guard)))))

(deftest barriers-do-not-create-transactions-or-run-predicates
  (with-db
    (fn [conn]
      (let [before @conn
            store-id (get-in before [:config :store :id])
            calls (atom 0)]
        (txp/ensure-tx-pred! store-id :guard (fn [_] (swap! calls inc)))
        (d/listen conn :barrier-test (fn [_] (swap! calls inc)))
        (try
          (with-redefs [writing/commit! (fn [& _]
                                          (throw (ex-info "A barrier must not commit" {})))]
            (is (= before (d/writer-barrier conn)))
            (is (= before @(d/writer-barrier! conn))))
          (is (= (:max-tx before) (:max-tx @conn)))
          (is (= (get-in before [:meta :datahike/commit-id])
                 (get-in @conn [:meta :datahike/commit-id])))
          (is (zero? @calls))
          (finally
            (d/unlisten conn :barrier-test)
            (txp/unregister-tx-pred! store-id :guard)))))))

(defn- run-queued-barrier [fail-second?]
  (with-db
    (fn [conn]
      (let [original writing/commit!
            entered (async/promise-chan)
            release-first (async/promise-chan)
            commit-count (atom 0)
            failure (ex-info "synthetic second commit failure" {:type ::commit-failure})]
        (with-redefs [writing/commit!
                      (fn [& args]
                        (let [ordinal (swap! commit-count inc)]
                          (async/go
                            (when (= 1 ordinal)
                              (async/>! entered true)
                              (async/<! release-first))
                            (if (and fail-second? (= 2 ordinal))
                              failure
                              (async/<! (apply original args))))))]
          (let [first-write (dispatch conn 'transact! [{:tx-data [{:db/id 1 :n 1}]}])]
            (is (= true (await entered)))
            (let [second-write (dispatch conn 'transact! [{:tx-data [{:db/id 2 :n 2}]}])
                  barrier (dispatch conn 'writer-barrier [])
                  third-write (dispatch conn 'transact! [{:tx-data [{:db/id 3 :n 3}]}])]
              ;; Observe the transaction loop past every report and marker
              ;; while the commit loop is still blocked on the first root.
              (is (= true (await (dispatch conn 'processed []))))
              (async/put! release-first true)
              (let [first-result (await first-write)
                    second-result (await second-write)
                    snapshot (await barrier)
                    third-result (await third-write)]
                (is (map? first-result))
                (if fail-second?
                  (do
                    (doseq [result [second-result snapshot third-result]]
                      (is (= ::commit-failure (:type (ex-data result)))))
                    (is (= 2 @commit-count))
                    (is (= #{[1]} (d/q '[:find ?n :where [_ :n ?n]] @conn)))
                    (is (= :writer-shut-down
                           (:type (ex-data (await (dispatch conn 'writer-barrier [])))))))
                  (do
                    (is (map? second-result))
                    (is (map? third-result))
                    (is (= #{[1] [2]} (d/q '[:find ?n :where [_ :n ?n]] snapshot)))
                    (is (= (:max-tx (:db-after second-result)) (:max-tx snapshot)))
                    (is (= #{[1] [2] [3]} (d/q '[:find ?n :where [_ :n ?n]] @conn)))
                    (is (= 3 @commit-count))))))))))))

(deftest barriers-separate-commit-batches
  (testing "later reports cannot enter the durable root returned by a barrier"
    (run-queued-barrier false)))

(deftest queued-barriers-fail-when-a-preceding-commit-fails
  (testing "a retained barrier and reports behind it fail without hanging"
    (run-queued-barrier true)))

(deftest queued-barriers-fail-when-transaction-processing-crashes
  (with-db
    (fn [conn]
      (let [entered (promise)
            release (promise)
            failure (dispatch conn 'fatal [entered release])]
        (is (= true (deref entered 10000 ::timed-out)))
        (let [barrier (dispatch conn 'writer-barrier [])]
          (deliver release true)
          (is (instance? AssertionError (await failure)))
          (is (instance? AssertionError (await barrier))))))))

(deftest shared-barriers-follow-transparent-head-conflict-retry
  (let [config {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :read
                :max-string-length 0
                :keep-history? false
                :writer {:backend :self
                         :writer-ownership :shared
                         :head-conflict-backoff-ms 0
                         :write-fn-map
                         {'held-transact
                          (fn [old entered release arg]
                            (deliver entered true)
                            @release
                            (writing/transact! old arg))}}}]
    (d/create-database config)
    (let [conn (d/connect config)
          original writing/commit!
          commits (atom 0)
          entered (promise)
          release (promise)]
      (try
        (with-redefs [writer/retryable-ops (conj writer/retryable-ops
                                                 'held-transact)
                      writing/commit!
                      (fn [& args]
                        (if (= 1 (swap! commits inc))
                          (async/to-chan!
                           [(ex-info "forced head conflict"
                                     {:type :konserve/revision-mismatch})])
                          (apply original args)))]
          (let [tx (dispatch conn 'held-transact
                             [entered release {:tx-data [{:db/id 1 :n 1}]}])]
            (is (= true (deref entered 10000 ::timed-out)))
            ;; The transaction loop is held inside the operation, so this
            ;; marker is deterministically queued behind that invocation before
            ;; its first commit attempt can begin.
            (let [barrier-1 (dispatch conn 'writer-barrier [])
                  barrier-2 (dispatch conn 'writer-barrier [])]
              (deliver release true)
              (let [report (await tx)
                    snapshot-1 (await barrier-1)
                    snapshot-2 (await barrier-2)]
                (is (map? report))
                (is (map? snapshot-1))
                (is (map? snapshot-2))
                (is (= 2 @commits))
                (doseq [snapshot [snapshot-1 snapshot-2]]
                  (is (= #{[1]} (d/q '[:find ?n :where [_ :n ?n]] snapshot)))
                  (is (= (:max-tx (:db-after report)) (:max-tx snapshot))))))))
        (finally
          (d/release conn)
          (d/delete-database config))))))
