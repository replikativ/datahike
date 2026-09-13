(ns datahike.test.backfill-online-batch-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.writer :as writer]
            [datahike.test.utils :as utils]))

(defn- dispatch [conn op & args]
  (writer/dispatch! (:writer @conn) {:op op :args (vec args)}))

(defn- result! [channel]
  (let [[result selected] (async/alts!! [channel (async/timeout 10000)])]
    (when-not (= selected channel) (throw (ex-info "Batch test writer timed out" {})))
    (when (instance? Throwable result) (throw result))
    result))

(defn- await! [f]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (if-let [result (f)] result
              (if (< (System/nanoTime) deadline)
                (do (Thread/sleep 10) (recur))
                (throw (ex-info "Batch test condition timed out" {})))))))

(deftest install-and-following-write-share-one-durable-group
  (doseq [following [:ordinary-write :new-generation]]
    (let [conn (utils/setup-db {:crypto-hash? false :schema-flexibility :write
                                :attribute-refs? false :keep-history? true
                                :index :datahike.index/persistent-set
                                :writer {:backend :self :writer-ownership :shared
                                         :require-fencing :process :max-batch 8}})
          config (:config @conn)
          drain-entered (promise) allow-drain (promise) following-enqueued (promise)
          following-channel (promise)
          gate-taken? (atom false) events (atom [])]
      (try
        (d/transact conn [{:db/ident :first :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                          {:db/ident :second :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                          {:db/id 10000 :first 1 :second 2}])
        (d/listen-commits conn ::groups #(swap! events conj %))
        (let [local-writer (:writer @conn)
              commit-queue (:commit-queue local-writer)
              coordinator (:avet-coordinator local-writer)
              original-poll async/poll!
              original-put async/put!]
          (with-redefs
           [async/poll!
            (fn [channel]
               ;; The commit loop already owns the install as its first report,
               ;; but has not drained following reports yet. Block ONLY this
               ;; exact queue and only once, leaving the tx loop free to enqueue.
              (when (and (identical? channel commit-queue)
                         (some #(= :awaiting-commit (:phase @%)) (vals (:jobs @(:state coordinator))))
                         (compare-and-set! gate-taken? false true))
                (deliver drain-entered true)
                (when (= ::timeout (deref allow-drain 10000 ::timeout))
                  (throw (ex-info "Batch test drain gate timed out" {}))))
              (original-poll channel))
            async/put!
            (fn [channel value & args]
               ;; The shared writer closes a batch as soon as its input queue
               ;; is empty. Queue the follower before returning from the install
               ;; enqueue, so it is present at that exact batching decision.
              (when (and (identical? channel commit-queue) (vector? value)
                         (contains? (first value) :datahike.writer/avet-install-token)
                         (not (realized? following-channel)))
                (deliver following-channel
                         (writer/dispatch! local-writer
                                           (case following
                                             :ordinary-write {:op 'transact! :args [{:tx-data [[:db/add 10000 :first 11]]}]}
                                             :new-generation {:op 'begin-avet-build! :args [{:second {:db/index true}}]}))))
              (let [result (apply original-put channel value args)]
                 ;; Unlike a predicate callback, this observes the actual
                 ;; commit-queue enqueue, so releasing the drain is deterministic.
                (when (and result (identical? channel commit-queue)
                           (vector? value)
                           (true? (get-in (first value) [:db-before :schema :first :db/index])))
                  (deliver following-enqueued true))
                result))]
            (let [first-report (result! (dispatch conn 'begin-avet-build! {:first {:db/index true}}))
                  first-id (get-in first-report [:db-after :avet-build :id])]
              (is (true? (deref drain-entered 10000 false)))
              (let [channel (deref following-channel 10000 nil)]
                (is (true? (deref following-enqueued 10000 false)))
                (deliver allow-drain true)
                (let [report (result! channel)
                      cid (get-in report [:db-after :meta :datahike/commit-id])
                      event (first (filter #(= cid (:commit-id %)) @events))]
                  (is (= 2 (:tx-count event)))
                  (is (true? (get-in (:db-after event) [:schema :first :db/index])))
                  (case following
                    :ordinary-write
                    (do
                      (is (= first-id (get-in report [:db-after :avet-build-result :id])))
                      (is (= :ready (get-in report [:db-after :avet-build-result :status])))
                      (is (= [11] (mapv :v (d/datoms (:db-after report) :avet :first)))))
                    :new-generation
                    (let [next-id (get-in report [:db-after :avet-build :id])]
                      (is (uuid? next-id))
                      (is (not= first-id next-id))
                      (await! #(and (= next-id (get-in @conn [:avet-build-result :id]))
                                    (= :ready (get-in @conn [:avet-build-result :status]))))
                      (is (= [1] (mapv :v (d/datoms @conn :avet :first))))
                      (is (= [2] (mapv :v (d/datoms @conn :avet :second))))))))
              (await! #(empty? (:jobs @(:state coordinator)))))))
        (finally
          (deliver allow-drain true)
          (d/unlisten-commits conn ::groups)
          (d/release conn)
          (d/delete-database config))))))

(deftest accepted-request-id-survives-a-different-final-group-generation
  (let [conn (utils/setup-db {:crypto-hash? false :schema-flexibility :write
                              :attribute-refs? false :keep-history? true
                              :index :datahike.index/persistent-set
                              :writer {:backend :self :writer-ownership :shared
                                       :require-fencing :process :max-batch 8}})
        config (:config @conn) injected? (atom false) gate-taken? (atom false)
        drain-entered (promise) allow-drain (promise) followers (promise)
        followers-enqueued (promise) follower-count (atom 0)]
    (try
      (d/transact conn [{:db/ident :first :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                        {:db/ident :second :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                        {:db/id 10000 :first 1 :second 2}])
      (let [local-writer (:writer @conn) commit-queue (:commit-queue local-writer)
            original-poll async/poll! original-put async/put!]
        (with-redefs
         [async/poll! (fn [channel]
                        (when (and (identical? channel commit-queue) @injected?
                                   (compare-and-set! gate-taken? false true))
                          (deliver drain-entered true)
                          (when (= ::timeout (deref allow-drain 10000 ::timeout))
                            (throw (ex-info "Request group gate timed out" {}))))
                        (original-poll channel))
          async/put!
          (fn [channel value & args]
            (let [commit-report? (and (identical? channel commit-queue) (vector? value))
                  first-request? (and commit-report? (:avet-build-id (first value))
                                      (contains? (get-in (first value) [:db-after :avet-build :patch]) :first))]
              (when (and first-request? (compare-and-set! injected? false true))
                (deliver followers
                         [(writer/dispatch! local-writer
                                            {:op 'transact!
                                             :args [{:tx-data [{:db/ident :unrelated :db/valueType :db.type/long
                                                                :db/cardinality :db.cardinality/one}]}]})
                          (writer/dispatch! local-writer
                                            {:op 'begin-avet-build! :args [{:second {:db/index true}}]})]))
              (let [result (apply original-put channel value args)]
                (when (and result commit-report? @injected? (not first-request?)
                           (= 2 (swap! follower-count inc)))
                  (deliver followers-enqueued true))
                result)))]
          (let [accepted-a (d/begin-avet-build! conn {:first {:db/index true}})]
            (is (true? (deref drain-entered 10000 false)))
            (is (true? (deref followers-enqueued 10000 false)))
            (deliver allow-drain true)
            (let [report-a @accepted-a
                  [schema-channel b-channel] (deref followers 10000 nil)
                  schema-report (result! schema-channel)
                  report-b (result! b-channel)
                  a-id (:avet-build-id report-a) b-id (:avet-build-id report-b)]
              (is (uuid? a-id))
              (is (uuid? b-id))
              (is (not= a-id b-id))
              (is (= b-id (get-in report-a [:db-after :avet-build :id])))
              (is (= (get-in report-a [:db-after :meta :datahike/commit-id])
                     (get-in schema-report [:db-after :meta :datahike/commit-id])
                     (get-in report-b [:db-after :meta :datahike/commit-id])))
              (await! #(and (= b-id (get-in @conn [:avet-build-result :id]))
                            (= :ready (get-in @conn [:avet-build-result :status]))))
              (is (not (get-in @conn [:schema :first :db/index])))
              (is (true? (get-in @conn [:schema :second :db/index])))
              (await! #(empty? (:jobs @(:state (:avet-coordinator local-writer)))))))))
      (finally (deliver allow-drain true) (d/release conn) (d/delete-database config)))))
