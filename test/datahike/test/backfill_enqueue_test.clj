(ns datahike.test.backfill-enqueue-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.backfill.runtime :as runtime]
            [datahike.test.utils :as utils]
            [datahike.tx-preds :as predicates]
            [datahike.writer :as writer]))

(defn- await! [f]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (if-let [value (f)] value
              (if (< (System/nanoTime) deadline)
                (do (Thread/sleep 10) (recur))
                (throw (ex-info "Enqueue test condition timed out" {})))))))

(defn- with-db [f]
  (let [conn (utils/setup-db {:crypto-hash? false :schema-flexibility :write
                              :attribute-refs? false :keep-history? true
                              :index :datahike.index/persistent-set
                              :writer {:backend :self :writer-ownership :shared
                                       :require-fencing :process :max-batch 8}})
        config (:config @conn)]
    (try
      (d/transact conn [{:db/ident :value :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                        {:db/id 10000 :value 1}])
      (f conn)
      (finally (d/release conn) (d/delete-database config)))))

(deftest cancellation-helper-catches-overflow-and-retires-only-exact-generation
  (let [owner (runtime/create! nil) id (random-uuid) other (random-uuid) queue (async/chan 1)]
    (try
      (doseq [generation [id other]]
        (runtime/prepare-report! owner {:db-before {}
                                        :db-after {:avet-build {:id generation :attrs #{:value}}
                                                   :max-tx 1}
                                        :tx-data []} 'begin-avet-build!))
      (with-redefs [async/put! (fn [& _] (throw (AssertionError. "Injected pending-put overflow")))]
        ;; This function is called from inside the writer's original failure
        ;; handler. Returning, rather than throwing, lets that handler finish.
        (is (= :returned (do (#'writer/enqueue-avet-cancel! owner queue id) :returned))))
      (is (true? (get-in @(:state owner) [:contexts id :retired?])))
      (is (false? (get-in @(:state owner) [:contexts other :retired?])))
      (is (= {:generation id :reason :dispatch-failed} (:last-failure @(:state owner))))
      (finally (async/close! queue) (runtime/close! owner)))))

(deftest worker-install-and-cancel-overflow-do-not-strand-live-runtime
  (with-db
    (fn [conn]
      (let [local-writer (:writer @conn) queue (:transaction-queue local-writer)
            owner (:avet-runtime local-writer) coordinator (:avet-coordinator local-writer)
            attempts (atom []) original-put async/put!]
        (with-redefs [async/put!
                      (fn [channel value & args]
                        (if (and (identical? channel queue)
                                 (contains? '#{try-install-avet-build! cancel-avet-build!} (:op value)))
                          (do (swap! attempts conj (:op value))
                              (throw (AssertionError. "Injected pending-put overflow")))
                          (apply original-put channel value args)))]
          (let [accepted @(d/begin-avet-build! conn {:value {:db/index true}})
                id (:avet-build-id accepted)]
            (await! #(and (= 2 (count @attempts)) (empty? (:jobs @(:state coordinator)))))
            (is (= '[try-install-avet-build! cancel-avet-build!] @attempts))
            (is (true? (get-in @(:state owner) [:contexts id :retired?])))
            (is (= {:generation id :reason :dispatch-failed} (:last-failure @(:state owner))))
            (let [report (d/transact conn [[:db/add 10000 :value 2]])]
              (is (= :failed (get-in report [:db-after :avet-build-result :status])))
              (is (= id (get-in report [:db-after :avet-build-result :id])))
              (is (nil? (get-in report [:db-after :avet-build])))
              (is (not (get-in report [:db-after :schema :value :db/index])))
              (is (= 2 (:value (d/entity (:db-after report) 10000)))))))))))

(deftest cancel-overflow-preserves-original-install-error-callback-and-writer
  (with-db
    (fn [conn]
      (let [local-writer (:writer @conn) queue (:transaction-queue local-writer)
            owner (:avet-runtime local-writer) coordinator (:avet-coordinator local-writer)
            store-id (get-in @conn [:config :store :id])
            original-error (promise) cancel-attempts (atom 0) original-put async/put!]
        (predicates/register-tx-pred!
         store-id ::reject-install
         (fn [report]
           (when (and (get-in report [:db-before :avet-build])
                      (= :ready (get-in report [:db-after :avet-build-result :status])))
             (throw (ex-info "Original activation rejection" {:type ::activation-rejected})))
           report))
        (try
          (with-redefs [async/put!
                        (fn [channel value & args]
                          (if (and (identical? channel queue) (= 'cancel-avet-build! (:op value)))
                            (do (swap! cancel-attempts inc)
                                (throw (AssertionError. "Injected pending-put overflow during error cleanup")))
                            (let [result (apply original-put channel value args)]
                              (when (and (instance? Throwable value)
                                         (= ::activation-rejected (:type (ex-data value))))
                                (deliver original-error value))
                              result)))]
            (let [accepted @(d/begin-avet-build! conn {:value {:db/index true}})
                  id (:avet-build-id accepted)]
              (is (instance? clojure.lang.ExceptionInfo (deref original-error 10000 nil)))
              (is (pos? @cancel-attempts))
              (await! #(empty? (:jobs @(:state coordinator))))
              (is (true? (get-in @(:state owner) [:contexts id :retired?])))
              (let [report (d/transact conn [[:db/add 10000 :value 3]])]
                (is (= :failed (get-in report [:db-after :avet-build-result :status])))
                (is (= id (get-in report [:db-after :avet-build-result :id])))
                (is (= 3 (:value (d/entity (:db-after report) 10000)))))))
          (finally (predicates/unregister-tx-pred! store-id ::reject-install)))))))
