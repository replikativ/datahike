(ns datahike.test.backfill-writer-lifecycle-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as async]
            [datahike.backfill.coordinator :as coordinator]
            [datahike.backfill.runtime :as runtime]
            [datahike.api :as d]
            [datahike.test.utils :as utils]
            [datahike.tx-preds :as predicates]
            [datahike.writer :as writer])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(defn- await-condition! [f]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (or (f)
          (if (< (System/nanoTime) deadline)
            (do (Thread/sleep 10) (recur))
            (throw (ex-info "Lifecycle condition timed out" {})))))))

(defn- invoke! [conn op & args]
  (let [channel (writer/dispatch! (:writer @conn) {:op op :args (vec args)})
        [result port] (async/alts!! [channel (async/timeout 10000)])]
    (when-not (= port channel) (throw (ex-info "Writer operation timed out" {})))
    (when (instance? Throwable result) (throw result))
    result))

(def ^:dynamic *predicate-context* nil)

(deftest normalized-predicate-runs-once-with-caller-bindings
  (let [owner (runtime/create! nil)
        report {:db-before {} :db-after {}}
        calls (atom [])
        wrapped (with-meta (fn [& _] (throw (ex-info "Wrapper must not run twice" {})))
                  {::writer/raw-write-fn (fn [_] report)})
        bindings (binding [*predicate-context* :caller] (get-thread-bindings))]
    (with-redefs [predicates/check-report (fn [r] (swap! calls conj *predicate-context*) r)]
      (is (= report (#'writer/apply-avet-write owner wrapped {} [] bindings 'transact!))))
    (is (= [:caller] @calls))))

(deftest failed-internal-cancel-can-recover-without-bypassing-predicates
  (let [conn (utils/setup-db {:crypto-hash? false :schema-flexibility :write
                              :index :datahike.index/persistent-set
                              :writer {:backend :self :writer-ownership :shared :require-fencing :process}})
        config (:config @conn)
        store-id (get-in config [:store :id])
        vetoes (atom 0)]
    (try
      (d/transact conn [{:db/ident :score :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one}
                        {:db/id 10000 :score 1} {:db/id 10001 :score 1}])
      (predicates/register-tx-pred!
       store-id ::reject-cancel
       (fn [report]
         (when (get-in report [:db-after :avet-build-result])
           (swap! vetoes inc)
           (throw (ex-info "Cancellation veto" {:type ::veto})))
         report))
      (let [report (invoke! conn 'begin-avet-build! {:score {:db/unique :db.unique/value}})
            id (get-in report [:db-after :avet-build :id])
            owner (:avet-runtime (:writer @conn))]
        (await-condition! #(= id (get-in @(:state owner) [:last-failure :generation])))
        (is (pos? @vetoes))
        (is (= id (get-in @conn [:avet-build :id])))
        (is (nil? (:avet-build-result @conn)))
        (is (nil? (get-in @conn [:schema :score :db/unique])))
        ;; Reconciliation is not permission to bypass the predicate either.
        (is (thrown? clojure.lang.ExceptionInfo (d/transact conn [[:db/add 10001 :score 2]])))
        (is (= #{[10000 1] [10001 1]} (d/q '[:find ?e ?v :where [?e :score ?v]] @conn)))
        (predicates/unregister-tx-pred! store-id ::reject-cancel)
        (d/transact conn [[:db/add 10001 :score 2]])
        (is (nil? (:avet-build @conn)))
        (is (= :failed (get-in @conn [:avet-build-result :status])))
        (invoke! conn 'begin-avet-build! {:score {:db/unique :db.unique/value}})
        (await-condition! #(= :ready (get-in @conn [:avet-build-result :status])))
        (is (= :db.unique/value (get-in @conn [:schema :score :db/unique]))))
      (finally
        (predicates/unregister-tx-pred! store-id ::reject-cancel)
        (d/release conn)
        (d/delete-database config)))))

(deftest failed-old-generation-does-not-retire-replacement
  (let [owner (runtime/create! nil)
        old (random-uuid) replacement (random-uuid)]
    (swap! (:state owner) assoc :contexts {old {:retired? false} replacement {:retired? false}})
    (runtime/fail-generation! owner old :cancel-rejected)
    (is (true? (get-in @(:state owner) [:contexts old :retired?])))
    (is (false? (get-in @(:state owner) [:contexts replacement :retired?])))
    (is (= {:generation old :reason :cancel-rejected} (:last-failure @(:state owner))))))

(deftest closed-cancellation-queue-retires-only-local-generation
  (let [owner (runtime/create! nil)
        id (random-uuid)
        queue (async/chan 1)]
    (swap! (:state owner) assoc :contexts {id {:retired? false}})
    (async/close! queue)
    (#'writer/enqueue-avet-cancel! owner queue id)
    (is (true? (get-in @(:state owner) [:contexts id :retired?])))
    (is (= {:generation id :reason :dispatch-failed} (:last-failure @(:state owner))))))

(deftest durable-hook-failure-retires-runtime-and-cancels-exact-generation
  (doseq [failure-stage [:runtime :coordinator]]
    (let [id (random-uuid) replacement (random-uuid)
          owner (runtime/create! nil)
          queue (async/chan 2)
          aborted (atom [])
          token (Object.)]
      (swap! (:state owner) assoc :contexts {id {:retired? false} replacement {:retired? false}})
      (with-redefs [runtime/committed! (fn [& _] (when (= :runtime failure-stage)
                                                   (throw (ex-info "Commit hook failed" {}))))
                    coordinator/after-commit! (fn [& _] (throw (ex-info "Start hook failed" {})))
                    coordinator/invalidate! (fn [_])
                    runtime/invalidate! (fn [r] (swap! (:state r) update :contexts
                                                       #(into {} (map (fn [[k v]] [k (assoc v :retired? true)])) %)))
                    coordinator/abort-install! (fn [_ t definite?] (swap! aborted conj [t definite?]))]
        (#'writer/committed-avet! owner {} queue {:avet-build {:id id}}
                                  [[{::writer/avet-install-token token}]])
        (is (every? :retired? (vals (:contexts @(:state owner)))))
        (is (= [[token true]] @aborted))
        (let [invocation (async/poll! queue)]
          (is (= 'cancel-avet-build! (:op invocation)))
          (is (= [id :build-failed] (:args invocation)))
          (when invocation (async/>!! (:callback invocation) {:committed true})))
        (is (nil? (async/poll! queue))))
      (async/close! queue))))

(deftest ownership-drift-after-predicate-does-not-append
  (let [owner (runtime/create! nil)
        id (random-uuid)]
    (try
      (let [begin (runtime/prepare-report! owner {:db-before {}
                                                  :db-after {:avet-build {:id id} :max-tx 1}}
                                           'begin-avet-build!)
            before (:db-after begin)
            proposed (runtime/normalize-report owner {:db-before before :db-after (assoc before :max-tx 2)}
                                               'transact!)
            staged (get-in @(:state owner) [:contexts id :staged])]
        (runtime/fail-generation! owner id :cancel-rejected)
        (let [failure (try (runtime/prepare-report! owner proposed 'transact! true) nil
                           (catch clojure.lang.ExceptionInfo e (ex-data e)))]
          (is (= ::runtime/normalization-conflict (:type failure)))
          (is (true? (:retryable? failure))))
        (is (= staged (get-in @(:state owner) [:contexts id :staged])))
        (is (= (get-in staged [:descriptor :end])
               (.length (java.io.File. (get-in staged [:descriptor :path]))))))
      (finally (runtime/close! owner)))))

(deftest blocked-worker-cleanup-does-not-starve-go-dispatch
  (let [workers 16
        entered (CountDownLatch. workers)
        release (CountDownLatch. 1)
        finished (CountDownLatch. workers)
        shutdowns (atom [])]
    (with-redefs [coordinator/close!
                  (fn [_]
                    (.countDown entered)
                    (try (.await release)
                         (finally (.countDown finished))))]
      (try
        (dotimes [_ workers]
          (let [done (async/promise-chan)
                local (writer/map->LocalWriter
                       {:thread done :transaction-queue (async/chan 1)
                        :avet-coordinator {}})]
            (async/>!! done :drained)
            (swap! shutdowns conj (writer/shutdown local))))
        ;; More simultaneous joins than the ordinary go pool can handle. They
        ;; must all begin on blocking threads while unrelated go work progresses.
        (is (.await entered 10 TimeUnit/SECONDS))
        (let [progress (async/go :progress)
              [value port] (async/alts!! [progress (async/timeout 2000)])]
          (is (= progress port))
          (is (= :progress value)))
        (finally
          (.countDown release)
          (is (.await finished 10 TimeUnit/SECONDS))
          (doseq [shutdown @shutdowns]
            (let [[value port] (async/alts!! [shutdown (async/timeout 2000)])]
              (is (= shutdown port))
              (is (= :drained value)))))))))
