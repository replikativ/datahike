(ns datahike.test.prepared-secondary-commit-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.db.utils :as dbu]
            [datahike.index.secondary :as sec]
            [datahike.online-gc :as online-gc]
            [datahike.versioning :as dv]
            [datahike.writing :as writing]
            [konserve.core :as k]))

(defn- delivered [value]
  (let [ch (async/promise-chan)]
    (async/put! ch value)
    ch))

(defn- generation-key-map [m]
  (merge {:format-version 1 :storage-owner :external} m))

(declare ->FakePreparation)

(defrecord FakePreparedIndex [key-map]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] #{})
  (-transact [this _] this)

  sec/IDurableSecondaryIndex
  (-sec-generation-key-map [_] key-map)
  (-sec-prepare [this _]
    (delivered (->FakePreparation (atom []) nil this nil)))
  (-sec-restore [this _ _] this))

(defrecord FakePreparation [events _key-map prepared-index publish-error]
  sec/IPreparedSecondaryGeneration
  (-sec-generation-index [_] prepared-index)
  (-sec-release [_ outcome]
    (case (:status outcome)
      :committed (swap! events conj [:publish (:primary-commit-id outcome)])
      :aborted (swap! events conj [:abort (:type (ex-data (:cause outcome)))])
      :unknown (swap! events conj [:unknown]))
    (let [publish-error (if (instance? clojure.lang.IAtom publish-error)
                          (first (swap-vals! publish-error (constantly nil)))
                          publish-error)]
      (delivered (or (when (= :committed (:status outcome)) publish-error)
                     true)))))

(defrecord RetryingCleanupPreparation [attempts fail-status]
  sec/IPreparedSecondaryGeneration
  (-sec-generation-index [_] nil)
  (-sec-release [_ outcome]
    (delivered
     (if (and (= fail-status (:status outcome))
              (= 1 (swap! attempts inc)))
       (ex-info "injected secondary cleanup failure"
                {:type :test/secondary-cleanup-failure})
       true))))

(defrecord FakeIndex [events key-map prepared-index publish-error]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] #{})
  (-transact [this _] this)

  sec/IDurableSecondaryIndex
  (-sec-generation-key-map [_] key-map)
  (-sec-prepare [_ context]
    (swap! events conj [:prepare (select-keys context
                                              [:branch :index-ident :attempt-id
                                               :base-primary-commit-id])])
    (delivered (->FakePreparation events key-map prepared-index
                                  (when publish-error (atom publish-error)))))
  (-sec-restore [this _ _] this))

(defrecord FailingPrepareIndex [events failure]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] #{})
  (-transact [this _] this)

  sec/IDurableSecondaryIndex
  (-sec-generation-key-map [_]
    (generation-key-map {:type :failing-preparation}))
  (-sec-prepare [_ context]
    (swap! events conj [:prepare-failed (:attempt-id context)])
    (delivered failure))
  (-sec-restore [this _ _] this))

(defrecord TransientIndex [events]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] #{})
  (-transact [this _] this))

(defonce ^:private prepared-branch-events (atom []))

(defrecord PreparedBranchRecorder [attrs generation]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] attrs)
  (-transact [this _] this)

  sec/IDurableSecondaryIndex
  (-sec-generation-key-map [_]
    (generation-key-map
     {:type :test/prepared-branch-recorder :generation generation}))
  (-sec-prepare [this _]
    (delivered (->FakePreparation prepared-branch-events nil this nil)))
  (-sec-restore [this _ key-map]
    (assoc this :generation (:generation key-map))))

(defonce ^:private _register-prepared-branch-recorder
  (sec/register-index-type!
   :test/prepared-branch-recorder
   (fn [config _db]
     (->PreparedBranchRecorder (set (:attrs config)) (random-uuid)))))

(defn- await-ready [conn idx-ident]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (let [status (get-in (d/db conn) [:schema idx-ident :db.secondary/status])]
        (cond
          (= :ready status) status
          (< (System/currentTimeMillis) deadline) (do (Thread/sleep 10) (recur))
          :else status)))))

(defn- fixture
  ([] (fixture {}))
  ([overrides]
   (let [config (merge {:store {:backend :memory :id (random-uuid)}
                        :schema-flexibility :write}
                       overrides)]
     (d/create-database config)
     {:config config :connection (d/connect config)})))

(defn- cleanup [{:keys [config connection]}]
  (d/release connection)
  (d/delete-database config))

(defn- fake-db [db events publish-error]
  (let [root (random-uuid)
        key-map (generation-key-map
                 {:type :fake-prepared
                  :generation root
                  :merkle-root root})
        prepared-index (->FakePreparedIndex key-map)]
    {:db (assoc db :secondary-indices
                {:idx/fake (->FakeIndex events key-map prepared-index
                                        publish-error)})
     :key-map key-map
     :prepared-index prepared-index}))

(deftest branching-a-live-prepared-index-copies-its-stored-generation
  (let [{:keys [connection] :as f}
        (fixture {:writer {:backend :self :writer-ownership :exclusive}})]
    (try
      (d/transact connection
                  [{:db/ident :item/name
                    :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}
                   {:item/name "one"}
                   {:db/ident :idx/prepared-branch
                    :db.secondary/type :test/prepared-branch-recorder
                    :db.secondary/attrs [:item/name]}])
      (is (= :ready (await-ready connection :idx/prepared-branch)))
      (let [store (:store @connection)
            source-key-map (get-in (k/get store :db nil {:sync? true})
                                   [:secondary-index-keys :idx/prepared-branch])]
        (reset! prepared-branch-events [])
        (dv/branch! connection :db :prepared-feature)
        (is (empty? @prepared-branch-events)
            "branching invokes no adapter lifecycle or native publication")
        (is (= source-key-map
               (get-in (k/get store :prepared-feature nil {:sync? true})
                       [:secondary-index-keys :idx/prepared-branch]))
            "the Datahike branch copies the exact immutable generation address"))
      (finally
        (cleanup f)))))

(deftest prepared-secondary-is-published-and-retained
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            {:keys [db key-map prepared-index]} (fake-db before events nil)
            committed (writing/commit! db #{} true)
            stored-head (k/get (:store committed)
                               (get-in committed [:config :branch])
                               nil {:sync? true})]
        (testing "preparation precedes post-head publication"
          (is (= :prepare (ffirst @events)))
          (is (= :publish (first (second @events))))
          (is (= (get-in committed [:meta :datahike/commit-id])
                 (second (second @events)))))
        (testing "the prepared key-map is durable and the prepared live value is retained"
          (is (= key-map (get-in stored-head
                                 [:secondary-index-keys :idx/fake])))
          (is (= key-map (get-in committed
                                 [:secondary-index-keys :idx/fake])))
          (is (identical? prepared-index
                          (get-in committed [:secondary-indices :idx/fake])))))
      (finally
        (cleanup f)))))

(deftest pre-head-failure-aborts-prepared-secondaries
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            old-head (k/get (:store before) (get-in before [:config :branch])
                            nil {:sync? true})
            {:keys [db key-map]} (fake-db before events nil)
            failure (ex-info "forced before primary publication"
                             {:type :test/pre-head-failure})]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"forced before primary publication"
             (with-redefs [writing/create-commit-id (fn [& _] (throw failure))]
               (writing/commit! db #{} true))))
        (is (= [:prepare :abort] (mapv first @events)))
        (is (= :test/pre-head-failure (second (second @events))))
        (is (= old-head
               (k/get (:store before) (get-in before [:config :branch])
                      nil {:sync? true}))
            "the primary head remains unchanged"))
      (finally
        (cleanup f)))))

(deftest publish-failure-does-not-reject-an-already-durable-primary-head
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            publish-error (ex-info "forced publish failure"
                                   {:type :test/publish-failure})
            {:keys [db key-map prepared-index]}
            (fake-db before events publish-error)
            committed (writing/commit! db #{} true)
            stored-head (k/get (:store committed)
                               (get-in committed [:config :branch])
                               nil {:sync? true})]
        (is (= [:prepare :publish] (mapv first @events)))
        (is (not-any? #(= :abort (first %)) @events)
            "a visible prepared generation must never be aborted")
        (is (= (get-in committed [:meta :datahike/commit-id])
               (get-in stored-head [:meta :datahike/commit-id])))
        (is (= key-map (get-in stored-head
                               [:secondary-index-keys :idx/fake])))
        (is (identical? prepared-index
                        (get-in committed [:secondary-indices :idx/fake])))
        (let [attempt-id (get-in (first @events) [1 :attempt-id])]
          (is (= true
                 (get-in (writing/unresolved-secondary-publications)
                         [attempt-id :head-proven?])))
          (is (true? (async/<!!
                      (writing/reconcile-secondary-publication! attempt-id))))
          (is (not (contains? (writing/unresolved-secondary-publications)
                              attempt-id)))))
      (finally
        (cleanup f)))))

(deftest asynchronous-commit-awaits-preparation-and-publication
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            {:keys [db key-map prepared-index]} (fake-db before events nil)
            committed (async/<!! (writing/commit! db #{} false))
            stored-head (async/<!! (k/get (:store committed)
                                          (get-in committed [:config :branch])
                                          nil {:sync? false}))]
        (is (= [:prepare :publish] (mapv first @events)))
        (is (= key-map (get-in stored-head
                               [:secondary-index-keys :idx/fake])))
        (is (identical? prepared-index
                        (get-in committed [:secondary-indices :idx/fake]))))
      (finally
        (cleanup f)))))

(deftest one-attempt-id-is-shared-by-all-secondary-preparations
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            first-index (:db (fake-db before events nil))
            second-key (generation-key-map
                        {:type :fake-prepared :generation (random-uuid)})
            db (assoc-in first-index [:secondary-indices :idx/second]
                         (->FakeIndex events second-key
                                      (->FakePreparedIndex second-key)
                                      nil))]
        (writing/commit! db #{} true)
        (let [prepares (filter #(= :prepare (first %)) @events)
              attempt-ids (mapv #(get-in % [1 :attempt-id]) prepares)]
          (is (= 2 (count attempt-ids)))
          (is (uuid? (first attempt-ids)))
          (is (apply = attempt-ids))
          (is (= #{:idx/fake :idx/second}
                 (set (map #(get-in % [1 :index-ident]) prepares))))))
      (finally
        (cleanup f)))))

(deftest a-prepared-index-does-not-make-an-unavailable-generation-safe
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            carried-key {:type :temporarily-unavailable
                         :generation (random-uuid)}
            {:keys [db key-map]} (fake-db before events nil)
            db (-> db
                   (assoc-in [:schema :idx/missing]
                             {:db.secondary/type :test/missing
                              :db.secondary/status :ready})
                   (assoc :secondary-index-keys {:idx/missing carried-key}))
            failure (try
                      (writing/commit! db #{} true)
                      nil
                      (catch clojure.lang.ExceptionInfo e e))]
        (is (= :secondary/unavailable-durable-generations
               (:type (ex-data failure))))
        (is (= [[:prepare :idx/fake]
                [:abort :secondary/unavailable-durable-generations]]
               (mapv (fn [[event detail]]
                       [event (if (= :prepare event)
                                (:index-ident detail)
                                detail)])
                     @events))
            "a prepared sibling is aborted before the head can carry a stale generation")
        (is (nil? (get-in (d/db connection)
                          [:secondary-index-keys :idx/missing]))))
      (finally
        (cleanup f)))))

(deftest building-secondary-is-neither-prepared-nor-published
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            old-key {:type :fake-prepared :generation (random-uuid)}
            {:keys [db]} (fake-db before events nil)
            db (-> db
                   (assoc-in [:schema :idx/fake]
                             {:db.secondary/type :test/fake
                              :db.secondary/status :building})
                   (assoc :secondary-index-keys {:idx/fake old-key}))
            committed (writing/commit! db #{} true)]
        (is (empty? @events))
        (is (nil? (get-in committed [:secondary-index-keys :idx/fake])))
        (is (nil? (get-in (k/get (:store committed)
                                 (get-in committed [:config :branch])
                                 nil {:sync? true})
                          [:merkle-roots :secondary :idx/fake]))))
      (finally
        (cleanup f)))))

(deftest revision-mismatch-aborts-a-prepared-generation
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            old-head (k/get (:store before) (get-in before [:config :branch])
                            nil {:sync? true})
            {:keys [db]} (fake-db before events nil)
            original-assoc k/assoc]
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"forced revision mismatch"
             (with-redefs [k/assoc
                           (fn [& args]
                             (let [opts (last args)]
                               (if (:expected-revision opts)
                                 (throw (ex-info "forced revision mismatch"
                                                 {:type :konserve/revision-mismatch}))
                                 (apply original-assoc args))))]
               (writing/commit! db #{} true nil :stale-revision))))
        (is (= [:prepare :abort] (mapv first @events)))
        (is (= old-head
               (k/get (:store before) (get-in before [:config :branch])
                      nil {:sync? true}))))
      (finally
        (cleanup f)))))

(deftest ambiguous-head-write-failure-does-not-abort
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            {:keys [db key-map]} (fake-db before events nil)
            original-assoc k/assoc
            failure
            (try
              (with-redefs [k/assoc
                            (fn [& args]
                              (let [opts (last args)]
                                (if (:expected-revision opts)
                                  (let [[store key value _] args]
                                    ;; Model an acknowledgement loss: the head
                                    ;; lands, then the caller observes a transport
                                    ;; error and cannot know the outcome.
                                    (original-assoc store key value
                                                    {:sync? (:sync? opts)})
                                    (throw (ex-info "forced transport failure"
                                                    {:type :test/transport-failure})))
                                  (apply original-assoc args))))]
                (writing/commit! db #{} true nil :current-revision))
              nil
              (catch clojure.lang.ExceptionInfo e e))]
        (is (re-find #"forced transport failure" (.getMessage failure)))
        (is (= [:prepare :unknown] (mapv first @events))
            "an ambiguous outcome is reported without authorizing deletion")
        (let [stored-head (k/get (:store before)
                                 (get-in before [:config :branch])
                                 nil {:sync? true})
              attempt-id (get-in (first @events) [1 :attempt-id])]
          (is (= attempt-id (:datahike/attempt-id (ex-data failure)))
              "the caller can correlate the failed operation with diagnostics")
          (is (= key-map
                 (get-in stored-head [:secondary-index-keys :idx/fake])))
          (is (= #{attempt-id}
                 (set (keys (writing/unresolved-secondary-publications))))
              "the preparation remains strongly reachable while its head is ambiguous")
          (is (true?
               (async/<!!
                (writing/reconcile-secondary-publication! attempt-id))))
          (is (= [:prepare :unknown :publish] (mapv first @events)))
          (is (empty? (writing/unresolved-secondary-publications)))))
      (finally
        (cleanup f)))))

(deftest partial-prepare-failure-aborts-earlier-preparations
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            first-key (generation-key-map
                       {:type :fake-prepared :generation (random-uuid)})
            failure (ex-info "second preparation failed"
                             {:type :test/prepare-failure})
            db (assoc before :secondary-indices
                      (array-map
                       :idx/first
                       (->FakeIndex events first-key
                                    (->FakePreparedIndex first-key)
                                    nil)
                       :idx/second (->FailingPrepareIndex events failure)))]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo
                              #"second preparation failed"
                              (writing/commit! db #{} true)))
        (is (= [:prepare :prepare-failed :abort]
               (mapv first @events)))
        (is (= :test/prepare-failure (second (last @events)))))
      (finally
        (cleanup f)))))

(deftest ambiguous-reconciliation-verifies-the-head-and-retries-cleanup
  (let [store-id (random-uuid)
        store (k/create-store {:backend :memory :id store-id} {:sync? true})
        attempt-id (random-uuid)
        expected-cid (random-uuid)
        wrong-cid (random-uuid)
        later-cid (random-uuid)
        key-maps {:idx/retry {:type :test/retry :generation (random-uuid)}}
        later-key-maps {:idx/retry {:type :test/retry
                                    :generation (random-uuid)}}
        attempts (atom 0)
        preparations {:idx/retry (->RetryingCleanupPreparation attempts
                                                               :committed)}
        outcome {:status :unknown
                 :attempt-id attempt-id
                 :store store
                 :branch :db
                 :primary-commit-id expected-cid
                 :secondary-index-keys key-maps}]
    (async/<!! (writing/release-secondary-generations! preparations outcome))
    (try
      (k/assoc store :db
               {:meta {:datahike/commit-id wrong-cid}
                :secondary-index-keys later-key-maps}
               {:sync? true})
      (is (false? (async/<!!
                   (writing/reconcile-secondary-publication! attempt-id)))
          "an unrelated primary head cannot authorize publication cleanup")
      (is (contains? (writing/unresolved-secondary-publications) attempt-id))
      (is (zero? @attempts))

      ;; The delayed write landed and a later commit advanced the branch.
      ;; Reconciliation proves the attempt through immutable ancestry rather
      ;; than requiring it to remain the exact current head forever.
      (k/assoc store expected-cid
               {:meta {:datahike/commit-id expected-cid
                       :datahike/parents #{wrong-cid}}
                :secondary-index-keys key-maps}
               {:sync? true})
      (k/assoc store later-cid
               {:meta {:datahike/commit-id later-cid
                       :datahike/parents #{expected-cid}}
                :secondary-index-keys later-key-maps}
               {:sync? true})
      (k/assoc store :db
               {:meta {:datahike/commit-id later-cid
                       :datahike/parents #{expected-cid}}
                :secondary-index-keys later-key-maps}
               {:sync? true})
      (is (false? (async/<!!
                   (writing/reconcile-secondary-publication! attempt-id)))
          "a failed completion remains retryable even though the head landed")
      (is (contains? (writing/unresolved-secondary-publications) attempt-id))
      (is (= 1 @attempts))
      (is (true? (async/<!!
                  (writing/reconcile-secondary-publication! attempt-id))))
      (is (= 2 @attempts))
      (is (not (contains? (writing/unresolved-secondary-publications)
                          attempt-id)))
      (finally
        ;; Keep the process-global diagnostics clean if an assertion throws.
        (when (contains? (writing/unresolved-secondary-publications) attempt-id)
          (async/<!!
           (writing/reconcile-secondary-publication! attempt-id)))))))

(deftest failed-abort-cleanup-remains-retryable
  (let [attempt-id (random-uuid)
        attempts (atom 0)
        preparations {:idx/retry (->RetryingCleanupPreparation attempts
                                                               :aborted)}
        outcome {:status :aborted
                 :attempt-id attempt-id
                 :branch :db}]
    (is (= #{:idx/retry}
           (set (keys
                 (:failures
                  (async/<!!
                   (writing/release-secondary-generations!
                    preparations outcome)))))))
    (is (= :aborted
           (get-in (writing/unresolved-secondary-publications)
                   [attempt-id :retry-status])))
    (is (true? (async/<!!
                (writing/reconcile-secondary-publication! attempt-id))))
    (is (= 2 @attempts))
    (is (not (contains? (writing/unresolved-secondary-publications)
                        attempt-id)))))

(deftest returned-db-retains-the-revision-created-by-a-fenced-head-write
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            {:keys [db]} (fake-db before events nil)
            original-assoc k/assoc
            committed
            (with-redefs [k/assoc
                          (fn [& args]
                            (let [opts (last args)]
                              (if (:expected-revision opts)
                                (let [[store key value _] args
                                      result (original-assoc
                                              store key value
                                              {:sync? (:sync? opts)})]
                                  [result :next-revision])
                                (apply original-assoc args))))]
              (writing/commit! db #{} true nil :current-revision))]
        (is (= :next-revision
               (:datahike.writing/head-revision committed)))
        (is (= [:prepare :publish] (mapv first @events))))
      (finally
        (cleanup f)))))

(deftest asynchronous-pre-head-failure-awaits-abort
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            {:keys [db]} (fake-db before events nil)
            failure (ex-info "async preparation must abort"
                             {:type :test/async-pre-head})
            result (with-redefs [writing/create-commit-id
                                 (fn [& _] (throw failure))]
                     (async/<!! (writing/commit! db #{} false)))]
        (is (= :test/async-pre-head (:type (ex-data result))))
        (is (= [:prepare :abort] (mapv first @events)))
        (is (= :test/async-pre-head (second (last @events)))))
      (finally
        (cleanup f)))))

(deftest post-head-online-gc-failure-does-not-reject-the-commit
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            {:keys [db]} (fake-db before events nil)
            db (assoc-in db [:config :online-gc :enabled?] true)
            failure (ex-info "forced online GC failure"
                             {:type :test/online-gc-failure})
            committed (with-redefs [online-gc/online-gc!
                                    (fn [& _] (delivered failure))]
                        (writing/commit! db #{} true))]
        (is (dbu/db? committed))
        (is (= [:prepare :publish] (mapv first @events))))
      (finally
        (cleanup f)))))

(deftest fatal-prepare-values-abort-in-sync-and-async-commits
  (doseq [sync? [true false]]
    (let [{:keys [connection] :as f} (fixture)]
      (try
        (let [events (atom [])
              before (d/db connection)
              first-key (generation-key-map
                         {:type :fake-prepared :generation (random-uuid)})
              fatal (AssertionError. "fatal preparation failure")
              db (assoc before :secondary-indices
                        (array-map
                         :idx/first
                         (->FakeIndex events first-key
                                      (->FakePreparedIndex first-key)
                                      nil)
                         :idx/fatal (->FailingPrepareIndex events fatal)))
              result (if sync?
                       (try
                         (writing/commit! db #{} true)
                         (catch Throwable e e))
                       (async/<!! (writing/commit! db #{} false)))]
          (is (= :fatal-commit-error (:type (ex-data result)))
              (str "fatal classification in sync?=" sync?))
          (is (some #(instance? AssertionError %)
                    (take-while some? (iterate ex-cause result))))
          (is (= [:prepare :prepare-failed :abort]
                 (mapv first @events))))
        (finally
          (cleanup f))))))

(deftest transient-index-is-never-persisted-or-prepared
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            db (assoc before :secondary-indices
                      {:idx/transient (->TransientIndex events)})
            committed (writing/commit! db #{} true)]
        (is (nil? (get-in committed [:secondary-index-keys :idx/transient])))
        (is (empty? @events)
            "transient indices have no durable lifecycle"))
      (finally
        (cleanup f)))))

(deftest preparation-rejects-and-aborts-an-invalid-live-view
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            before (d/db connection)
            root (random-uuid)
            key-map (generation-key-map
                     {:type :fake-prepared :generation root})
            db (assoc before :secondary-indices
                      {:idx/invalid-live
                       (->FakeIndex events key-map nil nil)})
            result (try
                     (writing/commit! db #{} true)
                     (catch Throwable e e))]
        (is (= :invalid-prepared-secondary-index
               (:type (ex-data result))))
        (is (= [:prepare :abort] (mapv first @events))))
      (finally
        (cleanup f)))))

(deftest preparation-rejects-an-unversioned-generation-envelope
  (let [{:keys [connection] :as f} (fixture)]
    (try
      (let [events (atom [])
            invalid-key-map {:type :fake-prepared :generation (random-uuid)}
            prepared-index (->FakePreparedIndex invalid-key-map)
            db (assoc (d/db connection) :secondary-indices
                      {:idx/invalid-envelope
                       (->FakeIndex events invalid-key-map prepared-index nil)})
            result (try
                     (writing/commit! db #{} true)
                     (catch Throwable failure failure))]
        (is (= :invalid-secondary-generation-key-map
               (:type (ex-data result))))
        (is (= [:prepare :abort] (mapv first @events))))
      (finally
        (cleanup f)))))
