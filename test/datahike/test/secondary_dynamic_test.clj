(ns datahike.test.secondary-dynamic-test
  "Tests for dynamic secondary index creation via schema transactions."
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datahike.api :as d]
   [datahike.gc-guard :as guard]
   [datahike.gc-roots :as roots]
   [konserve.core :as k]
   [datahike.index.secondary :as sec]
   [datahike.migrate.fs :as fs]
   [datahike.writing :as writing]
   [superv.async :refer [<?? S]]))

(defonce slow-build-control (atom nil))

(defrecord SlowImmutableIndex [attrs events values]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] attrs)
  (-transact [this {:keys [datom added?] :as tx-report}]
    (when-let [{:keys [entered release blocked?]} @slow-build-control]
      (when (compare-and-set! blocked? false true)
        (deliver entered true)
        @release))
    (let [pair [(:e datom) (:v datom)]]
      (assoc this
             :events (conj events tx-report)
             :values ((if added? conj disj) values pair))))

  clojure.lang.IDeref
  (deref [_] {:events events :values values}))

(defonce _register-slow-immutable
  (sec/register-index-type!
   :test/slow-immutable
   (fn [config _db]
     (->SlowImmutableIndex (set (:attrs config)) [] #{}))))

(defrecord VersionedRecorder [attrs flushes restores]
  sec/ISecondaryIndex
  (-search [_ _ _] nil)
  (-estimate [_ _] 0)
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] attrs)
  (-transact [this _] this)

  sec/IVersionedSecondaryIndex
  (-sec-flush [_ _ _]
    (swap! flushes inc)
    {:type :test/versioned-recorder :root :complete})
  (-sec-restore [this _ _]
    (swap! restores inc)
    this)
  (-sec-branch [this _ _ _] this)
  (-sec-mark [_] #{}))

(defonce versioned-recorder-control
  (atom {:flushes (atom 0) :restores (atom 0)}))

(defonce _register-versioned-recorder
  (sec/register-index-type!
   :test/versioned-recorder
   (fn [config _db]
     (let [{:keys [flushes restores]} @versioned-recorder-control]
       (->VersionedRecorder (set (:attrs config)) flushes restores)))))

(defn- await-status [conn idx-ident status]
  (let [deadline (+ (System/currentTimeMillis) 5000)]
    (loop []
      (let [actual (get-in (d/db conn) [:schema idx-ident :db.secondary/status])]
        (cond
          (= status actual) actual
          (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 10) (recur))
          :else actual)))))

(defn- throwable-type [error]
  (some (comp :type ex-data) (take-while some? (iterate ex-cause error))))

;; Register a test recorder index type for all tests
(defonce _register-recorder
  (sec/register-index-type!
   :test/dynamic-recorder
   (fn [config _db]
     (let [state (atom [])]
       (reify
         sec/ISecondaryIndex
         (-search [_ _ _] nil)
         (-estimate [_ _] 0)
         (-can-order? [_ _ _] false)
         (-slice-ordered [_ _ _ _ _ _] nil)
         (-indexed-attrs [_] (set (:attrs config)))
         (-transact [this tx-report]
           (swap! state conj tx-report)
           this)

         sec/ITransientSecondaryIndex
         (-as-transient [this] this)
         (-transact! [this tx-report]
           (swap! state conj tx-report))
         (-persistent! [this] this)

         clojure.lang.IDeref
         (deref [_] @state))))))

(deftest test-dynamic-secondary-index-creation
  (testing "schema transaction creates secondary index with :building status"
    (let [cfg {:store {:backend :memory
                       :id (random-uuid)}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        ;; First, define the attribute we want to index
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        ;; Add some data before creating the index
        (d/transact conn [{:person/name "Alice"}
                          {:person/name "Bob"}])

        ;; Now dynamically add a secondary index
        (d/transact conn [{:db/ident :idx/recorder
                           :db.secondary/type :test/dynamic-recorder
                           :db.secondary/attrs [:person/name]}])

        ;; Give the backfill writer op time to complete
        (Thread/sleep 500)

        (let [db (d/db conn)
              schema (:schema db)
              idx-schema (get schema :idx/recorder)]
          ;; Index should exist in secondary-indices
          (is (some? (get-in db [:secondary-indices :idx/recorder]))
              "Index instance should be created")
          ;; rschema should map :person/name -> #{:idx/recorder}
          (is (contains? (get-in db [:rschema :db.secondary/index :person/name]) :idx/recorder)
              "rschema should map attribute to index")
          ;; Status should be :ready after backfill
          (is (= :ready (:db.secondary/status idx-schema))
              "Index should be :ready after backfill")
          ;; Backfill should have fed existing datoms
          (let [idx (get-in db [:secondary-indices :idx/recorder])
                recorded @idx]
            (is (>= (count recorded) 2)
                "Backfill should have fed at least 2 existing datoms")))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest test-new-transactions-feed-into-building-index
  (testing "transactions after index creation feed datoms into the index"
    (let [cfg {:store {:backend :memory
                       :id (random-uuid)}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])

        ;; Create the secondary index
        (d/transact conn [{:db/ident :idx/recorder2
                           :db.secondary/type :test/dynamic-recorder
                           :db.secondary/attrs [:person/name]}])

        ;; Wait for backfill to complete
        (Thread/sleep 500)

        ;; Now transact new data — should feed into the index
        (d/transact conn [{:person/name "Charlie"}
                          {:person/name "Diana"}])

        (let [db (d/db conn)
              idx (get-in db [:secondary-indices :idx/recorder2])
              recorded @idx]
          ;; Should have received at least the 2 new datoms
          (is (>= (count (filter :added? recorded)) 2)
              "New datoms should feed into the index after creation"))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest test-secondary-index-status-ready
  (testing "secondary index reaches :ready after backfill"
    (let [cfg {:store {:backend :memory
                       :id (random-uuid)}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])

        ;; Create the secondary index and wait for backfill
        (d/transact conn [{:db/ident :idx/recorder3
                           :db.secondary/type :test/dynamic-recorder
                           :db.secondary/attrs [:person/name]}])
        (Thread/sleep 500)

        ;; Verify it's ready
        (is (= :ready (get-in (d/db conn) [:schema :idx/recorder3 :db.secondary/status])))

        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest asynchronous-backfill-replays-concurrent-deltas
  (testing "the writer remains available and immutable index updates are not lost"
    ;; A file store, not :memory: the collector is exercised DURING the scan
    ;; below, and a :memory store's index roots are never flushed, so its mark
    ;; cannot run at all ("Index needs to be properly flushed before marking").
    (let [cfg {:store {:backend :file
                       :id (random-uuid)
                       :path (fs/temp-store-path! "datahike-sec-deltas-")}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false
               :schema-flexibility :write}
          entered (promise)
          release (promise)
          control {:entered entered :release release :blocked? (atom false)}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (try
          (d/transact conn [{:db/ident :person/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}])
          (d/transact conn [{:person/name "Alice"} {:person/name "Bob"}])
          (let [alice (d/q '[:find ?e . :where [?e :person/name "Alice"]]
                           (d/db conn))]
            (reset! slow-build-control control)
            ;; This returns after publishing :building, not after the scan.
            (d/transact conn [{:db/ident :idx/slow
                               :db.secondary/type :test/slow-immutable
                               :db.secondary/attrs [:person/name]}])
            (is (= true (deref entered 5000 ::timeout))
                "the background scan reached its deterministic barrier")
            (is (= :building
                   (get-in (d/db conn) [:schema :idx/slow :db.secondary/status])))
            (let [store (:store @conn)
                  head (k/get store :db nil {:sync? true})
                  pins (vals (<?? S (roots/roots store)))
                  pinned (when (= 1 (count pins))
                           (k/get store (:record-key (first pins)) nil {:sync? true}))]
              (is (= 1 (count pins))
                  "the scanned snapshot is pinned with a durable root during the scan")
              (is (= :pin (:kind (first pins))))
              ;; Compared by index roots, not commit-id: the db a writer op
              ;; receives can carry a lagging :meta, and what the pin must
              ;; protect is the trees the scan reads.
              (is (= (select-keys head [:eavt-key :aevt-key :avet-key])
                     (select-keys pinned [:eavt-key :aevt-key :avet-key]))
                  "and the root names the very trees the scan is reading")
              ;; Offline GC no longer refuses; it collects around the pin. The
              ;; scan then completes against the pinned snapshot below, which
              ;; is the proof the pin held.
              (is (set? (<?? S (d/gc-storage conn (java.util.Date.))))
                  "offline GC runs during the scan instead of raising"))

            ;; Both transactions complete while the backfill is paused. The
            ;; card-one replacement contributes a retract and an assertion.
            (d/transact conn [[:db/add alice :person/name "Alicia"]])
            (d/transact conn [{:person/name "Charlie"}])
            (is (= 3 (count (get-in (d/db conn)
                                    [:secondary-index-build-deltas :idx/slow])))
                "concurrent changes are journaled instead of racing the build")
            ;; NOW the head has moved past the scanned snapshot, so with
            ;; `(Date.)` as remove-before that snapshot's own nodes are garbage
            ;; to everything but the pin. A collector that ignored the registry
            ;; would sweep them here and the scan below could not finish.
            (is (set? (<?? S (d/gc-storage conn (java.util.Date.))))
                "a full-range collection with the head advanced still succeeds")

            (deliver release true)
            (is (= :ready (await-status conn :idx/slow :ready)))
            (let [{:keys [values]} @(get-in (d/db conn)
                                            [:secondary-indices :idx/slow])]
              (is (= #{"Alicia" "Bob" "Charlie"}
                     (set (map second values))))
              (is (nil? (:secondary-index-build-deltas (d/db conn)))
                  "the install removes its in-memory delta journal")
              (is (not (guard/in-flight? (get-in cfg [:store :id])))
                  "the ready commit releases the build's GC guard")
              (is (empty? (<?? S (roots/roots (:store @conn))))
                  "the ready commit releases the snapshot's durable root")))
          (finally
            (reset! slow-build-control nil)
            (deliver release true)
            (d/release conn)
            (d/delete-database cfg)))))))

(deftest building-versioned-index-is-not-published
  (testing "partial building state is neither flushed nor carried as durable authority"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          flushes (atom 0)
          restores (atom 0)]
      (try
        (reset! versioned-recorder-control
                {:flushes flushes :restores restores})
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (let [idx (->VersionedRecorder #{:person/name} flushes restores)
              building (-> (d/db conn)
                           (assoc-in [:schema :idx/versioned]
                                     {:db.secondary/type :test/versioned-recorder
                                      :db.secondary/attrs [:person/name]
                                      :db.secondary/status :building
                                      :db.secondary/building-since-tx 1})
                           (assoc-in [:secondary-indices :idx/versioned] idx)
                           (assoc :secondary-index-keys
                                  {:idx/versioned {:type :test/versioned-recorder
                                                   :root :partial}}))
              stored (second (writing/db->stored building true))]
          (is (zero? @flushes))
          (is (nil? (:secondary-index-keys stored)))
          (is (nil? (get-in stored [:merkle-roots :secondary]))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest release-during-backfill-releases-gc-guard
  (testing "a scan finishing after its local writer shuts down does not strand a guard"
    (let [store-id (random-uuid)
          cfg {:store {:backend :memory :id store-id}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false
               :schema-flexibility :write}
          entered (promise)
          release-build (promise)]
      (d/create-database cfg)
      (let [conn (d/connect cfg)
            store (:store @conn)]
        (try
          (d/transact conn [{:db/ident :person/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}
                            {:person/name "Alice"}])
          (reset! slow-build-control
                  {:entered entered
                   :release release-build
                   :blocked? (atom false)})
          (d/transact conn [{:db/ident :idx/released
                             :db.secondary/type :test/slow-immutable
                             :db.secondary/attrs [:person/name]}])
          (is (= true (deref entered 5000 ::timeout)))
          (d/release conn)
          (deliver release-build true)
          (let [deadline (+ (System/currentTimeMillis) 5000)]
            (loop []
              (when (and (guard/in-flight? store-id)
                         (< (System/currentTimeMillis) deadline))
                (Thread/sleep 10)
                (recur))))
          (is (not (guard/in-flight? store-id)))
          (is (empty? (<?? S (roots/roots store)))
              "nor a durable root")
          (finally
            (reset! slow-build-control nil)
            (deliver release-build true)
            (d/release conn)
            (d/delete-database cfg)))))))

(deftest backfill-refuses-a-shared-writer
  (testing "another process cannot bypass the in-memory delta journal"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :writer {:backend :self :writer-ownership :shared}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:person/name "Alice"}])
        (let [error (try
                      (d/transact conn [{:db/ident :idx/shared
                                         :db.secondary/type :test/slow-immutable
                                         :db.secondary/attrs [:person/name]}])
                      (catch clojure.lang.ExceptionInfo e e))
              error-type (throwable-type error)]
          (is (= :secondary-index-backfill-unsupported-writer
                 error-type))
          (is (nil? (get-in (d/db conn) [:schema :idx/shared]))
              "the unsupported schema transaction is not committed"))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest recovery-after-crash-scans-the-journaled-transactions
  (testing "a datom journaled by the crashed process is indexed on reconnect"
    (let [path (fs/temp-store-path! "datahike-sec-crash-")
          cfg {:store {:backend :file
                       :id (java.util.UUID/randomUUID)
                       :path path}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false
               :schema-flexibility :write}
          entered (promise)
          release (promise)
          control {:entered entered :release release :blocked? (atom false)}]
      (d/create-database cfg)
      (try
        (let [conn (d/connect cfg)]
          (d/transact conn [{:db/ident :person/name
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}])
          (d/transact conn [{:person/name "Alice"} {:person/name "Bob"}])
          (reset! slow-build-control control)
          (d/transact conn [{:db/ident :idx/slow
                             :db.secondary/type :test/slow-immutable
                             :db.secondary/attrs [:person/name]}])
          (is (= true (deref entered 5000 ::timeout)))
          ;; Committed while the scan is paused: journaled in memory only.
          (d/transact conn [{:person/name "Carol"}])
          (is (= 1 (count (get-in (d/db conn)
                                  [:secondary-index-build-deltas :idx/slow]))))
          ;; The process stops before install; the journal dies with it.
          (d/release conn)
          (deliver release true)
          (reset! slow-build-control nil))
        (let [conn2 (d/connect cfg)]
          (try
            (is (= :ready (await-status conn2 :idx/slow :ready)))
            (is (= #{"Alice" "Bob" "Carol"}
                   (set (map second
                             (:values @(get-in (d/db conn2)
                                               [:secondary-indices :idx/slow])))))
                "the rebuilt index covers the transaction the lost journal held")
            (finally
              (d/release conn2))))
        (finally
          (reset! slow-build-control nil)
          (deliver release true)
          (d/delete-database cfg))))))

(deftest test-secondary-index-recovery-on-reconnect
  (testing "secondary index in :building state is recovered after reconnect"
    (let [path (fs/temp-store-path! "datahike-sec-recovery-")
          cfg {:store {:backend :file
                       :id (java.util.UUID/randomUUID)
                       :path path}
               :writer {:backend :self :writer-ownership :exclusive}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:person/name "Alice"} {:person/name "Bob"}])

        ;; Create secondary index and release immediately.
        ;; d/transact returns after the schema tx commits (with :building status).
        ;; The backfill + install runs asynchronously after that.
        ;; Releasing right away shuts down the writer before install can run,
        ;; leaving :building status on disk.
        (d/transact conn [{:db/ident :idx/recorder-recover
                           :db.secondary/type :test/dynamic-recorder
                           :db.secondary/attrs [:person/name]}])
        (d/release conn)

        ;; Reconnect — recovery should detect :building (no stored key-map
        ;; since the recorder type doesn't implement IVersionedSecondaryIndex)
        ;; and run build + install to transition to :ready.
        (let [conn2 (d/connect cfg)]
          (try
            (Thread/sleep 1000)
            (is (= :ready
                   (get-in (d/db conn2) [:schema :idx/recorder-recover :db.secondary/status]))
                "Recovery should transition index from :building to :ready")
            (finally
              (d/release conn2))))
        (finally
          (d/delete-database cfg))))))
