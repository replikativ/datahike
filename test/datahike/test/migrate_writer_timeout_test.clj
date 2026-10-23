(ns datahike.test.migrate-writer-timeout-test
  "An import must not wait forever on a writer that cannot answer, and must not
   read a JVM `Error` as a batch report.

   Both come from the same seam (#971):

   * superv.async's `<?-` rethrows `Exception`s only, so an `Error` the writer
     delivered arrived as a VALUE, and the import continued on it and ended in
     a successful-looking report;
   * a writer that loses an `Error` before its callback never answers, and a
     wait without a bound never returns — the hang a production import
     reported.

   `:writer-timeout-ms` bounds every wait on the writer. A timed-out write may
   still land, so the import keeps its GC protection until it resolves."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.constants :as const]
            [datahike.gc-guard :as guard]
            [datahike.gc-roots :as roots]
            [datahike.store :as dh-store]
            [datahike.migrate :as m]
            [datahike.migrate.fs :as fs]
            [datahike.tools :as dt]
            [datahike.writer :as dwriter]
            [superv.async :refer [<?-]]))

(defn- records []
  (let [t0 (inc const/tx0)]
    (into [[t0 :db/txInstant #inst "2021-01-01" t0 true]
           [100 :db/ident :name t0 true]
           [100 :db/valueType :db.type/string t0 true]
           [100 :db/cardinality :db.cardinality/one t0 true]]
          (mapcat (fn [k]
                    (let [t (+ t0 1 k)]
                      (into [[t :db/txInstant (java.util.Date. (+ 1609459200000 (* 1000 (inc k)))) t true]]
                            (for [i (range 10)] [(+ 1000 (* 10 k) i) :name (str "n" k "-" i) t true])))))
          (range 20))))

(defn- fresh [backend]
  (let [cfg (cond-> {:store {:backend backend :id (java.util.UUID/randomUUID)}
                     :keep-history? true :schema-flexibility :write}
              (= :file backend) (assoc-in [:store :path] (fs/temp-store-path! "dh-writer-timeout-")))]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- teardown [conn]
  (let [cfg (:config @conn)]
    (try (d/release conn) (catch Throwable _ nil))
    (try (d/delete-database cfg) (catch Throwable _ nil))))

(defn- import! [conn opts]
  (let [r (m/import-source conn (m/records->chunk-src (records) 30)
                           (merge {:verify? false :batch-size 25} opts))]
    (if (:sync? opts) r (async/<!! r))))

(defn- bounded
  "Run `f` on another thread; give up after `ms`. `::hung` if it never returned."
  [ms f]
  (let [fut (future (try (f) (catch Throwable t t)))
        v (deref fut ms ::hung)]
    (when (= ::hung v) (future-cancel fut))
    v))

(defn- cause-chain [^Throwable t]
  (take-while some? (iterate #(.getCause ^Throwable %) t)))

(deftest a-writer-that-never-answers-times-out
  (doseq [sync? [true false]]
    (testing (str ":sync? " sync?)
      (let [conn (fresh :memory)
            calls (atom 0)
            orig dwriter/load-entities-migrating]
        (try
          (with-redefs [dwriter/load-entities-migrating
                        (fn [& args]
                          (if (= 2 (swap! calls inc))
                            (dt/throwable-promise)  ; never delivered: a wedged writer
                            (apply orig args)))]
            (let [r (bounded 20000 #(import! conn {:sync? sync? :writer-timeout-ms 500}))]
              (is (not= ::hung r) "the import returned")
              (is (instance? Throwable r))
              ;; an infrastructure failure, not a record's: `:import/apply-failed`
              ;; carrying the cause, never `:import/corrupt-datom`
              (is (= :import/apply-failed (:error (ex-data r))))
              (is (= :writer/timeout (:cause-error (ex-data r))) "and said why")
              (is (re-find #"reconnect before resuming" (ex-message r))
                  "and that the batch may still land, so a resume needs a fresh connection")))
          (finally (teardown conn)))))))

(deftest an-error-from-the-writer-is-not-a-report
  (testing "#971: an `Error` delivered by the writer passed `<?-` as a value.
            On main the async import returned `:async/no-result` (the go block
            died on the rethrown Error) and the sync one let a bare
            AssertionError escape unclassified."
    (doseq [sync? [true false]]
      (let [conn (fresh :memory)
            calls (atom 0)
            orig dwriter/load-entities-migrating]
        (try
          (with-redefs [dwriter/load-entities-migrating
                        (fn [& args]
                          (if (= 2 (swap! calls inc))
                            (doto (dt/throwable-promise) (deliver (AssertionError. "injected")))
                            (apply orig args)))]
            (let [r (bounded 20000 #(import! conn {:sync? sync?}))]
              (is (instance? clojure.lang.ExceptionInfo r)
                  (str ":sync? " sync? ": the import fails rather than reporting " (pr-str r)))
              (is (= :import/apply-failed (:error (ex-data r))) (str ":sync? " sync?))
              (is (some #(instance? AssertionError %) (cause-chain r))
                  (str ":sync? " sync? ": with the Error as a cause"))))
          (finally (teardown conn)))))))

(deftest an-error-taken-from-a-datahike-promise-is-raised
  (testing "#971 at the promise: superv.async's `<?-` rethrows only `Exception`s,
            so a parked taker of a datahike promise got a delivered `Error` back
            as a VALUE. It is raised now, as an ex-info carrying the Error."
    (doseq [deliver! [(fn [p e] (deliver p e))
                      (fn [p e] (async/put! p e))]]
      (let [p (dt/throwable-promise)
            _ (deliver! p (AssertionError. "injected"))
            r (async/<!! (async/go (try (<?- p) :returned-a-value
                                        (catch Exception e e))))]
        ;; superv.async rewraps an Exception on the way out of `<?-`, so the
        ;; Error is further down the chain than one `ex-cause`.
        (is (instance? clojure.lang.ExceptionInfo r) (str "got " (pr-str r)))
        (is (= :async/error (dt/ex-error r)))
        (is (some #(instance? AssertionError %) (cause-chain r))))
      (testing "and `deref` still throws the Error itself"
        (let [p (dt/throwable-promise)]
          (deliver! p (AssertionError. "injected"))
          (is (thrown? AssertionError @p)))))))

(deftest a-timed-out-publish-keeps-its-gc-protection
  (testing "The index build's publish timed out, but it is still queued and may
            land. Releasing the build's guard and root then lets a collector
            delete the trees the late publish names."
    (let [conn (fresh :memory)
          store (:store @conn)
          store-id (dh-store/canonical-store-id store (get-in @conn [:config :store]))
          held (promise)
          orig dwriter/publish-built-db!
          checkpoints (fn [] (->> (roots/roots store {:sync? true}) vals
                                  (filter #(= :checkpoint (:kind %)))))]
      (try
        (let [r (with-redefs [dwriter/publish-built-db!
                              (fn [c fields]
                                (let [p (dt/throwable-promise)]
                                  (deliver held [c fields p])
                                  p))]
                  ;; `:schema {}`: the records carry their own schema datoms
                  ;; and no refs; the build path refuses without one.
                  (bounded 30000 #(import! conn {:sync? true :build-indexes? true :schema {}
                                                 :writer-timeout-ms 300})))]
          (is (instance? clojure.lang.ExceptionInfo r) (str "got " (pr-str r)))
          (is (= :import/apply-failed (:error (ex-data r))) "a publish timeout is an apply failure")
          (is (= :writer/timeout (:cause-error (ex-data r))))
          (is (guard/in-flight? store-id) "the guard is still held")
          (is (seq (checkpoints)) "the build's GC root is still registered")
          ;; A collector runs while the publish is pending.
          (async/<!! (d/gc-storage conn (java.util.Date.) {:min-age-ms 0}))
          ;; The late publish lands.
          (let [h (deref held 1000 nil)]
            (is h "the import reached the publish")
            (when-let [[c fields p] h]
              (p (deref (orig c fields) 20000 ::late-publish-hung))))
          (is (= 200 (count (d/q '[:find ?e :where [?e :name _]] @conn)))
              "the late publish names trees the collector spared")
          (let [deadline (+ (System/currentTimeMillis) 10000)]
            (while (and (or (guard/in-flight? store-id) (seq (checkpoints)))
                        (< (System/currentTimeMillis) deadline))
              (Thread/sleep 50)))
          (is (not (guard/in-flight? store-id)) "the guard is released once the publish resolved")
          (is (empty? (checkpoints)) "and so is the root"))
        (finally (teardown conn))))))

(deftest a-timed-deref-returns-its-timeout-value
  (testing "`IBlockingDeref` promises the timeout value; `throwable-promise`
            threw the `TimeoutException` instead"
    (is (= ::to (deref (dt/throwable-promise) 50 ::to)))
    (is (= 1 (deref (doto (dt/throwable-promise) (deliver 1)) 50 ::to)))
    (is (thrown? AssertionError
                 (deref (doto (dt/throwable-promise) (deliver (AssertionError. "x"))) 50 ::to))))
  (testing "a promise delivered nil or false returns that, not the timeout value"
    (is (nil? (deref (doto (dt/throwable-promise) (deliver nil)) 50 ::to)))
    (is (false? (deref (doto (dt/throwable-promise) (deliver false)) 50 ::to)))))
