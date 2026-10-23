(ns datahike.test.migrate-writer-timeout-test
  "An import must not wait forever on a writer that cannot answer, and must not
   read a JVM `Error` as a batch report.

   Both come from the same seam (#971, `.internal/ASYNC_WRITER_ERROR_HANG`):

   * superv.async's `<?-` rethrows `Exception`s only, so an `Error` the writer
     delivered arrived as a VALUE, and the import continued on it and ended in
     a successful-looking report;
   * an `Error` inside the commit pipeline's nested go blocks escapes without
     reaching the caller's callback, and a wait without a bound never returns —
     the hang a production import reported.

   `:writer-timeout-ms` bounds every wait on the writer. The tests inject both
   a writer that never answers and a real `Error` thrown from a store write in
   the middle of a commit."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.constants :as const]
            [datahike.migrate :as m]
            [datahike.migrate.fs :as fs]
            [datahike.tools :as dt]
            [datahike.writer :as dwriter]
            [konserve.core :as k]))

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
              (is (= :writer/timeout (:cause-error (ex-data r))) "and said why")))
          (finally (teardown conn)))))))

(deftest an-error-from-the-writer-is-not-a-report
  (testing "#971: an `Error` delivered by the writer passed `<?-` as a value"
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
              (is (instance? Throwable r)
                  (str ":sync? " sync? ": the import fails rather than reporting " (pr-str r)))))
          (finally (teardown conn)))))))

(deftest an-error-inside-a-commit-does-not-hang-the-import
  (testing "A real `Error` from a store write in the commit pipeline. Whether
            the writer manages to deliver it or not, the import must come back:
            with the Error, or with `:writer/timeout`."
    (doseq [sync? [true false]]
      (let [conn (fresh :file)
            writes (atom 0)
            orig-assoc k/assoc
            orig-multi k/multi-assoc]
        (try
          (let [r (with-redefs [k/assoc (fn
                                          ([s key v] (orig-assoc s key v))
                                          ([s key v o] (orig-assoc s key v o))
                                          ([s key v meta o]
                                           (when (and (map? meta) (:immutable? meta)
                                                      (> (swap! writes inc) 30))
                                             (throw (AssertionError. "injected: store write")))
                                           (orig-assoc s key v meta o)))
                                k/multi-assoc (fn [s kvs metas o]
                                                (when (> (swap! writes inc) 3)
                                                  (throw (AssertionError. "injected: store write")))
                                                (orig-multi s kvs metas o))]
                    (bounded 30000 #(import! conn {:sync? sync? :writer-timeout-ms 5000})))]
            (is (not= ::hung r) (str ":sync? " sync? ": the import came back"))
            (is (instance? Throwable r) (str ":sync? " sync? ": as a failure, got " (pr-str r))))
          (finally (teardown conn)))))))

(deftest a-timed-deref-returns-its-timeout-value
  (testing "`IBlockingDeref` promises the timeout value; `throwable-promise`
            threw the `TimeoutException` instead"
    (is (= ::to (deref (dt/throwable-promise) 50 ::to)))
    (is (= 1 (deref (doto (dt/throwable-promise) (deliver 1)) 50 ::to)))
    (is (thrown? AssertionError
                 (deref (doto (dt/throwable-promise) (deliver (AssertionError. "x"))) 50 ::to)))))
