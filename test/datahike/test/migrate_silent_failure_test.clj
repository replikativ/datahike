(ns datahike.test.migrate-silent-failure-test
  "A closed channel is not an empty result.

   `go-try-` turns an escaping Exception into a channel VALUE, so `<?-` rethrows
   it and the caller sees the failure. That covers the ordinary case and is why
   the import path reads as if it were safe. It does not cover a channel that
   CLOSES: a bare `go` that throws, a `go-try-` whose throwable is a JVM `Error`
   or (on ClojureScript) not a `js/Error`, or a `>!` refusing to forward a nil.
   In all of those `<?-` yields `nil` — not an error, a value. The synchronous
   half has the same hole with a plain nil return.

   Each consumer then does something plausible and wrong:

   * `(reduce rf acc nil)` is `acc`, so a chunk that failed to read becomes an
     EMPTY chunk and the import continues and reports success, short by exactly
     that chunk. Measured before the fix, 20 source datoms in 5 chunks with one
     read failing: `{:verify? false}` → `{:datom-count 16, :verified? nil,
     :errors 0}` and 16 datoms in the restored database; `{:on-error :collect}`
     → `{:datom-count 16, :verified? false, :errors 0}`. The DEFAULT threw —
     but as `:import/verify-failed`, a count mismatch, which reads as \"this
     dump is corrupt\" when the dump was intact and the reader was not.
   * a nil `load-entities` report has no `:ex`, so the batch reports SUCCESS —
     `progress` fires with the full count and `:migration` becomes nil, which
     restarts the id map and leaves dangling refs in the restored database.
   * `(>! callback (<! res))` in the writer refuses the nil, throws inside a bare
     `go`, and the caller's promise never resolves at all.

   The point of these tests is not that a chunk read can fail — it is that
   failing must be DISTINGUISHABLE from returning nothing. They inject the empty
   result directly, because the throwable that produces one is by definition the
   kind the surrounding machinery cannot report."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :refer [<!! chan close!]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.migrate.store :as mstore]
            [datahike.tools :as dt]
            [datahike.writer :as dwriter]
            [konserve.store :as ks]
            [datahike.test.utils :as utils]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- mem-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

(defn- no-result
  "What \"failed without being able to say so\" looks like in each mode: a bare
   nil synchronously, a closed channel asynchronously. Both reach the consumer
   as nil, which is the whole problem."
  [sync?]
  (if sync? nil (doto (chan) (close!))))

(defn- cause-chain [^Throwable t]
  (take-while some? (iterate #(.getCause ^Throwable %) t)))

(defn- take-result [r]
  (if (instance? clojure.core.async.impl.channels.ManyToManyChannel r)
    (let [v (<!! r)]
      (if (instance? Throwable v) (throw v) v))
    r))

(defn- filled []
  (let [conn (utils/setup-db (mem-cfg))]
    (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}])
    (doseq [i (range 8)] (d/transact conn [{:n i}]))
    conn))

(defn- dump-to-store! [conn]
  (let [store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)}
                               {:sync? true})
        target {:store store :prefix "silent"}]
    (m/export-db @conn target {:history? true :sort? false :chunk-size 4})
    target))

;; ---------------------------------------------------------------------------

(deftest the-helper-says-what-went-wrong
  (testing "and passes falsey-but-present values through — a result of `false`
            or `0` is a result; only nil is the absence"
    (is (= false (dt/delivered! false {:op :x})))
    (is (= 0 (dt/delivered! 0 {:op :x})))
    (is (= [] (dt/delivered! [] {:op :x})))
    (let [e (try (dt/delivered! nil {:op :read-chunk :chunk "c-1"}) nil
                 (catch Exception e e))]
      (is (some? e) "nil throws")
      (is (= :async/no-result (:error (ex-data e))))
      (is (= "c-1" (:chunk (ex-data e))) "and the site names itself"))))

(deftest a-chunk-that-fails-to-read-is-not-an-empty-chunk
  (testing "THE case. Silently importing an empty chunk is data loss that
            reports success — and none of the dump's own integrity signals can
            catch it, because they all describe what was WRITTEN."
    (doseq [sync? [true false]]
      (let [src    (filled)
            target (dump-to-store! src)
            orig   mstore/read-chunk
            n      (atom 0)]
        (with-redefs [mstore/read-chunk
                      (fn [& a]
                        ;; fail exactly the second chunk, the way a transient
                        ;; backend error would
                        (if (= 2 (swap! n inc)) (no-result sync?) (apply orig a)))]
          (let [tgt (utils/setup-db (mem-cfg))
                r   (try {:ok (take-result (m/import-db tgt target {:sync? sync?}))}
                         (catch Exception e {:error (:error (ex-data e))})
                         (finally (teardown tgt)))]
            (is (= 2 @n) "precondition: more than one chunk, so one could be failed")
            (is (= :async/no-result (:error r))
                (str "the import must refuse, :sync? " sync?))
            (is (nil? (:ok r)) "and must not report a short success")))
        (teardown src)))))

(deftest the-switches-that-disable-verification-do-not-disable-this
  (testing "the count check is the only thing that noticed a lost chunk before,
            and it is optional: `:verify? false` skips it and `:on-error
            :collect` downgrades it to a report. Both then returned a short
            import as a SUCCESS. The guard sits at the read, so it does not
            depend on any of them."
    (doseq [o [{:verify? false} {:on-error :collect}]]
      (let [src    (filled)
            target (dump-to-store! src)
            orig   mstore/read-chunk
            n      (atom 0)]
        (with-redefs [mstore/read-chunk
                      (fn [& a] (if (= 2 (swap! n inc)) nil (apply orig a)))]
          (let [tgt (utils/setup-db (mem-cfg))
                r   (try {:ok (m/import-db tgt target (assoc o :sync? true))}
                         (catch Exception e {:error (:error (ex-data e))})
                         (finally (teardown tgt)))]
            (is (= :async/no-result (:error r)) (str "refused under " o))))
        (teardown src)))))

(deftest a-failed-read-during-verification-is-not-a-corrupt-dump
  (testing "`verify` folds over the same chunks. Before the fix a failed read
            made the fold short, which surfaces as a COUNT MISMATCH — the dump
            gets blamed for the reader's failure."
    (let [src    (filled)
          target (dump-to-store! src)
          orig   mstore/read-chunk
          n      (atom 0)]
      (with-redefs [mstore/read-chunk
                    (fn [& a] (if (= 2 (swap! n inc)) (no-result true) (apply orig a)))]
        (let [r (try {:ok (m/verify target)}
                     (catch Exception e {:error (:error (ex-data e))}))]
          (is (= :async/no-result (:error r))
              "verify names the read, not the data")))
      (teardown src))))

(deftest an-async-op-that-cannot-report-does-not-hang-the-caller
  (testing "the writer forwards a background op's result with
            `(>! callback (<! res))`. `>!` REFUSES nil, so a closed `res` throws
            inside a bare `go` — which closes that go's own channel and never
            delivers the callback. The caller's promise then never resolves at
            all: not an error, a HANG. `gc-storage` takes this branch on every
            call.

            The timeout is the assertion. Without the fix this test does not
            fail, it never returns."
    (let [cfg  (assoc (mem-cfg)
                      :writer {:backend :self
                               ;; an op that fails the way `go-try-` cannot report
                               :write-fn-map {'gc-storage! (fn [& _] (doto (chan) (close!)))}})
          conn (utils/setup-db cfg)
          f    (future (try (deref (d/gc-storage conn)) ::no-throw
                            (catch Exception e e)))
          v    (deref f 10000 ::timeout)]
      (is (not= ::timeout v) "the caller must be answered, one way or the other")
      (is (not= ::no-throw v) "and a silent failure is not a success")
      ;; Down the cause chain, not on `v` itself: a writer failure reaches the
      ;; caller through `throwable-promise`, whose deref wraps it in the
      ;; `ExecutionException` a CompletableFuture raises, and `throw-if-exception-`
      ;; then re-wraps THAT — reading its (nil) ex-data. Every writer error
      ;; arrives this way, not just this one.
      (when (instance? Exception v)
        (is (= :async/no-result (some :error (map ex-data (cause-chain v))))
            "with the failure named rather than inferred"))
      (teardown conn))))

(deftest a-batch-with-no-report-is-not-a-batch-that-applied
  (testing "a nil report carries no `:ex`, so the abort path had nothing to
            trigger on: `progress` fired with the batch's full count and
            `:migration` was replaced by nil, restarting the id map mid-import.

            Async only — synchronously the writer's promise is deref'd, and a
            promise that was never delivered blocks rather than yielding nil."
    (let [src    (filled)
          target (dump-to-store! src)
          calls  (atom 0)]
      (with-redefs [dwriter/load-entities
                    (fn [& _] (swap! calls inc) (no-result false))]
        (let [tgt (utils/setup-db (mem-cfg))
              r   (try {:ok (take-result (m/import-db tgt target {:sync? false :batch-size 4}))}
                       (catch Exception e {:error (:error (ex-data e))})
                       (finally (teardown tgt)))]
          (is (pos? @calls) "precondition: batches really were applied this way")
          (is (= :async/no-result (:error r)) "and the import refuses")
          (is (nil? (:ok r)) "rather than reporting the batch as loaded")))
      (teardown src))))
