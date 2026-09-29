(ns datahike.test.query-cancel-test
  "Mid-query cancellation via the :cancel channel.

   Covers:
   - Pre-set flag raises at the first check point (fast path)
   - Concurrent flip from a watchdog thread interrupts a live scan
   - :cancel nil and :cancel (volatile! false) are free — results flow
   - Direct-HashSet path, relation path (predicate forces fallback),
     and the adaptive execute-plan outer loop each observe the flag

   Runs against the query planner engine; legacy engine cancellation is
   not yet implemented (pgwire enables the planner, so it's not on the
   critical path)."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [datahike.query :as q]
            [datalog.parser.impl :as dpi]))

(def ^:dynamic ^:private *conn* nil)

(def ^:private cfg
  {:store {:backend :memory :id #uuid "cafe0001-0000-0000-0000-cace10000001"}
   :schema-flexibility :read
   :keep-history? false})

(defn- setup-db
  "Load N datoms into a fresh memory db. N=50k keeps a full scan in
   the tens of ms so cancellation timing is observable but the fixture
   isn't painfully slow."
  [n]
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn (into []
                           (mapcat (fn [i]
                                     [[:db/add (inc i) :x i]
                                      [:db/add (inc i) :y (str "v" i)]]))
                           (range n)))
    conn))

(defn- with-db-fixture [f]
  (try (d/delete-database cfg) (catch Exception _ nil))
  (binding [*conn* (setup-db 50000)
            ;; Disable the result cache — repeated identical queries
            ;; would otherwise hit the cache and bypass execute entirely,
            ;; which short-circuits the cancel check we're trying to exercise.
            q/*query-result-cache?* false]
    (try (f)
         (finally
           (d/release *conn*)
           (d/delete-database cfg)))))

(use-fixtures :each with-db-fixture)

(defn- cancel-exception? [e]
  (and (instance? clojure.lang.ExceptionInfo e)
       (true? (:datahike/canceled (ex-data e)))))

(deftest cancel-nil-is-free
  (testing ":cancel nil (default) does not affect results"
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            r1 (d/q '[:find ?e ?v :where [?e :x ?v]] db)
            r2 (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                     :args [db]
                     :cancel nil})
            r3 (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                     :args [db]
                     :cancel (volatile! false)})]
        (is (= 50000 (count r1)))
        (is (= r1 r2))
        (is (= r1 r3))))))

(deftest preset-cancel-raises-fast
  (testing "pre-set cancel flag raises :datahike/canceled at first check point"
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            thrown (try
                     (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                           :args [db]
                           :cancel (volatile! true)})
                     nil
                     (catch Exception e e))]
        (is (some? thrown))
        (is (cancel-exception? thrown))))))

(deftest concurrent-cancel-direct-path
  (testing "watchdog flip interrupts a live scan on the direct path"
    ;; Without timing budgets: the bound loop (dotimes 200) would take
    ;; 1-2s in 50k-row queries if cancel did nothing; the cancel flip
    ;; makes it bail at the first check-cancel! site on the next
    ;; iteration after the flag is observed. We only assert correctness
    ;; (cancel-exception raised) — not a specific upper bound on
    ;; elapsed time, which is JIT/GC-sensitive.
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            cancel (volatile! false)
            watchdog (future
                       (Thread/sleep 5)
                       (vreset! cancel true))
            thrown (try
                     (dotimes [_ 200]
                       (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                             :args [db]
                             :cancel cancel}))
                     nil
                     (catch Exception e e))]
        @watchdog
        (is (cancel-exception? thrown))))))

(deftest concurrent-cancel-relation-path
  (testing "cancel also fires on the relation path (predicate forces fallback)"
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            cancel (volatile! false)
            watchdog (future
                       (Thread/sleep 5)
                       (vreset! cancel true))
            thrown (try
                     (dotimes [_ 200]
                       (d/q {:query '[:find ?v1 ?v2
                                      :where [?e1 :x ?v1]
                                      [?e2 :x ?v2]
                                      [(< ?v1 ?v2)]]
                             :args [db]
                             :cancel cancel}))
                     nil
                     (catch Exception e e))]
        @watchdog
        (is (cancel-exception? thrown))))))

(deftest cancel-reset-allows-reuse
  (testing "vreset! cancel false → query runs to completion again"
    (binding [q/*disable-planner* false]
      (let [db (d/db *conn*)
            cancel (volatile! true)]
        (is (thrown? Exception
                     (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                           :args [db]
                           :cancel cancel})))
        (vreset! cancel false)
        (is (= 50000 (count (d/q {:query '[:find ?e ?v :where [?e :x ?v]]
                                  :args [db]
                                  :cancel cancel}))))))))

;; ---------------------------------------------------------------------------
;; A deadline has to stop the JOIN, not just be noticed once it finishes
;; ---------------------------------------------------------------------------

(defn- cross-join-db
  "Two attributes with no variable in common, so a query over both is a
   Cartesian product: |left| x |right| tuples with nothing to prune it."
  [n]
  (let [cfg2 {:store {:backend :memory :id (java.util.UUID/randomUUID)}
              :schema-flexibility :write :keep-history? false}]
    (d/create-database cfg2)
    (let [c (d/connect cfg2)]
      (d/transact c [{:db/ident :l/v :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
                     {:db/ident :r/v :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
      (d/transact c (vec (for [i (range n)] {:l/v i})))
      (d/transact c (vec (for [i (range n)] {:r/v i})))
      [cfg2 c])))

(def ^:private cross-query
  '[:find ?x ?y :where [?x :l/v _] [?y :r/v _]])

(def ^:private deadline-ms
  "Small on purpose. The fix bails at deadline + epsilon whatever the
   fixture size, so a SHORT deadline widens the gap between a query
   that stops and one that merely finishes and then notices -- without
   making the test slower. At 1000ms the reference engine's full
   1200x1200 product (~1.3-1.6s) fitted inside a 3x bound, so removing
   the in-loop check did not fail anything."
  250)

(def ^:private deadline-slack
  "How far past the deadline a query may run before the test fails.

   Tight on purpose. A loose bound is the whole reason the first
   version of these tests was worthless: they passed with the in-loop
   check stubbed out, because a query that simply RUNS TO COMPLETION
   and then hits the exit check also throws `:datahike/query-timeout`.
   Only the elapsed time can tell the two apart, and the unfixed paths
   took the full product runtime -- 1.3s and up on this fixture, 45s on
   the 3000x3000 one that started this."
  4)

(deftest timeout-stops-a-cartesian-join
  ;; The product loops built every combination and only then let
  ;; `run-with-query-timeout` notice the deadline on the way out.
  ;;
  ;; `-collect` is the one that matters: `rel/hash-join` is never
  ;; reached for DISJOINT relations, since `collapse-rels` only joins
  ;; when the attributes intersect. Checking in `hash-join` alone left
  ;; the deadline inert on exactly the shape that runs away.
  (let [[cfg2 c] (cross-join-db 1200)]
    (try
      (binding [q/*query-result-cache?* false]
        (doseq [planner? [true false]]
          (binding [q/*disable-planner* (not planner?)]
            (let [t0 (System/currentTimeMillis)
                  r (try (count (d/q {:query cross-query :args [(d/db c)]
                                      :timeout deadline-ms}))
                         (catch clojure.lang.ExceptionInfo e
                           (:type (ex-data e))))
                  elapsed (- (System/currentTimeMillis) t0)
                  label (if planner? "planner" "reference")]
              (is (= :datahike/query-timeout r) (str label " must time out"))
              (is (< elapsed (* deadline-slack deadline-ms))
                  (str label " ran " elapsed "ms for a " deadline-ms
                       "ms deadline -- it is completing the join and only "
                       "then noticing"))))))
      (finally (d/release c) (d/delete-database cfg2)))))

(deftest timeout-stops-the-shapes-that-bypass-the-cartesian-split
  ;; The planner only splits into independent components for a plain
  ;; relation find. `:keys`, `:with`, aggregates and pulls decline the
  ;; split and fall back to `-collect` -- so `:keys` alone was enough to
  ;; make the deadline inert on the DEFAULT engine after the first fix.
  (let [[cfg2 c] (cross-join-db 1200)]
    (try
      (binding [q/*query-result-cache?* false q/*disable-planner* false]
        ;; NOT `:with ?z` over the full fixture -- that is a 1200^3
        ;; product, 1.7e9 tuples, which takes the JVM down rather than
        ;; failing an assertion when the check is removed.
        (doseq [[label qry] [["(:keys)" '[:find ?x ?y :keys a b
                                          :where [?x :l/v _] [?y :r/v _]]]
                             ["(pull)" '[:find (pull ?x [*]) ?y
                                         :where [?x :l/v _] [?y :r/v _]]]
                             ["(order-by)" '[:find ?x ?y
                                             :where [?x :l/v _] [?y :r/v _]
                                             :order-by [?x :desc]]]]]
          (let [t0 (System/currentTimeMillis)
                r (try (count (d/q {:query qry :args [(d/db c)] :timeout deadline-ms}))
                       (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))
                elapsed (- (System/currentTimeMillis) t0)]
            (is (= :datahike/query-timeout r) (str label " must time out"))
            (is (< elapsed (* deadline-slack deadline-ms))
                (str label " ran " elapsed "ms for a " deadline-ms "ms deadline")))))
      (finally (d/release c) (d/delete-database cfg2)))))

(deftest a-timed-out-query-does-not-poison-the-result-cache
  ;; `:timeout` is not part of the cache key, and the result used to be
  ;; cached before the deadline was noticed -- so the next caller was
  ;; served the full result of a query that had been reported as timed
  ;; out, pinned in the LRU. Cache ON here, deliberately.
  (let [[cfg2 c] (cross-join-db 1200)]
    (try
      (binding [q/*disable-planner* false]
        (let [db (d/db c)
              probe '[:find ?x ?y :where [?x :l/v _] [?y :r/v _] [(identity 7) ?z]]
              _ (is (= :datahike/query-timeout
                       (try (count (d/q {:query probe :args [db] :timeout 300}))
                            (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))))
              t0 (System/currentTimeMillis)
              _ (try (d/q {:query probe :args [db]}) (catch Exception _ nil))
              elapsed (- (System/currentTimeMillis) t0)]
          (is (> elapsed 300)
              (str "the next call returned in " elapsed
                   "ms -- it was served the timed-out query's cached result"))))
      (finally (d/release c) (d/delete-database cfg2)))))

(deftest timeout-beside-the-query-is-honoured
  ;; `:timeout` was read only from INSIDE the query, so the envelope
  ;; spelling -- the same shape as the `:limit` / `:offset` that `q`'s
  ;; own docstring shows -- set no deadline and said nothing.
  (let [[cfg2 c] (cross-join-db 1200)]
    (try
      (binding [q/*query-result-cache?* false q/*disable-planner* false]
        (let [db (d/db c)]
          (testing "beside :query and :args"
            (is (= :datahike/query-timeout
                   (try (count (d/q {:query cross-query :args [db] :timeout 800}))
                        (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))
          (testing "inside a map query"
            (is (= :datahike/query-timeout
                   (try (count (d/q {:query (assoc (dpi/query->map cross-query) :timeout 800)
                                     :args [db]}))
                        (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))
          (testing "inside a vector query"
            (is (= :datahike/query-timeout
                   (try (count (d/q {:query (into cross-query [:timeout 800]) :args [db]}))
                        (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))))
      (finally (d/release c) (d/delete-database cfg2)))))

(deftest a-callers-cancel-stops-a-join-without-a-timeout
  ;; `:cancel` only reached the query when a timeout happened to be set
  ;; too, because the timeout path is what binds the cell the query
  ;; reads. Setting the cell on its own did nothing and the query ran
  ;; to completion.
  (let [[cfg2 c] (cross-join-db 1200)]
    (try
      (binding [q/*query-result-cache?* false q/*disable-planner* false]
        (let [db (d/db c)
              cell (volatile! nil)
              fut (future (try (count (d/q {:query cross-query :args [db] :cancel cell}))
                               (catch clojure.lang.ExceptionInfo e
                                 (if (:datahike/canceled (ex-data e)) :canceled :other))
                               (catch Exception _ :other)))]
          (Thread/sleep 400)
          (let [t0 (System/currentTimeMillis)
                _ (vreset! cell true)
                r (deref fut 20000 :timed-out-waiting)
                stopped-in (- (System/currentTimeMillis) t0)]
            (is (= :canceled r))
            ;; ELAPSED, not just the exception: without it this passed
            ;; with the in-loop check removed, because a query that runs
            ;; to completion and then hits the exit check is also
            ;; ":canceled". That left the "cancel stops the join
            ;; mid-flight" half of this untested.
            (is (< stopped-in 1500)
                (str "took " stopped-in "ms to notice the cell -- it is "
                     "finishing the join first")))))
      (finally (d/release c) (d/delete-database cfg2)))))

(deftest no-deadline-means-no-behaviour-change
  ;; The check is a nil test when nothing set a deadline. Results must
  ;; be identical with and without one.
  (let [[cfg2 c] (cross-join-db 60)]
    (try
      (binding [q/*query-result-cache?* false q/*disable-planner* false]
        (let [db (d/db c)
              plain (d/q {:query cross-query :args [db]})
              with-slack (d/q {:query cross-query :args [db] :timeout 120000})]
          (is (= 3600 (count plain)) "60 x 60")
          (is (= plain with-slack))))
      (finally (d/release c) (d/delete-database cfg2)))))
