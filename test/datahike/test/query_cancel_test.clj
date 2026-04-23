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
            [datahike.query :as q]))

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
       (= "57014" (:sqlstate (ex-data e)))
       (true? (:datahike/canceled (ex-data e)))))

(deftest cancel-nil-is-free
  (testing ":cancel nil (default) does not affect results"
    (binding [q/*force-legacy* false]
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
  (testing "pre-set cancel flag raises 57014 at first check point"
    (binding [q/*force-legacy* false]
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
    (binding [q/*force-legacy* false]
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
    (binding [q/*force-legacy* false]
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
    (binding [q/*force-legacy* false]
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
