(ns datahike.test.query-plan-check-test
  "Pins the composition contract of `lower/*check-plan*`.

   The slot holds ONE function. It used to be installed with
   `(alter-var-root … (constantly f))`, so the second checker to load evicted
   the first — and an evicted checker cannot fail: it reports a clean sweep
   while examining nothing. That is the worst possible failure mode for a
   correctness check, and it is invisible unless something counts.

   So two properties are pinned here: every installed check runs, and the
   coverage counter moves. CI asserts the counter separately, because
   `0 violations` and `0 plans examined` are otherwise indistinguishable."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q]
   [datahike.query.lower :as lower]
   [datahike.test.query-eqcheck :as eqcheck]))

(defn- fresh-db []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}])
      (d/transact conn [{:db/id 100 :name "alice"}])
      (d/db conn))))

(deftest every-installed-plan-check-runs
  (testing "a second install composes with the first instead of evicting it"
    (let [saved @#'lower/*check-plan*
          saved-stats @lower/plan-check-stats
          a (atom 0)
          b (atom 0)]
      (try
        (lower/add-plan-check! (fn [_plan] (swap! a inc)))
        (lower/add-plan-check! (fn [_plan] (swap! b inc)))
        ;; Two preconditions for a check to see anything, and BOTH have to be
        ;; forced: with-plan-checks clears the plan cache (a warm cache builds
        ;; no plan), and the planner must be on (the base engine builds no plan
        ;; at all). CI runs the whole suite a second time with
        ;; DATAHIKE_QUERY_PLANNER=false, where the unbound version of this test
        ;; failed for that second reason — the mechanism was fine, the test was
        ;; asserting against an engine that has no plans to check.
        (eqcheck/with-plan-checks
          (binding [q/*disable-planner* false]
            (d/q '[:find ?e ?n :where [?e :name ?n]] (fresh-db))))
        (is (pos? @a) "the first-installed check must still run")
        (is (pos? @b) "the second-installed check must run")
        (is (= @a @b) "both checks see the same plans")
        (is (pos? (:plans @lower/plan-check-stats))
            "the coverage counter distinguishes `no violations` from `nothing examined`")
        (finally
          ;; Restore exactly: other namespaces' checkers live in this slot.
          (alter-var-root #'lower/*check-plan* (constantly saved))
          (reset! lower/plan-check-stats saved-stats))))))

(deftest coverage-counter-detects-a-warm-cache
  (testing "checks installed but no plan built leaves the counter untouched"
    (let [saved @#'lower/*check-plan*
          saved-stats @lower/plan-check-stats]
      (try
        (lower/clear-plan-checks!)
        (lower/add-plan-check! (fn [_plan] nil))
        (is (zero? (:plans @lower/plan-check-stats))
            "installing a check examines nothing on its own — this is the state
             that used to be reported as a clean sweep")
        (finally
          (alter-var-root #'lower/*check-plan* (constantly saved))
          (reset! lower/plan-check-stats saved-stats))))))
