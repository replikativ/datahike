(ns datahike.test.query-limit-demand-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]
   [datahike.query.execute :as execute]))

(def ^:private probe-count 100)

(defn- fixture-db
  ([] (fixture-db probe-count))
  ([n]
   (d/db-with
    (db/empty-db nil {:schema-flexibility :write
                      :keep-history? true})
    (into
     [{:db/ident :probe/n
       :db/valueType :db.type/long
       :db/cardinality :db.cardinality/one
       :db/index true}
      {:db/ident :probe/group
       :db/valueType :db.type/keyword
       :db/cardinality :db.cardinality/one
       :db/index true}]
     (map (fn [i]
            {:db/id (+ 1000 i)
             :probe/n i
             :probe/group (if (< i 50) :x :y)})
          (range n))))))

;; The cell counts EVERY cancellation check the query makes, not only the
;; scan candidates it pulls. Before a caller's `:cancel` was honoured
;; without a timeout (`raw-q`), only `execute-pattern-scan` ever read it,
;; so the count WAS the candidate count. It no longer is: each plan shape
;; also pays a small constant for the per-stage checks in `query.cljc`.
;;
;; What these tests assert is the term that depends on the fixture size.
;; `demand-is-flat-in-the-fixture-size` below pins that directly, so a
;; future check site moves a constant here and nothing else.
(defn- counting-cancel []
  (let [derefs (atom 0)]
    [derefs
     (reify clojure.lang.IDeref
       (deref [_]
         (swap! derefs inc)
         false))]))

(defn- demand-for
  "Runs `f` against a fixture of `n` entities and returns the deref count."
  [n f]
  (let [[derefs cancel] (counting-cancel)]
    (f (fixture-db n) cancel)
    @derefs))

(defn- uncached-q [query-map]
  (binding [q/*disable-planner* false
            q/*query-result-cache?* false]
    (d/q query-map)))

(defn strict-less-than?
  [left right]
  (< left right))

(deftest direct-limit-demand
  (let [db (fixture-db)]
    (testing "an unordered distinct scan stops after offset + limit tuples"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e :where [?e :probe/n ?n]]
                     :args [db]
                     :offset 7
                     :limit 5
                     :cancel cancel})]
        (is (= 5 (count result)))
        (is (= 13 @derefs)
            "the direct scan must not traverse the remainder of the attribute")))

    (testing "prepared direct execution receives the same demand"
      (let [[derefs cancel] (counting-cancel)
            result (binding [q/*disable-planner* false
                             execute/*prepared-execution* true
                             q/*fold-scalar-ins* false
                             q/*query-result-cache?* false]
                     (d/q {:query '[:find ?e
                                    :in $ ?group
                                    :where [?e :probe/group ?group]]
                           :args [db :x]
                           :offset 7
                           :limit 5
                           :cancel cancel}))]
        (is (= 5 (count result)))
        (is (= 13 @derefs))))))

(deftest prepared-scalar-range-remains-an-index-bound
  (let [db (fixture-db)
        [derefs cancel] (counting-cancel)
        result (binding [q/*disable-planner* false
                         execute/*prepared-execution* true
                         q/*fold-scalar-ins* false
                         q/*query-result-cache?* false]
                 (d/q {:query '[:find ?e
                                :in $ ?upper
                                :where
                                [?e :probe/n ?n]
                                [(< ?n ?upper)]]
                       :args [db 10]
                       :cancel cancel}))]
    (is (= 10 (count result)))
    (is (< @derefs 40)
        "the value-free scalar parameter must bound AVET before scanning")
    (testing "the rebound survives a namespaced-predicate relation fallback"
      (let [[fallback-derefs fallback-cancel] (counting-cancel)
            fallback-result
            (binding [q/*disable-planner* false
                      execute/*prepared-execution* true
                      q/*fold-scalar-ins* false
                      q/*query-result-cache?* false]
              (d/q {:query '[:find ?e
                             :in $ ?upper
                             :where
                             [?e :probe/n ?n]
                             [(datahike.test.query-limit-demand-test/strict-less-than?
                               ?n ?upper)]
                             [(< ?n ?upper)]]
                    :args [db 10]
                    :cancel fallback-cancel}))]
        (is (= result fallback-result))
        (is (< @fallback-derefs 40))))
    (testing "a separate candidate collection does not hide the scalar bound"
      (let [candidates (mapv first
                             (d/q '[:find ?e :where [?e :probe/n ?n]] db))
            [mixed-derefs mixed-cancel] (counting-cancel)
            mixed-result
            (binding [q/*disable-planner* false
                      execute/*prepared-execution* true
                      q/*fold-scalar-ins* false
                      q/*query-result-cache?* false]
              (d/q {:query '[:find ?candidate
                             :in $ ?upper [?candidate ...]
                             :where
                             [?candidate :probe/n ?n]
                             [(datahike.test.query-limit-demand-test/strict-less-than?
                               ?n ?upper)]
                             [(< ?n ?upper)]]
                    :args [db 10 candidates]
                    :cancel mixed-cancel}))]
        (is (= result mixed-result))
        (is (< @mixed-derefs 40))))
    (testing "a nested plan keeps the scalar as a relational obligation"
      (let [nested-result
            (binding [q/*disable-planner* false
                      execute/*prepared-execution* true
                      q/*fold-scalar-ins* false
                      q/*query-result-cache?* false]
              (d/q {:query '[:find ?e
                             :in $ ?upper
                             :where
                             [?e :probe/n _]
                             (not-join [?e ?upper]
                                       [?e :probe/n ?inside]
                                       [(>= ?inside ?upper)])]
                    :args [db 10]}))]
        (is (= result nested-result)
            "nested pushdown is declined until prepared rebinding reaches sub-plans")))))

(deftest unsafe-demand-remains-unbounded
  (let [db (fixture-db)]
    (testing "a post-filter cannot under-fill a requested page"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e
                              :where
                              [?e :probe/n ?n]
                              [(even? ?n)]]
                     :args [db]
                     :offset 7
                     :limit 5
                     :cancel cancel})]
        (is (= 5 (count result)))
        (is (= (inc probe-count) @derefs)
            "scan candidates are filtered only after direct collection")))

    (testing "projection duplicates cannot consume the result demand"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?group
                              :where [?e :probe/group ?group]]
                     :args [db]
                     :limit 2
                     :cancel cancel})]
        (is (= #{[:x] [:y]} result))
        (is (= (inc probe-count) @derefs)
            "both distinct values must survive post-scan hash deduplication")))

    (testing "ORDER BY still sees the complete input before sorting"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e ?n :where [?e :probe/n ?n]]
                     :args [db]
                     :order-by '[?n :desc]
                     :limit 5
                     :cancel cancel})]
        (is (= [99 98 97 96 95] (mapv second result)))
        (is (= (inc probe-count) @derefs))))

    (testing "offset + limit cannot overflow the executor's int counter"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e :where [?e :probe/n ?n]]
                     :args [db]
                     :offset Integer/MAX_VALUE
                     :limit 1
                     :cancel cancel})]
        (is (empty? result))
        (is (= (inc probe-count) @derefs))))

    (testing "historical scans retain their adjacent-dedup pass"
      (let [[derefs cancel] (counting-cancel)
            result (uncached-q
                    {:query '[:find ?e :where [?e :probe/n ?n]]
                     :args [(d/history db)]
                     :limit 1
                     :cancel cancel})]
        (is (= 1 (count result)))
        (is (= (inc probe-count) @derefs))))))

(deftest demand-is-flat-in-the-fixture-size
  ;; The assertions above are exact counts, which makes them precise but
  ;; blind to the difference that matters: a constant is a bounded scan,
  ;; anything proportional to the fixture is not. Check that difference
  ;; directly, so it survives a check site being added or moved.
  (let [scalar-bound
        (fn [db cancel]
          (binding [q/*disable-planner* false
                    execute/*prepared-execution* true
                    q/*fold-scalar-ins* false
                    q/*query-result-cache?* false]
            (d/q {:query '[:find ?e
                           :in $ ?upper
                           :where
                           [?e :probe/n ?n]
                           [(datahike.test.query-limit-demand-test/strict-less-than?
                             ?n ?upper)]
                           [(< ?n ?upper)]]
                  :args [db 10]
                  :cancel cancel})))
        direct-page
        (fn [db cancel]
          (uncached-q {:query '[:find ?e :where [?e :probe/n ?n]]
                       :args [db]
                       :offset 7
                       :limit 5
                       :cancel cancel}))
        unbounded
        (fn [db cancel]
          (uncached-q {:query '[:find ?e ?n :where [?e :probe/n ?n]]
                       :args [db]
                       :order-by '[?n :desc]
                       :limit 5
                       :cancel cancel}))]
    (testing "a scalar-bounded prepared scan does not grow with the fixture"
      (is (= (demand-for 100 scalar-bound)
             (demand-for 400 scalar-bound))))
    (testing "a direct offset+limit page does not grow with the fixture"
      (is (= (demand-for 100 direct-page)
             (demand-for 400 direct-page))))
    (testing "an ORDER BY over the whole attribute still does"
      ;; The control: if the instrument stopped tracking the scan, the two
      ;; assertions above would hold vacuously.
      (is (< (demand-for 100 unbounded)
             (demand-for 400 unbounded))))))
