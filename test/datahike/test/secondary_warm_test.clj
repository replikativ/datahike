(ns datahike.test.secondary-warm-test
  "The `:secondary` pass of `warm-db`, and the `ISecondaryWarmable` seam.

   Budgets are asserted in each family's OWN units and never compared across
   families — that non-translation is part of the design, not a gap. What is
   asserted across every index is the ENVELOPE: `:fetched`, `:ms`,
   `:budget-exhausted?` — the shape one caller logs as one decay metric.

   The stratum fixture is synced and RELOADED before warming, because an index
   built in this process has its trees in memory and a warm of it correctly
   reports zero — only a restored index has anything cold; see the fixture for
   why it also controls the chunk size. Vacuity is the standing trap of this
   whole arc."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.datom]
   [datahike.index.secondary :as sec]
   [datahike.index.secondary.stratum :as dstratum]
   [stratum.dataset :as sd]
   [stratum.index :as sidx]
   [datahike.warm :as warm]
   [konserve.memory :refer [new-mem-store]]))

(defn- restored-stratum-index
  "A stratum secondary index over a COLD, DEEP dataset.

   Built directly through stratum rather than through the adapter's transact
   path, for a measured reason: the adapter builds columns at stratum's
   default chunk size (8192 elements), so an adapter-built column is a
   single-leaf tree until ~half a million rows — restored root probed at
   level 0 — and a warm of it correctly fetches nothing. Small chunks give
   the tree interior structure at test scale; the object under test is the
   ADAPTER's `-sec-warm!` (protocol -> stratum.warm delegation, opts and
   budget pass-through), and it gets exactly the same dataset value either
   way."
  [n]
  (let [store (new-mem-store (atom {}) {:sync? true})
        ds    (sd/make-dataset
               {:eid    (sidx/index-from-seq :int64 (range n) {:chunk-size 4})
                :salary (sidx/index-from-seq :int64 (map #(* % 1000) (range n)) {:chunk-size 4})}
               {:name "warm-fixture"})]
    (sd/sync! ds store "main")
    (dstratum/->StratumIndex
     (sd/load store "main") #{:person/salary} nil {} (random-uuid))))

(deftest sec-warm-answers-the-envelope
  (let [idx (restored-stratum-index 2000)
        r   (sec/sec-warm! idx {:depth :with-leaves :budget 100000})]
    (is (pos? (:fetched r)) "a restored stratum index has cold tree nodes")
    (is (number? (:ms r)))
    (is (false? (:budget-exhausted? r)))
    (testing "the budget is a hard ceiling, in stratum's own units (nodes)"
      (let [idx2 (restored-stratum-index 2000)
            r2   (sec/sec-warm! idx2 {:depth :with-leaves :budget 2})]
        (is (= 2 (:fetched r2)))
        (is (true? (:budget-exhausted? r2)))))))

(deftest an-index-without-the-protocol-reports-not-pretends
  (let [naked (reify sec/ISecondaryIndex
                (-transact [this _] this)
                (-indexed-attrs [_] #{}))
        r     (sec/sec-warm! naked {})]
    (is (zero? (:fetched r)))
    (is (true? (:unsupported? r))
        "an index that cannot warm says so instead of being silently skipped")))

(deftest warm-db-carries-the-secondary-reports
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read
             :keep-history? false}
        _    (d/delete-database cfg)
        _    (d/create-database cfg)
        conn (d/connect cfg)
        idx  (restored-stratum-index 2000)
        db   (assoc @conn :secondary-indices {:idx/columns idx})]
    (try
      (testing "default is :none — no secondary key in the report at all"
        (let [r (warm/warm-db! db {})]
          (is (not (contains? r :secondary))
              "off by default: a secondary's full warm can dwarf the primary one")))
      (testing ":defaults warms every secondary and reports per index"
        (let [r (warm/warm-db! db {:secondary :defaults})]
          (is (contains? (:secondary r) :idx/columns))
          ;; :defaults means each family's defaults — :interior for stratum —
          ;; and on this fixture's height-1 trees that correctly fetches 0.
          ;; The envelope arriving is the assertion; the pos?-fetch proof is
          ;; the map-selecting case below, which asks for leaves.
          (is (number? (get-in r [:secondary :idx/columns :fetched])))
          (is (contains? (get-in r [:secondary :idx/columns]) :budget-exhausted?))))
      (testing "a map selects indices and passes each its own opts"
        (let [db2 (assoc @conn :secondary-indices {:idx/columns (restored-stratum-index 2000)})
              r   (warm/warm-db! db2 {:secondary {:idx/columns {:depth :with-leaves :budget 3}}})]
          (is (= 3 (get-in r [:secondary :idx/columns :fetched])))
          (is (true? (get-in r [:secondary :idx/columns :budget-exhausted?])))))
      (finally (d/release conn)))))
