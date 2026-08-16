(ns datahike.test.warm-test
  "The budget-bounded BFS index warm (`datahike.warm` over
   `datahike.index.persistent-set.warm`).

   File-backed and local, so a read is nearly free — which is the point. What
   needs pinning down here is not speed but the WALK's shape, because every claim
   the design rests on is a claim about shape:

     - `:interior` stops exactly at the leaf boundary
     - `:budget` degrades continuously, so there is no mode to fall off
     - a range warms only the covering subtree
     - one budget is shared FAIRLY across indices, not eaten by the first one
     - a warm built from `d/datoms` components COVERS that scan — asserted as a
       zero delta on the storage's `:reads` counter, since a warm of the wrong
       subtree fails nothing weaker

   A small branching factor gets a multi-level tree out of a few hundred datoms;
   the walk is branching-factor-independent, only the interior/total ratio is not.

   JVM-only (`.clj`, not `.cljc`): the walk's ClojureScript arm is a marked TODO
   and `-warm!` short-circuits there with `:unsupported :cljs`."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.warm :as warm])
  (:import [org.replikativ.persistent_sorted_set PersistentSortedSet]))

(def ^:private schema
  [{:db/ident :item/id    :db/valueType :db.type/long   :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :item/name  :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

(defn- tmp-path []
  (str (System/getProperty "java.io.tmpdir") "/warm-test-" (random-uuid)))

(defn- config [bf & {:as extra}]
  (merge {:store {:backend :file :path (tmp-path) :id (random-uuid)}
          :keep-history? false
          :schema-flexibility :write
          ;; A cache far above anything these fixtures warm. The budget is
          ;; CLAMPED to 0.8x this (warming past an entry-counted cache fetches
          ;; nodes only to evict them), so leaving it at the 1000-entry default
          ;; would make every shape assertion below depend on the clamp rather
          ;; than on the walk. `budget-is-clamped-to-the-node-cache` sets it low
          ;; on purpose; everything else wants it out of the way.
          :store-cache-size 1000000
          :index :datahike.index/persistent-set
          :index-config {:branching-factor bf}}
         extra))

(defn- fresh-db
  "A COLD database of `n` items at branching factor `bf`.

   Two fixture choices that are load-bearing, both learned the hard way:

   `:file`, not `:memory`. The memory backend does not split the tree into
   per-node blobs, so a reconnected root reports zero child addresses and there is
   nothing for a warm to fetch. Any assertion against it would pass vacuously.

   RELEASE and reconnect. Straight after a transact the tree is entirely in
   memory — measured, 0 of 4 child addresses populated — so a warm correctly
   fetches nothing. That is right behaviour (you cannot prefetch what is already
   in hand) and useless for testing the walk. Only a cold connection is
   address-rooted.

   bf 8 gets height 5 out of ~1200 items, so multi-round walks are reachable in a
   unit test. The walk is branching-factor-independent; only the interior/total
   ratio is not."
  ([n bf] (fresh-db n bf (config bf)))
  ([n bf cfg]
   (d/create-database cfg)
   (let [conn (d/connect cfg)]
     (d/transact conn schema)
     (doseq [chunk (partition-all 200 (range n))]
       (d/transact conn (vec (for [i chunk]
                               {:item/id i :item/name (str "item-" i)}))))
     (d/release conn))
   (d/connect cfg)))

(defn- height [db idx] (.level (.root ^PersistentSortedSet (get db idx))))

;; ── depth policies ──────────────────────────────────────────────────────────

(deftest interior-stops-at-the-leaf-boundary
  (testing ":interior fetches every branch level and no leaves; :with-leaves adds exactly one more round"
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (is (<= 2 (height db :eavt)) "fixture must be deep enough to have an interior at all")
        (let [interior (warm/warm-index! db :eavt {:depth :interior :budget 100000})
              leaves   (warm/warm-index! db :eavt {:depth :with-leaves :budget 100000})]
          (is (= (dec (:rounds leaves)) (:rounds interior))
              ":with-leaves is exactly one round deeper — the leaf round")
          (is (= (:by-level interior) (vec (butlast (:by-level leaves))))
              "and identical up to that round: same walk, one fewer level")
          (is (< (:fetched interior) (:fetched leaves))
              "leaves dominate the node count")
          (is (false? (:budget-exhausted? interior))
              "a budget this large is never the binding constraint"))
        (finally (d/release conn))))))

(deftest integer-depth-cuts-at-that-many-levels
  (let [conn (fresh-db 1200 8)
        db   @conn]
    (try
      (doseq [n [1 2]]
        (let [r (warm/warm-index! db :eavt {:depth n :budget 100000})]
          (is (= n (:rounds r)) (str "depth " n " walks exactly " n " levels"))
          (is (= n (count (:by-level r))))))
      (finally (d/release conn)))))

(deftest a-shallow-tree-has-no-interior
  (testing "when the root's children are leaves, :interior is correctly a no-op"
    ;; Not a degenerate case to work around — it is why a small per-tenant
    ;; database needs no warm at all: under :fuse-index-roots? the root rides in
    ;; the db record.
    (let [conn (fresh-db 5 512)
          db   @conn]
      (try
        (is (>= 1 (height db :eavt)) "root children are leaves (or the root IS a leaf)")
        (let [r (warm/warm-index! db :eavt {:depth :interior})]
          (is (zero? (:fetched r)))
          (is (zero? (:rounds r))))
        (finally (d/release conn))))))

;; ── budget ──────────────────────────────────────────────────────────────────

(deftest budget-degrades-continuously
  (testing "a partial budget buys a partial warm — monotone, with no step"
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (let [full (:fetched (warm/warm-index! db :eavt {:depth :with-leaves :budget 100000}))
              runs (for [b [1 5 20 100]]
                     (assoc (warm/warm-index! db :eavt {:depth :with-leaves :budget b})
                            :requested b))]
          (doseq [{:keys [requested fetched budget-exhausted?]} runs]
            (is (= (min requested full) fetched)
                "a budget below the tree size is spent exactly, never overshot")
            (is (= (< requested full) budget-exhausted?)
                ":budget-exhausted? distinguishes `ran out` from `ran dry`"))
          (is (apply <= (map :fetched runs))
              "monotone in the budget — the no-cliff property, as an assertion"))
        (finally (d/release conn))))))

(deftest a-budget-above-the-tree-warms-all-of-it
  (testing "`preload everything` is not a mode — it is what the loop does when it runs out of frontier"
    (let [conn (fresh-db 400 8)
          db   @conn]
      (try
        (let [r (warm/warm-index! db :eavt {:depth :with-leaves :budget 1000000})]
          (is (false? (:budget-exhausted? r)))
          (is (pos? (:fetched r))))
        (finally (d/release conn))))))

;; ── range scoping ───────────────────────────────────────────────────────────

(defn- datom-at [db i] (nth (seq (d/datoms db :eavt)) i))

(deftest a-range-warms-less-than-the-whole-tree
  (testing ":from/:to restrict the walk to the covering subtree"
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (let [all    (:fetched (warm/warm-index! db :eavt {:depth :with-leaves :budget 100000}))
              ds     (vec (take 400 (d/datoms db :eavt)))
              narrow (:fetched (warm/warm-index! db :eavt
                                                 {:depth :with-leaves :budget 100000
                                                  :from (nth ds 10) :to (nth ds 20)}))
              wide   (:fetched (warm/warm-index! db :eavt
                                                 {:depth :with-leaves :budget 100000
                                                  :from (nth ds 0) :to (nth ds 399)}))]
          (is (pos? narrow) "a range that exists warms something")
          (is (< narrow all) "and strictly less than the unbounded walk")
          (is (<= narrow wide) "a wider range warms at least as much — cost tracks the range"))
        (finally (d/release conn))))))

(deftest a-point-range-warms-one-path
  (testing "from = to descends a single path per level"
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (let [d0 (datom-at db 50)
              r  (warm/warm-index! db :eavt {:depth :with-leaves :budget 100000
                                             :from d0 :to d0})]
          (is (= (:rounds r) (count (:by-level r))))
          (is (every? #(= 1 %) (:by-level r))
              "one child per level — a range this narrow cannot straddle"))
        (finally (d/release conn))))))

;; ── multi-index fairness ────────────────────────────────────────────────────

(deftest one-budget-is-shared-across-indices
  (testing "a budget too small for all indices is split, not eaten by the first"
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (let [r (warm/warm-db! db {:depth :with-leaves :budget 12})
              by (:by-index r)]
          (is (= 12 (:fetched r)))
          (is (true? (:budget-exhausted? r)))
          (is (<= 3 (count by))
              "eavt, aevt and avet all got some of it — sequential spending would give the first one all 12")
          (is (every? pos? (vals by))
              "no index is starved")
          (is (< (apply max (vals by)) 12)
              "and none monopolizes the budget")
          ;; NOT asserting an even split. Round-robin is fair given equal SUPPLY,
          ;; and the three roots have different fanouts, so the shorter frontiers
          ;; drain first and the spread reflects the trees rather than the policy.
          ;; The property that matters is that a query against avet finds avet
          ;; warmed at all, which sequential spending did not give.
          )
        (finally (d/release conn))))))

(deftest budget-is-clamped-to-the-node-cache
  (testing "warming past an entry-counted cache would fetch and immediately evict"
    (let [cfg  (config 8 :store-cache-size 10)
          conn (fresh-db 400 8 cfg)]
      (try
        (let [r (warm/warm-db! @conn {:depth :with-leaves :budget 5000})]
          (is (true? (:budget-clamped? r)))
          (is (<= (:fetched r) 8) "clamped to 0.8x the 10-entry cache"))
        (finally (d/release conn))))))

;; ── warming for a specific scan ─────────────────────────────────────────────
;;
;; The property that makes `warm-datoms!` worth having: after it, the scan it was
;; built for does NO further storage reads. Anything weaker (counting nodes,
;; comparing ranges) would pass for a warm that covered the wrong subtree — which
;; is the exact failure a hand-built bound produces, silently.

(defn- reads
  "Cumulative node restores on this index's CachedStorage. `restore` increments
   :reads only on a cache MISS, so a delta of zero means every node the scan
   touched was already in hand."
  [db idx]
  (:reads @(:stats (.-_storage ^PersistentSortedSet (get db idx)))))

(defn- reads-during [db idx f]
  (let [before (reads db idx)]
    (f)
    (- (reads db idx) before)))

(deftest warm-datoms-covers-the-scan-it-was-built-for
  (testing "after warming a range, the matching d/datoms call reads nothing more"
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (let [cold-cost (reads-during db :eavt #(count (d/datoms db :eavt 300)))]
          (is (pos? cold-cost) "control: the same scan on a cold tree does read"))
        ;; A fresh connection, so the cache is empty again.
        (finally (d/release conn))))
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (warm/warm-datoms! db :eavt [300] {:depth :with-leaves :budget 100000})
        (is (zero? (reads-during db :eavt #(count (d/datoms db :eavt 300))))
            "warmed: the scan is served entirely from cache")
        (finally (d/release conn))))))

(deftest warm-datoms-handles-the-avet-permutation
  (testing ":avet components are [a v e tx] and are permuted into the key order"
    ;; The footgun this function exists to remove: a hand-built `:from` datom for
    ;; :avet must be datom(v, a, e, tx). Getting it wrong warms a valid-but-wrong
    ;; subtree and fails no assertion except this one.
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (let [cold (reads-during db :avet #(count (d/datoms db :avet :item/id 300)))]
          (is (pos? cold) "control: the same avet scan on a cold tree does read"))
        (finally (d/release conn))))
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (warm/warm-datoms! db :avet [:item/id 300] {:depth :with-leaves :budget 100000})
        (is (zero? (reads-during db :avet #(count (d/datoms db :avet :item/id 300))))
            "warmed via components: the avet scan is served from cache")
        (finally (d/release conn))))))

(deftest warm-seek-is-readahead-not-a-range
  (testing "seek's upper bound is the end of the index, so :budget is the only bound"
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (let [small (warm/warm-seek! db :eavt [300] {:depth :with-leaves :budget 5})
              big   (warm/warm-seek! db :eavt [300] {:depth :with-leaves :budget 100000})]
          (is (= 5 (:fetched small)))
          (is (true? (:budget-exhausted? small))
              "a seek always has more index ahead of it — the budget is what stops it")
          (is (< (:fetched small) (:fetched big))))
        (finally (d/release conn))))))

(deftest warming-an-absent-index-is-a-no-op
  (let [conn (fresh-db 100 8)
        db   @conn]
    (try
      ;; :temporal-eavt does not exist under :keep-history? false.
      (is (zero? (:fetched (warm/warm-datoms! db :temporal-eavt [1] {}))))
      (is (zero? (:fetched (warm/warm-index! db :temporal-avet {}))))
      (finally (d/release conn)))))

(deftest a-temporal-index-warms-through-its-primary-order
  (testing "components->pattern knows only the three primary orders; a temporal twin sorts the same way"
    ;; Under :keep-history? the temporal indices ARE present, so this is the case
    ;; where a naive (components->pattern db :temporal-eavt …) would throw on the
    ;; case dispatch rather than warm anything.
    (let [cfg  (config 8 :keep-history? true)
          conn (fresh-db 400 8 cfg)
          db   @conn]
      (try
        (let [r (warm/warm-datoms! db :temporal-eavt [300] {:depth :with-leaves :budget 100000})]
          (is (map? r))
          (is (pos? (:fetched r)) "the temporal tree is walked, not skipped"))
        (finally (d/release conn))))))

;; ── the async arm ───────────────────────────────────────────────────────────

(deftest the-async-arm-returns-a-channel-carrying-the-same-report
  (testing "`:sync? false` gives datahike's usual channel shape, not a value"
    ;; The fan-out is a thread pool, which has no core.async counterpart and so
    ;; cannot live inside the `async+sync` body — it is hoisted behind a `<?-`.
    ;; This is the assertion that the hoist actually delivers through it.
    (let [conn (fresh-db 1200 8)
          db   @conn]
      (try
        (let [sync-r  (warm/warm-index! db :eavt {:depth :interior :budget 100000})
              async-r (async/<!! (warm/warm-index! db :eavt {:depth :interior
                                                             :budget 100000
                                                             :sync? false}))]
          (is (map? async-r))
          (is (= (:fetched sync-r) (:fetched async-r)))
          (is (= (:by-level sync-r) (:by-level async-r))))
        (finally (d/release conn))))))

;; ── the public API surface ──────────────────────────────────────────────────

(deftest the-api-vars-are-the-warm-namespace
  (testing "d/warm-* are wired through api-specification to datahike.warm"
    (is (= @#'warm/warm-index!  @#'d/warm-index!))
    (is (= @#'warm/warm-datoms! @#'d/warm-datoms!))
    (is (= @#'warm/warm-seek!   @#'d/warm-seek!))
    (is (= @#'warm/warm-db!     @#'d/warm-db!))))
