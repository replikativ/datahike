(ns datahike.test.gc-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [clojure.set :as set]
   [superv.async :refer [<?? S]]
   [datahike.api :as d]
   [datahike.index.interface :refer [-mark]]
   [datahike.versioning :refer [branch! delete-branch! merge!
                                branch-history]]
   [konserve.core :as k]
   [datahike.gc]
   [datahike.test.core-test])
  (:import [java.util Date]))

#?(:cljs (def Throwable js/Error))

(defn- count-store [db]
  (count (k/keys (:store db) {:sync? true})))

(def count-query '[:find (count ?e) .
                   :where
                   [?e :age _]])

(def txs (vec (for [i (range 1000)] {:age i})))

(def cfg {:store              {:backend :file
                               :path "/tmp/gc-test"
                               :id #uuid "9c000000-0000-0000-0000-000000000001"}
          ;; These tests assert what a SINGLE-PROCESS collector reclaims, and say
          ;; so: under the default (shared) ownership the sweep carries a
          ;; fifteen-minute floor, because another process's commit in flight is
          ;; invisible here. The floor's own tests are below.
          :writer {:backend :self :writer-ownership :exclusive}
          :keep-history?      true
          :schema-flexibility :write
          :index              :datahike.index/persistent-set})

(def schema [{:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])

(deftest datahike-gc-test
  (let [cfg (assoc-in cfg [:store :path] "/tmp/dh-gc-test")
        conn (do
               (d/delete-database cfg)
               (d/create-database cfg)
               (d/connect cfg))
          ;; everything will fit into the root nodes of each index here
        num-roots 3
        fresh-count (+ num-roots 4) ;; :branches + :db + cid + roots + schema-meta
        history-count 3]
    (testing "Test initial store counts."
      (is (= 1 (count (-mark (:eavt @conn)))))
      (is (= fresh-count (count-store @conn)))
      (d/transact conn schema)
      (is (= 1 (count (-mark (:eavt @conn)))))
      (is (= (+ 2 history-count fresh-count num-roots) (count-store @conn))))
    (testing "Delete old db with roots."
      (is (= (+ num-roots 2) (count (<?? S (d/gc-storage conn (Date.))))))
      (is (= (+ history-count fresh-count) (count-store @conn))))
    (testing "Try to run on dirty index and fail."
      (is (thrown-with-msg? Throwable #"Index needs to be properly flushed before marking."
                            (-mark (:eavt
                                    (:db-after
                                     (d/with @conn [{:db/id 100
                                                     :age   5}])))))))

    (testing "Check that we can still read the data."
      (let [new-conn (d/connect cfg)]
        (d/transact conn txs)
        (<?? S (d/gc-storage conn (Date.)))
        (is (= 1000 (d/q count-query @new-conn)))
        (d/release new-conn)))
    (d/release conn)))

(deftest datahike-gc-versioning-test
  (let [cfg          (assoc-in cfg [:store :path] "/tmp/dh-gc-versioning-test")
        conn         (do
                       (d/delete-database cfg)
                       (d/create-database cfg)
                       (d/connect cfg))
        _            (d/transact conn schema)
        ;; create two more branches
        _            (branch! conn :db :branch1)
        cfg1         (assoc cfg :branch :branch1)
        conn-branch1 (d/connect cfg1)
        _            (branch! conn :db :branch2)
        cfg2         (assoc cfg :branch :branch2)
        conn-branch2 (d/connect cfg2)]
    (testing "Check branches."
      (d/transact conn-branch1 txs)
      (d/transact conn-branch2 txs)
      (<?? S (d/gc-storage conn (Date.)))
      (is (nil? (d/q count-query @conn)))
      (is (= 1000 (d/q count-query @conn-branch1)))
      (is (= 1000 (d/q count-query @conn-branch2)))
      (delete-branch! conn :branch2)
      (<?? S (d/gc-storage conn (Date.))))

    (d/release conn)
    (d/release conn-branch1)
    (d/release conn-branch2)

    (testing "Removed branch and after gc check."
      (let [cfg          (assoc-in cfg [:store :path] "/tmp/dh-gc-versioning-test")
            conn         (d/connect cfg)
            ;; create two more branches
            cfg1         (assoc cfg :branch :branch1)
            conn-branch1 (d/connect cfg1)
            cfg2         (assoc cfg :branch :branch2)]
        (is (nil? (d/q count-query @conn)))
        (is (= 1000 (d/q count-query @conn-branch1)))
        (is (thrown-with-msg? Throwable #"Database does not exist."
                              (d/connect cfg2)))
        (d/release conn)
        (d/release conn-branch1)))))

(deftest datahike-gc-range-test
  (let [cfg           (assoc-in cfg [:store :path] "/tmp/dh-gc-range-test")
        conn          (do
                        (d/delete-database cfg)
                        (d/create-database cfg)
                        (d/connect cfg))
        _             (d/transact conn schema)
        ;; create a branch
        _             (branch! conn :db :branch1)
        conn-branch1  (d/connect (assoc cfg :branch :branch1))
        ;; transact on each
        _             (d/transact conn txs)
        _             (d/transact conn-branch1 txs)
        ;; record before-date for gc
        _             (Thread/sleep 100)
        remove-before (Date.)]
    (Thread/sleep 100)
    ;; transact
    (d/transact conn [{:age 42}])
    (d/transact conn-branch1 [{:age 42}])
    ;; transact again
    (d/transact conn [{:age 42}])
    (d/transact conn-branch1 [{:age 42}])
    ;; merge back
    (merge! conn #{:branch1} [])
    (let [db-history       (<?? S (branch-history conn))
          branch1-history  (<?? S (branch-history conn-branch1))
          _ (delete-branch! conn :branch1)
          _ (testing "Check branch counts"
              (is (= 9 (count db-history)))
              (is (= 5 (count branch1-history)))
              (is (= 9 (count (set/union (set db-history) (set branch1-history))))))
          new-history      (set (filter (fn [db]
                                          (let [db-date ^Date (or (get-in db [:meta :datahike/updated-at])
                                                                  (get-in db [:meta :datahike/created-at]))]
                                            (> (.getTime db-date)
                                               (.getTime remove-before))))
                                        (concat db-history branch1-history)))
          ;; gc
          _                (<?? S (d/gc-storage conn remove-before))
          history-after-gc (set (<?? S (branch-history conn)))]
      (testing "Check that newer db roots are still there and counts after gc."
        (is (set/subset? new-history history-after-gc))
        (is (= 5 (count new-history)))
        (is (= 7 (count history-after-gc)))))
    (d/release conn)
    (d/release conn-branch1)))

;; ---------------------------------------------------------------------------
;; The sweep FLOOR (:min-age-ms). The exact bound — the in-process safe point —
;; is only available where the writing happens; a collector anywhere else gets
;; `now` from it, because ITS heap is idle rather than because the store is
;; quiet. The floor is what stands in for the missing information.

(defn- churned-conn
  "A connection with collectable garbage: history is on, and every snapshot
   before `remove-before` is erasable, so the superseded roots are unreachable.
   `(Date. 0)` as remove-before would keep ALL history and leave nothing to
   collect, which makes any \"the floor spared it\" assertion pass vacuously."
  ([path] (churned-conn path {}))
  ([path extra]
   (let [cfg (merge (assoc-in cfg [:store :path] path) extra)]
     (d/delete-database cfg)
     (d/create-database cfg)
     (let [conn (d/connect cfg)]
       (d/transact conn schema)
       (d/transact conn txs)
       conn))))

(deftest min-age-spares-recent-garbage
  (testing "an object young enough is spared even though the mark called it
            garbage — the property an offline collector rests on"
    (let [conn (churned-conn "/tmp/dh-gc-min-age")]
      (try
        (let [before (count-store @conn)
              swept  (<?? S (d/gc-storage conn (Date.) {:min-age-ms 86400000}))]
          (is (empty? swept)
              "nothing written in the last day may be deleted, reachable or not")
          (is (= before (count-store @conn))
              "and the store is untouched"))
        ;; The control, and it is the assertion that matters: the same store,
        ;; the same remove-before, no floor. If this collects nothing then the
        ;; scenario had no garbage and the assertion above proved nothing.
        (let [swept (<?? S (d/gc-storage conn (Date.) {:min-age-ms 0}))]
          (is (seq swept)
              "with no floor the same store does have collectable garbage"))
        (finally (d/release conn))))))

(deftest min-age-is-off-by-default-under-an-exclusive-writer
  (testing "where the writer lives and is alone the safe point is exact, so a
            wall-clock floor there would only retain garbage the collector was
            right about. Passing nothing must collect exactly as before."
    (is (zero? datahike.gc/DEFAULT_SWEEP_MIN_AGE_MS))
    (is (zero? (datahike.gc/default-min-age-ms
                {:writer {:backend :self :writer-ownership :exclusive}})))
    (let [conn (churned-conn "/tmp/dh-gc-min-age-default")]
      (try
        (is (seq (<?? S (d/gc-storage conn (Date.))))
            "the default collects the garbage it always did")
        (finally (d/release conn))))))

(deftest min-age-defaults-to-a-floor-under-a-shared-writer
  (testing "shared ownership is the connection's own statement that another
            writer may exist, and that writer's commit in flight is invisible to
            this collector — so the sweep carries a floor unless told otherwise"
    (is (= datahike.gc/DEFAULT_SHARED_SWEEP_MIN_AGE_MS
           (datahike.gc/default-min-age-ms {:writer {:backend :self :writer-ownership :shared}})))
    (is (= datahike.gc/DEFAULT_SHARED_SWEEP_MIN_AGE_MS
           (datahike.gc/default-min-age-ms {}))
        "no writer config at all is the shared default")
    (is (= datahike.gc/DEFAULT_SHARED_SWEEP_MIN_AGE_MS
           (datahike.gc/default-min-age-ms {:writer {:backend :datahike-server}}))
        "a remote writer means the writes happen elsewhere entirely")
    (let [conn (churned-conn "/tmp/dh-gc-min-age-shared"
                             {:writer {:backend :self :writer-ownership :shared}})]
      (try
        (let [before (count-store @conn)]
          (is (empty? (<?? S (d/gc-storage conn (Date.))))
              "nothing written in the last fifteen minutes is deleted by default")
          (is (= before (count-store @conn))))
        ;; The control: an explicit zero restores the unfloored sweep, which
        ;; also proves the store had garbage and the assertion above meant it.
        (is (seq (<?? S (d/gc-storage conn (Date.) {:min-age-ms 0})))
            "an explicit {:min-age-ms 0} sweeps without the floor")
        (finally (d/release conn))))))

(deftest min-age-is-a-floor-not-a-replacement
  (testing "cutoff is the MINIMUM of the guard and the floor, so the floor can
            only ever collect LESS — never more than the safe point allows"
    (let [conn (churned-conn "/tmp/dh-gc-min-age-floor")]
      (try
        (let [with-floor (<?? S (d/gc-storage conn (Date.) {:min-age-ms 86400000}))
              no-floor   (<?? S (d/gc-storage conn (Date.) {:min-age-ms 0}))]
          (is (empty? (clojure.set/difference (set with-floor) (set no-floor)))
              "a floored sweep deletes nothing an unfloored one would keep"))
        (finally (d/release conn))))))
