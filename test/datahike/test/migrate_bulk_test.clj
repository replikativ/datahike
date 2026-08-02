(ns datahike.test.migrate-bulk-test
  "Bulk index construction from a pre-sorted datom stream.

   `di/init-index` sorts its input in memory (`arrays/asort` over the whole
   array), so building an index costs O(n) heap — fine when the database fits in
   memory, which is the case a bulk restore is not.
   `di/init-index-sorted` streams an already-sorted seq into
   `persistent-sorted-set/from-sorted-seq`, so the build holds one node per level.

   The property that matters is that the two agree: a bulk-built index must be
   the same index, or the fast path is a different database."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [datahike.migrate.ids :as ids]
            [datahike.test.utils :as utils]
            [konserve.core :as k]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- populated-conn [n]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? false :schema-flexibility :read}
        c (utils/setup-db cfg)]
    (d/transact c (vec (for [i (range n)]
                         {:db/id (+ 100 i) :name (str "n" i) :age i})))
    c))

(deftest bulk-built-index-equals-normally-built
  (testing "same datoms in, same index out — for every index type.

            Contents equality is the floor, not the ceiling: a tree with the
            right datoms and the wrong fanout would pass this and then differ
            under slice and count. The SHAPE equivalence is asserted upstream in
            persistent-sorted-set's own suite, against `from-sorted-array`; here
            the question is whether datahike drives it correctly."
    (let [conn (populated-conn 500)
          db @conn
          store (:store db)
          datoms (vec (d/datoms db :eavt))]
      (is (pos? (count datoms)) "precondition: there are datoms")
      (doseq [index-type [:eavt :aevt]]
        (testing (name index-type)
          (let [cmp (dd/index-type->cmp-quick index-type false)
                sorted (sort cmp datoms)
                normal (di/init-index :datahike.index/persistent-set store datoms index-type 0 {})
                bulk (di/init-index-sorted :datahike.index/persistent-set store sorted index-type 0 {})]
            (is (= (count normal) (count bulk)) "same datom count")
            (is (= (vec normal) (vec bulk)) "same datoms in the same order"))))
      (teardown conn))))

(deftest bulk-built-index-supports-lookup-and-slice
  (testing "a bulk-built index answers the queries an index exists for.

            Worth separating from contents equality: the bulk builder produces
            address-only nodes with no resident children, so every read here
            exercises the lazy-load path that a normally-built tree does not."
    (let [conn (populated-conn 300)
          db @conn
          store (:store db)
          datoms (vec (d/datoms db :eavt))
          cmp (dd/index-type->cmp-quick :eavt false)
          bulk (di/init-index-sorted :datahike.index/persistent-set store
                                     (sort cmp datoms) :eavt 0 {})]
      (is (= (count datoms) (count bulk)))
      (is (= (vec (sort cmp datoms)) (vec bulk)) "full scan matches")
      (teardown conn))))

;; ---------------------------------------------------------------------------

(deftest the-streaming-build-is-actually-memory-bounded
  (testing "the reason `init-index-sorted` exists is that it holds only part of
            the tree — but `IStorage/store` merely buffers onto `pending-writes`,
            which nothing drained until commit. So the bound was nominal: the
            whole index stayed resident and the streaming builder was, in memory
            terms, exactly the thing it replaced. Measured before the fix, 200k
            datoms left 524 nodes pending — every datom still in heap.

            Sampling the PEAK rather than the count at the end, because the end
            is the one moment the buffer is legitimately small."
    (let [conn (populated-conn 10000)
          db @conn
          store (:store db)
          datoms (vec (d/datoms db :eavt))
          cmp (dd/index-type->cmp-quick :eavt false)
          sorted (sort cmp datoms)
          pending (-> store :storage :pending-writes)
          threshold 10
          peak (atom 0)
          sample (fn [xs] (map (fn [x] (swap! peak max (count @pending)) x) xs))]
      (reset! pending [])
      (let [writes-before (:writes @(-> store :storage :stats))
            keys-before (count (k/keys store {:sync? true}))
            bulk (di/init-index-sorted :datahike.index/persistent-set
                                       (assoc store :datahike/index-flush-threshold threshold)
                                       (sample sorted) :eavt 0 {})
            ;; the storage's own counter, because peak + leftover cannot see the
            ;; nodes that were flushed and cleared in between — which is the
            ;; whole point
            total-nodes (- (:writes @(-> store :storage :stats)) writes-before)]
        (is (<= @peak threshold)
            (str "peak pending-writes " @peak " must stay within the threshold"))
        (is (> total-nodes (* 3 threshold))
            (str "precondition: the build must produce enough nodes (" total-nodes
                 ") for the threshold to actually trip — otherwise this passes vacuously"))

        (testing "and flushing did not corrupt the index"
          (is (= (count sorted) (count bulk)) "same datom count")
          (is (= (vec sorted) (vec bulk)) "same datoms in the same order"))

        (testing "the flushed nodes really reached the store DURING the build,
                  rather than being dropped on the floor — the failure mode a
                  drain-and-clear has. No commit has run at this point, so
                  anything in the store got there from the flush."
          (is (> (count (k/keys store {:sync? true})) keys-before)
              "the store gained nodes before any commit")))
      (teardown conn))))

(deftest flushing-can-be-disabled
  (testing "threshold 0 opts out, for a caller who would rather have the single
            ordered commit batch than the bound."
    (let [conn (populated-conn 2000)
          db @conn
          store (:store db)
          datoms (vec (d/datoms db :eavt))
          cmp (dd/index-type->cmp-quick :eavt false)
          sorted (sort cmp datoms)
          pending (-> store :storage :pending-writes)]
      (reset! pending [])
      (let [bulk (di/init-index-sorted :datahike.index/persistent-set
                                       (assoc store :datahike/index-flush-threshold 0)
                                       sorted :eavt 0 {})]
        (is (= (vec sorted) (vec bulk)) "still the right index")
        (is (> (count @pending) 1)
            "with flushing off, every node is still buffered for commit"))
      (teardown conn))))

;; ---------------------------------------------------------------------------

(deftest the-pipeline-produces-sortable-records
  (testing "the pre-pass + pure rewrite yields records a bulk build can consume.

            This is the join between `datahike.migrate.ids` and the index builder:
            the mapping must be complete BEFORE sorting, because the sort order is
            over the final ids."
    (let [schema {:name {:db/valueType :db.type/string}
                  :pal {:db/valueType :db.type/ref}}
          records [[3 :name "a" 100 true]
                   [4 :name "b" 100 true]
                   [4 :pal 3 100 true]]
          mapping (ids/build-mapping {:schema schema :system-entities #{}
                                      :max-eid 10 :max-tx 200}
                                     (fn [rf init] (reduce rf init records)))
          rewritten (mapv #(ids/apply-mapping mapping schema %) records)]
      (testing "every id is above the target's maxima"
        (is (every? (fn [[e _ _ t _]] (and (> e 10) (> t 200))) rewritten)))
      (testing "the ref value moved with its entity"
        (let [[_ _ pal-v _ _] (last rewritten)
              [b-e _ _ _ _] (second rewritten)]
          (is (= (get (:eids mapping) 3) pal-v))
          (is (= (get (:eids mapping) 4) b-e))))
      (testing "and the result sorts deterministically — the precondition for a
                bulk index build"
        (is (= (sort-by (juxt first second) rewritten)
               (sort-by (juxt first second) rewritten)))))))
