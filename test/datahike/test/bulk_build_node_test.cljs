(ns datahike.test.bulk-build-node-test
  "`init-index-sorted` on Node — the streaming bulk index build.

   This is the cljs half of `migrate_bulk_test.clj`. It exists separately rather
   than as a `.cljc` because the two runtimes genuinely return different things:
   the JVM builder is synchronous and hands back a set, while on Node the default
   is `:sync? false` and the caller gets a partial-cps expression. That is the
   same `async+sync` contract every other storage-touching entry point in
   datahike has, and pretending otherwise in a shared file would mean a test full
   of reader conditionals.

   What is NOT different is the tree. A bulk-built index must equal a normally
   built one, or the fast path is a different database."
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.core.async :refer [go <!]]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [datahike.writing :as dw]))

(defn- populated [n]
  (go
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? false :schema-flexibility :read}]
      (<! (d/create-database cfg))
      (let [conn (d/connect cfg)]
        (<! (d/transact! conn (vec (for [i (range n)]
                                     {:db/id (+ 100 i) :name (str "n" i) :age i}))))
        [cfg conn]))))

(deftest bulk-built-index-equals-normally-built-on-node
  (testing "same datoms in, same index out. `init-index` sorts the whole array in
            memory; `init-index-sorted` streams an already-sorted seq and stores
            each node as it fills, so the trees are built by entirely different
            code and must still agree."
    (async done
           (go
             (let [[cfg conn] (<! (populated 400))
                   db @conn
                   store (:store db)
                   datoms (vec (d/datoms db :eavt))
                   cmp (dd/index-type->cmp-quick :eavt false)
                   sorted (sort cmp datoms)
                   normal (di/init-index :datahike.index/persistent-set
                                         store datoms :eavt 0 {})]
               (is (pos? (count datoms)) "precondition: there are datoms")
               ((di/init-index-sorted :datahike.index/persistent-set
                                      store sorted :eavt 0 {})
                (fn [bulk]
                  (is (= (count normal) (count bulk)) "same datom count")
                  (is (= (vec normal) (vec bulk)) "same datoms in the same order")
                  (go (<! (d/delete-database cfg)) (done)))
                (fn [e]
                  (is false (str "bulk build failed: " e))
                  (go (<! (d/delete-database cfg)) (done)))))))))

(deftest the-flush-hook-drains-pending-writes-on-node
  (testing "the reason `:flush-fn` exists.

            `IStorage/store` only buffers onto `pending-writes`; without a drain
            the buffer grows to the whole index and the builder's memory bound is
            real for the tree and nominal for datahike. On the JVM the drain is a
            blocking konserve write. Here it is an async one — which is the point
            the seq-wrapping version could never reach, because IO inside a `fn`
            literal cannot suspend under partial-cps.

            A deliberately tiny threshold, so the drain actually fires: at the
            default of 1000 nodes this database never trips it."
    (async done
           (go
             (let [[cfg conn] (<! (populated 8000))
                   db @conn
                   store (assoc (:store db) :datahike/index-flush-threshold 4)
                   pending (-> store :storage :pending-writes)
                   stats (-> store :storage :stats)
                   datoms (vec (d/datoms db :eavt))
                   cmp (dd/index-type->cmp-quick :eavt false)
                   sorted (sort cmp datoms)
                   peak (atom 0)
                   ;; sample the PEAK, not the count at the end — the end is the
                   ;; one moment the buffer is legitimately small
                   sampled (map (fn [x] (swap! peak max (count @pending)) x) sorted)
                   writes-before (:writes @stats)]
               (reset! pending [])
               ((di/init-index-sorted :datahike.index/persistent-set
                                      store sampled :eavt 0
                                      {:flush-fn (dw/bulk-flush-fn store false)})
                (fn [bulk]
                  (let [total (- (:writes @stats) writes-before)]
                    (is (= (count datoms) (count bulk)) "the index is still correct")
                    (is (> total 12)
                        (str "precondition: the build must produce enough nodes ("
                             total ") for the threshold to trip — an earlier version "
                             "of this test asserted a bound on a THREE-node tree and "
                             "so could not fail"))
                    (is (<= @peak 4)
                        (str "peak pending-writes " @peak " stayed within the threshold")))
                  (go (<! (d/delete-database cfg)) (done)))
                (fn [e]
                  (is false (str "bulk build failed: " e))
                  (go (<! (d/delete-database cfg)) (done)))))))))

(deftest avet-only-indexes-the-indexed-attributes
  (testing "`:avet` holds only indexed attributes, and the filter is applied
            before the builder sees the stream — so nothing indexes a datom it
            would discard. Worth pinning on Node because the filter sits between
            the sorted input and a builder that consumes it exactly once."
    (async done
           (go
             (let [[cfg conn] (<! (populated 200))
                   db @conn
                   store (:store db)
                   datoms (vec (d/datoms db :eavt))
                   cmp (dd/index-type->cmp-quick :avet false)
                   sorted (sort cmp datoms)
                   indexed #{:name}]
               ((di/init-index-sorted :datahike.index/persistent-set
                                      store sorted :avet 0 {:indexed indexed})
                (fn [bulk]
                  (is (pos? (count bulk)) "some datoms were indexed")
                  (is (every? #(contains? indexed (.-a %)) (vec bulk))
                      "and only the indexed attribute's datoms")
                  (go (<! (d/delete-database cfg)) (done)))
                (fn [e]
                  (is false (str "bulk build failed: " e))
                  (go (<! (d/delete-database cfg)) (done)))))))))

(deftest an-empty-input-builds-an-empty-index
  (testing "not an error and not nil — a database with no datoms of this index
            type is a database."
    (async done
           (go
             (let [[cfg conn] (<! (populated 1))
                   store (:store @conn)]
               ((di/init-index-sorted :datahike.index/persistent-set
                                      store [] :eavt 0 {})
                (fn [bulk]
                  (is (= 0 (count bulk)))
                  (is (= [] (vec bulk)))
                  (go (<! (d/delete-database cfg)) (done)))
                (fn [e]
                  (is false (str "bulk build failed: " e))
                  (go (<! (d/delete-database cfg)) (done)))))))))
