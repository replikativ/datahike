(ns datahike.test.cold-async-store-test
  "Drives the per-read storage seam through a genuinely ASYNC-ONLY store:
   a wrapper whose sync reads throw and whose async reads succeed. Pins the
   two contracted behaviors of the seam on a COLD read (empty node LRU):
   - the sync arm surfaces the actionable :storage/sync-read-unavailable
     error, decorated at the -slice boundary with the slice's logical
     [index from to] (the fault-retry machinery prefetches whole ranges);
   - the async arm ({:sync? false}) falls back to the channel adapter,
     admits the node into the LRU, and yields the same datoms the store
     held — after which the LRU is WARM and sync reads work again.
   Only the cold-store mock can catch this class: warm async completes
   synchronously and is indistinguishable from sync."
  (:require [cljs.test :refer [is testing] :include-macros true]
            [clojure.core.async :as a :refer [<!] :refer-macros [go]]
            [datahike.api :as d]
            [datahike.constants :refer [e0 tx0 emax txmax]]
            [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [datahike.index.persistent-set :as dip]
            [datahike.test.async :refer-macros [deftest-async]]
            [konserve.core :as k]
            [konserve.protocols :as kp]
            [org.replikativ.persistent-sorted-set :as psset]
            [org.replikativ.persistent-sorted-set.btset :as btset]))

(defrecord AsyncOnlyStore [inner]
  kp/PEDNKeyValueStore
  (-exists? [_ key opts] (kp/-exists? inner key opts))
  (-get-meta [_ key opts] (kp/-get-meta inner key opts))
  (-get-in [_ key-vec not-found opts]
    (if (:sync? opts)
      (throw (ex-info "async-only store: no synchronous reads" {:store :async-only}))
      (kp/-get-in inner key-vec not-found opts)))
  (-update-in [_ key-vec meta-up-fn up-fn opts] (kp/-update-in inner key-vec meta-up-fn up-fn opts))
  (-assoc-in [_ key-vec meta-up-fn val opts] (kp/-assoc-in inner key-vec meta-up-fn val opts))
  (-dissoc [_ key opts] (kp/-dissoc inner key opts))
  kp/PLockFreeStore
  (-lock-free? [_] true))

(defn- fresh-flushed-conn
  "Create + connect a memory-backed db and transact enough datoms that the
   indices are flushed to the store (returns a channel with [conn cfg])."
  []
  (go
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read
               :keep-history? false}
          _ (<! (d/create-database cfg))
          conn (<! (d/connect cfg {:sync? false}))]
      (<! (d/transact! conn (mapv (fn [i] {:db/id (inc i) :name (str "n" i)})
                                  (range 300))))
      [conn cfg])))

(defn- cold-index
  "A LAZY copy of the eavt index: the live tree is stored NODE-BY-NODE
   through a CachedStorage into the memory store (memory backends never
   node-flush on their own — that is exactly why they are sync-capable),
   then restored BY ADDRESS onto a fresh CachedStorage (empty LRU) over an
   async-only wrapper of the same store. The restored root must LOAD every
   node through the seam — genuinely cold."
  [store eavt]
  (let [write-storage (dip/create-storage store {:store-cache-size 1000})
        addr (psset/store eavt write-storage {:sync? true})]
    ;; drain the buffered node writes into the store
    (doseq [[a node] @(:pending-writes write-storage)]
      (k/assoc store a node {:sync? true}))
    (let [wrapped (->AsyncOnlyStore store)
          cold-storage (dip/create-storage wrapped {:store-cache-size 1000})]
      (psset/restore {:root-address addr
                      :comparator (.-comparator eavt)}
                     cold-storage))))

(defn- full-range []
  [(dd/datom e0 nil nil tx0) (dd/datom emax nil nil txmax)])

(deftest-async cold-sync-read-throws-decorated
  (let [[conn _] (<! (fresh-flushed-conn))
        db @conn
        eavt (cold-index (:store db) (:eavt db))
        [from to] (full-range)
        e (try (vec (di/-slice eavt from to :eavt))
               ::no-throw
               (catch :default e e))]
    (is (not= ::no-throw e) "cold sync slice over an async-only store must throw")
    (when (not= ::no-throw e)
      (let [data (ex-data e)]
        (is (= :storage/sync-read-unavailable (:error data))
            "the seam's actionable error surfaces")
        (is (some? (:address data)) "fault carries the node address")
        (is (= :eavt (:index data)) "decorated with the slice's index")
        (is (some? (:from data)) "decorated with the slice's from bound")
        (is (some? (:to data)) "decorated with the slice's to bound")))))

(deftest-async cold-async-read-succeeds-then-warms
  (let [[conn _] (<! (fresh-flushed-conn))
        db @conn
        expected (mapv (fn [d] (.-v d))
                       (di/-slice (:eavt db)
                                  (first (full-range)) (second (full-range)) :eavt))
        eavt (cold-index (:store db) (:eavt db))
        [from to] (full-range)
        expr (di/-slice eavt from to :eavt {:sync? false})
        ;; drive the async expr: cold reads go through the channel adapter,
        ;; so completion is genuinely asynchronous — bridge it to core.async
        done (a/promise-chan)]
    (expr (fn [aseq]
            ;; drain chunk-wise; each chunk hop may be cold too
            (letfn [(drain [s acc]
                      (if (nil? s)
                        (a/put! done {:datoms acc})
                        ((btset/achunk-next s)
                         (fn [chunk]
                           (if (nil? chunk)
                             (a/put! done {:datoms acc})
                             (let [[keys start end next] chunk]
                               (drain next
                                      (loop [i start v acc]
                                        (if (< i end) (recur (inc i) (conj v (aget keys i))) v))))))
                         (fn [err] (a/put! done {:error err})))))]
              (drain aseq [])))
          (fn [err] (a/put! done {:error err})))
    (let [{:keys [datoms error]} (<! done)]
      (is (nil? error) (str "async cold slice must succeed, got: " error))
      (is (= expected (mapv (fn [d] (.-v d)) datoms))
          "async-drained datoms equal the original warm slice")
      ;; the LRU is now warm: the SAME cold storage serves sync reads
      (is (= expected (mapv (fn [d] (.-v d)) (di/-slice eavt from to :eavt)))
          "after the async pass, sync reads succeed (LRU warmed)"))))
