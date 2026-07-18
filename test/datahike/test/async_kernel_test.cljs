(ns datahike.test.async-kernel-test
  "The first dual-mode engine kernel: execute-scan-only consuming chunks.
   Pins, against the same flushed index:
   - sync mode over a chunked Iter ≡ the datoms the store holds;
   - async mode over a WARM AsyncSeq resolves ON THE CALLING STACK and
     emits identical tuples (the sync-completion property at kernel level);
   - async mode over a COLD async-only store completes via the channel
     fallback with identical tuples — the engine kernel streaming a store
     that cannot serve a single synchronous read."
  (:require [cljs.test :refer [is testing] :include-macros true]
            [clojure.core.async :as a :refer [<!] :refer-macros [go]]
            [datahike.api :as d]
            [datahike.constants :refer [e0 tx0 emax txmax]]
            [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [datahike.index.persistent-set :as dip]
            [datahike.query.execute :as ex]
            [datahike.test.async :refer-macros [deftest-async]]
            [konserve.core :as k]
            [konserve.protocols :as kp]
            [org.replikativ.persistent-sorted-set :as psset]))

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

(defn- fresh-conn []
  (go
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :read
               :keep-history? false}
          _ (<! (d/create-database cfg))
          conn (<! (d/connect cfg {:sync? false}))]
      (<! (d/transact! conn (mapv (fn [i] {:db/id (inc i) :name (str "n" i)})
                                  (range 400))))
      conn)))

(defn- cold-restored
  "Node-granular store + restore onto a fresh CachedStorage over an
   async-only wrapper (see cold-async-store-test)."
  [store eavt]
  (let [write-storage (dip/create-storage store {:store-cache-size 1000})
        addr (psset/store eavt write-storage {:sync? true})]
    (doseq [[a node] @(:pending-writes write-storage)]
      (k/assoc store a node {:sync? true}))
    (psset/restore {:root-address addr :comparator (.-comparator eavt)}
                   (dip/create-storage (->AsyncOnlyStore store) {:store-cache-size 1000}))))

(defn- run-kernel!
  "Drive execute-scan-only over `slice` in the given mode; returns
   {:tuples [[e v]…] :sync-completed? bool} (async mode) or tuples (sync)."
  [slice sync?]
  (let [result-list #js []
        find-source (int-array [-1 -3])          ;; find-src-scan-e, find-src-scan-v
        r (ex/execute-scan-only slice nil nil
                                nil 0
                                nil 0 -1
                                (make-array 0) 2 find-source (make-array 2)
                                result-list -1 nil sync?)]
    (if sync?
      (mapv vec result-list)
      {:expr r :result-list result-list})))

(defn- full-range []
  [(dd/datom e0 nil nil tx0) (dd/datom emax nil nil txmax)])

(deftest-async scan-only-dual-mode
  (let [conn (<! (fresh-conn))
        db @conn
        eavt (:eavt db)
        [from to] (full-range)
        sync-tuples (run-kernel! (di/-slice eavt from to :eavt) true)]
    (testing "sync baseline emits the transacted datoms"
      (is (>= (count sync-tuples) 400)))
    (testing "async over the WARM set: resolves on the calling stack, identical tuples"
      (let [aslice (atom ::pending)
            _ ((di/-slice eavt from to :eavt {:sync? false})
               (fn [v] (reset! aslice v)) (fn [e] (throw e)))
            _ (is (not= ::pending @aslice) "warm async slice resolves synchronously")
            {:keys [expr result-list]} (run-kernel! @aslice false)
            done (atom ::pending)]
        (expr (fn [_] (reset! done :ok)) (fn [e] (reset! done [:err e])))
        (is (= :ok @done) "warm async kernel completes before returning")
        (is (= sync-tuples (mapv vec result-list)))))
    (testing "async over a COLD async-only store: channel fallback, identical tuples"
      (let [cold (cold-restored (:store db) eavt)
            achan (a/promise-chan)
            _ ((di/-slice cold from to :eavt {:sync? false})
               (fn [v] (a/put! achan {:slice v})) (fn [e] (a/put! achan {:error e})))
            {aslice :slice err :error} (<! achan)]
        (is (nil? err) (str "cold async slice failed: " err))
        (let [{:keys [expr result-list]} (run-kernel! aslice false)
              done (a/promise-chan)]
          (expr (fn [_] (a/put! done {:ok true})) (fn [e] (a/put! done {:error e})))
          (let [{:keys [error]} (<! done)]
            (is (nil? error) (str "cold async kernel failed: " error))
            (is (= sync-tuples (mapv vec result-list))
                "cold-streamed tuples equal the warm sync baseline")))))))
