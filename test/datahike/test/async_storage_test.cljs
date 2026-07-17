(ns datahike.test.async-storage-test
  "cljs async foundation: CachedStorage's per-read sync/async semantics.

   Sync-capability is a property of EACH READ, not of the store (a tiered
   store serves its warm memory frontend synchronously; only a frontend miss
   needs the async backend — browser CI proved a store-level classification
   wrong). Pins:
   - async restore over a warm datahike LRU resolves ON THE CALLING STACK
     (partial-cps trampolining — the property the future sync-q-over-async-
     engine contract rests on);
   - an LRU miss tries the synchronous read first and stays on the trampoline
     when the store can serve it;
   - when the synchronous read throws (async-only backend / tiered frontend
     miss), the async arm falls back to the channel-adapted read and defers;
   - the SYNC arm surfaces that situation as an actionable error instead of
     konserve's internal assertion."
  (:require [cljs.test :refer [deftest is testing] :include-macros true]
            [clojure.core.async :as a :refer [<!] :refer-macros [go]]
            [cljs.cache :as cache]
            [datahike.api :as d]
            [datahike.test.async :refer-macros [deftest-async]]
            [konserve.core :as k]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]))

(defn- run-async
  "Invoke a partial-cps async expression NOW. Returns
   {:sync-completed? bool :value v-or-::pending :result-atom atom}."
  [expr]
  (let [result (atom ::pending)]
    (expr (fn [v] (reset! result v))
          (fn [e] (reset! result [::error e])))
    {:sync-completed? (not= ::pending @result)
     :value @result
     :result-atom result}))

(defn- fresh-storage
  "Create a memory-store db, return its eavt CachedStorage with a marker
   value stored under a fresh address (restore is agnostic to node contents —
   konserve get + LRU admit)."
  []
  (go
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :write}]
      (<! (d/create-database cfg))
      (let [conn (<! (d/connect cfg {:sync? false}))
            _ (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                      :db/cardinality :db.cardinality/one}]))
            stor (.-storage (:eavt @conn))
            addr (random-uuid)]
        (<! (k/assoc (:store stor) addr {:marker :node} {:sync? false}))
        {:stor stor :addr addr}))))

(defn- evict! [stor]
  (reset! (:cache stor) (cache/lru-cache-factory {} :threshold 1000)))

(deftest-async async-restore-completes-synchronously-when-warm
  (let [{:keys [stor addr]} (<! (fresh-storage))]
    (testing "cache hit: async restore resolves on the calling stack"
      (storage/restore stor addr {:sync? true}) ;; ensure cached
      (let [{:keys [sync-completed? value]} (run-async (storage/restore stor addr {:sync? false}))]
        (is (true? sync-completed?))
        (is (some? value))))
    (testing "cache MISS over a sync-readable store: the per-read sync-first
              attempt keeps the computation on the synchronous trampoline"
      (evict! stor)
      (let [{:keys [sync-completed? value]} (run-async (storage/restore stor addr {:sync? false}))]
        (is (true? sync-completed?)
            "memory-store miss must not defer through the event loop")
        (is (some? value))))
    (testing "sync restore works unchanged"
      (is (some? (storage/restore stor addr {:sync? true}))))))

(deftest-async async-restore-falls-back-to-channel-when-sync-read-throws
  (testing "an LRU miss whose synchronous read throws (async-only backend /
            tiered frontend miss) routes through the channel adapter"
    (let [{:keys [stor addr]} (<! (fresh-storage))
          orig-get k/get]
      (evict! stor)
      (with-redefs [k/get (fn
                            ([store key] (orig-get store key))
                            ([store key default] (orig-get store key default))
                            ([store key default opts]
                             (if (:sync? opts)
                               (throw (ex-info "Synchronous operations not supported" {}))
                               (orig-get store key default opts))))]
        (let [{:keys [sync-completed? result-atom]} (run-async (storage/restore stor addr {:sync? false}))]
          (is (false? sync-completed?)
              "channel-adapted miss defers instead of blocking")
          (<! (a/timeout 10))
          (is (= {:marker :node} @result-atom)
              "…and resolves with the stored value on a later tick"))))))

(deftest-async sync-restore-raises-actionable-error-on-async-only-read
  (testing "the SYNC arm surfaces an async-only read as an actionable error"
    (let [{:keys [stor addr]} (<! (fresh-storage))
          orig-get k/get]
      (evict! stor)
      (with-redefs [k/get (fn
                            ([store key] (orig-get store key))
                            ([store key default] (orig-get store key default))
                            ([store key default opts]
                             (if (:sync? opts)
                               (throw (ex-info "Synchronous operations not supported" {}))
                               (orig-get store key default opts))))]
        (is (thrown-with-msg? js/Error #"requires asynchronous store access"
                              (storage/restore stor addr {:sync? true})))))))
