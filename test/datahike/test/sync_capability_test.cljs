(ns datahike.test.sync-capability-test
  "cljs async foundation: the empirical store sync-capability probe, the
   dispatch gate on the synchronous q API, and CachedStorage's async restore
   arm (cache-hit and opportunistic-sync miss must complete SYNCHRONOUSLY —
   the property the sync-q-over-async-engine contract rests on)."
  (:require [cljs.test :refer [deftest is testing] :include-macros true]
            [clojure.core.async :as a :refer [<!] :refer-macros [go]]
            [cljs.cache :as cache]
            [datahike.api :as d]
            [datahike.sync-capability :as dsc]
            [datahike.test.async :refer-macros [deftest-async]]
            [konserve.core :as k]
            [org.replikativ.persistent-sorted-set.impl.storage :as storage]))

(defn- run-async
  "Invoke a partial-cps async expression NOW. Returns
   {:sync-completed? bool :value v-or-::pending}."
  [expr]
  (let [result (atom ::pending)]
    (expr (fn [v] (reset! result v))
          (fn [e] (reset! result [::error e])))
    {:sync-completed? (not= ::pending @result)
     :value @result}))

(defn- fresh-conn
  "Create + connect a fresh memory-store db with 500 entities; channel-returning."
  []
  (go
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :schema-flexibility :write}]
      (<! (d/create-database cfg))
      (let [conn (<! (d/connect cfg {:sync? false}))]
        (<! (d/transact! conn [{:db/ident :name :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one}]))
        (<! (d/transact! conn (vec (for [i (range 500)]
                                     {:db/id (+ 100 i) :name (str "n" i)}))))
        conn))))

(deftest-async memory-store-probes-sync-capable
  (let [conn (<! (fresh-conn))]
    (is (true? (dsc/sync-read-capable? (:store @conn))))
    (is (true? (dsc/sync-read-capable? (:store @conn)))
        "memoized second call agrees")))

(deftest-async sync-q-gate-throws-on-async-only-store
  (let [conn (<! (fresh-conn))]
    (with-redefs [dsc/sync-read-capable? (fn [_] false)]
      (is (thrown-with-msg? js/Error #"requires asynchronous access"
                            (d/q '[:find ?n :where [?e :name ?n]] @conn))
          "the SYNCHRONOUS q API fails upfront with an actionable error"))
    ;; and works again once the store reads as capable
    (is (= 500 (count (d/q '[:find ?n :where [?e :name ?n]] @conn))))))

(deftest-async async-restore-completes-synchronously-when-warm
  (let [conn (<! (fresh-conn))
        eavt (:eavt @conn)
        stor (.-storage eavt)
        ;; restore is agnostic to node contents (konserve get + LRU admit) —
        ;; store a marker value under a fresh address to exercise the seam
        addr (random-uuid)
        _ (<! (k/assoc (:store stor) addr {:marker :node} {:sync? false}))]
    (testing "cache hit: async restore resolves on the calling stack"
      (storage/restore stor addr {:sync? true}) ;; ensure cached
      (let [{:keys [sync-completed? value]} (run-async (storage/restore stor addr {:sync? false}))]
        (is (true? sync-completed?))
        (is (some? value))))
    (testing "cache MISS over a sync-capable store: opportunistic sync read
              keeps the computation on the synchronous trampoline"
      (reset! (:cache stor) (cache/lru-cache-factory {} :threshold 1000))
      (let [{:keys [sync-completed? value]} (run-async (storage/restore stor addr {:sync? false}))]
        (is (true? sync-completed?)
            "memory-store miss must not defer through the event loop")
        (is (some? value))))
    (testing "sync restore still works unchanged"
      (is (some? (storage/restore stor addr {:sync? true}))))))

(deftest-async async-restore-defers-on-async-only-store
  (testing "cache miss over an async-INCAPABLE store routes through the
            channel adapter (pends now, resolves on a later tick)"
    (let [conn (<! (fresh-conn))
          eavt (:eavt @conn)
          stor (.-storage eavt)
          addr (random-uuid)
          _ (<! (k/assoc (:store stor) addr {:marker :node} {:sync? false}))]
      (reset! (:cache stor) (cache/lru-cache-factory {} :threshold 1000))
      (with-redefs [dsc/sync-read-capable? (fn [_] false)]
        (let [result (atom ::pending)
              _ ((storage/restore stor addr {:sync? false})
                 (fn [v] (reset! result v))
                 (fn [e] (reset! result [::error e])))
              pended? (= ::pending @result)]
          ;; konserve async ops go through core.async go → nextTick, so even a
          ;; memory store cannot resolve inline on this path — exactly what an
          ;; async-only backend will do.
          (is (true? pended?) "channel-adapted miss defers instead of blocking")
          ;; give the event loop a beat and confirm it completes with the node
          (<! (a/timeout 10))
          (is (some? @result))
          (is (not= ::pending @result)))))))
