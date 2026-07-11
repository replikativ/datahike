(ns datahike.test.background-gc-test
  "Background concurrent mark-and-sweep GC (datahike.gc/start-background-gc!).

   The portable smoke test (`deftest-async`) runs on JVM and cljs: it proves
   `gc-storage!` and `start-background-gc!` are invocable on both platforms,
   collect garbage, and leave data intact. The heavy stress tests are JVM-only —
   they need real threads/futures and a file backend to drive a pipelined writer
   concurrently with collection cycles (compressed versions of longer runs:
   133k txs at ~3k tx/s with concurrent sweeps, and divergent live branches)."
  #?(:clj  (:require [clojure.test :refer [deftest is testing]]
                     [datahike.api :as d]
                     [datahike.gc :as gc]
                     [datahike.versioning :as v]
                     [datahike.test.async :refer [deftest-async]]
                     [konserve.core :as k]
                     [clojure.core.async :as a :refer [<! go]]
                     [superv.async :refer [<?? S]])
     :cljs (:require [cljs.test :refer [is testing] :include-macros true]
                     [datahike.api :as d]
                     [datahike.gc :as gc]
                     [datahike.test.async :refer-macros [deftest-async]]
                     ;; register the :file backend on Node (the smoke test needs a
                     ;; FLUSHING store — GC's mark walk requires stored index nodes,
                     ;; so :memory, which keeps the tree inline, does not apply).
                     [konserve.node-filestore]
                     [cljs.nodejs :as nodejs]
                     [clojure.core.async :as a :refer [<!] :refer-macros [go]])))

(def ^:private schema
  [{:db/ident :id :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])

(defn- now-date [] (#?(:clj java.util.Date. :cljs js/Date.)))

(defn- tmp-path [id]
  #?(:clj  (str (System/getProperty "java.io.tmpdir") "/dh-bgc-portable-" id)
     :cljs (let [os (nodejs/require "os") p (nodejs/require "path")]
             (.join p (.tmpdir os) (str "dh-bgc-portable-" id)))))

;; ---------------------------------------------------------------------------
;; Portable smoke test — JVM + Node cljs (file backend, since GC needs a
;; flushing store). Proves gc-storage! and start-background-gc! are invocable
;; on both platforms, collect garbage, and leave data intact.
;; ---------------------------------------------------------------------------

(deftest-async background-gc-invokable-cross-platform
  (let [id   #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))
        cfg  {:store {:backend :file :path (tmp-path id) :id id}
              :schema-flexibility :write :keep-history? false}
        ;; create + connect (sync on JVM, async on cljs)
        _    #?(:clj  (do (when (d/database-exists? cfg) (d/delete-database cfg))
                          (d/create-database cfg))
                :cljs (do (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
                          (<! (d/create-database cfg))))
        conn #?(:clj  (d/connect cfg)
                :cljs (<! (d/connect cfg {:sync? false})))
        n    20]
    (<! (d/transact! conn schema))
    ;; seed, then upsert every id repeatedly so earlier index/commit nodes are
    ;; superseded — that superseded set is what a full-history sweep reclaims.
    (loop [i 0]
      (when (< i (* 3 n))
        (<! (d/transact! conn {:tx-data [{:id (long (mod i n)) :score (long i)}]}))
        (recur (inc i))))
    (testing "gc-storage! runs and collects on both platforms; data intact"
      (let [swept (<! (gc/gc-storage! @conn (now-date)))]  ;; remove-before = now => prune old snapshots
        (is (not (instance? #?(:clj Throwable :cljs js/Error) swept))
            "gc-storage! completes without error")
        (is (set? swept) "returns the set of swept blobs")
        (is (pos? (count swept)) "reclaimed the superseded nodes"))
      (is (= n (d/q '[:find (count ?e) . :where [?e :id _]] @conn))
          "all entities still present after collection")
      (is (= (long (- (* 3 n) 1))
             (d/q '[:find ?s . :in $ ?id :where [?e :id ?id] [?e :score ?s]] @conn (long (dec n))))
          "last upsert's value survives"))
    (testing "start-background-gc! is invocable, runs cycles, and stops cleanly"
      (let [stop (gc/start-background-gc! conn {:interval-ms 30})]
        (<! (a/timeout 150))  ;; let a few cycles fire
        (is (= :stopped (stop)) "stop function returns :stopped")
        (<! (a/timeout 60)))  ;; let any in-flight cycle finish before teardown
      (is (= n (d/q '[:find (count ?e) . :where [?e :id _]] @conn))
          "data intact after background collection cycles"))
    (d/release conn)
    #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg)))))

;; ---------------------------------------------------------------------------
;; JVM-only stress: real threads/futures + file backend
;; ---------------------------------------------------------------------------

#?(:clj
   (do
     (defn- files [store] (count (k/keys store {:sync? true})))

     (deftest background-gc-under-pipelined-writes
       (testing "concurrent collection cycles + pipelined writer: exact data, contained store"
         (let [cfg {:store {:backend :file
                            :path (str (System/getProperty "java.io.tmpdir") "/dh-bgc-pipe")
                            :id #uuid "b6c00000-0000-0000-0000-000000000001"}
                    :schema-flexibility :write :keep-history? false}]
           (d/delete-database cfg)
           (d/create-database cfg)
           (let [conn (d/connect cfg)]
             (d/transact conn schema)
             (doseq [b (partition-all 2000 (range 5000))]
               (d/transact conn (mapv (fn [i] {:id (long i) :score (long 0)}) b)))
             (let [model    (atom (into {} (for [i (range 5000)] [i 0])))
                   stop-gc  (gc/start-background-gc! conn {:interval-ms 3000})
                   inflight (java.util.concurrent.Semaphore. 64)
                   deadline (+ (System/currentTimeMillis) 12000)
                   tx-error (atom nil)]
               (loop [i 0]
                 (when (< (System/currentTimeMillis) deadline)
                   (.acquire inflight)
                   (let [id (long (mod i 5000)) v (long i)]
                     (swap! model assoc id v)
                     (a/take! (d/transact! conn {:tx-data [{:id id :score v}]})
                              (fn [r]
                                (when (instance? Throwable r) (compare-and-set! tx-error nil r))
                                (.release inflight))))
                   (recur (inc i))))
               (.acquire inflight 64)
               (stop-gc)
               (is (nil? @tx-error) "no transact failed during concurrent collection")
               ;; a final full sweep contains the store (writes are quiesced now)
               (<?? S (gc/gc-storage! @conn (java.util.Date.)))
               (let [store (:store @conn)
                     remaining (files store)]
                 (is (< remaining 1000)
                     (str "store contained after final sweep (got " remaining " objects)")))
               (is (= @model (into {} (d/q '[:find ?id ?s :where [?e :id ?id] [?e :score ?s]] @conn)))
                   "warm state matches the model exactly")
               (d/release conn))
             (let [conn2 (d/connect cfg)]
               (is (= 5000 (d/q '[:find (count ?e) . :where [?e :id _]] @conn2))
                   "cold reopen after collection is intact")
               (d/release conn2)))
           (d/delete-database cfg))))

     (deftest background-gc-with-live-branches
       (testing "collection with two diverging branches under writes keeps both exact"
         (let [cfg {:store {:backend :file
                            :path (str (System/getProperty "java.io.tmpdir") "/dh-bgc-branch")
                            :id #uuid "b6c00000-0000-0000-0000-000000000002"}
                    :schema-flexibility :write :keep-history? true}]
           (d/delete-database cfg)
           (d/create-database cfg)
           (let [conn (d/connect cfg)]
             (d/transact conn schema)
             (doseq [b (partition-all 2000 (range 4000))]
               (d/transact conn (mapv (fn [i] {:id (long i) :score (long 0)}) b)))
             (v/branch! conn :db :experiment)
             (let [conn-exp (d/connect (assoc cfg :branch :experiment))]
               (dotimes [i 150] (d/transact conn [{:id (long (mod i 4000)) :score (long (+ 1000 i))}]))
               (dotimes [i 100] (d/transact conn-exp [{:id (long (mod (* i 3) 4000)) :score (long (- -1000 i))}]))
               ;; full-range collection concurrent with more main-branch writes
               (let [gc-fut (future (<?? S (gc/gc-storage! @conn (java.util.Date.))))]
                 (dotimes [i 100] (d/transact conn [{:id (long i) :score (long 7777)}]))
                 @gc-fut)
               (let [main-vals (set (d/q '[:find [?s ...] :where [_ :score ?s]] @conn))
                     exp-vals  (set (d/q '[:find [?s ...] :where [_ :score ?s]] @conn-exp))]
                 (is (contains? main-vals 7777) "main branch has its writes")
                 (is (not-any? neg? main-vals) "main branch has no experiment writes")
                 (is (some neg? exp-vals) "experiment branch has its writes")
                 (is (not (contains? exp-vals 7777)) "experiment branch has no main writes"))
               (d/release conn) (d/release conn-exp))
             ;; cold: both branches restore after collection
             (let [c1 (d/connect cfg)
                   c2 (d/connect (assoc cfg :branch :experiment))]
               (is (= 4000 (d/q '[:find (count ?e) . :where [?e :id _]] @c1)))
               (is (= 4000 (d/q '[:find (count ?e) . :where [?e :id _]] @c2)))
               (d/release c1) (d/release c2)))
           (d/delete-database cfg))))))
