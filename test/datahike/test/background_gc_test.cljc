(ns datahike.test.background-gc-test
  "Background concurrent mark-and-sweep GC (datahike.gc/start-background-gc!).

   The portable smoke test (`deftest-async`) runs on JVM and cljs: it proves
   `gc-storage!` and `start-background-gc!` are invocable on both platforms,
   collect garbage, and leave data intact. The heavy stress tests are JVM-only —
   they need real threads/futures and a file backend to drive a pipelined writer
   concurrently with collection cycles (compressed versions of longer runs:
   133k txs at ~3k tx/s with concurrent sweeps, and divergent live branches)."
  #?(:clj  (:require [clojure.test :refer [deftest is testing]]
                     [clojure.set]
                     [datahike.api :as d]
                     [datahike.gc :as gc]
                     [datahike.gc-guard :as guard]
                     [datahike.versioning :as v]
                     [datahike.writing :as dw]
                     [datahike.test.async :refer [deftest-async]]
                     [konserve.core :as k]
                     [clojure.core.async :as a :refer [<! go]]
                     [superv.async :refer [<?? S]])
     :cljs (:require [cljs.test :refer [is testing] :include-macros true]
                     [datahike.api :as d]
                     [datahike.gc :as gc]
                     [datahike.gc-guard :as guard]
                     [datahike.test.async :refer-macros [deftest-async]]
                     ;; register the :file backend on Node (the smoke test needs a
                     ;; FLUSHING store — GC's mark walk requires stored index nodes,
                     ;; so :memory, which keeps the tree inline, does not apply).
                     [konserve.node-filestore]
                     [konserve.core :as k]
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
;; The safe point, CROSS-PLATFORM (JVM + Node). The corruption tests below are
;; JVM-only — they gate a mid-flush commit with real threads — so these cover the
;; same guarantee on cljs, where the bug is equally reachable (gc/writing are
;; .cljc, node's file writes are async, and go-blocks interleave on the event
;; loop). They gate NOTHING and redefine NOTHING: var redefinition would not
;; survive :advanced optimization on the node build, and a test that silently
;; stopped gating would pass vacuously.
;; ---------------------------------------------------------------------------

(deftest-async guard-spares-unreferenced-writes
  ;; The safe point's contract, stated directly: an object written while a write
  ;; sequence is OPEN is spared even though nothing references it; once the
  ;; sequence CLOSES it is ordinary garbage. That is exactly what makes a commit
  ;; mid-flush safe — and it is also the contract a user's out-of-band store write
  ;; relies on (a blob written before the transaction that names it).
  (let [id   #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))
        cfg  {:store {:backend :file :path (tmp-path id) :id id}
              :schema-flexibility :write :keep-history? false
              ;; :commit-graph? false is LOAD-BEARING — with the graph on, every
              ;; commit record stays reachable and the sweep collects nothing, so
              ;; both assertions below would hold vacuously.
              :commit-graph? false}
        _    #?(:clj  (do (when (d/database-exists? cfg) (d/delete-database cfg))
                          (d/create-database cfg))
                :cljs (do (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
                          (<! (d/create-database cfg))))
        conn #?(:clj  (d/connect cfg)
                :cljs (<! (d/connect cfg {:sync? false})))
        n    20]
    (<! (d/transact! conn schema))
    ;; churn, so the sweep has real garbage it IS allowed to take
    (loop [i 0]
      (when (< i (* 3 n))
        (<! (d/transact! conn {:tx-data [{:id (long (mod i n)) :score (long i)}]}))
        (recur (inc i))))
    (let [store (:store @conn)
          sid   (:id (:store (:config @conn)))
          probe :datahike.test/unreferenced-probe]
      (testing "an object written inside an open sequence is spared"
        (let [token (guard/writing! sid)]
          (<! (k/assoc store probe {:written "while the sequence is open"}))
          (let [swept (set (<! (gc/gc-storage! @conn)))]
            (is (pos? (count swept))
                "precondition: the cycle swept real garbage (not a vacuous pass)")
            (is (not (contains? swept probe))
                "the safe point spared an object that nothing references"))
          (guard/done! sid token)))
      (testing "once the sequence closes, the same object is collectable"
        (let [swept (set (<! (gc/gc-storage! @conn)))]
          (is (contains? swept probe)
              "a closed sequence's unreferenced object is ordinary garbage"))))
    (d/release conn)
    #?(:clj (d/delete-database cfg) :cljs (<! (d/delete-database cfg)))))

(deftest-async commit-holds-the-guard
  ;; The integration half: it is not enough that the safe point WORKS — commit!
  ;; must actually hold it across every object it writes, or the mechanism above
  ;; protects nothing in practice.
  ;;
  ;; Probed with a konserve write hook rather than a redefined var: hooks fire on
  ;; the real write path and survive :advanced optimization on node.
  ;;
  ;; Asserted with `in-flight?`, not with timestamps: `safe-point` returns `now`
  ;; both when nothing is in flight and when a sequence opened in the same
  ;; millisecond, so a clock comparison could not tell a held guard from a
  ;; missing one on a fast store.
  (let [id   #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid))
        cfg  {:store {:backend :file :path (tmp-path id) :id id}
              :schema-flexibility :write :keep-history? false}
        _    #?(:clj  (do (when (d/database-exists? cfg) (d/delete-database cfg))
                          (d/create-database cfg))
                :cljs (do (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
                          (<! (d/create-database cfg))))
        conn #?(:clj  (d/connect cfg)
                :cljs (<! (d/connect cfg {:sync? false})))]
    (<! (d/transact! conn schema))
    (let [store    (:store @conn)
          sid      (:id (:store (:config @conn)))
          observed (atom [])]
      (is (not (guard/in-flight? sid)) "no sequence is open before the commit")
      (k/add-write-hook! store ::probe
                         (fn [msg]                       ;; hook-fn takes ONE arg
                           (swap! observed conj {:key (:key msg)
                                                 :api-op (:api-op msg)
                                                 :guarded? (guard/in-flight? sid)})))
      (<! (d/transact! conn {:tx-data [{:id 1 :score 42}]}))
      (k/remove-write-hook! store ::probe)
      (is (seq @observed)
          "precondition: the commit actually wrote to the store")
      (is (every? :guarded? @observed)
          (str "every write a commit makes must happen with the guard OPEN; unguarded: "
               (pr-str (map :key (remove :guarded? @observed)))))
      (is (not (guard/in-flight? sid))
          "and the guard is released once the head has landed"))
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
           (d/delete-database cfg))))

     ;; -----------------------------------------------------------------------
     ;; The sweep must not delete the objects of a commit that is MID-FLUSH.
     ;;
     ;; `commit!` writes every value the new head references and only THEN flips
     ;; the branch head (the barrier invariant). A mark running inside that window
     ;; reads the OLD head, so those freshly written nodes are unreachable — AND
     ;; they pre-date `now`. A sweep cutoff of `now` therefore deletes them, and
     ;; the head then lands pointing at deleted objects. The cutoff must be the
     ;; store's SAFE POINT (datahike.gc-guard), which `commit!` holds open for
     ;; exactly the duration of that sequence.
     ;;
     ;; The window is gated deterministically — no sleeps: the head write is held
     ;; open until the collection has run.
     ;;
     ;; TWO preconditions are asserted, and they are not ceremony: without them a
     ;; broken guard that simply swept NOTHING would pass this test vacuously.
     ;;
     ;; NB :commit-graph? false is LOAD-BEARING. With the commit graph on and the
     ;; default remove-before (epoch), every commit record stays reachable, so the
     ;; sweep deletes nothing at all and the test is vacuous.
     (deftest sweep-spares-in-flight-commit
       (testing "a collection cycle racing a mid-flush commit leaves the store intact"
         (let [cfg   {:store {:backend :file
                              :path (str (System/getProperty "java.io.tmpdir") "/dh-bgc-inflight")
                              :id #uuid "b6c00000-0000-0000-0000-000000000003"}
                      :schema-flexibility :write :keep-history? false
                      :commit-graph? false}
               kset  (fn [store] (set (map :key (k/keys store {:sync? true}))))]
           (d/delete-database cfg)
           (d/create-database cfg)
           (let [conn (d/connect cfg)]
             (d/transact conn schema)
             ;; enough churn that the sweep has real garbage to collect
             (doseq [b (partition-all 500 (range 3000))]
               (d/transact conn (mapv (fn [i] {:id (long i) :score (long 0)}) b)))
             (let [store         (:store @conn)
                   before        (kset store)
                   nodes-written (promise)      ; writer -> test: nodes on disk, head not flipped
                   release-head  (promise)      ; test -> writer: flip the head now
                   orig          @#'dw/write-pending-kvs!
                   swept         (atom nil)
                   in-flight     (atom nil)]
               (with-redefs [dw/write-pending-kvs!
                             (fn [store kvs sync?]
                               (let [r (orig store kvs sync?)
                                     v (if (instance? clojure.core.async.impl.channels.ManyToManyChannel r)
                                         (a/<!! r) r)]
                                 (deliver nodes-written true)
                                 @release-head
                                 (if sync? v (go v))))]
                 (let [tx (future (d/transact conn (mapv (fn [i] {:id (long i) :score (long 1)})
                                                         (range 3000))))]
                   @nodes-written
                   (reset! in-flight (clojure.set/difference (kset store) before))
                   (reset! swept (set (<?? S (gc/gc-storage! @conn))))
                   (deliver release-head true)
                   @tx))
               (is (pos? (count @in-flight))
                   "precondition: the commit's nodes are on disk before the head flips")
               (is (pos? (count @swept))
                   "precondition: the cycle actually swept garbage (not a vacuous pass)")
               (is (empty? (clojure.set/intersection @swept @in-flight))
                   "the sweep deleted nodes the landing head references")
               (d/release conn))
             ;; cold read: no node cache can mask a missing node
             (let [c (d/connect cfg)]
               (is (= 3000 (d/q '[:find (count ?e) . :where [?e :id _]] @c))
                   "store is readable from disk after the raced collection")
               (d/release c)))
           (d/delete-database cfg))))

     ;; -----------------------------------------------------------------------
     ;; The SILENT half of the same bug. An in-flight commit writes `schema-meta`
     ;; before the head too. If the sweep takes it, `stored->db` does not throw —
     ;; it falls back (writing.cljc, `(or (:schema schema-meta) schema)`) — so the
     ;; database reopens with a schema that quietly lost the attribute the raced
     ;; commit added. A missing NODE is loud; a missing SCHEMA is not.
     (deftest sweep-spares-in-flight-schema
       (testing "a schema added by a mid-flush commit survives a racing collection"
         (let [cfg {:store {:backend :file
                            :path (str (System/getProperty "java.io.tmpdir") "/dh-bgc-schema")
                            :id #uuid "b6c00000-0000-0000-0000-000000000004"}
                    :schema-flexibility :write :keep-history? false
                    :commit-graph? false}]
           (d/delete-database cfg)
           (d/create-database cfg)
           (let [conn (d/connect cfg)]
             (d/transact conn schema)
             (doseq [b (partition-all 500 (range 2000))]
               (d/transact conn (mapv (fn [i] {:id (long i) :score (long 0)}) b)))
             (let [nodes-written (promise) release-head (promise)
                   orig          @#'dw/write-pending-kvs!]
               (with-redefs [dw/write-pending-kvs!
                             (fn [store kvs sync?]
                               (let [r (orig store kvs sync?)
                                     v (if (instance? clojure.core.async.impl.channels.ManyToManyChannel r)
                                         (a/<!! r) r)]
                                 (deliver nodes-written true)
                                 @release-head
                                 (if sync? v (go v))))]
                 ;; this commit ADDS an attribute, so it writes a NEW schema-meta
                 (let [tx (future (d/transact conn [{:db/ident :tag
                                                     :db/valueType :db.type/string
                                                     :db/cardinality :db.cardinality/one}]))]
                   @nodes-written
                   (is (pos? (count (<?? S (gc/gc-storage! @conn))))
                       "precondition: the cycle swept garbage (not a vacuous pass)")
                   (deliver release-head true)
                   @tx))
               ;; NOTHING may be transacted here. gc-storage! calls
               ;; sc/clear-write-cache, so the NEXT commit would rewrite
               ;; schema-meta and silently repair the damage — healing the very
               ;; thing this test exists to catch.
               (d/release conn))
             ;; cold reopen: the STORED schema must still know :tag. If the raced
             ;; sweep took schema-meta, stored->db falls back without erroring and
             ;; the attribute is simply gone — so assert on the schema, and then
             ;; prove it by transacting against it (:schema-flexibility :write
             ;; rejects an unknown attribute).
             (let [c (d/connect cfg)]
               (is (contains? (:schema @c) :tag)
                   "the attribute the raced commit added survived the collection")
               (d/transact c [{:id 1 :tag "ok"}])
               (is (= "ok" (d/q '[:find ?t . :where [?e :id 1] [?e :tag ?t]] @c)))
               (d/release c)))
           (d/delete-database cfg))))

     ;; -----------------------------------------------------------------------
     ;; `branch!` writes the new branch's head record and only THEN publishes it
     ;; into `:branches` — and GC builds its whitelist FROM `:branches`. A mark in
     ;; that window sees no such branch and deletes its head record.
     ;;
     ;; This is the case that decides WHERE the guard belongs: `branch!` runs on
     ;; the CALLER's thread and never touches the writer, so no amount of writer
     ;; serialization would cover it. The guard is in the store, so it does.
     (deftest sweep-spares-in-flight-branch
       (testing "a branch being created survives a racing collection"
         (let [cfg {:store {:backend :file
                            :path (str (System/getProperty "java.io.tmpdir") "/dh-bgc-branch")
                            :id #uuid "b6c00000-0000-0000-0000-000000000005"}
                    :schema-flexibility :write :keep-history? false}]
           (d/delete-database cfg)
           (d/create-database cfg)
           (let [conn (d/connect cfg)]
             (d/transact conn schema)
             (doseq [b (partition-all 500 (range 2000))]
               (d/transact conn (mapv (fn [i] {:id (long i) :score (long 0)}) b)))
             (let [head-written (promise) release-branches (promise)
                   orig         k/update]
               ;; gate between the new branch's head record and the `:branches` publish
               (with-redefs [k/update (fn [store key & more]
                                        (when (= key :branches)
                                          (deliver head-written true)
                                          @release-branches)
                                        (apply orig store key more))]
                 (let [br (future (d/branch! conn :db :experiment))]
                   @head-written
                   (<?? S (gc/gc-storage! @conn (java.util.Date.)))  ;; prune history => real sweeping
                   (deliver release-branches true)
                   @br))
               (d/release conn))
             ;; the new branch must be openable — its head record was not swept
             (let [c (d/connect (assoc cfg :branch :experiment))]
               (is (= 2000 (d/q '[:find (count ?e) . :where [?e :id _]] @c))
                   "the branch created during the collection is intact")
               (d/release c)))
           (d/delete-database cfg))))))
