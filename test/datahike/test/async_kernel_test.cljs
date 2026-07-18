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
            [datahike.query :as q]
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
                                  (range 1200))))
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
      (is (>= (count sync-tuples) 1200)))
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

(defn- drive!
  "Run a kernel invocation fn (given sync?) in async mode against warm and
   cold slices; returns {:warm tuples :cold tuples} via the channel."
  [warm-eavt cold-eavt invoke!]
  (go
    (let [[from to] (full-range)
          resolve-slice (fn [idx]
                          (let [ch (a/promise-chan)]
                            ((di/-slice idx from to :eavt {:sync? false})
                             (fn [v] (a/put! ch {:slice v})) (fn [e] (a/put! ch {:error e})))
                            ch))
          run (fn [aslice]
                (let [ch (a/promise-chan)
                      {:keys [expr result-list]} (invoke! aslice false)]
                  (expr (fn [_] (a/put! ch {:tuples (mapv vec result-list)}))
                        (fn [e] (a/put! ch {:error e})))
                  ch))
          warm (<! (run (:slice (<! (resolve-slice warm-eavt)))))
          cold (<! (run (:slice (<! (resolve-slice cold-eavt)))))]
      {:warm warm :cold cold})))

(deftest-async card-many-kernel-dual-mode
  (let [conn (<! (fresh-conn))
        db @conn
        _ (<! (d/transact! conn (mapv (fn [i] {:db/id (inc i) :tag (str "t" i)}) (range 1200))))
        db @conn
        eavt (:eavt db)
        eavt-pss eavt
        merge-ctx (to-array [(to-array [:tag])      ;; merge-attrs
                             (to-array [false])     ;; v-ground
                             (to-array [nil])       ;; v-vals
                             (to-array [false])     ;; anti
                             (to-array [true])      ;; card-many
                             (to-array [false])     ;; check-scan-v
                             (to-array [false])     ;; check-scan-tx
                             (to-array [nil])       ;; cursors
                             (to-array [false])     ;; optional
                             (to-array [nil])])     ;; defaults
        invoke! (fn [slice sync?]
                  (let [result-list #js []
                        r (ex/execute-card-many-merge
                           db eavt-pss slice
                           (fn [^js d] (= :name (.-a d))) nil   ;; ground-filter: scan only :name datoms
                           nil 0 nil 0 -1
                           (make-array 1) 2 (int-array [-1 0]) (make-array 2)
                           result-list -1 1 merge-ctx nil sync?)]
                    {:expr r :result-list result-list}))
        sync-tuples (let [{:keys [result-list]} (invoke! (di/-slice eavt (first (full-range)) (second (full-range)) :eavt) true)]
                      (mapv vec result-list))
        cold (cold-restored (:store db) eavt)
        {:keys [warm cold]} (<! (drive! eavt cold invoke!))]
    (is (= 1200 (count sync-tuples)) "each :name datom merges its entity's :tag")
    (is (= sync-tuples (:tuples warm)) "warm async card-many ≡ sync")
    (is (= sync-tuples (:tuples cold)) "cold async card-many ≡ sync")))

(deftest-async per-cursor-kernel-dual-mode
  (let [conn (<! (fresh-conn))
        db @conn
        _ (<! (d/transact! conn (mapv (fn [i] {:db/id (inc i) :tag (str "t" i)}) (range 1200))))
        db @conn
        eavt (:eavt db)
        merge-ctx (to-array [(to-array [:tag])      ;; merge-attrs
                             (to-array [false])     ;; v-ground
                             (to-array [nil])       ;; v-vals
                             (to-array [false])     ;; anti
                             (to-array [nil])       ;; cursors
                             (to-array [false])     ;; check-scan-v
                             (to-array [false])     ;; check-scan-tx
                             (to-array [false])     ;; optional
                             (to-array [nil])])     ;; defaults
        invoke! (fn [slice sync?]
                  (let [result-list #js []
                        r (ex/execute-per-cursor-merge
                           eavt slice
                           (fn [^js d] (= :name (.-a d))) nil
                           nil 0 nil 0 -1
                           (make-array 1) 2 (int-array [-1 0]) (make-array 2)
                           result-list -1 1 merge-ctx nil sync?)]
                    {:expr r :result-list result-list}))
        sync-tuples (let [{:keys [result-list]} (invoke! (di/-slice eavt (first (full-range)) (second (full-range)) :eavt) true)]
                      (mapv vec result-list))
        cold (cold-restored (:store db) eavt)
        {:keys [warm cold]} (<! (drive! eavt cold invoke!))]
    (is (= 1200 (count sync-tuples)))
    (is (= sync-tuples (:tuples warm)) "warm async per-cursor ≡ sync")
    (is (= sync-tuples (:tuples cold)) "cold async per-cursor ≡ sync")))

(deftest-async group-direct-async-dispatch
  (let [conn (<! (fresh-conn))
        db @conn
        [from to] (full-range)
        ;; a REAL planner op for [?e :name ?n]
        plan (q/get-or-create-plan db '[[?e :name ?n]] #{} {} nil)
        op (first (:ops plan))
        _ (is (= :pattern-scan (:op op)) "planner produced a pattern scan")
        run-gd (fn [gd-db sync? slice-override]
                 (let [result-list #js []
                       r (ex/execute-group-direct gd-db op [] '[?e ?n] nil
                                                  result-list nil 0 nil 0 -1 nil
                                                  :pipeline (:pipeline op)
                                                  :sync? sync?
                                                  :slice-override slice-override)]
                   {:expr r :result-list result-list}))
        sync-tuples (mapv vec (:result-list (run-gd db true nil)))
        cold-eavt (cold-restored (:store db) (:eavt db))
        cold-db (assoc db :eavt cold-eavt)
        done (a/promise-chan)]
    (is (= 1200 (count sync-tuples)) "sync dispatch baseline")
    ;; async: group-direct is ONE dual body — it acquires the scan slice
    ;; itself and resolves to the filled result-list
    (let [{:keys [expr result-list]} (run-gd cold-db false nil)]
      (expr (fn [_] (a/put! done {:tuples (mapv vec result-list)}))
            (fn [e] (a/put! done {:error e}))))
    (let [{:keys [tuples error]} (<! done)]
      (is (nil? error) (str "async dispatch failed: " error))
      (is (= sync-tuples tuples)
          "cold async dispatch through execute-group-direct ≡ warm sync"))))

(deftest-async plan-direct-async
  (let [conn (<! (fresh-conn))
        _ (<! (d/transact! conn (mapv (fn [i] {:db/id (inc i) :friend (inc (mod (* 7 i) 1200))})
                                      (range 600))))
        db @conn
        plan1 (q/get-or-create-plan db '[[?e :name ?n]] #{} {} nil)
        plan2 (q/get-or-create-plan db '[[?p :name ?n] [?b :friend ?p]] #{} {} nil)
        sync1 (ex/execute-plan-direct plan1 db '[?e ?n] nil nil nil)
        sync2 (ex/execute-plan-direct plan2 db '[?b] nil nil nil)
        cold-db (-> db
                    (assoc :eavt (cold-restored (:store db) (:eavt db)))
                    (assoc :aevt (cold-restored (:store db) (:aevt db)))
                    (assoc :avet (cold-restored (:store db) (:avet db))))
        run-async (fn [plan find-vars]
                    (let [ch (a/promise-chan)]
                      ((ex/execute-plan-direct plan cold-db find-vars nil nil nil false)
                       (fn [v] (a/put! ch {:result v}))
                       (fn [e] (a/put! ch {:error e})))
                      ch))]
    (is (pos? (count sync1)))
    (is (pos? (count sync2)) "multi-group hash-probe has results")
    (let [{:keys [result error]} (<! (run-async plan1 '[?e ?n]))]
      (is (nil? error) (str "plan1 async failed: " error))
      (is (= sync1 result) "single-group plan async over cold store ≡ warm sync"))
    (let [{:keys [result error]} (<! (run-async plan2 '[?b]))]
      (is (nil? error) (str "plan2 async failed: " error))
      (is (= sync2 result) "multi-group hash-probe plan async over cold store ≡ warm sync"))))

(deftest-async q-async-end-to-end
  (let [conn (<! (fresh-conn))
        _ (<! (d/transact! conn (mapv (fn [i] {:db/id (inc i) :friend (inc (mod (* 7 i) 1200))})
                                      (range 600))))
        db @conn
        query '[:find ?b :where [?p :name ?n] [?b :friend ?p]]
        ;; warm sync run — also caches the PLAN (warmth-independent key),
        ;; so the cold async run below skips estimate reads entirely
        warm (d/q query db)
        cold-db (-> db
                    (assoc :eavt (cold-restored (:store db) (:eavt db)))
                    (assoc :aevt (cold-restored (:store db) (:aevt db)))
                    (assoc :avet (cold-restored (:store db) (:avet db))))]
    (is (pos? (count warm)))
    (testing "sync q over the cold store throws the decorated fault"
      ;; MUST run first: the async pass below warms the LRU, after which
      ;; sync reads legitimately succeed (the designed cold→warm transition)
      ;; result-cache off: the warm run above cached the RESULT under a
      ;; db-hash key the cold copy shares — a hit would skip the indices
      (let [e (try (binding [q/*query-result-cache?* false] (d/q query cold-db))
                   ::no-throw (catch :default e e))]
        (is (not= ::no-throw e))
        (when (not= ::no-throw e)
          (is (= :storage/sync-read-unavailable (:error (ex-data e)))))))
    (testing "q-async over an ALL-COLD async-only store ≡ warm sync"
      (let [ch (a/promise-chan)]
        ((q/q-async query cold-db)
         (fn [v] (a/put! ch {:result v}))
         (fn [e] (a/put! ch {:error e})))
        (let [{:keys [result error]} (<! ch)]
          (is (nil? error) (str "q-async cold failed: " error))
          (is (= warm result)))))
    (testing "q-async over the WARM store resolves on the calling stack"
      (let [r (atom ::pending)]
        ((q/q-async query db)
         (fn [v] (reset! r v)) (fn [e] (reset! r [:err e])))
        (is (= warm @r) "warm q-async completed synchronously with the sync result")))
    (testing "after the async pass the SAME cold store serves sync q (LRU warmed)"
      (is (= warm (binding [q/*query-result-cache?* false] (d/q query cold-db)))))))
