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
            [datahike.db :as ddb]
            [datahike.index.interface :as di]
            [datahike.index.persistent-set :as dip]
            [datahike.query :as q]
            [datahike.query.execute :as ex]
            [datahike.test.async :refer-macros [deftest-async]]
            [datahike.test.async-mode :as am]
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

(deftest-async q-async-zero-warmup
  ;; THE contract: the FIRST query ever — planning included — runs against a
  ;; store with zero synchronous read capability, no prior warm run, no plan
  ;; cache entry (unique attribute names guarantee a fresh plan-cache key).
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read :keep-history? false}
        _ (<! (d/create-database cfg))
        conn (<! (d/connect cfg {:sync? false}))
        _ (<! (d/transact! conn (mapv (fn [i] {:db/id (inc i)
                                               :zw-name (str "n" i)
                                               :zw-friend (inc (mod (* 7 i) 1200))})
                                      (range 600))))
        db @conn
        query '[:find ?b :where [?p :zw-name ?n] [?b :zw-friend ?p]]
        cold-db (-> db
                    (assoc :eavt (cold-restored (:store db) (:eavt db)))
                    (assoc :aevt (cold-restored (:store db) (:aevt db)))
                    (assoc :avet (cold-restored (:store db) (:avet db))))
        ch (a/promise-chan)]
    ;; cold q-async FIRST — nothing has ever touched these indices
    ((q/q-async query cold-db)
     (fn [v] (a/put! ch {:result v}))
     (fn [e] (a/put! ch {:error e})))
    (let [{:keys [result error]} (<! ch)
          warm (binding [q/*query-result-cache?* false] (d/q query db))]
      (is (nil? error) (str "zero-warmup q-async failed: " error))
      (is (pos? (count warm)))
      (is (= warm result)
          "first-ever query, cold store, zero warmup: q-async ≡ warm sync"))))

(deftest-async q-async-lookup-refs-cold
  ;; lookup refs in :in bindings and clause positions resolve via the entid
  ;; prefetch (dt/*entid-cache*) instead of synchronous avet probes
  (let [db (-> (ddb/empty-db {:lr-email {:db/unique :db.unique/identity}
                              :lr-friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1 :lr-email "a@x" :lr-score 1}
                           {:db/id 2 :lr-email "b@x" :lr-score 2 :lr-friend 1}]))
        handle (am/flush-db! db)
        q-in '[:find ?s . :in $ ?e :where [?e :lr-score ?s]]
        q-clause '[:find ?s . :where [[:lr-email "b@x"] :lr-score ?s]]
        q-coll '[:find ?e ?s :in $ [?e ...] :where [?e :lr-score ?s]]
        sync-in (binding [q/*query-result-cache?* false] (d/q q-in db [:lr-email "a@x"]))
        sync-clause (binding [q/*query-result-cache?* false] (d/q q-clause db))
        sync-coll (binding [q/*query-result-cache?* false]
                    (d/q q-coll db [[:lr-email "a@x"] [:lr-email "b@x"]]))]
    (is (= 1 sync-in))
    (is (= 2 sync-clause))
    (doseq [[label query args expected]
            [["scalar :in lookup ref" q-in [[:lr-email "a@x"]] sync-in]
             ["clause-position lookup ref" q-clause [] sync-clause]
             ["collection :in lookup refs" q-coll [[[:lr-email "a@x"] [:lr-email "b@x"]]] sync-coll]]]
      (let [cold (am/cold-db db handle)
            out (<! (apply am/run-query-in-mode :async-cold query cold args))]
        (is (nil? (:fault out)) (str label ": cold faulted: " (some-> (:fault out) ex-message)))
        (is (nil? (:error out)) (str label ": cold errored: " (some-> (:error out) ex-message)))
        (is (= expected (:result out)) (str label ": cold ≡ sync"))))))

(deftest-async q-async-pull-cold
  ;; pull find elements walk the index via the dual pull-api chain: nested
  ;; subpatterns, wildcard, and recursion all stream cold
  (let [db (-> (ddb/empty-db {:pl-friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1 :pl-name "a" :pl-friend 2}
                           {:db/id 2 :pl-name "b" :pl-friend 3}
                           {:db/id 3 :pl-name "c"}]))
        handle (am/flush-db! db)
        queries ['[:find (pull ?e [:pl-name {:pl-friend [:pl-name]}])
                   :where [?e :pl-name "a"]]
                 '[:find (pull ?e [*]) :where [?e :pl-name ?n]]
                 '[:find (pull ?e [:pl-name {:pl-friend ...}])
                   :where [?e :pl-name "a"]]]]
    (doseq [query queries]
      (let [sync-r (binding [q/*query-result-cache?* false] (d/q query db))
            cold (am/cold-db db handle)
            out (<! (am/run-query-in-mode :async-cold query cold))]
        (is (nil? (:fault out))
            (str (pr-str query) " cold faulted: " (some-> (:fault out) ex-message)))
        (is (nil? (:error out))
            (str (pr-str query) " cold errored: " (some-> (:error out) ex-message)))
        (is (= sync-r (:result out)) (str (pr-str query) " cold ≡ sync"))))))

(deftest-async sync-opts-public-api
  ;; the public API's async mode: `:sync? false` in the arg-map forms of
  ;; q / pull / pull-many / datoms / seek-datoms / rseek-datoms /
  ;; index-range returns a partial-cps async expression — no -async
  ;; function variants. All compared cold ≡ sync.
  (let [db (-> (ddb/empty-db {:so-email {:db/unique :db.unique/identity}
                              :so-friend {:db/valueType :db.type/ref}})
               (d/db-with [{:db/id 1 :so-email "a@x" :so-score 1 :so-friend 2}
                           {:db/id 2 :so-email "b@x" :so-score 2}]))
        handle (am/flush-db! db)
        run (fn [expr]
              (let [ch (a/promise-chan)]
                (expr (fn [v] (a/put! ch {:result v}))
                      (fn [e] (a/put! ch {:error e})))
                ch))
        norm (fn [datoms] (mapv (fn [d] [(:e d) (:a d) (:v d)]) datoms))]
    (testing "q with :sync? false in the arg-map"
      (let [query '[:find ?e ?s :where [?e :so-score ?s]]
            sync-r (binding [q/*query-result-cache?* false] (d/q query db))
            cold (am/cold-db db handle)
            {:keys [result error]} (<! (run (d/q {:query query :args [cold] :sync? false})))]
        (is (nil? error) (str "q errored: " (some-> error ex-message)))
        (is (= sync-r result))))
    (testing "pull / pull-many with :sync? false"
      (let [sel [:so-email {:so-friend [:so-email]}]
            sync-p (d/pull db sel 1)
            sync-pm (d/pull-many db sel [1 2])
            cold (am/cold-db db handle)
            p (<! (run (d/pull cold {:selector sel :eid 1 :sync? false})))
            pm (<! (run (d/pull-many cold {:selector sel :eids [1 2] :sync? false})))]
        (is (= sync-p (:result p)) (str (some-> (:error p) ex-message)))
        (is (= sync-pm (:result pm)) (str (some-> (:error pm) ex-message)))))
    (testing "datoms / seek-datoms / rseek-datoms / index-range with :sync? false"
      (doseq [[label sync-v mk]
              [["datoms" (d/datoms db {:index :eavt :components [1]})
                #(d/datoms % {:index :eavt :components [1] :sync? false})]
               ["seek-datoms" (d/seek-datoms db {:index :eavt :components [2]})
                #(d/seek-datoms % {:index :eavt :components [2] :sync? false})]
               ["rseek-datoms" (d/rseek-datoms db {:index :eavt :components [1]})
                #(d/rseek-datoms % {:index :eavt :components [1] :sync? false})]
               ["index-range" (d/index-range db {:attrid :so-email :start "a" :end "z"})
                #(d/index-range % {:attrid :so-email :start "a" :end "z" :sync? false})]]]
        (let [cold (am/cold-db db handle)
              {:keys [result error]} (<! (run (mk cold)))]
          (is (nil? error) (str label " errored: " (some-> error ex-message)))
          (is (= (norm sync-v) (norm result)) (str label " cold ≡ sync")))))))

(deftest-async q-async-tiny-cache-eviction
  ;; adversarial residency: a node cache of size 2 evicts constantly
  ;; mid-query — correctness must not depend on nodes staying resident
  ;; (the design premise the async engine replaced rejection-sampling with)
  (let [db (-> (ddb/empty-db {:ev-friend {:db/valueType :db.type/ref}})
               (d/db-with (mapv (fn [i] {:db/id (inc i)
                                         :ev-name (str "n" i)
                                         :ev-friend (inc (mod (* 7 i) 500))})
                                (range 500))))
        handle (am/flush-db! db)
        query '[:find ?b :where [?p :ev-name ?n] [?b :ev-friend ?p]]
        sync-r (binding [q/*query-result-cache?* false] (d/q query db))
        cold (am/cold-db db handle 2)
        out (<! (am/run-query-in-mode :async-cold query cold))]
    (is (pos? (count sync-r)))
    (is (nil? (:fault out)) (str "faulted: " (some-> (:fault out) ex-message)))
    (is (nil? (:error out)) (str "errored: " (some-> (:error out) ex-message)))
    (is (= sync-r (:result out)) "constant-eviction cold ≡ sync")))

(deftest-async q-async-cancellation
  ;; a pre-set :cancel volatile rejects the async expression with
  ;; :datahike/canceled instead of resolving
  (let [db (-> (ddb/empty-db)
               (d/db-with (mapv (fn [i] {:db/id (inc i) :cn-name (str "n" i)})
                                (range 200))))
        handle (am/flush-db! db)
        cold (am/cold-db db handle)
        cancel (volatile! true)
        ch (a/promise-chan)]
    ((d/q {:query '[:find ?e :where [?e :cn-name ?n]]
           :args [cold] :cancel cancel :sync? false})
     (fn [v] (a/put! ch {:result v}))
     (fn [e] (a/put! ch {:error e})))
    (let [{:keys [result error]} (<! ch)]
      (is (nil? result))
      (is (some? error) "canceled query must reject")
      (is (true? (:datahike/canceled (ex-data error)))
          (str "unexpected error: " (some-> error ex-message))))))

(deftest-async q-async-concurrent-cold
  ;; several async queries interleave on ONE shared cold store instance —
  ;; node-load dedup and cache races must not corrupt results
  (let [db (-> (ddb/empty-db {:cc-friend {:db/valueType :db.type/ref}})
               (d/db-with (mapv (fn [i] {:db/id (inc i)
                                         :cc-name (str "n" i)
                                         :cc-friend (inc (mod (* 11 i) 400))})
                                (range 400))))
        handle (am/flush-db! db)
        query '[:find ?b :where [?p :cc-name ?n] [?b :cc-friend ?p]]
        sync-r (binding [q/*query-result-cache?* false] (d/q query db))
        cold (am/cold-db db handle)
        chans (mapv (fn [_]
                      (let [ch (a/promise-chan)]
                        ((q/q-async query cold)
                         (fn [v] (a/put! ch {:result v}))
                         (fn [e] (a/put! ch {:error e})))
                        ch))
                    (range 3))]
    (doseq [ch chans]
      (let [{:keys [result error]} (<! ch)]
        (is (nil? error) (str "concurrent q errored: " (some-> error ex-message)))
        (is (= sync-r result) "concurrent cold ≡ sync")))))

(deftest-async q-async-date-as-of-cold
  ;; Date-based as-of/since wrappers are normalized to numeric time-points by
  ;; one awaited txInstant scan, then ride the pure txpred path cold.
  ;; (A real connection — store-less db-with does not stamp :db/txInstant.)
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read :keep-history? true}
        _ (<! (d/create-database cfg))
        conn (<! (d/connect cfg {:sync? false}))
        _ (<! (d/transact! conn [{:db/id 1 :da-name "one"}]))
        _ (<! (d/transact! conn [{:db/id 2 :da-name "two"}]))
        db @conn
        handle (am/flush-db! db)
        instants (sort-by #(.getTime %) (d/q '[:find [?t ...] :where [?tx :db/txInstant ?t]] db))
        cuts [(first instants)
              (js/Date. (+ (.getTime (last instants)) 10000))]
        query '[:find ?n :where [?e :da-name ?n]]]
    (is (seq instants))
    (doseq [cut cuts]
      (let [warm-asof (d/as-of db cut)
            sync-r (binding [q/*query-result-cache?* false] (d/q query warm-asof))
            cold-asof (d/as-of (am/cold-db db handle) cut)
            warm-out (<! (am/run-query-in-mode :async-warm query warm-asof))
            cold-out (<! (am/run-query-in-mode :async-cold query cold-asof))]
        (is (= sync-r (:result warm-out)) (str "warm async ≡ sync at " cut))
        (is (nil? (:fault cold-out)) (str "date as-of cold faulted at " cut ": "
                                          (some-> (:fault cold-out) ex-message)))
        (is (= sync-r (:result cold-out)) (str "date as-of cold ≡ sync at " cut))))))
