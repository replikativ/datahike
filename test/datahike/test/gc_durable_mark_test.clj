(ns datahike.test.gc-durable-mark-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.gc-roots :as roots]
            [datahike.index.interface :as idx]
            [datahike.index.audit :as audit]
            [datahike.index.persistent-set :as pset]
            [datahike.versioning :as v]
            [konserve.core :as k]
            [konserve.memory :as mem]
            [org.replikativ.persistent-sorted-set :as pss]
            [konserve.gc :as kgc]
            [superv.async :refer [<?? S]])
  (:import [java.util Date]
           [org.replikativ.persistent_sorted_set Branch Leaf IStorage PersistentSortedSet]))

(def families [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn raw-closure
  "Oracle reads persisted node fields, without using the marker or PSS descent."
  [store root-address inline]
  (loop [pending [[root-address inline]] result #{}]
    (if-let [[address inline] (peek pending)]
      (if (contains? result address)
        (recur (pop pending) result)
        (let [node (or inline (k/get store address nil {:sync? true}))]
          (when-not (or (instance? Branch node) (instance? Leaf node))
            (throw (ex-info "Oracle encountered a missing node" {:address address})))
          (recur (into (pop pending)
                       (when (instance? Branch node) (map #(vector % nil) (.addresses ^Branch node))))
                 (conj result address))))
      result)))

(defn record-marks [store record context]
  (mapv (fn [family]
          (let [key (keyword (str (name family) "-key"))
                root-key (keyword (str (name family) "-root"))
                index (idx/with-storage :datahike.index/persistent-set (get record key) (:storage store))
                inline (get record root-key)
                index (cond-> index inline (idx/-seed-root! inline))]
            {:oracle (raw-closure store (audit/-merkle-root index) inline)
             :marked (idx/mark-shared index context)})) families))

(deftest buffered-durable-mark-survives-sweep-and-cold-reopen
  (doseq [budget [1 4 64] fused? [false true]]
    (let [id (random-uuid)
          cfg {:store {:backend :file :id id :path (str "/tmp/dh-durable-mark-" id)}
               :writer {:backend :self :writer-ownership :exclusive}
               :schema-flexibility :write :keep-history? true :crypto-hash? true
               :fuse-index-roots? fused? :index-config {:branching-factor 8 :diff-buf-size budget}}]
      (d/create-database cfg)
      (try
        (let [conn (d/connect cfg) released? (atom false)]
          (try
            (d/transact conn [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
                              {:db/ident :payload :db/valueType :db.type/store-ref :db/cardinality :db.cardinality/one}])
            (d/transact conn (mapv #(hash-map :n %) (range 600)))
            (let [ids (mapv :e (d/datoms @conn :aevt :n))
                  store (:store @conn)
                  old-blob (random-uuid) new-blob (random-uuid) orphan (random-uuid)]
              (doseq [blob [old-blob new-blob orphan]] (k/assoc store blob :bytes {:sync? true}))
              (d/transact conn [{:db/id (first ids) :payload old-blob}])
              (let [old @conn pin (roots/pin! old {:ttl-ms nil} {:sync? true})
                    old-values (d/q '[:find ?e ?n :where [?e :n ?n]] old)
                    old-t (:max-tx old)]
                (v/branch! conn :db :retained)
                (d/transact conn [{:db/id (first ids) :payload new-blob}])
                (doseq [round (range 8)]
                  (d/transact conn (mapv (fn [j] {:db/id (nth ids (mod (+ (* round 17) (* j 7)) 600))
                                                  :n (+ 10000 (* round 32) j)}) (range 32))))
                (d/transact conn (mapv #(hash-map :n %) (range 20000 20030)))
                (d/transact conn (mapv #(vector :db/retractEntity %) (take 20 (drop 10 ids))))
                (let [expected (d/q '[:find ?e ?n :where [?e :n ?n]] @conn)
                      context (idx/new-mark-context (:storage store))
                      current (k/get store :db nil {:sync? true})
                      retained (k/get store :retained nil {:sync? true})
                      marked (concat (record-marks store current context) (record-marks store retained context))
                      oracle (apply set/union (map :oracle marked))]
                  (is (= 610 (count expected)))
                  (is (= oracle (:addresses @context)))
                  (is (pos? (:pruned @context)))
                  (is (= (count oracle) (:expanded @context)))
                  (is (contains? (<?? S (gc/gc-storage! @conn (Date. 0) {:min-age-ms 0})) orphan))
                  (is (k/exists? store old-blob {:sync? true}))
                  (is (k/exists? store new-blob {:sync? true}))
                  (roots/release! old pin {:sync? true})
                  (d/release conn)
                  (reset! released? true)
                  (let [reopened (d/connect cfg)
                        retained-conn (d/connect (assoc cfg :branch :retained))]
                    (try
                      (is (= expected (d/q '[:find ?e ?n :where [?e :n ?n]] @reopened)))
                      (is (= old-values (d/q '[:find ?e ?n :where [?e :n ?n]] @retained-conn)))
                      (is (= old-values (d/q '[:find ?e ?n :where [?e :n ?n]] (d/as-of @reopened old-t))))
                      (finally (d/release reopened) (d/release retained-conn)))))))
            (finally (when-not @released? (d/release conn)))))
        (finally (d/delete-database cfg))))))

(deftest missing-or-invalid-durable-edges-abort-before-sweep
  (let [id (random-uuid)
        cfg {:store {:backend :file :id id :path (str "/tmp/dh-durable-errors-" id)}
             :schema-flexibility :write :index-config {:branching-factor 8 :diff-buf-size 4}}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :value :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        (d/transact conn (mapv #(hash-map :value %) (range 100)))
        (doseq [bad [{:level 2 :children [nil]} {:level 0 :children [(random-uuid)]}
                     {:level 1 :children []} nil]]
          (let [swept (atom false)]
            (with-redefs [pset/read-durable-node-edges (fn [& _] bad)
                          kgc/sweep! (fn [& _] (reset! swept true) #{})]
              (is (thrown? Exception (<?? S (gc/gc-storage! @conn (Date. 0) {:min-age-ms 0}))))
              (is (not @swept)))))
        (let [store (:store @conn) record (k/get store :db nil {:sync? true})
              root-address (audit/-merkle-root (:eavt-key record))
              addresses (raw-closure store root-address nil)
              inner-address (first (filter #(and (not= % root-address)
                                                  (instance? Branch (k/get store % nil {:sync? true})))
                                            addresses))]
          ;; Branches are read and validated. Leaves are named by their parent
          ;; and not read (as on main and in #1123), so a missing leaf is not
          ;; detected by the mark.
          (is (some? inner-address))
          (doseq [address [root-address inner-address]]
            (let [saved (k/get store address nil {:sync? true})
                  swept (atom false)]
              (k/dissoc store address {:sync? true})
              (try
                (with-redefs [kgc/sweep! (fn [& _] (reset! swept true) #{})]
                  (is (thrown? Exception (<?? S (gc/gc-storage! @conn (Date. 0) {:min-age-ms 0}))))
                  (is (not @swept)))
                (finally (k/assoc store address saved {:sync? true}))))))
        (finally (d/release conn) (d/delete-database cfg))))))

(deftest published-memory-nodes-are-detached-snapshots
  (let [store (mem/new-mem-store (atom {}) {:sync? true})
        storage (pset/create-storage store {:crypto-hash? true :store-cache-size 10000})
        cmp compare
        initial (into (pss/sorted-set* {:comparator cmp :storage storage
                                        :branching-factor 8 :diff-buf-size 1})
                      (mapv #(vector % 0) (range 1024)))
        publish! (fn [tree]
                   (let [address (pss/store tree)]
                     (doseq [[a node] @(:pending-writes storage)]
                       (k/assoc store a node {:sync? true}))
                     (reset! (:pending-writes storage) [])
                     address))
        first-address (publish! initial)
        stored-root (k/get store first-address nil {:sync? true})
        original-edges (pset/node-edges stored-root)
        second-tree (reduce (fn [tree n] (-> tree (disj [n 0]) (conj [n 1])))
                            initial (range 64))
        second-address (publish! second-tree)
        oracle (set/union (raw-closure store first-address nil)
                          (raw-closure store second-address nil))
        context (idx/new-mark-context storage)]
    (is (identical? stored-root (k/get store first-address nil {:sync? true}))
        "Memory storage preserves object identity; reads alone do not detach nodes")
    (is (not (identical? stored-root (.root ^PersistentSortedSet initial))))
    (is (nil? (.childrenArray ^Branch stored-root)))
    (is (= original-edges (pset/node-edges stored-root)))
    (is (= 1024 (count (vec initial))))
    (is (= 1024 (count (vec second-tree))))
    (idx/mark-shared initial context)
    (idx/mark-shared second-tree context)
    (is (= oracle (:addresses @context)))
    (is (= (count oracle) (:expanded @context)))
    (is (pos? (:pruned @context)))))

(deftest child-first-conflicting-fused-descriptor-fails-closed
  (let [root (random-uuid) child (random-uuid) leaf (random-uuid)
        descriptors {root {:level 2 :children [child]}
                     child {:level 1 :children [leaf]}
                     leaf {:level 0 :children []}}
        storage (reify IStorage
                  (store [_ _] (throw (UnsupportedOperationException.)))
                  (restore [_ _] (throw (UnsupportedOperationException.)))
                  (accessed [_ _] nil) (markFreed [_ _] nil)
                  (isFreed [_ _] false) (freedInfo [_ _] nil)
                  idx/IDurableNodeEdges
                  (-durable-node-edges [_ address] (get descriptors address)))
        restore (fn [address] (pss/restore-by compare address storage
                                              {:branching-factor 8 :diff-buf-size 4}))]
    (doseq [bad [{:level 1 :children [(random-uuid)]}
                 {:level 0 :children [(random-uuid)]}]]
      (let [context (idx/new-mark-context storage)
            inline (with-meta (restore child)
                     {::pset/fused-root-edges {:address child :edges bad}})]
        (is (= #{root child leaf} (idx/mark-shared (restore root) context)))
        (is (thrown? Exception (idx/mark-shared inline context)))
        (is (:failed? @context))))))

(deftest inline-proof-does-not-substitute-for-a-required-stored-child
  (let [parent (random-uuid) child (random-uuid)]
    (doseq [present? [false true]]
      (let [reads (atom [])
            storage (reify IStorage
                      (store [_ _] (throw (UnsupportedOperationException.)))
                      (restore [_ _] (throw (UnsupportedOperationException.)))
                      (accessed [_ _] nil) (markFreed [_ _] nil)
                      (isFreed [_ _] false) (freedInfo [_ _] nil)
                      idx/IDurableNodeEdges
                      (-durable-node-edges [_ address]
                        (swap! reads conj address)
                        (cond (= address parent) {:level 1 :children [child]}
                              (and present? (= address child)) {:level 0 :children []}
                              :else (throw (ex-info "Missing physical child" {:address address})))))
            restore (fn [a] (pss/restore-by compare a storage {:branching-factor 8 :diff-buf-size 4}))
            inline (with-meta (restore child)
                     {::pset/fused-root-edges {:address child :edges {:level 0 :children []}}})
            context (idx/new-mark-context storage)]
        (is (= #{child} (idx/mark-shared inline context)))
        (is (empty? @reads))
        (if present?
          (do (is (= #{parent} (idx/mark-shared (restore parent) context))
                  "a call returns only what it claimed; the inline call claimed child")
              (is (= #{parent child} (:addresses @context))))
          (do (is (thrown? Exception (idx/mark-shared (restore parent) context)))
              (is (:failed? @context))))
        (is (= [parent child] @reads))))))

(defn- read-recorder
  "Wraps the durable-edge reader; returns [levels-read-atom redef-fn]."
  []
  (let [reads (atom []) orig pset/read-durable-node-edges]
    [reads (fn [storage address]
             (let [edges (orig storage address)] (swap! reads conj (:level edges)) edges))]))

(defn- small-history-db [cfg]
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn [{:db/ident :value :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
    (dotimes [i 30] (d/transact conn (mapv #(hash-map :value (+ (* i 40) %)) (range 40))))
    conn))

(deftest reachable-store-refs-walks-no-index-nodes
  ;; Review guard: without a shared context every commit's trees were read in
  ;; full from disk (454 ms on main became 78 s). Store-refs come from the aevt
  ;; slice and the schema; the structural mark is not needed at all.
  (let [id (random-uuid)
        cfg {:store {:backend :file :id id :path (str "/tmp/dh-store-refs-cost-" id)}
             :keep-history? true :schema-flexibility :write
             :index-config {:branching-factor 8 :diff-buf-size 4}}
        conn (small-history-db cfg)]
    (try
      (let [[reads recorder] (read-recorder)
            marks (atom 0)
            mark-shared idx/mark-shared]
        (with-redefs [pset/read-durable-node-edges recorder
                      idx/mark-shared (fn [i c] (swap! marks inc) (mark-shared i c))]
          (is (= #{} (<?? S (gc/reachable-store-refs @conn (Date. 0))))))
        (is (zero? @marks))
        (is (empty? @reads)))
      (finally (d/release conn) (d/delete-database cfg)))))

(deftest gc-with-online-gc-reads-branches-only
  (let [id (random-uuid)
        cfg {:store {:backend :file :id id :path (str "/tmp/dh-online-gc-mark-" id)}
             :keep-history? false :schema-flexibility :write
             :index-config {:branching-factor 8}
             :online-gc {:enabled? true :grace-period-ms 0 :max-batch 1000}}
        conn (small-history-db cfg)]
    (try
      (let [[reads recorder] (read-recorder)
            expected (d/q '[:find (count ?e) . :where [?e :value]] @conn)]
        (with-redefs [pset/read-durable-node-edges recorder]
          (<?? S (gc/gc-storage! @conn (Date.) {:min-age-ms 0})))
        (is (seq @reads))
        ;; Only a root can be read at level 0 (its level is not known before
        ;; the read); the head commit has three trees without history.
        (is (<= (count (filter zero? @reads)) 3)
            "leaves are named by their parent, not read")
        (is (some pos? @reads))
        (d/release conn)
        (let [reopened (d/connect cfg)]
          (try (is (= expected (d/q '[:find (count ?e) . :where [?e :value]] @reopened)))
               (finally (d/release reopened)))))
      (finally (d/release conn) (d/delete-database cfg)))))

(deftest buffered-mark-without-durable-edges-fails-closed
  (let [storage (reify IStorage
                  (store [_ _] (throw (UnsupportedOperationException.)))
                  (restore [_ _] (throw (UnsupportedOperationException. "must not walk")))
                  (accessed [_ _] nil) (markFreed [_ _] nil)
                  (isFreed [_ _] false) (freedInfo [_ _] nil))
        tree (pss/restore-by compare (random-uuid) storage {:branching-factor 8 :diff-buf-size 4})
        context (idx/new-mark-context storage)]
    (is (= :datahike/gc-durable-edges-unavailable
           (try (idx/mark-shared tree context) nil
                (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))))
    (is (:failed? @context))
    (is (= :datahike/gc-durable-edges-unavailable
           (try (idx/-mark tree) nil
                (catch clojure.lang.ExceptionInfo e (:type (ex-data e))))))))
