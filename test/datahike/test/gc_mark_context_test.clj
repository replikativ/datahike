(ns datahike.test.gc-mark-context-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.gc-roots :as roots]
            [datahike.index.interface :as idx]
            [datahike.index.persistent-set :as pset]
            [datahike.versioning :as v]
            [konserve.core :as k]
            [konserve.gc :as kgc]
            [org.replikativ.persistent-sorted-set :as pss]
            [superv.async :refer [<?? S]])
  (:import [java.util Date]
           [org.replikativ.persistent_sorted_set ANode Branch IStorage Leaf PersistentSortedSet Settings Slot]))

(deftest diff-buffer-warm-walk-does-not-poison-a-later-cold-mark
  ;; Store snapshots of the durable representation, never live node references.
  ;; A small budget and mixed updates reach a resident buffered child whose
  ;; address array contains nil while its durable anchor names another blob.
  (let [disk (atom {})
        settings (Settings. 8 nil nil nil 1)
        mk-storage (fn mk-storage []
                     (let [cache (atom {})]
                       (reify IStorage
                         (store [_ node]
                           (let [^ANode n node a (random-uuid)]
                             (swap! disk assoc a
                                    {:level (.level n) :keys (vec (.keys n))
                                     :addresses (when (instance? Branch n) (vec (.addresses ^Branch n)))
                                     :slots (when (instance? Branch n) (.slotsForStorage ^Branch n))})
                             a))
                         (restore [_ a]
                           (or (get @cache a)
                               (let [{:keys [level keys addresses slots]} (get @disk a)
                                     n (if addresses (Branch. (int level) ^java.util.List keys ^java.util.List addresses settings)
                                           (Leaf. ^java.util.List keys settings))]
                                 (when (seq slots)
                                   (let [arr (object-array (count addresses))]
                                     (doseq [[i entry] slots]
                                       (aset arr (int i) (Slot. (:diff entry) (long (:count entry))
                                                                (:measure entry) (nth addresses (int i)))))
                                     (.installSlots ^Branch n arr Branch/BUF_LAZY)))
                                 (swap! cache assoc a n)
                                 n)))
                         (accessed [_ _] nil)
                         (markFreed [_ _] nil)
                         (isFreed [_ _] false)
                         (freedInfo [_ _] nil)
                         idx/IDurableNodeEdges
                         (-durable-node-edges [_ a]
                           (let [{:keys [level addresses]} (get @disk a)]
                             (when-not (contains? @disk a) (throw (ex-info "Missing node" {:address a})))
                             {:level level :children (or addresses [])})))))
        cmp (fn [[a av] [b bv]] (let [c (compare a b)] (if (zero? c) (compare av bv) c)))
        cmp-key (fn [[a _] [b _]] (compare a b))
        restore (fn [a storage] (pss/restore-by cmp a storage {:branching-factor 8 :diff-buf-size 1}))
        initial (reduce (fn [t k] (pss/conj t [k 0] cmp))
                        (pss/sorted-set* {:comparator cmp :storage (mk-storage)
                                          :branching-factor 8 :diff-buf-size 1}) (range 1024))
        rng (java.util.Random. 42)
        warm (reduce (fn [tree cycle]
                       (let [base (if (zero? (mod cycle 3)) (restore (pss/store tree) (mk-storage)) tree)
                             t (reduce (fn [t j]
                                         (let [k (.nextInt rng 1400)
                                               old (first (pss/slice t [k -1] [k 10000] cmp))]
                                           (case (mod (+ cycle j) 3)
                                             0 (if old (pss/replace t old [k (inc cycle)] cmp-key)
                                                   (pss/conj t [k (inc cycle)] cmp))
                                             1 (if old (pss/replace t old [k (inc cycle)] cmp-key) t)
                                             2 (if old (pss/disj t old cmp) t)))) base (range 32))]
                         (pss/store t)
                         t)) initial (range 4))
        storage (.-_storage ^PersistentSortedSet warm)
        cold (restore (.-_address ^PersistentSortedSet warm) storage)
        walk (fn [tree] (let [seen (atom #{})]
                          (pss/walk-addresses tree (fn [a] (swap! seen conj a) true)) @seen))
        a (walk warm) b (walk cold)
        context (idx/new-mark-context storage)
        shared (set/union (idx/mark-shared warm context) (idx/mark-shared cold context))
        naive (atom #{})]
    (is (= (vec warm) (vec cold)) "The logical contents agree; the difference is address enumeration")
    (is (= 1 (count (set/difference b a))) "Prove that the fixture reaches an omitted durable anchor")
    (doseq [tree [warm cold]]
      (pss/walk-addresses tree (fn [address]
                                 (let [seen? (contains? @naive address)]
                                   (swap! naive conj address) (not seen?)))))
    (is (= 1 (count (set/difference b @naive))) "Naive shared pruning prevents the cold walk recovering it")
    (is (= b (idx/-mark warm)) "Ordinary durable marking repairs the warm-only omission")
    (is (= (set/union a b) shared) "Shared durable marking retains the complete union")
    (is (= (count b) (:expanded @context)))
    (is (pos? (:pruned @context)))))

(defn- with-db [f]
  (let [id (random-uuid)
        cfg {:store {:backend :file :id id :path (str "/tmp/dh-gc-context-" id)}
             :writer {:backend :self :writer-ownership :exclusive}
             :index-config {:branching-factor 16}
             :schema-flexibility :write :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
      (try (f conn) (finally (d/release conn) (d/delete-database cfg))))))

(deftest a-partial-walk-error-aborts-before-sweep
  (with-db
    (fn [conn]
      (d/transact conn (mapv #(hash-map :age %) (range 300)))
      (let [read-edges pset/read-durable-node-edges
            failed (atom false)
            sweep-called (atom false)]
        (with-redefs [pset/read-durable-node-edges
                      (fn [storage address]
                        (if (compare-and-set! failed false true)
                          (do (read-edges storage address)
                              (throw (AssertionError. "partial expansion")))
                          (read-edges storage address)))
                      kgc/sweep! (fn [& _] (reset! sweep-called true) (throw (ex-info "Unexpected sweep" {})))]
          (is (thrown? Exception (<?? S (gc/gc-storage! @conn (Date. 0) {:min-age-ms 0}))))
          (is @failed)
          (is (not @sweep-called)))))))

(deftest overlapping-successful-walks-join-the-complete-closure
  (with-db
    (fn [conn]
      (d/transact conn (mapv #(hash-map :age %) (range 300)))
      (let [tree (:eavt @conn)
            oracle (idx/-mark tree)
            context (idx/new-mark-context (:storage (:store @conn)))
            entered (promise)
            continue (promise)
            first? (atom true)
            read-edges pset/read-durable-node-edges]
        (with-redefs [pset/read-durable-node-edges
                      (fn [storage address]
                        (when (compare-and-set! first? true false)
                          (deliver entered true)
                          (when (= ::timeout (deref continue 10000 ::timeout))
                            (throw (ex-info "Test schedule timed out" {}))))
                        (read-edges storage address))]
          (let [a (future (idx/mark-shared tree context))]
            (try
              (is (= true (deref entered 10000 ::timeout)))
              (let [b (idx/mark-shared tree context)]
                (deliver continue true)
                (is (= oracle (set/union @a b)))
                (is (= (count oracle) (:expanded @context))))
              (finally (deliver continue true)))))))))

(deftest collector-unions-branches-pins-and-store-reference-payloads
  (with-db
    (fn [conn]
      (d/transact conn [{:db/ident :blob :db/valueType :db.type/store-ref
                         :db/cardinality :db.cardinality/one}])
      (let [store (:store @conn)
            blob (random-uuid)]
        (k/assoc store blob :payload {:sync? true})
        (d/transact conn [{:blob blob}])
        (let [old @conn
              id (roots/pin! old {:ttl-ms nil} {:sync? true})
              calls (atom [])
              contexts (atom #{})
              mark idx/mark-shared]
          (v/branch! conn :db :retained)
          (d/transact conn (mapv #(hash-map :age %) (range 300)))
          (with-redefs [idx/mark-shared (fn [tree context]
                                          (swap! contexts conj context)
                                          (swap! calls conj tree)
                                          (mark tree context))]
            (let [deleted (<?? S (gc/gc-storage! @conn (Date.) {:min-age-ms 0}))]
              (is (not (contains? deleted blob)))
              (is (seq @calls))
              (is (= 1 (count @contexts)))
              (is (every? some? @contexts))))
          (v/delete-branch! conn :retained)
          (roots/release! @conn id {:sync? true})
          ;; The current head still names the blob. Retract it, then collect
          ;; history so only the removed branch/pin would have retained it.
          (d/transact conn [[:db/retract (ffirst (d/q '[:find ?e :where [?e :blob _]] @conn)) :blob blob]])
          (is (contains? (<?? S (gc/gc-storage! @conn (Date.) {:min-age-ms 0})) blob)
              "the next collection uses a fresh context and reclaims the removed roots' payload"))))))

(deftest late-root-walks-share-the-context-and-preserve-their-payloads
  (with-db
    (fn [conn]
      (d/transact conn [{:db/ident :blob :db/valueType :db.type/store-ref
                         :db/cardinality :db.cardinality/one}])
      (let [store (:store @conn) blob (random-uuid)]
        (k/assoc store blob :payload {:sync? true})
        (d/transact conn [{:blob blob}])
        (let [record (roots/commit-record @conn {:sync? true})
              eid (ffirst (d/q '[:find ?e :where [?e :blob _]] @conn))
              id (atom nil)
              contexts (atom #{})
              mark idx/mark-shared]
          (d/transact conn [[:db/retract eid :blob blob]])
          (with-redefs [idx/mark-shared
                        (fn [tree context]
                          (swap! contexts conj context)
                          (when-not @id
                            (reset! id (roots/root! @conn {:kind :pin :record record :ttl-ms nil} {:sync? true})))
                          (mark tree context))]
            (is (not (contains? (<?? S (gc/gc-storage! @conn (Date.) {:min-age-ms 0})) blob)))
            (is (some? @id))
            (is (= 1 (count @contexts))))
          (roots/release! @conn @id {:sync? true})
          (is (contains? (<?? S (gc/gc-storage! @conn (Date.) {:min-age-ms 0})) blob)))))))
