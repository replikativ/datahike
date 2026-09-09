(ns datahike.test.backfill-storage-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.backfill.storage :as storage]
            [datahike.datom :as dd]
            [datahike.index.interface :as di]
            [datahike.test.utils :as utils]
            [konserve.core :as k]
            [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.impl.nodes :as nodes])
  (:import [org.replikativ.persistent_sorted_set IStorage ANode]))

(defn- error-type [f]
  (try (f) nil (catch Exception e (:type (ex-data e)))))

(defn- with-db [f]
  (let [conn (utils/setup-db {:crypto-hash? false
                              :index :datahike.index/persistent-set
                              :writer {:writer-ownership :exclusive}})
        config (:config @conn)]
    (try (f conn) (finally (d/release conn) (d/delete-database config)))))

(deftest unsupported-and-invalid-options-have-no-io
  (with-redefs [k/assoc (fn [& _] (throw (AssertionError. "Unexpected write")))
                k/get (fn [& _] (throw (AssertionError. "Unexpected read")))]
    (doseq [config [{:index :datahike.index/hitchhiker-tree :crypto-hash? false}
                    {:index :datahike.index/persistent-set :crypto-hash? true}]]
      (is (= :backfill.storage/unsupported (error-type #(storage/create! {} config)))))
    (doseq [options [false {:unknown 1} {:pending-node-limit 0}
                     {:max-node-weight 100 :pending-weight-limit 99}]]
      (is (= :backfill.storage/invalid-options
             (error-type #(storage/create! {} {:index :datahike.index/persistent-set :crypto-hash? false}
                                           options)))))))

(deftest private-build-roundtrip-and-live-isolation
  (with-db
    (fn [conn]
      (d/transact conn [{:db/ident :score :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/one :db/index true}
                        {:db/id -1 :score 1}])
      (let [db @conn
            live (get-in db [:store :storage])
            old (vec (:avet db))
            before (into {} (map (fn [key] [key @(get live key)]))
                         [:pending-writes :freelist :freed-addresses :freed-set :cache :stats])
            poison (random-uuid)
            _ (swap! (:freelist live) conj poison)
            before (assoc before :freelist @(:freelist live))
            private (storage/create! (:store db) (:config db)
                                     {:pending-node-limit 2 :cache-node-limit 2})]
        (try
          (let [datoms (map #(dd/datom % :score % 42) (range 1 401))
                view (assoc (storage/build-store private) :datahike/branching-factor 8)
                tree (di/init-index-sorted :datahike.index/persistent-set view datoms :avet 0
                                           {:indexed #{:score}})]
            (storage/flush! private)
            (is (= (vec datoms) (vec tree)))
            (is (<= (:cached-nodes (storage/resources private)) 2))
            (is (= 0 (:pending-nodes (storage/resources private))))
            (is (pos? (:persisted (storage/resources private))))
            (is (not (k/exists? (:store db) poison {:sync? true})))
            ;; Every mutable source storage component remains exactly untouched.
            (doseq [[key value] before] (is (= value @(get live key)) (str key)))
            (is (= old (vec (:avet db)))))
          (finally (.close ^java.io.Closeable private)))))))

(deftest replay-keeps-no-private-free-ledger
  (with-db
    (fn [conn]
      (let [private (storage/create! (:store @conn) (:config @conn)
                                     {:pending-node-limit 2 :cache-node-limit 2})
            view (assoc (storage/build-store private) :datahike/branching-factor 8)]
        (try
          (let [base (di/init-index-sorted :datahike.index/persistent-set view
                                           (map #(dd/datom % :score % 42) (range 1 101)) :avet 0
                                           {:indexed #{:score}})]
            (storage/flush! private)
            (loop [tree base i 0]
              (when (< i 100)
                (let [value (dd/datom (+ 1000 i) :score (+ 1000 i) 43)
                      changed (conj tree value)
                      _ (pss/store changed)
                      restored (disj changed value)]
                  (pss/store restored)
                  (is (<= (:pending-nodes (storage/resources private)) 2))
                  (recur restored (inc i)))))
            (storage/flush! private)
            (is (= 100 (count base)))
            (is (nil? (:freed-set private)))
            (is (nil? (:freed-addresses private)))
            (dotimes [_ 10000] (.markFreed ^IStorage private (random-uuid)))
            (is (false? (.isFreed ^IStorage private (random-uuid))))
            (is (<= (:cached-nodes (storage/resources private)) 2)))
          (finally (.close ^java.io.Closeable private)))
        (is (= {:pending-nodes 0 :cached-nodes 0 :pending-weight 0 :cache-weight 0 :closed? true}
               (select-keys (storage/resources private)
                            [:pending-nodes :cached-nodes :pending-weight :cache-weight :closed?])))
        (is (= :backfill.storage/closed (error-type #(storage/flush! private))))))))

(deftest oversized-nodes-and-failed-flush
  (with-db
    (fn [conn]
      (let [private (storage/create! (:store @conn) (:config @conn)
                                     {:max-node-weight 2048 :pending-weight-limit 4096})
            view (assoc (storage/build-store private) :datahike/branching-factor 8)]
        (try
          (is (= :backfill.storage/node-too-large
                 (error-type #(di/init-index-sorted :datahike.index/persistent-set view
                                                    [(dd/datom 1 :score (apply str (repeat 2000 "x")) 42)]
                                                    :avet 0 {:indexed #{:score}}))))
          (is (= 0 (:pending-nodes (storage/resources private))))
          (di/init-index-sorted :datahike.index/persistent-set view
                                [(dd/datom 1 :score 1 42)] :avet 0 {:indexed #{:score}})
          (with-redefs [k/assoc (fn [& _] (throw (ex-info "write failed" {:type :injected})))]
            (is (= :injected (error-type #(storage/flush! private)))))
          (is (:failed? (storage/resources private)))
          (is (= :backfill.storage/failed (error-type #(storage/flush! private))))
          (finally (.close ^java.io.Closeable private)))))))

(defn- leaf [value]
  (nodes/blob->leaf (nodes/reader-context {})
                    {:keys [(dd/datom 1 :score value 42)]
                     :branching-factor 8 :diff-buf-size 0}))

(deftest restored-topology-has-no-mutable-aliases
  (with-db
    (fn [conn]
      (with-open [^java.io.Closeable private (storage/create! (:store @conn) (:config @conn))]
        (let [original ^ANode (leaf 1)
              address (.store ^IStorage private original)
              first-copy ^ANode (.restore ^IStorage private address)
              second-copy ^ANode (.restore ^IStorage private address)]
          (is (not (identical? original first-copy)))
          (is (not (identical? first-copy second-copy)))
          (is (not (identical? (.-_keys first-copy) (.-_keys second-copy))))
          ;; Deliberately mutate topology, not domain values. Neither the pending
          ;; blob nor another restored node may share these mutable arrays.
          (aset (.-_keys original) 0 (dd/datom 1 :score 100 42))
          (aset (.-_keys first-copy) 0 (dd/datom 1 :score 200 42))
          (is (= 1 (:v (aget (.-_keys second-copy) 0))))
          (storage/flush! private)
          (let [third-copy ^ANode (.restore ^IStorage private address)]
            (is (= 1 (:v (aget (.-_keys third-copy) 0))))
            (aset (.-_keys third-copy) 0 (dd/datom 1 :score 300 42)))
          (let [persisted ^ANode (k/get (:store @conn) address nil {:sync? true})]
            (is (= 1 (:v (aget (.-_keys persisted) 0))))))))))

(deftest flushed-roots-reopen-with-ordinary-storage-after-close
  (with-db
    (fn [conn]
      (let [db @conn
            private (storage/create! (:store db) (:config db)
                                     {:pending-node-limit 2 :cache-node-limit 2})
            datoms (mapv #(dd/datom % :score % 42) (range 1 401))
            root (try
                   (let [tree (di/init-index-sorted
                               :datahike.index/persistent-set
                               (assoc (storage/build-store private) :datahike/branching-factor 8)
                               datoms :avet 0 {:indexed #{:score}})
                         root (pss/store tree)]
                     (storage/flush! private)
                     root)
                   (finally (.close ^java.io.Closeable private)))
            before (storage/resources private)
            reopened (pss/restore-by (dd/index-type->cmp-quick :avet false)
                                     root (get-in db [:store :storage]))]
        (is (= datoms (vec reopened)))
        (is (= before (storage/resources private)))
        (is (:closed? before))
        (is (= 0 (:pending-nodes before) (:cached-nodes before)))))))

(defn- assert-budgets [private options]
  (let [resources (storage/resources private)]
    (doseq [[counter limit] [[:pending-nodes :pending-node-limit]
                             [:cached-nodes :cache-node-limit]
                             [:pending-weight :pending-weight-limit]
                             [:cache-weight :cache-weight-limit]]]
      (is (<= 0 (counter resources) (limit options)) (str counter)))))

(deftest count-and-weight-budgets-hold-after-every-operation
  (with-db
    (fn [conn]
      (let [node (leaf (apply str (repeat 200 "x")))
            weight (with-open [^java.io.Closeable probe (storage/create! (:store @conn) (:config @conn))]
                     (.store ^IStorage probe node)
                     (:pending-weight (storage/resources probe)))]
        (doseq [[mode options]
                [[:weight {:max-node-weight weight
                           :pending-node-limit 32 :cache-node-limit 32
                           :pending-weight-limit (inc weight) :cache-weight-limit (inc weight)}]
                 [:count {:max-node-weight weight
                          :pending-node-limit 2 :cache-node-limit 2
                          :pending-weight-limit (* 32 weight) :cache-weight-limit (* 32 weight)}]]]
          (with-open [^java.io.Closeable private (storage/create! (:store @conn) (:config @conn) options)]
            (let [addresses
                  (mapv (fn [_]
                          (let [address (.store ^IStorage private node)]
                            (assert-budgets private options)
                            (.restore ^IStorage private address)
                            (assert-budgets private options)
                            (.accessed ^IStorage private address)
                            (assert-budgets private options)
                            address))
                        (range 12))]
              (is (pos? (:persisted (storage/resources private))) (str mode " triggers flushing"))
              (when (= mode :weight)
                (is (= 1 (:pending-nodes (storage/resources private))))
                (is (= 1 (:cached-nodes (storage/resources private)))))
              (doseq [address addresses]
                (.restore ^IStorage private address)
                (assert-budgets private options))
              (storage/flush! private)
              (assert-budgets private options))))))))

(deftest partial-flush-poisons-build-and-close-clears-buffers
  (with-db
    (fn [conn]
      (let [private (storage/create! (:store @conn) (:config @conn))
            first-address (.store ^IStorage private (leaf 1))
            second-address (.store ^IStorage private (leaf 2))
            original-assoc k/assoc
            calls (atom 0)]
        (try
          (with-redefs [k/assoc (fn [& args]
                                  (if (= 1 (swap! calls inc))
                                    (apply original-assoc args)
                                    (throw (ex-info "second write failed" {:type :injected}))))]
            (is (= :injected (error-type #(storage/flush! private)))))
          (is (= 2 @calls))
          (is (= 1 (:persisted (storage/resources private))))
          (is (k/exists? (:store @conn) first-address {:sync? true}))
          (is (not (k/exists? (:store @conn) second-address {:sync? true})))
          (is (= :backfill.storage/failed
                 (error-type #(.restore ^IStorage private first-address))))
          (is (= :backfill.storage/failed
                 (error-type #(.store ^IStorage private (leaf 3)))))
          (is (= :backfill.storage/failed (error-type #(storage/flush! private))))
          (finally (.close ^java.io.Closeable private)))
        (.close ^java.io.Closeable private)
        (is (= {:closed? true :failed? true :pending-nodes 0 :cached-nodes 0
                :pending-weight 0 :cache-weight 0}
               (select-keys (storage/resources private)
                            [:closed? :failed? :pending-nodes :cached-nodes
                             :pending-weight :cache-weight])))))))
