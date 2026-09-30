(ns datahike.test.gc-mark-context-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.gc-roots :as roots]
            [datahike.index.interface :as idx]
            [datahike.versioning :as v]
            [konserve.core :as k]
            [konserve.gc :as kgc]
            [org.replikativ.persistent-sorted-set :as pss]
            [superv.async :refer [<?? S]])
  (:import [java.util Date]))

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
      (let [walk pss/walk-addresses
            failed (atom false)
            sweep-called (atom false)]
        (with-redefs [pss/walk-addresses
                      (fn [tree visitor]
                        (if (compare-and-set! failed false true)
                          (walk tree (fn [address]
                                       (visitor address)
                                       (throw (AssertionError. "partial expansion"))))
                          (walk tree visitor)))
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
            walk pss/walk-addresses]
        (with-redefs [pss/walk-addresses
                      (fn [t visitor]
                        (walk t (fn [address]
                                  (let [expand? (visitor address)]
                                    (when (compare-and-set! first? true false)
                                      (deliver entered true)
                                      (when (= ::timeout (deref continue 10000 ::timeout))
                                        (throw (ex-info "Test schedule timed out" {}))))
                                    expand?))))]
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
