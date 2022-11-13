(ns datahike.test.gc-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [clojure.set :as set]
   [superv.async :refer [<?? S]]
   [datahike.api :as d]
   [datahike.experimental.gc :refer [gc!]]
   [datahike.experimental.versioning :refer [branch! delete-branch! merge!
                                             branch-history]]
   [datahike.index.persistent-set :refer [mark]]
   [konserve.core :as k]
   [datahike.test.core-test])
  (:import [java.util Date]))

(defn- count-store [db]
  (count (k/keys (:store db) {:sync? true})))

(def count-query '[:find (count ?e) .
                   :where
                   [?e :age _]])

(def txs (vec (for [i (range 1000)] {:age i})))

(def cfg {:store              {:backend :file}
          :keep-history?      true
          :schema-flexibility :write
          :index              :datahike.index/persistent-set})

(def schema [{:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])

(deftest datahike-gc-test
  (testing "Test core garbage collector functionality."
    (let [cfg (assoc-in cfg [:store :path] "/tmp/dh-gc-test")
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
          ;; everything will fit into the root nodes of each index here
          num-roots 6
          fresh-count (+ num-roots 3) ;; :branches + :db + cid + roots
          ]
      (is (= 1 (count (mark (:eavt @conn)))))
      (is (= fresh-count (count-store @conn)))
      (d/transact conn schema)
      (is (= 1 (count (mark (:eavt @conn)))))
      (is (= (+ 1 fresh-count num-roots) (count-store @conn)))
      ;; delete old db with roots
      (is (= (+ 1 num-roots) (count (<?? S (gc! @conn (Date.))))))
      (is (= fresh-count (count-store @conn)))

      ;; try to run on dirty index
      (is (thrown-msg? "Index needs to be properly flushed before marking."
                       (mark (:eavt
                              (:db-after
                               (d/with @conn [{:db/id 100
                                               :age   5}]))))))

      ;; check that we can still read the data
      (d/transact conn txs)
      (<?? S (gc! @conn (Date.)))
      (is (= 1000 (d/q count-query @(d/connect cfg)))))))

(deftest datahike-gc-versioning-test
  (testing "Testing branch erasure under gc."
    (let [cfg (assoc-in cfg [:store :path] "/tmp/dh-gc-versioning-test")
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
          _ (d/transact conn schema)
          ;; create two more branches
          _ (branch! conn :db :branch1)
          cfg1 (assoc cfg :branch :branch1)
          conn-branch1 (d/connect cfg1)
          _ (branch! conn :db :branch2)
          cfg2 (assoc cfg :branch :branch2)
          conn-branch2 (d/connect cfg2)]
      ;; do gc & check
      (d/transact conn-branch1 txs)
      (d/transact conn-branch2 txs)
      (<?? S (gc! @conn (Date.)))
      (is (nil? (d/q count-query @(d/connect cfg))))
      (is (= 1000 (d/q count-query @(d/connect cfg1))))
      (is (= 1000 (d/q count-query @(d/connect cfg2))))
      ;; remove branch
      (delete-branch! conn :branch2)
      ;; do gc & check
      (<?? S (gc! @conn (Date.)))
      (is (nil? (d/q count-query @(d/connect cfg))))
      (is (= 1000 (d/q count-query @(d/connect cfg1))))
      (is (thrown-msg? "Database does not exist."
                       (d/q count-query @(d/connect cfg2)))))))

(deftest datahike-gc-range-test
  (testing "Testing temporal range behaviour of gc."
    (let [cfg (assoc-in cfg [:store :path] "/tmp/dh-gc-range-test")
          conn (do
                 (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
          _ (d/transact conn schema)
          ;; create a branch
          _ (branch! conn :db :branch1)
          conn-branch1 (d/connect (assoc cfg :branch :branch1))
          ;; transact on each
          _ (d/transact conn txs)
          _ (d/transact conn-branch1 txs)
          ;; record before-date for gc
          _ (Thread/sleep 100)
          remove-before (Date.)]
      (Thread/sleep 100)
      ;; transact
      (d/transact conn [{:age 42}])
      (d/transact conn-branch1 [{:age 42}])
      ;; transact again
      (d/transact conn [{:age 42}])
      (d/transact conn-branch1 [{:age 42}])
      ;; merge back
      _ (merge! conn #{:branch1} [])
      _ (delete-branch! conn :branch1)
      (let [db-history (<?? S (branch-history conn))
            branch1-history (<?? S (branch-history conn-branch1))
            _ (is (= 9 (count db-history)))
            _ (is (= 5 (count branch1-history)))
            _ (is (= 9 (count (set/union (set db-history) (set branch1-history)))))
            new-history (set (filter (fn [db]
                                       (let [db-date ^Date (or (get-in db [:meta :datahike/updated-at])
                                                               (get-in db [:meta :datahike/created-at]))]
                                         (> (.getTime db-date)
                                            (.getTime remove-before))))
                                     (concat db-history branch1-history)))
            ;; gc
            _ (<?? S (gc! @conn remove-before))
            history-after-gc (set (<?? S (branch-history conn)))]
        ;; check that newer db roots are still there
        (is (set/subset? new-history history-after-gc))
        (is (= 5 (count new-history)))
        (is (= 7 (count history-after-gc)))))))
