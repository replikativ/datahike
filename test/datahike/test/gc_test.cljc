(ns datahike.test.gc-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [clojure.set :as set]
   [superv.async :refer [<?? S]]
   [datahike.api :as d]
   [datahike.index.interface :refer [-mark]]
   [datahike.versioning :refer [branch! delete-branch! merge!
                                branch-history]]
   [konserve.core :as k]
   [datahike.gc :as gc]
   [datahike.test.core-test])
  (:import [java.util Date]))

#?(:cljs (def Throwable js/Error))

(defn- count-store [db]
  (count (k/keys (:store db) {:sync? true})))

(def count-query '[:find (count ?e) .
                   :where
                   [?e :age _]])

(def txs (vec (for [i (range 1000)] {:age i})))

(def cfg {:store              {:backend :file
                               :path "/tmp/gc-test"
                               :id #uuid "9c000000-0000-0000-0000-000000000001"}
          :keep-history?      true
          :schema-flexibility :write
          :index              :datahike.index/persistent-set})

(def schema [{:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])

(deftest datahike-gc-test
  (let [cfg (assoc-in cfg [:store :path] "/tmp/dh-gc-test")
        conn (do
               (d/delete-database cfg)
               (d/create-database cfg)
               (d/connect cfg))
          ;; everything will fit into the root nodes of each index here
        num-roots 3
        fresh-count (+ num-roots 4) ;; :branches + :db + cid + roots + schema-meta
        history-count 3]
    (testing "Test initial store counts."
      (is (= 1 (count (-mark (:eavt @conn)))))
      (is (= fresh-count (count-store @conn)))
      (d/transact conn schema)
      (is (= 1 (count (-mark (:eavt @conn)))))
      (is (= (+ 2 history-count fresh-count num-roots) (count-store @conn))))
    (testing "Delete old db with roots."
      (is (= (+ num-roots 2) (count (<?? S (d/gc-storage conn (Date.))))))
      (is (= (+ history-count fresh-count) (count-store @conn))))
    (testing "Try to run on dirty index and fail."
      (is (thrown-with-msg? Throwable #"Index needs to be properly flushed before marking."
                            (-mark (:eavt
                                    (:db-after
                                     (d/with @conn [{:db/id 100
                                                     :age   5}])))))))

    (testing "Check that we can still read the data."
      (let [new-conn (d/connect cfg)]
        (d/transact conn txs)
        (<?? S (d/gc-storage conn (Date.)))
        (is (= 1000 (d/q count-query @new-conn)))
        (d/release new-conn)))
    (d/release conn)))

(deftest datahike-gc-versioning-test
  (let [cfg          (assoc-in cfg [:store :path] "/tmp/dh-gc-versioning-test")
        conn         (do
                       (d/delete-database cfg)
                       (d/create-database cfg)
                       (d/connect cfg))
        _            (d/transact conn schema)
        ;; create two more branches
        _            (branch! conn :db :branch1)
        cfg1         (assoc cfg :branch :branch1)
        conn-branch1 (d/connect cfg1)
        _            (branch! conn :db :branch2)
        cfg2         (assoc cfg :branch :branch2)
        conn-branch2 (d/connect cfg2)]
    (testing "Check branches."
      (d/transact conn-branch1 txs)
      (d/transact conn-branch2 txs)
      (<?? S (d/gc-storage conn (Date.)))
      (is (nil? (d/q count-query @conn)))
      (is (= 1000 (d/q count-query @conn-branch1)))
      (is (= 1000 (d/q count-query @conn-branch2)))
      (delete-branch! conn :branch2)
      (<?? S (d/gc-storage conn (Date.))))

    (d/release conn)
    (d/release conn-branch1)
    (d/release conn-branch2)

    (testing "Removed branch and after gc check."
      (let [cfg          (assoc-in cfg [:store :path] "/tmp/dh-gc-versioning-test")
            conn         (d/connect cfg)
            ;; create two more branches
            cfg1         (assoc cfg :branch :branch1)
            conn-branch1 (d/connect cfg1)
            cfg2         (assoc cfg :branch :branch2)]
        (is (nil? (d/q count-query @conn)))
        (is (= 1000 (d/q count-query @conn-branch1)))
        (is (thrown-with-msg? Throwable #"Database does not exist."
                              (d/connect cfg2)))
        (d/release conn)
        (d/release conn-branch1)))))

(deftest datahike-gc-range-test
  (let [cfg           (assoc-in cfg [:store :path] "/tmp/dh-gc-range-test")
        conn          (do
                        (d/delete-database cfg)
                        (d/create-database cfg)
                        (d/connect cfg))
        _             (d/transact conn schema)
        ;; create a branch
        _             (branch! conn :db :branch1)
        conn-branch1  (d/connect (assoc cfg :branch :branch1))
        ;; transact on each
        _             (d/transact conn txs)
        _             (d/transact conn-branch1 txs)
        ;; record before-date for gc
        _             (Thread/sleep 100)
        remove-before (Date.)]
    (Thread/sleep 100)
    ;; transact
    (d/transact conn [{:age 42}])
    (d/transact conn-branch1 [{:age 42}])
    ;; transact again
    (d/transact conn [{:age 42}])
    (d/transact conn-branch1 [{:age 42}])
    ;; merge back
    (merge! conn #{:branch1} [])
    (let [db-history       (<?? S (branch-history conn))
          branch1-history  (<?? S (branch-history conn-branch1))
          _ (delete-branch! conn :branch1)
          _ (testing "Check branch counts"
              (is (= 9 (count db-history)))
              (is (= 5 (count branch1-history)))
              (is (= 9 (count (set/union (set db-history) (set branch1-history))))))
          new-history      (set (filter (fn [db]
                                          (let [db-date ^Date (or (get-in db [:meta :datahike/updated-at])
                                                                  (get-in db [:meta :datahike/created-at]))]
                                            (> (.getTime db-date)
                                               (.getTime remove-before))))
                                        (concat db-history branch1-history)))
          ;; gc
          _                (<?? S (d/gc-storage conn remove-before))
          history-after-gc (set (<?? S (branch-history conn)))]
      (testing "Check that newer db roots are still there and counts after gc."
        (is (set/subset? new-history history-after-gc))
        (is (= 5 (count new-history)))
        (is (= 7 (count history-after-gc)))))
    (d/release conn)
    (d/release conn-branch1)))

;; ---------------------------------------------------------------------------
;; Explicit GC roots
;;
;; datahike's native liveness is reachability AND recency: `reachable-in-branch`
;; follows ancestry only while a record is newer than `remove-before`. That is
;; correct when a branch head is the only pointer, but a consumer can have
;; pointers datahike cannot see — geschichte stores git refs as ordinary datoms,
;; so every one of its commits looks like unreferenced old history and a cutoff
;; GC silently deletes the snapshot the ref names.
;;
;; A root restores git's rule for such a consumer: referenced means live,
;; independent of age.
;; ---------------------------------------------------------------------------

(deftest test-gc-roots
  (let [id   (random-uuid)
        cfg  {:store {:backend :file :path (str "/tmp/gc-roots-" id) :id id}
              :schema-flexibility :write :keep-history? true :commit-graph? true
              :index :datahike.index/persistent-set}
        _    (d/delete-database cfg)
        _    (d/create-database cfg)
        conn (d/connect cfg)]
    (d/transact conn [{:db/ident       :note/text
                       :db/valueType   :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn [{:note/text "oldest"}])
    (let [old-cid (:datahike/commit-id (:meta @conn))
          store   (:store @conn)
          resolves? (fn [cid] (try (some? (d/commit-as-db conn cid))
                                   (catch Throwable _ false)))]
      ;; bury it under later history so a cutoff would otherwise reach it
      (dotimes [i 5] (d/transact conn [{:note/text (str "later " i)}]))
      (is (resolves? old-cid) "baseline: the old commit resolves before any GC")

      (testing "declaring a root"
        (is (= #{old-cid} (<?? S (gc/gc-root! store old-cid))))
        (is (= #{old-cid} (<?? S (gc/gc-roots store))))
        (is (= #{old-cid} (<?? S (gc/gc-root! store old-cid))) "idempotent"))

      (testing "a root survives a cutoff that would otherwise collect it"
        (<?? S (d/gc-storage conn (Date.)))
        (is (resolves? old-cid)
            "the rooted commit must still resolve after a full-cutoff GC")
        (is (some? (d/q '[:find ?t . :where [_ :note/text ?t]] @conn))
            "ordinary operation is unaffected"))

      (testing "unrooting releases it — the control for the assertion above"
        (is (= #{} (<?? S (gc/gc-unroot! store old-cid))))
        (<?? S (d/gc-storage conn (Date.)))
        (is (not (resolves? old-cid))
            "without the root the same GC collects it, so the root is what protected it")))))

(deftest test-gc-root-rejects-unresolvable-commit
  (let [id   (random-uuid)
        cfg  {:store {:backend :file :path (str "/tmp/gc-roots-" id) :id id}
              :schema-flexibility :write :keep-history? true :commit-graph? true
              :index :datahike.index/persistent-set}
        _    (d/delete-database cfg)
        _    (d/create-database cfg)
        conn (d/connect cfg)]
    ;; A root naming a missing commit retains nothing. Failing at declaration
    ;; keeps that from surfacing later as a store that already lost the history
    ;; the root was meant to protect.
    (is (thrown? Throwable (<?? S (gc/gc-root! (:store @conn) (random-uuid)))))))
