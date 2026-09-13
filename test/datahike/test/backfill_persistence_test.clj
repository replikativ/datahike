(ns datahike.test.backfill-persistence-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.test.utils :as utils]
            [datahike.writing :as writing]
            [konserve.core :as k]
            [konserve.gc :as kgc]
            [superv.async :refer [<?? S]]))

(deftest only-durable-avet-state-survives-roundtrip
  (let [conn (utils/setup-db {:index :datahike.index/persistent-set
                              :writer {:writer-ownership :exclusive}})
        config (:config @conn)]
    (try
      (let [job {:id (random-uuid) :status :building :attrs #{:score}
                 :patch {:score {:db/index true}}}
            result {:id (random-uuid) :status :failed :reason :schema-changed}
            database (assoc @conn :avet-build job :avet-build-result result
                            :avet-build-journal {:path "not-durable"}
                            :avet-build-effects [[:current :insert :not-durable nil]]
                            :avet-build-invalidated? true)
            stored (second (writing/db->stored database false))
            restored (writing/stored->db stored (:store @conn))]
        (is (= job (:avet-build stored) (:avet-build restored)))
        (is (= result (:avet-build-result stored) (:avet-build-result restored)))
        (doseq [key [:avet-build-journal :avet-build-effects :avet-build-invalidated?]]
          (is (not (contains? stored key)))
          (is (not (contains? restored key))))
        ;; Legacy heads have no CID; durable build metadata must still change
        ;; their materialization identity, without retaining old local cursors.
        (let [legacy (update stored :meta dissoc :datahike/commit-id)
              old (assoc (writing/stored->db legacy (:store @conn))
                         :avet-build-journal {:path "old-local"})
              moved (writing/reload-head old (dissoc legacy :avet-build) (:store @conn))]
          (is (nil? (:avet-build moved)))
          (is (nil? (:avet-build-journal moved)))))
      (finally (d/release conn) (d/delete-database config)))))

(deftest only-retained-building-seed-defers-collection
  (let [conn (utils/setup-db {:index :datahike.index/persistent-set
                              :writer {:writer-ownership :exclusive}})
        config (:config @conn)
        store (:store @conn)]
    (try
      (let [job {:id (random-uuid) :status :building :attrs #{:score}}
            initial (k/get store :db nil {:sync? true})
            ancestor (random-uuid)
            walk (fn [] (#'gc/reachable-in-branch store :db (java.util.Date. 0)
                                                  config (atom {}) {:sync? true}))
            sweeps (atom 0)]
        (k/assoc store :db (assoc initial :avet-build job) {:sync? true})
        (is (:building-secondary? (walk)))
        (with-redefs [kgc/sweep! (fn [& _]
                                   (swap! sweeps inc)
                                   (throw (AssertionError. "Building head must defer sweep")))]
          (is (= #{} (<?? S (gc/gc-storage! @conn (java.util.Date. 0) {:min-age-ms 0})))))
        (is (zero? @sweeps))
        ;; Keep the build marker in reachable history, but remove it from head.
        (k/assoc store ancestor (assoc initial :avet-build job) {:sync? true})
        (k/assoc store :db (assoc-in initial [:meta :datahike/parents] #{ancestor})
                 {:sync? true})
        (is (false? (boolean (:building-secondary? (walk)))))
        (let [sweep kgc/sweep!]
          (with-redefs [kgc/sweep! (fn [& args]
                                     (swap! sweeps inc)
                                     (apply sweep args))]
            (<?? S (gc/gc-storage! @conn (java.util.Date. 0) {:min-age-ms 0})))
          (is (pos? @sweeps))))
      (finally (d/release conn) (d/delete-database config)))))
