(ns datahike.test.gc-schedule-test
  "Deterministic schedules for the boundary between a complete mark and sweep.
   The bounded-mode tests document a counterexample, not a safety guarantee."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.gc :as gc]
            [datahike.gc-roots :as roots]
            [konserve.core :as k]
            [konserve.gc :as kgc]
            [superv.async :refer [<?? S]])
  (:import [java.util Date]))

(defn- with-db [f]
  (let [id (random-uuid)
        cfg {:store {:backend :file :id id :path (str "/tmp/dh-gc-schedule-" id)}
             :writer {:backend :self :writer-ownership :exclusive}
             :schema-flexibility :write
             :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try (f conn)
           (finally (d/release conn) (d/delete-database cfg))))))

(deftest collection-modes-fail-closed
  (is (= :bounded (gc/collection-mode nil)))
  (is (= :bounded (gc/collection-mode {:mode :bounded})))
  (is (= :datahike/gc-invalid-mode
         (try (gc/collection-mode {:mode :surprise})
              (catch Exception e (:type (ex-data e))))))
  (is (= :datahike/gc-coordination-unavailable
         (try (gc/collection-mode {:mode :coordinated})
              (catch Exception e (:type (ex-data e))))))
  (with-redefs [k/get (fn [& _] (throw (ex-info "Unexpected IO" {})))]
    (is (= :datahike/gc-coordination-unavailable
           (try (<?? S (gc/gc-storage! {} (Date. 0) {:mode :coordinated}))
                (catch Exception e (:type (ex-data e)))))
        "an unsupported mode is rejected before any store access")))

(deftest bounded-sweep-can-delete-old-objects-promoted-after-mark
  (with-db
    (fn [conn]
      (let [db (d/db conn)
            store (:store db)
            old-key [:schedule (random-uuid)]
            new-key [:schedule (random-uuid)]
            root-id (atom nil)
            sweep kgc/sweep!]
        (k/assoc store old-key :old {:sync? true})
        ;; Interpose exactly after the collector's final root read. The
        ;; collector can be thought of as frozen here while another publisher
        ;; runs. No sleeps, scheduler timing or timestamp forging are needed.
        (with-redefs [kgc/sweep!
                      (fn [s whitelist cutoff & args]
                        (is (not (contains? whitelist old-key)))
                        (k/assoc s new-key :new {:sync? true})
                        (reset! root-id
                                (roots/root! db {:kind :pin
                                                 :record {:datahike.gc/keys #{old-key new-key}}}
                                             {:sync? true}))
                        (apply sweep s whitelist cutoff (if (seq args) args [1000 {}])))]
          (let [deleted (<?? S (gc/gc-storage! db (Date. 0) {:mode :bounded :min-age-ms 0}))]
            (is (contains? deleted old-key)
                "the existing bounded contract permits this counterexample")
            (is (not (contains? deleted new-key))
                "objects written after the cutoff are spared")
            (is (= :new (k/get store new-key nil {:sync? true})))
            (is (nil? (k/get store old-key nil {:sync? true})))
            (is (contains? (roots/roots store {:sync? true}) @root-id)
                "the newly published root exists but names a deleted object")))))))

(deftest roots-visible-before-mark-preserve-old-objects
  (with-db
    (fn [conn]
      (let [db (d/db conn)
            store (:store db)
            key [:schedule (random-uuid)]]
        (k/assoc store key :retained {:sync? true})
        (let [id (roots/root! db {:kind :pin :record {:datahike.gc/keys #{key}}}
                              {:sync? true})]
          (is (not (contains? (<?? S (gc/gc-storage! db (Date. 0) {:min-age-ms 0})) key)))
          (is (= :retained (k/get store key nil {:sync? true})))
          (roots/release! db id {:sync? true})
          (is (contains? (<?? S (gc/gc-storage! db (Date. 0) {:min-age-ms 0})) key)
              "releasing the root makes the same old object collectable"))))))

(deftest public-api-forwards-the-collection-mode
  (with-db
    (fn [conn]
      (is (= :datahike/gc-coordination-unavailable
             (try (<?? S (d/gc-storage conn (Date. 0) {:mode :coordinated}))
                  (catch Exception e (:type (ex-data e)))))))))
