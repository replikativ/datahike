(ns datahike.test.commit-wait-test
  "Tests for commit-wait-time functionality and GraalVM-safe scheduling."
  (:require [datahike.api :as d]
            #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
               :clj  [clojure.test :as t :refer [is deftest testing]])))

(deftest test-commit-wait-time-delays-commits
  (testing "Non-zero commit-wait-time adds delay between commits"
    (let [wait-time-ms 50
          num-commits 5
          cfg {:store {:backend :memory
                       :id #uuid "c0001000-0000-0000-0000-000000000001"}
               :keep-history? false
               :schema-flexibility :read
               :writer {:backend :self
                        :commit-wait-time wait-time-ms}}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          start-time (System/currentTimeMillis)]

      ;; Do several commits sequentially
      (dotimes [i num-commits]
        (d/transact conn [{:db/id (inc i) :value i}]))

      (let [elapsed (- (System/currentTimeMillis) start-time)
            ;; We expect at least (num-commits - 1) * wait-time-ms delay
            ;; (first commit has no wait before it)
            expected-min-delay (* (dec num-commits) wait-time-ms)]

        ;; Verify data was written correctly
        (is (= num-commits
               (count (d/q '[:find ?e :where [?e :value _]] @conn))))

        ;; Verify timing - should take at least the expected delay
        ;; Allow some tolerance for commit processing time
        (is (>= elapsed expected-min-delay)
            (str "Expected at least " expected-min-delay "ms, got " elapsed "ms")))

      (d/release conn true)
      (d/delete-database cfg))))

(deftest test-zero-commit-wait-time-no-delay
  (testing "Zero commit-wait-time (default) has no artificial delay"
    (let [num-commits 20
          cfg {:store {:backend :memory
                       :id #uuid "c0002000-0000-0000-0000-000000000002"}
               :keep-history? false
               :schema-flexibility :read
               ;; Explicitly set to 0 (same as default)
               :writer {:backend :self
                        :commit-wait-time 0}}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)
          start-time (System/currentTimeMillis)]

      ;; Do many commits rapidly
      (dotimes [i num-commits]
        (d/transact conn [{:db/id (inc i) :value i}]))

      (let [elapsed (- (System/currentTimeMillis) start-time)]

        ;; Verify data was written correctly
        (is (= num-commits
               (count (d/q '[:find ?e :where [?e :value _]] @conn))))

        ;; With zero wait time, 20 in-memory commits should be fast
        ;; If there was a 50ms delay per commit, it would take > 1000ms
        (is (< elapsed 1000)
            (str "Expected fast commits without delay, but took " elapsed "ms")))

      (d/release conn true)
      (d/delete-database cfg))))

(deftest test-rapid-commits-with-wait-time
  (testing "Rapid parallel commits work correctly with commit-wait-time"
    (let [wait-time-ms 20
          num-commits 50
          cfg {:store {:backend :memory
                       :id #uuid "c0003000-0000-0000-0000-000000000003"}
               :keep-history? false
               :schema-flexibility :read
               :writer {:backend :self
                        :commit-wait-time wait-time-ms}}
          _ (d/delete-database cfg)
          _ (d/create-database cfg)
          conn (d/connect cfg)]

      ;; Fire off many transactions using transact! (async)
      (let [futures (doall
                      (for [i (range num-commits)]
                        (d/transact! conn {:tx-data [{:db/id (+ 1000 i) :value i}]})))]

        ;; Wait for all to complete
        (doseq [f futures]
          @f))

      ;; Verify all data was written correctly
      (is (= num-commits
             (count (d/q '[:find ?e :where [?e :value _]] @conn))))

      (d/release conn true)
      (d/delete-database cfg))))

