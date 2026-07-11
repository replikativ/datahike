(ns datahike.test.writer-error-test
  "Fatal Errors (AssertionError, OOM, ...) thrown inside the async commit
   pipeline must fail transacts LOUDLY, not hang them: go-try- catches
   Exception only, so an escaping Error used to kill the dispatch thread and
   leave the writer's commit loop parked forever on a silent channel — every
   queued transact hung. commit! now converts Errors to ex-info at the go
   boundary, so callbacks receive the error and the writer shuts down."
  (:require [datahike.api :as d]
            [datahike.writing :as dw]
            [clojure.test :refer [deftest is testing]]))

(deftest fatal-error-in-commit-fails-loudly
  (testing "a fatal Error during commit propagates to the caller within a bounded time"
    (let [cfg {:store {:backend :file
                       :path (str (System/getProperty "java.io.tmpdir") "/dh-fatal-commit")
                       :id #uuid "d1ffb000-0000-0000-0000-00000000fa7a"}
               :schema-flexibility :read :keep-history? false}]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/id 1 :n 1}])
        (let [orig dw/db->stored
              result (with-redefs [dw/db->stored (fn [& _] (throw (AssertionError. "synthetic fatal error")))]
                       (let [f (future (try (d/transact conn [{:db/id 2 :n 2}]) :no-error
                                            (catch Exception _ :failed-loudly)
                                            (catch AssertionError _ :failed-loudly)))]
                         (deref f 15000 :HUNG)))]
          (is (= :failed-loudly result)
              "transact must complete exceptionally, not hang")
          ;; writer shut down; the connection refuses further writes explicitly
          (is (thrown? Exception (d/transact conn [{:db/id 3 :n 3}]))
              "subsequent transacts on the dead writer fail loudly")
          ;; durable state is the last good commit; a fresh connection works
          (try (d/release conn) (catch Exception _))
          (let [conn2 (d/connect cfg)]
            (is (= 1 (d/q '[:find (count ?e) . :where [?e :n _]] @conn2))
                "store intact at the last successful commit")
            (d/transact conn2 [{:db/id 2 :n 2}])
            (is (= 2 (d/q '[:find (count ?e) . :where [?e :n _]] @conn2))
                "fresh connection transacts normally")
            (d/release conn2))
          (is (fn? orig))))
      (d/delete-database cfg))))

(deftest fatal-error-in-commit-loop-fails-loudly
  (testing "an Error thrown on the COMMIT thread (create-commit-id — only commit! calls it)
            reaches the caller instead of parking the writer forever"
    (let [cfg {:store {:backend :file
                       :path (str (System/getProperty "java.io.tmpdir") "/dh-fatal-commit2")
                       :id #uuid "d1ffb000-0000-0000-0000-00000000fa7b"}
               :schema-flexibility :read :keep-history? false}]
      (when (d/database-exists? cfg) (d/delete-database cfg))
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/id 1 :n 1}])
        (let [result (with-redefs [dw/create-commit-id (fn [& _] (throw (AssertionError. "synthetic commit-thread error")))]
                       (let [f (future (try (d/transact conn [{:db/id 2 :n 2}]) :no-error
                                            (catch Exception _ :failed-loudly)
                                            (catch AssertionError _ :failed-loudly)))]
                         (deref f 15000 :HUNG)))]
          (is (= :failed-loudly result) "commit-thread Error must not hang the transact"))
        (try (d/release conn) (catch Exception _)))
      (d/delete-database cfg))))
