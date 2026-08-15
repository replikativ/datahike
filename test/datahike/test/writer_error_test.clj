(ns datahike.test.writer-error-test
  "Fatal Errors (AssertionError, OOM, ...) thrown inside the async commit
   pipeline must fail transacts LOUDLY, not hang them: go-try- catches
   Exception only, so an escaping Error used to kill the dispatch thread and
   leave the writer's commit loop parked forever on a silent channel — every
   queued transact hung. commit! now converts Errors to ex-info at the go
   boundary, so callbacks receive the error and the writer shuts down."
  (:require [datahike.api :as d]
            [datahike.writing :as dw]
            [datahike.writer :as dwriter]
            [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]))

(deftest fatal-error-in-commit-fails-loudly
  (testing "a fatal Error during commit propagates to the caller within a bounded time"
    (let [cfg {:store {:backend :file
                       :path (str (System/getProperty "java.io.tmpdir") "/dh-fatal-commit-" (java.util.UUID/randomUUID))
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
                       :path (str (System/getProperty "java.io.tmpdir") "/dh-fatal-commit2-" (java.util.UUID/randomUUID))
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

(deftest an-unknown-op-names-itself-instead-of-blaming-the-connection
  (testing "`write-fn-map` is a plain map, so an op it does not hold gave nil
            and `(apply nil …)` threw a NullPointerException — which the error
            handler then REWROTE as \"connection may have been invalidated, e.g.
            through db deletion\". A caller whose only fault was naming an
            operation this writer does not have was sent to look at their
            storage. That is the version-skew case: a newer client against an
            older remote writer, and there is no version exchange on the wire to
            catch it earlier."
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :schema-flexibility :read :keep-history? false}
          _ (d/create-database cfg)
          conn (d/connect cfg)
          w (:writer @(:wrapped-atom conn))
          res (<!! (dwriter/dispatch! w {:op 'an-op-from-a-newer-client :args [[] {}]}))]

      (is (instance? clojure.lang.ExceptionInfo res))
      (is (= :writer/unknown-op (:type (ex-data res))))
      (is (= 'an-op-from-a-newer-client (:op (ex-data res))))

      (testing "the message names the missing op and what does exist, so version
                skew is visible rather than inferred"
        (is (re-find #"no operation `an-op-from-a-newer-client`" (ex-message res)))
        (is (re-find #"transact!" (ex-message res)))
        (is (not (re-find #"invalidated" (ex-message res)))
            "and it must not claim the connection was invalidated"))

      (testing "the writer SURVIVES — an unknown op is a caller error, not a
                fatal one, and killing the writer would take every other
                connection holder down with it"
        (is (map? (d/transact conn [{:n 1}]))))

      (d/release conn)
      (d/delete-database cfg))))
