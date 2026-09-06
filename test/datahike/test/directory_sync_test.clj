(ns datahike.test.directory-sync-test
  "A storage durability barrier failure must never become a successful tx-report."
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [konserve.filestore])
  (:import [java.nio.file Files AccessDeniedException]
           [java.io IOException]
           [java.util UUID]))

(defn- bounded-result [f]
  (let [task (future (try (f) (catch Throwable e e)))
        result (deref task 15000 ::timeout)]
    (when (= ::timeout result) (future-cancel task))
    result))

(deftest directory-sync-failure-fails-the-transaction
  (doseq [failure [(AccessDeniedException. "injected directory-open failure")
                   (IOException. "injected directory-force failure")]]
    (let [parent (Files/createTempDirectory "datahike-directory-sync-"
                                            (make-array java.nio.file.attribute.FileAttribute 0))
          path (str (.resolve parent "store"))
          cfg {:store {:backend :file :path path :id (UUID/randomUUID)}
               :schema-flexibility :read :keep-history? false}
          connection (atom nil)]
      (try
        (d/create-database cfg)
        (let [conn (d/connect cfg)
              _ (reset! connection conn)
              _ (d/transact conn [{:db/id 1 :n 1}])
              before @conn
              calls (atom 0)
              result (with-redefs-fn
                       {(ns-resolve 'konserve.filestore 'sync-base)
                        (fn [& _] (swap! calls inc) (throw failure))}
                       #(bounded-result (fn [] (d/transact conn [{:db/id 2 :n 2}]))))]
          (is (pos? @calls) "The real file-store directory barrier was reached")
          (is (instance? Throwable result) "No successful tx-report or hung transaction")
          (when (instance? Throwable result)
            (is (some #(identical? failure %)
                      (take-while some? (iterate ex-cause result)))
                "The caller receives the storage failure, possibly wrapped"))
          (is (= (:meta before) (:meta @conn)) "Failed commit is not published to the connection")
          ;; No assertion of disk rollback: a failed persistence barrier can
          ;; occur after replacement. Recovery must inspect actual stored state.
          (is (instance? Throwable
                         (bounded-result #(d/transact conn [{:db/id 3 :n 3}])))))
        (finally
          (when-let [conn @connection]
            (try (d/release conn) (catch Exception _)))
          (d/delete-database cfg)
          (Files/deleteIfExists parent))))))
