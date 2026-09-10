(ns datahike.test.backfill-online-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as async]
            [datahike.api :as d]
            [datahike.api.impl :as api-impl]
            [datahike.backfill.avet :as avet]
            [datahike.connections :as connections]
            [datahike.gc-roots :as roots]
            [datahike.test.utils :as utils]
            [datahike.tx-preds :as predicates]
            [datahike.writer :as writer]
            [datahike.writing :as writing]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]))

(defn- invoke! [conn op & args]
  (if (contains? #{'begin-avet-build! 'cancel-avet-build!} op)
    (let [result (deref (apply (if (= op 'begin-avet-build!) d/begin-avet-build! d/cancel-avet-build!)
                               conn args) 10000 ::timeout)]
      (when (= ::timeout result) (throw (ex-info "Public AVET operation timed out" {:op op})))
      result)
    (let [channel (writer/dispatch! (:writer @conn) {:op op :args (vec args)})
          [result selected] (async/alts!! [channel (async/timeout 10000)])]
      (when-not (= selected channel)
        (throw (ex-info "Writer operation timed out" {:op op})))
      (when (instance? Throwable result) (throw result))
      result)))

(defn- await! [f]
  (let [deadline (+ (System/nanoTime) 10000000000)]
    (loop []
      (if-let [value (f)] value
              (if (< (System/nanoTime) deadline)
                (do (Thread/sleep 10) (recur))
                (throw (ex-info "Background AVET condition timed out" {})))))))

(defn- with-db
  ([f] (with-db {} f))
  ([options f]
   (let [conn (utils/setup-db (merge {:crypto-hash? false :schema-flexibility :write
                                      :keep-history? true :index :datahike.index/persistent-set
                                      :writer {:backend :self :writer-ownership :shared
                                               :require-fencing :process :max-batch 8}} options))
         config (:config @conn)]
     (try
       (d/transact conn [{:db/ident :score :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}
                         {:db/id 10000 :score 1} {:db/id 10001 :score 2}])
       (f conn)
       (finally (d/release conn) (d/delete-database config))))))

(defn- ready? [conn]
  (= :ready (:status (d/avet-build-status @conn))))

(defn- settled! [conn]
  (let [coordinator (:avet-coordinator (:writer @conn))]
    (await! #(empty? (:jobs @(:state coordinator))))
    (is (empty? (<?? S (roots/roots (:store @conn)))))))

(deftest public-api-refuses-nonlocal-writers-before-dispatch
  (let [calls (atom 0)
        remote (reify writer/PWriter
                 (-dispatch! [_ _] (swap! calls inc) (async/promise-chan))
                 (-shutdown [_] (async/promise-chan))
                 (-streaming? [_] false))
        conn {:wrapped-atom (atom {:writer remote :config {:writer {:backend :self}}})}]
    (doseq [operation [#(api-impl/begin-avet-build! conn {:score {:db/index true}})
                       #(api-impl/cancel-avet-build! conn (random-uuid))]]
      (is (= :avet-build-unsupported-writer
             (try (operation) nil (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))
    (is (zero? @calls))))

(deftest online-build-catches-concurrent-writes-and-keeps-old-schema-until-ready
  (with-db
    (fn [conn]
      (let [entered (promise) proceed (promise) build avet/build!]
        (with-redefs [avet/build! (fn [& args]
                                    (deliver entered true)
                                    (when (= ::timeout (deref proceed 10000 ::timeout))
                                      (throw (ex-info "Test scan gate timed out" {})))
                                    (apply build args))]
          (try
            (let [report (invoke! conn 'begin-avet-build! {:score {:db/unique :db.unique/value}})
                  id (get-in report [:db-after :avet-build :id])]
              (is (true? (deref entered 10000 false)))
              (is (nil? (get-in @conn [:schema :score :db/unique])))
              (d/transact conn [[:db/add 10000 :score 3] [:db/retract 10001 :score 2]
                                [:db/add 10002 :score 4]])
              (deliver proceed true)
              (await! #(ready? conn))
              (is (= id (get-in @conn [:avet-build-result :id])))
              (is (= #{[10000 3] [10002 4]}
                     (d/q '[:find ?e ?v :where [?e :score ?v]] @conn)))
              (is (= [3 4] (mapv :v (d/datoms @conn :avet :score))))
              (is (thrown? clojure.lang.ExceptionInfo
                           (d/transact conn [[:db/add 10003 :score 3]])))
              (settled! conn))
            (finally (deliver proceed true))))))))

(deftest duplicate-source-fails-build-without-enabling-uniqueness
  (with-db
    (fn [conn]
      (d/transact conn [[:db/add 10001 :score 1]])
      (invoke! conn 'begin-avet-build! {:score {:db/unique :db.unique/value}})
      (await! #(= :failed (get-in @conn [:avet-build-result :status])))
      (is (nil? (get-in @conn [:schema :score :db/unique])))
      (is (= 2 (count (d/q '[:find ?e :where [?e :score 1]] @conn))))
      (settled! conn)
      (d/transact conn [[:db/add 10001 :score 2]])
      (invoke! conn 'begin-avet-build! {:score {:db/index true}})
      (await! #(ready? conn))
      (is (= [1 2] (mapv :v (d/datoms @conn :avet :score))))
      (settled! conn))))

(deftest rejected-install-retires-job-without-changing-data-or-schema
  (with-db
    (fn [conn]
      (let [store-id (get-in @conn [:config :store :id])]
        (predicates/register-tx-pred!
         store-id ::reject-install
         (fn [report]
           (when (and (get-in report [:db-before :avet-build])
                      (= :ready (get-in report [:db-after :avet-build-result :status])))
             (throw (ex-info "Reject prepared install" {:type ::rejected})))
           report))
        (try
          (invoke! conn 'begin-avet-build! {:score {:db/index true}})
          (await! #(= :failed (get-in @conn [:avet-build-result :status])))
          (is (not (get-in @conn [:schema :score :db/index])))
          (is (= #{[10000 1] [10001 2]} (d/q '[:find ?e ?v :where [?e :score ?v]] @conn)))
          (settled! conn)
          (finally (predicates/unregister-tx-pred! store-id ::reject-install)))))))

(deftest cancelled-generation-cannot-install-over-its-replacement
  (with-db
    (fn [conn]
      (let [entered (promise) proceed (promise) build avet/build! calls (atom 0)]
        (with-redefs [avet/build! (fn [& args]
                                    (when (= 1 (swap! calls inc))
                                      (deliver entered true)
                                      (deref proceed 10000 nil))
                                    (apply build args))]
          (try
            (let [first-id (get-in (invoke! conn 'begin-avet-build! {:score {:db/index true}})
                                   [:db-after :avet-build :id])]
              (is (true? (deref entered 10000 false)))
              (invoke! conn 'cancel-avet-build! first-id)
              (is (= :canceled (get-in @conn [:avet-build-result :status])))
              (let [second-id (get-in (invoke! conn 'begin-avet-build! {:score {:db/unique :db.unique/value}})
                                      [:db-after :avet-build :id])]
                (is (not= first-id second-id))
                (is (thrown? clojure.lang.ExceptionInfo (invoke! conn 'cancel-avet-build! first-id)))
                (deliver proceed true)
                (await! #(ready? conn))
                (is (= second-id (get-in @conn [:avet-build-result :id])))
                (is (= :db.unique/value (get-in @conn [:schema :score :db/unique])))
                (settled! conn)))
            (finally (deliver proceed true))))))))

(deftest schema-change-retires-build-but-commits-the-schema-change
  (with-db
    (fn [conn]
      (let [entered (promise) proceed (promise) build avet/build!]
        (with-redefs [avet/build! (fn [& args]
                                    (deliver entered true)
                                    (deref proceed 10000 nil)
                                    (apply build args))]
          (try
            (invoke! conn 'begin-avet-build! {:score {:db/index true}})
            (is (true? (deref entered 10000 false)))
            (d/transact conn [{:db/ident :other :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one}])
            (is (= :failed (get-in @conn [:avet-build-result :status])))
            (is (contains? (:schema @conn) :other))
            (is (not (get-in @conn [:schema :score :db/index])))
            (deliver proceed true)
            (settled! conn)
            (finally (deliver proceed true))))))))

(deftest lost-durable-pin-refuses-activation
  (with-db
    (fn [conn]
      (let [entered (promise) proceed (promise) build avet/build!]
        (with-redefs [avet/build! (fn [& args]
                                    (deliver entered true)
                                    (deref proceed 10000 nil)
                                    (apply build args))]
          (try
            (invoke! conn 'begin-avet-build! {:score {:db/index true}})
            (is (true? (deref entered 10000 false)))
            (let [pins (<?? S (roots/roots (:store @conn)))]
              (is (= 1 (count pins)))
              (doseq [id (keys pins)] (roots/release! @conn id {:sync? true})))
            (deliver proceed true)
            (await! #(= :failed (get-in @conn [:avet-build-result :status])))
            (is (not (get-in @conn [:schema :score :db/index])))
            (d/transact conn [[:db/add 10000 :score 9]])
            (is (= 9 (:score (d/entity @conn 10000))))
            (settled! conn)
            (finally (deliver proceed true))))))))

(deftest foreign-writer-retires-unowned-generation-without-losing-writes
  (with-db
    (fn [conn]
      (let [entered (promise) proceed (promise) build avet/build!
            registry (atom {})
            other (binding [connections/*connections* registry] (d/connect (:config @conn)))]
        (try
          (with-redefs [avet/build! (fn [& args]
                                      (deliver entered true)
                                      (deref proceed 10000 nil)
                                      (apply build args))]
            (try
              (invoke! conn 'begin-avet-build! {:score {:db/index true}})
              (is (true? (deref entered 10000 false)))
              (d/transact other [[:db/add 10000 :score 7]])
              (is (= :failed (get-in @other [:avet-build-result :status])))
              (d/transact conn [[:db/add 10001 :score 8]])
              (deliver proceed true)
              (settled! conn)
              (is (= #{[10000 7] [10001 8]}
                     (d/q '[:find ?e ?v :where [?e :score ?v]] @conn)))
              (is (not (get-in @conn [:schema :score :db/index])))
              (finally (deliver proceed true))))
          (finally (binding [connections/*connections* registry] (d/release other))))))))

(deftest published-file-index-reopens-after-private-worker-storage-is-closed
  (doseq [attribute-refs? [false true]]
    (let [path (java.nio.file.Files/createTempDirectory "datahike-online-avet-"
                                                        (make-array java.nio.file.attribute.FileAttribute 0))]
      (try
        (with-db
          {:attribute-refs? attribute-refs?
           :store {:backend :file :path (str path) :id (random-uuid)}}
          (fn [conn]
            (is (nil? (d/avet-build-status @conn)))
            (d/transact conn [[:db/add 10000 :score 5]])
            (let [history-before (set (d/q '[:find ?e ?v ?tx ?added
                                             :where [?e :score ?v ?tx ?added]] (d/history @conn)))]
              (invoke! conn 'begin-avet-build! {:score {:db/unique :db.unique/value}})
              (await! #(ready? conn))
              (settled! conn)
              (let [registry (atom {})
                    reopened (binding [connections/*connections* registry] (d/connect (:config @conn)))]
                (try
                  (is (= [2 5] (mapv :v (d/datoms @reopened :avet :score))))
                  (is (= history-before
                         (set (d/q '[:find ?e ?v ?tx ?added
                                     :where [?e :score ?v ?tx ?added]] (d/history @reopened)))))
                  (is (= :ready (:status (d/avet-build-status @reopened))))
                  (is (thrown? clojure.lang.ExceptionInfo
                               (d/transact reopened [[:db/add 10002 :score 5]])))
                  (finally (binding [connections/*connections* registry] (d/release reopened))))))))
        (finally (java.nio.file.Files/deleteIfExists path))))))

(deftest same-commit-head-rewrite-fences-an-offered-install
  (with-db
    (fn [conn]
      (let [commit writing/commit! rewritten? (atom false)
            store (:store @conn) branch (get-in @conn [:config :branch])]
        (with-redefs [writing/commit!
                      (fn [database & args]
                        (when (and (= :ready (get-in database [:avet-build-result :status]))
                                   (compare-and-set! rewritten? false true))
                          ;; Rewriting identical head data changes its storage revision,
                          ;; but neither max-tx nor content-addressed commit identity.
                          (let [head (k/get store branch nil {:sync? true})]
                            (k/assoc store branch head {:sync? true})
                            (is (= head (k/get store branch nil {:sync? true})))))
                        (apply commit database args))]
          (invoke! conn 'begin-avet-build! {:score {:db/index true}})
          (await! #(= :failed (:status (d/avet-build-status @conn))))
          (is @rewritten?)
          (is (not (get-in @conn [:schema :score :db/index])))
          (settled! conn))
        (d/transact conn [[:db/add 10000 :score 9]])
        (is (= 9 (:score (d/entity @conn 10000))))
        (invoke! conn 'begin-avet-build! {:score {:db/index true}})
        (await! #(ready? conn))
        (is (= [2 9] (mapv :v (d/datoms @conn :avet :score))))
        (settled! conn)))))
