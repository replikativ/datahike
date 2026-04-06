(ns datahike.test.secondary-dynamic-test
  "Tests for dynamic secondary index creation via schema transactions."
  (:require
   [clojure.test :refer [deftest testing is use-fixtures]]
   [datahike.api :as d]
   [datahike.index.secondary :as sec]))

;; Register a test recorder index type for all tests
(defonce _register-recorder
  (sec/register-index-type!
   :test/dynamic-recorder
   (fn [config _db]
     (let [state (atom [])]
       (reify
         sec/ISecondaryIndex
         (-search [_ _ _] nil)
         (-estimate [_ _] 0)
         (-can-order? [_ _ _] false)
         (-slice-ordered [_ _ _ _ _ _] nil)
         (-indexed-attrs [_] (set (:attrs config)))
         (-transact [this tx-report]
           (swap! state conj tx-report)
           this)

         sec/ITransientSecondaryIndex
         (-as-transient [this] this)
         (-transact! [this tx-report]
           (swap! state conj tx-report))
         (-persistent! [this] this)

         clojure.lang.IDeref
         (deref [_] @state))))))

(deftest test-dynamic-secondary-index-creation
  (testing "schema transaction creates secondary index with :building status"
    (let [cfg {:store {:backend :memory
                       :id (random-uuid)}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        ;; First, define the attribute we want to index
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        ;; Add some data before creating the index
        (d/transact conn [{:person/name "Alice"}
                          {:person/name "Bob"}])

        ;; Now dynamically add a secondary index
        (d/transact conn [{:db/ident :idx/recorder
                           :db.secondary/type :test/dynamic-recorder
                           :db.secondary/attrs [:person/name]}])

        ;; Give the backfill writer op time to complete
        (Thread/sleep 500)

        (let [db (d/db conn)
              schema (:schema db)
              idx-schema (get schema :idx/recorder)]
          ;; Index should exist in secondary-indices
          (is (some? (get-in db [:secondary-indices :idx/recorder]))
              "Index instance should be created")
          ;; rschema should map :person/name -> #{:idx/recorder}
          (is (contains? (get-in db [:rschema :db.secondary/index :person/name]) :idx/recorder)
              "rschema should map attribute to index")
          ;; Status should be :ready after backfill
          (is (= :ready (:db.secondary/status idx-schema))
              "Index should be :ready after backfill")
          ;; Backfill should have fed existing datoms
          (let [idx (get-in db [:secondary-indices :idx/recorder])
                recorded @idx]
            (is (>= (count recorded) 2)
                "Backfill should have fed at least 2 existing datoms")))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest test-new-transactions-feed-into-building-index
  (testing "transactions after index creation feed datoms into the index"
    (let [cfg {:store {:backend :memory
                       :id (random-uuid)}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])

        ;; Create the secondary index
        (d/transact conn [{:db/ident :idx/recorder2
                           :db.secondary/type :test/dynamic-recorder
                           :db.secondary/attrs [:person/name]}])

        ;; Wait for backfill to complete
        (Thread/sleep 500)

        ;; Now transact new data — should feed into the index
        (d/transact conn [{:person/name "Charlie"}
                          {:person/name "Diana"}])

        (let [db (d/db conn)
              idx (get-in db [:secondary-indices :idx/recorder2])
              recorded @idx]
          ;; Should have received at least the 2 new datoms
          (is (>= (count (filter :added? recorded)) 2)
              "New datoms should feed into the index after creation"))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest test-secondary-index-status-ready
  (testing "secondary index reaches :ready after backfill"
    (let [cfg {:store {:backend :memory
                       :id (random-uuid)}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])

        ;; Create the secondary index and wait for backfill
        (d/transact conn [{:db/ident :idx/recorder3
                           :db.secondary/type :test/dynamic-recorder
                           :db.secondary/attrs [:person/name]}])
        (Thread/sleep 500)

        ;; Verify it's ready
        (is (= :ready (get-in (d/db conn) [:schema :idx/recorder3 :db.secondary/status])))

        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(deftest test-secondary-index-recovery-on-reconnect
  (testing "secondary index in :building state is recovered after reconnect"
    (let [path (str "/tmp/datahike-sec-recovery-" (random-uuid))
          cfg {:store {:backend :file
                       :id (java.util.UUID/randomUUID)
                       :path path}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:person/name "Alice"} {:person/name "Bob"}])

        ;; Create secondary index and release immediately.
        ;; d/transact returns after the schema tx commits (with :building status).
        ;; The backfill + install runs asynchronously after that.
        ;; Releasing right away shuts down the writer before install can run,
        ;; leaving :building status on disk.
        (d/transact conn [{:db/ident :idx/recorder-recover
                           :db.secondary/type :test/dynamic-recorder
                           :db.secondary/attrs [:person/name]}])
        (d/release conn)

        ;; Reconnect — recovery should detect :building (no stored key-map
        ;; since the recorder type doesn't implement IVersionedSecondaryIndex)
        ;; and run build + install to transition to :ready.
        (let [conn2 (d/connect cfg)]
          (try
            (Thread/sleep 1000)
            (is (= :ready
                   (get-in (d/db conn2) [:schema :idx/recorder-recover :db.secondary/status]))
                "Recovery should transition index from :building to :ready")
            (finally
              (d/release conn2))))
        (finally
          (d/delete-database cfg))))))
