(ns datahike.test.nodejs-test
  (:require [cljs.test :refer [deftest is async] :as t]
            [datahike.api :as d]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [konserve.node-filestore] ;; Register :file backend for Node.js
            [cljs.core.async :refer [go <!] :include-macros true]
            [cljs.nodejs :as nodejs]
            ;; Sibling test namespaces — included so `bb node-cljs-test`
            ;; covers them too.
            [datahike.test.cljs-pattern-scan-test]))

;; Hook cljs.test's end-of-run callback so the Node process exits with
;; status 0 only when all tests pass. The previous setup always exited
;; 0 (via a fixed `(.exit js/process 0)` inside the last deftest's
;; finally-clause), which silently masked failing tests in CI.
(defmethod t/report [::t/default :end-run-tests] [m]
  (.exit js/process (if (t/successful? m) 0 1)))

(def fs (nodejs/require "fs"))
(def path (nodejs/require "path"))
(def os (nodejs/require "os"))

(defn tmp-dir []
  (let [dir (path.join (os.tmpdir) (str "datahike-node-test-" (rand-int 100000)))]
    dir))

(deftest roundtrip-test
  (let [dir (tmp-dir)
        store-id (random-uuid)
        cfg {:store {:backend :file :path dir :id store-id}
             :keep-history? true
             :schema-flexibility :write}]
    (async done
           (go
             (try
          ;; Create database
               (let [created (<! (d/create-database cfg))]
                 (is created "Database created"))

          ;; Connect
               (let [conn (d/connect cfg)]
                 (is conn "Connection established")

            ;; Add schema
                 (let [schema-tx [{:db/ident :name
                                   :db/valueType :db.type/string
                                   :db/cardinality :db.cardinality/one}
                                  {:db/ident :age
                                   :db/valueType :db.type/long
                                   :db/cardinality :db.cardinality/one}]]
                   (let [report (<! (d/transact! conn schema-tx))]
                     (is (:db-after report) "Schema added")))

            ;; Transact data
                 (let [tx-report (<! (d/transact! conn [{:name "Alice" :age 30}
                                                        {:name "Bob" :age 25}]))]
                   (is (:db-after tx-report) "Data transacted")
                   (is (pos? (count (:tx-data tx-report))) "Datoms were added"))

            ;; Verify data via datoms API
                 (let [all-datoms (vec (d/datoms @conn :eavt))
                       name-datoms (filter #(= :name (:a %)) all-datoms)
                       age-datoms (filter #(= :age (:a %)) all-datoms)]
                   (is (= 2 (count name-datoms)) "Found 2 name datoms")
                   (is (= 2 (count age-datoms)) "Found 2 age datoms"))

            ;; Pull API
                 (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
                       e1 (ffirst entities)]
                   (when e1
                     (let [pulled (d/pull @conn [:name :age] e1)]
                       (is (:name pulled) "Pull retrieved name")
                       (is (:age pulled) "Pull retrieved age"))))

            ;; Query API
                 (let [q1 (d/q '[:find ?e :where [?e :name _]] @conn)]
                   (is (= 2 (count q1)) "Single pattern: found 2 entities")
                   (is (every? #(number? (first %)) q1) "Single pattern: entity IDs are numbers"))

                 (let [q2 (d/q '[:find ?v :where [_ :name ?v]] @conn)
                       names (set (map first q2))]
                   (is (= 2 (count q2)) "Value query: found 2 names")
                   (is (contains? names "Alice") "Value query: found Alice")
                   (is (contains? names "Bob") "Value query: found Bob"))

                 (let [q3 (d/q '[:find ?e ?name ?age
                                 :where
                                 [?e :name ?name]
                                 [?e :age ?age]] @conn)
                       results (into {} (map (fn [[e name age]] [name {:e e :age age}]) q3))]
                   (is (= 2 (count q3)) "Join query: found 2 entity/name/age tuples")
                   (is (number? (get-in results ["Alice" :e])) "Join query: Alice has valid entity ID")
                   (is (= 30 (get-in results ["Alice" :age])) "Join query: Alice is 30")
                   (is (number? (get-in results ["Bob" :e])) "Join query: Bob has valid entity ID")
                   (is (= 25 (get-in results ["Bob" :age])) "Join query: Bob is 25"))

            ;; Entity API
                 (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
                       e1 (ffirst entities)]
                   (when e1
                     (let [entity (d/entity @conn e1)]
                       (is (:name entity) "Entity has name")
                       (is (:age entity) "Entity has age"))))

            ;; Update for history
                 (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
                       e1 (ffirst entities)]
                   (when e1
                     (<! (d/transact! conn [[:db/add e1 :age 31]]))))

            ;; Test as-of DB
                 (let [entities (d/q '[:find ?e :where [?e :name _]] @conn)
                       e1 (ffirst entities)
                       before-update-tx (:max-tx @conn)]
                   (when e1
                     (<! (d/transact! conn [[:db/add e1 :age 99]])))
                   (let [current-age-after (when e1 (:age (d/entity @conn e1)))]
                     (is (= 99 current-age-after) "Current DB shows updated age"))
                   (let [as-of-db (d/as-of @conn before-update-tx)
                         as-of-age (when e1 (:age (d/entity as-of-db e1)))]
                     (is (= 31 as-of-age) "as-of DB shows old age value")))

            ;; History DB
                 (let [hist-db (d/history @conn)
                       hist-datoms (vec (filter #(= :age (:a %)) (d/datoms hist-db :eavt)))]
                   (is (>= (count hist-datoms) 4) "History contains multiple age values"))

                 (d/release conn))

          ;; Reconnect to verify persistence
               (let [conn2 (d/connect cfg)]
                 (is conn2 "Reconnected successfully")

                 (let [all-datoms (vec (d/datoms @conn2 :eavt))
                       name-datoms (filter #(= :name (:a %)) all-datoms)]
                   (is (= 2 (count name-datoms)) "Data persisted after reconnect"))

            ;; Test different index access
                 (let [aevt-datoms (take 5 (d/datoms @conn2 :aevt))
                       avet-datoms (take 5 (d/datoms @conn2 :avet))]
                   (is (seq aevt-datoms) "Got datoms from AEVT index")
                   (is (seq avet-datoms) "Got datoms from AVET index"))

                 (d/release conn2))

          ;; Delete database
               (let [deleted (<! (d/delete-database cfg))]
                 (is (nil? deleted) "Database deleted"))

               (is (not (fs.existsSync dir)) "Directory removed")

               (catch js/Error e
                 (is false (str "Error: " (.-message e))))
               (finally
                 (done)))))))

(deftest branching-and-merge-test
  (let [dir (tmp-dir)
        cfg {:store {:backend :file :path dir :id (random-uuid)}
             :keep-history? false
             :schema-flexibility :write}]
    (async done
           (go
             (try
               ;; Create and connect
               (<! (d/create-database cfg))
               (let [conn (d/connect cfg)]

                 ;; Schema + data
                 (<! (d/transact! conn [{:db/ident :name
                                         :db/valueType :db.type/string
                                         :db/cardinality :db.cardinality/one}]))
                 (<! (d/transact! conn [{:name "Alice"}
                                        {:name "Bob"}]))

                 ;; Verify branches (should be just :db)
                 (let [bs (<! (d/branches conn))]
                   (is (contains? bs :db) "Default branch :db exists")
                   (is (= 1 (count bs)) "Only one branch initially"))

                 ;; Verify commit-id and parent-commit-ids
                 (let [cid (d/commit-id @conn)
                       pids (d/parent-commit-ids @conn)]
                   (is (uuid? cid) "commit-id is a UUID")
                   (is (set? pids) "parent-commit-ids is a set"))

                 ;; Branch
                 (<! (d/branch! conn :db :feature))
                 (let [bs (<! (d/branches conn))]
                   (is (= #{:db :feature} bs) "Feature branch created"))

                 ;; Connect to feature branch
                 (let [feat-conn (d/connect (assoc cfg :branch :feature))]

                   ;; Add data on feature
                   (<! (d/transact! feat-conn [{:name "Charlie"}]))
                   (let [feat-names (d/q '[:find ?n :where [_ :name ?n]] @feat-conn)
                         feat-name-set (set (map first feat-names))]
                     (is (contains? feat-name-set "Charlie") "Feature has Charlie")
                     (is (contains? feat-name-set "Alice") "Feature inherited Alice"))

                   ;; Main should not have Charlie
                   (let [main-names (d/q '[:find ?n :where [_ :name ?n]] @conn)
                         main-name-set (set (map first main-names))]
                     (is (not (contains? main-name-set "Charlie")) "Main does not have Charlie yet"))

                   ;; Merge feature into main
                   (let [merge-report (<! (d/merge-db! conn #{:feature} [{:name "Charlie"}]))]
                     (is (:db-after merge-report) "Merge produced db-after"))

                   ;; Main should now have Charlie
                   (let [main-names (d/q '[:find ?n :where [_ :name ?n]] @conn)
                         main-name-set (set (map first main-names))]
                     (is (contains? main-name-set "Charlie") "Main has Charlie after merge")
                     (is (contains? main-name-set "Alice") "Main still has Alice"))

                   ;; branch-as-db
                   (let [feat-db (<! (d/branch-as-db conn :feature))]
                     (is (some? feat-db) "branch-as-db returns a db"))

                   ;; commit-as-db
                   (let [cid (d/commit-id @conn)
                         cdb (<! (d/commit-as-db conn cid))]
                     (is (some? cdb) "commit-as-db returns a db"))

                   ;; Delete branch
                   (<! (d/delete-branch! conn :feature))
                   (let [bs (<! (d/branches conn))]
                     (is (= #{:db} bs) "Feature branch deleted"))

                   (d/release feat-conn))
                 (d/release conn))

               ;; Cleanup
               (<! (d/delete-database cfg))

               (catch js/Error e
                 (is false (str "Error: " (.-message e))))
               (finally
                 (done)))))))

(deftest online-gc-basic-test
  (async done
         (go
           (try
             (let [dir (tmp-dir)
                   cfg-no-gc {:store {:backend :file :path dir :id (random-uuid)}
                              :online-gc {:enabled? false}
                              :crypto-hash? false
                              :keep-history? false
                              :schema-flexibility :write}]

               ;; Create database without online GC initially
               (<! (d/create-database cfg-no-gc))
               (let [conn (d/connect cfg-no-gc)]

                 ;; Add schema
                 (<! (d/transact! conn [{:db/ident :name
                                         :db/valueType :db.type/string
                                         :db/cardinality :db.cardinality/one}]))

                 ;; Add data to create freed addresses
                 (<! (d/transact! conn [{:name "Alice"}]))
                 (<! (d/transact! conn [{:name "Bob"}]))

                 ;; Get freed count (should have freed addresses from schema + data txs)
                 (let [freed-atom (-> @conn :store :storage :freed-addresses)
                       initial-freed (count @freed-atom)]
                   (is (> initial-freed 0) "Should have freed addresses with GC disabled"))

                 ;; Run online GC explicitly
                 (let [gc-result (<! (online-gc/online-gc! (:store @conn)
                                                           {:enabled? true
                                                            :grace-period-ms 0
                                                            :sync? false}))]
                   (is (number? gc-result) "GC returned a count"))

                 ;; Check that freed addresses were cleared
                 (let [freed-atom (-> @conn :store :storage :freed-addresses)
                       final-freed (count @freed-atom)]
                   (is (= 0 final-freed) "Freed addresses should be cleared after GC"))

                 (d/release conn))

               ;; Cleanup
               (<! (d/delete-database cfg-no-gc)))

             (catch js/Error e
               (is false (str "Error in online-gc-basic-test: " (.-message e))))
             (finally
               (done))))))

(deftest online-gc-multi-branch-safety-test
  (async done
         (go
           (try
             (let [dir (tmp-dir)
                   cfg-no-gc {:store {:backend :file :path dir :id (random-uuid)}
                              :online-gc {:enabled? false}
                              :crypto-hash? false
                              :keep-history? false
                              :schema-flexibility :write}]

               ;; Create database without online GC initially
               (<! (d/create-database cfg-no-gc))
               (let [conn (d/connect cfg-no-gc)]

                 ;; Add schema and data
                 (<! (d/transact! conn [{:db/ident :name
                                         :db/valueType :db.type/string
                                         :db/cardinality :db.cardinality/one}]))
                 (<! (d/transact! conn [{:name "Alice"}]))

                 ;; Simulate multi-branch scenario by adding a second branch
                 (<! (k/assoc (:store @conn) :branches #{:db :branch-a}))

                 ;; Verify multi-branch state
                 (let [branches (<! (k/get (:store @conn) :branches))]
                   (is (= 2 (count branches)) "Should have two branches"))

                 ;; Add more data to generate freed addresses
                 (<! (d/transact! conn [{:name "Bob"}]))

                 (let [freed-before (count @(-> @conn :store :storage :freed-addresses))]
                   (is (> freed-before 0) "Should have freed addresses with GC disabled"))

                 ;; Run online GC - should detect multi-branch and SKIP entirely
                 (let [gc-result (<! (online-gc/online-gc! (:store @conn)
                                                           {:enabled? true
                                                            :grace-period-ms 0
                                                            :sync? false}))]
                   (is (= 0 gc-result) "Multi-branch GC should be skipped (return 0)"))

                 ;; Freed addresses should remain (not deleted, re-marked for offline GC)
                 (let [freed-after (count @(-> @conn :store :storage :freed-addresses))]
                   (is (> freed-after 0) "Multi-branch GC should leave freed addresses for offline GC"))

                 ;; Verify database is still functional
                 (let [result (d/q '[:find ?e ?n :where [?e :name ?n]] @conn)]
                   (is (= 2 (count result)) "Both Alice and Bob should still exist"))

                 (d/release conn))

               ;; Cleanup
               (<! (d/delete-database cfg-no-gc)))

             (catch js/Error e
               (is false (str "Error in online-gc-multi-branch-safety-test: " (.-message e))))
             (finally
               (done))))))

(defn -main []
  (t/run-tests 'datahike.test.nodejs-test
               'datahike.test.cljs-pattern-scan-test))
