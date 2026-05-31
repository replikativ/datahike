(ns datahike.test.nodejs-test
  (:require [cljs.test :refer [deftest is async] :as t]
            [cljs.reader]
            [datahike.api :as d]
            [datahike.index.audit :as ia]
            [datahike.audit :as audit]
            [datahike.online-gc :as online-gc]
            [konserve.core :as k]
            [konserve.node-filestore :as nfs] ;; Register :file backend for Node.js
            [cljs.core.async :refer [go <!] :include-macros true]
            [cljs.nodejs :as nodejs]
            ;; Sibling test namespaces — included so `bb node-cljs-test`
            ;; covers them too.
            [datahike.test.cljs-pattern-scan-test]
            [datahike.test.optimistic-test]))

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

;; OP_BUF_V5 phase-1 gate: read a JVM-written op-buf store from cljs and verify the
;; buffered-leaf projection (Branch.child) reconstructs identical datoms cross-host.
;; The store + reference datoms are produced by /tmp/dh_exchange_build.clj on the JVM;
;; this test is a no-op (passes) when that artifact is absent (e.g. normal CI).
(def ^:private exchange-expected-file "/tmp/dh-exchange-expected.edn")

(deftest jvm-opbuf-exchange-test
  (async done
    (go
      (try
        (if-not (fs.existsSync exchange-expected-file)
          (is true "JVM op-buf exchange artifact absent — skipped")
          (let [{:keys [store-id dir n-count n-sum datom-count datoms]}
                (cljs.reader/read-string (.readFileSync fs exchange-expected-file "utf8"))
                cfg {:store {:backend :file :path dir :id store-id}
                     :schema-flexibility :write :keep-history? false}
                conn (d/connect cfg)
                db   @conn
                got-datoms (->> (d/datoms db :eavt)
                                (map (fn [d] [(:e d) (name (:a d)) (str (:v d))]))
                                (sort)
                                (vec))
                got-n-count (d/q '[:find (count ?e) . :where [?e :n _]] db)
                got-n-sum   (reduce + (map :v (filter #(= :n (:a %)) (d/datoms db :eavt))))]
            (is (= datom-count (count got-datoms))
                (str "cljs read same datom count (jvm=" datom-count " cljs=" (count got-datoms) ")"))
            (is (= n-count got-n-count)
                (str ":n entity count matches (jvm=" n-count " cljs=" got-n-count ")"))
            (is (= n-sum got-n-sum)
                (str ":n value sum matches (projection-sound) (jvm=" n-sum " cljs=" got-n-sum ")"))
            (is (= datoms got-datoms)
                "cljs eavt datoms identical to JVM (full buffered-leaf projection)")
            (d/release conn)))
        (catch js/Error e
          (is false (str "jvm-opbuf-exchange-test error: " (.-message e))))
        (finally
          (done))))))

;; OP_BUF_V5 phase-2 gate: cljs WRITE path. Same-host (create+transact+query all in cljs,
;; avoiding the pre-existing cross-host connect bug). Incremental commits make leaves
;; content-only dirty → buffered leaf slots in the root → on cold reopen they project back.
;; Writes to a FIXED dir (not deleted) so buffering can be confirmed externally (grep slots).
(def ^:private cljs-opbuf-dir "/tmp/dh-cljs-opbuf")

(deftest cljs-opbuf-write-roundtrip-test
  (let [sid #uuid "00000000-0000-0000-0000-00000000c1c5"
        cfg {:store {:backend :file :path cljs-opbuf-dir :id sid}
             :schema-flexibility :write :keep-history? false
             :index :datahike.index/persistent-set
             :index-config {:op-buf-size 256}}]
    (async done
      (go
        (try
          (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
          (<! (d/create-database cfg))
          (let [conn (d/connect cfg)]
            (<! (d/transact! conn [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]))
            (loop [bs (partition-all 100 (range 3000))]
              (when (seq bs)
                (<! (d/transact! conn (mapv (fn [i] {:n i}) (first bs))))
                (recur (rest bs))))
            (let [db @conn
                  n-count (d/q '[:find (count ?e) . :where [?e :n _]] db)
                  n-sum   (reduce + (map :v (filter #(= :n (:a %)) (d/datoms db :eavt))))]
              (is (= 3000 n-count) (str "warm n-count=" n-count))
              (is (= 4498500 n-sum) (str "warm n-sum=" n-sum)))
            (d/release conn))
          ;; cold reopen → forces projection-on-read of buffered slots
          (let [conn2 (d/connect cfg)
                db2   @conn2
                n-count2 (d/q '[:find (count ?e) . :where [?e :n _]] db2)
                n-sum2   (reduce + (map :v (filter #(= :n (:a %)) (d/datoms db2 :eavt))))
                all-vs   (vec (sort (map :v (filter #(= :n (:a %)) (d/datoms db2 :eavt)))))]
            (is (= 3000 n-count2) (str "cold n-count=" n-count2))
            (is (= 4498500 n-sum2) (str "cold n-sum=" n-sum2))
            (is (= (vec (range 3000)) all-vs) "cold :n values exact 0..2999 (buffered-leaf projection sound)")
            (d/release conn2))
          (catch js/Error e
            (is false (str "cljs-opbuf-write-roundtrip error: " (.-message e))))
          (finally
            (done)))))))

;; OP_BUF_V5 phase-2 gate: cljs $remove path (retractions → leaf underflow → merge/borrow,
;; exercising the rotate/merge/merge-split slot-carry). Insert 2000, retract the even ones,
;; cold-reopen and verify the surviving odd set exactly.
(def ^:private cljs-opbuf-rm-dir "/tmp/dh-cljs-opbuf-rm")

(deftest cljs-opbuf-remove-roundtrip-test
  (let [sid #uuid "00000000-0000-0000-0000-0000000c1c5b"
        cfg {:store {:backend :file :path cljs-opbuf-rm-dir :id sid}
             :schema-flexibility :write :keep-history? false
             :index :datahike.index/persistent-set
             :index-config {:op-buf-size 256}}]
    (async done
      (go
        (try
          (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
          (<! (d/create-database cfg))
          (let [conn (d/connect cfg)]
            (<! (d/transact! conn [{:db/ident :n :db/valueType :db.type/long
                                    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}]))
            (loop [bs (partition-all 100 (range 2000))]
              (when (seq bs)
                (<! (d/transact! conn (mapv (fn [i] {:n i}) (first bs))))
                (recur (rest bs))))
            ;; retract even-:n entities (unique :n ⇒ lookup-ref retraction) in small commits
            (loop [bs (partition-all 100 (filter even? (range 2000)))]
              (when (seq bs)
                (<! (d/transact! conn (mapv (fn [i] [:db/retractEntity [:n i]]) (first bs))))
                (recur (rest bs))))
            (let [db @conn
                  vs (vec (sort (map :v (filter #(= :n (:a %)) (d/datoms db :eavt)))))]
              (is (= 1000 (count vs)) (str "warm survivors=" (count vs)))
              (is (= (vec (range 1 2000 2)) vs) "warm: exactly the odd :n survive"))
            (d/release conn))
          ;; cold reopen → projection-on-read of buffered slots after structural removes
          (let [conn2 (d/connect cfg)
                db2   @conn2
                vs    (vec (sort (map :v (filter #(= :n (:a %)) (d/datoms db2 :eavt)))))
                sum   (reduce + vs)]
            (is (= 1000 (count vs)) (str "cold survivors=" (count vs)))
            (is (= 1000000 sum) (str "cold sum of odds=" sum))
            (is (= (vec (range 1 2000 2)) vs) "cold: exactly the odd :n survive (remove+merge slot-carry sound)")
            (d/release conn2))
          (catch js/Error e
            (is false (str "cljs-opbuf-remove-roundtrip error: " (.-message e))))
          (finally
            (done)))))))

;; OP_BUF_V5 phase-2 gate: cljs $replace path. A cardinality-one re-assertion (upsert with an
;; old value) routes through psset/replace → Branch.$replace for eavt/aevt. Insert 1000 ids
;; with :n 0, then update each :n to its id in small commits, cold-reopen and verify :n == id.
(def ^:private cljs-opbuf-rep-dir "/tmp/dh-cljs-opbuf-rep")

(deftest cljs-opbuf-replace-roundtrip-test
  (let [sid #uuid "00000000-0000-0000-0000-0000000c1c5c"
        cfg {:store {:backend :file :path cljs-opbuf-rep-dir :id sid}
             :schema-flexibility :write :keep-history? false
             :index :datahike.index/persistent-set
             :index-config {:op-buf-size 256}}]
    (async done
      (go
        (try
          (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
          (<! (d/create-database cfg))
          (let [conn (d/connect cfg)]
            (<! (d/transact! conn [{:db/ident :id :db/valueType :db.type/long
                                    :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
                                   {:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]))
            (loop [bs (partition-all 100 (range 1000))]
              (when (seq bs)
                (<! (d/transact! conn (mapv (fn [i] {:id i :n 0}) (first bs))))
                (recur (rest bs))))
            ;; cardinality-one update of :n in small commits → upsert → $replace (eavt/aevt)
            (loop [bs (partition-all 100 (range 1000))]
              (when (seq bs)
                (<! (d/transact! conn (mapv (fn [i] [:db/add [:id i] :n i]) (first bs))))
                (recur (rest bs))))
            (let [db @conn
                  pairs (d/q '[:find ?id ?n :where [?e :id ?id] [?e :n ?n]] db)]
              (is (= 1000 (count pairs)) (str "warm pairs=" (count pairs)))
              (is (every? (fn [[id n]] (= id n)) pairs) "warm: every :n updated to its :id"))
            (d/release conn))
          ;; cold reopen → projection-on-read after $replace buffering
          (let [conn2 (d/connect cfg)
                db2   @conn2
                pairs (d/q '[:find ?id ?n :where [?e :id ?id] [?e :n ?n]] db2)
                nsum  (reduce + (map second pairs))]
            (is (= 1000 (count pairs)) (str "cold pairs=" (count pairs)))
            (is (every? (fn [[id n]] (= id n)) pairs) "cold: every :n == its :id ($replace projection sound)")
            (is (= 499500 nsum) (str "cold sum :n=" nsum)))
          (catch js/Error e
            (is false (str "cljs-opbuf-replace-roundtrip error: " (.-message e))))
          (finally
            (done)))))))

;; OP_BUF_V5 phase-2 soundness gate: randomized insert/retract churn under a SMALL op-buf
;; budget (more frequent buffer/write decisions, merges, borrows, splits) with periodic cold
;; reopens, compared against a reference set. Seeded LCG ⇒ deterministic/reproducible.
(def ^:private cljs-opbuf-gen-dir "/tmp/dh-cljs-opbuf-gen")

(deftest cljs-opbuf-generative-test
  (let [sid  #uuid "00000000-0000-0000-0000-0000000c1c5d"
        cfg  {:store {:backend :file :path cljs-opbuf-gen-dir :id sid}
              :schema-flexibility :write :keep-history? false
              :index :datahike.index/persistent-set
              :index-config {:op-buf-size 64}}
        seed (atom 777)
        rnd  (fn [n] (mod (swap! seed (fn [x] (mod (+ (* x 1103515245) 12345) 2147483648))) n))
        idset (fn [c] (set (d/q '[:find [?id ...] :where [_ :id ?id]] @c)))]
    (async done
      (go
        (try
          (when (<! (d/database-exists? cfg)) (<! (d/delete-database cfg)))
          (<! (d/create-database cfg))
          (let [present (atom #{})
                conn0 (d/connect cfg)]
            (<! (d/transact! conn0 [{:db/ident :id :db/valueType :db.type/long
                                     :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}]))
            ;; bulk-seed >bf entities so the index has BRANCH nodes (op-buf only engages on
            ;; branches; a sub-512 tree is a single leaf and never buffers).
            (loop [bs (partition-all 200 (range 2000))]
              (when (seq bs)
                (<! (d/transact! conn0 (mapv (fn [i] {:id i}) (first bs))))
                (recur (rest bs))))
            (reset! present (set (range 2000)))
            (loop [conn conn0, round 0]
              (if (>= round 40)
                (do (d/release conn)
                    (let [c (d/connect cfg)]
                      (is (= @present (idset c)) (str "final ref=" (count @present) " got=" (count (idset c))))
                      (d/release c)))
                (let [insert? (even? (rnd 2))
                      cand    (vec (distinct (repeatedly 40 #(rnd 4000))))
                      ops     (if insert? (vec (remove @present cand)) (vec (filter @present cand)))]
                  (when (seq ops)
                    (if insert?
                      (do (<! (d/transact! conn (mapv (fn [i] {:id i}) ops)))
                          (swap! present into ops))
                      (do (<! (d/transact! conn (mapv (fn [i] [:db/retractEntity [:id i]]) ops)))
                          (swap! present (fn [s] (reduce disj s ops))))))
                  (if (zero? (mod (inc round) 8))
                    (do (d/release conn)
                        (let [c (d/connect cfg)]
                          (is (= @present (idset c)) (str "round " round " ref=" (count @present) " got=" (count (idset c))))
                          (recur c (inc round))))
                    (recur conn (inc round)))))))
          (catch js/Error e
            (is false (str "cljs-opbuf-generative error: " (.-message e))))
          (finally
            (done)))))))

;; OP_BUF_V5 phase-3 gate: cljs MERKLE AUDIT (crypto-hash). Validates the cljs port of
;; branch-crypto-uuid/canon/walk-pss + -recompute-merkle-root, exercised via the real
;; datahike.audit/verify-chain :deep? API (which re-derives every node's content hash from
;; storage and confirms it matches its address). Covers baseline crypto AND crypto+op-buf
;; (branch hash folds the slots), warm and after a cold reopen (projection-on-read). Also
;; spot-checks the index-level protocol directly.
(defn- audit-indices [db]
  (mapv (fn [k] [k (:status (ia/-recompute-merkle-root (get db k)))])
        [:eavt :aevt :avet]))

(defn- deep-verify-ok? [db]
  (let [rep (audit/verify-chain db nil {:deep? true})]
    [(:status rep) (get-in rep [:deep :status]) (get-in rep [:deep :diffs])]))

(deftest cljs-merkle-audit-test
  (async done
    (go
      (try
        (doseq [[label opbuf] [["crypto baseline" 0] ["crypto + op-buf" 256]]]
          (let [dir (tmp-dir)
                cfg {:store {:backend :file :path dir :id (random-uuid)}
                     :schema-flexibility :write :keep-history? false
                     :crypto-hash? true
                     :index :datahike.index/persistent-set
                     :index-config (when (pos? opbuf) {:op-buf-size opbuf})}]
            (<! (d/create-database cfg))
            (let [conn (d/connect cfg)]
              (<! (d/transact! conn [{:db/ident :n :db/valueType :db.type/long :db/cardinality :db.cardinality/one}]))
              (loop [bs (partition-all 100 (range 2000))]
                (when (seq bs)
                  (<! (d/transact! conn (mapv (fn [i] {:n i}) (first bs))))
                  (recur (rest bs))))
              (let [res (audit-indices @conn)
                    [st deep diffs] (deep-verify-ok? @conn)]
                (is (every? (fn [[_ s]] (= :ok s)) res) (str label " warm index audit: " (pr-str res)))
                (is (and (= :ok st) (= :ok deep)) (str label " warm verify-chain deep: " st "/" deep " diffs=" (pr-str diffs))))
              (d/release conn))
            ;; cold reopen → audit must still re-derive matching hashes (op-buf projection)
            (let [conn2 (d/connect cfg)
                  res   (audit-indices @conn2)
                  [st deep diffs] (deep-verify-ok? @conn2)]
              (is (every? (fn [[_ s]] (= :ok s)) res) (str label " cold index audit: " (pr-str res)))
              (is (and (= :ok st) (= :ok deep)) (str label " cold verify-chain deep: " st "/" deep " diffs=" (pr-str diffs)))
              (d/release conn2))
            (<! (d/delete-database cfg))))
        (catch js/Error e
          (is false (str "cljs-merkle-audit error: " (.-message e))))
        (finally
          (done))))))

;; Isolation probe: read a JVM-konserve-written map (default fressian serializer) cross-host
;; to test fress deserialization of namespaced keywords etc. (datahike-independent).
;; Written by /tmp/kons_probe_write.clj. Skips if absent.
(deftest xhost-fress-probe-test
  (async done
    (go
      (try
        (if-not (fs.existsSync "/tmp/kons-probe")
          (is true "kons-probe artifact absent — skipped")
          (let [store (<! (nfs/connect-fs-store "/tmp/kons-probe" :opts {:sync? false}))
                v     (<! (k/get store :probe nil {:sync? false}))]
            (is (= :datahike.index/persistent-set (:ns-kw v)) (str ":ns-kw = " (pr-str (:ns-kw v))))
            (is (= :db.type/long (:ns-kw2 v)) (str ":ns-kw2 = " (pr-str (:ns-kw2 v))))
            (is (= :write (:simple-kw v)) (str ":simple-kw = " (pr-str (:simple-kw v))))
            (is (= :x/y (get-in v [:nested :inner])) (str ":nested :inner = " (pr-str (get-in v [:nested :inner]))))
            (is (= [:a/b :c] (:vec v)) (str ":vec = " (pr-str (:vec v))))))
        (catch js/Error e
          (is false (str "xhost-fress-probe error: " (.-message e))))
        (finally
          (done))))))

(defn -main []
  (t/run-tests 'datahike.test.nodejs-test
               'datahike.test.cljs-pattern-scan-test
               'datahike.test.optimistic-test))
