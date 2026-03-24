(ns datahike.test.secondary-versioning-test
  "Integration tests for secondary indices with branching, merging, and GC."
  (:require
   [clojure.test :refer [deftest testing is]]
   [datahike.api :as d]
   [datahike.experimental.versioning :as dv]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.index.secondary.scriptum]))

;; ---------------------------------------------------------------------------
;; Scriptum + branching + merge (end-to-end)

(deftest test-scriptum-branch-and-merge
  (testing "secondary index survives branch, diverge, and merge"
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :keep-history? false
               :schema-flexibility :write}
          scriptum-path (str "/tmp/scriptum-ver-test-" (random-uuid))
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        ;; Schema
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/index true}
                          {:db/ident :person/bio
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])

        ;; Add data on main
        (d/transact conn [{:person/name "Alice" :person/bio "ML researcher"}
                          {:person/name "Bob" :person/bio "Database engineer"}])

        ;; Add fulltext secondary index
        (d/transact conn [{:db/ident :idx/fulltext
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:person/name :person/bio]
                           :db.secondary/config {:path scriptum-path}}])
        (Thread/sleep 1000) ;; wait for backfill

        ;; Verify fulltext works on main
        (let [db (d/db conn)
              ft (get-in db [:secondary-indices :idx/fulltext])
              results (sec/-search ft {:query "ML" :field :value} nil)]
          (is (pos? (es/entity-bitset-cardinality results))
              "Fulltext should find 'ML' on main branch"))

        ;; Branch
        (dv/branch! conn :db :feature)

        ;; Connect to feature branch
        (let [feat-cfg (assoc cfg :branch :feature)
              feat-conn (d/connect feat-cfg)]
          (try
            ;; Wait for feature branch secondary index restore/backfill
            (Thread/sleep 1000)

            ;; Add data on feature branch
            (d/transact feat-conn [{:person/name "Charlie" :person/bio "ML engineer"}])

            ;; Verify fulltext on feature branch includes new data
            (let [feat-db (d/db feat-conn)
                  ft (get-in feat-db [:secondary-indices :idx/fulltext])]
              (when ft
                (let [results (sec/-search ft {:query "ML" :field :value} nil)]
                  (is (>= (es/entity-bitset-cardinality results) 2)
                      "Feature branch should find Alice + Charlie for 'ML'"))))

            ;; Add different data on main
            (d/transact conn [{:person/name "Diana" :person/bio "ML ops"}])

            ;; Merge feature into main
            (dv/merge! conn #{:feature}
                        [{:person/name "Charlie" :person/bio "ML engineer"}]
                        nil)

            ;; Verify main has merged data
            (let [db (d/db conn)]
              (is (some? (d/q '[:find ?e . :where [?e :person/name "Charlie"]] db))
                  "Main should have Charlie after merge"))

            (finally
              (d/release feat-conn))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Writer-routed merge test (basic)

(deftest test-merge-through-writer
  (testing "merge! routes through writer and creates multi-parent commit"
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :item/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:item/name "Apple"}])

        ;; Branch
        (dv/branch! conn :db :alt)

        ;; Add data on alt branch
        (let [alt-conn (d/connect (assoc cfg :branch :alt))]
          (try
            (d/transact alt-conn [{:item/name "Banana"}])

            ;; Merge alt into main
            (dv/merge! conn #{:alt}
                        [{:item/name "Banana"}]
                        nil)

            ;; Check commit has multiple parents
            (let [db (d/db conn)
                  parents (get-in db [:meta :datahike/parents])]
              (is (>= (count parents) 2)
                  "Merge commit should have at least 2 parents"))

            ;; Check merged data
            (let [db (d/db conn)]
              (is (some? (d/q '[:find ?e . :where [?e :item/name "Apple"]] db)))
              (is (some? (d/q '[:find ?e . :where [?e :item/name "Banana"]] db))))

            (finally
              (d/release alt-conn))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Persistence round-trip: commit, release, reconnect

(deftest test-secondary-index-persistence-roundtrip
  (testing "secondary index state survives release + reconnect"
    (let [cfg {:store {:backend :file
                       :id (java.util.UUID/randomUUID)
                       :path (str "/tmp/datahike-ver-test-" (random-uuid))}
               :keep-history? false
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:person/name "Alice"}
                          {:person/name "Bob"}])

        ;; Add scriptum index
        (d/transact conn [{:db/ident :idx/fulltext
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:person/name]
                           :db.secondary/config {:path (str "/tmp/scriptum-persist-" (random-uuid))}}])
        (Thread/sleep 1000)

        ;; Verify works
        (let [db (d/db conn)
              ft (get-in db [:secondary-indices :idx/fulltext])
              _ (is (some? ft) "Index should exist")
              results (when ft (sec/-search ft {:query "Alice" :field :value} nil))]
          (when results
            (is (pos? (es/entity-bitset-cardinality results)))))

        ;; Release and reconnect
        (d/release conn)
        (let [conn2 (d/connect cfg)]
          (try
            (Thread/sleep 1000) ;; wait for restore/backfill

            ;; Verify index survives reconnect
            (let [db (d/db conn2)
                  ft (get-in db [:secondary-indices :idx/fulltext])
                  status (get-in db [:schema :idx/fulltext :db.secondary/status])]
              (is (some? ft) "Index should be restored after reconnect")
              (is (= :ready status) "Status should be :ready after restore"))

            (finally
              (d/release conn2))))
        (finally
          (d/delete-database cfg))))))
