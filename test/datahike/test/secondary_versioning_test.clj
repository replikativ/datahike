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
        ;; Schema + data
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/bio
                           :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:person/name "Alice" :person/bio "Machine learning researcher"}
                          {:person/name "Bob" :person/bio "Database engineer"}])

        ;; Add fulltext secondary index
        (d/transact conn [{:db/ident :idx/fulltext
                           :db.secondary/type :scriptum
                           :db.secondary/attrs [:person/name :person/bio]
                           :db.secondary/config {:path scriptum-path}}])
        (Thread/sleep 1500)

        ;; Step 1: Verify fulltext works on main
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
              results (sec/-search ft {:query "machine" :field :value} nil)]
          (is (= 1 (es/entity-bitset-cardinality results))
              "Main should find 1 'machine' result (Alice)"))

        ;; Step 2: Branch
        (dv/branch! conn :db :feature)
        (let [feat-conn (d/connect (assoc cfg :branch :feature))]
          (try
            (Thread/sleep 1500)

            ;; Feature branch should inherit parent's fulltext index
            (let [ft (get-in (d/db feat-conn) [:secondary-indices :idx/fulltext])]
              (is (some? ft) "Feature branch should have fulltext index")
              (when ft
                (let [results (sec/-search ft {:query "machine" :field :value} nil)]
                  (is (= 1 (es/entity-bitset-cardinality results))
                      "Feature should inherit Alice from main"))))

            ;; Step 3: Add data on feature branch
            (d/transact feat-conn [{:person/name "Charlie" :person/bio "Machine learning engineer"}])
            (let [ft (get-in (d/db feat-conn) [:secondary-indices :idx/fulltext])]
              (when ft
                (let [results (sec/-search ft {:query "machine" :field :value} nil)]
                  (is (= 2 (es/entity-bitset-cardinality results))
                      "Feature should find Alice + Charlie for 'machine'"))))

            ;; Step 4: Merge feature into main
            (dv/merge! conn #{:feature}
                       [{:person/name "Charlie" :person/bio "Machine learning engineer"}]
                       nil)
            (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])
                  results (sec/-search ft {:query "machine" :field :value} nil)]
              (is (= 2 (es/entity-bitset-cardinality results))
                  "Main after merge should find Alice + Charlie"))

            ;; Verify merge commit has multiple parents
            (let [parents (get-in (d/db conn) [:meta :datahike/parents])]
              (is (>= (count parents) 2)
                  "Merge commit should have at least 2 parents"))

            (finally
              (d/release feat-conn))))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Writer-routed merge test

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

        (dv/branch! conn :db :alt)
        (let [alt-conn (d/connect (assoc cfg :branch :alt))]
          (try
            (d/transact alt-conn [{:item/name "Banana"}])

            (dv/merge! conn #{:alt}
                       [{:item/name "Banana"}]
                       nil)

            (let [db (d/db conn)
                  parents (get-in db [:meta :datahike/parents])]
              (is (>= (count parents) 2)
                  "Merge commit should have at least 2 parents"))

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
          scriptum-path (str "/tmp/scriptum-persist-" (random-uuid))
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
                           :db.secondary/config {:path scriptum-path}}])
        (Thread/sleep 1500)

        ;; Verify works
        (let [ft (get-in (d/db conn) [:secondary-indices :idx/fulltext])]
          (is (some? ft) "Index should exist")
          (when ft
            (is (= :ready (get-in (d/db conn) [:schema :idx/fulltext :db.secondary/status])))))

        ;; Release and reconnect
        (d/release conn)
        (let [conn2 (d/connect cfg)]
          (try
            (Thread/sleep 1500)

            ;; Verify index survives reconnect
            (let [db (d/db conn2)
                  ft (get-in db [:secondary-indices :idx/fulltext])
                  status (get-in db [:schema :idx/fulltext :db.secondary/status])]
              (is (some? ft) "Index should be restored after reconnect")
              (is (= :ready status) "Status should be :ready after restore")
              (when ft
                (let [results (sec/-search ft {:query "Alice" :field :value} nil)]
                  (is (= 1 (es/entity-bitset-cardinality results))
                      "Restored index should find Alice"))))

            (finally
              (d/release conn2))))
        (finally
          (d/delete-database cfg))))))
