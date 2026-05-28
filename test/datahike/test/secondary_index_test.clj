(ns datahike.test.secondary-index-test
  "Tests for the pluggable secondary index infrastructure."
  (:require
   [clojure.test :refer [deftest testing is]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.writer :as writer]))

;; ---------------------------------------------------------------------------
;; EntityBitSet tests

(deftest test-entity-bitset-ops
  (testing "empty bitset"
    (let [bs (es/entity-bitset)]
      (is (zero? (es/entity-bitset-cardinality bs)))))

  (testing "add and contains"
    (let [bs (-> (es/entity-bitset)
                 (es/entity-bitset-add! 1)
                 (es/entity-bitset-add! 42)
                 (es/entity-bitset-add! 1000))]
      (is (= 3 (es/entity-bitset-cardinality bs)))
      (is (es/entity-bitset-contains? bs 1))
      (is (es/entity-bitset-contains? bs 42))
      (is (es/entity-bitset-contains? bs 1000))
      (is (not (es/entity-bitset-contains? bs 2)))))

  (testing "from-longs"
    (let [bs (es/entity-bitset-from-longs [10 20 30 40 50])]
      (is (= 5 (es/entity-bitset-cardinality bs)))
      (is (es/entity-bitset-contains? bs 30))
      (is (not (es/entity-bitset-contains? bs 25)))))

  (testing "AND (intersection)"
    (let [a (es/entity-bitset-from-longs [1 2 3 4 5])
          b (es/entity-bitset-from-longs [3 4 5 6 7])
          c (es/entity-bitset-and a b)]
      (is (= 3 (es/entity-bitset-cardinality c)))
      (is (= #{3 4 5} (set (es/entity-bitset-seq c))))))

  (testing "OR (union)"
    (let [a (es/entity-bitset-from-longs [1 2 3])
          b (es/entity-bitset-from-longs [3 4 5])
          c (es/entity-bitset-or a b)]
      (is (= 5 (es/entity-bitset-cardinality c)))
      (is (= #{1 2 3 4 5} (set (es/entity-bitset-seq c))))))

  (testing "ANDNOT (subtraction)"
    (let [a (es/entity-bitset-from-longs [1 2 3 4 5])
          b (es/entity-bitset-from-longs [3 4])
          c (es/entity-bitset-andnot a b)]
      (is (= 3 (es/entity-bitset-cardinality c)))
      (is (= #{1 2 5} (set (es/entity-bitset-seq c)))))))

;; ---------------------------------------------------------------------------
;; Registry tests

(deftest test-secondary-index-registry
  (testing "register and create"
    (let [factory-called (atom false)]
      (sec/register-index-type!
       :test/dummy
       (fn [config _db]
         (reset! factory-called true)
         (reify sec/ISecondaryIndex
           (-search [_ _ _] nil)
           (-estimate [_ _] 0)
           (-can-order? [_ _ _] false)
           (-slice-ordered [_ _ _ _ _ _] nil)
           (-indexed-attrs [_] (:attrs config))
           (-transact [this _] this))))
      (is (contains? (sec/registered-types) :test/dummy))
      (let [idx (sec/create-index :test/dummy {:attrs #{:a :b}} nil)]
        (is @factory-called)
        (is (= #{:a :b} (sec/-indexed-attrs idx))))))

  (testing "unknown type throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown secondary index type"
                          (sec/create-index :nonexistent/type {} nil)))))

;; ---------------------------------------------------------------------------
;; Schema rschema mapping tests

(deftest test-rschema-secondary-mapping
  (testing "secondary index attrs appear in rschema"
    (let [schema {:person/name {:db/index true}
                  :person/bio {}
                  :idx/person-search {:db.secondary/type :scriptum
                                      :db.secondary/attrs [:person/name :person/bio]}}
          db (db/empty-db schema)]
      (is (= {:person/name #{:idx/person-search}
              :person/bio #{:idx/person-search}}
             (get-in db [:rschema :db.secondary/index])))))

  (testing "M-N mapping: attribute in multiple indices"
    (let [schema {:person/name {:db/index true}
                  :person/salary {:db/index true}
                  :person/bio {}
                  :idx/search {:db.secondary/type :scriptum
                               :db.secondary/attrs [:person/name :person/bio]}
                  :idx/analytics {:db.secondary/type :stratum
                                  :db.secondary/attrs [:person/name :person/salary]}}
          db (db/empty-db schema)]
      (is (= #{:idx/search :idx/analytics}
             (get-in db [:rschema :db.secondary/index :person/name])))
      (is (= #{:idx/search}
             (get-in db [:rschema :db.secondary/index :person/bio])))
      (is (= #{:idx/analytics}
             (get-in db [:rschema :db.secondary/index :person/salary])))))

  (testing "no secondary indices = empty map"
    (let [schema {:person/name {:db/index true}}
          db (db/empty-db schema)]
      (is (= {} (get-in db [:rschema :db.secondary/index]))))))

;; ---------------------------------------------------------------------------
;; In-transaction maintenance test

(deftest test-secondary-index-transact
  (testing "secondary index receives datoms during transaction"
    (let [received (atom [])
          ;; Register a test index type that records all transacted datoms
          _ (sec/register-index-type!
             :test/recorder
             (fn [config _db]
               (let [state (atom [])]
                 (reify sec/ISecondaryIndex
                   (-search [_ _ _] nil)
                   (-estimate [_ _] 0)
                   (-can-order? [_ _ _] false)
                   (-slice-ordered [_ _ _ _ _ _] nil)
                   (-indexed-attrs [_] (set (:attrs config)))
                   (-transact [this tx-report]
                     (swap! received conj tx-report)
                     this)))))
          schema {:person/name {:db/index true}
                  :person/age {:db/index true}
                  :idx/test {:db.secondary/type :test/recorder
                             :db.secondary/attrs [:person/name]}}
          db (db/empty-db schema)
          ;; Manually install the secondary index into the db
          idx (sec/create-index :test/recorder {:attrs [:person/name]} db)
          db (assoc db :secondary-indices {:idx/test idx})
          ;; Transact
          db2 (d/db-with db [{:db/id 1 :person/name "Alice" :person/age 30}
                             {:db/id 2 :person/name "Bob" :person/age 25}])]
      ;; :person/name datoms should be recorded, :person/age should not
      (is (= 2 (count @received)))
      (is (every? :added? @received))
      (is (every? #(some? (:datom %)) @received)))))

;; ---------------------------------------------------------------------------
;; Purge propagation
;;
;; Regression test for the GDPR-relevant invariant: when a datom is purged,
;; the secondary index covering its attribute must receive a retraction
;; (-transact with :added? false). Prior to the with-temporal-datom fix,
;; purge bypassed update-secondary-indices entirely, so secondary indices
;; silently retained purged datoms.

(deftest test-purge-propagates-to-secondary
  (testing "purge routes a retraction event to covering secondary indices"
    (let [received (atom [])
          _ (sec/register-index-type!
             :test/recorder-purge
             (fn [config _db]
               (reify sec/ISecondaryIndex
                 (-search [_ _ _] nil)
                 (-estimate [_ _] 0)
                 (-can-order? [_ _ _] false)
                 (-slice-ordered [_ _ _ _ _ _] nil)
                 (-indexed-attrs [_] (set (:attrs config)))
                 (-transact [this tx-report]
                   (swap! received conj tx-report)
                   this))))
          cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :keep-history? true
               :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (try
        (d/transact conn [{:db/ident :person/name
                           :db/valueType :db.type/string
                           :db/unique :db.unique/identity
                           :db/index true
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :person/age
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        ;; Install recorder over :person/name BEFORE adding data, so the
        ;; recorder sees the live add events directly (no async backfill).
        (d/transact conn [{:db/ident :idx/recorder
                           :db.secondary/type :test/recorder-purge
                           :db.secondary/attrs [:person/name]}])
        (d/transact conn [{:person/name "Alice" :person/age 30}
                          {:person/name "Bob" :person/age 25}])
        ;; Sanity: two adds for :person/name, none for :person/age (not covered)
        (is (= 2 (count (filter :added? @received)))
            (str "expected 2 add events on :person/name, got: " (pr-str @received)))

        (reset! received [])
        ;; Purge Alice's :person/name datom.
        (d/transact conn [[:db/purge [:person/name "Alice"] :person/name "Alice"]])

        (let [retracts (remove :added? @received)]
          (is (= 1 (count retracts))
              (str "expected exactly 1 retraction event after purge, "
                   "got: " (pr-str @received)))
          (is (= "Alice" (.-v ^datahike.datom.Datom (:datom (first retracts))))
              "the retracted datom should be Alice's :person/name"))

        (testing ":db.purge/entity also propagates"
          (reset! received [])
          (d/transact conn [[:db.purge/entity [:person/name "Bob"]]])
          (let [retracts (remove :added? @received)]
            (is (= 1 (count retracts))
                (str "expected exactly 1 retraction event after :db.purge/entity, "
                     "got: " (pr-str @received)))
            (is (= "Bob" (.-v ^datahike.datom.Datom (:datom (first retracts))))
                "the retracted datom should be Bob's :person/name")))

        (finally
          (d/release conn)
          (d/delete-database cfg))))))

;; ---------------------------------------------------------------------------
;; Backfill dispatch — regression for double-counting
;;
;; A secondary index is backfilled asynchronously after the tx that creates it
;; (status :building). The backfill skips datoms with tx > building-since-tx so
;; live transactions applied during the build are not re-delivered. The writer
;; must therefore dispatch the backfill *exactly once* — on the tx that first
;; flips the index to :building. If a later tx (applied while the index is still
;; building) also dispatched a backfill, that second build could run after the
;; first one's install-secondary-index! has dropped :building-since-tx, losing
;; the guard and re-delivering post-creation datoms that were already applied
;; live — counting them twice in the index.

(deftest test-backfill-dispatched-once
  (testing "backfill is dispatched only when an index transitions into :building"
    (let [detect @#'writer/detect-new-building-indices
          building {:db.secondary/type :scriptum :db.secondary/status :building}
          ready    {:db.secondary/type :scriptum :db.secondary/status :ready}]
      (testing "nil -> :building (index just created): dispatch"
        (is (= [:idx/a]
               (vec (detect {:db-before {:schema {}}
                             :db-after  {:schema {:idx/a building}}})))))
      (testing ":building -> :building (later tx during backfill): no re-dispatch"
        (is (empty? (detect {:db-before {:schema {:idx/a building}}
                             :db-after  {:schema {:idx/a building}}}))))
      (testing ":ready -> :building (index re-enabled): dispatch"
        (is (= [:idx/a]
               (vec (detect {:db-before {:schema {:idx/a ready}}
                             :db-after  {:schema {:idx/a building}}}))))))))
