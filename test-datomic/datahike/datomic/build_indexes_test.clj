(ns datahike.datomic.build-indexes-test
  "`import-from-datomic! {:build-indexes? true}` end to end, against `datomic:mem`.

   The bulk path needs what a Datomic source did not supply until now: an
   ident-keyed `:schema` to tell ref values from numbers, and FINAL entity ids
   before the sort. `partition-mapping` supplies the second without a
   per-entity map. The fixture has the shapes that break each: Datomic's own
   bootstrap entities, a custom partition, enum refs, a ref FROM a transaction
   to an entity, a ref TO a transaction, card-one overwrites, card-many
   retractions and a retracted entity."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.edn :as edn]
            [datomic.api :as dt]
            [datahike.api :as d]
            [datahike.migrate.datomic :as dtm]
            [datahike.constants :as const]))

(def ^:dynamic *dtm* nil)
(def ^:dynamic *facts* nil)

(defn- fixture! [uri]
  (dt/create-database uri)
  (let [c (dt/connect uri)
        tx! (fn [inst data] @(dt/transact c (into [[:db/add "datomic.tx" :db/txInstant inst]] data)))]
    (tx! #inst "2021-01-01"
         [{:db/ident :p/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
           :db/unique :db.unique/identity}
          {:db/ident :p/age :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
          {:db/ident :p/pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
          {:db/ident :p/tag :db/valueType :db.type/string :db/cardinality :db.cardinality/many}
          {:db/ident :p/color :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
          {:db/ident :audit/by :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
          {:db/ident :audit/tx :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
          {:db/id "tenant" :db/ident :part/tenant :db.install/_partition :db.part/db}])
    (tx! #inst "2021-01-02" [{:db/ident :color/red} {:db/ident :color/blue}])
    (let [r (tx! #inst "2021-02-01"
                 [{:db/id "a" :p/name "Ann" :p/age 30 :p/tag ["x" "y"] :p/color :color/red}
                  {:db/id (dt/tempid :part/tenant) :p/name "Tia" :p/age 22 :p/pal "a"}
                  {:db/id "b" :p/name "Bob" :p/age 40}])
          ann (get-in r [:tempids "a"])
          r2 (tx! #inst "2021-03-01" [[:db/add ann :p/age 31]
                                      [:db/add "datomic.tx" :audit/by ann]])
          tx2 (dt/t->tx (dt/basis-t (:db-after r2)))]
      (tx! #inst "2021-04-01" [[:db/retract ann :p/tag "y"]
                               {:p/name "Cid" :audit/tx tx2}])
      (tx! #inst "2021-05-01" [[:db/retractEntity [:p/name "Bob"]]])
      {:conn c :audited-t (dt/tx->t tx2)})))

(use-fixtures :each
  (fn [f]
    (let [uri (str "datomic:mem://dh-bulk-" (rand-int 1000000))
          facts (fixture! uri)]
      (binding [*dtm* (:conn facts) *facts* facts]
        (try (f) (finally (dt/release *dtm*) (dt/delete-database uri)))))))

(defn- fresh []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :keep-history? true :schema-flexibility :write
             :index :datahike.index/persistent-set}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- facts
  "What an import must get right, independent of how it numbered transactions."
  [db]
  {:people (d/q '[:find ?n ?a :where [?e :p/name ?n] [?e :p/age ?a]] db)
   :tags (d/q '[:find ?n ?t :where [?e :p/name ?n] [?e :p/tag ?t]] db)
   :pals (d/q '[:find ?n ?m :where [?e :p/pal ?p] [?e :p/name ?n] [?p :p/name ?m]] db)
   :colors (d/q '[:find ?n ?c :where [?e :p/color ?x] [?x :db/ident ?c] [?e :p/name ?n]] db)
   :audited-by (d/q '[:find ?n :where [?tx :audit/by ?e] [?e :p/name ?n]] db)
   ;; the ref TO a transaction lands on that transaction's entity
   :audit-tx (d/q '[:find ?n ?t :where [?e :audit/tx ?tx] [?tx :datomic/t ?t] [?e :p/name ?n]] db)
   :history (count (d/datoms (d/history db) :eavt))
   :eids (d/q '[:find ?e ?n :where [?e :p/name ?n]] (d/history db))})

(deftest the-bulk-path-imports-a-datomic-database
  (let [bulk (fresh)
        stream (fresh)
        pm (dtm/partition-mapping *dtm*)]
    (try
      (let [rb (dtm/import-from-datomic! bulk *dtm* {:build-indexes? true})
            rs (dtm/import-from-datomic! stream *dtm* {:eids pm})
            fb (facts @bulk)
            fs (facts @stream)]
        (is (true? (:verified? rb)) "the bulk import verifies against the source count")
        (is (true? (:build-indexes? rb)))
        (is (true? (:verified? rs)))
        (is (= #{["Ann" 31] ["Tia" 22]} (:people fb))
            "values, after the overwrite and the retracted entity")
        (is (= (dissoc fs :eids) (dissoc fb :eids))
            "the same values, refs and history as the streaming import")
        (is (= (:eids fs) (:eids fb)) "and the same entity ids: one mapping, two paths")
        (is (= #{["Cid" (:audited-t *facts*)]} (:audit-tx fb))
            "a ref TO a transaction names that transaction")
        (is (= #{["Ann"]} (:audited-by fb)) "a ref FROM a transaction names its entity")
        (is (every? (fn [d] (<= 0 (:e d))) (d/datoms (d/history @bulk) :eavt))
            "no negative entity: the provenance schema's -1/-2 are mapped")
        (is (< (:max-eid @bulk) const/tx0)))
      (finally (teardown bulk) (teardown stream)))))

(deftest a-ref-to-a-transaction-on-the-streaming-default
  (testing "`:allocate` (the streaming default) used to remap a tx ref as an
            entity id and allocate a phantom for it."
    (let [conn (fresh)]
      (try
        (dtm/import-from-datomic! conn *dtm*)
        (is (= #{["Cid" (:audited-t *facts*)]} (:audit-tx (facts @conn))))
        (finally (teardown conn))))))

(deftest a-partition-mapping-round-trips-through-edn
  (let [pm (dtm/partition-mapping *dtm*)
        params (dtm/mapping-params pm)
        again (dtm/params->mapping (edn/read-string (pr-str params)))
        ids (map :e (take 200 (dt/datoms (dt/history (dt/db *dtm*)) :eavt)))]
    (is (= params (dtm/mapping-params again)))
    (is (= (map #(get pm %) ids) (map #(get again %) ids)) "the same ids, rebuilt")
    (is (< 1 (count (:regions params))) "db, user and the custom partition")
    (let [es (->> (dt/datoms (dt/history (dt/db *dtm*)) :eavt)
                  (map :e) distinct
                  ;; transaction entities are the import's own `:tids`, not the mapping's
                  (remove #(= 3 (bit-shift-right (long %) 42))))]
      (is (apply distinct? (map #(get pm %) es)) "injective over the source's entities"))))

(deftest a-partition-mapping-refuses-what-it-was-not-built-for
  (let [pm (dtm/partition-mapping *dtm*)
        {:keys [regions]} (dtm/mapping-params pm)
        {:keys [min size]} (last regions)]
    (is (= :datomic/eid-outside-mapping
           (try (get pm (+ (long min) (long size))) nil (catch Exception e (:error (ex-data e)))))
        "past a region's end")
    (is (= :datomic/eid-outside-mapping
           (try (get pm (bit-shift-left 77 42)) nil (catch Exception e (:error (ex-data e)))))
        "an unknown partition")
    (is (nil? (get pm (inc const/tx0))) "datahike tx ids are not its business")
    (is (= :datomic/partition-mapping-exceeds-tx0
           (try (dtm/partition-mapping *dtm* {:base (- const/tx0 10)}) nil
                (catch Exception e (:error (ex-data e))))))))

(deftest a-mapping-that-overlaps-the-target-is-refused
  (let [conn (fresh)]
    (try
      (is (= :datomic/partition-mapping-overlaps-target
             (try (dtm/import-from-datomic! conn *dtm*
                                            {:build-indexes? true
                                             :eids (dtm/partition-mapping *dtm* {:base 0})})
                  nil
                  (catch Exception e (:error (ex-data e))))))
      (finally (teardown conn)))))
