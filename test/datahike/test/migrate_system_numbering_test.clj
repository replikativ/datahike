(ns datahike.test.migrate-system-numbering-test
  "An attribute-refs source whose system entities are numbered unlike the
   target's. Every database created before the system schema last grew is one:
   its user entities start where the current system schema still runs, and
   importing it used to map those user entities onto the target's system
   entities.

   The fixtures are REAL: an attribute-refs file store created with the code of
   c81aeed3 (#837, the last commit before #828 grew the system schema past 38
   entities), then exported by the current exporter (the directory dump) and by
   that commit's own exporter (the legacy single file).

   :person/name   string, unique identity   :person/friend  ref, card-many
   :person/kind   ref to a SYSTEM entity     :person/tags    keyword, card-many
   :person/age    long, card-one -> card-many in history
   plus tx-meta, retractions, a retractEntity and two tuple attributes."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.manifest :as mman]))

(def ^:private pre-828-dump "test/datahike/test/fixtures/attribute-refs-pre-828-dump")
(def ^:private pre-828-legacy "test/datahike/test/fixtures/attribute-refs-pre-828-legacy.cbor")

(def ^:private t1 536870913)
(def ^:private t2 536870914)

(defn- fresh-conn []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :attribute-refs? true
             :keep-history? true
             :schema-flexibility :write}]
    (d/create-database cfg)
    (d/connect cfg)))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(def ^:private shift
  "The source puts its system entities 100 above the target's, so its user
   entities 1-3 sit exactly on the target's :db/ident, :db/valueType and
   :db/cardinality."
  100)

(defn- source
  "Records and source meta for that source, as `export-db` writes them: schema
   values (`:db/valueType`, …) are the source's own system ids, and a user ref
   to a system entity is a `SysRef`."
  [target-db]
  (let [sys (mman/system-idents target-db)
        sid (let [by-ident (into {} (map (fn [[e i]] [i (+ e shift)])) sys)]
              #(get by-ident %))
        recs [[t1 :db/txInstant #inst "2021-01-01" t1 true]
              [1 :db/ident :person/name t1 true]
              [1 :db/valueType (sid :db.type/string) t1 true]
              [1 :db/cardinality (sid :db.cardinality/one) t1 true]
              [2 :db/ident :person/friend t1 true]
              [2 :db/valueType (sid :db.type/ref) t1 true]
              [2 :db/cardinality (sid :db.cardinality/one) t1 true]
              [5 :db/ident :person/kind t1 true]
              [5 :db/valueType (sid :db.type/ref) t1 true]
              [5 :db/cardinality (sid :db.cardinality/one) t1 true]
              [t2 :db/txInstant #inst "2021-02-01" t2 true]
              [3 :person/name "Ann" t2 true]
              [4 :person/name "Bob" t2 true]
              [4 :person/friend 3 t2 true]
              [4 :person/kind (mcbor/->SysRef :db.type/string) t2 true]]]
    {:records recs
     :source-meta {:expected-count (count recs)
                   :system-idents (into {} (map (fn [[e i]] [(+ e shift) i])) sys)}}))

(defn- imported-correctly? [conn]
  (let [db @conn]
    (is (= #{["Ann"] ["Bob"]} (d/q '[:find ?n :where [_ :person/name ?n]] db)))
    (is (= #{["Bob" "Ann"]}
           (d/q '[:find ?n ?f :where [?e :person/friend ?x] [?e :person/name ?n]
                  [?x :person/name ?f]] db))
        "the ref between user entities still resolves")
    (is (= :db.type/ref (get-in (d/schema db) [:person/friend :db/valueType]))
        "a schema value names the target's own system entity")
    (is (= #{[:db.type/string]}
           (d/q '[:find ?i :where [?e :person/name "Bob"] [?e :person/kind ?k]
                  [?k :db/ident ?i]] db))
        "so does a user ref to a system entity")
    (is (= :db/ident (:db/ident (d/pull db [:db/ident] 1)))
        "the target's system entity 1 is untouched")))

(deftest a-renumbered-source-streams-into-fresh-ids
  (let [conn (fresh-conn)]
    (try
      (let [{:keys [records source-meta]} (source @conn)
            report (m/import-source conn (m/records->chunk-src records 4)
                                    {:source-meta source-meta})]
        (is (true? (:verified? report)))
        (imported-correctly? conn))
      (finally (teardown conn)))))

(deftest preserve-is-refused-for-a-renumbered-source
  (let [conn (fresh-conn)]
    (try
      (let [{:keys [records source-meta]} (source @conn)]
        (is (= :import/preserve-renumbered-system
               (try (m/import-source conn (m/records->chunk-src records 4)
                                     {:source-meta source-meta :eids :preserve})
                    nil
                    (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
        (is (= #{} (d/q '[:find ?n :where [_ :person/name ?n]] @conn))
            "refused before anything was written"))
      (finally (teardown conn)))))

(defn- pre-828-imported-correctly? [conn]
  (let [db @conn]
    (is (= #{["Ann"] ["Bob"] ["Dee"]} (d/q '[:find ?n :where [_ :person/name ?n]] db)))
    (is (= #{["Ann" :db.type/string] ["Bob" :db.cardinality/many] ["Dee" :db.type/tuple]}
           (d/q '[:find ?n ?i :where [?e :person/name ?n] [?e :person/kind ?k]
                  [?k :db/ident ?i]] db))
        "refs to system entities name the target's own")
    (is (= #{["Bob" "Ann"] ["Cy" "Ann"] ["Cy" "Bob"]}
           (d/q '[:find ?n ?f :where [?e :person/friend ?x] [?e :person/name ?n]
                  [?x :person/name ?f]] (d/history db)))
        "refs between user entities, all later retracted, in history")
    (is (= :db.type/keyword (get-in (d/schema db) [:person/tags :db/valueType])))
    (is (= :db.valid/from (:db/ident (d/pull db [:db/ident] 39)))
        "entity 39 — the source's first user entity — is still the target's own")))

(deftest a-pre-828-dump-imports
  (doseq [eids [:allocate :offset]]
    (let [conn (fresh-conn)]
      (try
        (is (true? (:verified? (m/import-db conn pre-828-dump {:eids eids}))) (str eids))
        (pre-828-imported-correctly? conn)
        (finally (teardown conn))))))

(deftest preserve-refuses-a-pre-828-dump-before-writing
  (let [conn (fresh-conn)
        before (count (d/datoms @conn :eavt))]
    (try
      (is (= :import/preserve-renumbered-system
             (try (m/import-db conn pre-828-dump {:eids :preserve}) nil
                  (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
      (is (= before (count (d/datoms @conn :eavt))))
      (finally (teardown conn)))))

(deftest a-pre-828-legacy-dump-says-what-to-do
  (testing "the single-file format records no numbering, so it cannot be
            translated; the error says to export again"
    (let [conn (fresh-conn)]
      (try
        (let [e (try (m/import-db conn pre-828-legacy) nil
                     (catch clojure.lang.ExceptionInfo e e))]
          (is (some #(= :import/legacy-system-overlap (:error (ex-data %)))
                    (take-while some? (iterate ex-cause e)))
              (pr-str (ex-message e))))
        (finally (teardown conn))))))

(deftest an-undeclared-system-id-is-an-error-not-a-target-id
  (testing "a schema value the source's :system-idents does not cover would
            otherwise land as a target id and verify"
    (let [conn (fresh-conn)]
      (try
        (let [{:keys [records source-meta]} (source @conn)
              ;; drop :db.type/string from the declared numbering
              meta' (update source-meta :system-idents
                            #(into {} (remove (fn [[_ i]] (= i :db.type/string))) %))]
          (is (= :import/unknown-system-entity
                 (try (m/import-source conn (m/records->chunk-src records 4)
                                       {:source-meta meta'})
                      nil
                      (catch clojure.lang.ExceptionInfo e
                        (some #(:error (ex-data %))
                              (filter #(= :import/unknown-system-entity (:error (ex-data %)))
                                      (take-while some? (iterate ex-cause e)))))))))
        (finally (teardown conn))))))
