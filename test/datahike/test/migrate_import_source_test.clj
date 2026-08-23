(ns datahike.test.migrate-import-source-test
  "The PUBLIC door onto the record seam: `import-source` and `records->chunk-src`.

   `migrate-source-test` already covers the seam itself — it reaches `run-import`
   through `#'` because there was no public entry, and pins `source-meta`, the
   eid policies, `:merge?`, the batcher hazards and chunk-descriptor opacity.
   None of that is repeated here. What is new, and untested before this, is that
   a caller OUTSIDE the namespace can drive the same importer, plus the one
   helper that entry point ships with.

   Two things below exist because they broke while being written, and neither is
   visible from the sync JVM path a casual test would take:

     * `:read` is `<?-`'d, so under `:sync? false` it must return a CHANNEL.
       Returning the chunk directly raised \"No implementation of method: :take!
       of protocol: ReadPort\" — and `default-sync?` is FALSE on ClojureScript,
       so that was every Node caller by default rather than an edge case.
     * a chunk may hold SEVERAL whole transactions but never half of one, since
       the batcher's flush rule keys on `t` changing."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]
            [clojure.core.async :as a]
            [clojure.set]))

(def ^:private t1 536870913)
(def ^:private t2 536870914)
(def ^:private t3 536870915)

(defn- records
  "Transaction-grouped, `:db/txInstant` first, schema before the data using it —
   the order the contract requires. Carries a cardinality-one overwrite so the
   history assertion is not vacuous."
  []
  [[t1 :db/txInstant #inst "2021-01-01" t1 true]
   [100 :db/ident :person/name t1 true]
   [100 :db/valueType :db.type/string t1 true]
   [100 :db/cardinality :db.cardinality/one t1 true]
   [t2 :db/txInstant #inst "2021-02-01" t2 true]
   [200 :person/name "Ann" t2 true]
   [201 :person/name "Bob" t2 true]
   [t3 :db/txInstant #inst "2021-03-01" t3 true]
   [200 :person/name "Anna" t3 true]
   [200 :person/name "Ann" t3 false]])

(defn- fresh-conn []
  (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                   :keep-history? true
                   :schema-flexibility :write}))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(deftest a-caller-outside-the-namespace-can-import
  (testing "the whole point of the entry point: records, history and the SOURCE's
            transaction times all land, with no manifest, no checksums, no blobs"
    (let [conn (fresh-conn)
          recs (records)
          report (m/import-source conn (m/records->chunk-src recs 3)
                                  {:source-meta {:history? true
                                                 :expected-count (count recs)
                                                 :max-tx t3}})]
      (is (true? (:verified? report)))
      (is (= [] (:errors report)))
      (is (= {:status :ok :expected (count recs) :actual (count recs)}
             (:verification report))
          "the count check ran against :expected-count from source-meta")
      (is (= 0 (:max-tx-drift report)) "and the drift check against :max-tx")
      (is (= 3 (:tx-count report)))
      (is (= #{"Anna" "Bob"}
             (into #{} (map first) (d/q '[:find ?n :where [?e :person/name ?n]] @conn)))
          "the card-one overwrite applied")
      (is (= #{["Ann" true] ["Ann" false] ["Anna" true] ["Bob" true]}
             (into #{} (map (juxt first second))
                   (d/q '[:find ?n ?op :where [?e :person/name ?n _ ?op]] (d/history @conn))))
          "history: assert, retract and re-assert all present")
      (is (= [#inst "2021-01-01" #inst "2021-02-01" #inst "2021-03-01"]
             (sort (map first (d/q '[:find ?i :where [_ :db/txInstant ?i]] @conn))))
          "the SOURCE's transaction times, not the import's")
      (teardown conn))))

(deftest the-async-path-works
  (testing "`:read` is `<?-`'d, so under `:sync? false` it must return a channel.
            `default-sync?` is FALSE on ClojureScript, so a `:read` returning the
            chunk directly broke every Node caller by DEFAULT. Driven here from
            the JVM with an explicit `{:sync? false}`, which is the same code."
    (let [conn (fresh-conn)
          result (a/<!! (m/import-source conn (m/records->chunk-src (records) 3)
                                         {:sync? false :verify? false}))]
      (is (not (instance? Throwable result))
          (str "async import threw: "
               (when (instance? Throwable result) (ex-message result))))
      (is (= 3 (:tx-count result)))
      (is (= #{"Anna" "Bob"}
             (into #{} (map first) (d/q '[:find ?n :where [?e :person/name ?n]] @conn)))
          "and produces the database the sync path does")
      (teardown conn))))

(deftest a-chunk-never-splits-a-transaction
  (testing "`records->chunk-src`'s size is a MINIMUM: a chunk grows past it until
            `t` changes. A chunk may therefore hold SEVERAL whole transactions —
            asserting one-per-chunk would be a stronger claim than the helper
            makes — but never half of one, because the batcher flushes on `t`
            changing and a split transaction would hand it a partial one."
    (let [recs [[1 :a 1 100 true] [2 :a 2 100 true] [3 :a 3 100 true]
                [4 :a 4 101 true]
                [5 :a 5 102 true] [6 :a 6 102 true]]
          src (m/records->chunk-src recs 2)
          chunks (:chunks src)
          ts (mapv (fn [c] (into #{} (map #(nth % 3)) c)) chunks)]
      (is (< 1 (count chunks)) "precondition: it actually chunked")
      (is (= (reduce + (map count ts)) (count (apply clojure.set/union ts)))
          (str "no `t` appears in two chunks, got "
               (pr-str (mapv (fn [c] (mapv #(nth % 3) c)) chunks))))
      (is (= recs (vec (mapcat #((:read src) % {:sync? true}) chunks)))
          "reading every chunk in order reproduces the input exactly")
      (is (= recs (vec (mapcat #((:read src) % {:sync? true}) chunks)))
          ":read is RE-ENTRANT — verify and the index build both read twice"))))

(deftest an-undeclared-attribute-is-named-not-a-nil-datom
  (testing "Contract item 4 — a source emits an attribute's schema datoms before
            the data using it. Only an :attribute-refs? database can DETECT the
            violation: there `a` is the attribute's entity id, and `-ref-for`
            answers nil for an ident that was never installed. That nil used to
            become the datom's attribute and survive the write, detonating later
            in the index comparator as a bare NullPointerException naming neither
            the attribute nor the record.

            Reachable from a real Datomic source, which is why it is not
            hypothetical: `migrate.datomic` resolves idents against the CURRENT
            database, so every historical datom of a RENAMED attribute already
            carries the final ident while the `:db/ident` records still replay
            the rename — the data arrives before the name it is labelled with
            exists."
    (let [recs [[t1 :db/txInstant #inst "2021-01-01" t1 true]
                [100 :db/ident :person/name t1 true]
                [100 :db/valueType :db.type/string t1 true]
                [100 :db/cardinality :db.cardinality/one t1 true]
                [t2 :db/txInstant #inst "2021-02-01" t2 true]
                ;; never declared
                [200 :person/nickname "Annie" t2 true]]]
      (testing "attribute-refs: refused, and the attribute is named"
        (let [conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                                    :keep-history? true
                                    :schema-flexibility :write
                                    :attribute-refs? true})]
          (try
            (let [ex (try (m/import-source conn (m/records->chunk-src recs)
                                           {:sync? true :verify? false :schema {}})
                          nil
                          (catch Exception e
                            (loop [x e, found (ex-data e)]
                              (if-let [c (.getCause x)]
                                (recur c (or (ex-data c) found))
                                (or (ex-data x) found)))))]
              (is (some? ex) "the import must refuse rather than write a nil attribute")
              (is (= :transact/unknown-attribute (:error ex)))
              (is (= :person/nickname (:attribute ex))
                  "and say WHICH attribute, which the NullPointerException never did"))
            (finally (teardown conn)))))
      (testing "without attribute-refs the keyword IS the attribute, so it stores"
        (let [conn (fresh-conn)]
          (try
            (m/import-source conn (m/records->chunk-src recs)
                             {:sync? true :verify? false :schema {}})
            (is (= #{["Annie"]}
                   (into #{} (d/q '[:find ?v :where [?e :person/nickname ?v]] @conn)))
                "unchanged behaviour — this path has no entity to resolve")
            (finally (teardown conn))))))))
