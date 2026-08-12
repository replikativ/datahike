(ns datahike.test.migrate-legacy-golden-test
  "A dump written by the RELEASED writer must still import.

   `migrate_test/legacy-cbor-import-test` also covers the legacy path, but it
   builds its fixture with `clj-cbor` inside the test — it SIMULATES the old
   writer rather than using it, so anything the released `export-db` did that
   the simulation does not reproduce would go unnoticed.

   `legacy-dump-0.7.cbor` is not simulated. It was produced by checking out
   `origin/main` and running its own `datahike.migrate/export-db` over a
   database covering every builtin value type this format can carry, plus a
   retraction and an update so the dump holds HISTORY rather than a snapshot.
   1414 bytes, 44 records.

   Regenerating it means checking out the released writer again, which is the
   point: the bytes cannot drift with the code that reads them."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.migrate.legacy :as mlegacy]
            [datahike.test.utils :as utils]))

(def ^:private fixture-resource "datahike/test/fixtures/legacy-dump-0.7.cbor")

(defn- teardown [conn]
  (let [cfg (:config @conn)]
    (d/release conn)
    (d/delete-database cfg)))

(defn- fixture-file
  "Copy the fixture out of the classpath to a real path.

   The legacy reader opens a FILE — it is reading dumps that only ever existed
   on disk — so a resource URL will not do."
  []
  (let [tmp (java.io.File/createTempFile "legacy-golden-" ".cbor")]
    (with-open [in (io/input-stream (io/resource fixture-resource))]
      (io/copy in tmp))
    (.deleteOnExit tmp)
    tmp))

(deftest the-released-writers-dump-still-reads
  (testing "the fixture is present and decodes to the record count it was
            written with — if this fails the fixture is damaged, and every
            assertion below would be testing nothing"
    (let [f (fixture-file)]
      (is (= 44 (mlegacy/count-records (str f))))
      (.delete f))))

(def ^:private always-present
  "The keys `import-db` documents as present whatever `source` turned out to be."
  #{:datom-count :tx-count :max-tx :verified? :verification :errors})

(deftest import-db-returns-one-shape-for-both-dump-formats
  (testing "it used to return the report map for a manifest-and-chunks dump and
            a datahike.db.TxReport for a legacy one — two TYPES from one
            function, while the docstring promised the map unconditionally.
            Asserted on both formats in one test, because the defect was
            precisely that each was fine when looked at alone."
    (let [legacy-f (fixture-file)
          lconn    (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                                    :schema-flexibility :write :keep-history? true})
          lrep     (m/import-db lconn (str legacy-f))

          dir      (str (System/getProperty "java.io.tmpdir")
                        "/dh-shape-" (java.util.UUID/randomUUID))
          sconn    (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                                    :schema-flexibility :write :keep-history? true})
          _        (d/transact sconn [{:db/ident :name :db/valueType :db.type/string
                                       :db/cardinality :db.cardinality/one
                                       :db/unique :db.unique/identity}])
          _        (d/transact sconn [{:name "x"}])
          _        (m/export-db sconn dir)
          tconn    (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                                    :schema-flexibility :write :keep-history? true})
          nrep     (m/import-db tconn dir)]

      (is (map? lrep) "a legacy import must return a map, not a TxReport")
      (is (map? nrep))
      (doseq [[label rep] [["legacy" lrep] ["manifest" nrep]]]
        (is (empty? (remove (set (keys rep)) always-present))
            (str label " is missing documented keys: "
                 (pr-str (vec (remove (set (keys rep)) always-present))))))

      (testing "and the legacy report says it was NOT verified, rather than
                manufacturing a check. Records in a legacy dump are not
                one-for-one with the datoms a correct import leaves — the
                import stamps its own :db/txInstant — so `records = datoms` is
                false for a faithful restore, and the expectation that would
                make it true is derived from the import's own behaviour."
        (is (nil? (:verified? lrep)))
        (is (= :unavailable (:status (:verification lrep))))
        (is (= :legacy-format (:reason (:verification lrep))))
        (testing "while a manifest-backed dump verifies for real"
          (is (true? (:verified? nrep)))
          (is (= :ok (:status (:verification nrep))))))

      (teardown lconn)
      (teardown sconn)
      (teardown tconn)
      (.delete legacy-f))))

(deftest the-released-writers-dump-still-imports
  (let [f    (fixture-file)
        conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                              :schema-flexibility :write
                              :keep-history? true
                              :attribute-refs? false})]
    (m/import-db conn (str f))

    (testing "values survive with their TYPES, which is what a codec change
              threatens — a dump that imported but narrowed a double or lost an
              instant's precision would satisfy a datom-count check"
      (let [alice (d/pull @conn '[*] [:name "Alice"])
            bob   (d/pull @conn '[*] [:name "Bob"])]
        (is (= 25 (:age alice)))
        (is (= 1.5 (:score alice)))
        (is (= true (:ok alice)))
        (is (= #inst "1999-01-02T03:04:05.678-00:00" (:born alice)))
        (is (= #uuid "11111111-2222-3333-4444-555555555555" (:uid alice)))
        (is (instance? Double (:score alice)) "a double must not arrive narrowed")
        (is (instance? java.util.Date (:born alice)))

        (is (= 2.25 (:score bob)))
        (is (= false (:ok bob)))
        (is (= #inst "2001-12-31T23:59:59.999-00:00" (:born bob)))
        (is (= #uuid "66666666-7777-8888-9999-000000000000" (:uid bob)))))

    (testing "the UPDATE landed — Bob was transacted at 30 and then 31, so a
              dump replayed in the wrong order would leave 30"
      (is (= 31 (:age (d/pull @conn '[*] [:name "Bob"])))))

    (testing "the RETRACTION landed. Alice was given :tags #{:a :b} and :b was
              retracted, so a dump that dropped retractions — or replayed them
              out of order — leaves both"
      (is (= #{:a} (set (:tags (d/pull @conn '[*] [:name "Alice"])))))
      (is (= #{:c} (set (:tags (d/pull @conn '[*] [:name "Bob"]))))))

    (testing "and HISTORY survives, not just the current value: the retracted
              tag is still visible historically, which is the whole reason the
              dump carries history at all"
      (let [historical (->> (d/datoms (d/history @conn) :eavt)
                            (filter #(= :tags (:a %)))
                            (map :v)
                            set)]
        (is (contains? historical :b)
            "the retracted :tags value must remain in history")))

    (teardown conn)
    (.delete f)))
