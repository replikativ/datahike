(ns datahike.test.schema-meta-durability-test
  "Every commit record names its schema through `:schema-meta-key`, an
   out-of-line content-addressed blob. If that blob is not in the store, the
   database opens without a schema at all — `stored->db` used to hand back
   `:schema nil` / `:rschema nil` and the first query threw

     NullPointerException: Cannot invoke \"clojure.lang.IFn.invoke(Object)\"
     because \"this.rschema\" is null

   from `db/_attrs_by`, with nothing pointing at the cause. Worse, it is
   invisible while the writing process lives: the in-memory read cache answers,
   so the damage only surfaces on a cold JVM — by which time every commit since
   names the same missing key.

   Writing the blob used to be gated on a process-local cache that was marked
   BEFORE the write, keyed by the store's `:id` rather than by the store. These
   are the paths that produced a dangling key, each one reproduced against the
   old gate first."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.writing :as dw]
            [datahike.schema-cache :as sc]
            [datahike.gc :as gc]
            [konserve.core :as k]
            [superv.async :refer [<?? S]]
            [clojure.core.cache.wrapped :as cw]))

(def ^:private tmp (System/getProperty "java.io.tmpdir"))

(defn- cfg [name id]
  {:store {:backend :file :path (str tmp "/" name) :id id}
   :keep-history? true
   :schema-flexibility :write})

(defn- head [conn]
  (k/get (:store @conn) :db nil {:sync? true}))

(defn- schema-meta-on-disk?
  "Is the blob the CURRENT head names actually in the store?"
  [conn]
  (let [smk (:schema-meta-key (head conn))]
    [smk (k/exists? (:store @conn) smk {:sync? true})]))

(defn- cold!
  "Simulate a JVM restart without leaving the process: drop the only two pieces
   of state that could mask a missing blob. The read cache is a global, and the
   durability proof lives on the store, which a reconnect rebuilds anyway."
  []
  (alter-var-root #'sc/schema-meta-cache
                  (constantly (cw/lru-cache-factory {} :threshold 1024))))

(defn- fresh! [config]
  (when (d/database-exists? config) (d/delete-database config))
  (d/create-database config))

(def ^:private attr-x
  {:db/ident :x :db/valueType :db.type/long :db/cardinality :db.cardinality/one})

(def ^:private attr-y
  {:db/ident :y :db/valueType :db.type/long :db/cardinality :db.cardinality/one})

(deftest failed-commit-does-not-strand-later-commits
  (testing "A commit that throws AFTER `db->stored` used to record the blob as
            written and then not write it. The writer shuts down, but the claim
            outlived the connection, so the supervisor's reconnect committed a
            head naming a key nothing had stored. Proof is now only recorded
            after the write is awaited, so the retry writes it."
    (let [config (cfg "dh-smd-failed-commit" #uuid "5c4e3a00-0000-0000-0000-000000000101")]
      (fresh! config)
      (let [conn (d/connect config)]
        (d/transact conn [attr-x])
        (d/transact conn [{:x 1}])
        ;; a NEW schema, so this commit introduces a new schema-meta key
        (is (thrown? Throwable
                     (with-redefs [dw/create-commit-id (fn [& _] (throw (AssertionError. "boom")))]
                       (d/transact conn [attr-y])))
            "precondition: the commit fails after db->stored has run")
        (try (d/release conn) (catch Throwable _)))

      ;; the application reconnects in the SAME JVM and retries
      (let [conn (d/connect config)]
        (d/transact conn [attr-y])
        (d/transact conn [{:x 2 :y 3}])
        (let [[smk on-disk?] (schema-meta-on-disk? conn)]
          (is (true? on-disk?)
              (str "the head names " smk " — it must be in the store")))
        (d/release conn))

      (cold!)
      (let [conn (d/connect config)]
        (is (contains? (:schema @conn) :y)
            "a cold reopen must still know the schema the retry committed")
        (is (some? (:rschema @conn)))
        (is (= 1 (d/q '[:find (count ?e) . :where [?e :x 2]] @conn)))
        (d/release conn))
      (d/delete-database config))))

(deftest two-stores-sharing-an-id-are-independent
  (testing "`store-identity` is (:id config) alone, so two file stores at
            different paths that share an :id used to share one write cache and
            the second database was born with a dangling key — no error, no GC,
            no concurrency. The proof hangs on the store object instead."
    (let [id #uuid "5c4e3a00-0000-0000-0000-000000000102"
          a (cfg "dh-smd-share-a" id)
          b (cfg "dh-smd-share-b" id)]
      (doseq [config [a b]]
        (fresh! config)
        ;; sequential: the connection registry is keyed by [store-id branch] too,
        ;; so holding both at once returns the FIRST connection for both configs.
        ;; That is a separate defect; this test is about the schema-meta blob.
        (let [conn (d/connect config)]
          (d/transact conn [attr-x])
          (d/transact conn [{:x 1}])
          (let [[smk on-disk?] (schema-meta-on-disk? conn)]
            (is (true? on-disk?)
                (str (get-in config [:store :path]) " names " smk)))
          (d/release conn)))

      (cold!)
      (let [conn (d/connect b)]
        (is (contains? (:schema @conn) :x)
            "the second store must be readable on its own terms")
        (d/release conn))
      (doseq [config [a b]] (d/delete-database config)))))

(deftest projecting-a-db-onto-the-wire-claims-nothing
  (testing "`datahike.cbor` serializes a db with `(db->stored db false)` and
            DISCARDS the schema-meta KV it returns. Under the old gate that
            marked the key as written, so a db crossing the wire before the
            schema was committed stranded it. db->stored no longer records
            anything; only an awaited write does."
    (let [config (cfg "dh-smd-wire" #uuid "5c4e3a00-0000-0000-0000-000000000103")]
      (fresh! config)
      (let [conn (d/connect config)
            store (:store @conn)]
        (d/transact conn [attr-x])
        (sc/forget-schema-meta-durable! store)
        (let [[kv _stored] (dw/db->stored @conn false)
              [smk _] kv]
          (is (some? smk) "precondition: the projection offers the KV to its caller")
          (is (false? (sc/schema-meta-durable? store smk nil))
              "but a caller that drops it must not have left a durability claim"))
        (d/release conn))
      (d/delete-database config))))

(deftest steady-state-writes-schema-meta-once
  (testing "The point of the proof is to avoid write amplification: once the
            schema has settled, further commits must NOT re-write the blob. This
            also pins the literal keyword in datahike.store to
            schema-cache/durable-cell-key — if they drift, no proof is ever
            found here and the KV comes back on every commit."
      ;; :exclusive — this process is the only writer, so the proof never expires
    (let [config (assoc (cfg "dh-smd-steady" #uuid "5c4e3a00-0000-0000-0000-000000000104")
                        :writer {:backend :self :writer-ownership :exclusive})]
      (fresh! config)
      (let [conn (d/connect config)
            store (:store @conn)]
        (d/transact conn [attr-x])
        (let [smk (:schema-meta-key (head conn))]
          (is (true? (sc/schema-meta-durable? store smk nil))
              "the commit that wrote it recorded the proof")
          (dotimes [i 5] (d/transact conn [{:x (long i)}]))
          (is (nil? (first (dw/db->stored @conn false)))
              "a commit with an unchanged schema offers no schema-meta KV to write")
          (is (= smk (:schema-meta-key (head conn)))
              "and the head still names the same content-addressed blob"))

        (testing "a collection withdraws the proof, so the next commit re-earns it"
          (sc/forget-schema-meta-durable! store)
          (is (some? (first (dw/db->stored @conn false)))))
        (d/release conn))
      (d/delete-database config))))

(deftest shared-ownership-re-asserts-the-blob
  (testing "Under :shared ownership (the default) another process may collect,
            and a sweep whose mark ran before our head flip is blind to what we
            just wrote. Proof of our own write therefore expires, and the next
            commit writes the blob again. Because the key is content-addressed,
            that one write retroactively repairs every commit naming it — which
            is what makes a foreign deletion self-healing instead of permanent."
    (let [config (cfg "dh-smd-reassert" #uuid "5c4e3a00-0000-0000-0000-000000000107")]
      (fresh! config)
      (let [conn (d/connect config)
            store (:store @conn)]
        (d/transact conn [attr-x])
        (let [smk (:schema-meta-key (head conn))]
          (is (nil? (first (dw/db->stored @conn false)))
              "precondition: within the window the proof holds and nothing is re-written")

          ;; a collector outside datahike removes the blob; datahike cannot see this
          (k/dissoc store smk {:sync? true})
          (is (false? (k/exists? store smk {:sync? true})))

          ;; time passes beyond the window another process could have been blind
          ;; in. alter-var-root, not binding: the commit runs on the WRITER's
          ;; thread, where a thread-local binding from this one is invisible.
          (let [orig sc/*shared-re-assert-ms*]
            (try
              (alter-var-root #'sc/*shared-re-assert-ms* (constantly 0))
              (is (some? (first (dw/db->stored @conn false)))
                  "an expired proof must offer the KV again")
              (d/transact conn [{:x 9}])
              (finally
                (alter-var-root #'sc/*shared-re-assert-ms* (constantly orig)))))

          (is (true? (k/exists? store smk {:sync? true}))
              "and the commit that took it re-wrote the blob")
          (is (= smk (:schema-meta-key (head conn)))
              "at the same content-addressed key, so earlier commits are repaired too"))
        (d/release conn))

      (cold!)
      (let [conn (d/connect config)]
        (is (contains? (:schema @conn) :x))
        (d/release conn))
      (d/delete-database config))))

(deftest missing-schema-meta-fails-loudly
  (testing "Nothing in datahike deletes a live schema-meta blob — the GC mark
            unions every branch head's key. But a collector outside datahike
            sweeping by its own allow-list would not know to follow it, and the
            old behaviour was to open a schema-less database and throw
            'this.rschema is null' from somewhere unrelated. Name the key
            instead, so the store can be diagnosed and repaired."
    (let [config (cfg "dh-smd-missing" #uuid "5c4e3a00-0000-0000-0000-000000000105")]
      (fresh! config)
      (let [conn (d/connect config)]
        (d/transact conn [attr-x])
        (d/transact conn [{:x 1}])
        ;; delete the blob behind datahike's back, as a foreign sweep would
        (let [smk (:schema-meta-key (head conn))]
          (k/dissoc (:store @conn) smk {:sync? true})
          (d/release conn)
          (cold!)
          (let [e (try (d/connect config) nil
                       (catch clojure.lang.ExceptionInfo e e))]
            (is (some? e) "connect must fail rather than return a schema-less db")
            (is (= :schema-meta-missing (:type (ex-data e))))
            (is (= smk (:schema-meta-key (ex-data e)))
                "the error names the key an operator has to restore"))))
      (d/delete-database config))))

(deftest gc-refuses-to-mark-without-a-schema-it-was-told-to-have
  (testing "The GC mark derives which attributes hold `:db.type/store-ref` from
            the record's schema. With the schema-meta blob gone it derived NONE
            and swept blobs the database still names — and `reachable-store-refs`
            handed the same short set to the application, which deletes its own
            S3 prefix against it. The same nil also made a head's
            `:building` secondary invisible, so a collection stopped deferring to
            it. Both are the mark reporting less than the truth, which is the one
            thing a sweep may not tolerate."
    (let [config (cfg "dh-smd-gc" #uuid "5c4e3a00-0000-0000-0000-000000000107")]
      (fresh! config)
      (let [conn (d/connect config)]
        (d/transact conn [attr-x])
        (d/transact conn [{:x 1}])
        (let [smk (:schema-meta-key (head conn))]
          (k/dissoc (:store @conn) smk {:sync? true})
          (is (= :schema-meta-missing
                 (try (<?? S (gc/gc-storage! @conn)) nil
                      (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
              "a collection must refuse rather than sweep on an incomplete mark")
          (is (= :schema-meta-missing
                 (try (<?? S (gc/reachable-store-refs @conn)) nil
                      (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
              "and the live-blob set an application deletes against must not come back short")
          (is (= :schema-meta-missing
                 (try (<?? S (gc/record-store-refs (:store @conn) (head conn))) nil
                      (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))
              "nor may a sync walker ship a head's datoms without the blobs they name"))
        (try (d/release conn) (catch Throwable _)))
      (d/delete-database config))))

(deftest a-record-with-no-schema-meta-key-is-not-an-error
  (testing "The guard is conditioned on the KEY being named, not on the schema
            being nil — because a nil schema is ordinary. A bulk-import
            CHECKPOINT record (`datahike.migrate`) is `{:meta … :config …}` plus
            index keys: no schema-meta-key and no schema by construction, its
            blobs travelling in `:datahike.gc/keys` instead. Refusing on a nil
            schema alone would break every index build."
    (let [config (cfg "dh-smd-checkpoint" #uuid "5c4e3a00-0000-0000-0000-000000000108")]
      (fresh! config)
      (let [conn (d/connect config)]
        (d/transact conn [attr-x])
        (d/transact conn [{:x 1}])
        (let [stored (head conn)
              checkpoint (-> stored
                             (dissoc :schema-meta-key)
                             (assoc :meta {:datahike/commit-id (random-uuid)}))]
          (is (= #{} (<?? S (gc/record-store-refs (:store @conn) checkpoint)))
              "no key named, so nothing to be missing — report no refs, do not raise"))
        (d/release conn))
      (d/delete-database config))))

(deftest legacy-inline-schema-still-opens
  (testing "Records written before the schema was out-of-lined carry :schema and
            :rschema inline and NO :schema-meta-key. The loud failure above must
            not catch them — it is conditioned on the key being named."
    (let [config (cfg "dh-smd-legacy" #uuid "5c4e3a00-0000-0000-0000-000000000106")]
      (fresh! config)
      (let [conn (d/connect config)]
        (d/transact conn [attr-x])
        (d/transact conn [{:x 1}])
        (let [store (:store @conn)
              stored (head conn)
              schema-meta (k/get store (:schema-meta-key stored) nil {:sync? true})
              ;; the pre-#716 shape: schema inline, no key
              legacy (merge (dissoc stored :schema-meta-key) schema-meta)]
          (d/release conn)
          (cold!)
          (let [db (dw/stored->db legacy store)]
            (is (contains? (:schema db) :x))
            (is (some? (:rschema db))))))
      (d/delete-database config))))
