(ns datahike.test.schema-cache-test
  "The schema-meta DURABILITY PROOF, at unit level.

   This decides one thing: whether `writing/db->stored*` may skip re-writing a
   database's schema meta. Answering `false` costs one idempotent write of a
   content-addressed blob. Answering `true` when the blob is not actually there
   produces a commit that names a key nothing ever stored — a database that
   works until the process restarts and then has no schema at all, with every
   commit since pointing at the same missing key.

   So the only property that really matters is: `schema-meta-durable?` is true
   ONLY after something recorded proof, and proof is only recorded after an
   awaited write or a read that reached the store. The end-to-end paths that
   used to break that are in `datahike.test.schema-meta-durability-test`.

   This replaces a suite about a global LRU-of-LRUs keyed by store id, which had
   its own concurrency hazard (a `has?`-then-`lookup` pair that a concurrent
   `clear-write-cache` could empty in between, handing both callers nil to
   dereference — an NPE from a background collection, blamed on an unrelated
   transaction). The cell is now a plain atom owned by the store, so that shape
   is gone; `proof-survives-concurrent-forget` keeps a regression guard on the
   equivalent race anyway."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.schema-cache :as sc]))

(defn- store-with-cell
  "A stand-in for a konserve store carrying the cell that
   `datahike.store/add-cache-and-handlers` attaches. That the REAL store carries
   a cell under the key `schema-cache` reads is asserted end-to-end by
   `datahike.test.schema-meta-durability-test/steady-state-writes-schema-meta-once`
   — if the two literals ever drift, the proof is never found and that test sees
   a re-write on every commit."
  []
  {sc/durable-cell-key (sc/new-durable-cell)})

(deftest proof-must-be-recorded-before-it-is-believed
  (testing "a fresh store proves nothing"
    (let [store (store-with-cell)]
      (is (false? (sc/schema-meta-durable? store "k" nil))
          "an unmarked key is never durable, whatever the cadence")))

  (testing "marking records proof, forgetting withdraws it"
    (let [store (store-with-cell)]
      (sc/mark-schema-meta-durable! store "k")
      (is (true? (sc/schema-meta-durable? store "k" nil)))
      (is (false? (sc/schema-meta-durable? store "other" nil))
          "proof is per key, not per store")
      (sc/forget-schema-meta-durable! store)
      (is (false? (sc/schema-meta-durable? store "k" nil))
          "after a collection or a head rewind the next commit must write again")))

  (testing "a store with no cell at all answers false rather than throwing"
    (is (false? (sc/schema-meta-durable? {} "k" nil))
        "the commit path must degrade to a redundant write, never to an exception")
    (is (nil? (sc/mark-schema-meta-durable! {} "k")))
    (is (nil? (sc/forget-schema-meta-durable! {})))))

(deftest two-stores-never-share-a-proof
  (testing "this is the failure that made a shared store :id corrupt the second
            database: the old cache was keyed by (:id config), so a store that
            had never been written to inherited another store's claim. The cell
            hangs on the store OBJECT, and sibling connections each build their
            own."
    (let [a (store-with-cell)
          b (store-with-cell)]
      (sc/mark-schema-meta-durable! a "k")
      (is (true? (sc/schema-meta-durable? a "k" nil)))
      (is (false? (sc/schema-meta-durable? b "k" nil))))))

(deftest proof-expires-on-the-re-assert-clock
  (testing "nil means never — the exclusive-writer case, where the only deleter
            is our own GC and it drops the proof itself"
    (let [store (store-with-cell)]
      (sc/mark-schema-meta-durable! store "k")
      (is (true? (sc/schema-meta-durable? store "k" nil)))))

  (testing "a zero window means the proof is already stale, so the next commit
            re-asserts. This is the shared-writer case in miniature: proof of a
            write we made cannot outlive the window in which another process's
            sweep could have been blind to it."
    (let [store (store-with-cell)]
      (sc/mark-schema-meta-durable! store "k")
      (is (false? (sc/schema-meta-durable? store "k" 0)))))

  (testing "a wide window keeps it"
    (let [store (store-with-cell)]
      (sc/mark-schema-meta-durable! store "k")
      (is (true? (sc/schema-meta-durable? store "k" (* 60 60 1000)))))))

(deftest re-assert-cadence-follows-writer-ownership
  (testing "ownership is the connection's statement about whether another
            process may collect, which is exactly what the cadence substitutes
            for — the same question datahike.gc/default-min-age-ms asks"
    (is (nil? (sc/re-assert-after-ms {:writer {:backend :self :writer-ownership :exclusive}}))
        "exclusive: never re-assert, so a settled schema costs zero extra writes")
    (is (pos? (sc/re-assert-after-ms {:writer {:backend :self :writer-ownership :shared}}))
        "shared is the default ownership and must re-assert")
    (is (pos? (sc/re-assert-after-ms {}))
        "an unstated writer config is shared, the safe direction")
    (is (pos? (sc/re-assert-after-ms {:writer {:backend :datahike-server}}))
        "a remote backend writes elsewhere entirely")))

(deftest a-new-proof-replaces-the-old-one
  (testing "The cell holds ONE key, because one is all that is ever asked for:
            `db->stored` asks only about the db it is committing, whose schema is
            this connection's head, and a store object belongs to one connection.
            A schema change replaces the key rather than accumulating.

            This is also why there is no size threshold to get wrong. An earlier
            version recorded a proof on every store-reaching READ in
            `stored->db`, which fires for historical commits too — keys nobody
            ever asks about — so the cell needed a bound and an eviction policy.
            Proof now comes only from an awaited write, and one entry suffices."
    (let [store (store-with-cell)]
      (sc/mark-schema-meta-durable! store "k1")
      (sc/mark-schema-meta-durable! store "k2")
      (is (true? (sc/schema-meta-durable? store "k2" nil))
          "the key the next commit will ask for")
      (is (false? (sc/schema-meta-durable? store "k1" nil))
          "the superseded one is simply gone — and forgetting is the safe
           direction: it costs one idempotent re-write of a content-addressed
           blob, where wrongly REMEMBERING costs a commit naming a missing key")
      (is (= {:key "k2"} (dissoc @(get store sc/durable-cell-key) :at))
          "one entry, not a growing map"))))

(deftest proof-survives-concurrent-forget
  (testing "gc-storage! forgets a store's proof while writers are inside
            db->stored reading it. Whatever that interleaving produces, it may
            not be an exception on the commit path."
    (let [store (store-with-cell)
          errors (atom [])
          stop (atom false)
          forgetter (future (while (not @stop) (sc/forget-schema-meta-durable! store)))
          workers (mapv (fn [_]
                          (future
                            (dotimes [i 20000]
                              (try
                                (sc/schema-meta-durable? store (str "k" (mod i 7)) nil)
                                (sc/mark-schema-meta-durable! store (str "k" (mod i 7)))
                                (catch Throwable t (swap! errors conj t))))))
                        (range 8))]
      (run! deref workers)
      (reset! stop true)
      @forgetter
      (is (empty? @errors)
          (str "got " (count @errors) " failures, e.g. "
               (some-> @errors first ex-message))))))
