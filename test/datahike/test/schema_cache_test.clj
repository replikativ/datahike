(ns datahike.test.schema-cache-test
  "The per-store schema WRITE cache, under the concurrency it actually sees.

   This cache decides one thing: whether `writing/db->stored` may skip
   re-writing a database's schema meta. It is an optimisation — the key is
   `(uuid schema-meta)`, so writing again is idempotent — but it sits on the
   commit path, and anything that throws there kills a user's transaction.

   And it IS concurrent by design, not by accident: `gc/mark-and-sweep!` evicts
   a store's write cache (`clear-write-cache`) while writers are inside
   `db->stored` reading it. `background-gc-under-pipelined-writes` is the test
   that drives both at once, and it is where this first surfaced — as a
   NullPointerException from a background collection, blamed on an unrelated
   transaction, roughly one CI run in many.

   These are deterministic where that one is statistical."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.schema-cache :as sc]))

(defn- hammer
  "Run `readers` threads doing cache reads and writes while another thread
   evicts, and collect everything that escapes."
  [readers iterations]
  (let [store-config {:backend :memory :id (java.util.UUID/randomUUID)}
        errors (atom [])
        stop (atom false)
        evictor (future
                  (while (not @stop)
                    (sc/clear-write-cache store-config)))
        workers (mapv (fn [_]
                        (future
                          (dotimes [i iterations]
                            (try
                              (sc/write-cache-has? store-config (str "k" (mod i 7)))
                              (sc/add-to-write-cache store-config (str "k" (mod i 7)))
                              (catch Throwable t
                                (swap! errors conj t))))))
                      (range readers))]
    (run! deref workers)
    (reset! stop true)
    @evictor
    @errors))

(deftest write-cache-survives-eviction-by-a-concurrent-gc
  (testing "`get-or-create-write-cache` was `has?` then `lookup` — two
            operations on a cache a concurrent evict can empty in between. The
            loser got nil, and both callers then dereferenced it:

              write-cache-has?   (cw/has? nil k)  -> NPE \"fut is null\"
              add-to-write-cache (cw/miss nil k)  -> NPE \"atom is null\"

            Measured before the fix, 8 threads x 20000 iterations against a
            continuous evictor: 5533 failures, both variants. After: 0.

            The eviction is not hypothetical — `gc.cljc` calls
            `clear-write-cache` on every mark-and-sweep, and `schema-write-caches`
            is itself an LRU that evicts when a process touches more stores than
            `*schema-write-cache-max-db-count*`."
    (let [errors (hammer 8 20000)]
      (is (empty? errors)
          (str "the commit path must not throw when the GC evicts underneath it; got "
               (count errors) " failures, e.g. "
               (some-> errors first ex-message))))))

(deftest the-cache-still-caches
  (testing "the fix must not turn the optimisation off. `lookup-or-miss`
            installs the per-store cache on first use and returns the SAME one
            afterwards, so a key added is a key found — otherwise every commit
            would rewrite schema meta and the race would be 'fixed' by making
            the cache useless."
    (let [cfg {:backend :memory :id (java.util.UUID/randomUUID)}]
      (is (false? (sc/write-cache-has? cfg "fresh")))
      (sc/add-to-write-cache cfg "fresh")
      (is (true? (sc/write-cache-has? cfg "fresh"))
          "the second call sees the first call's cache, not a new one")
      (sc/clear-write-cache cfg)
      (is (false? (sc/write-cache-has? cfg "fresh"))
          "and an evicted store starts empty rather than throwing"))

    (testing "two stores do not share one cache"
      (let [a {:backend :memory :id (java.util.UUID/randomUUID)}
            b {:backend :memory :id (java.util.UUID/randomUUID)}]
        (sc/add-to-write-cache a "k")
        (is (true? (sc/write-cache-has? a "k")))
        (is (false? (sc/write-cache-has? b "k")))))))
