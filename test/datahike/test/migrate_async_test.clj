(ns datahike.test.migrate-async-test
  "The import path run in ASYNC mode, on the JVM.

   `migrate` is `async+sync` throughout, which means every function compiles
   twice — and until this namespace existed, only one of the two halves ever
   ran. That is not a theoretical gap. `async+sync` rewrites `<?-` to `do` in the
   sync branch, so a parked take in a place the `go` state machine cannot reach
   — inside a reducing function, a `group-by` predicate, a `catch` — works
   perfectly synchronously and DEADLOCKS asynchronously. `migrate.store` was
   written that way first and had to be restructured; the same trap applies to
   everything converted since.

   Running `{:sync? false}` here, on the JVM, means those mistakes surface in a
   REPL against a stack trace rather than in a shadow-cljs build against a
   promise that never resolves. Node then only has to prove the platform
   specifics, not the shape.

   JVM-only on purpose: it tests the ASYNC branch using a BLOCKING take, which
   is the one thing ClojureScript cannot do."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :refer [<!!]]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [konserve.store :as ks]
            [datahike.migrate.store :as mstore]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.compress :as mz]
            [datahike.migrate.digest :as dig]
            [datahike.test.utils :as utils]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- mem-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

(defn- take-result
  "The result of an import, however the mode delivered it.

   Sync returns the report; async returns a channel of it. `go-try-` CATCHES a
   thrown exception and delivers it as the channel's value, so an async caller
   that does not check gets a failure that looks like a result — which is why
   this rethrows rather than hiding it."
  [r]
  (if (instance? clojure.core.async.impl.channels.ManyToManyChannel r)
    (let [v (<!! r)]
      (if (instance? Throwable v) (throw v) v))
    r))

(deftest the-mode-actually-changes-what-import-db-returns
  "Guards against every other test here being vacuous.

   `take-result` accepts a report OR a channel, so if `{:sync? false}` quietly
   ran synchronously nothing else in this namespace would notice — it would all
   pass while testing the sync branch twice. This pins that the two modes really
   do return different things."
  (let [src (utils/setup-db (mem-cfg))]
    (d/transact src [{:db/ident :n :db/valueType :db.type/long
                      :db/cardinality :db.cardinality/one}])
    (d/transact src [{:n 1}])
    (let [path (str (System/getProperty "java.io.tmpdir") "/dh-async-shape-" (utils/get-time))
          _ (m/export-db src path {:history? true})
          t1 (utils/setup-db (mem-cfg))
          t2 (utils/setup-db (mem-cfg))
          sync-r (m/import-db t1 path {:sync? true})
          async-r (m/import-db t2 path {:sync? false})]
      (is (map? sync-r) "sync returns the report itself")
      (is (instance? clojure.core.async.impl.channels.ManyToManyChannel async-r)
          "async returns a channel — if this fails, every other test here is
           testing the sync branch twice")
      (is (map? (<!! async-r)) "and the channel carries the report")
      (teardown t1) (teardown t2))
    (teardown src)))

(deftest a-round-trip-is-identical-in-both-modes
  (testing "the async branch is not merely compiled, it produces the same
            database — same datoms, same transaction count, same report."
    (doseq [sync? [true false]]
      (let [src (utils/setup-db (mem-cfg))]
        (d/transact src [{:db/ident :name :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                         {:db/ident :n :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}])
        (doseq [i (range 5)]
          (d/transact src [{:name (str "e" i) :n i}]))
        (let [path (str (System/getProperty "java.io.tmpdir")
                        "/dh-async-" sync? "-" (utils/get-time))
              man (m/export-db src path {:history? true :chunk-size 3})
              tgt (utils/setup-db (mem-cfg))
              rep (take-result (m/import-db tgt path {:sync? sync?}))
              triples (fn [c] (set (map (juxt :e :a :v :added)
                                        (d/datoms (d/history @c) :eavt))))]
          (is (= (:count (:semantic-digest man)) (:datom-count rep))
              (str "datom count, :sync? " sync?))
          (is (= 6 (:tx-count rep)) "schema + 5 data transactions")
          (is (true? (:verified? rep)) "post-import verification ran and passed")
          (is (= (count (triples src)) (count (triples tgt))))
          (teardown tgt))
        (teardown src)))))

(deftest multiple-chunks-are-streamed-in-async-mode
  (testing "the OUTER loop — one await per chunk. With `:chunk-size 2` the
            importer reads several chunks, so a mistake in the chunk loop (as
            opposed to the record loop) has somewhere to show."
    (let [src (utils/setup-db (mem-cfg))]
      (d/transact src [{:db/ident :n :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one}])
      (doseq [i (range 8)] (d/transact src [{:n i}]))
      (let [path (str (System/getProperty "java.io.tmpdir") "/dh-async-multi-" (utils/get-time))
            man (m/export-db src path {:history? true :chunk-size 2})
            tgt (utils/setup-db (mem-cfg))]
        (is (> (count (:chunks man)) 3) "precondition: several chunks")
        (let [rep (take-result (m/import-db tgt path {:sync? false}))]
          (is (= (:count (:semantic-digest man)) (:datom-count rep))))
        (teardown tgt))
      (teardown src))))

(deftest small-batches-force-many-flushes-in-async-mode
  (testing "the INNER loop — one await per flush. `:batch-size 1` makes the
            batcher flush at nearly every transaction boundary, so every
            `load-entities` await is exercised rather than one at the end."
    (let [src (utils/setup-db (mem-cfg))]
      (d/transact src [{:db/ident :n :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one}])
      (doseq [i (range 6)] (d/transact src [{:n i}]))
      (let [path (str (System/getProperty "java.io.tmpdir") "/dh-async-batch-" (utils/get-time))
            man (m/export-db src path {:history? true})
            tgt (utils/setup-db (mem-cfg))
            rep (take-result (m/import-db tgt path {:sync? false :batch-size 1}))]
        (is (= (:count (:semantic-digest man)) (:datom-count rep)))
        (teardown tgt))
      (teardown src))))

;; ---------------------------------------------------------------------------
;; the narrowing retry, which is the intricate part

(defn- dump-with-a-bad-datom
  "A dump whose last chunk carries one record `load-entities` will reject — a
   numeric attribute in a non-ref database — appended as its own gzip member and
   with the chunk hash recomputed so the dump still describes itself."
  [src]
  (let [path (str (System/getProperty "java.io.tmpdir") "/dh-async-bad-" (utils/get-time))
        man (m/export-db src path {:history? true})
        chunk (io/file path (:file (last (:chunks man))))
        codec (:compression man)
        bad (mcbor/encode-record [9999 42 "x" 536870914 true])]
    (with-open [out (java.io.FileOutputStream. chunk true)]
      (.write out ^bytes (mz/compress-bytes codec bad)))
    (let [mf (io/file path "manifest.edn")
          m0 (read-string (slurp mf))
          ix (dec (count (:chunks m0)))
          sha (dig/sha256-hex (mz/decompress-bytes
                               codec (java.nio.file.Files/readAllBytes (.toPath chunk))))]
      (spit mf (pr-str (-> m0
                           (assoc-in [:chunks ix :sha256] sha)
                           (assoc-in [:chunks ix :bytes] (.length chunk))))))
    [path (:count (:semantic-digest man))]))

(deftest error-narrowing-works-in-both-modes
  (testing "the path most likely to be broken by the async conversion.

            `collect-apply!` retries per transaction, then per datom, and those
            retries used to live INSIDE the `catch` — where core.async cannot
            park — inside `mapcat` and `reduce` closures the go block cannot
            enter. All three were restructured. If that restructuring is wrong,
            it is wrong HERE and only in async mode."
    (doseq [sync? [true false]]
      (let [src (utils/setup-db (mem-cfg))]
        (d/transact src [{:db/ident :age :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}])
        (d/transact src [{:db/id "a" :age 30}])
        (let [[path good-count] (dump-with-a-bad-datom src)
              tgt (utils/setup-db (mem-cfg))
              rep (take-result (m/import-db tgt path {:sync? sync?
                                                      :on-error :collect
                                                      :verify? false}))]
          (is (= 1 (count (:errors rep)))
              (str "exactly one datom collected, :sync? " sync?))
          (is (= [9999 42 "x" 536870914 true] (:datom (first (:errors rep)))))
          (is (= 30 (d/q '[:find ?a . :where [?e :age ?a]] @tgt))
              "and the good data still landed")
          (teardown tgt))
        (teardown src)))))

(deftest abort-surfaces-the-failure-in-both-modes
  (testing "under `:on-error :abort` a bad datom must stop the import.

            The shapes differ and both are pinned: sync THROWS, async delivers
            the exception as the channel's value, because `go-try-` catches it.
            A caller taking with `<!` instead of `<?` would otherwise treat a
            failed import as a successful one."
      (let [src (utils/setup-db (mem-cfg))]
        (d/transact src [{:db/ident :age :db/valueType :db.type/long
                          :db/cardinality :db.cardinality/one}])
        (d/transact src [{:db/id "a" :age 30}])
        (let [[path _] (dump-with-a-bad-datom src)]
          (testing "sync throws"
            (let [tgt (utils/setup-db (mem-cfg))]
              (is (= :import/corrupt-datom
                     (try (m/import-db tgt path {:sync? true :on-error :abort :verify? false}) nil
                          (catch clojure.lang.ExceptionInfo e (:error (ex-data e)))))) 
              (teardown tgt)))
          (testing "async delivers the exception on the channel"
            (let [tgt (utils/setup-db (mem-cfg))
                  v (<!! (m/import-db tgt path {:sync? false :on-error :abort :verify? false}))]
              (is (instance? Throwable v) "not a report — a failure")
              (is (= :import/corrupt-datom (:error (ex-data v))))
              (teardown tgt))))
        (teardown src))))

;; ---------------------------------------------------------------------------
;; export

(deftest export-runs-in-async-mode-too
  (testing "the other half of the portable path.

            Import was converted first, which left `export-db` blocking on
            `mblobs/plan`, `copy-out!` and `mstore/write-chunks!` — all of them
            already `async+sync`, all of them called synchronously. It also held
            the konserve write inside a `write-to!` CLOSURE, which is exactly
            what the `go` state machine cannot enter; that is inlined now.

            `:sort? false` on purpose: the external merge sort is JVM-only (a
            k-way merge over a lazy seq of open files cannot pull from async IO),
            so the portable export is the no-scratch one."
    (let [src (utils/setup-db (mem-cfg))]
      (d/transact src [{:db/ident :name :db/valueType :db.type/string
                        :db/cardinality :db.cardinality/one}])
      (d/transact src [{:name "a"} {:name "b"}])
      (let [store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)}
                                   {:sync? true})
            target {:store store :prefix "async-export"}
            r (m/export-db src target {:history? true :sort? false :sync? false})]
        (is (instance? clojure.core.async.impl.channels.ManyToManyChannel r)
            "async export returns a channel")
        (let [man (take-result r)]
          (is (pos? (count (:chunks man))) "and the dump was written")
          (testing "and it imports back, also asynchronously — the whole path"
            (let [tgt (utils/setup-db (mem-cfg))
                  rep (take-result (m/import-db tgt target {:sync? false}))]
              (is (= (:count (:semantic-digest man)) (:datom-count rep)))
              (is (true? (:verified? rep)))
              (teardown tgt)))))
      (teardown src))))

(deftest a-filesystem-export-also-runs-async
  (testing "the same for a directory target, where the writes are synchronous on
            both runtimes but still have to compose with the awaits around them."
    (let [src (utils/setup-db (mem-cfg))]
      (d/transact src [{:db/ident :n :db/valueType :db.type/long
                        :db/cardinality :db.cardinality/one}])
      (doseq [i (range 4)] (d/transact src [{:n i}]))
      (let [path (str (System/getProperty "java.io.tmpdir") "/dh-async-exp-" (utils/get-time))
            man (take-result (m/export-db src path {:history? true :sort? false
                                                    :sync? false :chunk-size 2}))
            tgt (utils/setup-db (mem-cfg))
            rep (take-result (m/import-db tgt path {:sync? false}))]
        (is (= (:count (:semantic-digest man)) (:datom-count rep)))
        (teardown tgt))
      (teardown src))))

(deftest a-store-source-is-not-released-before-it-is-read
  (testing "the bug this namespace was written to catch and did not.

            `import-db` closed the source store in a `finally`. In async mode
            `run-import` returns a CHANNEL immediately, so the `finally` fired
            before a single chunk had been read — instrumented, the order was
            `[:CLOSE :READ :READ …]`, every read against a released store, and
            the import still reported `:verified? true`.

            It went unnoticed because `:memory` and `:file` release is near-nil.
            On a pooled backend — JDBC, RocksDB, an S3 client — that is a
            use-after-release that reports success. The earlier tests here missed
            it because they only used FILESYSTEM sources, so the close was never
            in play."
    (let [log (atom [])
          orig-close mstore/close
          orig-read mstore/read-chunk]
      (with-redefs [mstore/close (fn [& a] (swap! log conj :close) (apply orig-close a))
                    mstore/read-chunk (fn [& a] (swap! log conj :read) (apply orig-read a))]
        (let [src (utils/setup-db (mem-cfg))]
          (d/transact src [{:db/ident :n :db/valueType :db.type/long
                            :db/cardinality :db.cardinality/one}])
          (doseq [i (range 4)] (d/transact src [{:n i}]))
          (let [store (ks/create-store {:backend :memory :id (java.util.UUID/randomUUID)}
                                       {:sync? true})
                target {:store store :prefix "release-order"}]
            (m/export-db src target {:history? true :sort? false :chunk-size 2})
            (doseq [sync? [true false]]
              (reset! log [])
              (let [tgt (utils/setup-db (mem-cfg))]
                (take-result (m/import-db tgt target {:sync? sync?}))
                (let [l @log]
                  (is (some #{:read} l) (str "chunks were read, :sync? " sync?))
                  (is (some #{:close} l) "and the store was released")
                  (is (< (.indexOf ^java.util.List l :read)
                         (.indexOf ^java.util.List l :close))
                      (str "every read must precede the close, :sync? " sync?
                           " — got " (pr-str l))))
                (teardown tgt))))
          (teardown src))))))
