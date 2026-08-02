(ns datahike.test.migrate-store-test
  "The konserve-store dump medium, in BOTH modes on BOTH platforms.

   Why this exists separately from `migrate-test`, which already round-trips a
   dump through a store: every one of those tests runs `{:sync? true}`, because
   `export-db` is synchronous. So the async branch of `async+sync` had never
   executed anywhere.

   That matters more than it sounds. `async+sync` is a syntactic postwalk, and
   the async branch is a core.async `go` block whose state machine covers only
   code LEXICALLY inside it. Put IO inside a nested `fn` — a reducing function,
   a lazy-seq body — and the sync branch still works, because `<?-` is rewritten
   to `do`, while the async branch silently never completes. Green on the JVM,
   deadlocked on node.

   Running `{:sync? false}` on the JVM catches that without leaving Clojure, so
   most porting mistakes surface here rather than in a browser. The node run then
   covers what only node can: the actual backend handle shapes.

   The medium is exercised directly rather than through `export-db`, which is
   JVM-only by design — directories, POSIX permissions and `.tmp` renames have no
   counterpart in a browser."
  (:require #?(:clj [clojure.test :refer [deftest testing is]]
               :cljs [cljs.test :refer [deftest testing is async]])
            [clojure.core.async :refer [go #?(:clj <!!) <!]]
            [konserve.memory :refer [new-mem-store]]
            [datahike.migrate.store :as mstore]
            [datahike.migrate.cbor :as mcbor]
            [datahike.migrate.digest :as dig]))

(def ^:private records
  "Deliberately heterogeneous: a string, a long, a keyword, a boolean and a
   negative id, plus both ops. The chunk is CBOR, so a value type that did not
   survive would show as a decode failure rather than a wrong number."
  [[1 :name "Alice" 536870913 true]
   [2 :name "Bob" 536870913 true]
   [1 :score 42 536870914 true]
   [2 :tag :x 536870914 true]
   [1 :score 42 536870915 false]
   [1 :active? false 536870915 true]])

(defn- manifest-fn [digest chunks]
  {:chunks chunks :semantic-digest digest :format-version 1})

;; ---------------------------------------------------------------------------

(defn- roundtrip
  "Write `records` as chunks of `chunk-size`, then read every record back.
   Returns a channel of `{:manifest m :read [...]}` in async mode, the map itself
   in sync mode — mirroring what the medium's own functions do."
  [store chunk-size opts]
  (let [medium {:store store :prefix "t" :owned? false}]
    (if (:sync? opts)
      (let [m (mstore/write-chunks! medium records chunk-size manifest-fn
                                    (constantly nil) opts)]
        {:manifest m
         :read (vec (reverse (mstore/reduce-records medium m conj () opts)))})
      (go
        (let [m (<! (mstore/write-chunks! medium records chunk-size manifest-fn
                                          (constantly nil) opts))]
          {:manifest m
           :read (vec (reverse (<! (mstore/reduce-records medium m conj () opts))))})))))

(defn- check! [{:keys [manifest read]} chunk-size]
  (is (= records read)
      "every record survives, in order, through CBOR and the store")
  (is (= (long (#?(:clj Math/ceil :cljs js/Math.ceil)
                 (/ (count records) (double chunk-size))))
         (count (:chunks manifest)))
      "the manifest names one entry per chunk written")
  (is (= (count records) (:count (:semantic-digest manifest)))
      "the digest counted every record")
  (is (= (dig/digest-records (map mcbor/encode-record records))
         (:semantic-digest manifest))
      "and the streaming digest equals digesting the corpus in one go — the
       property `write-chunks!` relies on, since it accumulates while writing"))

;; ---------------------------------------------------------------------------
;; JVM: both modes

#?(:clj
   (deftest store-medium-round-trips-in-both-modes
     (doseq [opts [{:sync? true} {:sync? false}]
             chunk-size [2 100]]
       (testing (str "mode " opts ", chunk-size " chunk-size)
         (let [take* (if (:sync? opts) identity <!!)
               store (take* (new-mem-store (atom {}) opts))]
           (check! (take* (roundtrip store chunk-size opts)) chunk-size))))))

#?(:clj
   (deftest a-corrupted-chunk-is-refused-in-both-modes
     (testing "the per-chunk SHA-256 is the tamper-evidence control, so it has to
               fire on both branches — an integrity check that only runs when
               synchronous is not an integrity check.

               How it surfaces differs, and that is the convention rather than an
               accident: the sync branch is a plain `try`, so the exception
               propagates; the async branch is `go-try-`, which CATCHES it and
               delivers it as the channel's value. That is how the whole
               konserve/superv.async stack behaves — `<?` is the take that
               rethrows, `<!` the one that hands you the exception object. A
               caller using `<!` and not checking gets a silent corrupt import,
               so it is worth pinning both shapes here."
       (doseq [opts [{:sync? true} {:sync? false}]]
         (let [take* (if (:sync? opts) identity <!!)
               store (take* (new-mem-store (atom {}) opts))
               medium {:store store :prefix "t" :owned? false}
               m (take* (mstore/write-chunks! medium records 100 manifest-fn
                                              (constantly nil) opts))
               tampered (update m :chunks
                                (fn [cs] (mapv #(assoc % :sha256 (apply str (repeat 64 "0"))) cs)))
               check (fn [] (mstore/reduce-records medium tampered conj () opts))]
           (if (:sync? opts)
             (is (thrown? Exception (check)))
             (let [res (<!! (check))]
               (is (instance? Throwable res)
                   "async delivers the failure as the channel value")
               (is (= :import/checksum-failed (:error (ex-data res)))
                   "and it is the checksum failure, not some other error"))))))))

#?(:clj
   (deftest a-missing-chunk-is-refused
     (testing "a manifest naming a chunk the store does not hold is a truncated
               dump, not an empty one."
       (let [opts {:sync? true}
             store (new-mem-store (atom {}) opts)
             medium {:store store :prefix "t" :owned? false}
             m (mstore/write-chunks! medium records 100 manifest-fn (constantly nil) opts)
             ghost (update m :chunks conj {:file "datoms-000099" :count 1 :sha256 nil})]
         (is (thrown? Exception (mstore/reduce-records medium ghost conj () opts)))))))

;; ---------------------------------------------------------------------------
;; ClojureScript — async only, which is the whole point

#?(:cljs
   (deftest store-medium-round-trips-on-node
     (async done
            (go
              (let [opts {:sync? false}
                    store (<! (new-mem-store (atom {}) opts))]
                (check! (<! (roundtrip store 2 opts)) 2)
                (done))))))

#?(:cljs
   (deftest a-corrupted-chunk-is-refused-on-node
     (async done
            (go
              (let [opts {:sync? false}
                    store (<! (new-mem-store (atom {}) opts))
                    medium {:store store :prefix "t" :owned? false}
                    m (<! (mstore/write-chunks! medium records 100 manifest-fn
                                                (constantly nil) opts))
                    tampered (update m :chunks
                                     (fn [cs] (mapv #(assoc % :sha256 (apply str (repeat 64 "0"))) cs)))
                    res (<! (mstore/reduce-records medium tampered conj () opts))]
                ;; superv.async delivers a thrown exception as the channel value
                (is (instance? js/Error res)
                    "a checksum mismatch surfaces rather than being swallowed")
                (done))))))
