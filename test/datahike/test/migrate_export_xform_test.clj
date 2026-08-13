(ns datahike.test.migrate-export-xform-test
  "`:xform` must be applied EXACTLY ONCE, on every export path.

   This already went wrong. `export-record-seq` was extracted so `export-db` and
   `export-to-sink` could not disagree about the record stream, and it took the
   transducer application with it while leaving the one at export-db's call site.
   Every dump was then transformed twice: a source holding 1,2 exported and
   re-imported as 3,4, with the manifest, the semantic digest and `verify` all
   agreeing, because each is derived from the doubly-transformed stream.

   Nothing caught it, and the reason is the whole point of this namespace: every
   existing export-`:xform` test uses an IDEMPOTENT transducer — a `take` in
   `migrate-completeness-test`, a `filter` in `migrate-init-import-test`. Neither
   can distinguish one application from two.

   The retention fix (`sorted-record-seq` created at the point of use) makes this
   worse in principle: \"one instance for the whole export\" used to be structural,
   guaranteed by a single binding. Now it holds only because one arm of a
   `store-target?` fork runs. These tests are what keeps that true.

   Two transducers, because they fail differently:

     `inc-values`   NON-IDEMPOTENT — catches DOUBLE APPLICATION. 1 twice-incremented
                    is 3, not 2.
     `tag-ordinal`  STATEFUL — catches TWO INSTANCES. A second instance restarts
                    its counter, so ordinals repeat instead of running 0,1,2…"
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.migrate :as m]
            [datahike.test.utils :as utils]))

(defn- fresh-conn []
  (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                   :keep-history? false
                   :schema-flexibility :write}))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- populate! [conn]
  (d/transact conn [{:db/ident :n :db/valueType :db.type/long
                     :db/cardinality :db.cardinality/one}])
  (d/transact conn [{:n 1} {:n 2} {:n 3}])
  conn)

(def ^:private inc-values
  "Applied once, 1,2,3 become 2,3,4. Applied twice, 3,4,5."
  (map (fn [[e a v t op]] (if (= a :n) [e a (inc v) t op] [e a v t op]))))

(defn- tag-ordinal
  "Stateful: stamps each `:n` datom with its position in the stream, so the
   ordinals a caller gets back say how many records the transducer actually saw.

   Assert on the ordinal VALUES, not on their distinctness. A monotone counter
   produces distinct ordinals no matter how many times the stream is transformed
   or traversed — the first version of this asserted distinctness and stayed
   green under a deliberate double application (ordinals 3,5,7 rather than
   0,1,2), which is the same vacuity that let the original bug ship."
  []
  (let [i (volatile! -1)]
    (map (fn [[e a v t op]]
           (if (= a :n) [e a (+ (* 1000 (vswap! i inc)) v) t op] [e a v t op])))))

(defn- sink-values
  "The `:n` values a sink receives under `xform`."
  [conn xform]
  (let [got (atom [])]
    (m/export-to-sink @conn
                      {:open (fn [_] nil)
                       :write (fn [_ recs] (swap! got into recs) nil)
                       :close (fn [_] :ok)}
                      {:xform xform})
    (sort (keep (fn [[_ a v _ _]] (when (= a :n) v)) @got))))

(defn- dump-values
  "The `:n` values that survive an export-db → import-db round trip under `xform`."
  [conn xform]
  (let [dir (str "/tmp/claude-1000/xform-test-" (System/currentTimeMillis) "-" (rand-int 100000))
        tgt (fresh-conn)]
    (m/export-transformed @conn dir xform {})
    (m/import-db tgt dir)
    (let [vs (sort (map first (d/q '[:find ?v :where [?e :n ?v]] @tgt)))]
      (teardown tgt)
      vs)))

(deftest export-db-applies-xform-once
  (testing "1,2,3 -> 2,3,4. Applied twice it would be 3,4,5 — which is what
            shipped, and what every idempotent-transducer test missed."
    (let [conn (populate! (fresh-conn))]
      (is (= [2 3 4] (dump-values conn inc-values)))
      (teardown conn))))

(deftest export-to-sink-applies-xform-once
  (let [conn (populate! (fresh-conn))]
    (is (= [2 3 4] (sink-values conn inc-values)))
    (teardown conn)))

(deftest the-two-paths-agree-about-xform
  (testing "they disagreed for real: the sink applied it once while the dump
            applied it twice, which is the drift the shared helper exists to
            prevent"
    (let [conn (populate! (fresh-conn))]
      (is (= (dump-values conn inc-values) (sink-values conn inc-values)))
      (teardown conn))))

(deftest a-stateful-transducer-sees-each-record-exactly-once
  (testing "three `:n` datoms must be stamped 0,1,2 — the transducer saw the
            stream once. Ordinals starting anywhere else mean it saw more
            records than the export contains, because the stream was transformed
            twice or traversed twice. This is the property that stopped being
            structural when the record seq moved to its point of use, and the
            reason `export-record-seq` documents `:xform` as PURE: under
            `:sort? false` it makes two passes over `:eavt`."
    (let [conn (populate! (fresh-conn))
          vs   (sink-values conn (tag-ordinal))]
      (is (= 3 (count vs)))
      (is (= #{0 1 2} (set (map #(quot % 1000) vs)))
          (str "expected ordinals 0,1,2 — one pass over three records. Higher "
               "ordinals mean the transducer was fed the stream more than once, "
               "got " (pr-str vs)))
      (teardown conn))))
