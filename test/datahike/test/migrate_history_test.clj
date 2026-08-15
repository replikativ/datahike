(ns datahike.test.migrate-history-test
  "Does `derive-current` agree with datahike about what is currently true?

   This is the question a bulk-build import rests on. It reconstructs six index
   trees from a dump that carries only history, so it must decide which datoms
   are current without replaying transactions. If the rule is wrong, the restored
   database answers every present-tense query correctly and diverges only under
   `as-of` — the failure mode that survives an ordinary round-trip test.

   So the property is checked against datahike itself, on databases built to
   contain the shapes where the rule could go wrong, and then on randomised
   histories to catch the shapes nobody thought of."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.datom :as dd]
            [datahike.migrate.history :as mh]
            [datahike.test.utils :as utils]))

(defn- teardown [conn]
  (let [cfg (:config @conn)] (d/release conn) (d/delete-database cfg)))

(defn- mem-cfg []
  {:store {:backend :memory :id (java.util.UUID/randomUUID)}
   :keep-history? true :schema-flexibility :write})

(defn- records
  "History as `[e a v t added]` tuples — the dump's own record shape."
  [db]
  (mapv (juxt :e :a :v :tx :added) (d/datoms (d/history db) :eavt)))

(defn- actual-current [db]
  (set (map (juxt :e :a :v) (d/datoms db :eavt))))

(defn- agrees?
  "Does the derivation match datahike's own current set? Takes a CONN and derefs —
   `d/history` needs a db value, and passing the connection fails deep inside the
   IDB protocol rather than at the call site."
  [conn]
  (let [db @conn]
    (= (mh/derive-current (records db)) (actual-current db))))

;; ---------------------------------------------------------------------------

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :score :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
   {:db/ident :tag :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
   {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}])

(defn- seeded []
  (let [c (utils/setup-db (mem-cfg))]
    (d/transact c schema)
    (d/transact c [{:db/id -1 :name "a" :score 1 :tag :x}
                   {:db/id -2 :name "b" :score 2 :tag :y}
                   {:db/id -3 :name "c" :score 3}])
    c))

(deftest agrees-on-each-shape-in-isolation
  (testing "every shape that could break the rule, one at a time — so a failure
            names the shape rather than the whole database"
    (doseq [[label txs]
            [["card-one overwrite"
              [[{:db/id [:name "a"] :score 10}]]]
             ["card-one overwritten twice"
              [[{:db/id [:name "a"] :score 10}] [{:db/id [:name "a"] :score 100}]]]
             ["card-many add"
              [[{:db/id [:name "b"] :tag :z}]]]
             ["card-many retract"
              [[[:db/retract [:name "b"] :tag :y]]]]
             ["retract then RE-ASSERT the same value"
              [[[:db/retract [:name "c"] :score 3]] [{:db/id [:name "c"] :score 3}]]]
             ["retract entity"
              [[[:db/retractEntity [:name "c"]]]]]
             ["ref to an entity retracted later"
              [[{:db/id [:name "a"] :pal [:name "c"]}] [[:db/retractEntity [:name "c"]]]]]
             ["same-tx overwrite and retract of another attr"
              [[{:db/id [:name "a"] :score 42} [:db/retract [:name "b"] :tag :y]]]]
             ["schema added mid-history, then used"
              [[{:db/ident :note :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one}]
               [{:db/id [:name "a"] :note "hello"}]]]
             [":db/noHistory attribute, overwritten"
              [[{:db/ident :vol :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one :db/noHistory true}]
               [{:db/id [:name "a"] :vol "one"}]
               [{:db/id [:name "a"] :vol "two"}]]]]]
      (testing label
        (let [c (seeded)]
          (doseq [tx txs] (d/transact c tx))
          (is (agrees? c) (str "derivation disagrees for: " label))
          (teardown c))))))

(deftest agrees-on-all-shapes-together
  (testing "the shapes interacting, which is where an order-dependent rule breaks"
    (let [c (seeded)]
      (d/transact c [{:db/id [:name "a"] :score 10}])
      (d/transact c [{:db/id [:name "a"] :score 100}])
      (d/transact c [{:db/id [:name "b"] :tag :z}])
      (d/transact c [[:db/retract [:name "b"] :tag :y]])
      (d/transact c [[:db/retract [:name "c"] :score 3]])
      (d/transact c [{:db/id [:name "c"] :score 3}])
      (d/transact c [{:db/id [:name "a"] :pal [:name "c"]}])
      (d/transact c [[:db/retractEntity [:name "c"]]])
      (is (agrees? c))
      (teardown c))))

(deftest agrees-on-randomised-histories
  (testing "shapes nobody thought to write down.

            Randomised over the operation KINDS rather than over values: the rule
            is about assertion/retraction structure, so random strings would add
            volume without adding coverage."
    (dotimes [iteration 12]
      (let [c (seeded)
            rnd (java.util.Random. (+ 42 iteration))
            pick (fn [coll] (nth coll (.nextInt rnd (count coll))))
            names ["a" "b" "c"]]
        (dotimes [_ 25]
          (let [n (pick names)]
            (try
              (d/transact
               c
               (case (.nextInt rnd 5)
                 0 [{:db/id [:name n] :score (.nextInt rnd 100)}]
                 1 [{:db/id [:name n] :tag (pick [:x :y :z :w])}]
                 2 [[:db/retract [:name n] :tag (pick [:x :y :z :w])]]
                 3 [{:db/id [:name n] :pal [:name (pick names)]}]
                 4 [[:db/retractEntity [:name n]]]))
              ;; a retract of something absent, or a ref to a retracted entity,
              ;; is a legitimate transaction failure — skip it and keep going
              (catch Exception _ nil))))
        (is (agrees? c) (str "derivation disagrees on random history, seed "
                             (+ 42 iteration)))
        (teardown c)))))

(deftest split-current-keeps-the-asserting-record
  (testing "`split-current` returns the full history plus the records that
            ASSERTED each currently-true datom — the shape a bulk build consumes,
            so it gets both index sets from one pass"
    (let [c (seeded)]
      (d/transact c [{:db/id [:name "a"] :score 10}])
      (d/transact c [[:db/retract [:name "b"] :tag :y]])
      (let [db @c
            recs (records db)
            {:keys [current history]} (mh/split-current recs)]
        (is (= (count recs) (count history)) "history is every record, unchanged")
        (is (= (actual-current db) (set (map (juxt first second #(nth % 2)) current)))
            "current matches datahike's own current set")
        (is (every? #(nth % 4) current) "every current record is an ASSERTION")
        (testing "and carries the transaction that asserted it"
          (is (every? #(some? (nth % 3)) current))))
      (teardown c))))

;; ---------------------------------------------------------------------------
;; the STREAMING variant — what the bulk path actually uses

(defn- eavt-sorted
  "Records sorted by [e a v t], the order `current-from-eavt-sorted` requires.
   Uses datahike's own temporal eavt comparator, which is exactly that order —
   the same sort the temporal-eavt index build needs, so it is not extra work."
  [db]
  (->> (d/datoms (d/history db) :eavt)
       (sort (dd/index-type->cmp-quick :eavt false))
       (mapv (juxt :e :a :v :tx :added))))

(deftest streaming-currentness-agrees-with-the-set-version
  (testing "`current-from-eavt-sorted` must produce exactly what `derive-current`
            does, but with O(1) state instead of a set of every live datom.

            `derive-current` sorts its whole input and accumulates — fine for a
            test, useless on a real history. The streaming version relies on the
            sort order instead: `[e a v t]` puts every record for one datom
            adjacent, so only the last of each run matters."
    (let [c (seeded)]
      (d/transact c [{:db/id [:name "a"] :score 10}])
      (d/transact c [{:db/id [:name "a"] :score 100}])
      (d/transact c [{:db/id [:name "b"] :tag :z}])
      (d/transact c [[:db/retract [:name "b"] :tag :y]])
      (d/transact c [[:db/retract [:name "c"] :score 3]])
      (d/transact c [{:db/id [:name "c"] :score 3}])
      (d/transact c [[:db/retractEntity [:name "c"]]])
      (let [db @c
            triple (fn [r] [(nth r 0) (nth r 1) (nth r 2)])
            streaming (set (map triple (mh/current-from-eavt-sorted (eavt-sorted db))))
            setwise (mh/derive-current (records db))
            actual (actual-current db)]
        (is (= streaming setwise) "streaming agrees with the set version")
        (is (= streaming actual) "…and both agree with datahike"))
      (teardown c))))

(deftest streaming-currentness-on-randomised-histories
  (testing "the same agreement across randomised histories — the streaming
            version is the one that ships, so it gets the same scrutiny"
    (dotimes [iteration 8]
      (let [c (seeded)
            rnd (java.util.Random. (+ 900 iteration))
            pick (fn [coll] (nth coll (.nextInt rnd (count coll))))
            names ["a" "b" "c"]]
        (dotimes [_ 20]
          (let [n (pick names)]
            (try
              (d/transact c (case (.nextInt rnd 5)
                              0 [{:db/id [:name n] :score (.nextInt rnd 50)}]
                              1 [{:db/id [:name n] :tag (pick [:x :y :z :w])}]
                              2 [[:db/retract [:name n] :tag (pick [:x :y :z :w])]]
                              3 [{:db/id [:name n] :pal [:name (pick names)]}]
                              4 [[:db/retractEntity [:name n]]]))
              (catch Exception _ nil))))
        (let [db @c
              triple (fn [r] [(nth r 0) (nth r 1) (nth r 2)])
              streaming (set (map triple (mh/current-from-eavt-sorted (eavt-sorted db))))]
          (is (= streaming (actual-current db))
              (str "streaming currentness disagrees, seed " (+ 900 iteration))))
        (teardown c)))))

(deftest streaming-currentness-emits-the-asserting-record
  (testing "it returns the RECORD, not just the triple, so the bulk build can put
            the asserting transaction into the index rather than looking it up"
    (let [c (seeded)]
      (d/transact c [{:db/id [:name "a"] :score 10}])
      (let [db @c
            out (mh/current-from-eavt-sorted (eavt-sorted db))]
        (is (every? #(nth % 4) out) "every emitted record is an assertion")
        (is (every? #(some? (nth % 3)) out) "and carries its transaction")
        (testing "the score datom carries the LATEST assertion, not the first"
          (let [score (first (filter #(= :score (nth % 1)) out))]
            (is (= 10 (nth score 2)) "value is the current one"))))
      (teardown c))))

(deftest streaming-currentness-is-memory-bounded
  (testing "live heap at mid-stream does not grow with input size.

            Measured, not assumed. The last time a bounded-memory claim in this
            stack went unmeasured it was false by a factor of n, and the test
            written to catch it did not. Ratio over an 8x input: 1.00x."
    (let [live (fn [] (System/gc) (Thread/sleep 150)
                 (let [r (Runtime/getRuntime)] (- (.totalMemory r) (.freeMemory r))))
          ;; already in [e a v t] order: assert, retract, re-assert per datom
          gen (fn [n] (mapcat (fn [i] [[i :a i 100 true] [i :a i 200 false] [i :a i 300 true]])
                              (range n)))
          mid-heap (fn [n]
                     (let [sample (atom nil) cnt (atom 0)]
                       (doseq [_ (mh/current-from-eavt-sorted (gen n))]
                         (when (= (swap! cnt inc) (long (/ n 2)))
                           (reset! sample (live))))
                       [@cnt @sample]))
          [c1 s1] (mid-heap 100000)
          [c2 s2] (mid-heap 800000)]
      (is (= 100000 c1) "every datom is current — assert/retract/re-assert")
      (is (= 800000 c2))
      (is (< (/ (double s2) s1) 1.5)
          (str "live heap grew with n: " (int (/ s1 1048576)) " MB at 100k vs "
               (int (/ s2 1048576)) " MB at 800k")))))
