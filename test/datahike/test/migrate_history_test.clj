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
