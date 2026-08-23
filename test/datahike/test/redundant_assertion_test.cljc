(ns datahike.test.redundant-assertion-test
  "Asserting a value the database already holds must not move state DERIVED from
   the indexes — while still reaching the consumers that key on more than the
   value.

   The primary indexes are idempotent on `[e a v]`: `-insert` returns the
   receiver, `-upsert` replaces. Everything alongside them was not, so a
   redundant assertion moved it while the index stood still. The visible failure
   is a schema install refused on its third run:

     transact #1  ->  :db.entity/attrs [:name]
     transact #2  ->  :db.entity/attrs [:name :name]     (committed, no error)
     transact #3  ->  THROWS :transact/schema, :invalid-updates
                      #:db.entity{:attrs [[:name :name] [:name]]}

   `update-schema` `conj`s into the many-valued schema attributes
   (`:db.entity/attrs`, `:db.entity/preds`, `:db.attr/preds`), so each redundant
   assertion appends another copy; the schema-update check then compares what is
   being declared against the shape the previous run wrote, and refuses. The
   mangled value is persisted, so the failure survives a reconnect. Applying your
   schema on every startup is an ordinary, deliberately idempotent pattern.

   The predicate is `value-present?`, computed once in `transact-report` for the
   `:tx-data` suppression and threaded here rather than recomputed. It probes
   with `di/-slice`, so it compares through the index's own comparator — correct
   for `:db.type/bytes` and tuple values, where Clojure `=` compares byte arrays
   by identity, and for `:db.secondary/only` attributes, where the primary index
   holds a content hash.

   The decision is made in `transact-add`, at the operation, BEFORE any index is
   touched — where Datomic and DataScript both make it. Nothing is written and
   nothing is reported, and the datom keeps its original `tx`. Deciding it lower
   down, inside the index update, produced the two defects this fixes:
   cardinality-one re-dated the fact to the new transaction while history went on
   dating it to the original, and cardinality-many reported a datom in no index
   at all.

   Placement keeps import out of it too: `transact-entities-directly` calls
   `transact-report` directly and never reaches `transact-add`, so replaying a
   dump is unaffected.

   ONE EXCEPTION, `vt-mode-attr?`: an attribute covered by a secondary index in
   valid-time (SCD2) mode takes the full write. The stratum adapter builds each
   new version row from the assertions it receives during that transaction, with
   no merge from the open row, so a restatement is load-bearing there rather than
   redundant — suppress it and the column lands nil.

   That is a question about how the index STORES, so it is answered from the
   schema's `:db.secondary/config`. `IValidTimeAware` asks something else — a
   query capability, and an optional one — and would be wrong both ways:
   `StratumIndex` satisfies it whether or not it keeps windows, and the
   transient the transactor actually holds does not satisfy it at all."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.test.utils :as utils]))

(defn- mem-conn []
  (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                   :keep-history? false :schema-flexibility :write}))

(def ^:private entity-spec
  [{:db/ident :name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :spec :db.entity/attrs [:name]}])

;; ---------------------------------------------------------------------------

(deftest an-idempotent-schema-install-stays-idempotent
  (testing "transacting the same entity spec repeatedly neither grows the schema
            nor starts failing — the regression this commit exists for"
    (let [conn (mem-conn)]
      (try
        (dotimes [i 5]
          (d/transact conn entity-spec)
          (is (= [:name] (:db.entity/attrs (get (:schema @conn) :spec)))
              (str "after transact #" (inc i) " the spec must still be [:name]")))
        (finally (d/release conn))))))

(deftest a-redundant-assertion-does-not-move-the-hash
  (testing "the index already held the datom, so the running sum must not move.

            Stated over the USER datoms: the transaction itself is real work and
            allocates a tx entity, so a bare before/after comparison of the whole
            index would be asserting something false."
    (doseq [keep-history? [false true]]
      (let [conn (utils/setup-db {:store {:backend :memory
                                          :id (java.util.UUID/randomUUID)}
                                  :keep-history? keep-history?
                                  :schema-flexibility :write})
            user-datoms #(remove (fn [d] (= :db/txInstant (:a d)))
                                 (d/datoms % :eavt))
            sum #(reduce (fn [h d] (+ h (hash d))) 0 %)]
        (try
          (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one
                             :db/unique :db.unique/identity}
                            {:db/ident :tag :db/valueType :db.type/keyword
                             :db/cardinality :db.cardinality/many}])
          (d/transact conn [{:db/id -1 :name "a" :tag :x}])
          (let [before @conn
                before-user (vec (user-datoms before))]
            (d/transact conn [{:db/id [:name "a"] :tag :x}])
            (let [after @conn]
              (testing (str "keep-history? " keep-history?)
                (is (= before-user (vec (user-datoms after)))
                    "the user datoms are unchanged, down to their tx")
                (is (= (- (:hash after) (:hash before))
                       (- (sum (d/datoms after :eavt))
                          (sum (d/datoms before :eavt))))
                    "and the sum moved by exactly what the tx entity added"))))
          (finally (d/release conn)))))))

(deftest a-genuinely-new-value-still-lands
  (testing "the obvious way to break this fix is to gate too much"
    (let [conn (mem-conn)]
      (try
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :tag :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/many}])
        (d/transact conn [{:db/id -1 :name "a" :tag :x}])
        (let [before (:hash @conn)]
          (d/transact conn [{:db/id [:name "a"] :tag :y}])
          (let [db @conn]
            (is (= #{:x :y} (set (map :v (filter #(= :tag (:a %)) (d/datoms db :eavt)))))
                "both values are present")
            (is (not= before (:hash db)) "and the new one moved the sum")))
        (finally (d/release conn))))))

(deftest a-multi-attribute-spec-still-accumulates
  (testing "a spec declaring several attributes explodes into one
            `[:db/add e :db.entity/attrs v]` per attribute, and `update-schema`
            conj's each into the vector. That accumulation is the reachable path
            and the gating must not break it — only the REDUNDANT repeat of an
            attribute already present is suppressed.

            (Growing a spec across transactions is refused by the schema-update
            check regardless, so the only accumulation is within one.)"
    (let [conn (mem-conn)]
      (try
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}
                          {:db/ident :age :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:db/ident :spec :db.entity/attrs [:name :age]}])
        (is (= [:name :age] (:db.entity/attrs (get (:schema @conn) :spec)))
            "both attributes accumulate")
        (testing "and re-declaring the identical spec leaves it alone"
          (d/transact conn [{:db/ident :spec :db.entity/attrs [:name :age]}])
          (is (= [:name :age] (:db.entity/attrs (get (:schema @conn) :spec)))))
        (finally (d/release conn))))))

(deftest the-spec-still-enforces-after-repeated-installs
  (testing "whatever the schema shape, the entity spec must keep working — this
            is what made the old corruption inert and easy to miss"
    (let [conn (mem-conn)]
      (try
        (d/transact conn entity-spec)
        (d/transact conn entity-spec)
        (is (thrown? clojure.lang.ExceptionInfo
                     (d/transact conn [{:db/id -1 :db/ensure :spec}]))
            "an entity missing the required attribute is refused")
        (is (some? (d/transact conn [{:db/id -1 :name "ok" :db/ensure :spec}]))
            "and a valid one is accepted")
        (finally (d/release conn))))))

(deftest a-redundant-assertion-does-not-move-the-datoms-tx
  (testing "the fact was asserted once and still is — re-stating it must not
            re-date it. Deciding redundancy inside the index update let the
            cardinality-one write run, which moved the current index's `tx`
            while the temporal index went on dating the fact to the original
            transaction. Datomic leaves the tx alone; so does DataScript."
    (let [conn (mem-conn)]
      (try
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :tag :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/many}])
        (d/transact conn [{:db/id -1 :name "a" :tag :x}])
        (let [tx-of (fn [db a] (:tx (first (d/datoms db :eavt
                                                     (:e (first (d/datoms db :avet :name "a")))
                                                     a))))
              before-name (tx-of @conn :name)
              before-tag  (tx-of @conn :tag)]
          (d/transact conn [{:db/id [:name "a"] :name "a" :tag :x}])
          (is (= before-name (tx-of @conn :name)) "cardinality-one keeps its tx")
          (is (= before-tag  (tx-of @conn :tag))  "cardinality-many keeps its tx"))
        (finally (d/release conn))))))

(deftest a-cardinality-one-supersession-reports-its-retraction
  (testing "overwriting a cardinality-one value writes a retraction of the old
            one into history at this transaction. It was written and not
            reported, so history contradicted :tx-data — a consumer seeing only
            the assertion cannot undo the old value without separately knowing
            the attribute's cardinality. Datomic and DataScript report the pair."
    (let [conn (utils/setup-db {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                                :keep-history? true :schema-flexibility :write})]
      (try
        (d/transact conn [{:db/ident :score :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:db/id 100 :score 1}])
        (let [r (d/transact conn [{:db/id 100 :score 2}])
              user (->> (:tx-data r)
                        (filter #(= :score (:a %)))
                        (mapv (juxt :e :a :v :added)))]
          (is (= [[100 :score 1 false] [100 :score 2 true]] user)
              "the retraction of the old value precedes the new assertion")
          (testing "and it names a retraction history actually holds"
            (is (some (fn [d] (and (= 1 (:v d)) (false? (:added d))))
                      (d/datoms (d/history @conn) :eavt 100 :score)))))
        (finally (d/release conn))))))

(deftest a-fresh-cardinality-one-assertion-reports-only-itself
  (testing "there is no previous value, so there is nothing to retract — the
            obvious way to break the clause above is to emit one anyway"
    (let [conn (mem-conn)]
      (try
        (d/transact conn [{:db/ident :score :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}])
        (let [r (d/transact conn [{:db/id 100 :score 1}])]
          (is (= [[100 :score 1 true]]
                 (->> (:tx-data r) (filter #(= :score (:a %)))
                      (mapv (juxt :e :a :v :added))))))
        (finally (d/release conn))))))
