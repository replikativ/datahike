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

   SECONDARY INDICES STILL RECEIVE THE ASSERTION, and that is deliberate rather
   than an oversight. `update-secondary-indices` passes `:tx-meta` for the
   in-progress transaction — `:db/txInstant`, `:db.valid/from`, `:db.valid/to` —
   which `IValidTimeAware` adapters persist. Re-asserting an identical value with
   a different valid-time window is a real SCD2 version: the value is unchanged
   but the window is not. The primary indexes are idempotent on `[e a v]`; a
   bitemporal index keys on more than that, so the rule is narrower than \"a
   no-op changes nothing\"."
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
