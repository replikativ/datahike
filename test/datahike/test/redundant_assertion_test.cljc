(ns datahike.test.redundant-assertion-test
  "Asserting a datom the database already holds must change nothing.

   `di/-insert` is idempotent on `[e a v]` — it returns the receiver untouched.
   But `with-datom`'s assert branch ran its whole accumulator chain regardless,
   so a redundant assertion moved every DERIVED quantity while the index stood
   still.

   The visible failure is a schema install that is refused on its third run:

     transact #1  ->  :db.entity/attrs [:name]
     transact #2  ->  :db.entity/attrs [:name :name]     (committed, no error)
     transact #3  ->  THROWS :transact/schema, :invalid-updates
                      #:db.entity{:attrs [[:name :name] [:name]]}

   `update-schema` `conj`s into the many-valued schema attributes
   (`:db.entity/attrs`, `:db.entity/preds`, `:db.attr/preds`), so each redundant
   assertion appends another copy; the schema-update check then compares what you
   are declaring against the shape the previous run wrote, and refuses. Applying
   your schema on every startup — an ordinary, deliberately idempotent pattern —
   therefore works twice and fails forever after, including across a reconnect,
   because the mangled value is persisted.

   Nothing else is damaged: the indexes are correct, queries are unaffected, and
   the mangled spec still enforces (`[:name :name]` demands what `[:name]` does).
   And re-declaring the shape it now has is accepted but DOUBLES it again, so
   there is no fixpoint to converge on — recovery means retracting the schema
   datom, not re-declaring it.

   Only cardinality-MANY assertions are affected. Cardinality-one goes through
   `with-datom-upsert` (`transact-add`'s `upsert?` is `(not (multival? db a))`),
   retraction is guarded by its own `if-some`, and purge by `current?`. The
   damage needs an attribute that is card-many AND a schema attribute — which is
   exactly what an entity spec is."
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

(defn- spec-shape [conn]
  (:db.entity/attrs (get (:schema @conn) :spec)))

;; ---------------------------------------------------------------------------

(deftest an-idempotent-schema-install-stays-idempotent
  (testing "transacting the same entity spec repeatedly neither grows the schema
            nor starts failing — the regression this commit exists for"
    (let [conn (mem-conn)]
      (try
        (dotimes [i 5]
          (d/transact conn entity-spec)
          (is (= [:name] (spec-shape conn))
              (str "after transact #" (inc i) " the spec must still be [:name]")))
        (finally (d/release conn))))))

(deftest a-redundant-card-many-assertion-contributes-nothing
  (testing "the index already held the datom, so it must contribute nothing to
            the derived state.

            Stated over the USER datoms rather than over `:eavt` wholesale: the
            transaction itself is real work, and under `:keep-history? true` its
            tx entity puts a `:db/txInstant` datom into the current tree. That
            datom legitimately moves both the datom set and the sum, so a plain
            before/after comparison would be asserting something false."
    (doseq [keep-history? [false true]]
      (let [conn (utils/setup-db {:store {:backend :memory
                                          :id (java.util.UUID/randomUUID)}
                                  :keep-history? keep-history?
                                  :schema-flexibility :write})
            user-datoms (fn [db] (remove #(= :db/txInstant (:a %))
                                         (d/datoms db :eavt)))
            sum (fn [ds] (reduce (fn [h d] (+ h (hash d))) 0 ds))]
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
                (is (= (:max-eid before) (:max-eid after))
                    ":max-eid is unchanged")
                (is (= (sum (d/datoms after :eavt)) (:hash after))
                    ":hash still equals the sum over :eavt")
                (is (= (- (:hash after) (:hash before))
                       (- (sum (d/datoms after :eavt)) (sum (d/datoms before :eavt))))
                    "and it moved by exactly what the tx entity added — nothing
                     was attributed to the redundant assertion"))))
          (finally (d/release conn)))))))

(deftest a-genuinely-new-card-many-value-still-lands
  (testing "the guard must not swallow real assertions — the obvious way to
            break this fix is to gate too much"
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
          (let [db @conn
                tags (set (map :v (filter #(= :tag (:a %)) (d/datoms db :eavt))))]
            (is (= #{:x :y} tags) "both values are present")
            (is (not= before (:hash db)) "and the new one moved the sum")
            (is (= (reduce (fn [h d] (+ h (hash d))) 0 (d/datoms db :eavt))
                   (:hash db))
                "which is still exactly the sum over :eavt")))
        (finally (d/release conn))))))

(deftest duplicate-assertions-within-one-transaction
  (testing "the same datom twice in a single transaction — the redundant one is
            the second op of the same tx rather than a later one"
    (let [conn (mem-conn)]
      (try
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db/ident :tag :db/valueType :db.type/keyword
                           :db/cardinality :db.cardinality/many}])
        (d/transact conn [{:db/id -1 :name "a" :tag :x}
                          {:db/id -1 :name "a" :tag :x}])
        (let [db @conn]
          (is (= 1 (count (filter #(= :tag (:a %)) (d/datoms db :eavt))))
              "the index holds it once")
          (is (= (reduce (fn [h d] (+ h (hash d))) 0 (d/datoms db :eavt))
                 (:hash db))
              "and the sum counted it once"))
        (finally (d/release conn))))))

(deftest the-spec-still-enforces-after-repeated-installs
  (testing "whatever the schema shape, the entity spec must keep working — this
            is what made the old corruption inert and easy to miss"
    (let [conn (mem-conn)]
      (try
        (d/transact conn entity-spec)
        (d/transact conn entity-spec)
        (is (= [:name] (spec-shape conn)))
        (testing "an entity missing the required attribute is refused"
          (is (thrown? clojure.lang.ExceptionInfo
                       (d/transact conn [{:db/id -1 :db/ensure :spec}]))))
        (testing "and a valid one is accepted"
          (is (some? (d/transact conn [{:db/id -1 :name "ok" :db/ensure :spec}]))))
        (finally (d/release conn))))))
