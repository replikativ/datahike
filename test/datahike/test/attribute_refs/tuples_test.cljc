(ns datahike.test.attribute-refs.tuples-test
  "Composite `:db/tupleAttrs` and tuple validation under `:attribute-refs?`.

   DIFFERENTIAL by construction: every test runs the same schema and the same
   data through both modes and asserts they agree. That shape is deliberate —
   the bug these pin was not a wrong answer but a MISSING one, and only a
   comparison against the default mode makes an absence visible. Each assertion
   below passed with `:attribute-refs? false` and failed with it true.

   Root cause, common to both halves: a datom's `a` is a numeric ref in this
   mode, while `rschema` and `schema` are keyed by ident. Code that resolved
   before looking up was correct; code that did not silently found nothing.
   `datahike.test.tuples-test` covers the same ground in default mode only,
   which is why this went unnoticed."
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]))

#?(:cljs (def Throwable js/Error))

(def composite-schema
  [{:db/ident :t/a :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   {:db/ident :t/b :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   {:db/ident :t/ab :db/valueType :db.type/tuple
    :db/tupleAttrs [:t/a :t/b]
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}])

(def hetero-schema
  [{:db/ident :t/ht :db/valueType :db.type/tuple
    :db/tupleTypes [:db.type/keyword :db.type/keyword]
    :db/cardinality :db.cardinality/one}])

(defn- conn-with [schema attribute-refs?]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? false
             :attribute-refs? attribute-refs?}]
    (d/create-database cfg)
    (doto (d/connect cfg) (d/transact schema))))

(defn- both-modes
  "`(f conn)` under both modes, as `{false … true …}`, so a test can simply
   assert the two agree."
  [schema f]
  (into {} (for [refs? [false true]]
             [refs? (f (conn-with schema refs?))])))

(defn- attr-ref
  "The numeric entity id of an attribute — Datahike has no `entid`, and the ref
   IS what a datom's `a` looks like in this mode, so the tests need it to write
   the form that used to slip past validation."
  [db attr]
  (:db/id (d/entity db attr)))

(defn- eid-of-x
  "The entity carrying `:t/a :x`, resolved by QUERY rather than from `:tempids`
   — the tempid map is keyed by the tempid, and a wrong extraction there yields
   nil, which reads as a missing composite and would frame a passing fix as
   broken."
  [db]
  (d/q '[:find ?e . :where [?e :t/a :x]] db))

(defn- composite-of [db e]
  ;; by ident: `:components` accepts either representation, so one call works
  ;; in both modes
  (:v (first (d/datoms db {:index :eavt :components [e :t/ab]}))))

;; ---------------------------------------------------------------------------

(deftest composite-tuples-are-derived
  (let [r (both-modes composite-schema
                      (fn [conn]
                        (d/transact conn [{:t/a :x :t/b :y}])
                        (let [db (d/db conn)]
                          {:composite (composite-of db (eid-of-x db))
                           :in-avet (count (d/datoms (d/db conn)
                                                     {:index :avet :components [:t/ab]}))})))]
    (is (= (get r false) (get r true))
        "the whole point: attribute-refs must derive what the default mode derives")
    (is (= [:x :y] (:composite (get r true)))
        "the composite datom was never written at all in this mode")
    (is (= 1 (:in-avet (get r true)))
        "and so nothing was indexed for it, which is what a lookup ref needs")))

(deftest a-derived-composite-is-addressable-as-a-lookup-ref
  (let [r (both-modes composite-schema
                      (fn [conn]
                        (d/transact conn [{:t/a :x :t/b :y}])
                        (try (some? (d/pull (d/db conn) '[*] [:t/ab [:x :y]]))
                             (catch Throwable _ :unresolvable))))]
    (is (= (get r false) (get r true)))
    (is (true? (get r true)))))

(deftest uniqueness-on-a-composite-is-enforced
  ;; The half that loses data rather than merely omitting it: with no composite
  ;; datom there is nothing for :db.unique/identity to match, so a second write
  ;; of the same component values made a SECOND entity instead of upserting.
  (let [r (both-modes composite-schema
                      (fn [conn]
                        (d/transact conn [{:t/a :x :t/b :y}])
                        (d/transact conn [{:t/a :x :t/b :y}])
                        (count (d/q '[:find ?e :where [?e :t/a :x]] (d/db conn)))))]
    (is (= (get r false) (get r true)))
    (is (= 1 (get r true))
        "two entities here means the uniqueness constraint was never applied")))

(deftest retracting_a_component_updates_the_composite
  (let [r (both-modes composite-schema
                      (fn [conn]
                        (d/transact conn [{:t/a :x :t/b :y}])
                        (let [e (eid-of-x (d/db conn))]
                          (d/transact conn [[:db/retract e :t/b :y]])
                          (composite-of (d/db conn) e))))]
    (is (= (get r false) (get r true)))
    (is (= [:x nil] (get r true))
        "the composite has to track its components in both directions")))

(deftest a-composite-cannot-be-written-by-hand
  ;; Guarded by `check-tuple`, which resolved nothing and so guarded nothing:
  ;; addressing the attribute by its numeric ref walked straight past it.
  (doseq [refs? [false true]]
    (testing (str ":attribute-refs? " refs?)
      (let [conn (conn-with composite-schema refs?)
            db (d/db conn)
            _ (d/transact conn [{:t/a :x :t/b :y}])
            e (eid-of-x (d/db conn))]
        (is (thrown-with-msg? Throwable #"Can.t modify tuple attrs directly"
                              (d/transact conn [[:db/add e :t/ab [:p :q]]]))
            "by ident")
        (when refs?
          (is (thrown-with-msg? Throwable #"Can.t modify tuple attrs directly"
                                (d/transact conn [[:db/add e (attr-ref db :t/ab) [:p :q]]]))
              "and by numeric ref — the form that used to be accepted"))))))

(deftest heterogeneous-tuple-arity-is-validated
  (doseq [refs? [false true]]
    (testing (str ":attribute-refs? " refs?)
      (let [conn (conn-with hetero-schema refs?)
            db (d/db conn)]
        (is (thrown-with-msg? Throwable #"Cannot store heterogeneous tuple"
                              (d/transact conn [{:t/ht [:only-one]}]))
            "too few values, addressed by ident in an entity map")
        (when refs?
          (is (thrown-with-msg? Throwable #"Cannot store heterogeneous tuple"
                                (d/transact conn [[:db/add -1 (attr-ref db :t/ht) [:a :b :c :d]]]))
              "too many values, addressed by numeric ref"))))))
