(ns datahike.test.migrate-tuple-refs-test
  "Entity ids that a dump carries somewhere other than a ref attribute's value.

   Two places, both missed by the import and by `verify-against`:

   * a REF ELEMENT of a tuple — `:db/tupleTypes [:db.type/ref …]`, or a composite
     `:db/tupleAttrs` with a ref component. The value is an entity id like any
     other, so it has to move with the entity it names. It was written verbatim,
     so under `:eids :allocate` into a populated target, or `:offset`, the tuple
     pointed at whatever entity happened to hold the SOURCE's number.
   * a schema value under `:attribute-refs? true` — `:db/valueType`,
     `:db/cardinality`, `:db/unique` hold the id of a system entity. The dump
     holds the source's id and records the source's numbering in the manifest's
     `:system-idents`; `pull` answers the keyword. `verify-against` compared the
     two directly, so tier 3 reported every schema entity as `:field-mismatch`
     and no attribute-refs import could ever verify."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.db.interface :as dbi]
            [datahike.migrate :as m]
            [datahike.migrate.ids :as ids]
            [datahike.test.utils :as utils]))

(defn- teardown [conn]
  (let [cfg (:config @conn)]
    (d/release conn)
    (d/delete-database cfg)))

(defn- mem-cfg [extra]
  (merge {:store {:backend :memory :id (java.util.UUID/randomUUID)}
          :keep-history? true :schema-flexibility :write}
         extra))

(def ^:private schema
  [{:db/ident :name :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
   {:db/ident :pal :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   ;; heterogeneous tuple with a ref element
   {:db/ident :pt :db/valueType :db.type/tuple
    :db/tupleTypes [:db.type/ref :db.type/long] :db/cardinality :db.cardinality/one}
   ;; composite tuple with a ref component, unique — the usual reason to have one
   {:db/ident :pal+name :db/valueType :db.type/tuple
    :db/tupleAttrs [:pal :name] :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}])

(defn- eid-of [db n] (d/q '[:find ?e . :in $ ?n :where [?e :name ?n]] db n))

(defn- source-conn
  "b points at a through `:pal`, `:pt` and the derived `:pal+name`; later it is
   re-pointed at c, so the history holds a retracted tuple naming a as well."
  [attribute-refs?]
  (let [c (utils/setup-db (mem-cfg {:attribute-refs? attribute-refs?}))]
    (d/transact c schema)
    (d/transact c [{:name "a"} {:name "c"}])
    (let [a (eid-of @c "a")]
      (d/transact c [{:name "b" :pal a :pt [a 8]}]))
    (let [cc (eid-of @c "c")]
      (d/transact c [{:db/id [:name "b"] :pal cc :pt [cc 9]}]))
    c))

(defn- export! [conn]
  (let [p (str (System/getProperty "java.io.tmpdir") "/dh-tuple-refs-" (utils/get-time)
               "-" (java.util.UUID/randomUUID))]
    (m/export-db @conn p {:history? true})
    p))

(defn- populate!
  "Occupy the ids the source used, so an allocating import has to move them."
  [conn]
  (d/transact conn [{:db/ident :zzz :db/valueType :db.type/string
                     :db/cardinality :db.cardinality/one}])
  (d/transact conn (vec (for [i (range 6)] {:zzz (str "filler-" i)}))))

(defn- assert-tuple-refs-follow [db label]
  (let [b (eid-of db "b") cc (eid-of db "c") a (eid-of db "a")
        cur (d/pull db '[:pal :pt :pal+name] b)]
    (testing (str label ": current tuples name the TARGET's entities")
      (is (= cc (get-in cur [:pal :db/id])) "precondition: the plain ref moved")
      (is (= [cc 9] (:pt cur)))
      (is (= [cc "b"] (:pal+name cur))))
    (testing (str label ": so does the retracted history")
      (let [hist (d/q '[:find ?v ?op :in $ ?b :where [?b :pt ?v _ ?op]]
                      (d/history db) b)]
        (is (= #{[[a 8] true] [[a 8] false] [[cc 9] true]} hist))))
    (testing (str label ": and a lookup by the composite finds the entity")
      (is (= b (:db/id (d/pull db '[:db/id] [:pal+name [cc "b"]])))))))

(deftest tuple-ref-elements-follow-their-entities
  (doseq [ar? [false true]]
    (let [src (source-conn ar?)
          path (export! src)]
      (doseq [policy [:allocate :offset]]
        (testing (str ":attribute-refs? " ar? ", " policy " into a populated target")
          (let [tgt (utils/setup-db (mem-cfg {:attribute-refs? ar?}))]
            (populate! tgt)
            (m/import-db tgt path {:merge? true :eids policy})
            (is (not= (eid-of @src "a") (eid-of @tgt "a"))
                "precondition: ids actually moved")
            (assert-tuple-refs-follow @tgt (str policy " ar=" ar?))
            (teardown tgt))))
      (testing (str ":attribute-refs? " ar? ", :offset into a fresh target")
        ;; moves ids only under attribute-refs (the system schema raises the
        ;; target's max-eid); without, the offset is zero
        (let [tgt (utils/setup-db (mem-cfg {:attribute-refs? ar?}))
              r (m/import-db tgt path {:eids :offset})]
          (is (:verified? r))
          (when ar?
            (is (not= (eid-of @src "a") (eid-of @tgt "a"))
                "precondition: ids actually moved"))
          (assert-tuple-refs-follow @tgt (str "offset fresh ar=" ar?))
          (is (:ok? (m/verify-against @tgt path))
              "verify-against compares tuple values without their ids")
          (teardown tgt)))
      (testing (str ":attribute-refs? " ar? ", :preserve and :allocate into a fresh target")
        (doseq [policy [:preserve :allocate]]
          (let [tgt (utils/setup-db (mem-cfg {:attribute-refs? ar?}))]
            (m/import-db tgt path {:eids policy})
            (is (= (eid-of @src "a") (eid-of @tgt "a")))
            (assert-tuple-refs-follow @tgt (str policy " ar=" ar?))
            (is (:ok? (m/verify-against @tgt path)))
            (teardown tgt))))
      (teardown src))))

(deftest index-build-path-follows-tuple-refs
  (testing "the bulk path computes its mapping up front — a tuple's ref element
            has to allocate and remap there too"
    (let [src (source-conn false)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))
          _ (populate! tgt)
          {:keys [manifest] :as dump} (#'m/open-dump path)
          db @tgt
          reduce-recs (fn [rf init] (#'m/reduce-dump-records dump rf init))
          mapping (ids/build-mapping {:schema (:schema manifest)
                                      :system-entities (dbi/-system-entities db)
                                      :max-eid (:max-eid db) :max-tx (:max-tx db)}
                                     reduce-recs)
          raw (reduce-recs conj [])
          mapped (mapv #(ids/apply-mapping mapping (:schema manifest) %) raw)
          src-a (eid-of @src "a")
          tgt-a (get (:eids mapping) src-a)]
      (is (and tgt-a (not= src-a tgt-a)) "precondition: a was moved")
      (doseq [[attr tuple] [[:pt [tgt-a 8]] [:pal+name [tgt-a "b"]]]]
        (is (some (fn [[_ at v _ op]] (and (= at attr) op (= v tuple))) mapped)
            (str attr " names a's NEW id"))
        (is (not-any? (fn [[_ at v _ _]] (and (= at attr) (= (first v) src-a))) mapped)
            (str attr " keeps no source id")))
      (teardown tgt)
      (testing "and an index-build import round-trips and verifies"
        (let [tgt (utils/setup-db (mem-cfg {}))]
          (m/import-db tgt path {:build-indexes? true})
          (assert-tuple-refs-follow @tgt "index-build")
          (is (:ok? (m/verify-against @tgt path)))
          (teardown tgt)))
      (teardown src))))

(deftest verify-against-attribute-refs-schema-values
  (testing "an attribute-refs restore verifies on all three tiers: schema values
            are translated through the dump's own :system-idents, not compared
            as raw ids against pull's keywords"
    (doseq [ar? [false true]]
      (let [src (utils/setup-db (mem-cfg {:attribute-refs? ar?}))
            _ (d/transact src [{:db/ident :name :db/valueType :db.type/string
                                :db/cardinality :db.cardinality/one
                                :db/unique :db.unique/identity}
                               {:db/ident :tags :db/valueType :db.type/keyword
                                :db/cardinality :db.cardinality/many}])
            _ (d/transact src [{:name "x" :tags [:p :q]}])
            path (export! src)
            tgt (utils/setup-db (mem-cfg {:attribute-refs? ar?}))
            _ (m/import-db tgt path)
            r (m/verify-against @tgt path)]
        (is (pos? (get-in r [:tier3 :sampled])) "precondition: tier 3 sampled something")
        (is (:ok? (:tier3 r)) (str "ar=" ar? " " (pr-str (:tier3 r))))
        (is (:match? (:tier2 r)) (str "ar=" ar?))
        (is (:ok? r) (str "ar=" ar?))
        (teardown src) (teardown tgt)))))

(deftest verify-against-still-finds-real-differences
  (testing "what is taken out of the comparison is only the ids: a changed
            schema value and a changed non-ref tuple element are still caught"
    (doseq [ar? [false true]]
      (let [src (source-conn ar?)
            path (export! src)
            tgt (utils/setup-db (mem-cfg {:attribute-refs? ar?}))]
        (m/import-db tgt path)
        (is (:ok? (m/verify-against @tgt path)) "precondition: verifies before the change")
        (d/transact tgt [{:db/id [:name "b"] :pt [(eid-of @tgt "c") 10]}
                         {:db/id [:db/ident :name] :db/doc "changed"}])
        (is (= #{{:unique [:db/ident :name] :error :field-mismatch}
                 {:unique [:name "b"] :error :field-mismatch}}
               (set (get-in (m/verify-against @tgt path) [:tier3 :diffs])))
            (str "ar=" ar?))
        (teardown src) (teardown tgt)))))

(deftest verify-against-a-unique-value-replaced-in-history
  (testing "tier 3 samples from a history dump, so it can pick a unique value
            that a later transaction replaced. That value names nothing live —
            not a failure, and it was reported as :missing-in-live"
    (let [src (utils/setup-db (mem-cfg {}))
          _ (d/transact src [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db/unique :db.unique/identity}])
          _ (d/transact src [{:name "x"}])
          _ (d/transact src [{:db/id [:name "x"] :name "y"}])
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))]
      (m/import-db tgt path)
      (let [r (m/verify-against @tgt path)]
        (is (:ok? (:tier3 r)) (pr-str (:tier3 r)))
        (is (:ok? r)))
      (teardown src) (teardown tgt))))

(deftest non-id-values-in-a-ref-slot-are-carried-unchanged
  (testing "the transactor's :db.type/id spec lets a keyword or a string into a
            tuple's ref slot. Neither names an entity: it must not allocate a
            fresh empty entity (:allocate), throw (:offset), or be rewritten —
            the tuple is carried as the source holds it"
    (doseq [ar? [false true]]
      (let [src (source-conn ar?)
            _ (d/transact src [{:name "k" :pt [:some/kw 8]} {:name "s" :pt ["str" 9]}])
            path (export! src)
            odd (fn [db] (into {} (d/q '[:find ?n ?t :where [?e :name ?n] [?e :pt ?t]
                                         [(contains? #{"k" "s"} ?n)]] db)))]
        (is (= {"k" [:some/kw 8] "s" ["str" 9]} (odd @src)) "precondition")
        (doseq [policy [:allocate :offset :preserve]]
          (let [tgt (utils/setup-db (mem-cfg {:attribute-refs? ar?}))]
            ;; :preserve keeps source ids, so it goes into a fresh target
            (if (= :preserve policy)
              (m/import-db tgt path {:eids policy})
              (do (populate! tgt)
                  (m/import-db tgt path {:merge? true :eids policy})))
            (is (= {"k" [:some/kw 8] "s" ["str" 9]} (odd @tgt)) (str policy " ar=" ar?))
            (assert-tuple-refs-follow @tgt (str policy " ar=" ar?))
            (teardown tgt)))
        (testing "and the index-build path likewise"
          (when-not ar?
            (let [tgt (utils/setup-db (mem-cfg {}))]
              (m/import-db tgt path {:build-indexes? true})
              (is (= {"k" [:some/kw 8] "s" ["str" 9]} (odd @tgt)))
              (is (:ok? (m/verify-against @tgt path)))
              (teardown tgt))))
        (teardown src)))))

(deftest tier-3-compares-current-entities-and-says-how-many
  (testing "every unique value was replaced later in the history. Tier 3 used to
            pick the replaced values, skip them all and report `:sampled 25`
            having compared nothing — so a corrupted restore passed"
    (let [src (utils/setup-db (mem-cfg {}))
          _ (d/transact src [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db/unique :db.unique/identity}
                             {:db/ident :age :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}
                             ;; a unique REF: never comparable, must not use up the budget
                             {:db/ident :buddy :db/valueType :db.type/ref
                              :db/cardinality :db.cardinality/one
                              :db/unique :db.unique/value}])
          _ (d/transact src (vec (for [i (range 30)] {:name (str "x" i) :age i})))
          _ (d/transact src (vec (for [i (range 29)]
                                   {:db/id [:name (str "x" i)] :buddy [:name (str "x" (inc i))]})))
          _ (d/transact src (vec (for [i (range 30)] {:db/id [:name (str "x" i)] :name (str "y" i)})))
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))]
      (m/import-db tgt path)
      (let [t3 (:tier3 (m/verify-against @tgt path))]
        (is (:ok? t3) (pr-str t3))
        (is (= 25 (:sampled t3)) "25 current entities actually compared"))
      (d/transact tgt (vec (for [i (range 30)] {:db/id [:name (str "y" i)] :age (+ 100 i)})))
      (let [t3 (:tier3 (m/verify-against @tgt path))]
        (is (not (:ok? t3)) "the corruption is seen")
        (is (= :field-mismatch (:error (first (:diffs t3))))))
      (teardown src) (teardown tgt))))

(deftest tier-2-counts-tuple-ref-slots-structurally
  (testing "masked out of the digest, a tuple's ref elements are counted per
            [attribute slot] and into out-degree, like a plain ref"
    (let [schema {:pt {:db/valueType :db.type/tuple
                       :db/tupleTypes [:db.type/ref :db.type/long]}}
          slots-of #(ids/tuple-ref-slots schema %)
          norm (fn [_ v] v)
          step #(#'m/fp-step %1 %2 (constantly false) norm slots-of)
          fp (-> (#'m/fp-init) (step [10 :pt [5 1] true]) (step [11 :pt [:kw 2] true]))]
      (is (= {[:pt 0] 1} (:refc fp)) "the keyword is not an id")
      (is (= {10 1} (:outd fp))))))

(deftest verify-against-a-renamed-attribute-under-attribute-refs
  (testing "records of a renamed attribute name it by entity (AttrRef); verify
            reads them under the attribute's current ident"
    (let [src (utils/setup-db (mem-cfg {:attribute-refs? true}))
          _ (d/transact src [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db/unique :db.unique/identity}
                             {:db/ident :old :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}])
          _ (d/transact src [{:name "x" :old 1}])
          aid (:db/id (d/entity @src :old))
          ;; retract-then-add: the spelling that retires the old ident (#966)
          _ (d/transact src [[:db/retract aid :db/ident :old] [:db/add aid :db/ident :new]])
          _ (d/transact src [{:db/id [:name "x"] :new 2}])
          path (export! src)
          tgt (utils/setup-db (mem-cfg {:attribute-refs? true}))]
      (m/import-db tgt path)
      (let [r (m/verify-against @tgt path)]
        (is (:ok? r) (pr-str (select-keys r [:tier1 :tier2 :tier3]))))
      (teardown src) (teardown tgt))))
