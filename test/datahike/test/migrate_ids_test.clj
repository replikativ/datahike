(ns datahike.test.migrate-ids-test
  "The id-remap pre-pass.

   The properties here are the ones a bulk index build and a resumable import
   will rest on, so they are asserted directly rather than inferred from a
   successful round-trip: a round-trip can pass while the mapping is wrong in a
   way that only shows up on a populated target, which `import-db` refuses today
   and therefore never exercises."
  (:require [clojure.test :refer [deftest testing is]]
            [datahike.api :as d]
            [datahike.db.interface :as dbi]
            [datahike.migrate :as m]
            [datahike.migrate.ids :as ids]
            [datahike.test.utils :as utils]))

(defn- teardown
  "Local rather than `utils/teardown-db`, which derefs the connection AFTER
   releasing it and so throws :connection-has-been-released. `migrate_test` has
   the same local workaround."
  [conn]
  (let [cfg (:config @conn)]
    (d/release conn)
    (d/delete-database cfg)))

(defn- mem-cfg [extra]
  (merge {:store {:backend :memory :id (java.util.UUID/randomUUID)}
          :keep-history? true :schema-flexibility :write}
         extra))

(defn- source-conn []
  (let [c (utils/setup-db (mem-cfg {}))]
    (d/transact c [{:db/ident :name :db/valueType :db.type/string
                    :db/cardinality :db.cardinality/one}
                   {:db/ident :pal :db/valueType :db.type/ref
                    :db/cardinality :db.cardinality/one}])
    (d/transact c [{:db/id -1 :name "a"} {:db/id -2 :name "b" :pal -1}])
    c))

(defn- export! [conn]
  (let [p (str (System/getProperty "java.io.tmpdir") "/dh-ids-" (utils/get-time))]
    (m/export-db @conn p {:history? true})
    p))

(defn- mapping-for [conn path]
  (let [{:keys [manifest] :as dump} (#'m/open-dump path)
        db @conn]
    [(ids/build-mapping {:schema (:schema manifest)
                         :system-entities (dbi/-system-entities db)
                         :max-eid (:max-eid db)
                         :max-tx (:max-tx db)}
                        (fn [rf init] (#'m/reduce-dump-records dump rf init)))
     (:schema manifest)
     (vec (#'m/reduce-dump-records dump (fn [acc r] (conj acc r)) []))]))

(deftest empty-target-maps-to-identity
  (testing "into a freshly created db, allocation reproduces the source's own ids

            Worth pinning because it is the case a bulk build can shortcut: if
            the mapping is the identity there is nothing to rewrite before
            sorting."
    (let [src (source-conn)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))
          [mapping _ _] (mapping-for tgt path)]
      (is (ids/identity-mapping? mapping)
          (str "expected identity, got " (pr-str (select-keys mapping [:eids :tids]))))
      (teardown src) (teardown tgt))))

(deftest populated-target-shifts-above-existing-ids
  (testing "the case import-db currently refuses — every allocated id must land
            ABOVE anything already in the target, or a restore silently
            overwrites unrelated entities"
    (let [src (source-conn)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))
          _ (d/transact tgt [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}
                             {:db/ident :pal :db/valueType :db.type/ref
                              :db/cardinality :db.cardinality/one}])
          _ (d/transact tgt [{:db/id -1 :name "pre-existing"}])
          db @tgt
          existing (into #{} (map :e) (d/datoms db :eavt))
          sys (set (dbi/-system-entities db))
          [mapping _ _] (mapping-for tgt path)
          allocated (remove (fn [[k _]] (contains? sys k)) (:eids mapping))]
      (is (seq allocated) "precondition: something was allocated")
      (testing "no allocated id collides with one already present"
        (is (empty? (filter (fn [[_ v]] (contains? existing v)) allocated))))
      (testing "every allocated id is above the target's max-eid"
        (is (every? (fn [[_ v]] (> v (:max-eid db))) allocated)))
      (testing "and the mapping is injective — two source entities never merge"
        (let [vs (vals (:eids mapping))]
          (is (= (count vs) (count (distinct vs))))))
      (teardown src) (teardown tgt))))

(deftest ref-values-map-with-their-targets
  (testing "a ref VALUE is an entity id and must be rewritten to the same new id
            as the entity it names.

            This is the failure that would survive every count-based check: the
            datom count matches, the digest over [a v op] would even match if v
            were left alone, and the graph is silently rewired."
    (let [src (source-conn)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))
          _ (d/transact tgt [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}
                             {:db/ident :pal :db/valueType :db.type/ref
                              :db/cardinality :db.cardinality/one}])
          _ (d/transact tgt [{:db/id -1 :name "pre-existing"}])
          [mapping schema raw] (mapping-for tgt path)
          mapped (mapv #(ids/apply-mapping mapping schema %) raw)
          name-of (fn [recs e] (some (fn [[e' a v _ op]]
                                       (when (and (= e' e) (= a :name) op) v))
                                     recs))
          pal-pair (fn [recs]
                     (some (fn [[e a v _ op]]
                             (when (and (= a :pal) op)
                               [(name-of recs e) (name-of recs v)]))
                           recs))]
      (is (= ["b" "a"] (pal-pair raw)) "precondition: b's pal is a, before mapping")
      (is (= ["b" "a"] (pal-pair mapped)) "…and still after mapping")
      ;; The above two ALONE pass with `apply-mapping` stubbed to identity — they
      ;; only assert internal consistency, which the unmapped records already
      ;; satisfy. Verified by stubbing. So assert the ids actually MOVED, which is
      ;; the thing the test claims to guard.
      (testing "and the ids really were rewritten, not passed through"
        (is (not= (mapv first raw) (mapv first mapped))
            "entity ids must differ from the source's")
        (let [ref-raw (some (fn [[_ a v _ op]] (when (and (= a :pal) op) v)) raw)
              ref-mapped (some (fn [[_ a v _ op]] (when (and (= a :pal) op) v)) mapped)]
          (is (not= ref-raw ref-mapped) "the ref VALUE must have been rewritten too")
          (is (= (get (:eids mapping) ref-raw) ref-mapped)
              "…to exactly what the mapping says")))
      (teardown src) (teardown tgt))))

(deftest mapping-is-deterministic
  (testing "same dump + same target maxima ⇒ same mapping.

            This is the property resumability rests on: a partial import can only
            be continued if re-deriving the mapping yields the ids already
            written."
    (let [src (source-conn)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))
          _ (d/transact tgt [{:db/ident :name :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}])
          [m1 _ _] (mapping-for tgt path)
          [m2 _ _] (mapping-for tgt path)]
      ;; Both re-open the dump and re-scan, so this is not `f(x)` twice on one
      ;; in-memory vector — it is the property resumability needs: re-deriving
      ;; from the artifact yields the same ids.
      (is (= (:eids m1) (:eids m2)))
      (is (= (:tids m1) (:tids m2)))
      (is (= (:next-eid m1) (:next-eid m2)))
      (is (seq (:eids m1)) "precondition: the mapping is not vacuously empty")
      (testing "and a DIFFERENT target maximum yields a different mapping —
                otherwise the equality above proves nothing"
        (let [other (utils/setup-db (mem-cfg {}))]
          (d/transact other [{:db/ident :filler :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/one}])
          (d/transact other [{:db/id -1 :filler "pad"}])
          (let [[m3 _ _] (mapping-for other path)]
            (is (not= (:eids m1) (:eids m3))
                "a populated target must allocate different ids"))
          (teardown other)))
      (teardown src) (teardown tgt))))

(deftest non-ref-longs-are-left-alone
  (testing "a :db.type/long that happens to equal an entity id must NOT be
            rewritten. The guard is the schema, not the shape of the number —
            getting this wrong corrupts ordinary data that looks like an id."
    (let [schema {:age {:db/valueType :db.type/long}
                  :pal {:db/valueType :db.type/ref}}
          mapping {:eids {3 99} :tids {}}]
      (is (= [1 :age 3 5 true] (ids/apply-mapping mapping schema [1 :age 3 5 true]))
          "a long value of 3 stays 3")
      (is (= [1 :pal 99 5 true] (ids/apply-mapping mapping schema [1 :pal 3 5 true]))
          "a ref value of 3 becomes 99"))))

;; ---------------------------------------------------------------------------
;; :xform — the general import-time rewrite hook, a transducer over records

(deftest translate-renames-attributes
  (testing "an attribute rename is just a translator; no special facility needed.

            Note it must rewrite TWO positions: the attribute of a data datom,
            and the VALUE of the schema datom [e :db/ident :name] that declares
            it. Missing the second leaves the dump's own schema pointing at the
            old name — which is exactly the sort of thing a special-purpose
            :rename option would hide, and a general hook makes visible."
    (let [src (source-conn)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))]
      (let [rep (m/import-db tgt path
                             {:verify? false
                              :xform (map (fn [[e a v t op]]
                                            [e
                                             (if (= a :name) :moniker a)
                                             (if (and (= a :db/ident) (= v :name)) :moniker v)
                                             t op]))})]
        (is (true? (:transformed? rep)))
        (is (= #{"a" "b"} (set (map first (d/q '[:find ?n :where [?e :moniker ?n]] @tgt))))
            "renamed attribute landed")
        (is (empty? (d/q '[:find ?n :where [?e :name ?n]] @tgt))
            "and the old name is absent"))
      (teardown src) (teardown tgt))))

(deftest xform-can-drop-records-without-failing-verification
  (testing "returning nil drops a record, and the expected count is adjusted.

            The point of the adjustment: a deliberate drop must not be reported
            as corruption. A verification that cries wolf on correct usage is one
            people switch off, and then it is not there when it matters."
    (let [src (source-conn)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))
          dropped (atom 0)
          rep (m/import-db tgt path
                           {:xform (remove (fn [[_ a _ _ _]]
                                             (when (= a :pal)
                                               (swap! dropped inc)
                                               true)))})]
      (is (pos? @dropped) "precondition: something was dropped")
      (is (= @dropped (:dropped rep)) "the report counts the drops")
      (is (true? (:verified? rep))
          "verification PASSES — the drop is accounted for, not treated as loss")
      (is (empty? (d/q '[:find ?e ?p :where [?e :pal ?p]] @tgt))
          "the dropped attribute really is absent")
      (is (= #{"a" "b"} (set (map first (d/q '[:find ?n :where [?e :name ?n]] @tgt))))
          "everything else still landed")
      (teardown src) (teardown tgt))))

(deftest xform-rewrites-values
  (testing "value rewriting — the other half of a schema migration"
    (let [src (source-conn)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))]
      (m/import-db tgt path
                   {:verify? false
                    :xform (map (fn [[e a v t op]]
                                  [e a (if (= a :name) (clojure.string/upper-case v) v) t op]))})
      (is (= #{"A" "B"} (set (map first (d/q '[:find ?n :where [?e :name ?n]] @tgt)))))
      (teardown src) (teardown tgt))))

(deftest untransformed-import-is-unchanged
  (testing "no :xform ⇒ the report says so and nothing is dropped — the hook
            must be inert when unused"
    (let [src (source-conn)
          path (export! src)
          tgt (utils/setup-db (mem-cfg {}))
          rep (m/import-db tgt path {})]
      (is (false? (:transformed? rep)))
      (is (zero? (:dropped rep)))
      (is (true? (:verified? rep)))
      (teardown src) (teardown tgt))))

(deftest tx-meta-does-not-split-the-transaction-entity
  (testing "user :tx-meta attributes land on the SAME entity as :db/txInstant.

            `flush-tx-meta` writes arbitrary user attributes onto the transaction
            entity, so a rule keyed on `ds/meta-attr?` — a closed set of five
            idents — sent :db/txInstant through :tids and :author through :eids,
            splitting one entity in two and orphaning the metadata onto an id
            nothing else references. Silent. The discriminator is structural:
            `e` names the transaction exactly when `e` = `t`."
    (let [c (utils/setup-db (mem-cfg {:schema-flexibility :read}))]
      (d/transact c {:tx-data [{:db/id 1000 :name "x"}] :tx-meta {:author "alice"}})
      (let [recs (mapv (juxt :e :a :v :tx :added) (d/datoms @c :eavt))
            mapping (ids/build-mapping {:schema {} :system-entities #{}
                                        :max-eid 600000000 :max-tx 900000000}
                                       (fn [rf init] (reduce rf init recs)))
            tx-recs (filter (fn [[e _ _ t _]] (= e t)) recs)
            mapped-es (distinct (map #(first (ids/apply-mapping mapping {} %)) tx-recs))]
        (is (< 1 (count tx-recs)) "precondition: the tx entity has >1 datom")
        (is (= 1 (count mapped-es))
            (str "the transaction entity was SPLIT across " (pr-str mapped-es)))
        (is (contains? (set (vals (:tids mapping))) (first mapped-es))
            "and it landed in the transaction id space, not the entity one"))
      (teardown c))))

(deftest ref-to-a-transaction-is-not-reallocated
  (testing "a ref VALUE naming a transaction must resolve through :tids.

            Allocating it into :eids produces a reference to an id nothing was
            assigned — a dangling pointer that no count or digest would notice."
    (let [schema {:mytx {:db/valueType :db.type/ref}}
          records [[1 :mytx 536870913 536870913 true]
                   [536870913 :db/txInstant "now" 536870913 true]]
          mapping (ids/build-mapping {:schema schema :system-entities #{}
                                      :max-eid 100 :max-tx 500000000}
                                     (fn [rf init] (reduce rf init records)))
          [_ _ v' _ _] (ids/apply-mapping mapping schema (first records))
          tx-target (get (:tids mapping) 536870913)]
      (is (= tx-target v')
          (str "ref to a transaction resolved to " v' " but the tx became " tx-target))
      (is (not (contains? (:eids mapping) 536870913))
          "and it was not allocated an entity id at all"))))
