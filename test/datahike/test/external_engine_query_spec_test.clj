(ns datahike.test.external-engine-query-spec-test
  "The external-engine executor lets a secondary index own its query-spec format
   via an optional `:query-spec-fn` in the `:datahike/external-engine` metadata,
   with a backward-compatible `{:query :field}` default."
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datahike.api :as d]
   [datahike.index.secondary :as sec]
   [datahike.index.entity-set :as es]
   [datahike.query :as q]
   [datahike.query.execute]))

;; External-engine clauses are a planner feature: the planner recognizes the
;; `:datahike/external-engine` metadata and generates the op. The base
;; (relational) engine has no such mechanism, so under `persistent-set-test-
;; base-engine` (which disables the planner globally) the clause is evaluated as
;; an ordinary fn call and can't bind to `[[?e]]`. Force the planner on for the
;; d/q-driven tests here.
(use-fixtures :each (fn [f] (binding [q/*disable-planner* false] (f))))

(def ^:private build @#'datahike.query.execute/external-query-spec)

;; ---- minimal self-contained secondary index for the join regression ----
;; Stores the eids it is fed; -search returns all of them. Enough to exercise
;; the external-engine :filter execution path end-to-end via datalog.
(defrecord AllEidsIndex [state attrs]
  sec/ISecondaryIndex
  (-search [_ _query-spec entity-filter]
    (let [bs (es/entity-bitset)]
      (doseq [eid @state
              :when (or (nil? entity-filter)
                        (es/entity-bitset-contains? entity-filter (long eid)))]
        (es/entity-bitset-add! bs (long eid)))
      bs))
  (-estimate [_ _] (count @state))
  (-can-order? [_ _ _] false)
  (-slice-ordered [_ _ _ _ _ _] nil)
  (-indexed-attrs [_] attrs)
  (-transact [this {:keys [datom added?]}]
    (when added? (swap! state conj (long (nth datom 0))))
    this))

(defonce ^:private register-all-eids
  (sec/register-index-type! :test/all-eids
                            (fn [config _db]
                              (->AllEidsIndex (atom #{}) (set (:db.secondary/attrs config))))))

;; :filter-mode foreign var — binds ?e to each entity the index produces.
(defn ^{:datahike/external-engine
        {:index-key 0 :binding-columns [:entity-id] :input-vars :all-bound
         :cost-model (fn [_db _idx _args _n] {:estimated-card 10})}}
  all-eids-match [_idx-ident _q] true)

(deftest default-query-spec-is-backward-compatible
  (testing "no :query-spec-fn → the legacy {:query <arg0> :field <arg1>} shape"
    (is (= {:query :a :field :b} (build {} [:a :b])))
    (is (= {:query :a :field :b} (build {} [:a :b :c])) "extra args ignored by default")
    (is (= {:query :a :field nil} (build {} [:a])) "missing field is nil")
    (is (= {:query :a :field :b} (build {:some :other-meta} [:a :b])))))

(deftest custom-query-spec-fn-is-honored
  (testing ":query-spec-fn builds an arbitrary, arity-free opaque spec"
    (let [em {:query-spec-fn (fn [args]
                               {:pattern (nth args 0)
                                :depth (nth args 1)
                                :flags (nth args 2)})}]
      (is (= {:pattern :p :depth 3 :flags :f} (build em [:p 3 :f]))
          "index receives its own >2-arity spec, not {:query :field}"))
    (testing "the fn receives a vector of the args"
      (is (vector? (build {:query-spec-fn identity} '(:x :y)))))))

(deftest external-engine-entities-join-as-long
  (testing "entities produced by an external-engine index JOIN correctly with a
            scalar-attr scan. Roaring EntityBitSet yields 32-bit Int eids while
            datom eids are Longs; before the coercion fix the hash-join on the
            entity var silently dropped EVERY row (Java Integer != Long)."
    (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
               :schema-flexibility :write :keep-history? false}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                           :db/cardinality :db.cardinality/one}])
        (d/transact conn [{:db/ident :idx/test :db.secondary/type :test/all-eids
                           :db.secondary/attrs [:name]}])
        (d/transact conn [{:name "a"} {:name "b"} {:name "c"}])
        (Thread/sleep 200)                       ; let the index backfill
        ;; The DIRECT guard: the entity relation the external engine emits must
        ;; carry Long eids. Before the fix these were java.lang.Integer (Roaring),
        ;; so any hash-join keyed on the entity var (Integer != Long) dropped
        ;; every row. `:find ?e` alone surfaces the relation values directly.
        (let [es (d/q '[:find [?e ...] :where
                        [(datahike.test.external-engine-query-spec-test/all-eids-match :idx/test :_) [[?e]]]]
                      @conn)]
          (is (= 3 (count es)))
          (is (every? #(instance? Long %) es)
              "external-engine relation eids are Longs (were Integer before the fix)"))
        ;; And the end-to-end join to a scalar attribute yields the rows.
        (let [rows (d/q '[:find ?e ?n :where
                          [(datahike.test.external-engine-query-spec-test/all-eids-match :idx/test :_) [[?e]]]
                          [?e :name ?n]]
                        @conn)]
          (is (= #{"a" "b" "c"} (set (map second rows))))
          (d/release conn))))))
