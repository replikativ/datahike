(ns datahike.test.query-prepared-fallback-test
  "Prepared execution falling back to the relation executor keeps its parameters visible.

   Prepared mode keeps scalar inputs as one-row relations so one cached plan serves every
   value. When ANOTHER input has several rows (a collection), the point fast path declines
   and the plan runs on the relation executor. That fallback used to substitute the
   one-row values into the pattern clauses first. The fused scan finds a bound value
   through the VARIABLE in a clause — to seek it and to price it when it re-chooses the
   group's driving pattern — so a substituted pattern was priced at the cached plan's
   value-free estimate, the whole attribute, and lost the scan slot to any pattern that
   still had a variable. Measured on 300k entities: 63 ms a query against 0.2 ms.

   The driver is observed, not inferred from time."
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.query :as dq]
            [datahike.query.execute :as dqe]))

(def ^:private kinds ["k0" "k1" "k2" "k3" "k4" "k5"])
(def ^:private n 30000)

(defn- fresh-db []
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :item/key :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/unique :db.unique/identity}
                        {:db/ident :item/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :item/rank :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :item/kind :db/valueType :db.type/string :db/cardinality :db.cardinality/one :db/index true}])
      (doseq [b (partition-all 10000 (range n))]
        (d/transact conn (mapv (fn [i] {:item/key (str "id" i) :item/name (str "n" (mod i 2000))
                                        :item/rank i :item/kind (kinds (mod i 6))}) b)))
      [cfg conn])))

(defn- probe-driven-attrs [f]
  (let [seen (atom [])
        orig @#'dqe/probe-driven-iterable]
    (with-redefs [dqe/probe-driven-iterable (fn [index resolved-a probe-set field]
                                              (swap! seen conj resolved-a)
                                              (orig index resolved-a probe-set field))]
      (let [r (f)] {:result r :attrs @seen}))))

(defn- prepared [q db & args]
  ;; the planner is what is under test: bound on explicitly so the tier that runs the
  ;; suite with DATAHIKE_QUERY_PLANNER=false still exercises it
  (binding [dq/*disable-planner* false
            dq/*fold-scalar-ins* false dqe/*prepared-execution* true dq/*query-result-cache?* false]
    (apply d/q q db args)))

(defn- reference [q db & args]
  (binding [dq/*disable-planner* true dq/*query-result-cache?* false]
    (set (apply d/q q db args))))

(deftest a-scalar-parameter-keeps-the-scan-beside-a-collection-input
  (let [[cfg conn] (fresh-db)
        db @conn
        q '[:find [?e ...] :in $ ?name [?kind ...]
            :where [?e :item/name ?name] [?e :item/kind ?kind]]]
    (try
      (doseq [nm ["n7" "n1000" "n1999"] ks [["k1" "k4"] kinds]]
        (let [{:keys [result attrs]} (probe-driven-attrs #(prepared q db nm ks))]
          (is (= (reference q db nm ks) (set result)))
          (is (some #{:item/name} attrs) "the 15-row name is sought through its parameter")
          (is (not-any? #{:item/kind} attrs) "and a third of the database does not take the scan")))
      (finally (d/release conn) (d/delete-database cfg)))))

(deftest a-range-bound-from-a-parameter-is-still-bound-on-the-fallback
  ;; the pushdown bound is the one thing the fallback must still substitute: it reaches an
  ;; index comparator, which cannot read a relation
  (let [[cfg conn] (fresh-db)
        db @conn
        q-merge '[:find [?e ...] :in $ ?name ?lo [?kind ...]
                  :where [?e :item/name ?name] [?e :item/kind ?kind] [?e :item/rank ?r] [(> ?r ?lo)]]
        q-scan  '[:find [?e ...] :in $ ?lo ?hi [?kind ...]
                  :where [?e :item/rank ?r] [(> ?r ?lo)] [(<= ?r ?hi)] [?e :item/kind ?kind]]]
    (try
      (doseq [lo [-1 0 9000 29990 n]]
        (is (= (reference q-merge db "n7" lo ["k1" "k4"]) (set (prepared q-merge db "n7" lo ["k1" "k4"])))
            (str "range on a merged attribute, lo=" lo))
        (is (= (reference q-scan db lo (+ lo 40) ["k0" "k1" "k5"]) (set (prepared q-scan db lo (+ lo 40) ["k0" "k1" "k5"])))
            (str "range on the scanned attribute, lo=" lo)))
      (is (seq (prepared q-scan db 100 140 ["k0" "k1" "k5"])) "precondition: the range query selects rows")
      (finally (d/release conn) (d/delete-database cfg)))))
