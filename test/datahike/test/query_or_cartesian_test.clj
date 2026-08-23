(ns datahike.test.query-or-cartesian-test
  "An `or` branch must not be handed relations it shares no variable with.

   `execute-or-join` calls `rel/limit-context` before running its branches;
   `execute-or` did not — it passed the caller's whole relation set through
   (`(assoc ctx :rels (:rels ctx))`, a no-op). A branch then folded those
   relations in with `(reduce rel/hash-join …)`, and `hash-join` on an EMPTY
   common-attribute set is a full Cartesian product: `tuple-key-fn` over zero
   getters yields one constant key, so every left tuple matches every right one.

   `limit-rel` afterwards reprojects the columns but never touches `:tuples`, so
   the duplicate rows survive into the enclosing scope and the next nested `or`
   multiplies them again. The degree is the number of nested plain-`or` levels.

   Reported as datahike#953: a rule that is an `or` over two NAMED sub-rules is
   three levels, so the largest relation was

     |matched relations| x |:in collection|^3

   exact at every size measured — 225x5^3 = 28125, 225x10^3 = 225000,
   225x20^3 = 1800000. At the reporter's 75 ids that is 94.9M tuples, ~4.5 GB,
   and a heap histogram taken mid-run showed 30.0M `Object[]` holding 96% of a
   2 GB heap. Wall clock went 2.8 s (20 ids) -> 20.5 s (40) -> OOM (75); after
   the fix it is flat at 744/741/790 ms.

   ## Why this asserts on Cartesians rather than on time or rows

   The RESULTS were always correct — only the intermediates exploded — so a row
   count cannot see it. A timing assertion would need the failing size, which
   OOMs and would take the whole suite down with it. Counting zero-common-
   attribute joins is deterministic, fast, and names the actual defect.

   (Written with `fn` rather than `#()`: the query's `:in [$ % …]` rules binding
   is spelled `%`, which inside an anonymous function literal is the reader's
   argument placeholder.)"
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.set :as set]
            [datahike.api :as d]
            [datahike.query.relation :as rel]
            [datahike.test.utils :as utils]))

(def ^:private schema
  [{:db/ident :concept/id :db/valueType :db.type/string
    :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
   {:db/ident :relation/from :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :relation/to :db/valueType :db.type/ref :db/cardinality :db.cardinality/one}
   {:db/ident :relation/type :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])

;; The reporter's shape: an `edge` rule that is an `or` over two NAMED
;; sub-rules. Named rather than inlined `and`s on purpose — each named rule is
;; planned as its own OR level, and it is the NESTING that sets the exponent.
(def ^:private rules
  '[[(-fwd ?from ?type ?to ?r)
     [(ground ["broader" "related"]) [?type ...]]
     [?r :relation/from ?from]
     [?r :relation/type ?type]
     [?r :relation/to ?to]]
    [(-rev ?from ?type ?to ?r)
     [(ground ["narrower" "related"]) [?type ...]]
     [?r :relation/to ?from]
     [?r :relation/type ?type]
     [?r :relation/from ?to]]
    [(edge ?from ?type ?to ?r)
     (or (-fwd ?from ?type ?to ?r)
         (-rev ?from ?type ?to ?r))]])

(defn- cid [i] (format "c%04d" i))

(defn- conn-with [n fanout]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :keep-history? false :schema-flexibility :write}]
    (d/create-database cfg)
    (let [c (d/connect cfg)]
      (d/transact c schema)
      (d/transact c (vec (for [i (range n)] {:concept/id (cid i)})))
      (d/transact c (vec (for [i (range n) k (range 1 (inc fanout))
                               :let [j (mod (+ i k) n)]]
                           {:relation/from [:concept/id (cid i)]
                            :relation/type "broader"
                            :relation/to [:concept/id (cid j)]})))
      c)))

(defn- count-cartesian-joins
  "Run `f`, counting `hash-join` calls whose operands share NO attribute."
  [f]
  (let [n (atom 0)
        orig rel/hash-join]
    (with-redefs [rel/hash-join
                  (fn [r1 r2]
                    (when (empty? (set/intersection (set (keys (:attrs r1)))
                                                    (set (keys (:attrs r2)))))
                      (swap! n inc))
                    (orig r1 r2))]
      [(f) @n])))

(deftest or-branches-are-not-handed-disconnected-relations
  (testing "the `[?from-id ...]` collection binding shares no variable with
            either branch of the `edge` rule, so before the fix each branch
            Cartesian-multiplied by it — once per nested `or` level."
    (let [c (conn-with 40 3)]
      (try
        (let [db @c
              from-ids (into #{} (map cid) (range 20))
              [rows cartesians]
              (count-cartesian-joins
               (fn [] (count (d/q {:query '{:find [?from-id ?id]
                                            :in [$ % [?from-id ...] ?rt]
                                            :where [[?c :concept/id ?from-id]
                                                    (edge ?c ?rt ?related ?r)
                                                    [?related :concept/id ?id]]}
                                   :args [db rules from-ids "broader"]}))))]
          (is (= 60 rows)
              "20 from-ids x fanout 3 — the RESULT was always correct, which is
               why a row count cannot detect the defect")
          (is (zero? cartesians)
              (str "an `or` branch was joined against a relation it shares no "
                   "variable with, " cartesians " time(s) — each one multiplies "
                   "the branch by the size of that relation")))
        (finally (d/release c))))))

(deftest or-cost-does-not-grow-with-the-in-collection
  (testing "the signature of the defect: work scaled with the size of the `:in`
            collection even though the matched data did not. Asserted as a
            RATIO of Cartesian joins, which is size-independent when correct."
    (let [c (conn-with 40 3)]
      (try
        (let [db @c
              run (fn [k]
                    (second
                     (count-cartesian-joins
                      (fn [] (d/q {:query '{:find [?from-id ?id]
                                            :in [$ % [?from-id ...] ?rt]
                                            :where [[?c :concept/id ?from-id]
                                                    (edge ?c ?rt ?related ?r)
                                                    [?related :concept/id ?id]]}
                                   :args [db rules (into #{} (map cid) (range k)) "broader"]})))))]
          (is (= 0 (run 5) (run 20) (run 40))
              "no Cartesian at any :in size — before the fix this shape produced
               them at every size, and the resulting relation was
               |relations| x |:in|^3"))
        (finally (d/release c))))))
