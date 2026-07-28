(ns datahike.test.query-cyclic-join-test
  "Inter-group joins the fused direct path cannot enforce.

   The fused multi-group loop joins each consumer entity-group to ONE producer
   group on ONE probe variable. Two query shapes imply more equalities than
   that, and before the fix the surplus ones were silently dropped:

     2-cycle   [?a :e ?b] [?b :e ?a]           — two groups share TWO vars
     triangle  [?a :e ?b] [?b :e ?c] [?c :e ?a] — a group shares vars with two
                                                  earlier groups

   The 2-cycle returned extra tuples; the triangle additionally emitted a nil
   column, because the emitter found no source for the unjoined variable.

   Worst of all it depended on the NAMES of the variables: the probe var was
   `(first probe-vars)` off a hash-set, so the 4-cycle spelled with ?x/?y took
   the fused path and answered wrongly while the SAME query spelled with ?u/?w
   fell to the Relation engine and answered correctly. Every expectation below
   is therefore duplicated with renamed variables, and `rename-invariance`
   sweeps a shape over many namings at once.

   Expected sets are hand-written from the fact table, not taken from either
   engine."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as dq]))

;; Facts (both attributes are :db.type/ref, :db.cardinality/many):
;;   1 :e 2   1 :e 3   1 :f 3
;;   2 :e 1   2 :f 1   2 :f 4
;;   3 :e 4
;;   4 :e 4   4 :f 2
(def ^:private db
  (delay
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? false
               :schema-flexibility :write}]
      (d/create-database cfg)
      (let [conn (d/connect cfg)]
        (d/transact conn [{:db/ident :e :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/many}
                          {:db/ident :f :db/valueType :db.type/ref
                           :db/cardinality :db.cardinality/many}])
        (d/transact conn [{:db/id 1 :e [2 3] :f [3]}
                          {:db/id 2 :e [1] :f [1 4]}
                          {:db/id 3 :e [4]}
                          {:db/id 4 :e [4] :f [2]}])
        (d/db conn)))))

(defn- path [q]
  (second (re-find #"path: (\S+)" (d/explain q @db))))

(defn- both-engines [q]
  {:planner (binding [dq/*disable-planner* false] (d/q q @db))
   :legacy  (binding [dq/*disable-planner* true]  (d/q q @db))})

(defn- check
  "Assert `q` yields exactly `expected` on both engines, and that `explain`
   reports `want-path`."
  [label q expected want-path]
  (let [{:keys [planner legacy]} (both-engines q)]
    (is (= expected planner) (str label " — planner"))
    (is (= expected legacy) (str label " — reference engine"))
    (is (= want-path (path q)) (str label " — execution path"))))

;; ---------------------------------------------------------------------------
;; 2-cycle: both groups share BOTH ?a and ?b.
;; Only 1↔2 and the 4→4 self-loop are mutual; 3→4 and 4→4 are not (4 has no
;; :e back to 3). The fused path used to also return [3 1] and [4 3].

(deftest two-cycle
  (testing "two groups sharing two variables"
    (check "2-cycle"
           '[:find ?a ?b :where [?a :e ?b] [?b :e ?a]]
           #{[1 2] [2 1] [4 4]}
           "relation"))
  (testing "variable-renamed twin — the probe var came off a hash-set"
    (check "2-cycle renamed"
           '[:find ?p ?q :where [?p :e ?q] [?q :e ?p]]
           #{[1 2] [2 1] [4 4]}
           "relation")))

;; ---------------------------------------------------------------------------
;; 4-cycle across two entity-groups: group ?a supplies ?x (via :e) and ?y (via
;; :f); group ?b consumes both, in the opposite roles. Only ?x was enforced.
;; ?a=2 gives ?x∈{1}, ?y∈{1,4}; ?b=2 gives ?x∈{1,4}, ?y∈{1} → [1 1].
;; ?a=1 gives ?x∈{2,3}, ?y∈{3}; ?b=1 gives ?x∈{3}, ?y∈{2,3} → [3 3].

(deftest four-cycle
  (testing "four-cycle over two entity-groups"
    (check "4-cycle"
           '[:find ?x ?y :where [?a :e ?x] [?a :f ?y] [?b :f ?x] [?b :e ?y]]
           #{[1 1] [3 3]}
           "relation"))
  (testing "variable-renamed twin — this spelling was already correct by luck"
    (check "4-cycle renamed"
           '[:find ?u ?w :where [?a :e ?u] [?a :f ?w] [?b :f ?u] [?b :e ?w]]
           #{[1 1] [3 3]}
           "relation")))

;; ---------------------------------------------------------------------------
;; Triangle: group 2 shares ?a with group 0 AND ?c with group 1, but only the
;; earliest producer was recorded. ?c then had no column and came out nil.
;; 4→4→4→4 is the only closed triangle.

(deftest triangle
  (testing "three groups in a cycle"
    (check "triangle"
           '[:find ?a ?b ?c :where [?a :e ?b] [?b :e ?c] [?c :e ?a]]
           #{[4 4 4]}
           "relation"))
  (testing "variable-renamed twin"
    (check "triangle renamed"
           '[:find ?r ?s ?t :where [?r :e ?s] [?s :e ?t] [?t :e ?r]]
           #{[4 4 4]}
           "relation"))
  (testing "no nil columns leak into the result"
    (is (every? #(every? some? %)
                (d/q '[:find ?a ?b ?c :where
                       [?a :e ?b] [?b :e ?c] [?c :e ?a]]
                     @db)))))

;; ---------------------------------------------------------------------------
;; The decline must be EXACT, not a "looks cyclic" heuristic: a producer that
;; feeds several consumers on DISTINCT variables (the multi-consumer path) is
;; perfectly sound and must stay fused.

(deftest sound-shapes-stay-fused
  (testing "acyclic 2-group join"
    (is (= "direct" (path '[:find ?a ?b ?c :where [?a :e ?b] [?b :f ?c]]))))
  (testing "one producer, two consumers on distinct vars (star)"
    (is (= "direct" (path '[:find ?a ?b ?c ?d :where
                            [?a :e ?b] [?a :f ?c] [?c :e ?d]]))))
  (testing "multi-consumer edge/vertex shape stays fused"
    (let [cfg {:store {:backend :memory :id (random-uuid)}
               :keep-history? false :schema-flexibility :write}
          _ (d/create-database cfg)
          conn (d/connect cfg)]
      (d/transact conn [{:db/ident :marker :db/valueType :db.type/keyword
                         :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :src :db/valueType :db.type/ref
                         :db/cardinality :db.cardinality/one :db/index true}
                        {:db/ident :tgt :db/valueType :db.type/ref
                         :db/cardinality :db.cardinality/one :db/index true}])
      (d/transact conn [{:db/id "v1" :marker :V} {:db/id "v2" :marker :V}
                        {:db/id "v3" :marker :V}
                        {:db/id "e1" :marker :E :src "v1" :tgt "v2"}
                        {:db/id "e2" :marker :E :src "v2" :tgt "v3"}
                        {:db/id "e3" :marker :E :src "v3" :tgt "v1"}])
      (let [gdb (d/db conn)
            q '[:find ?v1 ?v2 ?e :where
                [?v1 :marker :V] [?v2 :marker :V] [?e :marker :E]
                [?e :src ?v1] [?e :tgt ?v2]]]
        (is (= "direct" (second (re-find #"path: (\S+)" (d/explain q gdb))))
            "star join must not be declined by a cyclic-looking heuristic")
        (is (= 3 (count (d/q q gdb))))))))

;; ---------------------------------------------------------------------------
;; The hash-order guard: the same shape under many variable namings must give
;; ONE answer and ONE path. Before the fix the 4-cycle was correct for exactly
;; half of the 300 namings below.

(deftest rename-invariance
  (let [names (mapv #(symbol (str "?" %)) "abcdefghijklmnop")
        quads (for [p (take 6 names)
                    q (take 6 names) :when (not= q p)
                    x (take 6 names) :when (not (#{p q} x))
                    y (take 6 names) :when (not (#{p q x} y))]
                [p q x y])]
    (testing "4-cycle answers and path do not depend on variable names"
      (let [outcomes (into #{}
                           (map (fn [[p q x y]]
                                  (let [qy [:find x y :where
                                            [p :e x] [p :f y] [q :f x] [q :e y]]]
                                    [(set (d/q qy @db)) (path qy)])))
                           quads)]
        (is (= 1 (count outcomes))
            (str "variable naming changed the outcome: " (pr-str outcomes)))
        (is (= #{[#{[1 1] [3 3]} "relation"]} outcomes))))))
