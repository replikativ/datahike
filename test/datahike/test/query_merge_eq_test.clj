(ns datahike.test.query-merge-eq-test
  "Regression tests for equality obligations between fused merge positions.

   A fused entity-group is one scan clause plus N merge clauses on the same
   entity var. A free variable that occurs in more than one value/tx position
   of that group is a self-join CONSTRAINT, not N independent bindings. The
   engine used to represent only ONE such obligation — 'this merge's v equals
   the SCAN's v' — so two merges sharing a variable were never compared with
   each other and each projected its own value.

   Every expectation below is hand-computed from the fixture data, not taken
   from either engine."
  (:require
   [clojure.test :refer [is deftest testing]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]))

(defn- planned
  "Run `query` with the planner pinned ON."
  [query db & args]
  (binding [q/*disable-planner* false]
    (apply d/q query db args)))

;; ---------------------------------------------------------------------------
;; Fixture A — two card-many ref attributes.
;;
;;   1 :e [2 3]  :f [3]
;;   2 :e [1]    :f [1 4]
;;   3 :e [4]
;;   4 :e [4]    :f [2]

(def ^:private schema-a
  {:e {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :f {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}})

(defn- db-a []
  (d/db-with (db/empty-db schema-a)
             [{:db/id 1 :e [2 3] :f [3]}
              {:db/id 2 :e [1] :f [1 4]}
              {:db/id 3 :e [4]}
              {:db/id 4 :e [4] :f [2]}]))

;; [?a :e ?b] [?a :f ?b] [?a :f ?c]
;;   ?a=1: :e {2,3} ∩ :f {3} => ?b=3;  ?c ∈ :f {3}         => [1 3 3]
;;   ?a=2: :e {1}   ∩ :f {1,4} => ?b=1; ?c ∈ :f {1,4}      => [2 1 1] [2 1 4]
;;   ?a=3: no :f                                            => none
;;   ?a=4: :e {4}   ∩ :f {2} = {}                           => none
(def ^:private expected-a #{[1 3 3] [2 1 1] [2 1 4]})

(deftest test-two-merges-share-a-value-var
  (testing "the shared ?b must be equal in BOTH merge clauses, on every fused path"
    (let [db (db-a)]
      (is (= expected-a
             (planned '[:find ?a ?b ?c
                        :where [?a :e ?b] [?a :f ?b] [?a :f ?c]]
                      db))
          "direct path")

      (is (= (into {} (map (fn [t] [t 4])) expected-a)
             (frequencies
              (planned '[:find ?a ?b ?c
                         :with ?d
                         :where [?a :e ?b] [?a :f ?b] [?a :f ?c] [?d :e ?d2]]
                       db)))
          ":with path — each answer once per the 4 ?d/?d2 witnesses")

      ;; per ?a/?b group: ?a=1 has one ?c (3); ?a=2 has two (1 and 4)
      (is (= #{[1 3 1] [2 1 2]}
             (set (planned '[:find ?a ?b (count ?c)
                             :where [?a :e ?b] [?a :f ?b] [?a :f ?c]]
                           db)))
          "columnar aggregate path")

      (is (= #{[{:db/id 1} 3 3] [{:db/id 2} 1 1] [{:db/id 2} 1 4]}
             (set (planned '[:find (pull ?a [:db/id]) ?b ?c
                             :where [?a :e ?b] [?a :f ?b] [?a :f ?c]]
                           db)))
          "pull path")

      (is (= expected-a
             (planned '[:find ?a ?b ?c
                        :in $ [?a ...]
                        :where [?a :e ?b] [?a :f ?b] [?a :f ?c]]
                      db [1 2 3 4]))
          "relation path (execute-fused-scan-rel, driven by :in-bound entities)")

      (is (= expected-a
             (planned '[:find ?a ?b ?c
                        :where [?a :e ?b] [?a :f ?b] [?a :f ?c] (not [?a :e 99])]
                      db))
          "per-cursor / anti-merge path")

      (is (= expected-a
             (planned '[:find ?a ?b ?c
                        :where [?a :e ?b] [?a :f ?b] [?a :f ?c] [(identity ?a) ?z]]
                      db))
          "group followed by a bind"))))

;; ---------------------------------------------------------------------------
;; Fixture B — card-one attributes, exercising the sorted-merge path.
;;
;;   1 :p 1 :q 1 :r 1 :s 5
;;   2 :p 1 :q 2 :r 1 :s 6
;;   3 :p 2 :q 2 :r 2 :s 7
;;   4 :p 3 :q 3 :r 4 :s 8

(def ^:private schema-b {:p {} :q {} :r {} :s {}})

(defn- db-b []
  (d/db-with (db/empty-db schema-b)
             [{:db/id 1 :p 1 :q 1 :r 1 :s 5}
              {:db/id 2 :p 1 :q 2 :r 1 :s 6}
              {:db/id 3 :p 2 :q 2 :r 2 :s 7}
              {:db/id 4 :p 3 :q 3 :r 4 :s 8}]))

(deftest test-card-one-merges-share-a-value-var
  (let [db (db-b)]
    (testing "two card-one MERGES share ?x (the scan is the third clause)"
      ;; :p = :q holds for 1 (1=1), 3 (2=2), 4 (3=3); not for 2 (1≠2).
      (is (= #{[1 1 1] [3 2 2] [4 3 4]}
             (planned '[:find ?a ?x ?y
                        :where [?a :p ?x] [?a :q ?x] [?a :r ?y]]
                      db))))

    (testing "three clauses share ?x"
      ;; :p = :q = :r holds for 1 (1,1,1) and 3 (2,2,2); 4 has :r 4 ≠ :p 3.
      (is (= #{[1 1] [3 2]}
             (planned '[:find ?a ?x
                        :where [?a :p ?x] [?a :q ?x] [?a :r ?x]]
                      db))))

    (testing "scan and merge share ?x — the case that already worked, still works"
      ;; :p = :r holds for 1 (1=1), 3 (2=2); 2 has :p 1, :r 1 => also 2.
      (is (= #{[1 1] [2 1] [3 2]}
             (planned '[:find ?a ?x
                        :where [?a :p ?x] [?a :r ?x]]
                      db))))

    (testing "no shared var — the plain sorted-merge shape is untouched"
      (is (= #{[1 1 1 5] [2 1 2 6] [3 2 2 7] [4 3 3 8]}
             (planned '[:find ?a ?x ?y ?z
                        :where [?a :p ?x] [?a :q ?y] [?a :s ?z]]
                      db))))))

;; ---------------------------------------------------------------------------
;; Fixture C — two transactions, so tx vars actually differ.
;;
;;   tx1: 1 :e 2, 1 :f 2, 1 :g 2 | 2 :e 3, 2 :f 4, 2 :g 3
;;   tx2: 1 :e 5                 | 2 :f 5, 2 :g 5

(def ^:private schema-c
  {:e {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :f {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :g {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}})

(defn- db-c []
  (-> (db/empty-db schema-c)
      (d/db-with [{:db/id 1 :e [2] :f [2] :g [2]}
                  {:db/id 2 :e [3] :f [4] :g [3]}])
      (d/db-with [{:db/id 1 :e [5]}
                  {:db/id 2 :f [5] :g [5]}])))

(deftest test-two-merges-share-a-tx-var
  (let [db (db-c)]
    (testing "two MERGES share ?t (the ground :g clause drives the scan)"
      ;; entity 1 only (:g 2 was asserted in tx1). Its :e datoms are
      ;; (1 :e 2 tx1) and (1 :e 5 tx2); its only :f datom is (1 :f 2 tx1).
      ;; ?t must be the SAME tx in both, so only the tx1 pair survives.
      (is (= #{[1 2 2]}
             (planned '[:find ?a ?b ?c
                        :where [?a :g 2] [?a :e ?b ?t] [?a :f ?c ?t]]
                      db))))

    (testing "scan and merge share ?t — the case that already worked"
      (is (= #{[1 2 2] [2 3 4]}
             (planned '[:find ?a ?b ?c
                        :where [?a :e ?b ?t] [?a :f ?c ?t]]
                      db))))

    (testing "a merge's VALUE var repeats the scan's TX var"
      ;; [?a :f ?t] would need an :f datom whose value equals a tx id
      ;; (536870913/…): impossible for these entity-valued refs.
      (is (= #{}
             (planned '[:find ?a ?b ?t
                        :where [?a :e ?b ?t] [?a :g ?d] [?a :f ?t]]
                      db))))))

;; ---------------------------------------------------------------------------
;; Column alignment in the relation path.

(deftest test-relation-path-column-alignment
  (testing "a duplicate merge var must not consume an output column"
    ;; execute-fused-scan-rel appended a tuple column for EVERY merge with a
    ;; free value var, but out-attrs only allocated one for NEW vars — so the
    ;; column of every var introduced after a repeated one was off by one, and
    ;; ?y read ?x's value.
    ;;   :p = :q holds for 1, 3, 4 (see fixture B); add card-many :e so a
    ;;   third, later merge introduces a fresh var.
    (let [schema (assoc schema-b :e {:db/valueType :db.type/ref
                                     :db/cardinality :db.cardinality/many})
          db (d/db-with (db/empty-db schema)
                        [{:db/id 1 :p 1 :q 1 :r 1 :s 5 :e [2 3]}
                         {:db/id 2 :p 1 :q 2 :r 1 :s 6 :e [1]}
                         {:db/id 3 :p 2 :q 2 :r 2 :s 7 :e [4]}
                         {:db/id 4 :p 3 :q 3 :r 4 :s 8 :e [4]}])]
      (is (= #{[1 1 2 5] [1 1 3 5] [3 2 4 7] [4 3 4 8]}
             (planned '[:find ?a ?x ?y ?z
                        :in $ [?a ...]
                        :where [?a :p ?x] [?a :q ?x] [?a :s ?z] [?a :e ?y]]
                      db [1 2 3 4]))
          "?y must be an :e ref (2/3/4), never a copy of ?x"))))
