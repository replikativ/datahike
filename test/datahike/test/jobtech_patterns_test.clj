(ns datahike.test.jobtech-patterns-test
  "Test jobtech-taxonomy-api query patterns against compiled engine.
   These patterns exercise: rules with OR, ground bindings, get-else,
   identity bindings, NOT, pull, collection bindings, and regex matching."
  (:require
   [clojure.test :refer [is deftest testing]]
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]))

;; ---------------------------------------------------------------------------
;; Test data: simplified jobtech taxonomy model

(def schema
  {:concept/id             {:db/unique    :db.unique/identity
                            :db/index     true}
   :concept/type           {:db/index     true}
   :concept/preferred-label {:db/index    true}
   :concept/deprecated     {}
   :concept/alternative-labels {:db/cardinality :db.cardinality/many}
   :concept/sort-order     {}
   :concept/replaced-by    {:db/valueType :db.type/ref}
   :concept.external-database.ams-taxonomy-67/id {}
   :relation/id            {:db/unique    :db.unique/identity
                            :db/index     true}
   :relation/concept-1     {:db/valueType :db.type/ref}
   :relation/concept-2     {:db/valueType :db.type/ref}
   :relation/type          {:db/index     true}})

(def test-db
  (delay
    (d/db-with
     (db/empty-db schema)
     [;; Concepts
      {:db/id 1 :concept/id "c1" :concept/type "occupation"
       :concept/preferred-label "Software Developer" :concept/sort-order 1}
      {:db/id 2 :concept/id "c2" :concept/type "occupation"
       :concept/preferred-label "Data Scientist" :concept/sort-order 2}
      {:db/id 3 :concept/id "c3" :concept/type "skill"
       :concept/preferred-label "Java" :concept/sort-order 1}
      {:db/id 4 :concept/id "c4" :concept/type "skill"
       :concept/preferred-label "Python" :concept/sort-order 2}
      {:db/id 5 :concept/id "c5" :concept/type "skill"
       :concept/preferred-label "Clojure" :concept/sort-order 3
       :concept/alternative-labels #{"CLJ" "Lisp dialect"}}
      {:db/id 6 :concept/id "c6" :concept/type "occupation"
       :concept/preferred-label "DevOps Engineer" :concept/deprecated true}
      {:db/id 7 :concept/id "c7" :concept/type "occupation"
       :concept/preferred-label "Platform Engineer"
       :concept.external-database.ams-taxonomy-67/id "AMS-123"}
      ;; c6 replaced-by c7
      {:db/id 6 :concept/replaced-by 7}
      ;; Relations (edges)
      ;; c1 --essential--> c3 (Software Developer needs Java)
      {:db/id 10 :relation/id "c1:essential:c3"
       :relation/concept-1 1 :relation/type "essential" :relation/concept-2 3}
      ;; c1 --essential--> c5 (Software Developer needs Clojure)
      {:db/id 11 :relation/id "c1:essential:c5"
       :relation/concept-1 1 :relation/type "essential" :relation/concept-2 5}
      ;; c2 --essential--> c4 (Data Scientist needs Python)
      {:db/id 12 :relation/id "c2:essential:c4"
       :relation/concept-1 2 :relation/type "essential" :relation/concept-2 4}
      ;; c1 --broader--> c7 (Software Developer broader Platform Engineer)
      {:db/id 13 :relation/id "c1:broader:c7"
       :relation/concept-1 1 :relation/type "broader" :relation/concept-2 7}
      ;; c3 --related--> c5 (Java related Clojure)
      {:db/id 14 :relation/id "c3:related:c5"
       :relation/concept-1 3 :relation/type "related" :relation/concept-2 5}])))

;; ---------------------------------------------------------------------------
;; The jobtech "edge" rule: bidirectional relation traversal

(def persisted-relations
  {"broader"              "narrower"
   "related"              "related"
   "essential"            "essential-for"
   "substitutability"     "substituted-by"})

(def reverse-relations
  (into {} (map (fn [[k v]] [v k]) persisted-relations)))

(def edge-rules
  [;; Forward edge
   (list '(-forward-edge ?from-concept ?type ?to-concept ?relation)
         [(list 'ground (vec (keys persisted-relations))) '[?type ...]]
         '[?relation :relation/concept-1 ?from-concept]
         '[?relation :relation/type ?type]
         '[?relation :relation/concept-2 ?to-concept])
   ;; Reverse edge
   (list '(-reverse-edge ?from-concept ?type ?to-concept ?relation)
         [(list 'ground reverse-relations) '[[?type ?reverse-type]]]
         '[?relation :relation/concept-2 ?from-concept]
         '[?relation :relation/type ?reverse-type]
         '[?relation :relation/concept-1 ?to-concept])
   ;; Bidirectional edge (OR over forward/reverse)
   '[(edge ?from-concept ?type ?to-concept ?relation)
     (or (-forward-edge ?from-concept ?type ?to-concept ?relation)
         (-reverse-edge ?from-concept ?type ?to-concept ?relation))]])

;; ---------------------------------------------------------------------------
;; Helper

(defn assert-engines-agree
  ([db query extra-args]
   (let [legacy  (binding [q/*force-legacy* true]
                   (apply d/q query db extra-args))
         compiled (binding [q/*force-legacy* false]
                    (apply d/q query db extra-args))]
     (is (= (set (seq legacy)) (set (seq compiled)))
         (str "Engines disagree on: " (pr-str query)))))
  ([db query]
   (assert-engines-agree db query [])))

;; ---------------------------------------------------------------------------
;; Pattern 1: Simple concept lookup by id

(deftest test-concept-by-id
  (testing "Find concept by id"
    (assert-engines-agree @test-db
                          '[:find ?c :in $ ?id :where [?c :concept/id ?id]]
                          ["c1"])))

;; Pattern 2: Concept by type (collection binding)

(deftest test-concept-by-type
  (testing "Find concepts by type (collection binding)"
    (assert-engines-agree @test-db
                          '[:find ?c :in $ [?type ...] :where [?c :concept/type ?type]]
                          [["occupation" "skill"]])))

;; Pattern 3: NOT deprecated

(deftest test-not-deprecated
  (testing "Filter out deprecated concepts"
    (assert-engines-agree @test-db
                          '[:find ?c :where
                            [?c :concept/id]
                            (not [?c :concept/deprecated true])])))

;; Pattern 4: Relation traversal with edge rule

(deftest test-edge-rule-forward
  (testing "Forward edge: find skills essential for Software Developer"
    (assert-engines-agree @test-db
                          '[:find ?to-id :in $ % ?from-id ?relation
                            :where
                            [?from :concept/id ?from-id]
                            (edge ?from ?relation ?to ?_)
                            [?to :concept/id ?to-id]]
                          [edge-rules "c1" "essential"])))

(deftest test-edge-rule-reverse
  (testing "Reverse edge: find what Java is essential-for"
    (assert-engines-agree @test-db
                          '[:find ?from-id :in $ % ?to-id ?relation
                            :where
                            [?to :concept/id ?to-id]
                            (edge ?to ?relation ?from ?_)
                            [?from :concept/id ?from-id]]
                          [edge-rules "c3" "essential-for"])))

(deftest test-edge-rule-symmetric
  (testing "Symmetric edge: related goes both ways"
    (assert-engines-agree @test-db
                          '[:find ?to-id :in $ % ?from-id ?relation
                            :where
                            [?from :concept/id ?from-id]
                            (edge ?from ?relation ?to ?_)
                            [?to :concept/id ?to-id]]
                          [edge-rules "c5" "related"])))

;; Pattern 5: get-else for optional attributes

(deftest test-get-else
  (testing "get-else for legacy id"
    (assert-engines-agree @test-db
                          '[:find ?c ?legacy-id :where
                            [?c :concept/id]
                            [(get-else $ ?c :concept.external-database.ams-taxonomy-67/id false)
                             ?legacy-id]])))

;; Pattern 6: Regex matching on preferred-label

(deftest test-regex-matching
  (testing "Case-insensitive label matching via .matches"
    (assert-engines-agree @test-db
                          '[:find ?c :in $ ?pattern :where
                            [?c :concept/preferred-label ?label]
                            [(.matches ^String ?label ?pattern)]]
                          ["(?i).*java.*"])))

;; Pattern 7: Combined query (type + not-deprecated + relation)

(deftest test-combined-query
  (testing "Skills essential for occupation c1, not deprecated"
    (assert-engines-agree @test-db
                          '[:find ?skill-id ?label :in $ % ?occ-id
                            :where
                            [?occ :concept/id ?occ-id]
                            (edge ?occ "essential" ?skill ?_)
                            [?skill :concept/id ?skill-id]
                            [?skill :concept/preferred-label ?label]
                            (not [?skill :concept/deprecated true])]
                          [edge-rules "c1"])))

;; Pattern 8: Ground binding in rule (the identity-binding pattern)

(deftest test-ground-in-rule
  (testing "Rule with ground collection binding"
    (let [rules '[[(typed-concept ?c ?type)
                   [?c :concept/type ?type]
                   [?c :concept/id]]]]
      (assert-engines-agree @test-db
                            '[:find ?id :in $ % ?type
                              :where
                              (typed-concept ?c ?type)
                              [?c :concept/id ?id]]
                            [rules "skill"]))))

;; Pattern 9: Rule with identity binding (constant call-arg)

(deftest test-rule-with-constant-arg
  (testing "Rule called with constant arg (identity binding)"
    (let [rules '[[(occupation-concept ?c)
                   [?c :concept/type "occupation"]
                   [?c :concept/id]]]]
      (assert-engines-agree @test-db
                            '[:find ?id :in $ %
                              :where
                              (occupation-concept ?c)
                              [?c :concept/id ?id]]
                            [rules]))))

;; Pattern 10: Multiple relation types via OR

(deftest test-or-relation-types
  (testing "Find concepts related by any of several types"
    (assert-engines-agree @test-db
                          '[:find ?to-id :in $ ?from-id
                            :where
                            [?from :concept/id ?from-id]
                            (or (and [?r :relation/concept-1 ?from]
                                     [?r :relation/type "essential"]
                                     [?r :relation/concept-2 ?to])
                                (and [?r :relation/concept-1 ?from]
                                     [?r :relation/type "broader"]
                                     [?r :relation/concept-2 ?to]))
                            [?to :concept/id ?to-id]]
                          ["c1"])))

;; Pattern 11: Count aggregate

(deftest test-count-aggregate
  (testing "Count concepts by type"
    (let [legacy  (binding [q/*force-legacy* true]
                    (d/q '[:find ?type (count ?c)
                           :where [?c :concept/type ?type]]
                         @test-db))
          compiled (binding [q/*force-legacy* false]
                     (d/q '[:find ?type (count ?c)
                            :where [?c :concept/type ?type]]
                          @test-db))]
      (is (= (set legacy) (set compiled))))))

;; Pattern 12: Pull pattern (compiled engine should handle via relation path)

(deftest test-pull-pattern
  (testing "Pull with nested ref"
    (let [legacy  (binding [q/*force-legacy* true]
                    (d/q '[:find (pull ?c [:concept/id :concept/preferred-label
                                           {:concept/replaced-by [:concept/id]}])
                           :where [?c :concept/deprecated true]]
                         @test-db))
          compiled (binding [q/*force-legacy* false]
                     (d/q '[:find (pull ?c [:concept/id :concept/preferred-label
                                            {:concept/replaced-by [:concept/id]}])
                            :where [?c :concept/deprecated true]]
                          @test-db))]
      (is (= (set legacy) (set compiled))))))

;; Pattern 13: Concept exists? (non-empty check)

(deftest test-concept-exists
  (testing "Check if concept exists by id"
    (assert-engines-agree @test-db
                          '[:find ?e :in $ ?id :where [?e :concept/id ?id]]
                          ["c1"])
    (assert-engines-agree @test-db
                          '[:find ?e :in $ ?id :where [?e :concept/id ?id]]
                          ["nonexistent"])))

;; Pattern 14: Relation lookup by id

(deftest test-relation-by-id
  (testing "Find relation by id"
    (assert-engines-agree @test-db
                          '[:find ?r :in $ ?id :where [?r :relation/id ?id]]
                          ["c1:essential:c3"])))

;; Pattern 15: Multi-hop: occupation -> skill -> related skill

(deftest test-multi-hop
  (testing "Two-hop: occupation's skills and their related skills"
    (assert-engines-agree @test-db
                          '[:find ?related-id :in $ ?occ-id
                            :where
                            [?occ :concept/id ?occ-id]
                            [?r1 :relation/concept-1 ?occ]
                            [?r1 :relation/type "essential"]
                            [?r1 :relation/concept-2 ?skill]
                            [?r2 :relation/concept-1 ?skill]
                            [?r2 :relation/type "related"]
                            [?r2 :relation/concept-2 ?related]
                            [?related :concept/id ?related-id]]
                          ["c1"])))

;; Pattern 16: FindScalar

(deftest test-find-scalar
  (testing "FindScalar: single concept label"
    (let [legacy  (binding [q/*force-legacy* true]
                    (d/q '[:find ?label . :in $ ?id
                           :where [?c :concept/id ?id]
                           [?c :concept/preferred-label ?label]]
                         @test-db "c1"))
          compiled (binding [q/*force-legacy* false]
                     (d/q '[:find ?label . :in $ ?id
                            :where [?c :concept/id ?id]
                            [?c :concept/preferred-label ?label]]
                          @test-db "c1"))]
      (is (= legacy compiled)))))

;; Pattern 17: FindColl

(deftest test-find-coll
  (testing "FindColl: all concept ids"
    (let [legacy  (binding [q/*force-legacy* true]
                    (d/q '[:find [?id ...] :where [?c :concept/id ?id]]
                         @test-db))
          compiled (binding [q/*force-legacy* false]
                     (d/q '[:find [?id ...] :where [?c :concept/id ?id]]
                          @test-db))]
      (is (= (set legacy) (set compiled))))))
