(ns datahike.test.strategy-test
  (:require [clojure.test :refer [is deftest testing]]
            [datahike.test.utils :as utils]
            [datahike.api :as d]
            [taoensso.timbre :as log]
            [datahike.query :as dq]))

(defn concept-id [index]
  (let [s (format "%010d" index)]
    (str (subs s 0 4) "_" (subs s 4 7) "_" (subs s 7 10))))

(defn temp-id [x]
  (str "tmp-" x))

(defn make-forest
  "This function constructs tx-data for a forest of concepts in a terminology, 
  for example a labor market terminology of occupations. The edges in the
  forest point toward the root and are labeled `:concept/broader` because the
  closer to the root we get, the broader the concept is. For instance, we could
  have a concept for the occupation name 'Software Engineer' and an edge from 
  that concept pointing at a broader concept 'Occuptions in Computer Science'.

  This function takes as input a concatenated list of pairs of `m` and `t`
  on the form `[m1 t1 m2 t2 ... mN tN]` that specifies how many sub nodes
  should be generated at each level from the root and the type. For example,
  `[5 'ssyk-level-1' 3 'ssyk-level-2']` means that we will construct 5 trees
  of root node type 'ssyk-level-1' and each one of them will have 3 children
  of node type 'ssyk-level-2'."
  [[root-count & tree-spec]]
  {:pre [(number? root-count)]}
  (loop [[tos & stack] (repeat root-count [nil tree-spec])
         tx-data []
         counter 0
         concept-map {}]
    (if (nil? tos)
      {:tx-data tx-data
       :concept-map concept-map}
      (let [[parent-id [this-type child-count & remaining-pairs]] tos
            concept-id (concept-id counter)
            tid (temp-id concept-id)
            parent-tid (temp-id parent-id)]
        (assert (or (nil? parent-id) (string? parent-id)))
        (recur (into stack
                     (when (seq remaining-pairs)
                       (repeat child-count [concept-id remaining-pairs])))
               (into tx-data
                     cat
                     [[[:db/add tid :concept/id concept-id]
                       [:db/add tid :concept/type this-type]]
                      (when parent-id
                        [[:db/add tid :concept/broader parent-tid]])])
               (inc counter)
               (cond-> concept-map
                 true (update concept-id
                              merge {:parent-id parent-id
                                     :type this-type
                                     :id concept-id})
                 parent-id (update-in [parent-id :child-ids] #(conj (or % []) concept-id))))))))

(def schema [#:db{:ident :concept/id,
                  :valueType :db.type/string,
                  :cardinality :db.cardinality/one,
                  :doc "Unique identifier for concepts",
                  :unique :db.unique/identity}
             #:db{:ident :concept/type,
                  :valueType :db.type/string,
                  :cardinality :db.cardinality/one,
                  :doc "The concepts main type"}
             #:db {:ident :concept/broader
                   :valueType :db.type/ref
                   :cardinality :db.cardinality/one
                   :doc "A broader concept. NOTE: This the JobTech Taxonomy, every relation between two concepts has an entity with attributes :relation/concept-1, :relation-concept-2 and :relation/type."}])

(defn initialize-test-db0 []
  (let [conn (utils/setup-db {:store {:backend :mem}
                              :schema-flexibility :write
                              :attribute-refs? false
                              :keep-history? true})]
    (d/transact conn {:tx-data schema})
    conn))

(defn evaluate-strategy-times [f]
  (into {}
        (map (fn [[k strategy]] [k (let [start (System/nanoTime)
                                         result (f strategy)
                                         end (System/nanoTime)]
                                     {:result result
                                      :elapsed-ns (- end start)})]))
        [[:identity identity]
         [:select-simple dq/select-simple]
         [:select-all dq/select-all]
         [:expand-once dq/expand-once]]))

(defn print-strategy-times [result-map]
  (doseq [[k v] (sort-by (comp :elapsed-ns val) result-map)]
    (log/info (format "%16s: %.6f" (name k) (* 1.0e-9 (:elapsed-ns v))))))

(defn faster-strategy? [strategy-times key-a key-b]
  (< (get-in strategy-times [key-a :elapsed-ns])
     (get-in strategy-times [key-b :elapsed-ns])))

(defn group-concepts-by-type [concept-map]
  (let [groups (update-vals (group-by (comp :type val)
                                      concept-map)
                            (fn [kv-pairs]
                              (mapv first kv-pairs)))]
    (doseq [[k v] (sort-by val (update-vals groups count))]
      (log/info k v))    
    (log/info "Total count:" (count concept-map))
    groups))


(deftest synthetic-ssyk-tree-test

  "In this test we construct a labor market taxonomy of occupations. Given
some concept ids, we look up broader concepts. The queries in this test will
include clauses with up to two unknown variables.

We perform two queries. In the first query, we only provide one input id and
look up concepts broader than that id. Here, we expect all strategies except
identity to perform well because there will always be some substitutions to be made.

In the second query, we provide two input ids. This means that the select-simple
strategy will not perform the substitution that it performed in the first case where
we only had one one input id. Therefore, it will perform about as bad as identity."
  
  (testing "Given some concepts, query concepts that are broader."
    (let [conn (initialize-test-db0)
          ssyk-data (make-forest
                     [3 "ssyk-level-1"
                      5 "ssyk-level-2"
                      30 "ssyk-level-3"
                      5 "ssyk-level-4"
                      2 "occupation-name"])
          ssyk-concept-map (:concept-map ssyk-data)
          _ (d/transact conn {:tx-data (:tx-data ssyk-data)})
          concepts-per-type (group-concepts-by-type ssyk-concept-map)
          ssyk-level-3-ids (concepts-per-type "ssyk-level-3")
          expected-result-fn (fn [concept-ids]
                               (into #{}
                                     (map (fn [cid]
                                            {:from_id cid
                                             :id (get-in ssyk-concept-map
                                                         [cid :parent-id])}))
                                     concept-ids))

          related-query '{:find [?from-id ?id],
                          :keys [from_id id],
                          :in [$ [?from-id ...]],
                          :where
                          [[?c :concept/id ?from-id]
                           [?c :concept/broader ?related-c]
                           [?related-c :concept/id ?id]]}]
      (testing "Query for 1 input concept id."
        (let [input-concept-id (first ssyk-level-3-ids)
              _ (is (string? input-concept-id))
              result-map (evaluate-strategy-times
                          (fn [strategy]
                            (d/q {:query related-query
                                  :settings {:relprod-strategy strategy}
                                  :args [(d/db conn) #{input-concept-id}]})))]
          (doseq [[_ {:keys [result]}] result-map]
            (is (= (expected-result-fn [input-concept-id])
                   (set result))))
          (log/info "Query related to 1 concept:")
          (print-strategy-times result-map)
          (is (faster-strategy? result-map :select-simple :identity))
          (is (faster-strategy? result-map :select-all :identity))
          (is (faster-strategy? result-map :expand-once :identity))))
      (testing "Query for 2 input ids."
        (let [input-ids (set (take 2 ssyk-level-3-ids))
              result-map (evaluate-strategy-times
                          (fn [strategy]
                            (d/q {:query related-query
                                  :settings {:relprod-strategy strategy}
                                  :args [(d/db conn) input-ids]})))]
          (doseq [[_ {:keys [result]}] result-map]
            (is (= (expected-result-fn input-ids)
                   (set result))))
          (log/info "Query related to 2 concepts:")
          (print-strategy-times result-map)
          (is (faster-strategy? result-map :select-all :identity))
          (is (faster-strategy? result-map :expand-once :identity))

          ;; Because we have more than one input id, select-simple no longer performs that well.
          (is (faster-strategy? result-map :select-all :select-simple))
          (is (faster-strategy? result-map :expand-once :select-simple)))))))

(deftest synthetic-ssyk-tree-test2

  "This test is designed for the select-all strategy to perform well. We construct a 
forest of four trees with each tree having 2000 subnodes each. We then pick the ids
of two of the root nodes of the trees and the ids from three of the children of one 
trees. Then we we query for all (parent,child) pairs.

The select-all strategy is going to expand the `[?cc :concept/broader ?pc]` to the 
6 = 2*3 combinations of the possible parent child pairs and consequently make six
queries to the database backend. Each such query will return either one or zero rows.

The expand-once strategy will only substitude the `?pc` variable in the 
`[?cc :concept/broader ?pc]` pattern because `?pc` has the smallest number of
possible bindings, that is two. So it will make two database lookups. Each one of 
those lookups will result in 2000 rows returned from the database backend and most 
of those rows will be discarded. So it will be slower.

Finally, the select-simple and identity strategies will both run the query
`[?cc :concept/broader ?pc]` without any substitutions which will return 8000 rows
to be filtered."
  
  (let [conn (initialize-test-db0)
        ssyk-data (make-forest [4 "ssyk-level-1" 2000 "ssyk-level-2"])
        ssyk-concept-map (:concept-map ssyk-data)
        _ (d/transact conn {:tx-data (:tx-data ssyk-data)})
        concepts-per-type (group-concepts-by-type ssyk-concept-map)]
    (testing "Query (parent,child) pairs from a *small* set of possible combinations in a labour market taxonomy."
      (let [parent-ids (take 2 (concepts-per-type "ssyk-level-1"))
            parent-id (first parent-ids)
            child-ids (take 3 (get-in ssyk-concept-map [parent-id :child-ids]))
            _ (is (= 2 (count parent-ids)))
            _ (is (= 3 (count child-ids)))
            result-map (evaluate-strategy-times
                        (fn [strategy]
                          (d/q {:query ' {:find [?parent-id ?child-id]
                                          :keys [parent_id child_id]
                                          :in [$
                                               [?parent-id ...]
                                               [?child-id ...]],
                                          :where
                                          [[?pc :concept/id ?parent-id]
                                           [?cc :concept/id ?child-id]
                                           [?cc :concept/broader ?pc]]}
                                :settings {:relprod-strategy strategy}
                                :args [(d/db conn)
                                       parent-ids
                                       child-ids]})))
            expected-result (into #{}
                                  (map (fn [child-id] {:parent_id parent-id :child_id child-id}))
                                  child-ids)]
        (is (= 3 (count expected-result)))
        (log/info "Find all edges between a few small sets of concept types")
        (print-strategy-times result-map)
        (doseq [[_k v] result-map]
          (is (-> v :result set (= expected-result))))
        (is (faster-strategy? result-map :select-all :expand-once))
        (is (faster-strategy? result-map :expand-once :select-simple))
        (is (faster-strategy? result-map :expand-once :identity))))))

(deftest synthetic-ssyk-tree-test3

  "This test is designed to show a case where select-all performs bad. We construct
a labor market taxonomy of 200 trees where each root node has one child. Then
we query all (parent, child) pairs.

When the select-all strategy encounters the `[?cc :concept/broader ?pc]` pattern,
it will perform 40000 = 200*200 substitutions for all possible combinations of 
`?cc` and `?pc`. Out of those 40000 combinations, only 200 will be valid. That means
39800 database backend queries that return nothing. All the other strategies, even 
identity, perform better than that."
  
  (let [conn (initialize-test-db0)
        ssyk-data (make-forest [200 "ssyk-level-1" 1 "ssyk-level-2"])
        ssyk-concept-map (:concept-map ssyk-data)
        _ (d/transact conn {:tx-data (:tx-data ssyk-data)})
        _concepts-per-type (group-concepts-by-type ssyk-concept-map)]
    (testing "Query (parent,child) pairs from a *large* set of possible combinations in a labour market taxonomy."
      (let [result-map (evaluate-strategy-times
                        (fn [strategy]
                          (d/q {:query ' {:find [?parent-id ?child-id]
                                          :keys [parent_id child_id]
                                          :in [$ %],
                                          :where
                                          [[?pc :concept/type "ssyk-level-1"]
                                           [?cc :concept/type "ssyk-level-2"]
                                           [?cc :concept/broader ?pc]
                                           [?pc :concept/id ?parent-id]
                                           [?cc :concept/id ?child-id]]}
                                :settings {:relprod-strategy strategy}
                                :args [(d/db conn)]})))
            expected-result (into #{}
                                  (keep (fn [[child-id {:keys [parent-id]}]]
                                          (when parent-id
                                            {:child_id child-id :parent_id parent-id})))
                                  ssyk-concept-map)]
        (is (= 200 (count expected-result)))
        (doseq [[_k v] result-map]
          (is (-> v :result set (= expected-result))))
        (log/info "Find all edges between concept types:")
        (print-strategy-times result-map)
        (is (faster-strategy? result-map :expand-once :select-all))))))
