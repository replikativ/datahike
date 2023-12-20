(ns datahike.test.strategy-test
  (:require [clojure.test :refer [is deftest testing]]
            [datahike.test.utils :as utils]
            [datahike.api :as d]
            [datahike.query :as dq]))

(defn concept-id [index]
  (let [s (format "%010d" index)]
    (str (subs s 0 4) "_" (subs s 4 7) "_" (subs s 7 10))))

(defn concept-id-generator []
  (let [counter (atom 0)]
    #(concept-id (swap! counter inc))))

(defn temp-id [x]
  (str "tmp-" x))

(defn relation-id [cid0 rel cid1]
  (str cid0 ":" rel ":" cid1))

(defn make-tree [id-gen type-count-pairs]
  (loop [[tos & stack] [[nil type-count-pairs]]
         tx-data []
         counter 0
         concept-map {}]
    (if (nil? tos)
      {:tx-data tx-data
       :concept-map concept-map}
      (let [[parent-id [[this-type child-count] & remaining-pairs]] tos
            concept-id (id-gen)
            tid (temp-id concept-id)
            edge-tid (format "edge-tmp-%d" counter)]
        (recur (into stack
                     (when (seq remaining-pairs)
                       (repeat child-count [concept-id remaining-pairs])))
               (into tx-data
                     cat
                     [[[:db/add tid :concept/id concept-id]
                       [:db/add tid :concept/type this-type]]
                      (when parent-id
                        [[:db/add edge-tid :relation/id
                          (relation-id concept-id "broader" parent-id)]
                         [:db/add edge-tid :relation/concept-1
                          tid]
                         [:db/add edge-tid :relation/type "broader"]
                         [:db/add edge-tid :relation/concept-2
                          (temp-id parent-id)]])])
               (inc counter)
               (assoc concept-map concept-id {:parent-id parent-id
                                              :type this-type
                                              :id concept-id}))))))
(def schema [ ;; Concepts
             #:db{:ident :concept/id,
                  :valueType :db.type/string,
                  :cardinality :db.cardinality/one,
                  :doc "Unique identifier for concepts",
                  :unique :db.unique/identity}
             #:db{:ident :concept/type,
                  :valueType :db.type/string,
                  :cardinality :db.cardinality/one,
                  :doc "The concepts main type"}

             ;; Relations
             #:db{:ident :relation/id,
                  :valueType :db.type/string,
                  :cardinality :db.cardinality/one,
                  :doc "Unique identifier for relations",
                  :unique :db.unique/identity}
             #:db{:ident :relation/type,
                  :valueType :db.type/string,
                  :cardinality :db.cardinality/one,
                  :doc "the type of relationship"}
             #:db{:ident :relation/concept-1,
                  :valueType :db.type/ref,
                  :cardinality :db.cardinality/one,
                  :doc "The entity ID of the first concept in a relation"}
             #:db{:ident :relation/concept-2,
                  :valueType :db.type/ref,
                  :cardinality :db.cardinality/one,
                  :doc "The entity ID of the second concept in a relation"}])

(defn initialize-test-db0 []
  (let [conn (utils/setup-db {:store {:backend :mem}
                              :schema-flexibility :write
                              :attribute-refs? false
                              :keep-history? true})]
    (d/transact conn {:tx-data schema})
    conn))

(def rules '[[(-forward-edge ?from-concept ?type ?to-concept ?relation)
              [(ground
                ["broader"
                 "related"
                 "possible-combination"
                 "unlikely-combination"
                 "substitutability"
                 "broad-match"
                 "exact-match"
                 "close-match"])
               [?type ...]]
              [?relation :relation/concept-1 ?from-concept]
              [?relation :relation/type ?type]
              [?relation :relation/concept-2 ?to-concept]]
             [(-reverse-edge ?from-concept ?type ?to-concept ?relation)
              [(ground
                {"narrower" "broader",
                 "related" "related",
                 "possible-combination" "possible-combination",
                 "unlikely-combination" "unlikely-combination",
                 "substituted-by" "substitutability",
                 "narrow-match" "broad-match",
                 "exact-match" "exact-match",
                 "close-match" "close-match"})
               [[?type ?reverse-type]]]
              [?relation :relation/concept-2 ?from-concept]
              [?relation :relation/type ?reverse-type]
              [?relation :relation/concept-1 ?to-concept]]
             [(edge ?from-concept ?type ?to-concept ?relation)
              (or
               (-forward-edge ?from-concept ?type ?to-concept ?relation)
               (-reverse-edge ?from-concept ?type ?to-concept ?relation))]])

(defn evaluate-strategy-times [f]
  (into {}
        (map (fn [[k strategy]] [k (let [start (System/nanoTime)
                                         result (f strategy)
                                         end (System/nanoTime)]
                                     {:result result
                                      :elapsed-ns (- end start)})]))
        [[:identity identity]
         [:select-simple dq/relprod-select-simple]
         [:select-all dq/relprod-select-all]
         [:expand-once dq/relprod-expand-once]]))

(defn print-strategy-times [result-map]
  (doseq [[k v] (sort-by (comp :elapsed-ns val) result-map)]
    (println (format "%16s: %.6f" (name k) (* 1.0e-9 (:elapsed-ns v))))))

(defn faster-strategy? [strategy-times key-a key-b]
  (< (get-in strategy-times [key-a :elapsed-ns])
     (get-in strategy-times [key-b :elapsed-ns])))

(def related-query '{:find [?from-id ?id],
                     :keys [from_id id],
                     :in [$ % [?from-id ...] ?relation-type],
                     :where
                     [[?c :concept/id ?from-id]
                      (edge ?c ?relation-type ?related-c ?r)
                      [?related-c :concept/id ?id]]})

(deftest synthetic-ssyk-tree-test
  (let [conn (initialize-test-db0)
        cgen (concept-id-generator)
        ssyk-data (make-tree cgen
                             [["ssyk-level-1" 8]
                              ["ssyk-level-2" 8]
                              ["ssyk-level-3" 32]
                              ["ssyk-level-4" 2]
                              ["occupation-name" 0]])
        ssyk-concept-map (:concept-map ssyk-data)
        _ (d/transact conn {:tx-data (:tx-data ssyk-data)})
        concepts-per-type (update-vals (group-by (comp :type val) ssyk-concept-map)
                                       (fn [kv-pairs]
                                         (mapv first kv-pairs)))
        ;;ssyk-level-2-ids (concepts-per-type "ssyk-level-2")
        ssyk-level-3-ids (concepts-per-type "ssyk-level-3")
        expected-result-fn (fn [concept-ids]
                             (into #{}
                                   (map (fn [cid]
                                          {:from_id cid
                                           :id (get-in ssyk-concept-map
                                                       [cid :parent-id])}))
                                   concept-ids))]
    (def the-concepts-per-type concepts-per-type)
    (doseq [[k v] (sort-by val (update-vals concepts-per-type count))]
      (println k v))
    (is (seq ssyk-level-3-ids))

    (testing "That the strategies select-simple, select-all and expand-once are more efficient than identiy"
      (let [input-concept-id (first ssyk-level-3-ids)
            _ (is (string? input-concept-id))
            result-map (evaluate-strategy-times
                        (fn [strategy]
                          (d/q {:query related-query
                                :settings {:relprod-strategy strategy}
                                :args [(d/db conn)
                                       rules
                                       #{input-concept-id}
                                       "broader"]})))]
        (doseq [[_ {:keys [result]}] result-map]
          (is (= (expected-result-fn [input-concept-id])
                 (set result))))
        (println "Query related to 1 concept:")
        (print-strategy-times result-map)
        (is (faster-strategy? result-map :select-simple :identity))
        (is (faster-strategy? result-map :select-all :identity))
        (is (faster-strategy? result-map :expand-once :identity))))
    (testing "Test that the strategies select-all and expand-once are more efficient in case we have more than one input id"
      (let [input-ids (set (take 2 ssyk-level-3-ids))
            result-map (evaluate-strategy-times
                        (fn [strategy]
                          (d/q {:query related-query
                                :settings {:relprod-strategy strategy}
                                :args [(d/db conn)
                                       rules
                                       input-ids
                                       "broader"]})))]
        (doseq [[_ {:keys [result]}] result-map]
          (is (= (expected-result-fn input-ids)
                 (set result))))
        (println "Query related to 2 concepts:")
        (print-strategy-times result-map)
        (is (faster-strategy? result-map :select-all :identity))
        (is (faster-strategy? result-map :expand-once :identity))

        ;; Because we have more than one input id the select-simple strategy will
        ;; not be able to do as many substitutions. Therefore, it will perform worse than
        ;; select-all and expand-once. But it may still be faster than identity, although
        ;; the difference is no longer as remarkable.
        (is (faster-strategy? result-map :select-all :select-simple))
        (is (faster-strategy? result-map :expand-once :select-simple))))
    
    (testing "Find all edges between ssyk-level-2 concepts and ssyk-level-3 concepts. The strategy expand-once seems about twice as fast as select-all"
      (let [result-map (evaluate-strategy-times
                        (fn [strategy]
                          (d/q {:query ' {:find [?parent-id ?child-id]
                                          :keys [parent_id child_id]
                                          :in [$ %],
                                          :where
                                          [[?pc :concept/type "ssyk-level-2"]
                                           [?cc :concept/type "ssyk-level-3"]
                                           (edge ?cc "broader" ?pc ?r)
                                           [?pc :concept/id ?parent-id]
                                           [?cc :concept/id ?child-id]]}
                                :settings {:relprod-strategy strategy}
                                :args [(d/db conn)
                                       rules]})))]
        (doseq [[_k v] result-map]
          (is (= 64 (count (:result v)))))
        (println "Find all edges between concept types:")
        (print-strategy-times result-map)
        (is (faster-strategy? result-map :expand-once :select-all))))


    (testing "Find all edges between ssyk-level-2 concepts and ssyk-level-3 concepts and let the edge type be a parameter. Why is expand-once better than select-all?"
      (let [result-map (evaluate-strategy-times
                        (fn [strategy]
                          (d/q {:query ' {:find [?parent-id ?child-id]
                                          :keys [parent_id child_id]
                                          :in [$ % [?relation-type ...]],
                                          :where
                                          [[?pc :concept/type "ssyk-level-2"]
                                           [?cc :concept/type "ssyk-level-3"]
                                           (edge ?cc ?relation-type ?pc ?r)
                                           [?pc :concept/id ?parent-id]
                                           [?cc :concept/id ?child-id]]}
                                :settings {:relprod-strategy strategy}
                                :args [(d/db conn)
                                       rules
                                       #{"broader" "narrower" "related" "close-match"}]})))]
        (doseq [[_k v] result-map]
          (is (= 64 (count (:result v)))))
        (println "Find all edges between concept types with the edge type being parameterized:")
        (print-strategy-times result-map)
        (is (faster-strategy? result-map :expand-once :select-all))))

    (testing "WIP: why is expand-once so fast???"
      (let [result-map (evaluate-strategy-times
        (fn [strategy]
          (d/q {:query ' {:find [?parent-id ?child-id]
                          :keys [parent_id child_id]
                          :in [$ %
                               [?parent-id ...]
                               [?child-id ...]],
                          :where
                          [[?pc :concept/id ?parent-id]
                           [?cc :concept/id ?child-id]
                           (edge ?cc "broader" ?pc ?r)]}
                :settings {:relprod-strategy strategy}
                :args [(d/db conn)
                       rules
                       (take 2 (the-concepts-per-type "ssyk-level-3"))
                       (take 2 (the-concepts-per-type "ssyk-level-4"))]})))]
        (println "Find all edges between a few small sets of concept types")
        (print-strategy-times result-map)))))
