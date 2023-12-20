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

(defn make-tree [type-count-pairs]
  (loop [[tos & stack] [[nil type-count-pairs]]
         tx-data []
         counter 0
         concept-map {}]
    (if (nil? tos)
      {:tx-data tx-data
       :concept-map concept-map}
      (let [[parent-id [[this-type child-count] & remaining-pairs]] tos
            concept-id (concept-id counter)
            tid (temp-id concept-id)
            parent-tid (temp-id parent-id)]
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
         [:select-simple dq/relprod-select-simple]
         [:select-all dq/relprod-select-all]
         [:expand-once dq/relprod-expand-once]]))

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
      (println k v))    
    (log/info "Total count:" (count concept-map))
    groups))

(deftest synthetic-ssyk-tree-test
  (let [conn (initialize-test-db0)
        ssyk-data (make-tree [["ssyk-level-1" 8]
                              ["ssyk-level-2" 8]
                              ["ssyk-level-3" 32]
                              ["ssyk-level-4" 2]
                              ["occupation-name" 0]])
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
    (testing "That the strategies select-simple, select-all and expand-once are more efficient than identiy"
      (let [input-concept-id (first ssyk-level-3-ids)
            _ (is (string? input-concept-id))
            result-map (evaluate-strategy-times
                        (fn [strategy]
                          (d/q {:query related-query
                                :settings {:relprod-strategy strategy}
                                :args [(d/db conn)
                                       ;rules
                                       #{input-concept-id}
                                       ;"broader"
                                       ]})))]
        (doseq [[_ {:keys [result]}] result-map]
          (is (= (expected-result-fn [input-concept-id])
                 (set result))))
        (log/info "Query related to 1 concept:")
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
                                       input-ids]})))]
        (doseq [[_ {:keys [result]}] result-map]
          (is (= (expected-result-fn input-ids)
                 (set result))))
        (log/info "Query related to 2 concepts:")
        (print-strategy-times result-map)
        (is (faster-strategy? result-map :select-all :identity))
        (is (faster-strategy? result-map :expand-once :identity))

        ;; Because we have more than one input id, select-simple no longer performs that well.
        (is (faster-strategy? result-map :select-all :select-simple))
        (is (faster-strategy? result-map :expand-once :select-simple))))))

(deftest synthetic-ssyk-tree-test2
  (let [conn (initialize-test-db0)
        ssyk-data (make-tree [["ssyk-level-1" 4]
                              ["ssyk-level-2" 2000]
                              ["ssyk-level-3" 0]])
        ssyk-concept-map (:concept-map ssyk-data)
        _ (d/transact conn {:tx-data (:tx-data ssyk-data)})
        concepts-per-type (group-concepts-by-type ssyk-concept-map)]
    (testing "The select-all strategy is expected to perform best because of a small number of possible combinations"
      (let [parent-ids (take 2 (concepts-per-type "ssyk-level-2"))
            child-ids (take 3 (get-in ssyk-concept-map [(first parent-ids) :child-ids]))
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
                                       child-ids]})))]
        (log/info "Find all edges between a few small sets of concept types")
        (print-strategy-times result-map)
        (doseq [[_k v] result-map]
          (is (= 3 (count (:result v)))))
        (is (faster-strategy? result-map :select-all :expand-once))
        (is (faster-strategy? result-map :expand-once :select-simple))
        (is (faster-strategy? result-map :expand-once :identity))))))

(deftest synthetic-ssyk-tree-test3
  (let [conn (initialize-test-db0)
        ssyk-data (make-tree [["ssyk-level-1" 200]
                              ["ssyk-level-2" 1]
                              ["ssyk-level-3" 0]])
        ssyk-concept-map (:concept-map ssyk-data)
        _ (d/transact conn {:tx-data (:tx-data ssyk-data)})
        _concepts-per-type (group-concepts-by-type ssyk-concept-map)
        ]
    (testing "The select-all strategy will perform poorly because of a large number of possible combinations"
      (let [result-map (evaluate-strategy-times
                        (fn [strategy]
                          (d/q {:query ' {:find [?parent-id ?child-id]
                                          :keys [parent_id child_id]
                                          :in [$ %],
                                          :where
                                          [[?pc :concept/type "ssyk-level-2"]
                                           [?cc :concept/type "ssyk-level-3"]
                                           [?cc :concept/broader ?pc]
                                           [?pc :concept/id ?parent-id]
                                           [?cc :concept/id ?child-id]]}
                                :settings {:relprod-strategy strategy}
                                :args [(d/db conn)]})))]
        (doseq [[_k v] result-map]
          (is (= 200 (count (:result v)))))
        (log/info "Find all edges between concept types:")
        (print-strategy-times result-map)
        (is (faster-strategy? result-map :expand-once :select-all))))))
