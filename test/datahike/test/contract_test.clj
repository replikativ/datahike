(ns datahike.test.contract-test
  (:require [clojure.test :refer [deftest is]]
            [datahike.api :as d]
            [datahike.experimental.contract :as contract]))

(def window
  '[:find ?id :in $ $delta
    :where [$delta ?lo ?hi ?target]
    [?entity :sample/n ?n] [(>= ?n ?lo)] [(<= ?n ?hi)]
    [?entity :sample/id ?id] [(= ?n ?target)]])

(def attrs #{:sample/id :sample/n})

(deftest indexed-window-and-projection
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write}]
    (d/create-database cfg)
    (let [c (d/connect cfg)]
      (try
        (d/transact c [{:db/ident :sample/n :db/valueType :db.type/long :db/index true
                        :db/cardinality :db.cardinality/one}
                       {:db/ident :sample/id :db/valueType :db.type/long :db/cardinality :db.cardinality/one}])
        (d/transact c (mapv #(hash-map :sample/n % :sample/id %) (range 150)))
        (let [db @c
              rows [[0 149 149]]
              run #(contract/execute %1 attrs 3 db %2 %3)
              result (run window rows 4096)
              project-all (vec (butlast window))
              expected (set (d/q window db rows))]
          (is (= #{[149]} expected (:rows result)))
          (is (= 150 (get-in result [:events :predicate])))
          (is (= :relation-limit (:status (run project-all rows 4096))))
          (doseq [limit [0 1 2 3 150 300 (dec (:used result))]]
            (let [r (run window rows limit)]
              (is (= [:out-of-fuel limit nil] ((juxt :status :used :rows) r)))
              (is (= limit (reduce + (vals (:events r)))))))
          (is (= #{[149]} (:rows (run window [[149 149 149]] 4096))))
          (is (= #{} (:rows (run window [[149 0 149]] 4096))))
          (doseq [form ['[:find ?sku (sum ?qty) (count ?qty) :with ?leg :in $ $delta
                          :where [$delta ?leg ?sku ?qty]]
                        '[:find ?sku (sum ?qty) :with ?leg :in $ $delta :where [$delta ?leg ?sku ?qty]]
                        '[:find ?sku (sum ?qty) :in $ $delta :where [$delta ?leg ?sku ?qty]]
                        '[:find ?leg :in $ $delta :where [$delta ?leg ?sku ?qty] [(< ?qty 0)]]]]
            (let [rows [[0 :a 2] [1 :a 2] [1 :a 2] [2 :b -1]]]
              (is (= (set (d/q form db rows)) (:rows (run form rows 4096))))))
          (let [sum-form '[:find (sum ?x) :with ?id :in $ $delta :where [$delta ?id ?x ?unused]]
                numeric '[:find ?id :in $ $delta :where [$delta ?id ?x ?unused] [(< ?x 4)]]]
            (doseq [[query input status] [[sum-form [[0 Long/MAX_VALUE 0] [1 1 0]] :numeric-overflow]
                                          [numeric [[0 :keyword 0]] :type-error]
                                          [window [["a" 3 3]] :type-error]]]
              (let [r (run query input 4096)]
                (is (= [status nil] ((juxt :status :rows) r))))))
          (let [form '[:find ?sku (sum ?qty) :with ?leg :in $ $delta :where [$delta ?leg ?sku ?qty]]
                data [[0 :nuts 2] [1 :nuts 2] [2 :bolts -3] [3 :bolts 3]]]
            (doseq [rows [data (vec (reverse data))]]
              (is (= #{[:nuts 4] [:bolts 0]} (:rows (run form rows 12))))
              (doseq [limit (range 12)]
                (let [r (run form rows limit)]
                  (is (= [:out-of-fuel limit nil] ((juxt :status :used :rows) r)))))))
          (doseq [clause ['[(slurp ?target)] '[?e ?attribute ?target] '[?e :secret/id ?target]
                          '[?e :sample/n ?new] '(external ?target) '[(identity ?target) ?x]
                          '(not-join [?new] [?e :sample/n ?new])]]
            (let [form (conj '[:find ?target :in $ $delta :where [$delta ?lo ?hi ?target]] clause)]
              (is (thrown? Exception (contract/admit form attrs 3 (:schema db))))))
          (is (thrown? Exception
                       (contract/admit '[:find ?e :in $ $delta :where [$delta ?x] [?e :sample/id ?x]]
                                       attrs 1 (:schema db)))))
        (finally (d/release c) (d/delete-database cfg))))))
