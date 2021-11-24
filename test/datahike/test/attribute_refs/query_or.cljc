(ns datahike.test.attribute-refs.query-or
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest are]]
      :clj [clojure.test :as t :refer [is deftest are]])
   [datahike.test.attribute-refs.util :refer [ref-db ref-e0
                                              shift-entities
                                              wrap-direct-datoms
                                              shift]]
   [datahike.api :as d]))

(def test-db
  (d/db-with ref-db
             (shift-entities ref-e0 [{:db/id 1 :weight 40 :age 10}
                                     {:db/id 2 :weight 40 :age 20}
                                     {:db/id 3 :weight 45 :age 10}
                                     {:db/id 4 :weight 45 :age 20}
                                     {:db/id 5 :weight 40 :age 10}
                                     {:db/id 6 :weight 40 :age 20}])))

(deftest test-or
  (are [q res] (= (into #{} (map vector) res)
                  (d/q (concat '[:find ?e :where] q) test-db))

    ;; intersecting results
    '[(or [?e :weight 45]
          [?e :age 10])]
    (shift #{1 3 4 5} ref-e0)

    ;; one branch empty
    '[(or [?e :weight 45]
          [?e :age 30])]
    (shift #{3 4} ref-e0)

    ;; both empty
    '[(or [?e :weight 60]
          [?e :age 30])]
    #{}

    ;; join with 1 var
    '[[?e :weight 40]
      (or [?e :weight 45]
          [?e :age 10])]
    (shift #{1 5} ref-e0)

    ;; join with 2 vars
    '[[?e :age ?a]
      (or (and [?e :weight 40]
               [_ :age  ?a])
          (and [?e :weight 45]
               [_ :age  ?a]))]
    (shift #{1 2 3 4 5 6} ref-e0)

    ;; OR introduces vars
    '[(or (and [?e :weight 40]
               [_ :age  ?a])
          (and [?e :weight 45]
               [_ :age  ?a]))
      [?e :age ?a]]
    (shift #{1 2 3 4 5 6} ref-e0)

    ;; OR introduces vars in different order
    '[(or (and [?e :weight 40]
               [_  :age  ?a])
          (and [_  :age  ?a]
               [?e :weight 45]))
      [?e :age ?a]]
    (shift #{1 2 3 4 5 6} ref-e0)))

(deftest test-or-join
  (are [q res] (= (into #{} (map vector) res)
                  (d/q (concat '[:find ?e :where] q)
                       test-db))
    '[(or-join [?e]
               [?e :weight ?n]
               (and [?e :age ?a]
                    [?e :weight ?n]))]
    (shift #{1 2 3 4 5 6} ref-e0)

    '[[?e  :weight ?a]
      [?e2 :weight ?a]
      (or-join [?e]
               (and [?e  :age ?a]
                    [?e2 :age ?a]))]
    (shift #{1 2 3 4 5 6} ref-e0)))

(deftest test-default-source
  (let [db1 (d/db-with ref-db
                       (wrap-direct-datoms ref-db ref-e0 :db/add [[1 :weight 40]
                                                                  [2 :weight 45]]))
        db2 (d/db-with ref-db
                       (wrap-direct-datoms ref-db ref-e0 :db/add [[1 :age 10]
                                                                  [2 :age 20]]))]
    (are [q res] (= (d/q (concat '[:find ?e :in $ $2 :where] q) db1 db2)
                    (into #{} (map vector) res))
      ;; OR inherits default source
      '[[?e :weight]
        (or [?e :weight 40])]
      (shift #{1} ref-e0)

      ;; OR can reference any source
      '[[?e :weight]
        (or [$2 ?e :age 10])]
      (shift #{1} ref-e0)

      ;; OR can change default source
      '[[?e :weight]
        ($2 or [?e :age 10])]
      (shift #{1} ref-e0)

      ;; even with another default source, it can reference any other source explicitly
      '[[?e :weight]
        ($2 or [$ ?e :weight 40])]
      (shift #{1} ref-e0)

      ;; nested OR keeps the default source
      '[[?e :weight]
        ($2 or (or [?e :age 10]))]
      (shift #{1} ref-e0)

      ;; can override nested OR source
      '[[?e :weight]
        ($2 or ($ or [?e :weight 40]))]
      (shift #{1} ref-e0))))

(deftest test-errors
  (is (thrown-msg? "Join variable not declared inside clauses: [?a]"
                   (d/q '[:find ?e
                          :where (or [?e :weight _]
                                     [?e :age ?a])]
                        test-db)))

  (is (thrown-msg? "Insufficient bindings: #{?e} not bound in (or-join [[?e]] [?e :weight 40])"
                   (d/q '[:find ?e
                          :where (or-join [[?e]]
                                          [?e :weight 40])]
                        test-db))))
