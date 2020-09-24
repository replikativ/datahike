(ns datahike.test.query-or
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest]]
      :clj  [clojure.test :as t :refer [is are deftest]])
   [datahike.core :as d]
   [datahike.test.core :as tdc]))

(def test-db
  (delay (d/db-with
          (d/empty-db)
          [{:db/id tdc/e1 :name "Ivan" :age 10}
           {:db/id tdc/e2 :name "Ivan" :age 20}
           {:db/id tdc/e3 :name "Oleg" :age 10}
           {:db/id tdc/e4 :name "Oleg" :age 20}
           {:db/id tdc/e5 :name "Ivan" :age 10}
           {:db/id tdc/e6 :name "Ivan" :age 20}])))

(deftest test-or
  (are [q res] (= (d/q (concat '[:find ?e :in $ :where] (quote q)) ;; TODO: make dependent on database config
                       @test-db)
                  (into #{} (map vector) res))

               ;; intersecting results
    [(or [?e :name "Oleg"]
         [?e :age 10])]
    #{tdc/e1 tdc/e3 tdc/e4 tdc/e5}

               ;; one branch empty
    [(or [?e :name "Oleg"]
         [?e :age 30])]
    #{tdc/e3 tdc/e4}

               ;; both empty
    [(or [?e :name "Petr"]
         [?e :age 30])]
    #{}

               ;; join with 1 var
    [[?e :name "Ivan"]
     (or [?e :name "Oleg"]
         [?e :age 10])]
    #{tdc/e1 tdc/e5}

    #_(identity                                  ;; TODO: activate tests for config without system-schema in database
                  ;; join with 2 vars
       [[?e :age ?a]
        (or (and [?e :name "Ivan"]
                 [1 :age ?a])
            (and [?e :name "Oleg"]
                 [2 :age ?a]))]
       #{tdc/e1 tdc/e5 tdc/e4}

               ;; OR introduces vars
       [(or (and [?e :name "Ivan"]
                 [1 :age ?a])
            (and [?e :name "Oleg"]
                 [2 :age ?a]))
        [?e :age ?a]]
       #{tdc/e1 tdc/e5 tdc/e4}

                  ;; OR introduces vars in different order
       [(or (and [?e :name "Ivan"]
                 [1 :age ?a])
            (and [2 :age ?a]
                 [?e :name "Oleg"]))
        [?e :age ?a]]
       #{tdc/e1 tdc/e5 tdc/e4})))

(deftest test-or-join
  (are [q res] (= (d/q (concat '[:find ?e :where] q) @test-db)
                  (into #{} (map vector) res))
    '[(or-join [?e]
               [?e :name ?n]
               (and [?e :age ?a]
                    [?e :name ?n]))]
    #{tdc/e1 tdc/e2 tdc/e3 tdc/e4 tdc/e5 tdc/e6}

    '[[?e :name ?a]
      [?e2 :name ?a]
      (or-join [?e]
               (and [?e :age ?a]
                    [?e2 :age ?a]))]
    #{tdc/e1 tdc/e2 tdc/e3 tdc/e4 tdc/e5 tdc/e6})

  (is (= #{[:a1 :b1 :c1]
           [:a2 :b2 :c2]}
         (d/q '[:find ?a ?b ?c
                :in $xs $ys
                :where [$xs ?a ?b ?c]                       ;; check join by ?a, ignoring ?b, dropping ?c ?d
                (or-join [?a]
                         [$ys ?a ?b ?d])]
              [[:a1 :b1 :c1]
               [:a2 :b2 :c2]
               [:a3 :b3 :c3]]
              [[:a1 :b1 :d1]                                ;; same ?a, same ?b
               [:a2 :b2* :d2]                               ;; same ?a, different ?b. Should still be joined
               [:a4 :b4 :c4]])))                            ;; different ?a, should be dropped

  (is (= #{[:a1 :c1] [:a2 :c2]}
         (d/q '[:find ?a ?c
                :in $xs $ys
                :where (or-join [?a ?c]
                                [$xs ?a ?b ?c]              ; rel with hole (?b gets dropped, leaving {?a 0 ?c 2} and 3-element tuples)
                                [$ys ?a ?c])]
              [[:a1 :b1 :c1]]
              [[:a2 :c2]]))))

(deftest test-default-source
  (let [db1 (d/db-with (d/empty-db)
                       [[:db/add tdc/e1 :name "Ivan"]
                        [:db/add tdc/e2 :name "Oleg"]])
        db2 (d/db-with (d/empty-db)
                       [[:db/add tdc/e1 :age 10]
                        [:db/add tdc/e2 :age 20]])]
    (are [q res] (= (d/q (concat '[:find ?e :in $ $2 :where] q) db1 db2)
                    (into #{} (map vector) res))
                 ;; OR inherits default source
      '[[?e :name]
        (or [?e :name "Ivan"])]
      #{tdc/e1}

                 ;; OR can reference any source
      '[[?e :name]
        (or [$2 ?e :age 10])]
      #{tdc/e1}

                 ;; OR can change default source
      '[[?e :name]
        ($2 or [?e :age 10])]
      #{tdc/e1}

                 ;; even with another default source, it can reference any other source explicitly
      '[[?e :name]
        ($2 or [$ ?e :name "Ivan"])]
      #{tdc/e1}

                 ;; nested OR keeps the default source
      '[[?e :name]
        ($2 or (or [?e :age 10]))]
      #{tdc/e1}

                 ;; can override nested OR source
      '[[?e :name]
        ($2 or ($ or [?e :name "Ivan"]))]
      #{tdc/e1})))

(deftest test-errors
  (is (thrown-msg? "Join variable not declared inside clauses: [?a]"
                   (d/q '[:find ?e
                          :where (or [?e :name _]
                                     [?e :age ?a])]
                        @test-db)))

  (is (thrown-msg? "Insufficient bindings: #{?e} not bound in (or-join [[?e]] [?e :name \"Ivan\"])"
                   (d/q '[:find ?e
                          :where (or-join [[?e]]
                                          [?e :name "Ivan"])]
                        @test-db))))