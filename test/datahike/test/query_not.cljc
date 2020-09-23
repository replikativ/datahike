(ns datahike.test.query-not
  (:require
    #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer [is are deftest testing]])
    [datahike.core :as d]
    [datahike.test.core :as tdc])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(def test-db
  (delay (d/db-with
           (d/empty-db)
           [{:db/id tdc/e1 :name "Ivan" :age 10}
            {:db/id tdc/e2 :name "Ivan" :age 20}
            {:db/id tdc/e3 :name "Oleg" :age 10}
            {:db/id tdc/e4 :name "Oleg" :age 20}
            {:db/id tdc/e5 :name "Ivan" :age 10}
            {:db/id tdc/e6 :name "Ivan" :age 20}])))

(deftest test-not
  (are [q res] (= (set (d/q (concat '[:find [?e ...] :where] q) @test-db))
                  res)
               '[[?e :name]
                (not [?e :name "Ivan"])]
               #{tdc/e3 tdc/e4}

               '[[?e :name]
                (not
                  [?e :name "Ivan"]
                  [?e :age 10])]
               #{tdc/e2 tdc/e3 tdc/e4 tdc/e6}

               '[[?e :name]
                (not [?e :name "Ivan"])
                (not [?e :age 10])]
               #{tdc/e4}

               ;; full exclude
               '[[?e :name]
                (not [?e :age])]
               #{}

               ;; not-intersecting rels
               '[[?e :name "Ivan"]
                (not [?e :name "Oleg"])]
               #{tdc/e1 tdc/e2 tdc/e5 tdc/e6}

               ;; exclude empty set
               '[[?e :name]
                (not [?e :name "Ivan"]
                     [?e :name "Oleg"])]
               #{tdc/e1 tdc/e2 tdc/e3 tdc/e4 tdc/e5 tdc/e6}

               ;; nested excludes
               '[[?e :name]
                (not [?e :name "Ivan"]
                     (not [?e :age 10]))]
               #{tdc/e1 tdc/e3 tdc/e4 tdc/e5}

               ;; extra binding in not
               '[[?e :name ?a]
                (not [?e :age ?f]
                     [?e :age 10])]
               #{tdc/e2 tdc/e4 tdc/e6}))


(deftest test-not-join
  (are [q res] (= (d/q (concat '[:find ?e ?a :where] q) @test-db)
                  res)
               '[[?e :name]
                [?e :age ?a]
                (not-join [?e]
                          [?e :name "Oleg"]
                          [?e :age ?a])]
               #{[tdc/e1 10] [tdc/e2 20] [tdc/e5 10] [tdc/e6 20]}

               '[[?e :age ?a]
                [?e :age 10]
                (not-join [?e]
                          [?e :name "Oleg"]
                          [?e :age ?a]
                          [?e :age 10])]
               #{[tdc/e1 10] [tdc/e5 10]}))


(deftest test-default-source
  (let [db1 (d/db-with (d/empty-db)
                       [[:db/add tdc/e1 :name "Ivan"]
                        [:db/add tdc/e2 :name "Oleg"]])
        db2 (d/db-with (d/empty-db)
                       [[:db/add tdc/e1 :age 10]
                        [:db/add tdc/e2 :age 20]])]
    (are [q res] (= (set (d/q (concat '[:find [?e ...]
                                        :in $ $2
                                        :where]
                                      q)
                              db1 db2))
                    res)
                 ;; NOT inherits default source
                 '[[?e :name]
                  (not [?e :name "Ivan"])]
                 #{tdc/e2}

                 ;; NOT can reference any source
                 '[[?e :name]
                  (not [$2 ?e :age 10])]
                 #{tdc/e2}

                 ;; NOT can change default source
                 '[[?e :name]
                  ($2 not [?e :age 10])]
                 #{tdc/e2}

                 ;; even with another default source, it can reference any other source explicitly
                 '[[?e :name]
                  ($2 not [$ ?e :name "Ivan"])]
                 #{tdc/e2}

                 ;; nested NOT keeps the default source
                 '[[?e :name]
                  ($2 not (not [?e :age 10]))]
                 #{tdc/e1}

                 ;; can override nested NOT source
                 '[[?e :name]
                  ($2 not ($ not [?e :name "Ivan"]))]
                 #{tdc/e1})))

(deftest test-impl-edge-cases
  (are [q res] (= (d/q q @test-db)
                  res)
               ;; const \ empty
               '[:find ?e
                :where [?e :name "Oleg"]
                [?e :age 10]
                (not [?e :age 20])]
               #{[tdc/e3]}

               ;; const \ const
               '[:find ?e
                :where [?e :name "Oleg"]
                [?e :age 10]
                (not [?e :age 10])]
               #{}

               ;; rel \ const
               '[:find ?e
                :where [?e :name "Oleg"]
                (not [?e :age 10])]
               #{[tdc/e4]}

               ;; 2 rels \ 2 rels
               '[:find ?e ?e2
                :where [?e :name "Ivan"]
                [?e2 :name "Ivan"]
                (not [?e :age 10]
                     [?e2 :age 20])]
               #{[tdc/e2 tdc/e1] [tdc/e6 tdc/e5] [tdc/e1 tdc/e1] [tdc/e2 tdc/e2] [tdc/e5 tdc/e5] [tdc/e6 tdc/e6] [tdc/e2 tdc/e5] [tdc/e1 tdc/e5] [tdc/e2 tdc/e6]
                 [tdc/e6 tdc/e1] [tdc/e5 tdc/e1] [tdc/e6 tdc/e2]}

               ;; 2 rels \ rel + const
               '[:find ?e ?e2
                :where [?e :name "Ivan"]
                [?e2 :name "Oleg"]
                (not [?e :age 10]
                     [?e2 :age 20])]
               #{[tdc/e2 tdc/e3] [tdc/e1 tdc/e3] [tdc/e2 tdc/e4] [tdc/e6 tdc/e3] [tdc/e5 tdc/e3] [tdc/e6 tdc/e4]}

               ;; 2 rels \ 2 consts
               '[:find ?e ?e2
                :where [?e :name "Oleg"]
                [?e2 :name "Oleg"]
                (not [?e :age 10]
                     [?e2 :age 20])]
               #{[tdc/e4 tdc/e3] [tdc/e3 tdc/e3] [tdc/e4 tdc/e4]}
               ))


(deftest test-insufficient-bindings
  (are [q msg] (thrown-msg? msg
                            (d/q (concat '[:find ?e :where] q) @test-db))
               '[(not [?e :name "Ivan"])
                [?e :name]]
               "Insufficient bindings: none of #{?e} is bound in (not [?e :name \"Ivan\"])"

               '[[?e :name]
                (not-join [?e]
                          (not [1 :age ?a])
                          [?e :age ?a])]
               "Insufficient bindings: none of #{?a} is bound in (not [1 :age ?a])"

               '[[?e :name]
                (not [?a :name "Ivan"])]
               "Insufficient bindings: none of #{?a} is bound in (not [?a :name \"Ivan\"])"
               ))
