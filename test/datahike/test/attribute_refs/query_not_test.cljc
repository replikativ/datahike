(ns datahike.test.attribute-refs.query-not-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [deftest are]]
      :clj [clojure.test :as t :refer [deftest are]])
   [datahike.test.attribute-refs.utils :refer [ref-db ref-e0
                                               shift-entities shift shift-in
                                               wrap-direct-datoms]]
   [datahike.api :as d]))

(def test-db
  (d/db-with ref-db
             (shift-entities ref-e0 [{:db/id 1 :mname "Ivan" :age 10}
                                     {:db/id 2 :mname "Ivan" :age 20}
                                     {:db/id 3 :mname "Oleg" :age 10}
                                     {:db/id 4 :mname "Oleg" :age 20}
                                     {:db/id 5 :mname "Ivan" :age 10}
                                     {:db/id 6 :mname "Ivan" :age 20}])))

(deftest test-not
  (are [q res] (= res
                  (set (d/q (concat '[:find [?e ...] :where] q) test-db)))
    '[[?e :mname]
      (not [?e :mname "Ivan"])]
    (shift #{3 4} ref-e0)

    '[[?e :mname]
      (not
       [?e :mname "Ivan"]
       [?e :age  10])]
    (shift #{2 3 4 6} ref-e0)

    '[[?e :mname]
      (not [?e :mname "Ivan"])
      (not [?e :age 10])]
    (shift #{4} ref-e0)

    ;; full exclude
    '[[?e :mname]
      (not [?e :age])]
    #{}

    ;; not-intersecting rels
    '[[?e :mname "Ivan"]
      (not [?e :mname "Oleg"])]
    (shift #{1 2 5 6} ref-e0)

    ;; exclude empty set
    '[[?e :mname]
      (not [?e :mname "Ivan"]
           [?e :mname "Oleg"])]
    (shift #{1 2 3 4 5 6} ref-e0)

    ;; nested excludes
    '[[?e :mname]
      (not [?e :mname "Ivan"]
           (not [?e :age 10]))]
    (shift #{1 3 4 5} ref-e0)

    ;; extra binding in not
    '[[?e :mname ?a]
      (not [?e :age ?f]
           [?e :age 10])]
    (shift #{2 4 6} ref-e0)))

(deftest test-not-join
  (are [q res] (= res
                  (d/q (concat '[:find ?e ?a :where] q)
                       test-db))
    '[[?e :mname]
      [?e :age  ?a]
      (not-join [?e]
                [?e :mname "Oleg"]
                [?e :age ?a])]
    (shift-in #{[1 10] [2 20] [5 10] [6 20]} [0] ref-e0)

    '[[?e :age  ?a]
      [?e :age  10]
      (not-join [?e]
                [?e :mname "Oleg"]
                [?e :age  ?a]
                [?e :age  10])]
    (shift-in #{[1 10] [5 10]} [0] ref-e0)))

(deftest test-default-source
  (let [db1 (d/db-with ref-db
                       (wrap-direct-datoms ref-db ref-e0 :db/add
                                           [[1 :mname "Ivan"]
                                            [2 :mname "Oleg"]]))
        db2 (d/db-with ref-db
                       (wrap-direct-datoms ref-db ref-e0 :db/add
                                           [[1 :age 10]
                                            [2 :age 20]]))]
    (are [q res] (= res
                    (set (d/q (concat '[:find [?e ...]
                                        :in   $ $2
                                        :where]
                                      q)
                              db1 db2)))
      ;; NOT inherits default source
      '[[?e :mname]
        (not [?e :mname "Ivan"])]
      (shift #{2} ref-e0)

      ;; NOT can reference any source
      '[[?e :mname]
        (not [$2 ?e :age 10])]
      (shift #{2} ref-e0)

      ;; NOT can change default source
      '[[?e :mname]
        ($2 not [?e :age 10])]
      (shift #{2} ref-e0)

      ;; even with another default source, it can reference any other source explicitly
      '[[?e :mname]
        ($2 not [$ ?e :mname "Ivan"])]
      (shift #{2} ref-e0)

      ;; nested NOT keeps the default source
      '[[?e :mname]
        ($2 not (not [?e :age 10]))]
      (shift #{1} ref-e0)

      ;; can override nested NOT source
      '[[?e :mname]
        ($2 not ($ not [?e :mname "Ivan"]))]
      (shift #{1} ref-e0))))

(deftest test-impl-edge-cases
  (are [q res] (= res
                  (d/q q test-db))
    ;; const \ empty
    '[:find ?e
      :where [?e :mname "Oleg"]
      [?e :age  10]
      (not [?e :age 20])]
    (shift-in #{[3]} [0] ref-e0)

    ;; const \ const
    '[:find ?e
      :where [?e :mname "Oleg"]
      [?e :age  10]
      (not [?e :age 10])]
    #{}

    ;; rel \ const
    '[:find ?e
      :where [?e :mname "Oleg"]
      (not [?e :age 10])]
    (shift-in #{[4]} [0] ref-e0)

    ;; 2 rels \ 2 rels
    '[:find ?e ?e2
      :where [?e  :mname "Ivan"]
      [?e2 :mname "Ivan"]
      (not [?e :age 10]
           [?e2 :age 20])]
    (shift-in #{[2 1] [6 5] [1 1] [2 2] [5 5] [6 6] [2 5] [1 5] [2 6] [6 1] [5 1] [6 2]} [0 1] ref-e0)

    ;; 2 rels \ rel + const
    '[:find ?e ?e2
      :where [?e  :mname "Ivan"]
      [?e2 :mname "Oleg"]
      (not [?e :age 10]
           [?e2 :age 20])]
    (shift-in #{[2 3] [1 3] [2 4] [6 3] [5 3] [6 4]} [0 1] ref-e0)

    ;; 2 rels \ 2 consts
    '[:find ?e ?e2
      :where [?e  :mname "Oleg"]
      [?e2 :mname "Oleg"]
      (not [?e :age 10]
           [?e2 :age 20])]
    (shift-in #{[4 3] [3 3] [4 4]} [0 1] ref-e0)))

(deftest test-insufficient-bindings
  (are [q msg] (thrown-msg? msg
                            (d/q (concat '[:find ?e :where] q)
                                 test-db))
    '[(not [?e :mname "Ivan"])
      [?e :mname]]
    "Insufficient bindings: none of #{?e} is bound in (not [?e :mname \"Ivan\"])"

    '[[?e :mname]
      (not-join [?e]
                (not [1 :age ?a])
                [?e :age ?a])]
    "Insufficient bindings: none of #{?a} is bound in (not [1 :age ?a])"

    '[[?e :mname]
      (not [?a :mname "Ivan"])]
    "Insufficient bindings: none of #{?a} is bound in (not [?a :mname \"Ivan\"])"))
