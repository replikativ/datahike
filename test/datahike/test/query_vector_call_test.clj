(ns datahike.test.query-vector-call-test
  "A function or predicate call may be written as a vector, `[count ?v]`, as
   well as a list, `(count ?v)`. CBOR and JSON have no list type, so every
   query sent in those formats arrives with its calls as vectors, and the
   datalog parser accepts both. Before #1145 the planner read a one-argument
   vector call `[[count ?v] ?c]` as a data pattern with the lookup ref
   `[count ?v]` and raised \"Invalid entid\", and read an `or` branch
   `[[= ?n \"Ivan\"]]` as a branch holding the pattern `[= ?n \"Ivan\"]`, which
   answered wrongly without an error."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as q]))

(defn- fresh-db []
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :read}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :name :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one
                         :db/unique :db.unique/identity}])
      (d/transact conn [{:name "Ivan" :age 15}
                        {:name "Oleg" :age 37}
                        {:name "Petr" :age 40}])
      (d/db conn))))

;; The result cache is keyed by the query form, and a list spelling and a
;; vector spelling are `=`; disable it so every call runs the engine.
(defn- run [planner? query & args]
  (binding [q/*disable-planner* (not planner?)
            q/*query-result-cache?* false]
    (apply d/q query args)))

(def ^:private rules
  '[[(adult ?e) [?e :age ?a] [[>= ?a 18]]]
    [(shout ?e ?u) [?e :name ?n] [[clojure.string/upper-case ?n] ?u]]])

(deftest vector-calls-on-both-engines
  (let [db (fresh-db)
        ivan (:db/id (d/entity db [:name "Ivan"]))]
    (doseq [planner? [true false]]
      (testing (if planner? "planner" "base engine")
        (testing "one-argument function"
          (is (= 4 (run planner? '[:find ?c . :in $ ?v :where [[count ?v] ?c]]
                        db [1 2 3 4])))
          (is (= #{["IVAN"] ["OLEG"] ["PETR"]}
                 (run planner? '[:find ?u :where [?e :name ?n]
                                 [[clojure.string/upper-case ?n] ?u]]
                      db)))
          (is (= "ABC" (run planner? '[:find ?u . :in $ ?x
                                       :where [[clojure.string/upper-case ?x] ?u]]
                            db "abc"))
              "input bound to a constant")
          (is (= 7 (run planner? '[:find ?x . :where [[ground 7] ?x]] db)))
          (is (= #{[ivan]} (run planner? '[:find ?x :where [?e :name "Ivan"]
                                           [[identity ?e] ?x]]
                                db))))
        (testing "one-argument predicate"
          (is (= #{["Ivan"] ["Oleg"] ["Petr"]}
                 (run planner? '[:find ?n :where [?e :name ?n] [[some? ?n]]] db))))
        (testing "two-argument function and predicate"
          (is (= 1.0 (run planner? '[:find ?c . :in $ ?v :where [[get ?v 0] ?c]]
                          db [1.0 2.0])))
          (is (= #{["Oleg"] ["Petr"]}
                 (run planner? '[:find ?n :where [?e :name ?n] [?e :age ?a]
                                 [[> ?a 30]]]
                      db))))
        (testing "a constant output"
          (is (= #{["Ivan"] ["Oleg"] ["Petr"]}
                 (run planner? '[:find ?n :where [?e :name ?n] [[count ?n] 4]] db)))
          (is (= #{}
                 (run planner? '[:find ?n :where [?e :name ?n] [[count ?n] 3]] db))))
        (testing "inside not and not-join"
          (is (= #{["Ivan"] ["Oleg"] ["Petr"]}
                 (run planner? '[:find ?n :where [?e :name ?n]
                                 (not [[clojure.string/blank? ?n]])]
                      db)))
          (is (= #{["Oleg"] ["Petr"]}
                 (run planner? '[:find ?n :where [?e :name ?n] (not [[= ?n "Ivan"]])]
                      db)))
          (is (= #{["Petr"]}
                 (run planner? '[:find ?n :where [?e :name ?n]
                                 (not-join [?e] [?e :age ?a] [[odd? ?a]])]
                      db))))
        (testing "as an or branch"
          (is (= #{["Ivan"] ["Oleg"]}
                 (run planner? '[:find ?n :where [?e :name ?n]
                                 (or [[= ?n "Ivan"]] [[= ?n "Oleg"]])]
                      db)))
          (is (= #{["Ivan"] ["Petr"]}
                 (run planner? '[:find ?n :where [?e :name ?n]
                                 (or [[clojure.string/starts-with? ?n "I"]]
                                     [[clojure.string/ends-with? ?n "r"]])]
                      db)))
          (is (= #{["Ivan" "IVAN"] ["Ivan" "ivan"] ["Oleg" "OLEG"]
                   ["Oleg" "oleg"] ["Petr" "PETR"] ["Petr" "petr"]}
                 (run planner? '[:find ?n ?u :where [?e :name ?n]
                                 (or-join [?n ?u]
                                          [[clojure.string/upper-case ?n] ?u]
                                          [[clojure.string/lower-case ?n] ?u])]
                      db)))
          (is (= #{["Ivan"] ["Petr"]}
                 (run planner? '[:find ?n :where [?e :name ?n]
                                 (or (and [?e :age ?a] [[> ?a 38]])
                                     (and [?e :age ?a] [[< ?a 16]]))]
                      db))
              "inside an and branch"))
        (testing "in a rule body"
          (is (= #{["OLEG"] ["PETR"]}
                 (run planner? '[:find ?u :in $ % :where (adult ?e) (shout ?e ?u)]
                      db rules))))
        (testing "an inline lookup ref is still a lookup ref"
          (is (= 15 (run planner? '[:find ?a . :where [[:name "Ivan"] :age ?a]] db))))))))

;; The normalized-query memo is keyed by the query form, and a list spelling
;; and a vector spelling are `=` with one hash, so whichever ran first in the
;; process decided how the other was read. Each case below uses a form no
;; other test uses, so its first run here is its first run in the process.
(deftest list-and-vector-spellings-in-either-order
  (let [db (fresh-db)]
    (doseq [planner? [true false]]
      (testing (if planner? "planner" "base engine")
        (testing "list form first, then vector form"
          (let [list-q (if planner?
                         '[:find ?c1 . :in $ ?v1 :where [(count ?v1) ?c1]]
                         '[:find ?c2 . :in $ ?v2 :where [(count ?v2) ?c2]])
                vec-q  (if planner?
                         '[:find ?c1 . :in $ ?v1 :where [[count ?v1] ?c1]]
                         '[:find ?c2 . :in $ ?v2 :where [[count ?v2] ?c2]])]
            (is (= list-q vec-q) "the two spellings are one memo key")
            (is (= 3 (run planner? list-q db [1 2 3])))
            (is (= 3 (run planner? vec-q db [1 2 3])))))
        (testing "vector form first, then list form"
          (let [vec-q  (if planner?
                         '[:find ?n3 :where [?e3 :name ?n3] [[some? ?n3]]]
                         '[:find ?n4 :where [?e4 :name ?n4] [[some? ?n4]]])
                list-q (if planner?
                         '[:find ?n3 :where [?e3 :name ?n3] [(some? ?n3)]]
                         '[:find ?n4 :where [?e4 :name ?n4] [(some? ?n4)]])]
            (is (= list-q vec-q) "the two spellings are one memo key")
            (is (= #{["Ivan"] ["Oleg"] ["Petr"]} (run planner? vec-q db)))
            (is (= #{["Ivan"] ["Oleg"] ["Petr"]} (run planner? list-q db)))))))))
