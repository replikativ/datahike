(ns datahike.test.query-vector-call-test
  "A call is a list, `[(count ?v) ?c]`. Spelled as a vector, `[[count ?v] ?c]`,
   the engine read it as a data pattern or lookup ref: \"Invalid entid\" or, on
   the planner, a wrong answer (#1145). It is what a query looks like after a
   codec without a list type, so it is now refused with an error that says so."
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
                         :db/unique :db.unique/identity}
                        {:db/ident :friend :db/valueType :db.type/ref
                         :db/cardinality :db.cardinality/many}])
      (d/transact conn [{:db/id -1 :name "Ivan" :age 15 :friend [-1 -2]}
                        {:db/id -2 :name "Oleg" :age 37 :friend [-1]}
                        {:name "Petr" :age 40}])
      (d/db conn))))

;; The result cache is keyed by the query form, and a list and a vector
;; spelling are `=`; disable it so every call goes through normalization.
(defn- run [planner? query & args]
  (binding [q/*disable-planner* (not planner?)
            q/*query-result-cache?* false]
    (apply d/q query args)))

(defn- vector-call-error
  "The ex-data of the vector-call error `f` raises, or what it returned."
  [f]
  (try
    [:returned (f)]
    (catch clojure.lang.ExceptionInfo e
      (if (= :datahike.query/vector-call (:type (ex-data e)))
        (assoc (ex-data e) :message (ex-message e))
        [:other-error (ex-message e) (ex-data e)]))))

(def ^:private vector-rules
  '[[[adult ?e] [?e :age ?a] [(>= ?a 18)]]])

(def ^:private rules-with-vector-call
  '[[(adult ?e) [?e :age ?a] [[>= ?a 18]]]])

(deftest a-vector-spelled-call-is-refused-in-every-clause-position
  (let [db (fresh-db)
        cases
        [["one-argument function"
          '[:find ?c . :in $ ?v :where [[count ?v] ?c]] [db [1 2]] '[count ?v]]
         ["one-argument predicate"
          '[:find ?n :where [?e :name ?n] [[some? ?n]]] [db] '[some? ?n]]
         ["two-argument function"
          '[:find ?c . :in $ ?v :where [[get ?v 0] ?c]] [db [1 2]] '[get ?v 0]]
         ["two-argument predicate"
          '[:find ?n :where [?e :name ?n] [?e :age ?a] [[> ?a 30]]] [db] '[> ?a 30]]
         ["constant output"
          '[:find ?n :where [?e :name ?n] [[count ?n] 4]] [db] '[count ?n]]
         ["fully qualified function"
          '[:find ?u :where [?e :name ?n] [[clojure.string/upper-case ?n] ?u]] [db]
          '[clojure.string/upper-case ?n]]
         ["inside not"
          '[:find ?n :where [?e :name ?n] (not [[= ?n "Ivan"]])] [db] '[= ?n "Ivan"]]
         ["inside not-join"
          '[:find ?n :where [?e :name ?n] (not-join [?e] [?e :age ?a] [[odd? ?a]])] [db]
          '[odd? ?a]]
         ["or branch"
          '[:find ?n :where [?e :name ?n] (or [[= ?n "Ivan"]] [[= ?n "Oleg"]])] [db]
          '[= ?n "Ivan"]]
         ["or-join branch"
          '[:find ?n ?u :where [?e :name ?n]
            (or-join [?n ?u] [(clojure.string/upper-case ?n) ?u]
                     [[clojure.string/lower-case ?n] ?u])]
          [db] '[clojure.string/lower-case ?n]]
         ["and inside or"
          '[:find ?n :where [?e :name ?n]
            (or (and [?e :age ?a] [[> ?a 38]]) (and [?e :age ?a] [(< ?a 16)]))]
          [db] '[> ?a 38]]
         ["not written as a vector"
          '[:find ?n :where [?e :name ?n] [not [?e :age 15]]] [db] '[not [?e :age 15]]]
         ["or written as a vector"
          '[:find ?n :where [?e :name ?n] [or [?e :age 15] [?e :age 40]]] [db]
          '[or [?e :age 15] [?e :age 40]]]
         ["rule call written as a vector"
          '[:find ?n :in $ % :where [?e :name ?n] [adult ?e]]
          [db '[[(adult ?e) [?e :age ?a] [(>= ?a 18)]]]] '[adult ?e]]
         ["rule head written as a vector"
          '[:find ?n :in $ % :where [?e :name ?n] (adult ?e)] [db vector-rules]
          '[adult ?e]]
         ["call in a rule body"
          '[:find ?n :in $ % :where [?e :name ?n] (adult ?e)] [db rules-with-vector-call]
          '[>= ?a 18]]
         ["map query"
          '{:find [?c .] :in [$ ?v] :where [[[count ?v] ?c]]} [db [1 2]] '[count ?v]]
         ["string query"
          "[:find ?c . :in $ ?v :where [[count ?v] ?c]]" [db [1 2]] '[count ?v]]]]
    (doseq [planner? [true false]
            [label query args form] cases]
      (testing (str (if planner? "planner: " "base engine: ") label)
        (let [err (vector-call-error #(apply run planner? query args))]
          (is (map? err) (pr-str err))
          (when (map? err)
            (is (= form (:form err)))
            (is (re-find #"must be a list, not a vector" (:message err)))))))))

;; The normalized-query memo is keyed by the query form, and the two spellings
;; are `=` with one hash, so the check runs before the memo. Each case uses a
;; form no other test uses, so its first run here is its first in the process.
(deftest the-spelling-is-checked-whatever-ran-first
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
            (is (map? (vector-call-error #(run planner? vec-q db [1 2 3]))))))
        (testing "vector form first, then list form"
          (let [vec-q  (if planner?
                         '[:find ?n3 :where [?e3 :name ?n3] [[some? ?n3]]]
                         '[:find ?n4 :where [?e4 :name ?n4] [[some? ?n4]]])
                list-q (if planner?
                         '[:find ?n3 :where [?e3 :name ?n3] [(some? ?n3)]]
                         '[:find ?n4 :where [?e4 :name ?n4] [(some? ?n4)]])]
            (is (= list-q vec-q) "the two spellings are one memo key")
            (is (map? (vector-call-error #(run planner? vec-q db))))
            (is (= #{["Ivan"] ["Oleg"] ["Petr"]} (run planner? list-q db)))))))))

(deftest legitimate-vectors-are-not-mistaken-for-calls
  (let [db (fresh-db)
        ivan (:db/id (d/entity db [:name "Ivan"]))
        rules '[[(adult ?e) [?e :age ?a] [(>= ?a 18)]]
                [(friendly ?a ?b) [?a :friend ?b]]
                [(same ?a ?a) [?a :name _]]]
        cases
        [["data patterns with _ and constants"
          '[:find ?n :where [_ :name ?n] [?e :name ?n] [?e :age 15]] [db] #{["Ivan"]}]
         ["source-prefixed pattern"
          '[:find ?n :in $ :where [$ ?e :name ?n] [$ ?e :age 37]] [db] #{["Oleg"]}]
         ["lookup ref as entity"
          '[:find ?a . :where [[:name "Ivan"] :age ?a]] [db] 15]
         ["entity id constant"
          [:find '?n '. :where [ivan :name '?n]] [db] "Ivan"]
         ["vector argument and collection binding"
          '[:find ?x :where [(ground [1 2]) [?x ...]]] [db] #{[1] [2]}]
         ["tuple binding"
          '[:find ?x ?y :where [?e :name "Ivan"] [?e :age ?a] [(vector ?e ?a) [?x ?y]]]
          [db] #{[ivan 15]}]
         ["relation binding"
          '[:find ?a ?b :where [(ground [[1 2] [3 4]]) [[?a ?b]]]] [db] #{[1 2] [3 4]}]
         ["coll and tuple inputs"
          '[:find ?n :in $ [?n ...] [?lo ?hi] :where [?e :name ?n] [?e :age ?a]
            [(<= ?lo ?a)] [(<= ?a ?hi)]]
          [db ["Ivan" "Oleg"] [10 20]] #{["Ivan"]}]
         ["pull in find"
          '[:find (pull ?e [:name]) . :where [?e :age 15]] [db] {:name "Ivan"}]
         ["get-else"
          '[:find ?n ?x :where [?e :name ?n] [(get-else $ ?e :nick "none") ?x] [?e :age 15]]
          [db] #{["Ivan" "none"]}]
         ["not-join and or-join"
          '[:find ?n :where [?e :name ?n]
            (not-join [?e] [?e :age ?a] [(odd? ?a)])
            (or-join [?e] [?e :age 40] [?e :age 15])]
          [db] #{["Petr"]}]
         ["rules: list heads, a repeated head var, a blank argument"
          '[:find ?n :in $ % :where [?e :name ?n] (adult ?e) (friendly ?e _) (same ?e ?e)]
          [db rules] #{["Oleg"]}]
         ["options after :where"
          '[:find ?n :where [?e :name ?n] [?e :age 40] :limit 1] [db] #{["Petr"]}]]]
    (doseq [planner? [true false]
            [label query args expected] cases]
      (testing (str (if planner? "planner: " "base engine: ") label)
        (is (= expected (apply run planner? query args)))))))
