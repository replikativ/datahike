(ns datahike.test.query-scoped-input-test
  "A scalar :in var inside an `or-join` / `not-join` that does not declare it.

   The declared var list is the ONLY thing such a clause unifies with the query around
   it. A scalar input used to ignore that — its value was written into the clause text —
   so the same query meant one thing here and another in Datomic (checked against the
   peer library: 20 and 0 results for the two queries below, where the folded value gave
   5 and 10). The shape is refused, on every engine and in every input mode, and the
   declared form gives one answer everywhere."
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer [is deftest testing]])
   [datahike.api :as d]
   [datahike.query :as dq]))

(defn- fresh-db []
  (let [cfg {:store {:backend :memory :id (random-uuid)} :schema-flexibility :write :keep-history? false}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn [{:db/ident :item/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                        {:db/ident :item/group :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                        {:db/ident :item/kind :db/valueType :db.type/string :db/cardinality :db.cardinality/one}])
      (d/transact conn (vec (for [i (range 60)]
                              {:item/name (str "n" (mod i 10)) :item/group (str "g" (mod i 4)) :item/kind (str "k" (mod i 3))})))
      [cfg conn])))

(def ^:private engines
  {:planner-fold     #(binding [dq/*disable-planner* false dq/*fold-scalar-ins* true] (%))
   :planner-relation #(binding [dq/*disable-planner* false dq/*fold-scalar-ins* false] (%))
   :relational       #(binding [dq/*disable-planner* true] (%))})

(defn- outcome [run thunk]
  (try (count (run thunk))
       (catch #?(:clj Exception :cljs :default) e
         (if (re-find #"bound by :in but not declared" (str (ex-message e))) ::refused (throw e)))))

(deftest an-undeclared-scalar-input-in-a-join-body-is-refused
  (let [[cfg conn] (fresh-db) db @conn]
    (try
      (doseq [[label q args]
              [[:or-join '[:find ?e :in $ ?x :where [?e :item/kind "k1"]
                           (or-join [?e] [?e :item/name ?x] [?e :item/group ?x])] ["g1"]]
               [:not-join '[:find ?e :in $ ?x :where [?e :item/group "g1"]
                            (not-join [?e] [?e :item/kind ?x])] ["k1"]]
               [:in-a-predicate '[:find ?e :in $ ?x :where [?e :item/group "g1"]
                                  (not-join [?e] [?e :item/kind ?k] [(= ?k ?x)])] ["k1"]]
               [:source-prefixed '[:find ?e :in $ ?x :where [?e :item/group "g1"]
                                   ($ not-join [?e] [?e :item/kind ?x])] ["k1"]]
               [:declared-outside-only '[:find ?e :in $ ?x :where [?e :item/group "g1"]
                                         (or-join [?e ?x] [?e :item/kind ?x]
                                                  (not-join [?e] [?e :item/name ?x]))] ["k1"]]]
              [engine run] engines]
        (is (= ::refused (outcome run #(apply d/q q db args))) (str label " on " engine)))
      (testing "the message names the variable and the fix"
        (is (thrown-with-msg? #?(:clj Exception :cljs js/Error)
                              #"\?x is bound by :in but not declared in \(not-join \[\?e\] …\).*\(not-join \[\?e \?x\] …\)"
                              (d/q '[:find ?e :in $ ?x :where [?e :item/group "g1"] (not-join [?e] [?e :item/kind ?x])] db "k1"))))
      (finally (d/release conn) (d/delete-database cfg)))))

(deftest what-is-declared-or-not-an-input-is-unaffected
  (let [[cfg conn] (fresh-db) db @conn]
    (try
      (doseq [[label q args expected]
              [[:or-join-declared '[:find ?e :in $ ?x :where [?e :item/kind "k1"]
                                    (or-join [?e ?x] [?e :item/name ?x] [?e :item/group ?x])] ["g1"] 5]
               [:not-join-declared '[:find ?e :in $ ?x :where [?e :item/group "g1"]
                                     (not-join [?e ?x] [?e :item/kind ?x])] ["k1"] 10]
               [:plain-or '[:find ?e :in $ ?x :where [?e :item/kind "k1"]
                            (or [?e :item/name ?x] [?e :item/group ?x])] ["g1"] 5]
               [:plain-not '[:find ?e :in $ ?x :where [?e :item/group "g1"] (not [?e :item/kind ?x])] ["k1"] 10]
               ;; a var bound by a pattern, and a collection input, were always local to the body
               [:pattern-var '[:find ?e :where [?e :item/kind "k1"] [?e :item/group ?g]
                               (or-join [?e] [?e :item/name ?g] [?e :item/kind ?g])] [] 20]
               [:collection-input '[:find ?e :in $ [?x ...] :where [?e :item/kind "k1"]
                                    (or-join [?e] [?e :item/name ?x] [?e :item/group ?x])] [["g1" "g2"]] 20]
               ;; a declared var that only ONE branch uses: the other branch holds for every
               ;; value the query binds it to (the relational engine used to refuse this)
               [:one-branch-scalar '[:find ?e :in $ ?x :where [?e :item/kind "k1"]
                                     (or-join [?e ?x] [?e :item/name ?x] [?e :item/group "g1"])] ["n1"] 6]
               [:one-branch-collection '[:find ?e ?x :in $ [?x ...] :where [?e :item/kind "k1"]
                                         (or-join [?e ?x] [?e :item/name ?x] [?e :item/group "g1"])] [["n1" "n4"]] 13]
               [:one-branch-pattern-var '[:find ?e ?g :where [?e :item/kind "k1"] [?e :item/group ?g]
                                          (or-join [?e ?g] [?e :item/name ?g] [?e :item/name "n1"])] [] 2]
               ;; a function is called, not unified: it stays visible in every body
               [:function-input '[:find ?e :in $ ?f :where [?e :item/group "g1"]
                                  (not-join [?e] [?e :item/kind ?k] [(?f ?k)])] [(fn [k] (= k "k1"))] 10]]
              [engine run] engines]
        (is (= expected (outcome run #(apply d/q q db args))) (str label " on " engine)))
      (finally (d/release conn) (d/delete-database cfg)))))
