(ns datahike.test.prepared-execution-test
  "Differential contract for the prepared-execution machinery
   (execute/*prepared-execution*): for every shape, every engine
   configuration must agree —

     legacy        *disable-planner* true
     planner       stock defaults (prepared OFF)
     prepared      *prepared-execution* true, *fold-scalar-ins* false
     prepared+rc   prepared with the result cache on

   The shapes cover the seams the machinery touches: scalar/collection/
   tuple :in bindings, point lookups on unique attrs, get-else defaults,
   predicates over :in vars, params echoed into :find, non-unique value
   probes, not/or bodies, joins, and lookup-ref bindings (which must
   route to the relation path and project the ORIGINAL binding)."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.api :as d]
   [datahike.query :as dq]
   [datahike.query.execute :as execute]))

(def ^:private cfg
  {:store {:backend :memory :id #?(:clj (random-uuid) :cljs (random-uuid))}
   :schema-flexibility :write
   :keep-history? false})

(defn- test-db []
  (d/delete-database cfg)
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn [{:db/ident :t/id :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:db/ident :t/rx :db/valueType :db.type/boolean
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :t/v :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one}
                      {:db/ident :t/name :db/valueType :db.type/string
                       :db/cardinality :db.cardinality/one}])
    (d/transact conn {:tx-data (mapv (fn [i] {:t/id i :t/rx true :t/v (* 2 i)
                                              :t/name (str "n" i)})
                                     (range 500))})
    (d/db conn)))

(def ^:private shapes
  ;; [query & argsets]
  [['{:find [?v] :in [$ ?id]
      :where [[?e :t/id ?id] [?e :t/rx true]
              [(get-else $ ?e :t/v -1) ?v]]}
    [42] [499] [-7]]
   ['{:find [?e] :in [$ ?id] :where [[?e :t/id ?id] [?e :t/rx true]]}
    [7] [1000]]
   ['{:find [?e ?v] :in [$ ?id] :where [[?e :t/id ?id] [?e :t/v ?v]]}
    [55] [0]]
   ['{:find [?v] :in [$ ?id]
      :where [[?e :t/id ?id] [(some? ?id)] [?e :t/v ?v]]}
    [10]]
   ['{:find [?id ?v] :in [$ ?id]
      :where [[?e :t/id ?id] [?e :t/v ?v]]}
    [11]]
   ['{:find [?v] :in [$ ?id ?rx]
      :where [[?e :t/id ?id] [?e :t/rx ?rx] [?e :t/v ?v]]}
    [12 true] [12 false]]
   ['{:find [?e] :in [$ ?vv] :where [[?e :t/v ?vv]]}
    [84] [83]]
   ['{:find [?v] :in [$ [?id ...]]
      :where [[?e :t/id ?id] [?e :t/v ?v]]}
    [[1 2 3]] [[]]]
   ['{:find [?v] :in [$ [?id ?rx]]
      :where [[?e :t/id ?id] [?e :t/rx ?rx] [?e :t/v ?v]]}
    [[5 true]]]
   ['{:find [?e] :in [$ ?id]
      :where [[?e :t/id ?id] (not [?e :t/v -1])]}
    [20]]
   ['{:find [?e] :in [$ ?id]
      :where [[?e :t/id ?id] (or [?e :t/rx true] [?e :t/rx false])]}
    [21]]
   ['{:find [?v] :in [$ ?id]
      :where [[?e :t/id ?id] [?e :t/v ?v] [(> ?v 10)]]}
    [4] [100]]
   ['{:find [?w] :in [$ ?id]
      :where [[?e :t/id ?id] [(get-else $ ?e :t/missing :none) ?w]]}
    [40]]
   ;; lookup-ref binding: the projected value must be the ORIGINAL ref
   ['{:find [?e ?n] :in [$ ?e] :where [[?e :t/name ?n]]}
    [[:t/id 3]]]])

(deftest engines-agree-on-every-shape
  (let [db (test-db)]
    (doseq [[q & argsets] shapes
            args argsets]
      (let [legacy (binding [dq/*disable-planner* true]
                     (apply d/q q db args))
            planner (binding [dq/*disable-planner* false
                              dq/*query-result-cache?* false]
                      (apply d/q q db args))
            prepared (binding [dq/*disable-planner* false
                               execute/*prepared-execution* true
                               dq/*fold-scalar-ins* false
                               dq/*query-result-cache?* false]
                       (apply d/q q db args))
            prepared-rc (binding [dq/*disable-planner* false
                                  execute/*prepared-execution* true
                                  dq/*fold-scalar-ins* false]
                          (apply d/q q db args))]
        (is (= legacy planner prepared prepared-rc)
            (str "engines disagree on " (pr-str q) " args " (pr-str args)
                 "\n legacy=" (pr-str legacy)
                 "\n planner=" (pr-str planner)
                 "\n prepared=" (pr-str prepared)
                 "\n prepared+rc=" (pr-str prepared-rc)))))))

(deftest prepared-repeats-one-plan-across-values
  (testing "the same shape with many distinct argument values stays correct
            (the property the value-free plan cache exists for)"
    (let [db (test-db)
          q '{:find [?v] :in [$ ?id]
              :where [[?e :t/id ?id] [(get-else $ ?e :t/v -1) ?v]]}]
      (binding [dq/*disable-planner* false
                execute/*prepared-execution* true
                dq/*fold-scalar-ins* false]
        (doseq [i (range 200)]
          (is (= #{[(* 2 i)]} (d/q q db i))))
        (is (= #{} (d/q q db 100000)))))))

(deftest flag-off-is-stock
  (testing "with the flag off (default), scalar rels disqualify the direct
            path exactly as before — behavior and results are stock"
    (let [db (test-db)
          q '{:find [?e] :in [$ ?id] :where [[?e :t/id ?id]]}]
      (is (false? execute/*prepared-execution*))
      (binding [dq/*fold-scalar-ins* false
                dq/*query-result-cache?* false]
        (is (= (binding [dq/*disable-planner* true] (d/q q db 3))
               (d/q q db 3)))))))

(deftest prepared-honors-cancellation
  (testing "a pre-set :cancel raises :datahike/canceled on the prepared
            point paths, matching the generic executors"
    (let [db (test-db)
          q '{:find [?v] :in [$ ?id]
              :where [[?e :t/id ?id] [(get-else $ ?e :t/v -1) ?v]]}]
      (binding [dq/*disable-planner* false
                execute/*prepared-execution* true
                dq/*fold-scalar-ins* false
                dq/*query-result-cache?* false]
        ;; warm: compile the point program
        (is (= #{[8]} (d/q q db 4)))
        (is (thrown-with-msg?
             #?(:clj clojure.lang.ExceptionInfo :cljs js/Error) #"(?i)cancel"
             (d/q {:query q :args [db 4] :cancel (volatile! true)})))))))
