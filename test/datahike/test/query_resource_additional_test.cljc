(ns datahike.test.query-resource-additional-test
  (:require
   #?(:clj [clojure.test :refer [deftest is testing]]
      :cljs [cljs.test :refer-macros [deftest is testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]
   [datahike.query.resource :as resource]))

(defn- failure-data [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs :default) e
         (ex-data e))))

(deftest temporal-joins-preserve-snapshots-and-enforce-the-guard
  (let [before (d/db-with (db/empty-db {:value {:db/index true}}
                                       {:keep-history? true})
                          (for [e (range 1 6)] {:db/id e :value 1}))
        after (d/db-with before [[:db/add 1 :value 2]])
        old-pairs (set (for [a (range 1 6) b (range 1 6)] [a b 1]))
        current-pairs (conj (set (for [a (range 2 6) b (range 2 6)] [a b 1]))
                            [1 1 2])
        query '[:find ?a ?b ?v :where [?a :value ?v] [?b :value ?v]]]
    (doseq [legacy? [false true]
            [source expected] [[after current-pairs]
                               [(d/as-of after (:max-tx before)) old-pairs]
                               [(d/history after) (conj old-pairs [1 1 2])]]]
      (binding [q/*disable-planner* legacy?]
        (is (= expected (d/q query source)))
        (is (= expected (binding [resource/*max-intermediate-slots* 1000]
                          (d/q query source))))
        (is (= :query/intermediate-limit
               (:error (binding [resource/*max-intermediate-slots* 40]
                         (failure-data #(d/q query source))))))))))

(deftest indexed-recursive-join-expansion-is-guarded
  (let [parents (range 1 7)
        hub 20
        leaves (range 31 37)
        edges (concat (for [p parents] [:db/add p :edge hub])
                      (for [c leaves] [:db/add hub :edge c]))
        source (d/db-with (db/empty-db {:edge {:db/valueType :db.type/ref
                                               :db/cardinality :db.cardinality/many
                                               :db/index true}})
                          edges)
        rules '[[(reach ?a ?b) [?a :edge ?b]]
                [(reach ?a ?b) [?a :edge ?m] (reach ?m ?b)]]
        query '[:find ?a ?b :in $ % :where (reach ?a ?b)]
        expected (set (concat (for [p parents] [p hub])
                              (for [c leaves] [hub c])
                              (for [p parents c leaves] [p c])))]
    ;; Twelve initial edges fit the delta-driven shortcut's small-delta shape;
    ;; their next join expands into thirty-six parent/leaf pairs.
    (doseq [legacy? [false true]]
      (binding [q/*disable-planner* legacy?]
        (is (= expected (d/q query source rules)))
        (is (= expected (binding [resource/*max-intermediate-slots* 2000]
                          (d/q query source rules))))
        (is (= :query/intermediate-limit
               (:error (binding [resource/*max-intermediate-slots* 80]
                         (failure-data #(d/q query source rules))))))))))

(deftest guarded-fallback-keeps-optional-anti-and-pushed-predicates
  (let [source (d/db-with (db/empty-db {:tag {:db/cardinality :db.cardinality/many}}
                                       {:keep-history? true})
                          [{:db/id 1 :tag [1 2 4] :nick "one"}
                           {:db/id 2 :tag [1 3]}
                           {:db/id 3 :tag [1 2] :blocked true}])
        query '[:find ?e ?tag ?nick
                :where [?e :tag ?tag]
                [(get-else $ ?e :nick "none") ?nick]
                (not [?e :blocked true])
                [(< ?tag 3)]]
        expected #{[1 1 "one"] [1 2 "one"] [2 1 "none"]}]
    (doseq [legacy? [false true]
            source [source (d/history source)]]
      (binding [q/*disable-planner* legacy?]
        (is (= expected (d/q query source)))
        (is (= expected (binding [resource/*max-intermediate-slots* 1000]
                          (d/q query source))))))))

(deftest guarded-multi-source-joins-use-each-sources-values
  (let [left (d/db-with (db/empty-db {:bucket {:db/index true}})
                        (for [e (range 1 7)] {:db/id e :bucket :shared}))
        right (d/db-with (db/empty-db {:bucket {:db/index true}})
                         (concat (for [e (range 20 26)] {:db/id e :bucket :shared})
                                 [{:db/id 99 :bucket :other}]))
        query '[:find ?a ?b :in $left $right
                :where [$left ?a :bucket ?v] [$right ?b :bucket ?v]]
        expected (set (for [a (range 1 7) b (range 20 26)] [a b]))]
    (doseq [legacy? [false true]]
      (binding [q/*disable-planner* legacy?]
        (is (= expected (d/q query left right)))
        (is (= expected (binding [resource/*max-intermediate-slots* 2000]
                          (d/q query left right))))
        (is (= :query/intermediate-limit
               (:error (binding [resource/*max-intermediate-slots* 60]
                         (failure-data #(d/q query left right))))))))))

#?(:clj
   (defn ^{:datahike/cost 1000} expensive-identity [x] x))

#?(:clj
   (deftest cost-probes-do-not-materialize-unguarded-fused-products
     (let [source (d/db-with
                   (db/empty-db {:left {:db/cardinality :db.cardinality/many}
                                 :right {:db/cardinality :db.cardinality/many}})
                   [{:db/id 1 :left [1 2 3] :right [4 5 6]}])
           query '[:find ?l ?r ?copy
                   :in $ [?e ...]
                   :where [?e :left ?l] [?e :right ?r]
                   [(datahike.test.query-resource-additional-test/expensive-identity ?e) ?copy]]
           expected (set (for [l [1 2 3] r [4 5 6]] [l r 1]))
           probe-var (ns-resolve 'datahike.query.execute 'probe-card)
           fused-var (ns-resolve 'datahike.query.execute 'execute-fused-scan-rel)
           original-probe @probe-var
           probes (atom 0)]
       (binding [q/*disable-planner* false q/*query-result-cache?* false]
         (is (= expected (d/q query source [1])))
         ;; Any fused group evaluation, including the pre-dispatch cost probe,
         ;; is forbidden while the relation fallback owns the size checks.
         (with-redefs-fn
           {probe-var (fn [& args]
                        (swap! probes inc)
                        (apply original-probe args))
            fused-var (fn [& _]
                        (throw (ex-info "Unprotected fused cost probe" {})))}
           (fn []
             (is (= expected (binding [resource/*max-intermediate-slots* 1000]
                               (d/q query source [1]))))
             (is (pos? @probes) "the physical cost-probe seam was reached")
             (is (= :query/intermediate-limit
                    (:error (binding [resource/*max-intermediate-slots* 20]
                              (failure-data #(d/q query source [1]))))))))))))
