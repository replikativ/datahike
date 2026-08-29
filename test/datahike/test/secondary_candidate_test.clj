(ns datahike.test.secondary-candidate-test
  "Focused tests for the optional paged secondary candidate contract."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datahike.db :as db]
   [datahike.index.secondary :as sec]))

(defn- candidate-index
  [calls result]
  (reify
    sec/ISecondaryIndex
    (-search [_ _ _] nil)
    (-estimate [_ _] 0)
    (-can-order? [_ _ _] false)
    (-slice-ordered [_ _ _ _ _ _] nil)
    (-indexed-attrs [_] #{:doc/body})
    (-transact [this _] this)

    sec/ISecondaryCandidateScan
    (-candidate-page [_ query-spec entity-filter page-request]
      (swap! calls conj [query-spec entity-filter page-request])
      result)))

(defn- legacy-index
  []
  (reify sec/ISecondaryIndex
    (-search [_ _ _] nil)
    (-estimate [_ _] 0)
    (-can-order? [_ _ _] false)
    (-slice-ordered [_ _ _ _ _ _] nil)
    (-indexed-attrs [_] #{:doc/body})
    (-transact [this _] this)))

(defn- error-data
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (ex-data e))))

(deftest candidate-contract-is-additive
  (testing "an existing ISecondaryIndex does not have to implement candidate scans"
    (let [idx (legacy-index)]
      (is (satisfies? sec/ISecondaryIndex idx))
      (is (false? (sec/candidate-scannable? idx)))
      (is (= :secondary-candidate-scan-unsupported
             (:type (error-data
                     #(sec/candidate-page
                       (db/empty-db {}) :idx/legacy idx {} nil {:limit 10}))))))))

(deftest candidate-scan-cancellation-is-optional-and-idempotent
  (let [closed (atom [])
        idx (reify
              sec/ISecondaryCandidateScanLifecycle
              (-close-candidate-scan [_ continuation]
                (swap! closed conj continuation)))]
    (is (nil? (sec/close-candidate-scan! idx :cursor)))
    (is (nil? (sec/close-candidate-scan! idx :cursor)))
    (is (= [:cursor :cursor] @closed)
        "the adapter owns idempotence because it owns the resource")
    (is (nil? (sec/close-candidate-scan! (legacy-index) :ignored)))
    (is (nil? (sec/close-candidate-scan! idx nil)))))

(deftest candidate-page-preserves-independent-correctness-axes
  (doseq [[precision recall ordering]
          [[:exact :complete :exact]
           [:recheck :complete :none]
           [:exact :approximate :approximate]
           [:recheck :approximate :exact]]]
    (testing (str precision "/" recall "/" ordering)
      (let [calls (atom [])
            page {:candidates (list {:entity-id 7
                                     :attribute :doc/body
                                     :value-hash "value-7"
                                     :score 0.75})
                  :precision precision
                  :recall recall
                  :ordering ordering
                  :exhausted? false
                  :continuation {:after 7}}
            idx (candidate-index calls page)
            result (sec/candidate-page
                    (db/empty-db {}) :idx/search idx {:query "needle"}
                    :entity-filter {:limit 25})]
        (is (= page (assoc result :candidates (seq (:candidates result)))))
        (is (vector? (:candidates result)) "candidate sequences are normalized")
        (is (= (= precision :recheck)
               (sec/candidate-recheck-required? result)))
        (is (= (= recall :complete)
               (sec/candidate-recall-complete? result)))
        (is (= (= ordering :exact)
               (sec/candidate-order-exact? result)))
        (is (= [[{:query "needle"} :entity-filter {:limit 25}]] @calls))))))

(deftest candidate-page-validates-adapter-output
  (let [valid {:candidates [{:entity-id 1 :attribute :doc/body}]
               :precision :exact
               :recall :complete
               :ordering :none
               :exhausted? true
               :continuation nil
               :stop-reason :source-exhausted}
        invalid-pages [(assoc valid :precision :lossy)
                       (assoc valid :recall :unknown)
                       (assoc valid :ordering :ranked)
                       (assoc valid :exhausted? :yes)
                       (assoc valid :continuation :after)
                       (assoc valid :exhausted? false :continuation nil)
                       (dissoc valid :stop-reason)
                       (assoc valid :stop-reason :made-up)
                       (assoc valid :stats {:visited -1})
                       (assoc valid :candidates [{:entity-id 1}])
                       (assoc valid :candidates [{:attribute :doc/body}])]]
    (is (= valid (sec/validate-candidate-page valid)))
    (doseq [page invalid-pages]
      (is (= :invalid-secondary-candidate-page
             (:type (error-data #(sec/validate-candidate-page page))))
          (pr-str page)))))

(deftest adapter-scan-conformance-helper
  (let [first-page {:candidates [{:entity-id 1 :attribute :doc/body}
                                 {:entity-id 2
                                  :attribute :doc/body
                                  :value-hash "two-a"}]
                    :precision :recheck
                    :recall :complete
                    :ordering :exact
                    :exhausted? false
                    :continuation {:after 2}}
        final-page {:candidates [{:entity-id 2
                                  :attribute :doc/body
                                  :value-hash "two-b"}
                                 {:entity-id 3 :attribute :doc/body}]
                    :precision :recheck
                    :recall :complete
                    :ordering :exact
                    :exhausted? true
                    :continuation nil
                    :stop-reason :source-exhausted}]
    (is (= [first-page final-page]
           (sec/validate-candidate-scan [first-page final-page])))

    (doseq [pages [[first-page (assoc final-page :recall :approximate)]
                   [first-page (assoc final-page
                                      :candidates
                                      [{:entity-id 1 :attribute :doc/body}])]
                   [(assoc first-page :continuation :same)
                    (assoc first-page :continuation :same)
                    final-page]
                   [first-page (assoc final-page
                                      :exhausted? false
                                      :continuation :more
                                      :stop-reason nil)]]]
      (is (= :invalid-secondary-candidate-scan
             (:type (error-data #(sec/validate-candidate-scan pages))))
          (pr-str pages)))))

(deftest candidate-page-validates-request-before-dispatch
  (let [calls (atom [])
        idx (candidate-index calls {:candidates []
                                    :precision :exact
                                    :recall :complete
                                    :ordering :none
                                    :exhausted? true
                                    :continuation nil
                                    :stop-reason :source-exhausted})]
    (doseq [request [nil {} {:limit 0} {:limit -1} {:limit 1.5}]]
      (is (= :invalid-secondary-candidate-request
             (:type (error-data
                     #(sec/candidate-page
                       (db/empty-db {}) :idx/search idx {} nil request))))))
    (is (empty? @calls))))

(deftest candidate-page-enforces-requested-bound
  (let [idx (candidate-index
             (atom [])
             {:candidates [{:entity-id 1 :attribute :doc/body}
                           {:entity-id 2 :attribute :doc/body}]
              :precision :exact
              :recall :complete
              :ordering :none
              :exhausted? true
              :continuation nil
              :stop-reason :source-exhausted})]
    (is (= :invalid-secondary-candidate-page
           (:type (error-data
                   #(sec/candidate-page
                     (db/empty-db {}) :idx/search idx {} nil {:limit 1})))))))

(deftest candidate-page-honours-query-readiness
  (let [calls (atom [])
        result {:candidates []
                :precision :recheck
                :recall :complete
                :ordering :none
                :exhausted? true
                :continuation nil
                :stop-reason :source-exhausted}
        idx (candidate-index calls result)
        base (db/empty-db {:idx/search {:db.secondary/status :ready}})]
    (doseq [status [:building :disabled :failed]]
      (let [unavailable (assoc-in base [:schema :idx/search :db.secondary/status]
                                  status)]
        (is (= {:type :secondary-index-unavailable
                :index-ident :idx/search
                :status status}
               (error-data
                #(sec/candidate-page
                  unavailable :idx/search idx {} nil {:limit 10}))))))
    (is (empty? @calls) "unavailable indexes are rejected before adapter dispatch")

    (is (= result
           (sec/candidate-page base :idx/search idx {} nil {:limit 10})))
    (is (= result
           (sec/candidate-page
            (update-in base [:schema :idx/search] dissoc :db.secondary/status)
            :idx/search idx {} nil {:limit 10})))
    (is (= 2 (count @calls)) ":ready and legacy nil states dispatch")))
