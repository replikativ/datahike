(ns datahike.test.query-lookup-resource-test
  (:require
   #?(:clj [clojure.test :refer [deftest is testing]]
      :cljs [cljs.test :refer-macros [deftest is testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   #?(:cljs [datahike.datom :as datom])
   [datahike.query :as q]
   [datahike.query.relation :as rel]
   [datahike.query.resource :as resource]))

(defn- failure [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs :default) e (ex-data e))))

(defn- input-binding [form]
  (first (:qin (q/memoized-parse-query {:find '[?x] :in [form] :where []}))))

(defn- rows [relation]
  (mapv vec (:tuples relation)))

(defn- search-context [values strategy]
  (let [r (rel/->Relation {'?v 0} (mapv vector values))]
    {:source (db/empty-db)
     :bsm (q/bound-symbol-map [r])
     :clean-pattern '[nil :n ?v nil nil]
     :strategy-vec strategy
     :rels [r]}))

(deftest collection-source-reserves-before-tuple-copy
  (let [copies (atom 0)
        original to-array
        source [[1 :a] [2 :b] [3 :c]]]
    (binding [resource/*max-intermediate-slots* 6]
      (with-redefs [#?(:clj clojure.core/to-array :cljs cljs.core/to-array)
                    (fn [tuple] (swap! copies inc) (original tuple))]
        (let [error (failure #(q/lookup-pattern-coll source '[?e ?v] '[?e ?v]))]
          (is (= :query/intermediate-limit (:error error)))
          (is (= :collection-scan-output (:operator error)))
          (is (= 6 (:retained error)))
          (is (= 2 @copies) "the third copied tuple is never constructed"))))))

(deftest collection-source-counts-physical-width-and-bounds-lazy-tuples
  (binding [resource/*max-intermediate-slots* 4]
    (is (= :query/intermediate-limit
           (:error (failure #(q/lookup-pattern-coll [[1 2 3 4 5]] '[?e] '[?e])))))
    (let [visited (atom 0)
          tuple (map (fn [i] (swap! visited inc) i) (iterate inc 1))]
      (is (= :query/intermediate-limit
             (:error (failure #(q/lookup-pattern-coll [tuple] '[?e] '[?e])))))
      (is (<= @visited 5) "width inspection does not traverse the infinite tail")))
  (binding [resource/*max-intermediate-slots* 3]
    (is (= [[2 :kept]]
           (rows (q/lookup-pattern-coll [[1 :drop] [2 :kept] [3 :drop]]
                                        '[?e :kept] '[?e ?v]))))))

(deftest datom-distinct-accounts-for-seen-keys-without-charging-duplicates
  (binding [resource/*max-intermediate-slots* 6]
    (let [tuple [1 :n 2 3 true]]
      (is (= [tuple] (into [] (q/relation-from-datoms-xform) (repeat 100 tuple))))
      (let [error (failure #(into [] (q/relation-from-datoms-xform)
                                  [tuple [2 :n 2 3 true]]))]
        (is (= :distinct-tuples-seen (:operator error)))
        (is (= 6 (:retained error))))))
  (binding [resource/*max-intermediate-slots* 5]
    (is (= :datom-tuple
           (:operator (failure #(into [] (q/relation-from-datoms-xform)
                                      [[1 :n 2 3 true]])))))))

(deftest distinct-transducer-reapplication-has-fresh-accounts
  (doseq [limit [nil 4]]
    (binding [resource/*max-intermediate-slots* limit]
      (let [xf (q/distinct-tuples)]
        (dotimes [_ 3]
          (is (= [[1] [2]] (into [] xf [[1] [1] [2]]))))))))

(deftest reusable-tuple-transducers-read-policy-at-application
  (doseq [created-limit [nil 1]]
    (let [xf (binding [resource/*max-intermediate-slots* created-limit]
               (q/distinct-tuples))]
      (binding [resource/*max-intermediate-slots* 2]
        (dotimes [_ 2]
          (let [error (failure #(into [] xf [[1] [2]]))]
            (is (= :distinct-tuples-seen (:operator error)))
            (is (= 2 (:retained error))))))
      (binding [resource/*max-intermediate-slots* nil]
        (dotimes [_ 2]
          (is (= [[1] [2]] (into [] xf [[1] [1] [2]])))))))
  (doseq [created-limit [nil 1]]
    (let [xf (binding [resource/*max-intermediate-slots* created-limit]
               (q/relation-from-datoms-xform))
          tuples [[1 :n 1 1 true] [2 :n 2 1 true]]]
      (binding [resource/*max-intermediate-slots* 5]
        (is (= :datom-tuple
               (:operator (failure #(into [] xf tuples))))))
      (binding [resource/*max-intermediate-slots* 6]
        (dotimes [_ 2]
          (let [error (failure #(into [] xf tuples))]
            (is (= :distinct-tuples-seen (:operator error)))
            (is (= 6 (:retained error))))))
      (binding [resource/*max-intermediate-slots* nil]
        (dotimes [_ 2]
          (is (= tuples (mapv vec (into [] xf tuples)))))))))

#?(:clj
   (deftest reusable-retention-transducer-has-an-identity-disabled-path
     (doseq [created-limit [nil 1]]
       (let [xf (binding [resource/*max-intermediate-slots* created-limit]
                  (#'q/retaining-tuples-xform :retention-reuse))
             rf (fn [result tuple] (conj result tuple))]
         (binding [resource/*max-intermediate-slots* nil]
           (is (identical? rf (xf rf)))
           (is (= [[1] [2]] (into [] xf [[1] [2]]))))
         (binding [resource/*max-intermediate-slots* 2]
           (dotimes [_ 2]
             (let [error (failure #(into [] xf [[1] [2]]))]
               (is (= :retention-reuse (:operator error)))
               (is (= 2 (:retained error))))))))))

#?(:cljs
   (deftest relation-output-admission-precedes-datom-conversion
     (let [source (mapv #(datom/datom % :n % 1 true) [1 2 3])
           converted (atom 0)
           original keyword]
       (binding [resource/*max-intermediate-slots* 12]
         ;; Attribute conversion is the first allocation in converting a Datom
         ;; to the retained JS tuple. The forbidden third row must never reach it.
         (with-redefs [cljs.core/keyword
                       (fn
                         ([value]
                          (when (= "n" value) (swap! converted inc))
                          (original value))
                         ([ns name] (original ns name)))]
           (let [error (failure #(q/relation-from-datoms {:consts {}} '[?e :n ?v] source))]
             (is (= :relation-output (:operator error)))
             (is (= 12 (:retained error)))
             (is (= 2 @converted))))))
     (binding [resource/*max-intermediate-slots* 4]
       (is (= :relation-tuple
              (:operator (failure #(q/relation-from-datoms
                                    {:consts {}} '[?e] [#js [1 2 3 4]]))))))))

(deftest batch-output-is-bounded-independently-of-datom-transformation
  (let [context {:source (db/empty-db) :bsm {}
                 :clean-pattern [nil :n nil nil nil] :rels []}
        values [[1 :n 10 1 true] [2 :n 20 1 true] [3 :n 30 1 true]]]
    (binding [resource/*max-intermediate-slots* 12]
      (let [error (failure #((q/search-batch-fn context)
                             [nil nil nil nil]
                             (fn [& _] values)
                             identity))]
        (is (= :batch-scan-output (:operator error)))
        (is (= 12 (:retained error)))))))

(deftest substitution-pairs-and-filter-features-have-local-accounts
  (let [sub-context (search-context [1 2 3] [nil nil :substitute nil])
        filter-context (search-context [1 2 3] [nil nil :filter nil])]
    (binding [resource/*max-intermediate-slots* 8]
      (let [error (failure #(q/initialization-and-substitution-xform sub-context #{0}))]
        (is (= :substitution-pairs (:operator error)))
        (is (= 8 (:retained error)))
        (is (= 4 (:requested error)))))
    (binding [resource/*max-intermediate-slots* 4]
      (let [error (failure #(q/datom-filter-predicate #{0} filter-context))]
        (is (= :filter-features (:operator error)))
        (is (= 4 (:retained error))))))
  (binding [resource/*max-intermediate-slots* 2]
    (let [context (search-context (repeat 100 7) [nil nil :filter nil])
          predicate (q/datom-filter-predicate #{0} context)]
      (is (predicate [1 :n 7 1 true]))
      (is (not (predicate [1 :n 8 1 true]))))))

(deftest scalar-and-singleton-inputs-cannot-bypass-the-limit
  (let [input (input-binding '?x)]
    (binding [resource/*max-intermediate-slots* 1]
      (is (= :input-scalar (:operator (failure #(q/in->rel input :value)))))))
  (binding [resource/*max-intermediate-slots* 2]
    (is (= [[1]] (rows (q/in->rel (input-binding '[?x ...]) [1])))))
  (binding [resource/*max-intermediate-slots* 6]
    (is (= [[1] [1] [1]] (rows (q/in->rel (input-binding '[?x ...]) [1 1 1])))))
  (binding [resource/*max-intermediate-slots* 5]
    (is (= :input-collection
           (:operator (failure #(q/in->rel (input-binding '[?x ...]) [1 1 1])))))))

(deftest collection-input-reserves-before-scalar-tuple-construction
  (let [input (input-binding '[?x ...])
        original into-array
        copies (atom 0)]
    (binding [resource/*max-intermediate-slots* 4]
      (with-redefs [#?(:clj clojure.core/into-array :cljs cljs.core/into-array)
                    (fn
                      ([values]
                       (swap! copies inc)
                       ;; CLJS one-arity into-array delegates through the live
                       ;; two-arity Var. Call the saved implementation directly
                       ;; so one actual copy is observed exactly once.
                       #?(:clj (original values)
                          :cljs (original nil values)))
                      ([type values] (swap! copies inc) (original type values)))]
        (is (= :input-collection
               (:operator (failure #(q/in->rel input [1 2 3])))))
        (is (= 2 @copies) "the third retained row is rejected before copying")))))

(deftest tuple-input-only-inspects-required-prefix-and-preserves-nesting
  (binding [resource/*max-intermediate-slots* 6]
    (is (= [[1 2]] (rows (q/in->rel (input-binding '[?x ?y]) (iterate inc 1)))))
    (is (= [[1 10] [1 11]]
           (rows (q/in->rel (input-binding '[?x [?y ...]]) [1 [10 11]]))))
    (is (= [[1]] (rows (q/in->rel (input-binding '[?x _]) [1 :ignored :surplus])))))
  (binding [resource/*max-intermediate-slots* 5]
    (is (= :query/intermediate-limit
           (:error (failure #(q/in->rel (input-binding '[?x [?y ...]]) [1 [10 11]]))))))
  (binding [resource/*max-intermediate-slots* 100]
    (is (= :query/binding
           (:error (failure #(q/in->rel (input-binding '[?x ?y]) [1])))))))

(deftest lazy-collection-input-stops-before-consuming-its-tail
  (let [visited (atom 0)
        values (map (fn [i] (swap! visited inc) i) (iterate inc 1))]
    (binding [resource/*max-intermediate-slots* 6]
      (is (= :input-collection
             (:operator (failure #(q/in->rel (input-binding '[?x ...]) values)))))
      (is (<= @visited 4)))))

(deftest lookup-reference-error-catches-preserve-resource-errors
  (let [replace (q/vec-lookup-ref-replacer
                 (fn [& _] (throw (ex-info "limit" {:error :query/intermediate-limit}))) [0])]
    (is (= :query/intermediate-limit (:error (failure #(replace [1]))))))
  (let [replace (q/vec-lookup-ref-replacer
                 (fn [& _] (throw (ex-info "invalid lookup" {}))) [0])]
    (is (nil? (replace [1])))))

(deftest reference-query-results-remain-correct-with-generous-limits
  (let [source (d/db-with (db/empty-db {:tag {:db/cardinality :db.cardinality/many}})
                          [{:db/id 1 :n 10 :tag [:a :b]}
                           {:db/id 2 :n 20 :tag [:b]}
                           {:db/id 3 :n 30 :tag [:c]}])]
    (doseq [[query args] [['[:find ?e ?n :where [?e :n ?n]] [source]]
                          ['[:find ?e ?tag :in $ [?e ...] :where [?e :tag ?tag]] [source [1 2]]]
                          ['[:find ?e ?n :in $ ?n :where [?e :n ?n]] [source 20]]]]
      (binding [q/*disable-planner* true q/*query-result-cache?* false]
        (is (= (apply d/q query args)
               (binding [resource/*max-intermediate-slots* 1000]
                 (apply d/q query args))))))))
