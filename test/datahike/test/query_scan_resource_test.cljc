(ns datahike.test.query-scan-resource-test
  (:require
   #?(:clj [clojure.test :refer [deftest is testing]]
      :cljs [cljs.test :refer-macros [deftest is testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.datom :as datom]
   [datahike.index.interface :as di]
   [datahike.query :as q]
   [datahike.query.cancel :as cancel]
   [datahike.query.execute :as execute]
   [datahike.query.relation :as rel]
   [datahike.query.resource :as resource]))

(defn- failure-data [f]
  (try (f) nil
       (catch #?(:clj Exception :cljs :default) e (ex-data e))))

(defn- fixture []
  (d/db-with (db/empty-db {:value {:db/index true}})
             (for [i (range 1 101)] {:db/id i :value i})))

(deftest capped-scans-keep-matching-values-and-fresh-executions
  (let [source (fixture)
        query '[:find ?e ?v :where [?e :value ?v]]
        expected (set (for [i (range 1 101)] [i i]))]
    (is (= expected (d/q query source)))
    (dotimes [_ 2]
      (is (= expected (binding [resource/*max-intermediate-slots* 1000]
                        (d/q query source)))))
    (is (= :query/intermediate-limit
           (:error (binding [resource/*max-intermediate-slots* 30]
                     (failure-data #(d/q query source))))))
    (is (= expected (d/q query source)))))

(defn- executor-var [sym]
  #?(:clj (ns-resolve 'datahike.query.execute sym)
     ;; CLJS Var quotation deliberately permits private implementation seams;
     ;; it keeps these tests portable without exposing a production test API.
     :cljs (case sym
             make-probe-set #'execute/make-probe-set
             probe-set-add #'execute/probe-set-add
             probe-set-contains? #'execute/probe-set-contains?
             probe-set-size #'execute/probe-set-size
             probe-set-seek-vals #'execute/probe-set-seek-vals
             make-probe-map #'execute/make-probe-map
             probe-map-add #'execute/probe-map-add
             probe-map-get #'execute/probe-map-get
             probe-map-entry-size #'execute/probe-map-entry-size)))

(defn- invoke [sym & args]
  (apply @(executor-var sym) args))

(deftest probe-admission-preserves-portable-value-containers
  (binding [resource/*max-intermediate-slots* 4]
    (let [probe (invoke 'make-probe-set 1000000)]
      (doseq [value [(keyword "one") (keyword "two") (keyword "one")]]
        (invoke 'probe-set-add probe value))
      (is (= 2 (invoke 'probe-set-size probe)))
      (is (= [:one :two] (vec (invoke 'probe-set-seek-vals probe))))
      (is (= :query/intermediate-limit
             (:error (failure-data #(invoke 'probe-set-add probe :third)))))
      (is (= 2 (invoke 'probe-set-size probe)))
      (is (= [:one :two] (vec (invoke 'probe-set-seek-vals probe)))
          "rejection cannot append a raw seek value either")
      (is (false? (invoke 'probe-set-contains? probe :third)))))
  (binding [resource/*max-intermediate-slots* 60]
    (let [index (invoke 'make-probe-map 1000000)]
      ;; Passing eight distinct keys promotes CLJS's transient array map.
      ;; Keep its replacement root, all buckets and one shared account.
      (doseq [round (range 2) i (range 12)]
        (invoke 'probe-map-add index (keyword (str "key-" i)) [round i]))
      (doseq [i (range 12)]
        (is (= 2 (invoke 'probe-map-entry-size
                         (invoke 'probe-map-get index (keyword (str "key-" i)))))))
      (let [error (failure-data #(invoke 'probe-map-add index :thirteenth [:new]))]
        (is (= :query/intermediate-limit (:error error)))
        (is (= 60 (:retained error)))
        (is (= 4 (:requested error))))
      (is (nil? (invoke 'probe-map-get index :thirteenth))))))

(deftest array-probes-retain-raw-values-with-content-membership
  (doseq [make-array [#?(:clj #(byte-array [1 2]) :cljs #(js/Int8Array. #js [1 2]))
                      #?(:clj #(float-array [1 2]) :cljs #(js/Float32Array. #js [1 2]))
                      #?(:clj #(double-array [1 2]) :cljs #(js/Float64Array. #js [1 2]))]]
    (let [raw (make-array)]
      (binding [resource/*max-intermediate-slots* 2]
        (let [probe (invoke 'make-probe-set 1000000)]
          (invoke 'probe-set-add probe raw)
          (invoke 'probe-set-add probe (make-array))
          (is (= 1 (invoke 'probe-set-size probe)))
          (is (invoke 'probe-set-contains? probe (make-array)))
          (is (identical? raw (first (invoke 'probe-set-seek-vals probe)))))))))

#?(:clj
   (deftest standalone-scan-admits-physical-tuples-before-construction
     (let [source (fixture)
           scan-op {:clause '[?e :value ?v] :index :aevt :estimated-card 100}
           copies (atom 0)
           tuple-var (executor-var 'scan-datom-tuple)
           tuple-fn @tuple-var]
       (with-redefs-fn {tuple-var (fn [d] (swap! copies inc) (tuple-fn d))}
         (fn []
           (binding [resource/*max-intermediate-slots* 12]
             (let [error (failure-data #(invoke 'execute-pattern-scan source scan-op nil []))]
               (is (= :query/intermediate-limit (:error error)))
               (is (= :pattern-scan-output (:operator error)))
               (is (= 12 (:retained error)))
               (is (= 6 (:requested error)))
             ;; The rejected third tuple must never reach its constructor.
               (is (= 2 @copies))))))
       (doseq [_ (range 2)]
         (let [small (d/db-with (db/empty-db) [{:db/id 1 :value 1}
                                               {:db/id 2 :value 2}])]
           (is (= 2 (count (:tuples (binding [resource/*max-intermediate-slots* 12]
                                      (invoke 'execute-pattern-scan small scan-op nil [])))))))))))

#?(:clj
   (deftest filtered-scan-charges-retention-instead-of-visits
     (let [source (fixture)
           datoms (d/datoms source {:index :aevt :components [:value]})
           visited (atom 0)
           op {:clause '[999 :value ?v] :index :aevt :estimated-card 100}]
       (with-redefs-fn {(executor-var 'scan-datoms)
                        (fn [& _] (map #(do (swap! visited inc) %) datoms))}
         (fn []
           (is (empty? (:tuples (binding [resource/*max-intermediate-slots* 1]
                                  (invoke 'execute-pattern-scan source op nil [])))))
           (is (= 100 @visited))))
       (binding [resource/*max-intermediate-slots* 5]
         (is (= {:datahike/canceled true}
                (failure-data #(invoke 'execute-pattern-scan source
                                       (assoc op :clause '[?e :value ?v])
                                       (volatile! true) []))))))))

#?(:clj
   (deftest probe-sets-admit-both-representations-atomically
     (let [mutations (atom [])
           members (proxy [java.util.HashSet] []
                     (add [x] (swap! mutations conj :member) (proxy-super add x)))
           values (proxy [java.util.ArrayList] []
                    (add [x] (swap! mutations conj :raw) (proxy-super add x)))
           account (binding [resource/*max-intermediate-slots* 4]
                     (resource/account :probe-set))
           probe (datahike.query.execute.ProbeSet. members values account)]
       (doseq [value [:first :second :first :second]]
         (invoke 'probe-set-add probe value))
       (is (= [:member :raw :member :raw] @mutations))
       (is (= 2 (invoke 'probe-set-size probe)))
       (let [error (failure-data #(invoke 'probe-set-add probe :third))]
         (is (= :query/intermediate-limit (:error error)))
         (is (= 4 (:retained error)))
         (is (= 2 (:requested error))))
       (is (= [:member :raw :member :raw] @mutations))
       (is (= [:first :second] (vec (invoke 'probe-set-seek-vals probe)))))))

#?(:clj
   (deftest advisory-capacities-are-bounded-before-native-allocation
     (binding [resource/*max-intermediate-slots* 4]
       (is (= 2 (invoke 'bounded-probe-capacity Integer/MAX_VALUE 2)))
       (is (= 1 (invoke 'bounded-probe-capacity Integer/MAX_VALUE 4)))
       (let [probe (invoke 'make-probe-set Integer/MAX_VALUE)
             index (invoke 'make-probe-map Integer/MAX_VALUE)]
         (invoke 'probe-set-add probe :value)
         (invoke 'probe-map-add index :value [:tuple])
         (is (= 1 (invoke 'probe-set-size probe)))
         (is (= 1 (invoke 'probe-map-entry-size (invoke 'probe-map-get index :value))))))))

#?(:clj
   (deftest probe-maps-share-one-account-across-buckets
     (binding [resource/*max-intermediate-slots* 8]
       (let [index (invoke 'make-probe-map 1000000)
             capacity-var (executor-var 'bounded-probe-capacity)
             capacity-fn @capacity-var
             buckets (atom 0)]
         (with-redefs-fn {capacity-var (fn [& args]
                                         (swap! buckets inc)
                                         (apply capacity-fn args))}
           (fn []
             (invoke 'probe-map-add index :first [:a])
             (invoke 'probe-map-add index :second [:b])
             (is (= 2 @buckets))
             (let [error (failure-data #(invoke 'probe-map-add index :third [:c]))]
               (is (= :query/intermediate-limit (:error error)))
               (is (= 8 (:retained error)))
               (is (= 4 (:requested error))))
             (is (= 2 @buckets) "no third bucket constructor is entered")
             (is (nil? (invoke 'probe-map-get index :third))))))
       (let [index (invoke 'make-probe-map 1000000)]
         (doseq [i (range 5)] (invoke 'probe-map-add index :one-bucket [i]))
         (is (= 5 (invoke 'probe-map-entry-size (invoke 'probe-map-get index :one-bucket))))
         (is (= 1 (:requested (failure-data #(invoke 'probe-map-add index :one-bucket [:sixth])))))
         (is (= 5 (invoke 'probe-map-entry-size (invoke 'probe-map-get index :one-bucket))))))))

#?(:clj
   (deftest sorted-probe-copy-is-admitted-before-vector-copy
     (let [probe (invoke 'make-probe-set 16)]
       (doseq [x [3 1 2 2]] (invoke 'probe-set-add probe x))
       (is (= [1 2 3] (vec (binding [resource/*max-intermediate-slots* 3]
                             (invoke 'probe-set-sorted-seek-array probe)))))
       (let [copies (atom 0)
             original-vec vec]
         (with-redefs [clojure.core/vec (fn [xs] (swap! copies inc) (original-vec xs))]
           (is (= :probe-seek-copy
                  (:operator (binding [resource/*max-intermediate-slots* 2]
                               (failure-data #(invoke 'probe-set-sorted-seek-array probe))))))
           (is (zero? @copies)))))))

#?(:clj
   (deftest selective-seeks-keep-their-small-retained-output
     (let [source (fixture)
           op {:clause '[?e :value ?v] :index :aevt :estimated-card 100000}
           inputs [(rel/->Relation {'?v 0} [[77]])]
           original-slice di/-slice
           seek-var (executor-var 'probe-driven-iterable)
           original-seek @seek-var
           slices (atom [])
           seeks (atom [])]
       (with-redefs-fn
         {#'di/-slice (fn [index from to order]
                        (swap! slices conj order)
                        (original-slice index from to order))
          seek-var (fn [index attr probe field]
                     (swap! seeks conj [(identical? index (:avet source)) attr field])
                     (original-seek index attr probe field))}
         (fn []
           (let [result (binding [resource/*max-intermediate-slots* 6]
                          (invoke 'execute-pattern-scan source op nil inputs))]
             (is (= [[77 :value 77]] (mapv #(subvec % 0 3) (:tuples result))))
             (is (= [[true :value 2]] @seeks) "the selective AVET cursor seek remains active")
             (is (empty? @slices) "the selective seek never opens a full scan slice")))))))

#?(:clj
   (deftest scan-filter-and-full-scan-regimes-remain-bounded
     (let [source (fixture)
           op {:clause '[?e :value ?v] :index :aevt :estimated-card 100}
           inputs [(rel/->Relation {'?v 0} [[77] [88]])]]
       (is (= [77 88]
              (mapv #(nth % 2)
                    (:tuples (binding [resource/*max-intermediate-slots* 12]
                               (invoke 'execute-pattern-scan source op nil inputs))))))
       (is (= 100 (count (:tuples (binding [resource/*max-intermediate-slots* 600]
                                    (invoke 'execute-pattern-scan source op nil []))))))
       (is (= :pattern-scan-output
              (:operator (binding [resource/*max-intermediate-slots* 12]
                           (failure-data #(invoke 'probe-card source (assoc op :op :pattern-scan) {:rels [] :cancel nil}))))))
       (is (= {:datahike/canceled true}
              (binding [resource/*max-intermediate-slots* 5 cancel/*cancel* (volatile! true)]
                (failure-data #(invoke 'execute-pattern-scan source op nil []))))))))

#?(:clj
   (deftest variable-attribute-probe-pairs-have-bounded-distinct-state
     (let [source (fixture)
           relation (rel/->Relation {'?a 0 '?v 1} (vec (repeat 20 [:value 77])))
           query '[?e ?a ?v]]
       (binding [resource/*max-intermediate-slots* 3]
         (is (= [[:value 77]] (invoke 'var-attr-avet-pairs source query [relation] 100000))))
       (let [two-pairs (rel/->Relation {'?a 0 '?v 1} [[:value 77] [:value 88]])
             error (binding [resource/*max-intermediate-slots* 5]
                     (failure-data #(invoke 'var-attr-avet-pairs source query [two-pairs] 100000)))]
         (is (= :query/intermediate-limit (:error error)))
         (is (= :avet-probe-pairs (:operator error)))
         (is (= 3 (:retained error)))
         (is (= 3 (:requested error)))))))
