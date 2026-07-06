(ns datahike.test.lru-weighted-test
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer [is deftest]])
   [datahike.lru :as lru]
   #?(:clj [datahike.api :as d])
   #?(:clj [datahike.query :as q])))

(defn- weighted-state [c] (.-state c))
(defn- total-weight [c] (:total-weight (weighted-state c)))
(defn- entry-count [c] (count (:key-value (weighted-state c))))

(deftest count-bound-evicts-oldest
  (let [c (-> (lru/weighted-lru 2 0) (assoc :a 1) (assoc :b 2) (assoc :c 3))]
    (is (nil? (get c :a)))
    (is (= 2 (get c :b)))
    (is (= 3 (get c :c)))
    (is (= 2 (entry-count c)))))

(deftest weight-bound-evicts-oldest
  (let [c (-> (lru/weighted-lru 100 5 count) (assoc :a [1 2 3]) (assoc :b [4 5 6]))]
    (is (nil? (get c :a)))
    (is (= [4 5 6] (get c :b)))
    (is (<= (total-weight c) 5))))

(deftest keeps-newest-even-when-alone-over-budget
  (let [c (-> (lru/weighted-lru 100 2 count) (assoc :big [1 2 3 4 5]))]
    (is (= [1 2 3 4 5] (get c :big)))
    (is (= 1 (entry-count c)))))

(deftest lru-touch-protects-recently-used
  (let [c (-> (lru/weighted-lru 2 0) (assoc :a 1) (assoc :b 2) (assoc :a 1) (assoc :c 3))]
    (is (= 1 (get c :a)))
    (is (nil? (get c :b)))
    (is (= 3 (get c :c)))))

(deftest shrink-loop-evicts-until-within-budget
  (let [c (reduce (fn [c k] (assoc c k [k k k]))
                  (lru/weighted-lru 100 4 count)
                  [:a :b :c :d])]
    (is (nil? (get c :a)))
    (is (nil? (get c :c)))
    (is (= [:d :d :d] (get c :d)))
    (is (<= (total-weight c) 4))))

(deftest disabled-budget-behaves-as-count-lru
  (let [c (reduce (fn [c i] (assoc c i (vec (range 100))))
                  (lru/weighted-lru 3 0)
                  (range 10))]
    (is (= 3 (entry-count c)))
    (is (zero? (total-weight c)))))

#?(:clj
   (deftest query-cache-bounded-by-weight-budget
     (let [orig-limit @#'q/*query-cache-weight-limit*
           orig-size  @#'q/*query-cache-size*
           cfg        {:store {:backend :memory :id (random-uuid)}
                       :schema-flexibility :read
                       :keep-history?      false}]
       (try
         (q/set-query-cache-size! 1000)
         (q/set-query-cache-weight-limit! 200)
         (q/clear-query-cache!)
         (d/create-database cfg)
         (let [conn (d/connect cfg)]
           (d/transact conn {:tx-data (vec (for [i (range 60)]
                                             {:db/id (- (inc i)) :nm (str "e" i) :age i}))})
           (dotimes [t 30]
             (d/transact conn {:tx-data [{:nm (str "x" t)}]})
             (d/q '[:find ?n ?a :where [?e :nm ?n] [?e :age ?a]] @conn))
           (let [st (.-state @q/query-result-cache)]
             (is (<= (:total-weight st) 200))
             (is (< (count (:key-value st)) 30)))
           (d/release conn))
         (finally
           (d/delete-database cfg)
           (q/set-query-cache-size! orig-size)
           (q/set-query-cache-weight-limit! orig-limit)
           (q/clear-query-cache!))))))

#?(:clj
   (deftest scalar-find-cacheable-under-weight-budget
     ;; Regression: a scalar find (`[:find ?x .]`) caches a non-collection
     ;; result. The weigh path must not `(count ...)` it — that threw
     ;; "count not supported on this type" and broke every scalar query
     ;; while the (default) weight budget was enabled.
     (let [orig-limit @#'q/*query-cache-weight-limit*
           cfg        {:store {:backend :memory :id (random-uuid)}
                       :schema-flexibility :write
                       :keep-history?      false}]
       (try
         (q/set-query-cache-weight-limit! 1000000)   ; default budget on
         (q/clear-query-cache!)
         (d/create-database cfg)
         (let [conn (d/connect cfg)]
           (d/transact conn [{:db/ident :pos/x :db/valueType :db.type/long
                              :db/cardinality :db.cardinality/one}])
           (d/transact conn [{:pos/x 42}])
           (let [db @conn]
             (is (= 42 (d/q '[:find ?x . :where [_ :pos/x ?x]] db)))
             (is (= 42 (d/q '[:find ?x . :where [_ :pos/x ?x]] db)))   ; cache-hit path
             (is (= #{[42]} (d/q '[:find ?x :where [_ :pos/x ?x]] db)))) ; other finds still fine
           (d/release conn))
         (finally
           (d/delete-database cfg)
           (q/set-query-cache-weight-limit! orig-limit)
           (q/clear-query-cache!))))))
