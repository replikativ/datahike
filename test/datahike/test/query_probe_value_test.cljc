(ns datahike.test.query-probe-value-test
  (:require
   #?(:clj [clojure.test :refer [deftest is testing]]
      :cljs [cljs.test :refer-macros [deftest is testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.query :as q]
   [datahike.query.execute :as execute]))

(defn- bytes-value [n]
  #?(:clj (byte-array [n (inc n)])
     :cljs (js/Int8Array. #js [n (inc n)])))

(defn- floats-value [n]
  #?(:clj (float-array [n (inc n)])
     :cljs (js/Float32Array. #js [n (inc n)])))

(defn- doubles-value [n]
  #?(:clj (double-array [n (inc n)])
     :cljs (js/Float64Array. #js [n (inc n)])))

(def value-makers
  [[:number identity]
   [:string #(str "bucket-" %)]
   [:keyword #(keyword (str "bucket-" %))]
   [:symbol #(symbol (str "bucket-" %))]
   [:vector #(vector % (inc %))]
   [:bytes bytes-value]
   [:floats floats-value]
   [:doubles doubles-value]])

(deftest direct-probe-maps-group-equal-values-from-separate-datoms
  (doseq [[kind make-value] value-makers]
    (testing (name kind)
      (let [source (d/db-with (db/empty-db)
                              [[:db/add 1 :bucket (make-value 1)]
                               [:db/add 2 :bucket (make-value 1)]
                               [:db/add 3 :bucket (make-value 2)]
                               [:db/add 4 :bucket (make-value 2)]])
            query '[:find ?a ?b :where [?a :bucket ?v] [?b :bucket ?v]]
            expected #{[1 1] [1 2] [2 1] [2 2]
                       [3 3] [3 4] [4 3] [4 4]}]
        (doseq [legacy? [false true]]
          (binding [q/*disable-planner* legacy? q/*query-result-cache?* false]
            (is (= expected (d/q query source)))))))))

(deftest direct-probe-sets-match-equal-values-from-another-group
  (doseq [[kind make-value] value-makers]
    (testing (name kind)
      (let [source (d/db-with (db/empty-db)
                              [[:db/add 1 :left (make-value 1)]
                               [:db/add 2 :left (make-value 1)]
                               [:db/add 10 :right (make-value 1)]
                               [:db/add 11 :right (make-value 1)]
                               [:db/add 12 :right (make-value 2)]])
            query '[:find ?b :where [?a :left ?v] [?b :right ?v]]]
        (doseq [legacy? [false true]]
          (binding [q/*disable-planner* legacy? q/*query-result-cache?* false]
            (is (= #{[10] [11]} (d/q query source)))))))))

(deftest prepared-non-point-programs-fall-back-to-the-direct-join
  (let [source (d/db-with (db/empty-db)
                          [{:db/id 1 :bucket :shared}
                           {:db/id 2 :bucket :shared}
                           {:db/id 3 :bucket :other}])
        query '[:find ?a ?b :in $ ?value
                :where [?a :bucket ?value] [?b :bucket ?value]]]
    (binding [q/*disable-planner* false
              q/*fold-scalar-ins* false
              q/*query-result-cache?* false
              execute/*prepared-execution* true]
      (is (= #{[1 1] [1 2] [2 1] [2 2]} (d/q query source :shared)))
      (is (= #{[3 3]} (d/q query source :other)))
      (is (= #{} (d/q query source :absent)))
      (is (= #{[1 1] [1 2] [2 1] [2 2]} (d/q query source :shared))))))
