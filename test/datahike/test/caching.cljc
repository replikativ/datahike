(ns datahike.test.caching
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer [is deftest]])
   [datahike.constants :refer [tx0]]
   [datahike.core :as d]
   [datahike.api :as dh]))

(defmacro timed
  "Evaluates expr. Returns the value of expr and the time in a map."
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     {:res ret# :t (/ (double (- (. System (nanoTime)) start#)) 1000000.0)}))

(def test-schema {:i {:db/cardinality :db.cardinality/one}})

(def test-datoms (mapv (fn [e] (d/datom e :i (rand-int 10000) tx0))
                       (range 100000)))

(def test-db (d/init-db test-datoms test-schema))

(deftest pattern-caching-test
  (let [{res1 :res t1 :t} (timed (dh/q '[:find ?e :where [?e :i 50]] test-db))
        {res2 :res t2 :t} (timed (dh/q '[:find ?e :where [?e :i 50]] test-db))]
    (println "t1 " t1 " t2 " t2)
    (is (= res1 res2))
    (is (< t2 t1))))
(let [{res1 :res t1 :t} (timed (dh/q '[:find ?e :where [?e :i 50]] test-db))
      {res2 :res t2 :t} (timed (dh/q '[:find ?e :where [?e :i 50]] test-db))]
  (println "t1 " t1 " t2 " t2))