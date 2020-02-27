(ns performance.measure
  (:require [datahike.api :as d]
            [datomic.api :as da]
            [incanter.stats :as is]
            [datahike-leveldb.core]
            [datahike-postgres.core]
            [criterium.core :as c]
            [clojure.string :as str])
  (:import (java.io StringWriter)))

(defmacro with-out-str-data-map
  [& body]
  `(let [s# (new StringWriter)]
     (binding [*out* s#]
       (let [r# ~@body]
         {:res r#
          :str (str s#)}))))


(defn e-time [f]
  "Return execution time of function"
  (read-string (nth (str/split (with-out-str (time (f))) #" ")
                    2)))

(defn e-time-with-res [f]
  "Return result and execution time of function"
  (let [res (with-out-str-data-map (time (f)))]
    [(:res res) (read-string (nth (str/split (:str res) #" ")
                                  2))]))

(defn measure-tx-times [iteration-count lib conn tx-gen]
  (let [t (vec (repeatedly iteration-count
                         (fn [] (let [txs (tx-gen)]
                                  (case lib
                                    "datahike" (e-time #(d/transact conn txs))
                                    "datomic" (e-time #(deref (da/transact conn txs))))))))]
    {:samples t :mean (is/mean t) :sd (is/sd t)}))

(defn measure-tx-times-auto [lib conn tx-gen]         ;; test !
  "Use criterium for measurements"
  (let [txs (tx-gen)]
    (case lib
      "datahike" (c/benchmark (d/transact conn txs) {:verbose false}) ;; creates out of memory exceptions when trying to measure asynchron function
      "datomic" (c/benchmark @(da/transact conn txs) {:verbose false}))))


(defn measure-connect-times [iteration-count lib uri]
  (let [t (vec (repeatedly iteration-count
                         (fn [] (case lib
                                  "datahike" (do (let [[conn t] (e-time-with-res #(d/connect uri))]
                                                   (d/release conn)
                                                   t))
                                  "datomic" (do (let [[conn t] (e-time-with-res #(da/connect uri))]
                                                  (da/release conn)
                                                  t))))))]
    {:samples t :mean (is/mean t) :sd (is/sd t)}))

(defn measure-connect-times-auto [lib uri]
  (case lib
    "datahike" (c/benchmark (let [conn (d/connect uri)] (d/release conn)) {:verbose false})
    "datomic" (c/benchmark (let [conn (da/connect uri)] (da/release conn)) {:verbose false})))

(defn measure-query-times [iteration-count lib db query-gen]
  (let [t (vec (repeatedly iteration-count
                         (fn [] (let [query (query-gen)]
                                  (case lib
                                       "datahike" (e-time #(d/q query db))
                                       "datomic" (e-time #(da/q query db)))))))]
    {:samples t :mean (is/mean t) :sd (is/sd t)}))

(defn measure-query-times-auto [lib db query-gen]
  (let [query (query-gen)]
    (case lib
      "datahike" (c/benchmark (d/q db query) {:verbose false})
      "datomic" (c/benchmark (da/q db query) {:verbose false}))))
