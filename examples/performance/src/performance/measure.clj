(ns performance.measure
  (:require [datahike.api :as d]
            [datomic.api :as da]
            [performance.db.hitchhiker :as hh]
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


(defn execution-time-with-res
  "Return result and execution time of function as vector [result time]"
  [f]
  (let [res (with-out-str-data-map (time (f)))]
    [(:res res) (read-string (nth (str/split (:str res) #" ")
                                  2))]))

(defn execution-times
  "Get statistics for execution time.
  - setup-fn is called in each iteration before the function to measure and must return a collection
  - f-to-measure is applied to result of setup-fn
  - tear-down-fn is called after each iteration and takes the result of f-to-measure as argument"
  ([iterations f-to-measure]
   (execution-times iterations vector f-to-measure identity))
  ([iterations setup-fn f-to-measure]
   (execution-times iterations setup-fn f-to-measure identity))
  ([iterations setup-fn f-to-measure tear-down-fn]
   (let [times (vec (repeatedly iterations
                                #(let [args (setup-fn)
                                       [res t] (execution-time-with-res (apply f-to-measure args))]
                                   (tear-down-fn res)
                                   t)))]
     {:samples times :mean (is/mean times) :sd (is/sd times)})))


(defn execution-times-stable
  "Uses criterion to find more precise statistics for execution time.
   Runs multiple times and can take a long time to run.
   - one-time-setup-fn is called once before the measurements and must return a collection
   - f-to-measure is applied to result of setup function
   - one-time-tear-down-fn is applied to result of setup function and called once after all iterations"
  ([f-to-measure]
   (execution-times-stable vector f-to-measure seq))
  ([one-time-setup-fn f-to-measure]
   (execution-times-stable one-time-setup-fn f-to-measure seq))
  ([one-time-setup-fn f-to-measure one-time-tear-down-fn]
   (let [args (one-time-setup-fn)
         stats (c/benchmark (apply f-to-measure args) {:verbose false})]
     (apply one-time-tear-down-fn args)
     stats)))



;; Measurements for specific functions

(defmulti measure-transaction-times
  "Returns mean and standard deviation of measured transaction times.
     {:mean mean-time :sd standard-deviation}
   If iteration count is not provided, criterion is used for measurements.
   This provides more stable measurements but same transaction is used for all iterations"
  (fn [_ lib _ _] lib)
  (fn [lib _ _] lib))

(defmethod measure-transaction-times :datahike
  ([iterations _ conn tx-gen]
   (execution-times iterations tx-gen #(d/transact conn %)))
  ([_ conn tx-gen]
   (execution-times-stable tx-gen #(d/transact conn %))))  ;; creates out of memory exceptions when trying to measure asynchronous function

(defmethod measure-transaction-times :datomic
  ([iterations _ conn tx-gen]
   (execution-times iterations tx-gen #(deref (da/transact conn %))))
  ([_ conn tx-gen]
   (execution-times-stable tx-gen #(deref (da/transact conn %)))))

(defmethod measure-transaction-times :hitchhiker
  ([iterations _ conn tx-gen]
   (execution-times iterations #(vector (:tree conn) (hh/entities->nodes conn (tx-gen))) #(hh/insert-many %1 %2)))
  ([_ conn tx-gen]
   (execution-times-stable #(vector (:tree conn) (hh/entities->nodes conn (tx-gen))) #(hh/insert-many %1 %2))))


(defmulti measure-connection-times
          "Returns mean and standard deviation of measured transaction times.
             {:mean mean-time :sd standard-deviation}
           If iteration count is not provided, criterion is used for measurements.
           This provides more stable measurements but same transaction is used for all iterations"
          (fn [_ lib _ ] lib)
          (fn [lib _] lib))

(defmethod measure-connection-times :datahike
  ([iterations _ uri] (execution-times iterations vector #(d/connect uri) #(d/release %)))
  ([_ uri] (execution-times-stable vector #(d/connect uri) #(d/release %))))

(defmethod measure-connection-times :datomic
  ([iterations _ uri] (execution-times iterations vector #(da/connect uri) #(da/release %)))
  ([_ uri] (execution-times-stable vector #(da/connect uri) #(da/release %))))


(defmulti measure-connection-release-times
          "Returns mean and standard deviation of measured transaction times.
             {:mean mean-time :sd standard-deviation}
           If iteration count is not provided, criterion is used for measurements.
           This provides more stable measurements but same transaction is used for all iterations"
          (fn [_ lib _ ] lib)
          (fn [lib _] lib))

(defmethod measure-connection-release-times :datahike
  ([iterations _ uri] (execution-times iterations #(d/release (d/connect uri))))
  ([_ uri] (execution-times-stable #(d/release (d/connect uri)))))

(defmethod measure-connection-release-times :datomic
  ([iterations _ uri] (execution-times iterations #(da/release (da/connect uri))))
  ([_ uri] (execution-times-stable #(da/release (da/connect uri)))))


(defmulti measure-query-times
          "Returns mean and standard deviation of measured transaction times.
             {:mean mean-time :sd standard-deviation}
           If iteration count is not provided, criterion is used for measurements.
           This provides more stable measurements but same transaction is used for all iterations"
          (fn [_ lib _ _] lib)
          (fn [lib _ _] lib))

(defmethod measure-transaction-times :datahike
  ([iterations _ db query-gen] (execution-times iterations query-gen #(d/q db %)))
  ([_ db query-gen] (execution-times-stable query-gen #(d/q db %))))

(defmethod measure-transaction-times :datomic
  ([iterations _ db query-gen] (execution-times iterations query-gen #(da/q db %)))
  ([_ db query-gen] (execution-times-stable query-gen #(da/q db %))))
