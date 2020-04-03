(ns performance.measure
  (:require [datahike.api :as d]
            [datomic.api :as da]
            [performance.db.hitchhiker :as hh]
            [performance.db.api :as db]
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
  - f-to-measure takes the result of setup-fn as argument
  - tear-down-fn is called after each iteration and takes the result of setup-fn as first argument
    and the result of f-to-measure as second argument
  i.e. (let [args (setup-fn)
             res (f-to-measure args)]
          (tear-down-fn args res)"
  ([iterations f-to-measure]
   (execution-times iterations list f-to-measure list))
  ([iterations setup-fn f-to-measure]
   (execution-times iterations setup-fn f-to-measure list))
  ([iterations setup-fn f-to-measure tear-down-fn]
   (let [times (vec (repeatedly iterations
                                #(let [args (setup-fn)
                                       [res t] (execution-time-with-res (fn [] (f-to-measure args)))]
                                   (tear-down-fn args res)
                                   t)))]
     {:samples times :mean (is/mean times) :sd (if (< (count times) 2) 0 (is/sd times))})))


(defn execution-times-stable
  "Uses criterion to find more precise statistics for execution time.
   Runs multiple times and can take a long time to run.
   - one-time-setup-fn is called once before the measurements and must return a collection
   - f-to-measure takes the result of setup-fn as argument
   - one-time-tear-down-fn is called once after all iterations and takes the result of setup-fn as argument
  i.e. (let [args (setup-fn)
             _ (f-to-measure args)]
          (one-time-tear-down-fn args)"
  ([f-to-measure]
   (execution-times-stable list f-to-measure list))
  ([one-time-setup-fn f-to-measure]
   (execution-times-stable one-time-setup-fn f-to-measure list))
  ([one-time-setup-fn f-to-measure one-time-tear-down-fn]
   (let [args (one-time-setup-fn)
         stats (c/benchmark (f-to-measure args) {:verbose false})]
     (one-time-tear-down-fn args)
     stats)))


;; Measurements for specific functions

(defn prepare-transaction-measurements [uri schema tx-gen tx-datom-count db-size]
  (let [sor (:schema-on-read uri)
        ti (:temporal-index uri)
        conn (db/prepare-db-and-connect (:lib uri) (:uri uri) (if sor [] schema) (tx-gen db-size) :schema-on-read sor :temporal-index ti)
        tx-data (tx-gen tx-datom-count)]
    [conn tx-data]))

(defmulti measure-transaction-times
  "Returns mean and standard deviation of measured transaction times.
     {:mean mean-time :sd standard-deviation}
   If iteration count is not provided, criterion is used for measurements.
   This provides more stable measurements but same transaction is used for all iterations"
  (fn [lib & _] lib))

(defmethod measure-transaction-times :datahike
  ([_ uri schema tx-gen tx-datom-count db-size iterations]
   (execution-times iterations
                    (fn [] (prepare-transaction-measurements uri schema tx-gen tx-datom-count db-size))
                    (fn [[conn tx-data]] (d/transact conn tx-data))
                    (fn [[conn _] _] (d/release conn))))
  ([_ uri schema tx-gen tx-datom-count db-size]
   (execution-times-stable (fn [] (prepare-transaction-measurements uri schema tx-gen tx-datom-count db-size))
                           (fn [conn tx-data] (d/transact conn tx-data))     ;; creates out of memory exceptions when trying to measure asynchronous function
                           (fn [[conn _]] (d/release conn)))))

(defmethod measure-transaction-times :datomic
  ([_ uri schema tx-gen tx-datom-count db-size iterations]
   (execution-times iterations
                    (fn [] (prepare-transaction-measurements uri schema tx-gen tx-datom-count db-size))
                    (fn [[conn tx-data]] (deref (da/transact conn tx-data)))
                    (fn [[conn _] _] (da/release conn))))
  ([_ uri schema tx-gen tx-datom-count db-size]
   (execution-times-stable (fn [] (prepare-transaction-measurements uri schema tx-gen tx-datom-count db-size))
                           (fn [conn tx-data] (deref (da/transact conn tx-data)))
                           (fn [[conn _]] (da/release conn)))))

(defmethod measure-transaction-times :hitchhiker
  ([_ uri schema tx-gen tx-datom-count db-size iterations]
   (execution-times iterations
                    (fn [] (let [[conn tx-data] (prepare-transaction-measurements uri schema tx-gen tx-datom-count db-size)]
                             [(:tree conn) (hh/entities->nodes conn tx-data)]))
                    (fn [[tree values]] (hh/insert-many tree values))))
  ([_ uri schema tx-gen tx-datom-count db-size]
   (execution-times-stable (fn [] (let [[conn tx-data] (prepare-transaction-measurements uri schema tx-gen tx-datom-count db-size)]
                                    [(:tree conn) (hh/entities->nodes conn tx-data)]))
                           (fn [[tree values]] (hh/insert-many tree values)))))


(defmulti measure-connection-times
          "Returns mean and standard deviation of measured transaction times.
             {:mean mean-time :sd standard-deviation}
           If iteration count is not provided, criterion is used for measurements.
           This provides more stable measurements but same transaction is used for all iterations"
          (fn [lib & _] lib))

(defmethod measure-connection-times :datahike
  ([_ uri iterations] (execution-times iterations
                                       list
                                       (fn [_] (d/connect uri))
                                       (fn [_ conn] (d/release conn)))))

(defmethod measure-connection-times :datomic
  ([_ uri iterations] (execution-times iterations
                                       list
                                       (fn [_] (da/connect uri))
                                       (fn [_ conn] (da/release conn)))))


(defmulti measure-connection-release-times
          "Returns mean and standard deviation of measured transaction times.
             {:mean mean-time :sd standard-deviation}
           If iteration count is not provided, criterion is used for measurements.
           This provides more stable measurements but same transaction is used for all iterations"
          (fn [lib & _] lib))

(defmethod measure-connection-release-times :datahike
  ([_ uri iterations] (execution-times iterations (fn [_] (d/release (d/connect uri)))))
  ([_ uri] (execution-times-stable (fn [_] (d/release (d/connect uri))))))

(defmethod measure-connection-release-times :datomic
  ([_ uri iterations] (execution-times iterations (fn [_] (da/release (da/connect uri)))))
  ([_ uri] (execution-times-stable (fn [_] (da/release (da/connect uri))))))


(defmulti measure-query-times
          "Returns mean and standard deviation of measured transaction times.
             {:mean mean-time :sd standard-deviation}
           If iteration count is not provided, criterion is used for measurements.
           This provides more stable measurements but same transaction is used for all iterations"
          (fn [lib & _] lib))

(defmethod measure-query-times :datahike
  ([_ db query-gen iterations] (execution-times iterations query-gen (fn [query] (d/q query db))))
  ([_ db query-gen] (execution-times-stable query-gen (fn [query] (d/q query db)))))

(defmethod measure-query-times :datomic
  ([_ db query-gen iterations] (execution-times iterations query-gen (fn [query] (da/q query db))))
  ([_ db query-gen] (execution-times-stable query-gen (fn [query] (da/q query db)))))
