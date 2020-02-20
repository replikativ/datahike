(ns performance.measure
  (:require [datahike.api :as d]
            [datahike-leveldb.core]
            [datahike-postgres.core]
            [datomic.api :as da]
            [criterium.core :as c]
            [incanter.stats :as s]
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

(defn measure-tx-times [iteration-count datahike? conn tx-gen]
  (vec (repeatedly iteration-count
                   (fn [] (let [txs (tx-gen)]
                            (if datahike?
                              (e-time #(d/transact conn txs))
                              (e-time #(deref (da/transact conn txs)))))))))

(defn measure-tx-times-auto [datahike? conn tx-gen]         ;; test !
  "Use criterium for measurements"
  (let [txs (tx-gen)
        res (if datahike?
              (c/benchmark (d/transact conn txs) {:verbose false}) ;; creates out of memory exceptions when trying to measure asynchron function
              (c/benchmark @(da/transact conn txs) {:verbose false}))]
    (:samples res)))


(defn measure-connect-times [iteration-count datahike? uri]
  (vec (repeatedly iteration-count
                   (fn [] (if datahike?
                            (do (let [[conn t] (e-time-with-res #(d/connect uri))]
                                  (d/release conn)
                                  t))
                            (do (let [[conn t] (e-time-with-res #(da/connect uri))]
                                  (da/release conn)
                                  t)))))))

(defn measure-connect-times-auto [datahike? uri]
  (if datahike?
    (c/benchmark (let [conn (d/connect uri)] (d/release conn)) {:verbose false})
    (c/benchmark (let [conn (da/connect uri)] (da/release conn)) {:verbose false})))

(defn measure-query-times [iteration-count datahike? conn query]
  (vec (repeatedly iteration-count
                   (fn [] (if datahike?
                            (e-time #(d/q conn query))
                            (e-time #(da/q conn query)))))))

(defn measure-query-times-auto [iteration-count datahike? db query]
  (if datahike?
    (c/benchmark (d/q db query) {:verbose false})
    (c/benchmark (da/q db query) {:verbose false})))

;;(def schema [{:db/ident       :name :db/valueType   :db.type/string :db/cardinality :db.cardinality/one}])


;;(def uri "datahike:level:///tmp/level-perf")



;;(def dconn (let [uri "datahike:file:///tmp/file-perf"] (d/delete-database uri) (d/create-database uri :initial-tx schema) (d/connect uri)))

;;(def cconn (let [uri "datomic:mem://datahike-vs-datomic"] (da/delete-database uri)  (da/create-database uri) (da/connect uri) ) )
;;@(da/transact cconn schema)


;;(first (:mean (measure-tx-times-auto true dconn 512)))
;;(s/mean (measure-tx-times 100 true dconn 512))

;;(measure-tx-times 10 true dconn #(create-n-transactions 512))
;;(s/mean (measure-connect-times 10 true uri))