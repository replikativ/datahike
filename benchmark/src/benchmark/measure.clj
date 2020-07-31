(ns benchmark.measure
  (:require [benchmark.config :as c]
            [datahike.api :as d]
            [clojure.string :as str])
  (:import (java.io StringWriter)))

(defmacro with-out-str-data-map
  [& body]
  `(let [s# (new StringWriter)]
     (binding [*out* s#]
       (let [r# ~@body]
         {:res r#
          :str (str s#)}))))


(defn get-time-with-res
  "Return result and execution time of function as vector [result time]"
  [f]
  (let [res (with-out-str-data-map (time (f)))]
    [(:res res) (let [tokens (str/split (:str res) #" ")]
                  (read-string (nth tokens (- (count tokens) 2))))]))

(defn init-db [initial-size config]
  (d/delete-database config)
  (d/create-database config)

  (let [entity-count (int (Math/floor (/ initial-size (count c/schema))))
        tx           (vec (repeatedly entity-count c/rand-entity))
        conn (d/connect config)]
     (d/transact conn c/schema)
     (println "1")
     (when (pos? (count tx))
        (d/transact conn tx))
     (d/release conn)
     tx))


(defn measure-performance-full [initial-size n-datoms config]
  (init-db initial-size config)
  (let [[conn t-connection-0] (get-time-with-res #(d/connect config))
        entity-count (int (Math/ceil (/ n-datoms (count c/schema))))
        tx           (vec (repeatedly entity-count c/rand-entity))
        [_ t-transaction-n] (get-time-with-res #(d/transact conn tx))

        _ (d/release conn)
        [_ t-connection-n] (get-time-with-res #(d/connect config))

        rand-s-val (rand-nth (mapv :s1 tx))
        [_ t-query1-n] (get-time-with-res #(d/q (c/q1 rand-s-val) @conn))

        rand-i-val (rand-nth (mapv :i1 tx))
        [_ t-query2-n] (get-time-with-res #(d/q (c/q2 rand-i-val) @conn))

        final-size (+ initial-size n-datoms)]
    (d/release conn)
    [{:time t-connection-0  :context {:db config :function :connection  :db-size initial-size}}
     {:time t-transaction-n :context {:db config :function :transaction :db-size initial-size :tx-size n-datoms}}
     {:time t-connection-n  :context {:db config :function :connection  :db-size final-size}}
     {:time t-query1-n      :context {:db config :function :query1      :db-size final-size}}
     {:time t-query2-n      :context {:db config :function :query2      :db-size final-size}}]))
