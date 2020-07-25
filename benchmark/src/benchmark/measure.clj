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
    [(:res res) (read-string (nth (str/split (:str res) #" ") 2))]))

(defn init-db [initial-size {:keys [store schema-on-read temporal-index index]}]
  (d/delete-database store)
  (d/create-database store :schema-on-read schema-on-read
                           :temporal-index temporal-index
                           :index          index)

  (let [entity-count (int (Math/floor (/ initial-size (count c/schema))))
        tx (repeatedly entity-count c/rand-entity)
        conn (d/connect store)]
     (d/transact conn c/schema)
     (when (pos? (count tx))
        (d/transact conn tx)
     (d/release conn)
     tx)))

(defn measure-performance-full [initial-size n-datoms {:keys [store] :as config}]
 (let [_ (init-db initial-size config)
      [conn t-connection-0] (get-time-with-res #(d/connect store))

      entity-count (int (Math/floor (/ n-datoms (count c/schema))))
      tx (repeatedly entity-count c/rand-entity)
      [conn t-transaction-n] (get-time-with-res #(d/transact store tx))

      _ (d/release store)
      [conn t-connection-n] (get-time-with-res #(d/connect store))

      rand-s-val (rand-nth (map :s1 tx))
      [res t-query1-n] (d/q (c/q1 rand-s-val))

      rand-i-val (rand-nth (map :i1 tx))
      [res t-query2-n] (d/q (c/q2 rand-i-val))

      final-size (+ initial-size n-datoms)]
    (d/release store)
    [{:time t-connection-0  :context {:db config :function :connection  :db-size initial-size}}
     {:time t-transaction-n :context {:db config :function :transaction :db-size initial-size :tx-size n-datoms}}
     {:time t-connection-n  :context {:db config :function :connection  :db-size final-size}}
     {:time t-query1-n      :context {:db config :function :query1      :db-size final-size}}
     {:time t-query2-n      :context {:db config :function :query2      :db-size final-size}}]))
