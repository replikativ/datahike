(ns benchmark.measure
  (:require [benchmark.config :as c]
            [datahike.api :as d]))


(defmacro timed
  "Evaluates expr. Returns the value of expr and the time in a map."
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     {:res ret# :t (/ (double (- (. System (nanoTime)) start#)) 1000000.0)}))


(defn init-db [initial-size config]
  (d/delete-database config)
  (d/create-database config)

  (let [entity-count (int (Math/floor (/ initial-size (count c/schema))))
        tx           (vec (repeatedly entity-count c/rand-entity))
        conn (d/connect config)]
     (d/transact conn c/schema)
     (when (pos? (count tx))
        (d/transact conn tx))
     (d/release conn)
     tx))


(defn measure-performance-full [initial-size n-datoms config]
  (init-db initial-size config)
  (let [{conn :res t-connection-0 :t} (timed (d/connect config))
        entity-count (int (Math/ceil (/ n-datoms (count c/schema))))
        tx           (vec (repeatedly entity-count c/rand-entity))
        t-transaction-n (:t (timed (d/transact conn tx)))

        _ (d/release conn)
        t-connection-n (:t (timed (d/connect config)))

        rand-s-val (rand-nth (mapv :s1 tx))
         t-query1-n (:t (timed (d/q (c/q1 rand-s-val) @conn)))

        rand-i-val (rand-nth (mapv :i1 tx))
        t-query2-n   (:t (timed (d/q (c/q2 rand-i-val) @conn)))

        final-size (+ initial-size n-datoms)]
    (d/release conn)
    [{:time t-connection-0  :context {:db config :function :connection  :db-size initial-size}}
     {:time t-transaction-n :context {:db config :function :transaction :db-size initial-size :tx-size n-datoms}}
     {:time t-connection-n  :context {:db config :function :connection  :db-size final-size}}
     {:time t-query1-n      :context {:db config :function :query1      :db-size final-size}}
     {:time t-query2-n      :context {:db config :function :query2      :db-size final-size}}]))
