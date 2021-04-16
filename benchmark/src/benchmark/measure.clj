(ns benchmark.measure
  (:require [benchmark.config :as c]
            [taoensso.timbre :as log]
            [datahike.api :as d])
  (:import [java.util UUID]))


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


(defn measure-performance-full [initial-size n-datoms {:keys [name config]}]
  (log/debug (str "Measuring database with config named '" name "', database size " initial-size " and " n-datoms " datom" (when (not= n-datoms 1) "s") " in transaction..."))
  (let [unique-config (assoc config :name (str (UUID/randomUUID)))
        _ (init-db initial-size unique-config)
        simple-config (-> config
                          (assoc :name name)
                          (assoc :dh-backend (get-in config [:store :backend]))
                          (dissoc :store))
        final-size (+ initial-size n-datoms)

        {conn :res t-connection-0 :t} (timed (d/connect unique-config))
        entity-count (int (Math/ceil (/ n-datoms (count c/schema))))
        entities (vec (repeatedly entity-count c/rand-entity))
        t-transaction-n (:t (timed (d/transact conn entities)))

        _ (d/release conn)
        t-connection-n (:t (timed (d/connect unique-config)))

        queries (vec (for [{:keys [function query details]} (c/queries @conn entities)]
                       (do (log/debug (str " Querying with " function " using " details "..."))
                           {:time (:t (timed (d/q query @conn)))
                            :context {:db simple-config :function function :exec-details details :db-size final-size}})))]
    (d/release conn)
    (conj queries
          {:time t-connection-0  :context {:db simple-config :function :connection  :db-size initial-size}}
          {:time t-transaction-n :context {:db simple-config :function :transaction :db-size initial-size :tx-size n-datoms}}
          {:time t-connection-n  :context {:db simple-config :function :connection  :db-size final-size}})))

(defn time-statistics [times]
  (let [n (count times)
        mean (/ (apply + times) n)]
    {:mean mean
     :median (nth (sort times) (int (/ n 2)))
     :std (->> times
               (map #(* (- % mean) (- % mean)))
               (apply +)
               (* (/ 1.0 n))
               Math/sqrt)}))

(defn get-measurements [databases]
  (->> (for [config databases
             initial-size c/initial-datoms
             n c/datom-counts
             _ (range c/iterations)]
         (measure-performance-full initial-size n config))
       (apply concat)
       (group-by :context)
       (map (fn [[context group]] {:context context
                                   :time (time-statistics (map :time group))}))))