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
    entity-count))


(defn measure-performance-full [initial-size n-datoms {:keys [config-name config] }]
  (log/debug (str "Measuring database with config named '" name 
                  ", schema size " (count c/schema)
                  "', database datoms " initial-size 
                  " and " n-datoms " datom" (when (not= n-datoms 1) "s") 
                  " in transaction..."))
  (let [unique-config (assoc config :name (str (UUID/randomUUID)))
        simple-config (-> config
                          (assoc :name config-name)
                          (assoc :backend (get-in config [:store :backend]))
                          (dissoc :store))
      
        initial-entities (init-db initial-size unique-config)
        initial-datoms (* initial-entities (count c/schema))

        {conn :res t-connection-0 :t} (timed (d/connect unique-config))

        tx-entities (int (Math/ceil (/ n-datoms (count c/schema))))
        tx-datoms (* tx-entities (count c/schema))

        tx (vec (repeatedly tx-entities c/rand-entity))
        t-transaction-n (:t (timed (d/transact conn tx)))

        final-datoms (+ initial-datoms tx-datoms)
        final-entities (+ initial-entities tx-entities)

        _ (d/release conn)
        t-connection-0n (:t (timed (d/connect unique-config)))


        queries0n (vec (for [{:keys [function query details]} (c/queries @conn tx)]
                        (do (log/debug (str " Querying with " function " using " details "..."))
                            {:time (:t (timed (d/q query @conn)))
                             :context {:dh-config simple-config :function function :db-entities final-entities :db-datoms final-datoms
                                       :execution details}})))]
    (d/release conn) 
    (concat queries0n
            [{:time t-connection-0  :context {:dh-config simple-config :function :connection  :db-entities initial-entities :db-datoms initial-datoms}}
             {:time t-transaction-n :context {:dh-config simple-config :function :transaction :db-entities initial-entities :db-datoms initial-datoms 
                                              :execution {:tx-entities tx-entities :tx-datoms tx-datoms}}}
             {:time t-connection-0n  :context {:dh-config simple-config :function :connection  :db-entities final-entities :db-datoms final-datoms}}])))

(defn time-statistics [times]
  (let [n (count times)
        mean (/ (apply + times) n)]
    {:mean mean
     :median (nth (sort times) (int (/ n 2)))
     :std (->> times
               (map #(* (- % mean) (- % mean)))
               (apply +)
               (* (/ 1.0 n))
               Math/sqrt)
     :count n}))

(defn get-measurements [{:keys [db-datom-counts tx-datom-counts config-name iterations]}]
  (->> (for [config (if config-name
                      (filter #(= (:config-name %) config-name) c/db-configs)
                      c/db-configs)
             initial-size db-datom-counts
             n tx-datom-counts
             _ (range iterations)]
         (measure-performance-full initial-size n config))
       (apply concat)
       (group-by :context)
       (map (fn [[context group]]
              {:context context
               :time (time-statistics (map :time group))}))))
