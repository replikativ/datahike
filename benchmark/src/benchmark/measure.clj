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


(defn init-db [entity-count config]
  (d/delete-database config)
  (d/create-database config)

  (let [tx (vec (repeatedly entity-count (partial c/rand-entity entity-count)))
        conn (d/connect config)]
    (d/transact conn c/schema)
    (when (pos? (count tx))
      (d/transact conn tx))
    (d/release conn)))


(defn measure-performance-full [db-entity-count tx-entity-count {:keys [config-name config] }]
  (log/debug (str "Measuring database with config named '" name ","
                  (count c/schema) " attributes in schema, "
                  db-entity-count " entities in database, and "
                  tx-entity-count " entities in transaction..."))
  (let [unique-config (assoc config :name (str (UUID/randomUUID)))
        simple-config (-> config
                          (assoc :name config-name)
                          (assoc :backend (get-in config [:store :backend]))
                          (dissoc :store))
        _ (init-db db-entity-count unique-config)

        initial-entities db-entity-count
        initial-datoms (* initial-entities (count c/schema))

        {conn :res t-connection-0 :t} (timed (d/connect unique-config))

        tx-entities tx-entity-count
        tx-datoms (* tx-entities (count c/schema))

        tx (vec (repeatedly tx-entities c/rand-entity))
        t-transaction-n (:t (timed (d/transact conn tx)))

        final-datoms (+ initial-datoms tx-datoms)
        final-entities (+ initial-entities tx-entities)

        _ (d/release conn)
        t-connection-0n (:t (timed (d/connect unique-config)))


        queries0n (vec (for [{:keys [function query details]} (if (pos? (count tx))
                                                                (c/all-queries @conn tx)
                                                                (c/non-var-queries @conn))]
                        (do (log/debug (str " Querying with " function " using " details "..."))
                            {:time (:t (timed (d/q query @conn)))
                             :context {:dh-config simple-config :function function
                                       :db-entities final-entities :db-datoms final-datoms
                                       :execution details}})))]
    (d/release conn)
    (concat queries0n
            [{:time t-connection-0  :context {:dh-config simple-config :function :connection
                                              :db-entities initial-entities :db-datoms initial-datoms}}
             {:time t-transaction-n :context {:dh-config simple-config :function :transaction
                                              :db-entities initial-entities :db-datoms initial-datoms
                                              :execution {:tx-entities tx-entities :tx-datoms tx-datoms}}}
             {:time t-connection-0n :context {:dh-config simple-config :function :connection
                                              :db-entities final-entities :db-datoms final-datoms}}])))

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
     :count n
     :observations (vec times)}))

(defn get-measurements [{:keys [db-entity-counts tx-entity-counts config-name iterations] :as options}]
  (println options)
  (->> (for [config (if config-name
                      (filter #(= (:config-name %) config-name) c/db-configs)
                      c/db-configs)
             db-entities db-entity-counts
             tx-entities tx-entity-counts
             _ (range iterations)]
         (measure-performance-full db-entities tx-entities config))
       doall
       (apply concat)
       (group-by :context)
       (map (fn [[context group]]
              {:context context
               :time (time-statistics (map :time group))}))))

