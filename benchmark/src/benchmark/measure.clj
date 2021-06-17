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
    {:initial-tx tx
     :conn conn}))

(defn measure-performance-full [db-entities
                                {:keys [iterations function query data-found-opts data-types tx-entity-counts] :as _options}
                                {:keys [config-name config]}]
  (log/info (str "Measuring database with config named '" config-name ", "
                  (count c/schema) " attributes in entity, and "
                  db-entities " entities in database"))
  
  (let [unique-config (assoc config :name (str (UUID/randomUUID)))
        simple-config (-> config
                          (assoc :name config-name)
                          (assoc :backend (get-in config [:store :backend]))
                          (dissoc :store))
        db-datoms (* db-entities (count c/schema))

        {:keys [initial-tx conn]} (init-db db-entities unique-config)

        [conn2 connection-times] (if (#{:all :connection} function)
                                   (let [_  (d/release conn)
                                         {conn-new :res conn-t :t} (timed (d/connect unique-config))]
                                     [conn-new [{:time conn-t
                                                 :context {:dh-config simple-config
                                                           :function :connection
                                                           :db-entities db-entities
                                                           :db-datoms db-datoms}}]])
                                   [conn []])

        query-times (if (#{:all :query} function)
                      (let [data-found (case data-found-opts
                                         true [true]
                                         false [false]
                                         :all [true false])
                            queries (if (pos? (count initial-tx))
                                      (c/all-queries @conn2 initial-tx data-types data-found)
                                      (c/non-var-queries @conn2 data-types (count initial-tx)))
                            filtered-queries (if (= query :all)
                                               queries
                                               (filter #(= query (:function %)) queries))]
                        (vec (for [{:keys [function query details]} filtered-queries]
                               (do (log/debug (str " Querying with " function " using " details "..."))
                                    {:time (:t (timed (d/q query @conn2)))
                                     :context {:dh-config simple-config
                                               :function function
                                               :db-entities db-entities
                                               :db-datoms db-datoms
                                               :execution details}} ))))
                      [])
        _ (d/release conn)
        _ (d/delete-database unique-config)
        transaction-times (if (contains? #{:all :transaction} function)
                            (vec (for [tx-entities tx-entity-counts
                                       _ (range iterations)]
                                   (let [unique-config (assoc config :name (str (UUID/randomUUID)))
                                         {:keys [conn]} (init-db db-entities unique-config)
                                         tx (vec (repeatedly tx-entities (partial c/rand-entity Integer/MAX_VALUE)))
                                         t (:t (timed (d/transact conn tx)))]
                                     (d/release conn)
                                     (d/delete-database unique-config)
                                     {:time t
                                      :context {:dh-config simple-config
                                                :function :transaction
                                                :db-entities db-entities
                                                :db-datoms db-datoms
                                                :execution {:tx-entities tx-entities
                                                            :tx-datoms (* (count c/schema) tx-entities)}}})))
                            [])]
    (concat query-times connection-times transaction-times)))

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

(defn get-measurements [{:keys [db-entity-counts config-name iterations] :as options}]
  (->> (for [config (if (= config-name :all)
                      c/db-configs
                      (filter #(= (:config-name %) config-name) c/db-configs))
             db-entities db-entity-counts
             _ (range iterations)]
         (measure-performance-full db-entities options config))
       doall
       (apply concat)
       vec
       (group-by :context)
       (map (fn [[context group]]
              (let [measurements (vec group)
                    times (map :time measurements)]
                (if (nil? context)
                  nil
                  {:context context
                   :time (time-statistics times)}))))
      
       (remove nil?)))
