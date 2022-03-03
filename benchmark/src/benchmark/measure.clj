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

(defn init-db [config]
  (d/delete-database config)
  (d/create-database config)
  (d/connect config))

(defn init-tx [entity-count conn]
  (d/transact conn c/schema)
  (if (pos? entity-count)
    (d/transact conn (vec (repeatedly entity-count (partial c/rand-entity entity-count))))
    []))

(defn time-context-map
  ([t config function entity-count datom-count]
   (time-context-map t config function entity-count datom-count nil))
  ([t config function entity-count datom-count exec]
   (cond-> {:time t
            :context {:dh-config config
                      :function function
                      :db-entities entity-count
                      :db-datoms datom-count}}
     (some? exec) (assoc-in [:context :execution] exec))))

(defn measure-connection-time [unique-cfg simple-cfg entity-count datom-count]
  [(time-context-map
   (:t (timed (d/connect unique-cfg))) simple-cfg :connection entity-count datom-count)])

(defn measure-query-times
  [{:keys [data-found-opts query data-types]} initial-tx conn config entity-count datom-count]
  (let [data-found (case data-found-opts
                     true [true]
                     false [false]
                     :all [true false])
        queries (if (pos? (count initial-tx))
                  (c/all-queries @conn initial-tx data-types data-found)
                  (c/non-var-queries @conn data-types (count initial-tx)))
        filtered-queries (if (= query :all)
                           queries
                           (filter #(= query (:function %)) queries))]
    (vec (for [{:keys [function query details]} filtered-queries]
           (do (log/debug (str " Querying with " function " using " details "..."))
               (time-context-map
                (:t (timed (d/q query @conn))) config function entity-count datom-count details))))))

(defn measure-transaction-times
  [{:keys [iterations tx-entity-counts]} config simple-config entity-count datom-count]
  (vec (for [tx-entities tx-entity-counts
             _ (range iterations)]
         (let [unique-config (assoc config :name (str (UUID/randomUUID)))
               conn (init-db unique-config)
               _ (init-tx entity-count conn)
               tx (vec (repeatedly tx-entities (partial c/rand-entity Integer/MAX_VALUE)))
               t (:t (timed (d/transact conn tx)))]
           (d/release conn)
           (d/delete-database unique-config)
           (time-context-map t simple-config :transaction entity-count datom-count
                             {:tx-entities tx-entities :tx-datoms (* (count c/schema) tx-entities)})))))

(defn measure-performance-full
  ([entity-count options cfg] (measure-performance-full entity-count options cfg {}))
  ([entity-count options {:keys [config-name config]}
    {:keys [spec-fn-name make-fn-invocation] :as specified-fn}]
   (let [unique-cfg (assoc config :name (str (UUID/randomUUID)))
         simple-cfg (-> config
                        (assoc :name config-name)
                        (assoc :backend (get-in config [:store :backend]))
                        (dissoc :store))
         datom-count (* entity-count (count c/schema))
         conn (init-db unique-cfg)
         initial-tx (init-tx entity-count conn)
         custom (some? make-fn-invocation)]
     (log/info (str "Measuring " (if custom (str "function '" spec-fn-name " on ") "")
                    "database with config named '" config-name ", " (count c/schema)
                    " attributes in entity, and " entity-count " entities in database"))
     (if custom
       (let [fn-time (:t (timed (make-fn-invocation conn)))]
         (d/delete-database unique-cfg)
         [(time-context-map fn-time simple-cfg (keyword spec-fn-name) entity-count datom-count)])
       (let [function (:function options)
             connection-times
             (when (#{:all :connection} function)
               (d/release conn)
               (measure-connection-time unique-cfg simple-cfg entity-count datom-count))
             query-times
             (when (#{:all :query} function)
               (measure-query-times options initial-tx (d/connect unique-cfg)
                                    simple-cfg entity-count datom-count))
             _ (d/release conn)
             _ (d/delete-database unique-cfg)
             transaction-times
             (when (contains? #{:all :transaction} function)
               (measure-transaction-times options config simple-cfg entity-count datom-count))]
         (concat connection-times query-times transaction-times))))))

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

(defn get-measurements
  ([options] (get-measurements options {}))
  ([{:keys [config-name db-entity-counts iterations make-custom-expr] :as options} specified-fn]
   (->> (for [cfg (if (= config-name :all)
                    c/db-configs
                    (filter #(= (:config-name %) config-name) c/db-configs))
              entity-count db-entity-counts
              _ (range iterations)]
          (measure-performance-full entity-count options cfg specified-fn))
        doall
        (apply concat)
        vec
        (group-by :context)
        (map (fn [[context group]]
               {:context context :time (time-statistics (map :time group))})))))
