(ns benchmark.measure
  (:require [benchmark.config :as c]
            [taoensso.timbre :as log]
            [datahike.api :as d]
            [datahike.datom :as datom])
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
    (vec (for [entities (->> (repeatedly entity-count (partial c/rand-entity entity-count))
                             (partition 100000 100000 nil))]
           (d/transact conn (vec entities))))
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

(defn measure-connection-time [iterations unique-cfg simple-cfg entity-count datom-count]
  (log/info (str "Measuring connection time on database with config named '" (:name simple-cfg) ", "
                 (count c/schema) " attributes in entity, and " entity-count " entities in database"))
  (for [_ (range iterations)]
    (let [timed-conn (timed (d/connect unique-cfg))]
      (d/release (:res timed-conn))
      (time-context-map (:t timed-conn) simple-cfg :connection entity-count datom-count))))

(defn measure-query-times
  [{:keys [iterations data-found-opts query data-types]} initial-tx conn config entity-count datom-count]
  (log/info (str "Measuring query times on database with config named '" (:name config) ", "
                 (count c/schema) " attributes in entity, and " entity-count " entities in database"))
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
    (flatten
     (for [{:keys [function query details]} filtered-queries]
       (do
         (log/debug (str " Querying with " function " using " details "..."))
         (for [_ (range iterations)]
           (time-context-map (:t (timed (d/q query @conn)))
                             config function entity-count datom-count details)))))))

(defn measure-transaction-times
  [{:keys [iterations tx-entity-counts]} conn config entity-count datom-count]
  (log/info (str "Measuring transaction times on database with config named '" (:name config) ", "
                 (count c/schema) " attributes in entity, and " entity-count " entities in database"))
  (for [tx-entities tx-entity-counts
        _ (range iterations)]
    (let [tx (vec (repeatedly tx-entities (partial c/rand-entity Integer/MAX_VALUE)))
          timed-transact (timed (d/transact conn tx))]
      (d/transact conn (mapv
                        (fn [dat]
                          [(if (:keep-history? (:config @conn)) :db.purge/entity :db/retractEntity) (.-e dat)])
                        (:tx-data (:res timed-transact))))
      (time-context-map (:t timed-transact) config :transaction entity-count datom-count
                        {:tx-entities tx-entities :tx-datoms (* (count c/schema) tx-entities)}))))

(defn measure-performance-full
  ([entity-count options cfg] (measure-performance-full entity-count options cfg {}))
  ([entity-count {:keys [iterations function] :as options} {:keys [config-name config]}
    {:keys [spec-fn-name make-fn-invocation] :as specified-fn}]
   (let [unique-cfg (assoc config :name (str (UUID/randomUUID)))
         simple-cfg (-> config
                        (assoc :name config-name)
                        (assoc :backend (get-in config [:store :backend]))
                        (dissoc :store))
         datom-count (* entity-count (count c/schema))
         conn (init-db unique-cfg)
         initial-tx (init-tx entity-count conn)
         measurements (vec (if (some? make-fn-invocation)
                             (do
                               (log/info
                                (str "Measuring function '" spec-fn-name " on database with config named '" config-name ", "
                                     (count c/schema) " attributes in entity, and " entity-count " entities in database"))
                               (for [_ (range iterations)]
                                 (time-context-map (:t (timed (make-fn-invocation conn))) simple-cfg (keyword spec-fn-name)
                                                   entity-count datom-count)))
                             (let [query-times
                                   (when (#{:all :query} function)
                                     (measure-query-times options initial-tx conn simple-cfg entity-count datom-count))
                                   transaction-times
                                   (when (contains? #{:all :transaction} function)
                                     (measure-transaction-times options conn simple-cfg entity-count datom-count))
                                   connection-times
                                   (when (#{:all :connection} function)
                                     (d/release conn)
                                     (measure-connection-time iterations unique-cfg simple-cfg entity-count datom-count))]
                               (concat connection-times query-times transaction-times))))]
     (d/delete-database unique-cfg)
     measurements)))

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
  ([options] (get-measurements options c/db-configs {}))
  ([options configs] (get-measurements options configs {}))
  ([{:keys [db-entity-counts db-samples] :or {db-samples 1} :as options} configs specified-fn]
   (->> (for [cfg configs
              entity-count db-entity-counts
              _ (range db-samples)]
              (measure-performance-full entity-count options cfg specified-fn))
        doall
        (apply concat)
        vec
        (group-by :context)
        (map (fn [[context group]]
               {:context context :time (time-statistics (map :time group))}))
        (sort-by (fn [m] [(-> (:context m) :dh-config :name) (-> (:context m) :db-entities)])))))
