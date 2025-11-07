(ns benchmark.measure
  (:require [benchmark.config :as c]
            [taoensso.timbre :as log]
            [datahike.api :as d]
            [datahike.store :as ds]
            [datahike.index :as di])
  (:import [java.util UUID]))

(defmacro timed
  "Evaluates expr. Returns the value of expr and the time in a map."
  [expr]
  `(let [start# (. System (nanoTime))
         ret# ~expr]
     {:res ret# :t (/ (double (- (. System (nanoTime)) start#)) 1000000.0)}))

(defn init-db [config]
  (d/delete-database config)
  (let [config (d/create-database config)]
    [(d/connect config) config]))

(defn init-tx [entity-count conn]
  (when (= :write (get-in @conn [:config :schema-flexibility]))
    (d/transact conn c/schema))
  (if (pos? entity-count)
    (vec (for [entities (->> (repeatedly entity-count (partial c/rand-entity entity-count))
                             (partition 100000 100000 nil))]
           (d/transact conn (vec entities))))
    []))

(defn time-context-map
  ([t config function db-entities db-datoms]
   (time-context-map t config function db-entities db-datoms nil))
  ([t config function db-entities db-datoms exec]
   (cond-> {:time    t
            :context {:dh-config   config
                      :function    function
                      :db-entities db-entities
                      :db-datoms   db-datoms}}
           (some? exec) (assoc-in [:context :execution] exec))))

(defn measure-connection-time [iterations unique-cfg simple-cfg db-entities db-datoms]
  (->> (range iterations)
       (map #(do (log/debug (str "  - iteration no. " % ))
                 (let [timed-conn (timed (d/connect unique-cfg))]
                   (d/release (:res timed-conn))
                   (time-context-map (:t timed-conn) simple-cfg :connection db-entities db-datoms))) )
       doall))

(defn measure-query-times
  [{:keys [iterations data-found-opts query data-types]} initial-tx conn config db-entities db-datoms]
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
    (->> filtered-queries
         (mapcat (fn [{:keys [function query details]}]
                (log/info (str "   Querying with " function " using " details ))
                (->> (range iterations)
                     (map #(do (log/debug (str "   - iteration no. " % ))
                               (time-context-map (:t (timed (d/q query @conn)))
                                                 config function db-entities db-datoms details)))
                     doall)))
         doall)))

(defn measure-transaction-times
  [{:keys [iterations tx-entity-counts]} conn config db-entities db-datoms]
  (->> tx-entity-counts
       (mapcat (fn [tx-entities]
              (let [tx-datoms (* (count c/schema) tx-entities)]
                (log/info (str "   Transacting " tx-datoms " entities"))
                (->> (range iterations)
                     (map #(do (log/debug (str "   - iteration no. " % ))
                               (let [tx (vec (repeatedly tx-entities (partial c/rand-entity Integer/MAX_VALUE)))
                                     timed-transact (timed (d/transact conn tx))]
                                 (d/transact conn (mapv (fn [dat]
                                                          [(if (:keep-history? (:config @conn))
                                                             :db.purge/entity
                                                             :db/retractEntity)
                                                           (.-e dat)])
                                                        (:tx-data (:res timed-transact))))
                                 (time-context-map (:t timed-transact) config :transaction db-entities db-datoms
                                                   {:tx-entities tx-entities :tx-datoms tx-datoms}))))
                     doall))))
       doall))

(defn measure-performance-full
  ([entity-count options cfg] (measure-performance-full entity-count options cfg {}))
  ([entity-count {:keys [iterations function] :as options} config
    {:keys [spec-fn-name make-fn-invocation] :as _specified-fn}]
   (let [unique-cfg (assoc config :name (str (UUID/randomUUID)))
         simple-cfg (-> config
                        (assoc :backend (get-in config [:store :backend]))
                        (dissoc :store))
         datom-count (* entity-count (count c/schema))
         [conn unique-cfg] (init-db unique-cfg)
         initial-tx (init-tx entity-count conn)
         measurements (vec (if (some? make-fn-invocation)
                             (do
                               (log/info (str " Measuring function '" spec-fn-name "'..."))
                               (->> (range iterations)
                                    (map #(do (log/debug (str " - iteration no. " % ))
                                              (time-context-map (:t (timed (make-fn-invocation conn))) simple-cfg (keyword spec-fn-name)
                                                                entity-count datom-count)))
                                    doall))
                             (let [query-times
                                   (when (#{:all :query} function)
                                     (log/info (str " Measuring query times..." ))
                                     (measure-query-times options initial-tx conn simple-cfg entity-count datom-count))
                                   transaction-times
                                   (when (contains? #{:all :transaction} function)
                                     (log/info (str " Measuring transaction times..."))
                                     (measure-transaction-times options conn simple-cfg entity-count datom-count))
                                   connection-times
                                   (when (#{:all :connection} function)
                                     (log/info (str " Measuring connection times..."))
                                     (d/release conn)
                                     (measure-connection-time iterations unique-cfg simple-cfg entity-count datom-count))]
                               (concat query-times transaction-times connection-times))))]
     (d/delete-database unique-cfg)
     measurements)))

(defn time-statistics [times]
  (let [n (count times)
        sorted (sort times)
        mean (/ (apply + times) n)]
    {:mean mean
     :median (nth sorted (int (/ n 2)))
     :std (->> times
               (map #(* (- % mean) (- % mean)))
               (apply +)
               (* (/ 1.0 n))
               Math/sqrt)
     :min (first sorted)
     :max (last sorted)
     :count n
     :observations (vec times)}))

(defn requested-configs [{:keys [config-name history backend search-caches store-caches schema index] :as _options}]
  (if config-name
    [(get c/named-db-configs config-name)]
    (vec (for [index-type (if (= :all index)
                            (-> (methods di/default-index-config) keys set (disj :default))
                            [index])
               backend-type (if (= :all backend)
                              (-> (methods ds/default-config) keys set (disj :default))
                              [backend])
               keep-history (if (= :all history)
                              [true false]
                              [history])
               schema-flexibility (if (= :all schema)
                                    [:read :write]
                                    [schema])
               search-cache search-caches
               store-cache store-caches]
           {:index index-type
            :store {:backend backend-type}
            :keep-history? keep-history
            :search-cache-size search-cache
            :store-cache-size store-cache
            :schema-flexibility schema-flexibility}))))

(defn get-measurements
  ([options] (get-measurements options (requested-configs options) {}))
  ([options configs] (get-measurements options configs {}))
  ([{:keys [db-entity-counts db-samples] :as options} configs specified-fn]
   (->> (for [cfg configs
              entity-count db-entity-counts]
          (let [datom-count (* entity-count (count c/schema))
                simple-cfg (-> cfg
                               (assoc :backend (get-in cfg [:store :backend]))
                               (update :index name)
                               (dissoc :store))]
            (log/info "Get measurements for DB of size" datom-count "and config:" simple-cfg)
            (->> (range db-samples)
                 (mapcat #(do (log/debug (str "- db instance no. " % ))
                              (measure-performance-full entity-count options cfg specified-fn)))
                 doall)))
        (apply concat)
        (group-by :context)
        (map (fn [[context group]]
               {:context context :time (time-statistics (map :time group))})))))
