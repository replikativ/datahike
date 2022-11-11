(ns ^:no-doc datahike.connector
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [datahike.index :as di]
            [datahike.store :as ds]
            [datahike.config :as dc]
            [datahike.tools :as dt :refer [throwable-promise]]
            [datahike.transactor :as t]
            [konserve.core :as k]
            [hasch.core :refer [uuid]]
            [superv.async :refer [<?? S]]
            [taoensso.timbre :as log]
            [clojure.spec.alpha :as s]
            [clojure.core.async :refer [go <!]])
  (:import  [clojure.lang Atom]))

(s/def ::connection #(instance? Atom %))

(defn stored-db? [obj]
  (let [keys-to-check [:eavt-key :aevt-key :avet-key :schema :rschema
                       :system-entities :ident-ref-map :ref-ident-map :config
                       :max-tx :max-eid :op-count :hash :meta]]
    (= (count (select-keys obj keys-to-check))
       (count keys-to-check))))

(defn update-and-flush-db
  ([connection tx-data tx-meta update-fn]
   (let [{:keys [store config]} @connection
         {{:keys [datahike/commit-id]} :meta :as old-db} (k/get store (:branch config) nil {:sync? true})]
     (when old-db
       (k/assoc store commit-id old-db {:sync? true}))
     (update-and-flush-db connection tx-data tx-meta update-fn #{commit-id})))
  ([connection tx-data tx-meta update-fn parents]
   (let [{:keys                  [db-after]
          {:keys [db/txInstant]} :tx-meta
          :as                    tx-report}       @(update-fn connection tx-data tx-meta)
         {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                 schema rschema system-entities ident-ref-map ref-ident-map config
                 max-tx max-eid op-count hash meta]}
         db-after
         store                 (:store @connection)
         backend               (di/konserve-backend (:index config) store)
         ;; flush for in-memory backends would only make sense if multiple processes access the db
         ;; TODO: update can also be skipped for external stores when single process writes unless memory should be freed up
         ;;       -> add options to config
         backend?              (not= :mem (-> config :store :backend))
         eavt-flushed          (cond-> eavt backend? (di/-flush backend))
         aevt-flushed          (cond-> aevt backend? (di/-flush backend))
         avet-flushed          (cond-> avet backend? (di/-flush backend))
         keep-history?         (:keep-history? config)
         temporal-eavt-flushed (when keep-history? (cond-> temporal-eavt backend? (di/-flush backend)))
         temporal-aevt-flushed (when keep-history? (cond-> temporal-aevt backend? (di/-flush backend)))
         temporal-avet-flushed (when keep-history? (cond-> temporal-avet backend? (di/-flush backend)))]
     (k/assoc store :db
              (merge
               {:schema          schema
                :rschema         rschema
                :system-entities system-entities
                :ident-ref-map   ident-ref-map
                :ref-ident-map   ref-ident-map
                :config          config
                :meta            (assoc meta
                                        :datahike/parents parents
                                        :datahike/updated-at txInstant
                                        :datahike/commit-id (uuid [hash max-tx max-eid]))
                :hash            hash
                :max-tx          max-tx
                :max-eid         max-eid
                :op-count        op-count
                :eavt-key        eavt-flushed
                :aevt-key        aevt-flushed
                :avet-key        avet-flushed}
               (when keep-history?
                 {:temporal-eavt-key temporal-eavt-flushed
                  :temporal-aevt-key temporal-aevt-flushed
                  :temporal-avet-key temporal-avet-flushed}))
              {:sync? true})
     (when backend?
       (reset! connection (assoc db-after
                                 :eavt eavt-flushed
                                 :aevt aevt-flushed
                                 :avet avet-flushed
                                 :temporal-eavt temporal-eavt-flushed
                                 :temporal-aevt temporal-aevt-flushed
                                 :temporal-avet temporal-avet-flushed)))
     tx-report)))

(defn transact!
  [connection {:keys [tx-data tx-meta]}]
  {:pre [(d/conn? connection)]}
  (let [p (throwable-promise)]
    (go
      (let [tx-report (<! (t/send-transaction! (:transactor @connection) tx-data tx-meta 'datahike.core/transact))]
        (deliver p tx-report)))
    p))

(defn transact [connection arg-map]
  (let [arg (cond
              (and (map? arg-map) (contains? arg-map :tx-data)) arg-map
              (vector? arg-map) {:tx-data arg-map}
              (seq? arg-map) {:tx-data arg-map}
              :else (dt/raise "Bad argument to transact, expected map with :tx-data as key.
                               Vector and sequence are allowed as argument but deprecated."
                              {:error :transact/syntax :argument arg-map}))
        _ (log/debug "Transacting with arguments: " arg)]
    (try
      (deref (transact! connection arg))
      (catch Exception e
        (log/errorf "Error during transaction %s" (.getMessage e))
        (throw (.getCause e))))))

(defn load-entities [connection entities]
  (let [p (throwable-promise)]
    (go
      (let [tx-report (<! (t/send-transaction! (:transactor @connection) entities nil 'datahike.core/load-entities))]
        (deliver p tx-report)))
    p))

(defn release [connection]
  (<?? S (t/shutdown (:transactor @connection)))
  (ds/release-store (get-in @connection [:config :store]) (:store @connection)))

;; deprecation begin
(defprotocol IConfiguration
  (-connect [config])
  (-create-database [config opts])
  (-delete-database [config])
  (-database-exists? [config]))

(extend-protocol IConfiguration
  String
  (-connect [uri]
    (-connect (dc/uri->config uri)))

  (-create-database [uri & opts]
    (apply -create-database (dc/uri->config uri) opts))

  (-delete-database [uri]
    (-delete-database (dc/uri->config uri)))

  (-database-exists? [uri]
    (-database-exists? (dc/uri->config uri)))

  clojure.lang.IPersistentMap
  (-database-exists? [config]
    (let [config (dc/load-config config)
          store-config (:store config)
          raw-store (ds/connect-store store-config)]
      (if (not (nil? raw-store))
        (let [store (ds/add-cache-and-handlers raw-store config)
              stored-db (k/get store :db nil {:sync? true})]
          (ds/release-store store-config store)
          (not (nil? stored-db)))
        (do
          (ds/release-store store-config raw-store)
          false))))

  (-connect [config]
    (let [config (dc/load-config config)
          _ (log/debug "Using config " (update-in config [:store] dissoc :password))
          store-config (:store config)
          raw-store (ds/connect-store store-config)
          _ (when-not raw-store
              (dt/raise "Backend does not exist." {:type :backend-does-not-exist
                                                   :config config}))
          store (ds/add-cache-and-handlers raw-store config)
          stored-db (k/get store (:branch config) nil {:sync? true})
          _ (when-not stored-db
              (ds/release-store store-config store)
              (dt/raise "Database does not exist." {:type :db-does-not-exist
                                                    :config config}))
          {:keys [eavt-key aevt-key avet-key
                  temporal-eavt-key temporal-aevt-key temporal-avet-key
                  schema rschema system-entities ref-ident-map ident-ref-map
                  config max-tx max-eid op-count hash meta]
           :or {op-count 0}} stored-db
          empty (db/empty-db nil config store)
          conn (d/conn-from-db (assoc empty
                                      :max-tx max-tx
                                      :max-eid max-eid
                                      :config config
                                      :meta meta
                                      :schema schema
                                      :hash hash
                                      :op-count op-count
                                      :eavt eavt-key
                                      :aevt aevt-key
                                      :avet avet-key
                                      :temporal-eavt temporal-eavt-key
                                      :temporal-aevt temporal-aevt-key
                                      :temporal-avet temporal-avet-key
                                      :rschema rschema
                                      :system-entities system-entities
                                      :ident-ref-map ident-ref-map
                                      :ref-ident-map ref-ident-map
                                      :store store))]
      (swap! conn assoc :transactor (t/create-transactor (:transactor config) conn update-and-flush-db))
      conn))

  (-create-database [config & deprecated-config]
    (let [{:keys [keep-history? initial-tx] :as config} (dc/load-config config deprecated-config)
          store-config (:store config)
          store (ds/add-cache-and-handlers (ds/empty-store store-config) config)
          stored-db (k/get store :db nil {:sync? true})
          _ (when stored-db
              (dt/raise "Database already exists."
                        {:type :db-already-exists :config store-config}))
          {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                  schema rschema system-entities ref-ident-map ident-ref-map
                  config max-tx max-eid op-count hash meta]}
          (db/empty-db nil config store)
          backend (di/konserve-backend (:index config) store)]
      (k/assoc store :branches #{:db})
      (k/assoc store :db
               (merge {:schema          schema
                       :max-tx          max-tx
                       :max-eid         max-eid
                       :op-count        op-count
                       :hash            hash
                       :rschema         rschema
                       :system-entities system-entities
                       :ident-ref-map   ident-ref-map
                       :ref-ident-map   ref-ident-map
                       :config          config
                       :meta            (assoc meta :datahike/commit-id (uuid [hash max-tx max-eid]))
                       :eavt-key        (di/-flush eavt backend)
                       :aevt-key        (di/-flush aevt backend)
                       :avet-key        (di/-flush avet backend)}
                      (when keep-history?
                        {:temporal-eavt-key (di/-flush temporal-eavt backend)
                         :temporal-aevt-key (di/-flush temporal-aevt backend)
                         :temporal-avet-key (di/-flush temporal-avet backend)}))
               {:sync? true})
      (ds/release-store store-config store)
      (when initial-tx
        (let [conn (-connect config)]
          (transact conn initial-tx)
          (release conn)))))

  (-delete-database [config]
    (let [config (dc/load-config config {})]
      (ds/delete-store (:store config)))))

(defn connect
  ([]
   (-connect {}))
  ([config]
   (-connect config)))
;;deprecation end

(defn create-database
  ([]
   (-create-database {} nil))
  ([config & opts]
   (-create-database config opts)))

(defn delete-database
  ([]
   (-delete-database {}))
  ;;deprecated
  ([config]
   ;; TODO log deprecation notice with #54
   (-delete-database config)))

(defn database-exists?
  ([]
   (-database-exists? {}))
  ([config]
   ;; TODO log deprecation notice with #54
   (-database-exists? config)))
