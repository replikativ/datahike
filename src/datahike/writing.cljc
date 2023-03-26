(ns datahike.writing
  "Manage all state changes and access to state of durable store."
  (:require [datahike.connections :refer [delete-connection! connections]]
            [datahike.db :as db]
            [datahike.db.utils :as dbu]
            [datahike.index :as di]
            [datahike.store :as ds]
            [datahike.tools :as dt]
            [datahike.core :as core]
            [datahike.config :as dc]
            [konserve.core :as k]
            [taoensso.timbre :as log]
            [hasch.core :refer [uuid]]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async :refer [poll!]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]))

;; mapping to storage

(defn stored-db? [obj]
  ;; TODO use proper schema to match?
  (let [keys-to-check [:eavt-key :aevt-key :avet-key :schema :rschema
                       :system-entities :ident-ref-map :ref-ident-map :config
                       :max-tx :max-eid :op-count :hash :meta]]
    (= (count (select-keys obj keys-to-check))
       (count keys-to-check))))

(defn flush-pending-writes [store sync?]
  (if sync?
    (when-let [pending-futures (:pending-writes (:storage store))]
      ;; spin lock
      (loop [pfs @pending-futures]
        (let [f (first pfs)]
          (when f
            (let [fv (poll! f)]
              (if fv
                (recur (rest pfs))
                (do (Thread/sleep 1)
                    (recur pfs)))))))
      (reset! (:pending-writes (:storage store)) []))
    (go-try-
     (when-let [pending-futures (:pending-writes (:storage store))]
       (loop [[f & r] @pending-futures]
         (when f (<?- f) (recur r)))
       (reset! (:pending-writes (:storage store)) [])))))

(defn db->stored
  "Maps memory db to storage layout and flushes dirty indices."
  [db flush?]
  (when-not (dbu/db? db)
    (dt/raise "Argument is not a database."
              {:type     :argument-is-not-a-db
               :argument db}))
  (let [{:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                schema rschema system-entities ident-ref-map ref-ident-map config
                max-tx max-eid op-count hash meta store]} db
        backend                                           (di/konserve-backend (:index config) store)
        not-in-memory?                                    (not= :mem (-> config :store :backend))
        flush! (and flush? not-in-memory?)]
    (merge
     {:schema          schema
      :rschema         rschema
      :system-entities system-entities
      :ident-ref-map   ident-ref-map
      :ref-ident-map   ref-ident-map
      :config          config
      :meta            meta
      :hash            hash
      :max-tx          max-tx
      :max-eid         max-eid
      :op-count        op-count
      :eavt-key        (cond-> eavt flush! (di/-flush backend))
      :aevt-key        (cond-> aevt flush! (di/-flush backend))
      :avet-key        (cond-> avet flush! (di/-flush backend))}
     (when (:keep-history? config)
       {:temporal-eavt-key (cond-> temporal-eavt flush! (di/-flush backend))
        :temporal-aevt-key (cond-> temporal-aevt flush! (di/-flush backend))
        :temporal-avet-key (cond-> temporal-avet flush! (di/-flush backend))}))))

(defn stored->db
  "Constructs in-memory db instance from stored map value."
  [stored-db store]
  (let [{:keys [eavt-key aevt-key avet-key
                temporal-eavt-key temporal-aevt-key temporal-avet-key
                schema rschema system-entities ref-ident-map ident-ref-map
                config max-tx max-eid op-count hash meta]
         :or   {op-count 0}} stored-db
        empty              (db/empty-db nil config store)]
    (assoc empty
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
           :store store)))

(defn branch-heads-as-commits [store parents]
  (set (doall (for [p parents]
                (if-not (keyword? p) p
                        (let [{{:keys [datahike/commit-id]} :meta :as old-db}
                              (k/get store p nil {:sync? true})]
                          (when-not old-db
                            (dt/raise "Parent does not exist in store."
                                      {:type   :parent-does-not-exist-in-store
                                       :parent p}))
                          commit-id))))))

(defn create-commit-id [db]
  (let [{:keys [hash max-tx max-eid config]
         {:keys [parents]} :meta} db]
    (if (:crypto-hash? config)
      (uuid [hash max-tx max-eid parents])
      (uuid))))

(defn commit!
  ([store config db parents]
   (commit! store config db parents true))
  ([store config db parents sync?]
   (async+sync sync? *default-sync-translation*
               (go-try-
                (let [parents       (or parents #{(get config :branch)})
                      parents       (branch-heads-as-commits store parents)
                      cid           (create-commit-id db)
                      db            (-> db
                                        (assoc-in [:meta :datahike/commit-id] cid)
                                        (assoc-in [:meta :datahike/parents] parents))
                      db-to-store   (db->stored db true)
                      _             (<?- (flush-pending-writes store sync?))
                      commit-log-op (k/assoc store cid db-to-store {:sync? sync?})
                      branch-op     (k/assoc store (:branch config) db-to-store {:sync? sync?})]
                  (<?- commit-log-op)
                  (<?- branch-op)
                  db)))))

(defn update-and-flush-db
  ([connection tx-data tx-meta update-fn]
   (update-and-flush-db connection tx-data tx-meta update-fn nil))
  ([connection tx-data tx-meta update-fn parents]
   (update-and-flush-db connection tx-data tx-meta update-fn parents false))
  ([connection tx-data tx-meta update-fn parents commit?]
   (let [{:keys [db-after]
          {:keys [db/txInstant]}
          :tx-meta
          :as   tx-report}     @(update-fn connection tx-data tx-meta)
         {:keys [config meta]} db-after
         meta                  (assoc meta :datahike/updated-at txInstant)
         db                    (assoc db-after :meta meta)
         store                 (:store @(:wrapped-atom connection))
         db                    (if commit? (commit! store config db parents false) db)]
     (reset! connection db)
     tx-report
     (if commit?
       (assoc-in tx-report [:tx-meta :db/commitId]
                 (get-in db [:meta :datahike/commit-id]))
       tx-report))))

(defprotocol PDatabaseManager
  (-create-database [config opts])
  (-delete-database [config])
  (-database-exists? [config]))

(extend-protocol PDatabaseManager
  String
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

  (-create-database [config & deprecated-config]
    (let [{:keys [keep-history?] :as config} (dc/load-config config deprecated-config)
          store-config (:store config)
          store (ds/add-cache-and-handlers (ds/empty-store store-config) config)
          stored-db (k/get store :db nil {:sync? true})
          _ (when stored-db
              (dt/raise "Database already exists."
                        {:type :db-already-exists :config store-config}))
          {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                  schema rschema system-entities ref-ident-map ident-ref-map
                  config max-tx max-eid op-count hash meta] :as db}
          (db/empty-db nil config store)
          backend (di/konserve-backend (:index config) store)
          cid (create-commit-id db)
          meta (assoc meta :datahike/commit-id cid)
          db-to-store (merge {:schema          schema
                              :max-tx          max-tx
                              :max-eid         max-eid
                              :op-count        op-count
                              :hash            hash
                              :rschema         rschema
                              :system-entities system-entities
                              :ident-ref-map   ident-ref-map
                              :ref-ident-map   ref-ident-map
                              :config          (update config :initial-tx (comp not empty?))
                              :meta            meta
                              :eavt-key        (di/-flush eavt backend)
                              :aevt-key        (di/-flush aevt backend)
                              :avet-key        (di/-flush avet backend)}
                             (when keep-history?
                               {:temporal-eavt-key (di/-flush temporal-eavt backend)
                                :temporal-aevt-key (di/-flush temporal-aevt backend)
                                :temporal-avet-key (di/-flush temporal-avet backend)}))]
      (k/assoc store :branches #{:db} {:sync? true})
      (k/assoc store cid db-to-store {:sync? true})
      (k/assoc store :db db-to-store {:sync? true})
      (ds/release-store store-config store)
      config))

  (-delete-database [config]
    (let [config (dc/load-config config {})
          config-store-id (ds/store-identity (:store config))
          active-conns (filter (fn [[store-id _branch]]
                                 (= store-id config-store-id))
                               (keys @connections))]
      (doseq [conn active-conns]
        (log/warn "Deleting database without releasing all connections first: " conn "."
                  "All connections will be released now, but this cannot be ensured for remote readers.")
        (delete-connection! conn))
      (ds/delete-store (:store config)))))

;; public API

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

(defn transact! [connection {:keys [tx-data tx-meta]}]
  (log/debug "Transacting" (count tx-data) " objects with meta: " tx-meta)
  (log/trace "Transaction data" tx-data)
  (update-and-flush-db connection tx-data tx-meta core/transact))

(defn load-entities [connection entities]
  (log/debug "Loading" (count entities) " entities.")
  (update-and-flush-db connection entities nil core/load-entities))
