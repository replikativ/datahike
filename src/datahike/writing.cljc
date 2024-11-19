(ns datahike.writing
  "Manage all state changes and access to state of durable store."
  (:require [datahike.connections :refer [delete-connection! *connections*]]
            [datahike.db :as db]
            [datahike.db.transaction :as dbt]
            [datahike.db.utils :as dbu]
            [datahike.index :as di]
            [datahike.store :as ds]
            [datahike.tools :as dt]
            [datahike.core :as core]
            [datahike.config :as dc]
            [datahike.schema-cache :as sc]
            [konserve.core :as k]
            [taoensso.timbre :as log]
            [hasch.core :refer [uuid]]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async :refer [poll!]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]))

;; mapping to storage

(defn stored-db? [obj]
  ;; TODO use proper schema to match?
  (let [keys-to-check [:eavt-key :aevt-key :avet-key :config
                       :max-tx :max-eid :op-count :hash :meta]]
    (= (count (select-keys obj keys-to-check))
       (count keys-to-check))))

(defn flush-pending-writes [store sync?]
  (let [pending-writes (:pending-writes (:storage store))
        current-writes (atom nil)]
      ;; atomic extraction and reset
    (when pending-writes
      ;; at least as new as the current commit, maybe some more writes are included
      (swap! pending-writes (fn [old] (reset! current-writes old) [])))
    (if sync?
      (loop [pfs @current-writes]
        (let [f (first pfs)]
          (when f
            (let [fv (poll! f)]
              (if fv
                (recur (rest pfs))
                (do (Thread/sleep 1)
                    (recur pfs)))))))
      (go-try-
       (loop [[f & r] @current-writes]
         (when f (<?- f) (recur r)))))))

(defn db->stored
  "Maps memory db to storage layout and flushes dirty indices."
  [last-flushed db tx-data flush? sync?]
  (when-not (dbu/db? db)
    (dt/raise "Argument is not a database."
              {:type     :argument-is-not-a-db
               :argument db}))
  (let [{:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet
                schema rschema system-entities ident-ref-map ref-ident-map config
                max-tx max-eid op-count hash meta store]} db
        {:keys [keep-history? tx-log-size]} config
        tx-data (vec (concat (:tx-data last-flushed) tx-data))
        schema-meta {:schema schema
                     :rschema rschema
                     :system-entities system-entities
                     :ident-ref-map ident-ref-map
                     :ref-ident-map ref-ident-map}
        schema-meta-key (uuid schema-meta)
        backend                                           (di/konserve-backend (:index config) store)
        not-in-memory?                                    (not= :mem (-> config :store :backend))
        tx-log-too-big? (> (count tx-data) tx-log-size)
        flush! (and flush? not-in-memory? tx-log-too-big?)
        schema-meta-op (when-not (sc/write-cache-has? (:store config) schema-meta-key)
                         (sc/add-to-write-cache (:store config) schema-meta-key)
                         (k/assoc store schema-meta-key schema-meta {:sync? sync?}))
        _ (when-not (sc/cache-has? schema-meta-key)
            (sc/cache-miss schema-meta-key schema-meta))
        eavt (if flush! (di/-flush eavt backend) (:eavt-key last-flushed))
        aevt (if flush! (di/-flush aevt backend) (:aevt-key last-flushed))
        avet (if flush! (di/-flush avet backend) (:avet-key last-flushed))
        temporal-eavt (if (and keep-history? flush!) (di/-flush temporal-eavt backend) (:temporal-eavt-key last-flushed))
        temporal-aevt (if (and keep-history? flush!) (di/-flush temporal-aevt backend) (:temporal-aevt-key last-flushed))
        temporal-avet (if (and keep-history? flush!) (di/-flush temporal-avet backend) (:temporal-avet-key last-flushed))
        stored-tx-data (if tx-log-too-big? [] tx-data)
        db (merge
            {:schema-meta-key  schema-meta-key
             :tx-data          stored-tx-data
             :config          config
             :meta            meta
             :hash            hash
             :max-tx          max-tx
             :max-eid         max-eid
             :op-count        op-count
             :eavt-key        eavt
             :aevt-key        aevt
             :avet-key        avet}
            (when (:keep-history? config)
              {:temporal-eavt-key temporal-eavt
               :temporal-aevt-key temporal-aevt
               :temporal-avet-key temporal-avet}))
        last-flushed (if flush!
                       {:tx-data stored-tx-data
                        :eavt-key eavt
                        :aevt-key aevt
                        :avet-key avet
                        :temporal-eavt-key temporal-eavt
                        :temporal-aevt-key temporal-aevt
                        :temporal-avet-key temporal-avet}
                       (assoc last-flushed :tx-data tx-data))]
    [schema-meta-op last-flushed db]))

(defn stored->db
  "Constructs in-memory db instance from stored map value."
  [stored-db store]
  (let [{:keys [eavt-key aevt-key avet-key
                temporal-eavt-key temporal-aevt-key temporal-avet-key
                schema rschema system-entities ref-ident-map ident-ref-map
                config max-tx max-eid op-count hash meta schema-meta-key tx-data]
         :or   {op-count 0}} stored-db
        schema-meta (or (sc/cache-lookup schema-meta-key)
                        ;; not in store in case we load an old db where the schema meta data was inline
                        (when-let [schema-meta (k/get store schema-meta-key nil {:sync? true})]
                          (sc/cache-miss schema-meta-key schema-meta)
                          schema-meta))
        empty       (db/empty-db nil config store)]
    (reduce dbt/with-datom
            (merge
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
                    :store store)
             schema-meta)
            tx-data)))

(defn branch-heads-as-commits [store parents]
  (set (doall (for [p parents]
                (do
                  (when (nil? p)
                    (dt/raise "Parent cannot be nil." {:type :parent-cannot-be-nil
                                                       :parent p}))
                  (if-not (keyword? p) p
                          (let [{{:keys [datahike/commit-id]} :meta :as old-db}
                                (k/get store p nil {:sync? true})]
                            (when-not old-db
                              (dt/raise "Parent does not exist in store."
                                        {:type   :parent-does-not-exist-in-store
                                         :parent p}))
                            commit-id)))))))

(defn create-commit-id [db]
  (let [{:keys [hash max-tx max-eid meta]} db]
    (uuid [hash max-tx max-eid meta])))

(defn commit!
  ([last-flushed db-after tx-data parents]
   (commit! last-flushed db-after tx-data parents true))
  ([last-flushed db-after tx-data parents sync?]
   (async+sync sync? *default-sync-translation*
               (go-try-
                (let [{:keys [store config]} db-after
                      parents       (or parents #{(get config :branch)})
                      parents       (branch-heads-as-commits store parents)
                      cid           (create-commit-id db-after)
                      db            (-> db-after
                                        (assoc-in [:meta :datahike/commit-id] cid)
                                        (assoc-in [:meta :datahike/parents] parents))
                      [schema-meta-op last-flushed db-to-store]   (db->stored last-flushed db tx-data true sync?)
                      _             (<?- (flush-pending-writes store sync?))
                      commit-log-op (k/assoc store cid db-to-store {:sync? sync?})
                      branch-op     (k/assoc store (:branch config) db-to-store {:sync? sync?})]
                      ;; now wait for all the writes to complete
                  (when (and schema-meta-op (not sync?)) (<?- schema-meta-op))
                  (<?- commit-log-op)
                  (<?- branch-op)
                  [last-flushed db])))))

(defn complete-db-update [old tx-report]
  (let [{:keys [writer]} old
        {:keys [db-after]
         {:keys [db/txInstant]} :tx-meta} tx-report
        new-meta  (assoc (:meta db-after) :datahike/updated-at txInstant)
        db        (assoc db-after :meta new-meta :writer writer)
        tx-report (assoc tx-report :db-after db)]
    tx-report))

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
          schema-meta {:schema schema
                       :rschema rschema
                       :system-entities system-entities
                       :ident-ref-map ident-ref-map
                       :ref-ident-map ref-ident-map}
          schema-meta-key (uuid schema-meta)
          db-to-store (merge {:max-tx          max-tx
                              :max-eid         max-eid
                              :op-count        op-count
                              :hash            hash
                              :schema-meta-key schema-meta-key
                              :config          (update config :initial-tx (comp not empty?))
                              :meta            meta
                              :eavt-key        (di/-flush eavt backend)
                              :aevt-key        (di/-flush aevt backend)
                              :avet-key        (di/-flush avet backend)}
                             (when keep-history?
                               {:temporal-eavt-key (di/-flush temporal-eavt backend)
                                :temporal-aevt-key (di/-flush temporal-aevt backend)
                                :temporal-avet-key (di/-flush temporal-avet backend)}))]
      ;;we just created the first data base in this store, so the write cache is empty
      (k/assoc store schema-meta-key schema-meta {:sync? true})
      (sc/add-to-write-cache (:store config) schema-meta-key)
      (when-not (sc/cache-has? schema-meta-key)
        (sc/cache-miss schema-meta-key schema-meta))
      (flush-pending-writes store true)
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
                               (keys @*connections*))]
      (sc/clear-write-cache (:store config))
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

(defn transact! [old {:keys [tx-data tx-meta]}]
  (log/debug "Transacting" (count tx-data) "objects")
  (log/trace "Transaction data" tx-data  "with meta:" tx-meta)
  (complete-db-update old (core/with old tx-data tx-meta)))

(defn load-entities [old entities]
  (log/debug "Loading" (count entities) " entities.")
  (complete-db-update old (core/load-entities-with old entities nil)))