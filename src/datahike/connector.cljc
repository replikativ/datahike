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
            [taoensso.timbre :as log]
            [clojure.spec.alpha :as s]
            [clojure.core.async :refer [go <!]])
  (:import [clojure.lang IDeref IAtom IMeta ILookup]))

;; mapping to storage

(defn stored-db? [obj]
  ;; TODO use proper schema to match?
  (let [keys-to-check [:eavt-key :aevt-key :avet-key :schema :rschema
                       :system-entities :ident-ref-map :ref-ident-map :config
                       :max-tx :max-eid :op-count :hash :meta]]
    (= (count (select-keys obj keys-to-check))
       (count keys-to-check))))

(defn db->stored
  "Maps memory db to storage layout and flushes dirty indices."
  [db flush?]
  (when-not (db/db? db)
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

;; connection

(declare deref-db)

(deftype Connection [wrapped-atom]
  IDeref
  (deref [conn] (deref-db conn))
  ;; These interfaces should not be used from the outside, they are here to keep
  ;; the internal interfaces lean and working.
  ILookup
  (valAt [c k] (if (= k :wrapped-atom) wrapped-atom nil))
  IAtom
  (swap [_ f] (swap! wrapped-atom f))
  (swap [_ f arg] (swap! wrapped-atom f arg))
  (swap [_ f arg1 arg2] (swap! wrapped-atom f arg1 arg2))
  (swap [_ f arg1 arg2 args] (apply swap! wrapped-atom f arg1 arg2 args))
  (compareAndSet [_ oldv newv] (compare-and-set! wrapped-atom oldv newv))
  (reset [_ newval] (reset! wrapped-atom newval))

  IMeta
  (meta [_] (meta wrapped-atom)))

(defn deref-db [^Connection conn]
  (let [wrapped-atom (.-wrapped-atom conn)]
    (if (not (t/streaming? (get @wrapped-atom :transactor)))
      (let [store  (:store @wrapped-atom)
            stored (k/get store (:branch (:config @wrapped-atom)) nil {:sync? true})]
        (log/trace "Fetched db for deref: " (:config stored))
        (stored->db stored store))
      @wrapped-atom)))

(defn conn-from-db
  "Creates a mutable reference to a given immutable database. See [[create-conn]]."
  [db]
  (Connection. (atom db :meta {:listeners (atom {})})))

(s/def ::connection #(and (instance? Connection %)
                          (not= @(:wrapped-atom %) :deleted)))

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

(defn commit! [store config db parents]
  (let [parents (or parents #{(get config :branch)})
        parents (branch-heads-as-commits store parents)
        cid (create-commit-id db)
        db (-> db
               (assoc-in [:meta :datahike/commit-id] cid)
               (assoc-in [:meta :datahike/parents] parents))
        db-to-store (db->stored db true)]
    (k/assoc store cid db-to-store {:sync? true})
    (k/assoc store (:branch config) db-to-store {:sync? true})
    db))

(defn update-and-flush-db
  ([connection tx-data tx-meta update-fn]
   (update-and-flush-db connection tx-data tx-meta update-fn nil))
  ([connection tx-data tx-meta update-fn parents]
   (let [{:keys [db/noCommit]} tx-meta
         {:keys [db-after]
          {:keys [db/txInstant]}
          :tx-meta
          :as   tx-report}     @(update-fn connection tx-data tx-meta)
         {:keys [config meta]} db-after
         meta                  (assoc meta :datahike/updated-at txInstant)
         db                    (assoc db-after :meta meta)
         store                 (:store @(:wrapped-atom connection))
         db                    (if noCommit db (commit! store config db parents))]
     (reset! connection db)
     (if noCommit
       tx-report
       (assoc-in tx-report [:tx-meta :db/commitId] (get-in db [:meta :datahike/commit-id]))))))

(defn transact!
  [connection {:keys [tx-data tx-meta]}]
  {:pre [(d/conn? connection)]}
  (let [p (throwable-promise)]
    (go
      (let [tx-report (<! (t/send-transaction! (:transactor @(:wrapped-atom connection)) tx-data tx-meta 'datahike.core/transact))]
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
        _ (log/debug "Transacting" (count (:tx-data arg)) " objects with arguments: " (dissoc arg :tx-data))
        _ (log/trace "Transaction data" (:tx-data arg))]
    (try
      (deref (transact! connection arg))
      (catch Exception e
        (log/errorf "Error during transaction %s" (.getMessage e))
        (throw (.getCause e))))))

(defn load-entities [connection entities]
  (let [p (throwable-promise)]
    (go
      (let [tx-report (<! (t/send-transaction! (:transactor @(:wrapped-atom connection)) entities nil 'datahike.core/load-entities))]
        (deliver p tx-report)))
    p))

(def connections (atom {}))

(defn get-connection [conn-id]
  (when-let [conn (get-in @connections [conn-id :conn])]
    (swap! connections update-in [conn-id :count] inc)
    conn))

(defn add-connection! [conn-id conn]
  (swap! connections assoc conn-id {:conn conn :count 1}))

(defn delete-connection! [conn-id]
  (reset! (get-connection conn-id) :deleted)
  (swap! connections dissoc conn-id))

(defn release [connection]
  (let [db @(:wrapped-atom connection)
        conn-id (conj (ds/store-identity (get-in db [:config :store]))
                      (get-in db [:config :branch]))]
    (if-not (get @connections conn-id)
      (log/info "Connection already released." conn-id)
      (let [new-conns (swap! connections update-in [conn-id :count] dec)]
        (when (zero? (get-in new-conns [conn-id :count]))
          (delete-connection! conn-id)
          (t/shutdown (:transactor db))
          nil)))))

;; deprecation begin
(defprotocol IConfiguration
  (-connect [config])
  (-create-database [config opts])
  (-delete-database [config])
  (-database-exists? [config]))

(defn config-merge [stored-config config]
  (let [merged-config (dt/deep-merge config stored-config)]
    (dt/deep-merge merged-config (select-keys config #{:branch}))))

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
          store-id (ds/store-identity store-config)
          conn-id (conj store-id (:branch config))]
      (if-let [conn (get-connection conn-id)]
        conn
        (let [raw-store (ds/connect-store store-config)
              _         (when-not raw-store
                          (dt/raise "Backend does not exist." {:type   :backend-does-not-exist
                                                               :config store-config}))
              store     (ds/add-cache-and-handlers raw-store config)
              stored-db (k/get store (:branch config) nil {:sync? true})
              _         (when-not stored-db
                          (ds/release-store store-config store)
                          (dt/raise "Database does not exist." {:type   :db-does-not-exist
                                                                :config config}))
              [config store stored-db]
              (let [intended-index (:index config)
                    stored-index   (get-in stored-db [:config :index])]
                (if-not (= intended-index stored-index)
                  (do
                    (log/warn (str "Stored index does not match configuration. Please set :index explicitly to " stored-index " in config. The default index is now :datahike/persistent-set. Using stored index setting now, but this might throw an error in the future."))
                    (let [config    (assoc config :index stored-index)
                          store     (ds/add-cache-and-handlers raw-store config)
                          stored-db (k/get-in store [:db] nil {:sync? true})]
                      [config store stored-db]))
                  [config store stored-db]))
              config    (config-merge (:config stored-db) config)
              conn      (conn-from-db (stored->db (assoc stored-db :config config) store))]
          (swap! (:wrapped-atom conn) assoc :transactor
                 (t/create-transactor (:transactor config) conn update-and-flush-db))
          (add-connection! conn-id conn)
          conn))))

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
                              :config          config
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
