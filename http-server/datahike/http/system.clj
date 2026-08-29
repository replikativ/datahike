(ns datahike.http.system
  "The standalone server's durable catalog and permission database.

   Catalog configs are deliberately redacted before storage: credentials stay
   in deployment configuration, not in the database they protect."
  (:require
   [clojure.edn :as edn]
   [datahike.api :as d]
   [datahike.http.routes :as routes]
   [datahike.store :as store]
   [eacl.datahike.schema :as eschema]))

(def register-key ::register!)
(def prepare-register-key ::prepare-register!)
(def prepare-delete-key ::prepare-delete!)
(def delete-key ::delete!)
(def cancel-delete-key ::cancel-delete!)
(def conn-key ::conn)
(def listeners-key ::listeners)

(defn subscribe!
  "Subscribe to catalog lifecycle events. Returns an opaque subscription id."
  [config listener]
  (let [subscription (random-uuid)]
    (swap! (get config listeners-key) assoc subscription listener)
    subscription))

(defn unsubscribe! [config subscription]
  (when-let [listeners (get config listeners-key)]
    (swap! listeners dissoc subscription))
  nil)

(defn- notify! [listeners event config principal]
  (doseq [listener (vals @listeners)]
    (listener {:event event :config config :principal principal})))

(defn prepare-register!
  "Let catalog consumers reject a create before physical storage is changed."
  [listeners config principal]
  (notify! listeners :creating config principal)
  config)

(def catalog-schema
  [{:db/ident       :datahike.system.database/id
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}
   {:db/ident       :datahike.system.database/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/value}
   {:db/ident       :datahike.system.database/config
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :datahike.system.database/state
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index       true}
   {:db/ident       :datahike.system.database/created-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident       :datahike.system.database/created-by
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :datahike.system.database/deleted-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident       :datahike.system.database/deleted-by
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :datahike.system/principal
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}])

(def ^:private catalog-pull
  [:datahike.system.database/id
   :datahike.system.database/name
   :datahike.system.database/config
   :datahike.system.database/state
   :datahike.system.database/created-at
   :datahike.system.database/created-by
   :datahike.system.database/deleted-at
   :datahike.system.database/deleted-by])

(defn- configured-system-db [config]
  (when (contains? config :auth-db)
    (throw (ex-info ":auth-db was replaced by :system-db; rename this configuration key"
                    {:type :datahike.http/obsolete-auth-database})))
  (:system-db config))

(defn- deterministic-id [path]
  (java.util.UUID/nameUUIDFromBytes
   (.getBytes (str "datahike-system-db:" path))))

(defn- system-config [configured]
  (let [path (get-in configured [:store :path])]
    (-> (merge eschema/default-config configured)
        ;; The catalog is also the audit trail; allowing history to be disabled
        ;; would make its public lifecycle contract configuration-dependent.
        (assoc :keep-history? true)
        (update-in [:store :id]
                   #(or % (if path
                            (deterministic-id path)
                            (random-uuid)))))))

(defn- install-missing-schema! [conn]
  (let [installed (set (keys (d/schema @conn)))
        missing   (remove #(contains? installed (:db/ident %)) catalog-schema)]
    (when (seq missing)
      (d/transact conn (vec missing)))))

(defn- open! [configured]
  (let [cfg (system-config configured)]
    (if (d/database-exists? cfg)
      (doto (d/connect cfg)
        (install-missing-schema!))
      (do
        (d/create-database cfg)
        (doto (d/connect cfg)
          (d/transact (eschema/merge-schema catalog-schema)))))))

(defn- store-id [config]
  (some-> config :store store/store-identity str))

(defn- principal-id [principal]
  (or (:sub principal) "unknown"))

(defn- stored-config [config]
  (-> config
      (dissoc :remote-peer :writer)
      routes/redact
      pr-str))

(defn register!
  "Upsert an active catalog entry after a database was created successfully."
  [conn listeners config principal]
  (let [id (or (store-id config)
               (throw (ex-info "A cataloged database needs a stable store id"
                               {:type :datahike.http/missing-database-id})))
        lookup [:datahike.system.database/id id]
        exists? (some? (d/entity @conn lookup))
        now (java.util.Date.)
        who (principal-id principal)
        entry (cond-> {:datahike.system.database/id     id
                       :datahike.system.database/name   (or (:name config) id)
                       :datahike.system.database/config (stored-config config)
                       :datahike.system.database/state  :active}
                (not exists?)
                (assoc :datahike.system.database/created-at now
                       :datahike.system.database/created-by who))
        tx-data (cond-> [entry]
                  exists? (conj [:db.fn/retractAttribute lookup
                                 :datahike.system.database/deleted-at]
                                [:db.fn/retractAttribute lookup
                                 :datahike.system.database/deleted-by]))]
    (d/transact conn {:tx-data tx-data
                      :tx-meta {:datahike.system/principal who}})
    (notify! listeners :created config principal)
    config))

(defn prepare-delete!
  "Mark a database as deleting and let listeners release live resources."
  [conn listeners config principal]
  (let [id (store-id config)
        lookup [:datahike.system.database/id id]
        who (principal-id principal)]
    (when id
      (when-not (d/entity @conn lookup)
        (register! conn listeners config principal))
      (d/transact conn
                  {:tx-data [{:db/id lookup
                              :datahike.system.database/state :deleting}]
                   :tx-meta {:datahike.system/principal who}})
      (try
        (notify! listeners :deleting config principal)
        (catch Throwable e
          (d/transact conn
                      {:tx-data [{:db/id lookup
                                  :datahike.system.database/state :active}]
                       :tx-meta {:datahike.system/principal who}})
          (try
            (notify! listeners :delete-cancelled config principal)
            (catch Throwable _))
          (throw e)))))
  config)

(defn mark-deleted!
  "Soft-delete a catalog entry after its physical database was deleted."
  [conn listeners config principal]
  (let [id (store-id config)
        lookup [:datahike.system.database/id id]
        who (principal-id principal)]
    (when id
      ;; A database created before the catalog may not have an entry. Register
      ;; enough identity/config to make its deletion visible before marking it.
      (when-not (d/entity @conn lookup)
        (register! conn listeners config principal))
      (d/transact conn
                  {:tx-data [{:db/id lookup
                              :datahike.system.database/state :deleted
                              :datahike.system.database/deleted-at (java.util.Date.)
                              :datahike.system.database/deleted-by who}]
                   :tx-meta {:datahike.system/principal who}})))
  (when (store-id config)
    (notify! listeners :deleted config principal))
  nil)

(defn cancel-delete!
  "Restore an active entry and its listeners when physical deletion failed."
  [conn listeners config principal]
  (let [id (store-id config)
        who (principal-id principal)]
    (when id
      (d/transact conn
                  {:tx-data [{:db/id [:datahike.system.database/id id]
                              :datahike.system.database/state :active}]
                   :tx-meta {:datahike.system/principal who}})
      (notify! listeners :delete-cancelled config principal)))
  nil)

(defn- parse-stored-config [value]
  (try
    (edn/read-string value)
    (catch Exception _ nil)))

(defn entries
  "Catalog entries as API data, newest state included."
  [conn]
  (->> (d/q '[:find [?e ...]
              :where [?e :datahike.system.database/id]]
            @conn)
       (map #(d/pull @conn catalog-pull %))
       (map (fn [entry]
              {:store-id   (:datahike.system.database/id entry)
               :name       (:datahike.system.database/name entry)
               :config     (parse-stored-config (:datahike.system.database/config entry))
               :state      (:datahike.system.database/state entry)
               :created-at (:datahike.system.database/created-at entry)
               :created-by (:datahike.system.database/created-by entry)
               :deleted-at (:datahike.system.database/deleted-at entry)
               :deleted-by (:datahike.system.database/deleted-by entry)}))
       (sort-by (juxt :name :store-id))
       vec))

(defn- visible? [config principal {:keys [store-id]}]
  (if-let [authorize (:authorize config)]
    (authorize {:op :read
                :principal principal
                :db {:store-id (parse-uuid store-id) :branch :db}
                :payload nil})
    true))

(defn routes
  "GET /databases for active entries the caller may read."
  [{::keys [conn] :as config}]
  (when conn
    [["/databases"
      {:get {:summary "List databases visible to the caller."
             :metric-op :read
             :handler
             (fn [{principal :datahike/principal}]
               (try
                 {:status 200
                  :body (->> (entries conn)
                             (filter #(= :active (:state %)))
                             (filter #(visible? config principal %))
                             vec)}
                 (catch Exception e
                   (routes/error-response e))))}}]]))

(defn configure
  "Open the configured system database and install catalog callbacks."
  [config]
  (if-let [configured (configured-system-db config)]
    (let [conn (open! configured)
          listeners (atom {})]
      (-> config
          (assoc :system-db configured
                 conn-key conn
                 listeners-key listeners
                 prepare-register-key (partial prepare-register! listeners)
                 register-key (partial register! conn listeners)
                 prepare-delete-key (partial prepare-delete! conn listeners)
                 delete-key (partial mark-deleted! conn listeners)
                 cancel-delete-key (partial cancel-delete! conn listeners))))
    config))

(defn close! [config]
  (when-let [conn (get config conn-key)]
    (d/release conn)))
