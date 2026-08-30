(ns datahike.http.pg
  "Optional pg-datahike listener over the standalone server's catalog.

   The HTTP server owns database connections and their lifecycle. pg-datahike
   receives the same connections under catalog names and remains responsible
   only for the PostgreSQL protocol surface."
  (:require
   [datahike.api :as d]
   [datahike.connections :as connections]
   [datahike.http.system :as system]
   [datahike.pg.server :as pg]
   [datahike.store :as store]
   [replikativ.logging :as log]))

(defn- deep-merge [left right]
  (merge-with (fn [a b]
                (if (and (map? a) (map? b))
                  (deep-merge a b)
                  b))
              left right))

(defn- redacted-paths
  ([value] (redacted-paths [] value))
  ([path value]
   (cond
     (map? value)
     (mapcat (fn [[key child]] (redacted-paths (conj path key) child)) value)

     (coll? value)
     (mapcat (fn [[index child]] (redacted-paths (conj path index) child))
             (map-indexed vector value))

     (= "REDACTED" value) [path]
     :else [])))

(defn- store-id [config]
  (some-> config :store store/store-identity str))

(defn- effective-config [{:keys [database-overrides]} entry]
  (let [base     (:config entry)
        override (or (get database-overrides (:name entry))
                     (get database-overrides (:store-id entry))
                     (when-let [name (:name entry)]
                       (get database-overrides (keyword name)))
                     {})
        config   (deep-merge base override)
        unresolved (vec (redacted-paths config))]
    (when (seq unresolved)
      (throw (ex-info
              (str "Database " (pr-str (:name entry))
                   " needs deployment-side secret overrides before pg-datahike can connect")
              {:type :datahike.http/unresolved-database-secrets
               :database (:name entry)
               :store-id (:store-id entry)
               :paths unresolved})))
    config))

(defn- connect! [connections config]
  (binding [connections/*connections* connections]
    (d/connect config)))

(defn- release! [connections conn]
  (binding [connections/*connections* connections]
    (d/release conn)))

(defn- active-entries [config]
  (filter #(= :active (:state %))
          (system/entries (get config system/conn-key))))

(defn- reject-independent-provisioning! [listener-config]
  (when-let [keys (seq (filter #(contains? listener-config %)
                               [:database-template
                                :on-create-database
                                :on-delete-database]))]
    (throw (ex-info
            "pg-datahike database provisioning must use the shared system catalog"
            {:type :datahike.http/independent-pg-provisioning
             :keys (vec keys)}))))

(defn start!
  "Start the configured pg-datahike listener, or return nil when absent."
  [config connections]
  (when-let [listener-config (let [listener-config (:pg-listener config)]
                               (when (and listener-config
                                          (not (false? (:enabled? listener-config))))
                                 listener-config))]
    (when-not (get config system/conn-key)
      (throw (ex-info ":pg-listener requires :system-db"
                      {:type :datahike.http/missing-system-database})))
    (reject-independent-provisioning! listener-config)
    (log/warn :datahike/pg-listener-beta
              "The pg-datahike listener is beta and does not yet provide the full PostgreSQL surface")
    (let [opened   (atom {})
          runtime  (atom nil)
          validate-name!
          (fn [{:keys [name store-id]}]
            (when-let [existing (get @opened name)]
              (when-not (= store-id (:store-id existing))
                (throw
                 (ex-info
                  (str "PostgreSQL database name " (pr-str name)
                       " identifies more than one catalog store")
                  {:type :datahike.http/duplicate-pg-database-name
                   :database name
                   :store-ids [(:store-id existing) store-id]})))))
          add!     (fn [entry]
                     (let [name (:name entry)
                           id   (:store-id entry)]
                       (locking opened
                         (validate-name! entry)
                         (if-let [existing (get @opened name)]
                           (:conn existing)
                           (let [config (effective-config listener-config entry)
                                 conn   (connect! connections config)]
                             (try
                               (when-let [server @runtime]
                                 (pg/add-database! server name conn))
                               (swap! opened assoc name {:conn conn
                                                         :config config
                                                         :store-id id})
                               conn
                               (catch Throwable e
                                 (release! connections conn)
                                 (throw e))))))))
          remove!  (fn [config]
                     (locking opened
                       (let [id (store-id config)
                             [name {:keys [conn]}]
                             (some (fn [[name entry]]
                                     (when (= id (:store-id entry))
                                       [name entry]))
                                   @opened)]
                         (when name
                           (when-let [server @runtime]
                             (pg/remove-database! server name))
                           (swap! opened dissoc name)
                           (release! connections conn)))))
          add-event! (fn [entry event]
                       (try
                         (add! entry)
                         (catch Throwable e
                           ;; The physical database and durable catalog already
                           ;; exist. Do not turn that success into a misleading
                           ;; failed HTTP create/rollback; expose the degraded
                           ;; listener loudly and retry on the next server start.
                           (log/error :datahike/pg-catalog-add-failed
                                      (ex-message e)
                                      {:event event
                                       :database (:name entry)
                                       :store-id (:store-id entry)
                                       :error-class (.getName (class e))}))))
          listener (fn [{:keys [event config]}]
                     (case event
                       :creating
                       (locking opened
                         (validate-name!
                          {:name (or (:name config) (store-id config))
                           :store-id (store-id config)}))

                       :created
                       (add-event! {:name (or (:name config) (store-id config))
                                    :store-id (store-id config)
                                    :config config}
                                   event)

                       :deleting (remove! config)

                       :delete-cancelled
                       (add-event! {:name (or (:name config) (store-id config))
                                    :store-id (store-id config)
                                    :config config}
                                   event)

                       nil))]
      (try
        (doseq [entry (active-entries config)]
          (add! entry))
        (let [registry (into {} (map (fn [[name {:keys [conn]}]] [name conn]) @opened))
              options  (dissoc listener-config :database-overrides :enabled?)
              server   (pg/start-server registry options)
              _        (reset! runtime server)
              subscription (system/subscribe! config listener)]
          {:server server
           :subscription subscription
           :opened opened
           :config config
           :connections connections})
        (catch Throwable e
          (when-let [server @runtime]
            (try
              (pg/stop-server server)
              (catch Throwable _)))
          (doseq [[_ {:keys [conn]}] @opened]
            (release! connections conn))
          (throw e))))))

(defn stop! [{:keys [server subscription config opened connections]}]
  (when subscription
    (system/unsubscribe! config subscription))
  (try
    (when server
      (pg/stop-server server))
    (finally
      (when opened
        (doseq [[_ {:keys [conn]}] @opened]
          (release! connections conn)))))
  nil)
