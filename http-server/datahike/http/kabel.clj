(ns datahike.http.kabel
  "Optional JWT-authenticated Kabel endpoint for the standalone server.

   This is a database transport, not an identity provider. The server validates
   a JWT issued elsewhere and stamps its claims onto every Kabel message as the
   principal. What a principal may do is the same question the HTTP routes ask,
   answered by the same `:authorize` policy (see `datahike.http.permissions`):
   a remote call to `datahike.kabel/dispatch` is a `:transact` on its store,
   `create-database` a `:create`, `delete-database` a `:delete`, and a sync
   subscription to a store's topic a `:read`. Without a policy every
   authenticated caller may do everything, as over HTTP."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [datahike.api :as d]
            [datahike.http.routes :as routes]
            [datahike.kabel.cbor-handlers :as cbor]
            [datahike.kabel.handlers :as handlers]
            [kabel.auth.websocket :as auth]
            [kabel.remote :as remote]
            [kabel.http-kit :refer [create-http-kit-handler!]]
            [kabel.peer :as peer]
            [konserve-sync.core :as sync]
            [konserve.core :as k]
            [konserve.store :as ks]
            [replikativ.logging :as log]
            [superv.async :refer [S <??]]))

(def default-peer-id
  "The stable peer id used when `:kabel :peer-id` is omitted. Browser writer
   configurations address this id, so a random id on every boot is not useful."
  #uuid "aaaaaaaa-0000-0000-0000-000000000001")

(defn validate-config
  "Validate and normalize the opt-in `:kabel` configuration. A Kabel endpoint
   always validates JWTs; omitting `:kabel` disables it."
  [config]
  (if-not (contains? config :kabel)
    config
    (let [kabel (:kabel config)
          fail  (fn [message data]
                  (throw (ex-info message
                                  (assoc data :type :datahike.kabel/invalid-config))))]
      (when-not (map? kabel)
        (fail ":kabel must be a map" {:kabel kabel}))
      (let [{:keys [port jwt]} kabel
            host    (or (:host kabel) (:host config) "127.0.0.1")
            peer-id (or (:peer-id kabel) default-peer-id)
            store   (or (:store kabel) {:backend :file :path "data/kabel"})]
        (when-not (and (integer? port) (< 0 port 65536))
          (fail ":kabel :port must be an integer from 1 through 65535" {:port port}))
        (when-not (and (string? host) (not (str/blank? host)))
          (fail ":kabel :host must be a nonblank string" {:host host}))
        (when-not (uuid? peer-id)
          (fail ":kabel :peer-id must be a UUID" {:peer-id peer-id}))
        (when-not (and (map? jwt) (:alg jwt)
                       (or (:secret jwt) (:public-key jwt) (:issuers jwt)))
          (fail ":kabel :jwt must pin an algorithm and verification key or issuer registry" {}))
        (when-not (contains? #{:memory :file} (:backend store))
          (fail "The first Kabel server supports :memory and :file stores"
                {:backend (:backend store)}))
        (when (and (= :file (:backend store))
                   (or (not (string? (:path store))) (str/blank? (:path store))))
          (fail "A Kabel :file store requires a nonblank :path" {}))
        (assoc config :kabel (assoc kabel :host host :peer-id peer-id :store store))))))

(defn- store-config-fn [{:keys [store]}]
  (fn [store-id _client-config]
    (case (:backend store)
      :memory {:backend :memory :id store-id}
      :file   {:backend :file
               :path (.getPath (io/file (:path store) (str store-id)))
               :id store-id})))

(defn- remote-name
  "The function a call names, as the symbol kabel resolves it to: kabel
   accepts the string spelling too, so the gate must see both the same way."
  [fn-name]
  (cond (symbol? fn-name) fn-name
        (string? fn-name) (symbol fn-name)
        :else nil))

(defn- remote-op
  "What one of Datahike's own remote functions does, for authorization; nil
   for any other name."
  [fn-name arg-map]
  (case (remote-name fn-name)
    datahike.kabel/dispatch        (if (= 'gc-storage! (get-in arg-map [:arg-map :op])) :admin :transact)
    datahike.kabel/create-database :create
    datahike.kabel/delete-database :delete
    nil))

(defn- remote-store-id
  "The store a remote call reaches; only a UUID counts as one."
  [fn-name arg-map]
  (let [id (case (remote-name fn-name)
             datahike.kabel/dispatch (:store-id arg-map)
             (get-in arg-map [:config :store :id]))]
    (when (uuid? id) id)))

(defn- topic-store-id
  "The store a sync topic belongs to: the store id itself, or the store
   behind a `:tx-report/scope-<id>` broadcast topic."
  [topic]
  (cond
    (uuid? topic) topic
    (and (keyword? topic) (= "tx-report" (namespace topic))
         (str/starts-with? (name topic) "scope-"))
    (parse-uuid (subs (name topic) (count "scope-")))
    :else nil))

(defn- allowed?
  "Ask `config`'s `:authorize` as the HTTP routes do: once per database the
   call reaches, with `:db nil` when it reaches none. Without a policy every
   authenticated principal may proceed."
  [config op principal store-id payload]
  (boolean
   (and principal
        (if-let [policy (:authorize config)]
          (policy {:op op :principal principal
                   :db (when store-id {:store-id store-id})
                   :payload payload})
          true))))

(defn authorize-remote
  "The gate for `kabel.remote/serve`, in the `kabel.authorize` shape.

   Datahike's own remote functions are asked for as the database operation
   they perform. Any other function the host registered on this process (its
   own domain operations, guarded the way it sees fit) is asked for as
   `{:op :invoke :fn-name … :db nil}`, so the built-in permissions allow it
   to server admins only and a custom `:authorize` decides for everyone else."
  [config]
  (fn [{:keys [principal fn-name arg-map]}]
    (let [sym (remote-name fn-name)]
      (cond
        (remote-op sym arg-map)
        (allowed? config (remote-op sym arg-map) principal (remote-store-id sym arg-map) arg-map)

        ;; Nothing else under Datahike's own namespace is served; refuse any
        ;; spelling of it rather than asking the host about it.
        (= "datahike.kabel" (some-> sym namespace))
        false

        :else
        (boolean
         (and principal
              (if-let [policy (:authorize config)]
                (policy {:op :invoke :fn-name sym :principal principal
                         :db nil :payload arg-map})
                true)))))))

(defn authorize-sync
  "The gate for the sync middleware: subscribing to a store's topic reads the
   store; nobody publishes into the server."
  [config]
  (fn [{:keys [op principal topic]}]
    (case op
      :subscribe (allowed? config :read principal (topic-store-id topic) nil)
      false)))

(defn- reopen-databases!
  "Serve again the databases an earlier run of this listener created: every
   UUID-named directory below the file store's path is opened and registered
   for dispatch and sync. Without this a restart leaves every existing
   database unreachable until it is deleted."
  [server {:keys [store] :as kabel}]
  (when (= :file (:backend store))
    (let [config-fn (store-config-fn kabel)]
      (doseq [dir (some->> (io/file (:path store)) .listFiles (filter #(.isDirectory ^java.io.File %)))
              :let [store-id (parse-uuid (.getName ^java.io.File dir))]
              :when store-id]
        (try
          ;; Reopen with the configuration the database was created with:
          ;; Datahike refuses a connect whose settings differ from the stored ones.
          (let [store-config (config-fn store-id nil)
                store (ks/connect-store store-config {:sync? true})
                stored (try (k/get store :db nil {:sync? true})
                            (finally (ks/release-store store-config store {:sync? true})))
                config (-> (:config stored) (assoc :store store-config :writer {:backend :self}))
                conn (d/connect config)]
            (handlers/register-store-for-remote-access! store-id conn server)
            (log/info :datahike/kabel-database-reopened {:store-id store-id}))
          (catch Throwable t
            (log/warn :datahike/kabel-database-reopen-failed
                      {:store-id store-id :error (ex-message t)})))))))

(defn start!
  "Start the configured Kabel listener. Returns an owned peer resource or nil."
  [config]
  (when-let [{:keys [host port peer-id jwt] :as kabel}
             (:kabel (validate-config config))]
    (let [url     (str "ws://" host ":" port)
          handler (create-http-kit-handler! S url peer-id (atom {}) (atom {})
                                            {:server-opts {:ip host}})
          server  (peer/server-peer
                   S handler peer-id
                   (comp (sync/server-middleware {:authorize (authorize-sync config)})
                         remote/middleware
                         (auth/auth-middleware {:validate {:jwt jwt}}))
                   cbor/datahike-cbor-middleware)
          served  (remote/serve server {:authorize (authorize-remote config)})]
      (handlers/register-global-handlers!
       server {:store-config-fn (store-config-fn kabel)
               ;; a go block carries its bindings across parks and the local
               ;; writer onto its thread, so the request's resolver holds
               ;; through the whole dispatch
               :wrap-handler (let [run (routes/query-function-binding config)]
                               (fn [handler] (fn [arg-map] (run #(handler arg-map)))))})
      (reopen-databases! server kabel)
      (<?? S (peer/start server))
      (log/info :datahike/kabel-server-started {:url url :peer-id peer-id})
      {:peer server :url url :peer-id peer-id :served served})))

(defn stop! [resource]
  (when-let [server (:peer resource)]
    (try
      (when-let [stop! (get-in resource [:served :stop!])] (stop!))
      (<?? S (peer/stop server))
      (finally
        (log/info :datahike/kabel-server-stopped {:peer-id (:peer-id resource)}))))
  nil)
