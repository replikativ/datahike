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
  (:require [clojure.core.async :refer [<!!]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datahike.kabel.cbor-handlers :as cbor]
            [datahike.kabel.handlers :as handlers]
            [kabel.auth.websocket :as auth]
            [kabel.remote :as remote]
            [kabel.http-kit :refer [create-http-kit-handler!]]
            [kabel.peer :as peer]
            [konserve-sync.core :as sync]
            [replikativ.logging :as log]
            [superv.async :refer [S go-try <?]]))

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

(defn- remote-op
  "What a remote function does, for authorization; nil for a name the server
   does not serve, which is refused."
  [fn-name arg-map]
  (case (some-> fn-name name)
    "dispatch"        (if (= 'gc-storage! (get-in arg-map [:arg-map :op])) :admin :transact)
    "create-database" :create
    "delete-database" :delete
    nil))

(defn- remote-store-id
  "The store a remote call reaches."
  [fn-name arg-map]
  (case (some-> fn-name name)
    "dispatch" (:store-id arg-map)
    (get-in arg-map [:config :store :id])))

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
  "The gate for `kabel.remote/serve`, in the `kabel.authorize` shape."
  [config]
  (fn [{:keys [principal fn-name arg-map]}]
    (if-let [op (remote-op fn-name arg-map)]
      (allowed? config op principal (remote-store-id fn-name arg-map) arg-map)
      false)))

(defn authorize-sync
  "The gate for the sync middleware: subscribing to a store's topic reads the
   store; nobody publishes into the server."
  [config]
  (fn [{:keys [op principal topic]}]
    (case op
      :subscribe (allowed? config :read principal topic nil)
      false)))

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
       server {:store-config-fn (store-config-fn kabel)})
      (<!! (go-try S (<? S (peer/start server))))
      (log/info :datahike/kabel-server-started {:url url :peer-id peer-id})
      {:peer server :url url :peer-id peer-id :served served})))

(defn stop! [resource]
  (when-let [server (:peer resource)]
    (try
      (when-let [stop! (get-in resource [:served :stop!])] (stop!))
      (<!! (go-try S (<? S (peer/stop server))))
      (finally
        (log/info :datahike/kabel-server-stopped {:peer-id (:peer-id resource)}))))
  nil)
