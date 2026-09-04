(ns datahike.http.server
  "HTTP server implementation for Datahike: `datahike.http.routes/handler`
   behind Jetty, with the operator page at `/`, Swagger UI at `/swagger`,
   CORS, and — given a `:system-db` — eacl-backed permissions. Embedding hosts
   want `datahike.http.routes`."
  (:require
   [datahike.http.admin :as admin]
   [datahike.http.backends]
   [datahike.http.config :as server-config]
   [datahike.http.kabel :as kabel-server]
   [datahike.http.nrepl :as server-nrepl]
   [datahike.metrics :as metrics]
   [datahike.http.permissions :as permissions]
   [datahike.http.pg :as pg]
   [datahike.http.routes :as routes]
   [datahike.http.system :as system]
   [reitit.ring :as ring]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [ring.middleware.cors :refer [wrap-cors]]
   [datahike.tools :as tools]
   [konserve.core :as k]
   [konserve.metrics :as konserve-metrics]
   [konserve.store]
   [replikativ.metrics :as registry]
   [replikativ.metrics.jvm :as jvm-metrics]
   [replikativ.metrics.konserve :as metrics-konserve]
   [replikativ.metrics.prometheus :as prometheus]
   [replikativ.logging :as log]
   [ring.adapter.jetty :refer [run-jetty]]))

(def ^:private prometheus-content-type
  "text/plain; version=0.0.4; charset=utf-8")

(def ^:private plain-text-content-type
  "text/plain;charset=utf-8")

(def ^:private readiness-probe-key ::readiness-probe)

(def ^:private default-shutdown-timeout-ms 30000)

(defn- text-response [status body]
  {:status  status
   :headers {"content-type" plain-text-content-type}
   ;; Keep format middleware from negotiating or encoding operational text.
   :body    (java.io.ByteArrayInputStream.
             (.getBytes ^String body "UTF-8"))})

(defn- probe-connection!
  "Touch one connection's logical Konserve store without relying on a cached
   database key. A missing probe key is success: the read itself is the check."
  [[store-id conn]]
  (try
    (let [;; Do not deref the Connection itself: shared writers refresh their
          ;; branch on deref and may rebuild indices. Read its raw state so the
          ;; health check performs exactly one cheap, non-mutating store read.
          db    @(get conn :wrapped-atom)
          store (:store db)]
      (k/get store readiness-probe-key nil {:sync? true})
      true)
    (catch Throwable e
      (log/warn :datahike/readiness-probe-failed
                {:database (some-> store-id str)
                 ;; Backend exceptions can carry URLs or credentials in their
                 ;; message/ex-data. Health logs identify the target and error
                 ;; class without copying those values.
                 :error-type  (:type (ex-data e))
                 :error-class (.getName (class e))})
      false)))

(defn- ready?
  "Check every store the server currently owns: live API connections and the
   configured system database. Do not short-circuit, so one outage does not hide
   another from the operator logs."
  [config connections]
  (let [targets (concat
                 (for [[[store-id _branch] {:keys [conn]}] @connections
                       :when conn]
                   [store-id conn])
                 (when-let [conn (get config system/conn-key)]
                   [[:system-db conn]]))]
    (reduce (fn [ready target]
              (let [target-ready? (probe-connection! target)]
                (and ready target-ready?)))
            true
            targets)))

(defn- health-routes [config connections]
  [["/health/live"
    {:public? true
     :get {:no-doc    true
           :metric-op :health
           :handler   (fn [_] (text-response 200 "live\n"))}}]
   ["/health/ready"
    {:public? true
     :get {:no-doc    true
           :metric-op :health
           :handler   (fn [_]
                        (if (ready? config connections)
                          (text-response 200 "ready\n")
                          (text-response 503 "not ready\n")))}}]])

(defn- registered-konserve-backends []
  ;; Konserve backends become usable by registering a connect-store method.
  ;; Report the runtime truth, including third-party backends loaded by an
  ;; embedding host, rather than maintaining a second hard-coded list here.
  (if-let [connect-store (ns-resolve 'konserve.store '-connect-store)]
    (->> (methods @connect-store)
         keys
         (remove #{:default})
         sort
         vec)
    []))

(defn- version-route [server-config]
  ["/version"
   {:get {:no-doc    true
          :metric-op :version
          :handler   (fn [_]
                       {:status 200
                        :body   {:datahike-version  tools/datahike-version
                                 :git-sha           tools/datahike-git-sha
                                 :konserve-version  tools/konserve-version
                                 :konserve-backends (registered-konserve-backends)
                                 :config            (routes/redact server-config)}})}}])

(defn- metrics-route [config connections]
  (when-not (false? (:metrics config))
    ["/prometheus"
     (cond-> {:get {:no-doc    true
                    :metric-op :metrics
                    :handler   (fn [_]
                                 (let [body (prometheus/text
                                             (registry/snapshot)
                                             (concat (jvm-metrics/samples)
                                                     (metrics/connection-samples connections)))]
                                   {:status  200
                                    :headers {"content-type" prometheus-content-type}
                                    ;; Keep format middleware from negotiating or
                                    ;; encoding Prometheus text as JSON/EDN.
                                    :body    (java.io.ByteArrayInputStream.
                                              (.getBytes ^String body "UTF-8"))}))}}
       (true? (get-in config [:metrics :public?])) (assoc :public? true))]))

(def ^:private swagger-route
  ["/swagger.json"
   {:public? true
    :get {:no-doc  true
          :swagger {:info {:title       "Datahike API"
                           :description "Transaction and query functions for Datahike.\n\nThe signatures match those of the Clojure API. All functions take their arguments passed as a vector/list in the POST request body."}}
          :handler (swagger/create-swagger-handler)}}])

(defn app
  "The server's Ring handler over `connections`. With `:system-db` in `config`,
   the catalog and permissions database is opened here; `(::config (meta app))`
   is the configured config, for `stop-server`.

   This handler exposes process metrics but does not install a global Konserve
   sink: an embedding host owns that lifecycle. `start-server` installs and
   reference-counts the standalone server's sink."
  ([config connections]
   (app config connections (server-nrepl/status-atom)))
  ([config connections nrepl-status]
   (let [server-config config
         config  (system/configure config)
         config  (if (get config system/conn-key)
                   (permissions/configure config)
                   config)
         handler (routes/handler config
                                 {:connections     connections
                                  :extra-routes    (concat [swagger-route]
                                                           (admin/routes config connections nrepl-status)
                                                           (health-routes config connections)
                                                           [(version-route server-config)]
                                                           (keep identity [(metrics-route config connections)])
                                                           (system/routes config)
                                                           (permissions/routes config))
                                  :default-handler (ring/routes
                                                    (swagger-ui/create-swagger-ui-handler
                                                     {:path   "/swagger"
                                                      :config {:validatorUrl     nil
                                                               :operationsSorter "alpha"}})
                                                    (ring/create-default-handler))})]
     ;; CORS outermost, so the gate's own 401/413 carry the headers too.
     (with-meta (wrap-cors handler
                           :access-control-allow-origin (or (:access-control-allow-origin config)
                                                            [#"http://localhost" #"http://localhost:8080"])
                           :access-control-allow-methods [:get :put :post :delete])
       (assoc (meta handler) ::config config)))))

(defonce ^:private owned
  ;; server -> what it opened, so `stop-server` can close it without a
  ;; change of signature.
  (atom {}))

(defonce ^:private metrics-sink-leases (atom #{}))

(def ^:private metrics-sink-id ::datahike)

(defn- acquire-metrics-sink! []
  (let [lease (Object.)]
    (locking metrics-sink-leases
      (when (empty? @metrics-sink-leases)
        (metrics-konserve/describe!)
        (konserve-metrics/add-sink! metrics-sink-id metrics-konserve/sink))
      (swap! metrics-sink-leases conj lease))
    lease))

(defn- release-metrics-sink! [lease]
  (when lease
    (locking metrics-sink-leases
      (swap! metrics-sink-leases disj lease)
      (when (empty? @metrics-sink-leases)
        (konserve-metrics/remove-sink! metrics-sink-id))))
  nil)

(defn- shutdown-timeout
  [{:keys [shutdown-timeout-ms]}]
  (let [timeout (or shutdown-timeout-ms default-shutdown-timeout-ms)]
    (when-not (and (integer? timeout) (<= 0 timeout Long/MAX_VALUE))
      (throw (ex-info ":shutdown-timeout-ms must be a nonnegative 64-bit integer"
                      {:type :datahike.http/invalid-shutdown-timeout
                       :shutdown-timeout-ms timeout})))
    timeout))

(defn- with-graceful-shutdown
  [{:keys [configurator] :as config} timeout]
  (assoc config :configurator
         (fn [^org.eclipse.jetty.server.Server server]
           ;; A positive Jetty stop timeout first closes the connectors to
           ;; new work, then waits for active handlers before stopping the
           ;; thread pool. Zero deliberately requests an immediate stop.
           (.setStopTimeout server (long timeout))
           (when configurator
             (configurator server)))))

(defn- cleanup-owned!
  [connections config nrepl-resource pg-listener kabel-resource metrics-lease]
  (try
    (server-nrepl/stop! nrepl-resource)
    (finally
      (try
        (kabel-server/stop! kabel-resource)
        (finally
          (try
            (pg/stop! pg-listener)
            (finally
              (try
                (routes/release-all! connections)
                (finally
                  (try
                    (system/close! config)
                    (finally
                      (release-metrics-sink! metrics-lease))))))))))))

(defn start-server
  "Start Jetty and acquire the standalone server's shared Konserve metric sink
   unless `:metrics` is false. `stop-server` releases both."
  [config]
  (let [requested-config (-> config
                             server-config/assert-safe-nrepl!
                             server-config/assert-safe-bind!)
        config      requested-config
        timeout     (shutdown-timeout config)
        connections (atom {})
        nrepl-status (server-nrepl/status-atom)
        app         (app config connections nrepl-status)
        config      (::config (meta app))
        jetty-config (with-graceful-shutdown config timeout)
        pg-listener (try
                      (pg/start! config connections)
                      (catch Throwable t
                        (cleanup-owned! connections config nil nil nil nil)
                        (throw t)))
        kabel-resource (try
                         (kabel-server/start! config)
                         (catch Throwable t
                           (cleanup-owned! connections config nil pg-listener nil nil)
                           (throw t)))
        metrics-lease (try
                        (when-not (false? (:metrics config))
                          (acquire-metrics-sink!))
                        (catch Throwable t
                          (cleanup-owned! connections config nil pg-listener kabel-resource nil)
                          (throw t)))
        nrepl-resource (try
                         (server-nrepl/start! config (routes/redact requested-config)
                                              connections nrepl-status)
                         (catch Throwable t
                           (cleanup-owned! connections config nil pg-listener kabel-resource metrics-lease)
                           (throw t)))
        server      (try (run-jetty app jetty-config)
                         (catch Throwable t
                           ;; Nothing owns what `app` opened if Jetty never started.
                           (cleanup-owned! connections config nrepl-resource pg-listener kabel-resource metrics-lease)
                           (throw t)))]
    (swap! owned assoc server {:connections connections
                               :config config
                               :nrepl-resource nrepl-resource
                               :pg-listener pg-listener
                               :kabel-resource kabel-resource
                               :metrics-lease metrics-lease})
    server))

(defn stop-server
  "Gracefully stop `server`, then release everything the standalone server
   opened. Resource ownership is claimed atomically so concurrent or repeated
   calls cannot release a connection, permission database, or metric lease
   twice."
  [^org.eclipse.jetty.server.Server server]
  (let [[before _] (swap-vals! owned dissoc server)
        {:keys [connections config nrepl-resource pg-listener kabel-resource metrics-lease]}
        (get before server)]
    (try (.stop server)
         (finally
           (when connections
             (cleanup-owned! connections config nrepl-resource pg-listener kabel-resource metrics-lease))))))

(defn- shutdown-hook [server config]
  (Thread.
   ^Runnable
   (fn []
     (log/info :datahike/http-server-stopping
               {:reason :jvm-shutdown
                :timeout-ms (get config :shutdown-timeout-ms default-shutdown-timeout-ms)})
     (try
       (stop-server server)
       (log/info :datahike/http-server-stopped "Server stopped")
       (catch Throwable t
         (log/error :datahike/http-server-stop-failed
                    (ex-message t)
                    {:error-class (.getName (class t))}))))
   "datahike-http-server-shutdown"))

(defn start-main!
  "Start the standalone server after its lightweight launcher has configured
   process logging, and own its JVM shutdown lifecycle. Kept separate so
   config and logging load before backends."
  [config]
  (log/info :datahike/http-server-starting {:version tools/datahike-version})
  (log/info :datahike/http-server-config {:config (routes/redact config)})
  (let [server  (start-server (assoc config :join? false))
        runtime (Runtime/getRuntime)
        hook    (shutdown-hook server config)
        hooked? (atom false)]
    (try
      (.addShutdownHook runtime hook)
      (reset! hooked? true)
      (log/info :datahike/http-server-started "Server started")
      (.join ^org.eclipse.jetty.server.Server server)
      (finally
        ;; During JVM shutdown hooks may not be removed. The hook is already
        ;; stopping this server in that case; stop-server's ownership claim
        ;; keeps the cleanup exactly once.
        (when @hooked?
          (try (.removeShutdownHook runtime hook)
               (catch IllegalStateException _)))
        (stop-server server)))))
