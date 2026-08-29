(ns datahike.http.server
  "HTTP server implementation for Datahike: `datahike.http.routes/handler`
   behind Jetty, with swagger-ui at `/`, CORS, and — given an `:auth-db` —
   eacl-backed permissions. Embedding hosts want `datahike.http.routes`."
  (:gen-class)
  (:require
   [clojure.edn :as edn]
   [datahike.metrics :as metrics]
   [datahike.http.permissions :as permissions]
   [datahike.http.routes :as routes]
   [reitit.ring :as ring]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [ring.middleware.cors :refer [wrap-cors]]
   [datahike.tools :refer [datahike-version]]
   [konserve.metrics :as konserve-metrics]
   [replikativ.metrics :as registry]
   [replikativ.metrics.jvm :as jvm-metrics]
   [replikativ.metrics.konserve :as metrics-konserve]
   [replikativ.metrics.prometheus :as prometheus]
   [replikativ.logging :as log]
   [ring.adapter.jetty :refer [run-jetty]]))

(def ^:private prometheus-content-type
  "text/plain; version=0.0.4; charset=utf-8")

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
  "The server's Ring handler over `connections`. With `:auth-db` in `config`
   the permissions database is opened here; `(::config (meta app))` is the
   configured config, for `stop-server`.

   This handler exposes process metrics but does not install a global Konserve
   sink: an embedding host owns that lifecycle. `start-server` installs and
   reference-counts the standalone server's sink."
  [config connections]
  (let [config  (if (:auth-db config) (permissions/configure config) config)
        handler (routes/handler config
                                {:connections     connections
                                 :extra-routes    (concat [swagger-route]
                                                          (keep identity [(metrics-route config connections)])
                                                          (permissions/routes config))
                                 :default-handler (ring/routes
                                                   (swagger-ui/create-swagger-ui-handler
                                                    {:path   "/"
                                                     :config {:validatorUrl     nil
                                                              :operationsSorter "alpha"}})
                                                   (ring/create-default-handler))})]
    ;; CORS outermost, so the gate's own 401/413 carry the headers too.
    (with-meta (wrap-cors handler
                          :access-control-allow-origin (or (:access-control-allow-origin config)
                                                           [#"http://localhost" #"http://localhost:8080"])
                          :access-control-allow-methods [:get :put :post :delete])
      (assoc (meta handler) ::config config))))

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

(defn start-server
  "Start Jetty and acquire the standalone server's shared Konserve metric sink
   unless `:metrics` is false. `stop-server` releases both."
  [config]
  (let [connections (atom {})
        app         (app config connections)
        config      (::config (meta app))
        metrics-lease (when-not (false? (:metrics config))
                        (acquire-metrics-sink!))
        server      (try (run-jetty app config)
                         (catch Throwable t
                           ;; Nothing owns what `app` opened if Jetty never started.
                           (release-metrics-sink! metrics-lease)
                           (permissions/close! config)
                           (throw t)))]
    (swap! owned assoc server {:connections connections
                               :config config
                               :metrics-lease metrics-lease})
    server))

(defn stop-server [^org.eclipse.jetty.server.Server server]
  (try (.stop server)
       (finally
         (when-let [{:keys [connections config metrics-lease]} (get @owned server)]
           (swap! owned dissoc server)
           (routes/release-all! connections)
           (permissions/close! config)
           (release-metrics-sink! metrics-lease)))))

(defn -main [& args]
  (let [{:keys [level] :as config} (edn/read-string (slurp (first args)))]
    (when (#{:trace :debug :info nil} level)
      (println "Datahike HTTP Server" datahike-version "- https://datahike.io"))
    (log/info :datahike/http-server-config {:config (routes/redact config)})
    (start-server config)
    (log/info :datahike/http-server-started "Server started")))
