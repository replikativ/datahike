(ns datahike.http.server
  "HTTP server implementation for Datahike: `datahike.http.routes/handler`
   behind Jetty, with swagger-ui at `/`, CORS, and — given an `:auth-db` —
   eacl-backed permissions. Embedding hosts want `datahike.http.routes`."
  (:gen-class)
  (:require
   [clojure.edn :as edn]
   [datahike.http.permissions :as permissions]
   [datahike.http.routes :as routes]
   [reitit.ring :as ring]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [ring.middleware.cors :refer [wrap-cors]]
   [datahike.tools :refer [datahike-version]]
   [replikativ.logging :as log]
   [ring.adapter.jetty :refer [run-jetty]]))

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
   configured config, for `stop-server`."
  [config connections]
  (let [config  (if (:auth-db config) (permissions/configure config) config)
        handler (routes/handler config
                                {:connections     connections
                                 :extra-routes    (concat [swagger-route] (permissions/routes config))
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

(defn start-server [config]
  (let [connections (atom {})
        app         (app config connections)
        config      (::config (meta app))
        server      (try (run-jetty app config)
                         (catch Throwable t
                           ;; Nothing owns what `app` opened if Jetty never started.
                           (permissions/close! config)
                           (throw t)))]
    (swap! owned assoc server {:connections connections :config config})
    server))

(defn stop-server [^org.eclipse.jetty.server.Server server]
  (try (.stop server)
       (finally
         (when-let [{:keys [connections config]} (get @owned server)]
           (swap! owned dissoc server)
           (routes/release-all! connections)
           (permissions/close! config)))))

(defn -main [& args]
  (let [{:keys [level] :as config} (edn/read-string (slurp (first args)))]
    (when (#{:trace :debug :info nil} level)
      (println "Datahike HTTP Server" datahike-version "- https://datahike.io"))
    (log/info :datahike/http-server-config {:config (routes/redact config)})
    (start-server config)
    (log/info :datahike/http-server-started "Server started")))
