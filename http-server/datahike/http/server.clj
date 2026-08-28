(ns datahike.http.server
  "HTTP server implementation for Datahike: `datahike.http.routes` behind
   Jetty, with swagger-ui at `/` and CORS. Embedding hosts want `routes`."
  (:gen-class)
  (:require
   [clojure.edn :as edn]
   [datahike.http.routes :as routes]
   [reitit.ring :as ring]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [ring.middleware.cors :refer [wrap-cors]]
   [datahike.tools :refer [datahike-version]]
   [replikativ.logging :as log]
   [ring.adapter.jetty :refer [run-jetty]]))

(defn app [config server-connections]
  (let [rtr (routes/router
             config
             {:extra-routes
              [["/swagger.json"
                {:public? true
                 :get {:no-doc  true
                  :swagger {:info {:title       "Datahike API"
                                   :description "Transaction and query functions for Datahike.\n\nThe signatures match those of the Clojure API. All functions take their arguments passed as a vector/list in the POST request body."}}
                  :handler (swagger/create-swagger-handler)}}]]})]
    (routes/wrap-api
     (-> (ring/ring-handler
          rtr
          (ring/routes
           (swagger-ui/create-swagger-ui-handler
            {:path   "/"
             :config {:validatorUrl     nil
                      :operationsSorter "alpha"}})
           (ring/create-default-handler)))
         (wrap-cors :access-control-allow-origin (or (:access-control-allow-origin config)
                                                     [#"http://localhost" #"http://localhost:8080"])
                    :access-control-allow-methods [:get :put :post :delete]))
     rtr config server-connections)))

(defonce ^:private owned-connections
  ;; server -> the atom its routes share, so `stop-server` can release the
  ;; databases the server opened without changing its signature.
  (atom {}))

(defn start-server [config]
  (let [connections (atom {})
        server      (run-jetty (app config connections) config)]
    (swap! owned-connections assoc server connections)
    server))

(defn stop-server [^org.eclipse.jetty.server.Server server]
  (.stop server)
  (when-let [connections (get @owned-connections server)]
    (swap! owned-connections dissoc server)
    (routes/release-all! connections)))

(defn -main [& args]
  (let [{:keys [level token] :as config} (edn/read-string (slurp (first args)))]
    (when (#{:trace :debug :info nil} level)
      (println "Datahike HTTP Server" datahike-version "- https://datahike.io"))
    (log/info :datahike/http-server-config {:config (if token (assoc config :token "REDACTED") config)})
    (start-server config)
    (log/info :datahike/http-server-started "Server started")))
