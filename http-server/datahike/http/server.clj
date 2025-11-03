(ns datahike.http.server
  "HTTP server implementation for Datahike using the router namespace."
  (:gen-class)
  (:refer-clojure :exclude [read-string filter])
  (:require
   [clojure.string :as str]
   [clojure.edn :as edn]
   [datahike.connections :refer [*connections*]]
   [datahike.http.router :as router]
   [datahike.http.middleware :as middleware]
   [datahike.readers :refer [edn-readers]]
   [datahike.transit :as transit]
   [datahike.json :as json]
   [datahike.api :refer :all :as api]
   [datahike.writing]
   [datahike.writer]
   [reitit.ring :as ring]
   [reitit.coercion.spec]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.exception :as exception]
   [reitit.ring.middleware.multipart :as multipart]
   [reitit.ring.middleware.parameters :as parameters]
   [ring.middleware.cors :refer [wrap-cors]]
   [muuntaja.core :as m]
   [clojure.spec.alpha :as s]
   [datahike.spec :as spec]
   [datahike.tools :refer [datahike-version datahike-logo]]
   [datahike.impl.entity :as de]
   [taoensso.timbre :as log]
   [ring.adapter.jetty :refer [run-jetty]]
   [spec-tools.core :as st])
  (:import [datahike.datom Datom]))

;; Handler wrapper for server-specific functionality (caching, logging)
(defn wrap-server-handler
  "Wraps a handler with server-specific functionality like caching and logging."
  [handler config]
  (fn [request]
    (let [response (handler request)]
      (merge response
             (when (and (= (:request-method request) :get)
                        (get-in request [:params "args-id"])
                        (get-in config [:cache :get :max-age]))
               {:headers {"Cache-Control" (str (when-not (:token config) "public, ")
                                               "max-age=" (get-in config [:cache :get :max-age]))}})))))

;; Convert router routes to Reitit format with Swagger support
(defn routes-to-reitit
  "Converts routes from the router namespace to Reitit format with Swagger metadata."
  [routes config]
  (vec
   (for [{:keys [path method handler name doc]} routes]
     [path
      {:swagger {:tags ["API"]}
       method {:operationId (str name)
               :summary (when doc (str (first (str/split doc #"\.\s")) "."))
               :description doc
               :parameters {:body (st/spec {:spec any?
                                           :name (str name)})}
               :handler (wrap-server-handler handler config)}}])))

(def muuntaja-with-opts
  (m/create
   (-> m/default-options
       (assoc-in [:formats "application/edn" :decoder-opts]
                 {:readers edn-readers})
       (assoc-in [:formats "application/json" :decoder-opts]
                 json/mapper-opts)
       (assoc-in [:formats "application/json" :encoder-opts]
                 json/mapper-opts)
       (assoc-in [:formats "application/transit+json" :decoder-opts]
                 {:handlers transit/read-handlers})
       (assoc-in [:formats "application/transit+json" :encoder-opts]
                 {:handlers transit/write-handlers}))))

(defn default-route-opts [muuntaja-with-opts]
  {:data      {:coercion   reitit.coercion.spec/coercion
               :muuntaja   muuntaja-with-opts
               :middleware [swagger/swagger-feature
                            parameters/parameters-middleware
                            muuntaja/format-negotiate-middleware
                            muuntaja/format-response-middleware
                            exception/exception-middleware
                            muuntaja/format-request-middleware
                            (middleware/encode-plain-value muuntaja-with-opts)
                            middleware/support-embedded-edn-in-json
                            coercion/coerce-response-middleware
                            coercion/coerce-request-middleware
                            multipart/multipart-middleware
                            middleware/patch-swagger-json]}})

;; Writer routes handled by router namespace

(defn app [config route-opts server-connections]
  (-> (ring/ring-handler
       (ring/router
        (concat
         [["/swagger.json"
           {:get {:no-doc  true
                  :swagger {:info {:title       "Datahike API"
                                   :description "Transaction and query functions for Datahike.\n\nThe signatures match those of the Clojure API. All functions take their arguments passed as a vector/list in the POST request body."}}
                  :handler (swagger/create-swagger-handler)}}]]
         ;; Get routes from router namespace
         (let [api-routes (router/create-routes :format :raw :include-writers? false)
               writer-routes (router/generate-writer-routes)
               reitit-api-routes (routes-to-reitit api-routes config)
               reitit-writer-routes (routes-to-reitit writer-routes config)]
           (map (fn [route]
                  (let [method (if (:get (second route)) :get :post)]
                    (assoc-in route [1 method :middleware]
                              [(partial middleware/token-auth config)
                               (partial middleware/auth config)])))
                (concat reitit-api-routes
                        reitit-writer-routes)))) route-opts)
       (ring/routes
        (swagger-ui/create-swagger-ui-handler
         {:path   "/"
          :config {:validatorUrl     nil
                   :operationsSorter "alpha"}})
        (ring/create-default-handler)))
      (wrap-cors :access-control-allow-origin (or (:access-control-allow-origin config)
                                                  [#"http://localhost" #"http://localhost:8080"])
                 :access-control-allow-methods [:get :put :post :delete])))

(defn start-server [config]
  (run-jetty (app config (default-route-opts muuntaja-with-opts) (atom {})) config))

(defn stop-server [^org.eclipse.jetty.server.Server server]
  (.stop server))

(defn -main [& args]
  (let [{:keys [level token] :as config} (edn/read-string (slurp (first args)))]
    (when level (log/set-level! level))
    (when (#{:trace :debug :info nil} level)
      (println)
      (println "Welcome to datahike.http.server!")
      (println "For more information visit https://datahike.io,")
      (println "or if you encounter any problem feel free to open an issue at https://github.com/replikativ/datahike.")
      (println (str "\n" datahike-logo))
      (println))
    (log/info "Config:" (if token (assoc config :token "REDACTED") config))
    (log/info "Datahike version:" datahike-version)
    (start-server config)
    (log/info "Server started.")))
