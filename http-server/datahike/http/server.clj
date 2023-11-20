(ns datahike.http.server
  "HTTP server implementation for Datahike."
  (:gen-class)
  (:refer-clojure :exclude [read-string filter])
  (:require
   [clojure.string :as str]
   [clojure.edn :as edn]
   [datahike.connections :refer [*connections*]]
   [datahike.api.specification :refer [api-specification ->url]]
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

(defn generic-handler [config f]
  (fn [request]
    (try
      (let [{{body :body} :parameters
             :keys [headers params method]} request
            _ (log/trace "request body" f body)
          ;; TODO move this to client
            ret-body
            (cond (= f #'api/create-database)
                ;; remove remote-peer and re-add
                  (assoc
                   (apply f (dissoc (first body) :remote-peer) (rest body))
                   :remote-peer (:remote-peer (first body)))

                  (= f #'api/delete-database)
                  (apply f (dissoc (first body) :remote-peer) (rest body))

                  :else
                  (apply f body))]
        (log/trace "return body" ret-body)
        (merge
         {:status 200
          :body
          (when-not (headers "no-return-value")
            ret-body)}
         (when (and (= method :get)
                    (get params "args-id")
                    (get-in config [:cache :get :max-age]))
           {:headers {"Cache-Control" (str (when-not (:token config) "public, ")
                                           "max-age=" (get-in config [:cache :get :max-age]))}})))
      (catch Exception e
        {:status 500
         :body   {:msg (ex-message e)
                  :ex-data (ex-data e)}})))))

(declare create-routes)

(defn extract-first-sentence [doc]
  (str (first (str/split doc #"\.\s")) "."))

;; This code expands and evals the server route construction given the
;; API specification. 
;; TODO This would not need macro-expansion if s/spec would not be a macro.
(eval
 `(defn ~'create-routes [~'config]
    ~(vec
      (for [[n {:keys [args doc supports-remote? referentially-transparent?]}] api-specification
            :when supports-remote?]
        `[~(str "/" (->url n))
          {:swagger {:tags ["API"]}
           ~(if referentially-transparent? :get :post)
           {:operationId ~(str n)
            :summary     ~(extract-first-sentence doc)
            :description ~doc
            :parameters  {:body (st/spec {:spec (s/spec ~args)
                                          :name ~(str n)})}
            :handler     (generic-handler ~'config ~(resolve n))}}]))))

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

(defn internal-writer-routes [server-connections]
  [["/delete-database-writer"
    {:post {:parameters  {:body (st/spec {:spec any?
                                          :name "delete-database-writer"})},
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters}]
                           (try
                             {:status 200
                              :body   (apply datahike.writing/delete-database body)}
                             (catch Exception e
                               {:status 500
                                :body   {:msg (ex-message e)
                                         :ex-data (ex-data e)}})))
            :operationId "delete-database"},
     :swagger {:tags ["Internal"]}}]
   ["/create-database-writer"
    {:post {:parameters  {:body (st/spec {:spec any?
                                          :name "create-database-writer"})},
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters}]
                           (try
                             {:status 200
                              :body   (apply datahike.writing/create-database
                                             (dissoc (first body) :remote-peer :writer)
                                             (rest body))}
                             (catch Exception e
                               {:status 500
                                :body   {:msg (ex-message e)
                                         :ex-data (ex-data e)}})))
            :operationId "create-database"},
     :swagger {:tags ["Internal"]}}]
   ["/transact!-writer"
    {:post {:parameters  {:body (st/spec {:spec any?
                                          :name "transact-writer"})},
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters}]
                           (binding [*connections* server-connections]
                             (try
                               (let [conn (api/connect (dissoc (first body) :remote-peer :writer)) ;; TODO maybe release?
                                     res @(apply datahike.writer/transact! conn (rest body))]
                                 {:status 200
                                  :body   res})
                               (catch Exception e
                                 {:status 500
                                  :body   {:msg (ex-message e)
                                           :ex-data (ex-data e)}}))))
            :operationId "transact"},
     :swagger {:tags ["Internal"]}}]])

(defn app [config route-opts server-connections]
  (-> (ring/ring-handler
       (ring/router
        (concat
         [["/swagger.json"
           {:get {:no-doc  true
                  :swagger {:info {:title       "Datahike API"
                                   :description "Transaction and query functions for Datahike.\n\nThe signatures match those of the Clojure API. All functions take their arguments passed as a vector/list in the POST request body."}}
                  :handler (swagger/create-swagger-handler)}}]]
         (map (fn [route]
                (let [method (if (:get (second route)) :get :post)]
                  (assoc-in route [1 method :middleware]
                            [(partial middleware/token-auth config)
                             (partial middleware/auth config)])))
              (concat (create-routes config)
                      (internal-writer-routes server-connections)))) route-opts)
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
