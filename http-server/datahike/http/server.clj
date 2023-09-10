(ns datahike.http.server
  "HTTP server implementation for Datahike."
  (:gen-class)
  (:refer-clojure :exclude [read-string filter])
  (:require
   [clojure.string :as str]
   [clojure.edn :as edn]
   [datahike.api.specification :refer [api-specification ->url]]
   [datahike.http.middleware :as middleware]
   [datahike.readers :refer [edn-readers]]
   [datahike.transit :refer [read-handlers write-handlers]]
   [datahike.api :refer :all :as api]
   [reitit.ring :as ring]
   [reitit.coercion.spec]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.multipart :as multipart]
   [reitit.ring.middleware.parameters :as parameters]
   [ring.middleware.cors :refer [wrap-cors]]
   [muuntaja.core :as m]
   [clojure.spec.alpha :as s]
   [datahike.spec :as spec]
   [datahike.impl.entity :as de]
   [taoensso.timbre :as log]
   [ring.adapter.jetty :refer [run-jetty]]
   [spec-tools.core :as st])
  (:import [datahike.datom Datom]))

(defn generic-handler [f]
  (fn [request]
    (let [{{body :body} :parameters
           :keys [headers]} request
          _ (log/trace "req-body" f body (= f #'api/create-database))
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
      (log/trace "ret-body" ret-body)
      {:status 200
       :body
       (when-not (headers "no-return-value")
         ret-body)})))

(declare create-routes)

(defn extract-first-sentence [doc]
  (str (first (str/split doc #"\.\s")) "."))

;; This code expands and evals the server route construction given the
;; API specification. 
;; TODO This would not need macro-expansion if s/spec would not be a macro.
(eval
 `(defn ~'create-routes [~'config]
    ~(vec
      (for [[n {:keys [args doc supports-remote?]}] api-specification
            :when supports-remote?]
        `[~(str "/" (->url n))
          {:swagger {:tags ["API"]}
           :post {:operationId ~(str n)
                  :summary     ~(extract-first-sentence doc)
                  :description ~doc
                  :parameters  {:body (st/spec {:spec (s/spec ~args)
                                                :name ~(str n)})}
                  :handler     (generic-handler ~(resolve n))}}]))))

(def muuntaja-with-opts
  (m/create
   (-> m/default-options
       (assoc-in [:formats "application/edn" :decoder-opts]
                 {:readers edn-readers})
       (assoc-in [:formats "application/transit+json" :decoder-opts]
                 {:handlers read-handlers})
       (assoc-in [:formats "application/transit+json" :encoder-opts]
                 {:handlers write-handlers}))))

(defn default-route-opts [muuntaja-with-opts]
  {;; :reitit.middleware/transform dev/print-request-diffs ;; pretty diffs
   ;; :validate spec/validate ;; enable spec validation for route data
   ;; :reitit.spec/wrap spell/closed ;; strict top-level validation
   ; :exception pretty/exception
   :data      {:coercion   reitit.coercion.spec/coercion
               :muuntaja   muuntaja-with-opts
               :middleware [swagger/swagger-feature
                            (middleware/encode-plain-value muuntaja-with-opts)
                            parameters/parameters-middleware
                            muuntaja/format-middleware
                            middleware/wrap-fallback-exception
                            middleware/wrap-server-exception
                            coercion/coerce-response-middleware
                            coercion/coerce-request-middleware
                            multipart/multipart-middleware]}})

(def internal-writer-routes
  [["/delete-database-writer"
    {:post    {:parameters  {:body (st/spec {:spec any?
                                             :name "delete-database-writer"})},
               :summary     "Internal endpoint. DO NOT USE!"
               :no-doc      true
               :handler     (fn [{{:keys [body]} :parameters}]
                              {:status 200
                               :body   (apply datahike.writing/delete-database body)})
               :operationId "delete-database"},
     :swagger {:tags ["Internal"]}}]
   ["/create-database-writer"
    {:post    {:parameters  {:body (st/spec {:spec any?
                                             :name "create-database-writer"})},
               :summary     "Internal endpoint. DO NOT USE!"
               :no-doc      true
               :handler     (fn [{{:keys [body]} :parameters}]
                              {:status 200
                               :body   (apply datahike.writing/create-database
                                              (dissoc (first body) :remote-peer)
                                              (rest body))})
               :operationId "create-database"},
     :swagger {:tags ["Internal"]}}]
   ["/transact-writer"
    {:post    {:parameters  {:body (st/spec {:spec any?
                                             :name "transact-writer"})},
               :summary     "Internal endpoint. DO NOT USE!"
               :no-doc      true
               :handler     (fn [{{:keys [body]} :parameters}]
                              (let [res (apply datahike.writing/transact! body)]
                                {:status 200
                                 :body   res}))
               :operationId "transact"},
     :swagger {:tags ["Internal"]}}]])

(defn app [config route-opts]
  (-> (ring/ring-handler
       (ring/router
        (concat
         [["/swagger.json"
           {:get {:no-doc  true
                  :swagger {:info {:title       "Datahike API"
                                   :description "Transaction and query functions for Datahike.\n\nThe signatures match those of the Clojure API. All functions take their arguments passed as a vector/list in the POST request body."}}
                  :handler (swagger/create-swagger-handler)}}]]
         (map (fn [route]
                (assoc-in route [1 :post :middleware]
                          [(partial middleware/token-auth config)
                           (partial middleware/auth config)]))
              (concat (create-routes config)
                      internal-writer-routes))) route-opts)
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
  (run-jetty (app config (default-route-opts muuntaja-with-opts)) config))

(defn stop-server [^org.eclipse.jetty.server.Server server]
  (.stop server))

(defn -main [& args]
  (let [{:keys [level token] :as config} (edn/read-string (slurp (first args)))]
    (log/info "Starting datahike.http.server with config:")
    (log/info "Config: " (if token (assoc config :token "REDACTED") config))
    (when level (log/set-level! level))
    (start-server config)
    (log/info "Server started.")))
