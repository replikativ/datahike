(ns datahike.http.server
  "HTTP server implementation for Datahike."
  (:gen-class)
  (:refer-clojure :exclude [read-string filter])
  (:require
   [clojure.string :as str]
   [clojure.edn :as edn]
   [clojure.core.async :as async]
   [datahike.connections :refer [*connections*]]
   [datahike.api.specification :refer [api-specification ->url]]
   [datahike.api.types :as types]
   [datahike.http.middleware :as middleware]
   [datahike.readers :refer [edn-readers]]
   [datahike.transit :as transit]
   [datahike.json :as json]
   [datahike.api :refer :all :as api]
   [datahike.writing]
   [datahike.writer]
   [reitit.ring :as ring]
   [reitit.coercion.malli]
   [malli.util :as mu]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.exception :as exception]
   [reitit.ring.middleware.multipart :as multipart]
   [reitit.ring.middleware.parameters :as parameters]
   [ring.middleware.cors :refer [wrap-cors]]
   [muuntaja.core :as m]
   [datahike.tools :refer [datahike-version]]
   [datahike.impl.entity :as de]
   [taoensso.timbre :as log]
   [ring.adapter.jetty :refer [run-jetty]])
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
                  :ex-data (ex-data e)}}))))

(declare create-routes)

(defn extract-first-sentence [doc]
  (str (first (str/split doc #"\.\s")) "."))

(defn extract-input-schema
  "Extract input schema from malli function schema for HTTP body validation.
   Converts [:=> [:cat Type1 Type2] ret] to [:tuple Type1 Type2]
   or [:function [:=> [:cat T1] ret] [:=> [:cat T1 T2] ret]] to [:or [:tuple T1] [:tuple T1 T2]]

   The HTTP body is a tuple/vector of arguments that matches the function signature.
   For zero-arity functions, we use [:= []] to match an empty vector."
  [schema]
  (cond
    ;; Multi-arity: [:function [:=> [:cat ...] ret] ...]
    (and (vector? schema) (= :function (first schema)))
    (let [input-schemas (for [arity-schema (rest schema)
                              :when (and (vector? arity-schema)
                                         (= :=> (first arity-schema)))
                              :let [[_ input-schema _] arity-schema
                                    args (when (and (vector? input-schema)
                                                    (= :cat (first input-schema)))
                                           (rest input-schema))]]
                          (if (seq args)
                            (vec (cons :tuple args))
                            [:= []]))]  ;; Empty vector for zero-arity
      (if (> (count input-schemas) 1)
        (vec (cons :or input-schemas))
        (first input-schemas)))

    ;; Single arity: [:=> [:cat Type1 Type2] ret]
    (and (vector? schema) (= :=> (first schema)))
    (let [[_ input-schema _] schema]
      (if (and (vector? input-schema) (= :cat (first input-schema)))
        (let [args (rest input-schema)]
          (if (seq args)
            (vec (cons :tuple args))
            [:= []]))  ;; Empty vector for zero-arity
        [:sequential :any]))

    ;; Fallback
    :else [:sequential :any]))

;; This code expands and evals the server route construction given the
;; API specification.
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
            :parameters  {:body ~(extract-input-schema args)}
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
  {:data      {:coercion   (reitit.coercion.malli/create
                            {:compile mu/closed-schema
                             :strip-extra-keys true
                             :default-values true
                             :options {:registry types/registry}})
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
    {:post {:parameters  {:body [:sequential :any]},
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters}]
                           (binding [*connections* server-connections]
                             (let [cfg (dissoc (first body) :remote-peer :writer)]
                               (try
                                 (try
                                   (api/release (api/connect cfg) true)
                                   (catch Exception _))
                                 {:status 200
                                  :body   (async/<!! (apply datahike.writing/delete-database cfg (rest body)))}
                                 (catch Exception e
                                   {:status 500
                                    :body   {:msg (ex-message e)
                                             :ex-data (ex-data e)}})))))
            :operationId "delete-database"},
     :swagger {:tags ["Internal"]}}]
   ["/create-database-writer"
    {:post {:parameters  {:body [:sequential :any]},
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters}]
                           (let [cfg (dissoc (first body) :remote-peer :writer)]
                             (try
                               {:status 200
                                :body   (async/<!! (apply datahike.writing/create-database
                                                          cfg
                                                          (rest body)))}
                               (catch Exception e
                                 {:status 500
                                  :body   {:msg (ex-message e)
                                           :ex-data (ex-data e)}}))))
            :operationId "create-database"},
     :swagger {:tags ["Internal"]}}]
   ["/transact!-writer"
    {:post {:parameters  {:body [:sequential :any]},
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
      (println "Datahike HTTP Server" datahike-version "- https://datahike.io"))
    (log/info "Config:" (if token (assoc config :token "REDACTED") config))
    (start-server config)
    (log/info "Server started.")))
