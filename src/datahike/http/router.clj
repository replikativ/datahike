(ns datahike.http.router
  "Route generation and handlers for Datahike HTTP API.
   This namespace can be used in embedded mode without pulling in server dependencies."
  (:require [datahike.api :as d]
            [datahike.api.specification :refer [api-specification]]
            [datahike.tools :as dt]
            [clojure.string :as string]))

;; -----------------------------------------------------------------------------
;; Handler functions
;; -----------------------------------------------------------------------------

(defn wrap-api-handler
  "Wraps a Datahike API function as a Ring handler."
  [api-fn fn-name]
  (fn [request]
    (let [;; Support both direct body and Reitit-style parameters
          body (or (get-in request [:parameters :body])
                   (:body-params request)
                   (:body request)
                   [])
          ;; Support both vector (direct args) and map format
          [config args] (if (map? body)
                          [(:config body) (:args body [])]
                          [nil body])
          result (try
                   (case fn-name
                     create-database
                     (let [cfg (or config (first body))
                           rest-args (if config args (rest body))
                           res (apply api-fn cfg rest-args)]
                       (if (:remote-peer cfg)
                         (assoc res :remote-peer (:remote-peer cfg))
                         res))

                     delete-database
                     (let [cfg (or config (first body))
                           rest-args (if config args (rest body))]
                       (apply api-fn (dissoc cfg :remote-peer) rest-args))

                     (apply api-fn body))
                   (catch Exception e
                     {:error (str "Error in " fn-name ": " (.getMessage e))
                      :type (str (type e))}))]
      {:status (if (:error result) 500 200)
       :body result})))

(defn wrap-writer-handler
  "Wraps writer operations for distributed mode only."
  [api-fn fn-name]
  (fn [request]
    (let [body (or (get-in request [:parameters :body])
                   (:body-params request)
                   (:body request)
                   [])
          result (try
                   (case fn-name
                     delete-database-writer
                     (let [cfg (dissoc (first body) :remote-peer :writer)]
                       (apply d/delete-database cfg (rest body)))

                     create-database-writer
                     (let [cfg (dissoc (first body) :remote-peer :writer)]
                       (apply d/create-database cfg (rest body)))

                     transact!-writer
                     (let [cfg (dissoc (first body) :remote-peer :writer)]
                       (let [conn (d/connect cfg)
                             [_ tx-data tx-meta] body]
                         @(apply d/transact! conn tx-data (when tx-meta [tx-meta]))))

                     (apply api-fn body))
                   (catch Exception e
                     {:error (str "Error in writer " fn-name ": " (.getMessage e))
                      :type (str (type e))}))]
      {:status (if (:error result) 500 200)
       :body result})))

;; -----------------------------------------------------------------------------
;; Route generation
;; -----------------------------------------------------------------------------

(defn api-fn-name->path
  "Converts function name to URL path (e.g., create-database -> /create-database)."
  [fn-name]
  (str "/" (name fn-name)))

(defn generate-api-routes
  "Generates routes for all remotely-supported API functions.
   Each route has :path, :method, :handler, :name, and :doc keys."
  []
  (vec
   (for [[fn-name {:keys [supports-remote? referentially-transparent? doc]}] api-specification
         :when supports-remote?]
     (let [api-fn (resolve (symbol "datahike.api" (name fn-name)))
           method (if referentially-transparent? :get :post)]
       {:path (api-fn-name->path fn-name)
        :method method
        :handler (wrap-api-handler api-fn fn-name)
        :name (keyword fn-name)
        :doc doc}))))

(defn generate-writer-routes
  "Generates writer routes for distributed mode only."
  []
  [{:path "/delete-database-writer"
    :method :post
    :handler (wrap-writer-handler d/delete-database 'delete-database-writer)
    :name :delete-database-writer
    :doc "Internal endpoint for distributed writer - DO NOT USE DIRECTLY"}

   {:path "/create-database-writer"
    :method :post
    :handler (wrap-writer-handler d/create-database 'create-database-writer)
    :name :create-database-writer
    :doc "Internal endpoint for distributed writer - DO NOT USE DIRECTLY"}

   {:path "/transact!-writer"
    :method :post
    :handler (wrap-writer-handler d/transact! 'transact!-writer)
    :name :transact!-writer
    :doc "Internal endpoint for distributed writer - DO NOT USE DIRECTLY"}])

;; -----------------------------------------------------------------------------
;; Minimal middleware for embedded use
;; -----------------------------------------------------------------------------

(defn wrap-format-response
  "Handles response formatting."
  [handler]
  (fn [request]
    (let [response (handler request)]
      (if (and (map? response) (contains? response :body))
        response
        {:status 200 :body response}))))

(defn wrap-error-handling
  "Basic error handling middleware."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        {:status 500
         :body {:error (.getMessage e)
                :type (str (type e))}}))))

;; -----------------------------------------------------------------------------
;; Authentication middleware (compatible with server config)
;; -----------------------------------------------------------------------------

(defn extract-token
  "Extracts token from Authorization header (supports 'token' and 'Bearer' formats)."
  [request]
  (when-let [auth-header (get-in request [:headers "authorization"])]
    (let [parts (clojure.string/split auth-header #" ")]
      (when (>= (count parts) 2)
        (case (clojure.string/lower-case (first parts))
          "token" (second parts)
          "bearer" (second parts)
          nil)))))

(defn wrap-token-auth
  "Token authentication middleware. Uses :token from config, bypasses if :dev-mode is true."
  [handler config]
  (if (or (:dev-mode config) (nil? (:token config)))
    handler
    (fn [request]
      (let [token (extract-token request)]
        (if (= token (:token config))
          (handler request)
          {:status 401
           :body {:error "Not authorized"}})))))

;; -----------------------------------------------------------------------------
;; Route compilation for different routers
;; -----------------------------------------------------------------------------

(defn routes-for-reitit
  "Converts routes to Reitit format.
   Options:
   - :include-writers? - Include internal writer routes (default false)
   - :prefix - URL prefix for all routes (e.g., \"/datahike\" or \"/api/db\")
   - :middleware - Additional middleware to apply"
  [& {:keys [include-writers? prefix middleware]
      :or {include-writers? false
           prefix ""
           middleware []}}]
  (let [api-routes (generate-api-routes)
        writer-routes (when include-writers? (generate-writer-routes))
        all-routes (concat api-routes writer-routes)]
    (vec
     (for [{:keys [path method handler name doc]} all-routes]
       [(str prefix path)
        {method handler
         :name name
         :summary doc
         :middleware middleware}]))))

(defn routes-for-compojure
  "Converts routes to Compojure format.
   Returns a function that can be used with defroutes.
   Options:
   - :include-writers? - Include internal writer routes (default false)
   - :prefix - URL prefix for all routes (e.g., \"/datahike\" or \"/api/db\")"
  [& {:keys [include-writers? prefix]
      :or {include-writers? false
           prefix ""}}]
  (let [api-routes (generate-api-routes)
        writer-routes (when include-writers? (generate-writer-routes))
        all-routes (concat api-routes writer-routes)]
    (fn []
      (vec
       (for [{:keys [path method handler]} all-routes]
         (let [full-path (str prefix path)]
           (case method
             :get `(~'GET ~full-path request# (~handler request#))
             :post `(~'POST ~full-path request# (~handler request#)))))))))

(defn routes-for-ring
  "Returns a simple Ring handler that matches routes.
   Options:
   - :include-writers? - Include internal writer routes (default false)
   - :prefix - URL prefix for all routes (e.g., \"/datahike\" or \"/api/db\")
   - :not-found-handler - Handler for unmatched routes"
  [& {:keys [include-writers? prefix not-found-handler]
      :or {include-writers? false
           prefix ""
           not-found-handler (fn [_] {:status 404 :body "Not found"})}}]
  (let [api-routes (generate-api-routes)
        writer-routes (when include-writers? (generate-writer-routes))
        all-routes (concat api-routes writer-routes)
        route-map (reduce
                   (fn [m {:keys [path method handler]}]
                     (assoc-in m [(str prefix path) method] handler))
                   {}
                   all-routes)]
    (fn [request]
      (if-let [handler (get-in route-map [(:uri request) (:request-method request)])]
        (handler request)
        (not-found-handler request)))))

;; -----------------------------------------------------------------------------
;; Public API
;; -----------------------------------------------------------------------------

(defn create-routes
  "Creates routes for Datahike HTTP API.

   Options:
   - :format - Router format (:reitit, :compojure, :ring, or :raw)
   - :prefix - URL prefix for all routes (e.g., \"/datahike\" or \"/api/db\")
   - :include-writers? - Include internal writer routes (for distributed mode)
   - :middleware - Additional middleware (for formats that support it)

   Returns routes in the specified format:
   - :raw - Vector of route maps (prefix added to path field)
   - :reitit - Reitit-compatible route data
   - :compojure - Compojure-compatible routes
   - :ring - Simple Ring handler function"
  [& {:keys [format prefix include-writers? middleware]
      :or {format :raw
           prefix ""
           include-writers? false
           middleware []}}]
  (case format
    :raw (let [routes (concat (generate-api-routes)
                              (when include-writers? (generate-writer-routes)))]
           (if (empty? prefix)
             routes
             (map #(update % :path (fn [p] (str prefix p))) routes)))
    :reitit (routes-for-reitit :include-writers? include-writers?
                               :prefix prefix
                               :middleware middleware)
    :compojure (routes-for-compojure :include-writers? include-writers?
                                     :prefix prefix)
    :ring (routes-for-ring :include-writers? include-writers?
                           :prefix prefix)
    (let [routes (concat (generate-api-routes)
                         (when include-writers? (generate-writer-routes)))]
      (if (empty? prefix)
        routes
        (map #(update % :path (fn [p] (str prefix p))) routes)))))

(defn create-ring-handler
  "Creates a Ring handler for embedded use.

   Options:
   - :config - Auth config (:token, :dev-mode, :level)
   - :prefix - URL prefix (e.g., \"/datahike\", \"/api/db\")
   - :include-writers? - Include writer routes (default false)
   - :middleware - Additional Ring middleware
   - :not-found-handler - Custom 404 handler"
  [& {:keys [config prefix include-writers? middleware not-found-handler]
      :or {config {}
           prefix ""
           include-writers? false
           middleware []
           not-found-handler (fn [_] {:status 404 :body "Not found"})}}]
  (let [base-handler (routes-for-ring :include-writers? include-writers?
                                      :prefix prefix
                                      :not-found-handler not-found-handler)
        handler-with-auth (if (or (:token config) (:dev-mode config))
                            (wrap-token-auth base-handler config)
                            base-handler)]
    (reduce (fn [h mw] (mw h))
            handler-with-auth
            (reverse middleware))))