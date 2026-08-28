(ns datahike.http.routes
  "Datahike's HTTP API as Ring routes, for embedding.

   Everything the server serves is here as DATA — reitit route vectors built
   from `datahike.api.specification` — plus the muuntaja instance and middleware
   chain that make them work: edn, json, transit and CBOR, malli coercion of
   request bodies, token auth. `datahike.http.server` is a thin main over this
   namespace; an application with its own Ring stack takes `handler` (or the
   route data itself) and mounts it where it likes.

   Two things an embedding host needs beyond the routes, both explicit here:

   - `handler` takes a `:prefix`, so the API can live under `/datahike` (or
     anything) in a host that already owns `/`. A remote writer or client then
     names that prefix in its `:url`.
   - The connections the routes open live in an atom the host passes in (or
     one made for it), bound as `datahike.connections/*connections*` for every
     request. A host that also uses those databases directly shares them by
     binding the same atom — one connection per database per process, not one
     per caller.

   The shape follows alekcz's `datahike.http.router` (#755): routes without a
   server, a mountable handler, a shared registry. What differs is that nothing
   is re-implemented — the route generation, formats and auth are the server's
   own, moved here, so the two cannot drift and CBOR (what the remote writer
   speaks) comes along."
  (:refer-clojure :exclude [read-string filter])
  (:require
   [clojure.string :as str]
   [clojure.core.async :as async]
   [datahike.connections :refer [*connections*]]
   [datahike.store]
   [reitit.core :as reitit]
   [datahike.api.specification :refer [api-specification ->url]]
   [datahike.api.types :as types]
   [datahike.http.middleware :as middleware]
   [datahike.readers :refer [edn-readers]]
   [datahike.transit :as transit]
   [datahike.remote.cbor :as rcbor]
   [datahike.http.cbor :as cbor]
   [datahike.json :as json]
   [datahike.api :refer :all :as api]
   [datahike.writing]
   [datahike.writer]
   [reitit.ring :as ring]
   [reitit.coercion.malli]
   [malli.util :as mu]
   [reitit.swagger :as swagger]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.muuntaja :as muuntaja]
   [reitit.ring.middleware.exception :as exception]
   [reitit.ring.middleware.multipart :as multipart]
   [reitit.ring.middleware.parameters :as parameters]
   [muuntaja.core :as m]
   [replikativ.logging :as log]))

(def ^:private secret-keys
  "Config keys whose values must never leave the server in an error body."
  #{:token :secret :access-key :secret-key :password})

(defn- redact
  "ex-data with secret-valued keys blanked, at any depth."
  [x]
  (cond (map? x)  (into {} (map (fn [[k v]] [k (if (secret-keys k) "REDACTED" (redact v))])) x)
        (coll? x) (into (empty x) (map redact) x)
        :else     x))

(defn- error-response
  "The 500 body the clients decode into a throwable: message plus ex-data,
   with credentials redacted — a backend config in ex-data used to carry them
   straight to the caller."
  [e]
  {:status 500
   :body   {:msg (ex-message e) :ex-data (redact (ex-data e))}})

(defn- borrow-connection
  "The server's ONE connection to `cfg`'s database, opened on first use.

   `api/connect` on a cached connection increments a ref count that a request
   would never decrement, so the writer routes borrow the existing connection
   from `*connections*` and connect only when there is none. The server holds
   exactly one lease per database and releases it on shutdown
   (`release-all!`); requests change no counts."
  [cfg]
  (let [conn-id [(datahike.store/store-identity (:store cfg)) (:branch cfg :db)]]
    (or (get-in @*connections* [conn-id :conn])
        (api/connect cfg))))

(defn release-all!
  "Release every connection in `connections`, for a server shutting down."
  [connections]
  (doseq [[_ {:keys [conn]}] @connections]
    (when conn
      (try (api/release conn true) (catch Exception _)))))

(defn generic-handler [config f]
  (fn [request]
    (try
      (let [{{body :body} :parameters
             :keys [headers params method]} request
            _ (log/trace :datahike/http-handler-request {:handler f :body body})
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
        (log/trace :datahike/http-handler-response {:body ret-body})
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

(defn has-cat-operators?
  "Check if args list contains :cat-specific operators like [:* ...], [:+ ...], [:alt ...], etc.
   These operators are only valid in :cat schemas, not in :tuple schemas."
  [args]
  (some #(and (vector? %) (#{:* :+ :? :alt :altn} (first %))) args))

(defn extract-input-schema
  "Extract input schema from malli function schema for HTTP body validation.
   Converts [:=> [:cat Type1 Type2] ret] to [:tuple Type1 Type2]
   or [:function [:=> [:cat T1] ret] [:=> [:cat T1 T2] ret]] to [:or [:tuple T1] [:tuple T1 T2]]

   The HTTP body is a tuple/vector of arguments that matches the function signature.
   For zero-arity functions, we use [:= []] to match an empty vector.
   For functions with :cat operators ([:* ...], [:alt ...], etc), we use [:sequential :any]
   since tuples can't express these dynamic patterns."
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
                          (cond
                            (not (seq args)) [:= []]  ;; Zero-arity
                            (has-cat-operators? args) [:sequential :any]  ;; Has :cat operators - can't use tuple
                            :else (vec (cons :tuple args))))]  ;; Fixed arity
      (if (> (count input-schemas) 1)
        (vec (cons :or input-schemas))
        (first input-schemas)))

    ;; Single arity: [:=> [:cat Type1 Type2] ret]
    (and (vector? schema) (= :=> (first schema)))
    (let [[_ input-schema _] schema]
      (if (and (vector? input-schema) (= :cat (first input-schema)))
        (let [args (rest input-schema)]
          (cond
            (not (seq args)) [:= []]  ;; Zero-arity
            (has-cat-operators? args) [:sequential :any]  ;; Has :cat operators
            :else (vec (cons :tuple args))))  ;; Fixed arity
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

;; One registry per process, not per request: building it walks every handler
;; and the result is immutable.
(def ^:private cbor-registry (rcbor/server-registry))

(def muuntaja-with-opts
  (m/create
   (-> m/default-options
       ;; `application/cbor` is an addition, not a replacement — it takes its
       ;; place beside edn/json/transit and is reached only when a client asks
       ;; for it by Accept or Content-Type. The format carries its own codec
       ;; options, so there is no second place for them to drift from.
       (assoc-in [:formats "application/cbor"] (cbor/cbor-format cbor-registry))
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

(defn internal-writer-routes []
  [["/delete-database-writer"
    {:post {:parameters  {:body [:sequential :any]},
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters}]
                           ;; Deletion is process-wide by nature: it releases
                           ;; and invalidates every connection to the store,
                           ;; the host's included. A host sharing the atom
                           ;; must treat it as such.
                           (let [cfg (dissoc (first body) :remote-peer :writer)]
                             (try
                               (try
                                 (api/release (api/connect cfg) true)
                                 (catch Exception _))
                               {:status 200
                                :body   (async/<!! (apply datahike.writing/delete-database cfg (rest body)))}
                               (catch Exception e
                                 (error-response e)))))
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
                                 (error-response e)))))
            :operationId "create-database"},
     :swagger {:tags ["Internal"]}}]
   ["/transact!-writer"
    {:post {:parameters  {:body [:sequential :any]},
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters}]
                           (try
                             (let [conn (borrow-connection (dissoc (first body) :remote-peer :writer))
                                   res  @(apply datahike.writer/transact! conn (rest body))]
                               {:status 200
                                :body   res})
                             (catch Exception e
                               (error-response e))))
            :operationId "transact"},
     :swagger {:tags ["Internal"]}}]])

;; ---------------------------------------------------------------------------
;; The embeddable surface
;; ---------------------------------------------------------------------------

(defn- with-auth
  "Every API route behind the token/auth middleware, as `app` always applied
   them — an embedded handler must not be less protected than the server."
  [config routes]
  (map (fn [route]
         (let [method (if (:get (second route)) :get :post)]
           (assoc-in route [1 method :middleware]
                     [(partial middleware/token-auth config)
                      (partial middleware/auth config)])))
       routes))

(defn api-routes
  "The API and internal writer routes as reitit route data, authenticated."
  [config]
  (with-auth config (concat (create-routes config)
                            (internal-writer-routes))))

(defn- normalize-prefix
  "\"/datahike/\" and \"datahike\" mean \"/datahike\"; \"\" and \"/\" mean none."
  [prefix]
  (let [p (str/replace (str prefix) #"^/*|/*$" "")]
    (when (seq p) (str "/" p))))

(defn router
  "A reitit router over `api-routes`, with Datahike's coercion, formats and
   middleware. `:prefix` nests every route under a path; `:extra-routes` are
   the host's own routes on the same router — the server adds `/swagger.json`
   this way, marked `:public? true` so the gate lets it through unauthenticated."
  [config {:keys [prefix extra-routes]}]
  (let [routes (vec (concat extra-routes (api-routes config)))]
    (ring/router (if-let [p (normalize-prefix prefix)] [p routes] routes)
                 (default-route-opts muuntaja-with-opts))))

(def default-max-body-bytes (* 64 1024 1024))

(defn- limited-stream
  "`in`, failing past `limit` bytes and flagging `exceeded` — so a chunked
   body cannot bypass the Content-Length check by omitting it."
  [^java.io.InputStream in ^long limit exceeded]
  (let [read-so-far (atom 0)
        check!      (fn [n] (when (and (pos? n) (> (swap! read-so-far + n) limit))
                              (reset! exceeded true)
                              (throw (java.io.IOException. "Request body too large"))))]
    (proxy [java.io.FilterInputStream] [in]
      (read
        ([] (let [b (.read in)] (check! (if (neg? b) 0 1)) b))
        ([^bytes buf] (let [n (.read in buf)] (check! n) n))
        ([^bytes buf off len] (let [n (.read in buf off len)] (check! n) n))))))

(def ^:private too-large {:status 413 :body "Request body too large"})

(defn wrap-api
  "Everything a Datahike API request needs around the router's handler, in
   the order that matters:

   1. The gate. Before ANY decoding: a request the router does not route is
      handed on untouched (the host's, or the server's swagger-ui and 404); a
      public route (`:public? true`, i.e. `/swagger.json`) passes; every other
      route requires the token here, and its body is capped at
      `:max-body-bytes` (default 64 MiB), Content-Length and stream alike. The
      per-route auth middleware still runs — this only makes sure nothing is
      parsed, and no database handle decoded, for a caller that has no token.
   2. The registry. `*connections*` is bound to `connections` for the WHOLE
      request, decoding included, so every route — `connect`, `q`, `release`,
      a database handle inside a body — resolves the same connection a host
      holds under that atom."
  [ring-handler rtr config connections]
  (let [max-bytes (or (:max-body-bytes config) default-max-body-bytes)
        authed?   (fn [request]
                    (or (:dev-mode config)
                        (= (str "token " (:token config))
                           (get-in request [:headers "authorization"]))))]
    (fn [request]
      (binding [*connections* connections]
        (if-let [match (reitit/match-by-path rtr (:uri request))]
          (cond
            (get-in match [:data :public?]) (ring-handler request)
            (not (authed? request))          {:status 401 :body "Not authorized"}
            (> (or (some-> (get-in request [:headers "content-length"]) parse-long) 0) max-bytes)
            too-large
            :else
            (let [exceeded (atom false)
                  response (ring-handler (cond-> request
                                           (:body request) (update :body limited-stream max-bytes exceeded)))]
              (if @exceeded too-large response)))
          (ring-handler request))))))

(defn handler
  "A Ring handler serving Datahike's HTTP API, for mounting in a host app.

     (routes/handler {:token \"…\"} {:prefix \"/datahike\" :connections conns})

   Requests the router does not match fall through as 404 — CORS, static
   files and the host's other routes are the host's business. `connections`
   defaults to a fresh atom; pass your own to share databases with the host:
   the host's `(binding [*connections* conns] (d/connect cfg))` and a client's
   `/connect` then resolve the identical connection. Deletion through the API
   invalidates every connection to that database, the host's included.
   Call `release-all!` on the atom when the host shuts down."
  ([config] (handler config {}))
  ([config {:keys [connections] :as opts}]
   (let [connections (or connections (atom {}))
         rtr         (router config opts)]
     (wrap-api (ring/ring-handler rtr (ring/create-default-handler))
               rtr config connections))))
