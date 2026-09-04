(ns datahike.http.routes
  "Datahike's HTTP API as a Ring handler, for embedding.

   Everything the server serves is here — reitit route data built from
   `datahike.api.specification`, the muuntaja instance (edn, json, transit,
   CBOR), malli coercion, the writer routes — behind ONE composition unit,
   `handler`, that adds what the routes cannot do for themselves: authenticate
   before anything is decoded, cap the body, bind the connection registry for
   the whole request, and authorize each call. `datahike.http.server` is a thin
   main over it; an application with its own Ring stack mounts it under a
   `:prefix`, shares its databases through `:connections`, and lets its own
   auth in through `:validator`/`:authorize`. See doc/http-routes.md.

   The shape follows alekcz's `datahike.http.router` (#755): routes without a
   server, a mountable handler, a shared registry. What differs is that nothing
   is re-implemented — the route generation, formats and auth are the server's
   own, moved here, so the two cannot drift and CBOR (what the remote writer
   speaks) comes along."
  (:refer-clojure :exclude [read-string filter])
  (:require
   [datahike.http.stores :as stores]
   [datahike.query.resolve :as qr]
   [clojure.string :as str]
   [clojure.core.async :as async]
   [datahike.connections :refer [*connections*]]
   [datahike.store]
   [reitit.core :as reitit]
   [datahike.api.specification :refer [api-specification ->url]]
   [datahike.api.types :as types]
   [datahike.http.middleware :as middleware]
   [datahike.metrics :as metrics]
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
   [replikativ.metrics :as registry]
   [replikativ.logging :as log])
  (:import [datahike.datom Datom]
           [datahike.impl.entity Entity]
           [java.util.concurrent.locks ReentrantReadWriteLock]))

;; ---------------------------------------------------------------------------
;; Errors, secrets, authorization
;; ---------------------------------------------------------------------------

(def ^:private secret-keys
  "Config keys whose values must never leave the server in an error body."
  #{:token :secret :access-key :secret-key :password
    :account-key :api-key :client-secret :connection-string
    :credential :credentials :private-key :sas-token})

(defn redact
  "`x` with secret-valued keys blanked, at any depth."
  [x]
  (cond (map? x)  (into {} (map (fn [[k v]] [k (if (secret-keys k) "REDACTED" (redact v))])) x)
        (coll? x) (into (empty x) (map redact) x)
        :else     x))

(defn error-response
  "The 500 body the clients decode into a throwable: message plus ex-data,
   with credentials redacted — a backend config in ex-data used to carry them
   straight to the caller."
  [e]
  {:status 500
   :body   {:msg (ex-message e) :ex-data (redact (ex-data e))}})

(defn- store-refused
  "403 for a store the server's `:create-database` policy does not allow."
  [e]
  (metrics/http-rejected! :forbidden)
  {:status 403
   :body   {:msg (ex-message e) :ex-data (redact (ex-data e))}})

(defn forbidden
  "The 403 for `op` on `databases` (`[{:store-id :branch}]`, may be empty)."
  [op databases]
  (metrics/http-rejected! :forbidden)
  {:status 403
   :body   {:msg     (str "Forbidden: " (name op)
                          (when (seq databases)
                            (str " on " (str/join ", " (map (comp str :store-id) databases)))))
            :ex-data {:type :datahike.http/forbidden :op op :databases databases}}})

(declare store-config?)

(defn- config-of
  "The database config behind an argument: a config map, a DB (or a history,
   as-of, since or filtered view of one), a connection, or an entity."
  [x]
  (cond (instance? clojure.lang.IDeref x) (config-of @x)
        (instance? Entity x) (config-of (.-db ^Entity x))
        ;; A DB carries its konserve store under :store too, so :config first.
        (map? x) (cond (:config x)        (config-of (:config x))
                       (:origin-db x)     (config-of (:origin-db x))
                       (:unfiltered-db x) (config-of (:unfiltered-db x))
                       (store-config? (:store x)) x
                       :else              nil)
        :else nil))

(defn- store-config?
  "A Datahike store config, not an application's own `:store` attribute: a
   map naming a `:backend` and carrying the store's UUID."
  [store]
  (and (map? store)
       (keyword? (:backend store))
       (uuid? (datahike.store/store-identity store))))

(defn- database-of [x]
  (when-let [{:keys [store branch]} (config-of x)]
    (when (store-config? store)
      {:store-id (datahike.store/store-identity store) :branch (or branch :db)})))

(defn- descend?
  "Walk into collections that can carry a database — not into a DB's own
   internals, a datom, or bytes."
  [x]
  (and (coll? x) (nil? (database-of x)) (not (instance? Datom x))))

(defn databases
  "Every database `args` reaches, searched in full — a write's transaction
   data included, since a transaction function can be handed a connection or
   database value for another database and read it. Handles are what the wire
   formats decode them to (connections, DB values and their views, entities)
   plus config maps; the walk is linear in the size of the arguments, which
   the request already paid to decode."
  [args]
  (->> args
       (mapcat #(tree-seq descend? seq %))
       (keep database-of)
       distinct
       vec))

(defn authorize
  "The 403 for `op` with `args` under `config`'s `:authorize`, or nil when the
   call may proceed. `:authorize` is `(fn [{:keys [op principal db payload]}])`
   and is asked once per database the call reaches — every one must be
   allowed — or once with `:db nil` when it reaches none. Without `:authorize`
   every authenticated caller may do everything."
  [config request op args]
  (when-let [policy (:authorize config)]
    (let [dbs       (databases args)
          principal (:datahike/principal request)
          ask       (fn [db] (policy {:op op :principal principal :db db :payload args}))]
      (when-not (if (seq dbs) (every? ask dbs) (ask nil))
        (forbidden op dbs)))))

(defn route-op
  "What an API function does, for authorization: every GET is a `:read`, and
   so are the POSTs that only open, inspect or close; the rest write."
  [n referentially-transparent?]
  (cond referentially-transparent? :read
        ('#{create-database} n) :create
        ('#{delete-database delete-branch!} n) :delete
        ('#{connect release db database-exists? branches branch-as-db commit-as-db} n) :read
        ('#{gc-storage} n) :admin
        :else :transact))

;; ---------------------------------------------------------------------------
;; Connections the routes hold
;; ---------------------------------------------------------------------------

(defn- conn-id [cfg]
  [(datahike.store/store-identity (:store cfg)) (:branch cfg :db)])

(defn- new-state
  "What the routes keep per handler: `leases`, per database — `:base`, whether
   the server holds its base lease, and `:api`, how many leases it has handed
   out through `connect` — and a read-write lock that lets a delete wait for
   the calls pinned on the database it removes."
  []
  {:leases (atom {})
   :lock   (ReentrantReadWriteLock.)})

(defn- with-connection
  "Run `f` with a connection to `cfg`'s database, pinned for the duration.

   Two leases are at work. This request's own — `api/connect` validates the
   config against the cached connection and counts one up, `api/release` in
   `finally` counts it back — which keeps a concurrent release from pulling
   the connection out from under the call. And the server's base lease, taken
   once per database so that the connection (and its writer) survives between
   requests instead of being rebuilt for each; `release-all!` drops it on
   shutdown, a delete drops it. The call holds the read side of the lock, so
   a delete (`with-exclusive`) waits for it."
  [cfg {:keys [leases ^ReentrantReadWriteLock lock]} f]
  (let [id (conn-id cfg)]
    (.lock (.readLock lock))
    (try
      (locking leases
        (when-not (and (get-in @leases [id :base]) (get-in @*connections* [id :conn]))
          (api/connect cfg)
          (swap! leases assoc-in [id :base] true)))
      (let [conn (api/connect cfg)]
        (try (f conn)
             (finally (api/release conn))))
      (finally (.unlock (.readLock lock))))))

(defn- with-exclusive
  "Run `f`, a deletion, once every pinned call has finished and none starts."
  [{:keys [^ReentrantReadWriteLock lock]} f]
  (.lock (.writeLock lock))
  (try (f)
       (finally (.unlock (.writeLock lock)))))

(defn- granted!
  "Count a lease handed out through `connect`."
  [{:keys [leases]} conn]
  (swap! leases update-in [(conn-id (:config @conn)) :api] (fnil inc 0))
  conn)

(defn- give-back!
  "Release one lease `connect` handed out — and only such a lease: a caller
   releasing more often than it connected must not drain the host's or the
   server's own."
  [{:keys [leases]} conn]
  (when-let [id (try (conn-id (:config @conn)) (catch Exception _ nil))]
    ;; Check and decrement in one swap: two releases racing on the last
    ;; granted lease must not both succeed.
    (let [[before after] (swap-vals! leases
                                     (fn [m] (if (pos? (get-in m [id :api] 0))
                                               (update-in m [id :api] dec)
                                               m)))]
      (when (not= before after)
        (api/release conn)))))

(defn release-all!
  "Release every connection in the registry — the routes' and, if the host
   shared its atom, the host's own — for a process shutting down. Takes the
   connections atom or the handler `handler` returned."
  [handler-or-connections]
  (let [connections (or (::connections (meta handler-or-connections)) handler-or-connections)]
    (binding [*connections* connections]
      (doseq [[_ {:keys [conn]}] @connections]
        (when conn
          (try (api/release conn true) (catch Exception _)))))))

;; ---------------------------------------------------------------------------
;; The API routes
;; ---------------------------------------------------------------------------

(defn- generic-handler [config state op f]
  (fn [request]
    (try
      (let [{{body :body} :parameters
             :keys [headers params]
             method :request-method} request]
        (log/trace :datahike/http-handler-request {:handler f :op op})
        (or (authorize config request op body)
            ;; The server IS the writer: a config's :writer names a client's
            ;; way to reach one (possibly this very server, which would loop
            ;; back over HTTP into a lock this route holds), never the
            ;; server's own. :remote-peer likewise.
            (let [local    (fn [cfg] (dissoc cfg :remote-peer :writer))
                  ret-body
                  (cond (= f #'api/create-database)
                        (with-exclusive state
                          (fn []
                            ;; Remove remote-peer while this server creates the
                            ;; physical store, then restore it in the response.
                            (let [requested (stores/assign (:create-database config) (local (first body)))
                                  principal (:datahike/principal request)]
                              (when-let [prepare! (:datahike.http.system/prepare-register! config)]
                                (prepare! requested principal))
                              (let [created (apply f requested (rest body))]
                                (when-let [register! (:datahike.http.system/register! config)]
                                  (register! created principal))
                                (assoc created :remote-peer (:remote-peer (first body)))))))

                        (= f #'api/delete-database)
                        (with-exclusive state
                          (fn []
                            (let [id           (conn-id (first body))
                                  lease-state  (get @(:leases state) id)
                                  local-config (local (first body))
                                  principal    (:datahike/principal request)]
                              (when-let [prepare! (:datahike.http.system/prepare-delete! config)]
                                (prepare! local-config principal))
                              (swap! (:leases state) dissoc id)
                              (let [result
                                    (try
                                      (apply f local-config (rest body))
                                      (catch Throwable e
                                        (when lease-state
                                          (swap! (:leases state) assoc id lease-state))
                                        (when-let [cancel! (:datahike.http.system/cancel-delete! config)]
                                          (try
                                            (cancel! local-config principal)
                                            (catch Throwable cleanup-error
                                              (log/error :datahike/catalog-delete-rollback-failed
                                                         (ex-message cleanup-error)
                                                         {:error-class (.getName (class cleanup-error))}))))
                                        (throw e)))]
                                (when-let [deleted! (:datahike.http.system/delete! config)]
                                  (deleted! local-config principal))
                                result))))

                        (= f #'api/connect)
                        (granted! state (apply f (cons (local (first body)) (rest body))))

                        ;; One caller releases one lease of its own.
                        ;; `release-all?` would close a connection the host
                        ;; and other callers share.
                        (= f #'api/release)
                        (give-back! state (first body))

                        :else
                        (apply f body))]
              (merge
               {:status 200
                :body
                (when-not (headers "no-return-value")
                  ret-body)}
               (when (and (= method :get)
                          (or (get params "args-id") (get params "args"))
                          (get-in config [:cache :get :max-age]))
                 {:headers {"Cache-Control" (str (when (:dev-mode config) "public, ")
                                                 "max-age=" (get-in config [:cache :get :max-age]))}})))))
      (catch Exception e
        (if (= :datahike.http/store-refused (:type (ex-data e)))
          (store-refused e)
          (error-response e))))))

(declare create-routes)

(defn- extract-first-sentence [doc]
  (str (first (str/split doc #"\.\s")) "."))

(defn- has-cat-operators?
  "Check if args list contains :cat-specific operators like [:* ...], [:+ ...], [:alt ...], etc.
   These operators are only valid in :cat schemas, not in :tuple schemas."
  [args]
  (some #(and (vector? %) (#{:* :+ :? :alt :altn} (first %))) args))

(defn- extract-input-schema
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

;; One registry per process, not per request: building it walks every handler
;; and the result is immutable.
(def ^:private cbor-registry (rcbor/server-registry))

(def ^:private muuntaja-with-opts
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

(def ^:private url-args-formats
  {"cbor"    "application/cbor"
   "transit" "application/transit+json"
   "edn"     "application/edn"
   "json"    "application/json"})

(defn- url-args-middleware
  "A GET may carry its arguments in the URL: `args` is the argument vector,
   encoded in the format `f` names (`cbor` by default) and base64url'd. A
   browser cannot send a GET with a body, and a URL is what an HTTP cache
   keys on, so this is how a browser client gets a cached read. Decoded into
   `:body-params`, where the body would have landed, so coercion and the
   handler see no difference."
  [muuntaja-with-opts]
  (fn [handler]
    (fn [request]
      (if-let [encoded (and (= :get (:request-method request))
                            (get-in request [:query-params "args"]))]
        (let [format (get url-args-formats (get-in request [:query-params "f"] "cbor"))]
          (when-not format
            (throw (ex-info "Unknown args format" {:type :datahike.http/bad-request
                                                   :f (get-in request [:query-params "f"])})))
          (handler (assoc request :body-params
                          (m/decode muuntaja-with-opts format
                                    (java.io.ByteArrayInputStream.
                                     (.decode (java.util.Base64/getUrlDecoder) ^String encoded))))))
        (handler request)))))

(defn- default-route-opts [muuntaja-with-opts]
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
                            (url-args-middleware muuntaja-with-opts)
                            (middleware/encode-plain-value muuntaja-with-opts)
                            middleware/support-embedded-edn-in-json
                            coercion/coerce-response-middleware
                            coercion/coerce-request-middleware
                            multipart/multipart-middleware
                            middleware/patch-swagger-json]}})

;; This code expands and evals the server route construction given the
;; API specification.
(eval
 `(defn- ~'create-routes [~'config ~'state]
    ~(vec
      (for [[n {:keys [args doc supports-remote? referentially-transparent?]}] api-specification
            :when supports-remote?]
        (let [route {:operationId (str n)
                     :metric-op   (route-op n referentially-transparent?)
                     :summary     (extract-first-sentence doc)
                     :description doc
                     :parameters  {:body (extract-input-schema args)}
                     :handler     `(generic-handler ~'config ~'state ~(route-op n referentially-transparent?) ~(resolve n))}]
          `[~(str "/" (->url n))
            ;; A read is a GET, cached by its URL: the arguments travel in the
            ;; `args` parameter (`url-args-middleware`) or, for a client that
            ;; can, in the body. It answers POST as well, for arguments too
            ;; large for a URL; that path carries no cache headers.
            ~(if referentially-transparent?
               {:swagger {:tags ["API"]} :get route :post (assoc route :no-doc true)}
               {:swagger {:tags ["API"]} :post route})])))))

(defn- internal-writer-routes
  "What a `:datahike-server` writer posts to; `state` as in `new-state`."
  [config state]
  [["/delete-database-writer"
    {:post {:parameters  {:body [:sequential :any]},
            :metric-op   :delete
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters :as request}]
                           ;; Deletion is process-wide by nature: it releases
                           ;; and invalidates every connection to the store,
                           ;; the host's included. A host sharing the atom
                           ;; must treat it as such.
                           (let [cfg (dissoc (first body) :remote-peer :writer)]
                             (or (authorize config request :delete (cons cfg (rest body)))
                                 (try
                                   (with-exclusive state
                                     (fn []
                                       (swap! (:leases state) dissoc (conn-id cfg))
                                       (try
                                         (api/release (api/connect cfg) true)
                                         (catch Exception _))
                                       {:status 200
                                        :body   (async/<!! (apply datahike.writing/delete-database cfg (rest body)))}))
                                   (catch Exception e
                                     (error-response e))))))
            :operationId "delete-database"},
     :swagger {:tags ["Internal"]}}]
   ["/create-database-writer"
    {:post {:parameters  {:body [:sequential :any]},
            :metric-op   :create
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters :as request}]
                           (let [cfg (dissoc (first body) :remote-peer :writer)]
                             (or (authorize config request :create (cons cfg (rest body)))
                                 (try
                                   {:status 200
                                    :body   (async/<!! (apply datahike.writing/create-database
                                                              (stores/assign (:create-database config) cfg)
                                                              (rest body)))}
                                   (catch Exception e
                                     (if (= :datahike.http/store-refused (:type (ex-data e)))
                                       (store-refused e)
                                       (error-response e)))))))
            :operationId "create-database"},
     :swagger {:tags ["Internal"]}}]
   ["/transact!-writer"
    {:post {:parameters  {:body [:sequential :any]},
            :metric-op   :transact
            :summary     "Internal endpoint. DO NOT USE!"
            :no-doc      true
            :handler     (fn [{{:keys [body]} :parameters :as request}]
                           (let [cfg (dissoc (first body) :remote-peer :writer)]
                             (or (authorize config request :transact (cons cfg (rest body)))
                                 (try
                                   {:status 200
                                    :body   (with-connection cfg state
                                              (fn [conn] @(apply datahike.writer/transact! conn (rest body))))}
                                   (catch Exception e
                                     (error-response e))))))
            :operationId "transact"},
     :swagger {:tags ["Internal"]}}]])

;; ---------------------------------------------------------------------------
;; The embeddable surface
;; ---------------------------------------------------------------------------

(defn- normalize-prefix
  "\"/datahike/\" and \"datahike\" mean \"/datahike\"; \"\" and \"/\" mean none."
  [prefix]
  (let [p (str/replace (str prefix) #"^/*|/*$" "")]
    (when (seq p) (str "/" p))))

(defn- router
  [config {:keys [prefix extra-routes state]}]
  (let [routes (vec (concat extra-routes
                            (create-routes config state)
                            (internal-writer-routes config state)))]
    (ring/router (if-let [p (normalize-prefix prefix)] [p routes] routes)
                 (default-route-opts muuntaja-with-opts))))

(def default-max-body-bytes (* 64 1024 1024))

(defn- read-body
  "The whole body as bytes, or `::too-large` past `limit` — read up front, so
   no decoder ever sees an unbounded stream and a body that a decoder would
   read only the first form of is still measured in full."
  [^java.io.InputStream in ^long limit]
  (let [out (java.io.ByteArrayOutputStream.)
        buf (byte-array 8192)]
    (loop [total 0]
      (let [n (.read in buf)]
        (cond (neg? n)             (.toByteArray out)
              (> (+ total n) limit) ::too-large
              :else                (do (.write out buf 0 n)
                                       (recur (+ total n))))))))

(def ^:private too-large {:status 413 :body "Request body too large"})

(defn- wrap-api
  "Everything a Datahike API request needs around the router's handler, in
   the order that matters:

   1. The gate, before ANY decoding. A request the router does not route, or
      routes but not for that method (an OPTIONS preflight, a wrong method),
      is handed on untouched — the host's, CORS, swagger-ui, 404, 405. For a
      routed one: the body is read in full and capped at `:max-body-bytes`
      (default 64 MiB), public route or not, so no decoder sees an unbounded
      stream; then, unless the route is `:public? true` (`/swagger.json`),
      the caller must authenticate — `:token`, `:validator`, `:dev-mode`,
      `:auth :upstream`, see `middleware/validators` — or gets 401 with
      nothing parsed. The principal is on the request as
      `:datahike/principal` for the route's authorization.
   2. The registry. `*connections*` is bound to `connections` for the WHOLE
      request, decoding included, so every route — `connect`, `q`, `release`,
      a database handle inside a body — resolves the same connection a host
      holds under that atom."
  [ring-handler rtr config connections]
  (let [max-bytes  (or (:max-body-bytes config) default-max-body-bytes)
        validators (middleware/validators config)]
    (fn [request]
      (binding [*connections* connections]
        (let [method     (:request-method request)
              match      (reitit/match-by-path rtr (:uri request))
              matched?   (and match (contains? (:data match) method))
              metric-op  (get-in match [:data method :metric-op])
              started    (when metric-op (registry/timer))
              response
              (if-not matched?
                (ring-handler request)
                (let [body (when (:body request) (read-body (:body request) max-bytes))
                      ;; Buffered before anyone reads it: a validator that looks
                      ;; at the body (a signed request, say) gets its own copy and
                      ;; the route still gets the whole thing.
                      buffered  (fn [] (cond-> request body (assoc :body (java.io.ByteArrayInputStream. body))))
                      principal (when-not (= ::too-large body)
                                  (middleware/authenticate validators (buffered)))]
                  (cond
                    (> (or (some-> (get-in request [:headers "content-length"]) parse-long) 0) max-bytes)
                    (do (metrics/http-rejected! :too-large) too-large)

                    (= ::too-large body)
                    (do (metrics/http-rejected! :too-large) too-large)

                    (and (not (get-in match [:data :public?])) (nil? principal))
                    (do (metrics/http-rejected! :unauthorized)
                        {:status 401 :body "Not authorized"})

                    :else
                    (ring-handler (assoc (buffered) :datahike/principal principal)))))]
          (when started
            (metrics/http-request! metric-op (:status response) started))
          response)))))

(defn query-function-binding
  "How client input is evaluated: `(fn [thunk])` running `thunk` under the
   query function resolver `config` asks for. Queries, aggregates, attribute
   and entity predicates in what a client sends resolve symbols only to the
   curated safe functions, the read-only Datahike functions and what the
   process registered with `datahike.query.resolve/register-fn!`; the
   binding is carried onto the writer thread by the local writer, so
   transactions are covered too. `:query-functions :permissive` evaluates
   client input with the process's own resolver, which on the JVM reaches
   every function on the classpath, `load-string` included; it is for a
   server that trusts all of its clients and is logged as a warning."
  [{:keys [query-functions] :or {query-functions :safe}}]
  (case query-functions
    :safe (fn [thunk] (binding [qr/*symbol-resolver* qr/safe-symbol-resolver] (thunk)))
    :permissive (do (log/warn :datahike/query-functions
                              {:mode :permissive
                               :warning "clients may call any function on the classpath, load-string included"})
                    (fn [thunk] (thunk)))
    (throw (ex-info ":query-functions must be :safe or :permissive"
                    {:type :datahike.http/invalid-config :query-functions query-functions}))))

(defn- wrap-query-functions
  "Run every request under `config`'s query function resolver."
  [handler config]
  (let [run (query-function-binding config)]
    (fn [request] (run #(handler request)))))

(defn handler
  "A Ring handler serving Datahike's HTTP API, for mounting in a host app.

     (routes/handler {:token \"…\"} {:prefix \"/datahike\" :connections conns})

   `config` is the server config: `:token`, `:validator`, `:dev-mode`,
   `:auth :upstream`, `:authorize`, `:max-body-bytes`. Options:

   - `:prefix` — the path every route lives under; normalized.
   - `:connections` — the registry, an atom; a fresh one by default. Pass
     your own to share databases with the host: the host's
     `(binding [*connections* conns] (d/connect cfg))` and a client's
     `/connect` then resolve the identical connection. Deletion through the
     API invalidates every connection to that database, the host's included.
   - `:extra-routes` — your reitit routes on the same router, under the
     prefix, behind the gate unless marked `:public? true`.
   - `:default-handler` — what answers a request the router does not match;
     reitit's default 404 unless given (the server puts swagger-ui here).

   `:create-database` restricts what a client may create (see
   `datahike.http.stores`): `{:backends #{…}}` allows only those backends,
   `{:store {:backend :file :path root}}` makes the server choose the store
   below its own root. Without it any store configuration is accepted.

   Every request runs under the query function resolver `config` asks for
   (`:query-functions`, `:safe` by default; see `query-function-binding`),
   so what a client sends never resolves symbols the host did not allow,
   while the host's own queries are untouched.

   The returned fn carries the atom as metadata; `(release-all! handler)`
   releases what the routes opened when the host shuts down."
  ([config] (handler config {}))
  ([config {:keys [connections default-handler] :as opts}]
   (if (stores/validate-policy (:create-database config))
     (log/info :datahike/create-database-policy (select-keys (:create-database config) [:backends :store]))
     (log/info :datahike/create-database-policy
               {:policy :unrestricted
                :note "clients with :create may name any store configuration; set :create-database to restrict"}))
   (let [connections (or connections (atom {}))
         rtr         (router config (assoc opts :state (new-state)))
         h           (-> (wrap-api (ring/ring-handler rtr (or default-handler (ring/create-default-handler)))
                                   rtr config connections)
                         (wrap-query-functions config))]
     (with-meta h {::connections connections}))))
