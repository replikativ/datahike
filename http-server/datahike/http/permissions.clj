(ns datahike.http.permissions
  "Who may do what, kept in a Datahike database of its own.

   Authentication says who a caller is (`datahike.http.middleware/validators`);
   this namespace says what they may do, with eacl — relationship-based
   authorization in the SpiceDB model, situated: the permission graph lives in
   the server's `:system-db`, and checks are local reads.

   The graph has three kinds of object. `user`s are principals, by their
   `:sub`. The one `server` has `admin`s, who may do everything — create
   databases, administer any of them, grant. A `database` (by store id; its
   branches share it) has `owner`s, `writer`s and `reader`s:

     read     = reader + writer + owner
     transact = writer + owner
     delete, admin, grant = owner

   and `grant` is the permission to edit a database's own relationships —
   permission administration is one more permission in the graph, not a
   special case. The shared-token principal (`:token-subject`, \"root\") is
   seeded as a server admin, so the token stays the break-glass identity and
   everyone else is granted through `/permissions/relationships!`.

   `configure` opens the database and returns the config with `:authorize`
   set; `routes` are the permission management routes, authorized by the same
   graph. Servers that share one system database share its admins: the `server`
   object is one per system database, not per server."
  (:require
   [clojure.string :as str]
   [datahike.api :as d]
   [datahike.http.routes :as routes]
   [datahike.http.system :as system]
   [eacl.core :as eacl]
   [eacl.datahike.core :as ed]))

(def schema
  "The permission graph, in SpiceDB's schema language."
  "definition user {}

definition server {
  relation admin: user
  permission administer = admin
  permission create = admin
}

definition database {
  relation owner: user
  relation writer: user
  relation reader: user
  permission admin = owner
  permission grant = owner
  permission delete = owner
  permission transact = writer + owner
  permission read = reader + writer + owner
}")

;; eacl ids are global, not per type, so the type is part of the id.

(defn- object [type id]
  (eacl/spice-object type (str (name type) ":" id)))

(defn- <-object [{:keys [type id]}]
  {:type type :id (str/replace-first id (str (name type) ":") "")})

(def ^:private server (object :server "datahike"))
(defn- user [principal] (object :user (:sub principal)))
(defn- database [store-id] (object :database (str store-id)))

(defn- ensure-objects!
  "eacl relates objects that exist; `:eacl/id` is an identity, so this is idempotent."
  [conn objects]
  (d/transact conn (vec (for [{:keys [id]} objects] {:eacl/id id}))))

(defn- admin? [acl principal]
  (eacl/can? acl (user principal) :administer server))

(defn- policy
  "The `:authorize` fn: server admins may do everything; anyone else what the
   database's relationships grant. Nobody unauthenticated, and nobody creates
   a database but an admin — who then grants its owner."
  [acl]
  (fn [{:keys [op principal db]}]
    (boolean
     (and principal
          (or (admin? acl principal)
              (and db (not= op :create)
                   (eacl/can? acl (user principal) op (database (:store-id db)))))))))

(defn- compose
  "A host's own `:authorize` on top of the built-in one. The host's function
   sees the call plus `:default`, a thunk of the built-in decision, so it can
   rule on what the graph does not model (`:invoke` of its own remote
   functions, a write's content) and fall back for the rest."
  [built-in host-policy]
  (if host-policy
    (fn [ctx] (boolean (host-policy (assoc ctx :default #(built-in ctx)))))
    built-in))

(defn configure
  "Use the configured system database for EACL, seed the token principal as
   server admin, and return `config` with `:authorize` set: the built-in
   policy, or the host's own `:authorize` composed over it (see `compose`)."
  [{:keys [token-subject] :as input-config}]
  (let [config (if (get input-config system/conn-key)
                 input-config
                 (system/configure input-config))
        conn (or (get config system/conn-key)
                 (throw (ex-info "Permissions require :system-db"
                                 {:type :datahike.http/missing-system-database})))
        acl  (ed/make-client conn {})
        root (object :user (or token-subject "root"))]
    (eacl/write-schema! acl schema)
    (ensure-objects! conn [root server])
    (eacl/write-relationship! acl :touch root :admin server)
    (assoc config
           :authorize (compose (policy acl) (:authorize input-config))
           ::acl acl ::conn conn)))

(defn close! [config]
  (system/close! config))

;; ---------------------------------------------------------------------------
;; Routes
;; ---------------------------------------------------------------------------

(defn- kw [x] (if (keyword? x) x (keyword (str x))))

(defn- ->object [{:keys [type id]}] (object (kw type) (str id)))

(defn- may-grant?
  "May `principal` edit relationships on `resource`? Server admins anywhere;
   a database's owners on it."
  [acl principal resource]
  (or (admin? acl principal)
      (and (= :database (:type resource))
           (eacl/can? acl (user principal) :grant resource))))

(defn- guarded
  "Errors as the decodable 500 the API routes produce."
  [f]
  (fn [request]
    (try (f request)
         (catch Exception e (routes/error-response e)))))

(defn- relationship->map [r]
  {:subject  (<-object (:subject r))
   :relation (:relation r)
   :resource (<-object (:resource r))})

(defn routes
  "The permission routes for a `configure`d config; none without a system database.
   Bodies are maps; objects are `{:type :user|:database|:server :id \"…\"}`.

   - POST /permissions/check `{:subject :permission :resource}` → `{:allowed}`.
     Anyone may ask about themselves (`:subject` omitted); about others only
     who may grant on the resource.
   - POST /permissions/relationships `{:resource}` → the relationships on it.
   - POST /permissions/relationships! `[{:operation :touch|:create|:delete
     :relationship {:subject :relation :resource}} …]`, each allowed by
     `may-grant?` on its resource."
  [{::keys [acl conn]}]
  (when acl
    (let [tags {:tags ["Permissions"]}]
      [["/permissions/check"
        {:swagger tags
         :post {:summary    "Is a subject allowed a permission on a resource?"
                :metric-op  :read
                :parameters {:body :any}
                :handler
                (guarded
                 (fn [{{body :body} :parameters principal :datahike/principal}]
                  (let [subject  (if (:subject body) (->object (:subject body)) (user principal))
                        resource (->object (:resource body))]
                    (if (or (= subject (user principal)) (may-grant? acl principal resource))
                      {:status 200 :body {:allowed (eacl/can? acl subject (kw (:permission body)) resource)}}
                      (routes/forbidden :grant [])))))}}]
       ["/permissions/relationships"
        {:swagger tags
         :post {:summary    "The relationships on a resource."
                :metric-op  :read
                :parameters {:body :any}
                :handler
                (guarded
                 (fn [{{body :body} :parameters principal :datahike/principal}]
                  (let [resource (->object (:resource body))]
                    (if (may-grant? acl principal resource)
                      {:status 200
                       :body   (mapv relationship->map
                                     (:data (eacl/read-relationships acl {:resource/type (:type resource)
                                                                          :resource/id   (:id resource)})))}
                      (routes/forbidden :grant [])))))}}]
       ["/permissions/relationships!"
        {:swagger tags
         :post {:summary    "Create, touch or delete relationships."
                :metric-op  :admin
                :parameters {:body :any}
                :handler
                (guarded
                 (fn [{{body :body} :parameters principal :datahike/principal}]
                  (let [updates (for [{:keys [operation relationship]} body]
                                  {:operation (kw operation)
                                   :subject   (->object (:subject relationship))
                                   :relation  (kw (:relation relationship))
                                   :resource  (->object (:resource relationship))})]
                    (if (every? #(may-grant? acl principal (:resource %)) updates)
                      (do (ensure-objects! conn (mapcat (juxt :subject :resource) updates))
                          ;; One transaction: the batch lands whole or not at all.
                          (eacl/write-relationships!
                           acl
                           (for [{:keys [operation subject relation resource]} updates]
                             (eacl/->RelationshipUpdate operation (eacl/->Relationship subject relation resource))))
                          {:status 200 :body {:written (count updates)}})
                      (routes/forbidden :grant [])))))}}]])))
