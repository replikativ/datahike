# Embedding the HTTP API

`datahike.http.routes` is the HTTP API of the Datahike server as a Ring
handler you mount inside your own application, instead of running
`datahike.http.server` as a separate process. Same routes, same clients,
same authentication — under a prefix of your choosing, sharing the databases
your application already has open.

This is what you want when your service *is* the writer: it holds the
database, other processes read the storage directly ([distributed
mode](distributed.md)) and send their transactions to your service. The
standalone server stays the right choice when nothing else lives in that
process — see [Running the server](#running-the-standalone-server).

The namespace lives in the `http-server` source path; add the `:http-server`
alias (or the path) to your deps, plus a Ring adapter (Jetty below).

## Quick start

```clojure
(ns my.app
  (:require [datahike.api :as d]
            [datahike.connections :refer [*connections*]]
            [datahike.http.routes :as routes]
            [ring.adapter.jetty :refer [run-jetty]]))

(def connections (atom {}))

(def datahike
  (routes/handler {:token (System/getenv "DATAHIKE_TOKEN")}
                  {:prefix "/datahike" :connections connections}))

(defn app [request]
  (if (clojure.string/starts-with? (:uri request) "/datahike")
    (datahike request)
    my-own-handler))

(run-jetty app {:port 8080 :join? false})
```

Any Datahike client now talks to `http://host:8080/datahike`:

```clojure
;; a reader that sends its writes to your service
{:store  {:backend :file :path "/shared/store" :id #uuid "…"}
 :writer {:backend :datahike-server :url "http://host:8080/datahike" :token "…"}}

;; or the full remote API (no local storage at all)
(require '[datahike.http.client :as client])
(client/connect {:store {:backend :memory :id #uuid "…"}
                 :remote-peer {:backend :datahike-server
                               :url "http://host:8080/datahike" :token "…" :format :cbor}})
```

## What `handler` gives you

`(routes/handler config opts)` returns a Ring handler. `config` is the server
config map — authentication (`:token`, `:validator`, `:dev-mode`,
`:auth :upstream`), `:authorize`, `:max-body-bytes`; `opts`:

| option | default | meaning |
|---|---|---|
| `:prefix` | none (mount at `/`) | Path every route is nested under. `"/datahike"`, `"datahike/"` and `"/datahike/"` all mean `/datahike`. |
| `:connections` | a fresh atom | The connection registry the routes use. Pass your own to share databases with the host (below). |
| `:extra-routes` | none | Your own reitit routes on the same router, under the same prefix and behind the gate; the server adds `/swagger.json` and the permission routes this way. Mark a route `:public? true` to exempt it from authentication (not from the body cap). |
| `:default-handler` | reitit's 404 | What answers a request the router does not match (the standalone server puts Swagger UI at `/swagger` here). |

The handler adds no middleware beyond what the API itself needs — CORS,
static files, TLS and the rest of your application are yours. It carries
its connections atom as metadata: `(routes/release-all! handler)` releases
every connection in that atom — the routes' and, if you shared yours, the
host's own — so call it when the process shuts down.

### The request contract

Every request the router matches for its method goes through the gate, in
this order — a request it does not match, or matches for another method (an
OPTIONS preflight, a wrong method), is handed on untouched, to your CORS
middleware, reitit's 405 or the `:default-handler`:

1. **The body is read in full and capped** at `:max-body-bytes` (default
   64 MiB), by `Content-Length` and by actually reading it, public route or
   not, before any decoder sees a byte: `413` past the cap. No decoder ever
   gets an unbounded stream, and a body a decoder would read only the first
   form of is still measured whole.
2. **Authentication, before decoding.** Unless the route is `:public? true`
   (`/swagger.json`), the caller must be accepted by one of the validators
   `config` describes (next section), or gets `401` with nothing parsed. The
   principal is put on the request as `:datahike/principal`.
3. **The registry.** `datahike.connections/*connections*` is bound to your
   atom for the *whole* request, decoding included. Every route — `connect`,
   `q`, `release`, the writer routes, a database handle inside a body —
   resolves connections in that atom, never in the process-wide default.
4. **Authorization, after decoding**, per route, with the databases the call
   reaches (below).

A handler with no `:token`, no `:validator`, no `:dev-mode` and no
`:auth :upstream` admits nobody; that is the secure default, not a bug.

## Authentication: who is calling

The config describes a chain of validators, each
`(fn [ring-request] → principal | nil)`, tried in order; the first principal
wins. A principal is a map with `:sub`, the subject's id — the same shape
`kabel.auth.jwt/build-bearer-validator` returns, so one validator can serve
the HTTP API and a kabel WebSocket peer.

| config | who gets in |
|---|---|
| `:token "…"` | Whoever sends `authorization: token <token>`, as the principal `:token-subject` (`"root"` by default). The shared secret; keep it in an environment variable or a secrets manager. |
| `:validator f` | Whoever `f` accepts. JWT, JWKS-backed OIDC (WorkOS, Clerk, Auth0), mTLS-derived identity — `kabel.auth.jwt` has the JWT ones ready. A principal claiming the token's subject is refused: subjects share one namespace and that one is reserved. |
| `:auth :upstream` | Everyone, as the `:datahike/principal` your own middleware put on the request (or `{:sub "upstream"}`). For a host that authenticates before the request reaches Datahike. Only ever behind such middleware. |
| `:dev-mode true` | Everyone, as `{:sub "dev"}`. Local development only; never deploy with it. |

```clojure
(require '[kabel.auth.jwt :as jwt])   ; kabel's :auth alias

(routes/handler {:token     (System/getenv "DATAHIKE_TOKEN")        ; break-glass
                 :validator (jwt/build-bearer-validator             ; everyone else
                             {:issuers {"https://issuer" {:alg :RS256 :jwks-url "…"}}
                              :key-resolver (jwks/make-key-resolver)})}
                {:prefix "/datahike" :connections connections})
```

## Authorization: what they may do

`:authorize` is one function, `(fn [{:keys [op principal db payload]}] → boolean)`,
asked per call for every database the call reaches — a config, a connection,
a database value or any history/as-of/since/filtered view of one, positional
or in the map form — and once with `:db nil` for a call that reaches none.
Every database must be allowed, or the call is `403` with
`{:type :datahike.http/forbidden :op … :databases […]}` in its ex-data,
which the clients raise as an exception.

| `op` | routes |
|---|---|
| `:read` | every GET (`q`, `pull`, `datoms`, `history`, `as-of`, `metrics`, …) and the POSTs that only open, inspect or close: `connect`, `release`, `db`, `database-exists?`, `branches`, `branch-as-db`, `commit-as-db` |
| `:transact` | `transact`, `load-entities`, `merge-db`, `branch!`, and the writer's `transact!` |
| `:create` | `create-database`, the writer's too |
| `:delete` | `delete-database`, `delete-branch!`, the writer's `delete-database` |
| `:admin` | `gc-storage` |

`db` is `{:store-id uuid :branch keyword}`; `payload` is the call's argument
vector. The arguments are searched in full, transaction data included: a
transaction function can be handed a connection or database value for
another database, and that database is then part of the call. Without `:authorize`, every authenticated caller may do everything —
today's behaviour.

### Catalog and permissions in the system database

The server ships a policy so you need not write one: relationship-based
authorization in the [SpiceDB](https://authzed.com/spicedb) model via
[eacl](https://github.com/theronic/eacl), *situated* — the permission graph
and database catalog live in a Datahike database of their own, `:system-db`,
and every permission check is a local read. Give the server one and both are
on:

```clojure
;; config.edn
{:port     4444
 :token    "…"                                   ; the break-glass identity
 :validator …                                    ; everyone else, by JWT
 :system-db {:store {:backend :file :path "/var/lib/datahike/system"}}}
```

A durable store without an `:id` gets one derived from its path, the same
on every start; a memory store gets a fresh one per server, so two servers
in one process never share a system database by accident. Servers that do
share one share its catalog and admins. New system databases retain history.

Successful database creates and deletes update the catalog with the store id,
optional `:name`, a credential-redacted config, timestamps, and the acting
principal. `GET /databases` returns active entries filtered through the same
`:read` permission as database calls; callers cannot discover databases they
cannot read.

### Optional PostgreSQL listener (beta)

The batteries-included server bundles
[pg-datahike](https://github.com/replikativ/pg-datahike). Add a
`:pg-listener` map to serve every active catalog entry under its `:name` (or
store UUID when unnamed):

```clojure
{:system-db {:store {:backend :file :path "/var/lib/datahike/system"}}
 :pg-listener
 {:host "127.0.0.1"
  :port 5432
  :database-overrides
  {"accounts" {:store {:password "secret-from-deployment"}}}}}
```

Set `:pg-listener {:enabled? false}` to keep a deployment-supplied listener
configuration present without opening the port.

Catalog configs never retain passwords or cloud credentials. Generate or
mount the deployment config securely. If a redacted value is still unresolved
after applying the matching name or store-id override, the listener refuses to
start and reports only the missing config paths.

HTTP creates are added to the PostgreSQL registry immediately. Before an HTTP
delete touches storage, the listener unregisters the database and releases its
connection; a failed delete restores it. pg-datahike's independent
`CREATE/DROP DATABASE` hooks are rejected so there is no second catalog.
Catalog names exposed through PostgreSQL must be unique; the server rejects a
conflicting HTTP create before it changes physical storage.

This listener is beta and currently restricted to loopback because released
pg-datahike versions do not authenticate the wire connection. Once password
authentication is available, the server will accept an authenticated public
bind. Use the HTTP API for provisioning and permission administration in the
meantime.

The PostgreSQL listener is currently a trusted local data surface: PostgreSQL
users are not mapped to Datahike EACL principals, so a client that can reach
the listener can access every database it exposes. The loopback restriction is
a security boundary, not merely a beta convenience. Do not publish or proxy
this port to untrusted clients.

The graph (`datahike.http.permissions/schema`):

```
definition user {}
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
}
```

Users are principals by their `:sub`; the one server has admins who may do
everything; a database — by store id, its branches share it — has owners,
writers and readers, and `grant` is the permission to edit that database's
relationships, so permission administration is one more permission in the
graph rather than a special case. The token principal (`:token-subject`,
`"root"`) is seeded as a server admin on every start, so the shared secret
stays the break-glass identity and everyone else is granted:

```clojure
;; as root: alice may write to the database, bob may read it
(client/request-cbor :post "permissions/relationships!" root-peer
  [{:operation :touch :relationship {:subject  {:type :user :id "alice"}
                                     :relation :writer
                                     :resource {:type :database :id "<store uuid>"}}}
   {:operation :touch :relationship {:subject  {:type :user :id "bob"}
                                     :relation :reader
                                     :resource {:type :database :id "<store uuid>"}}}])
```

Routes, all behind the gate, bodies as maps, objects as
`{:type :user|:database|:server :id "…"}`:

- `POST /permissions/check` `{:subject :permission :resource}` → `{:allowed}`.
  Anyone may ask about themselves (omit `:subject`); about others only who may
  grant on the resource.
- `POST /permissions/relationships` `{:resource}` → the relationships on it,
  for who may grant on it.
- `POST /permissions/relationships!` `[{:operation :touch|:create|:delete :relationship {…}} …]`,
  each allowed by grant on its resource (server admins anywhere).

Only server admins create databases — the creator then grants an owner —
and only owners delete. A database's permissions apply to what the server
mediates: the full remote API, and the writer routes for a process that
reads storage directly and sends its writes here. Such a process has read
access by holding the storage credentials; the server governs its writes.

Embedding hosts get the same by calling `permissions/configure` on their
config and adding `(permissions/routes config)` to `:extra-routes`; the
`datahike.http.permissions` namespace needs eacl on the classpath, which the
`:http-server` alias brings and `datahike.http.routes` does not need.

### Sharing connections with the host

Connections are keyed by store identity and branch, so the host's connection
and one the API opens for the same config are *the same object* when they are
looked up in the same atom. Bind the atom when the host connects:

```clojure
(def conn (binding [*connections* connections]
            (d/connect cfg)))

;; a client's `connect` with the same config now returns this connection,
;; and what it transacts is visible on `@conn` immediately.
```

If the host does not bind, the host's connection lives in the default
registry and the API opens its own — two connections to one store, which is
allowed (they share the storage and coordinate through it) but wastes memory
and, for the writer routes, means your service transacts through a connection
it does not hold.

Two rules that follow from sharing:

- **Leases.** The writer routes hold one base lease per database, taken on
  first use, so the connection and its writer survive between requests; each
  request pins the connection for its duration and lets go. `connect` and
  `release` through the API count leases exactly as the API does for a local
  caller — one lease per call; a client's `release-all?` is ignored, since it
  would close a connection the host and other callers share — and decoding a
  database handle takes none. Call
  `(routes/release-all! handler)` (or with the atom) when your service shuts
  down.
- **Deletion is process-wide.** `delete-database` through the API releases
  and invalidates *every* connection to that database in the atom, the host's
  included — as it would if the host called it itself. If your service must
  keep a database, do not grant `delete` on it.

### What the writer routes accept

A `:datahike-server` writer sends `create-database`, `delete-database` and
`transact!` to your service. Other writer-side operations
(`load-entities`, `merge-db!`, `gc-storage!`, `publish-built-db!`) are not
available over this writer; a client calling them gets an error naming the
operation and the writer stays usable. Run those where the writer runs — in
your service — or give that client a `:self` writer.

## Security

- Tokens travel in the `authorization` header only: the clients strip their
  credentials from the configs they send, the server strips `:writer` and
  `:remote-peer` from any config it receives, and neither logs request
  bodies.
- Error bodies (500) carry the exception's message and `ex-data` with
  credential-valued keys (`:token`, `:secret`, `:access-key`, `:secret-key`,
  `:password`) redacted.
- TLS is your reverse proxy's job (nginx, Caddy, a cloud load balancer). Do
  not send tokens over plain HTTP outside a private network.
- `:dev-mode true` disables authentication entirely; `:auth :upstream`
  trusts whatever reached the handler. Never deploy either exposed.

Checklist: a token or validator set · `:dev-mode` false · secrets in
env/secret store · TLS at the edge · `release-all!` on shutdown · `delete`
granted only to owners who may.

The standalone server enforces the first two items at bind time: without a
nonblank token or a validator, every resolved bind address must be loopback.
Missing and wildcard hosts count as public. `:auth :upstream` is valid only for
an embedded handler behind authentication middleware and does not authorize a
standalone public bind.

## Running the standalone server

The server is the same routes with a lightweight operator page at `/` (also
`/admin`), Swagger UI at `/swagger`, `/swagger.json`, CORS and Jetty around
them:

```bash
clojure -M:http-server --config config.edn
```

```clojure
;; config.edn
{:port     4444
 :join?    false
 :token    "securerandompassword"
 :dev-mode false
 :level    :info
 :log-format :text}
```

### Operator landing page

Open the server root (or `/admin`) for a read-only overview of readiness, build
identity, useful operational endpoints, and the active databases visible to
your principal. Swagger UI remains available at `/swagger` for development and
API exploration.

The Datahike logo links to [datahike.io](https://datahike.io), and the page's
documentation link opens the repository's canonical
[`doc/README.md`](https://github.com/replikativ/datahike/blob/main/doc/README.md)
index.

The HTML, CSS, and JavaScript shell is public just like Swagger UI, but it
contains no server data. Entering a token makes the page call the existing
authenticated `/version` and `/databases` endpoints with the normal
`Authorization: token …` header. The token is kept only in the browser tab's
session storage; it is never placed in a URL, cookie, HTML response, or
persistent local storage.

The database list is permission-filtered by `/databases`, so the page cannot
discover catalog entries its principal may not read. In `:dev-mode` or behind
`:auth :upstream`, leave the token field blank. The page is intentionally
read-only: Swagger remains the API explorer and permission or database changes
go through the normal API.

### Prometheus metrics

The standalone server exposes Prometheus text at
`GET /prometheus`. The dedicated path is intentional: `GET /metrics` already
belongs to Datahike's public API and returns database/index statistics from
`d/metrics`.

The endpoint requires the server's normal authentication by default:

```bash
curl -H 'Authorization: token securerandompassword' \
  http://localhost:4444/prometheus
```

It includes durable transaction and conflict metrics, HTTP latency and
rejections, live connection leases, Konserve operations, and JVM/process
samples. A scraper on an otherwise protected private network can be admitted
without a token with `:metrics {:public? true}`. Set `:metrics false` to omit
the endpoint and keep the standalone server from installing the process-wide
Konserve metrics sink.

Multiple servers in one JVM share one reference-counted Konserve sink; stopping
one does not interrupt the others, and the last stop removes it. Calling
`datahike.http.server/app` directly does not claim ownership of that global
sink—the embedding host owns its process lifecycle—but its endpoint still
exports Datahike, connection, and JVM metrics. An embedding host that wants
Konserve metrics can register `replikativ.metrics.konserve/sink` with
`konserve.metrics/add-sink!` under its own id and remove it during shutdown.

### Graceful shutdown

The standalone launcher installs a JVM shutdown hook. On SIGTERM it stops
Jetty from accepting new requests, gives active handlers up to 30 seconds to
finish, and only then releases Datahike connections, the permissions database,
and the process metrics lease. Configure the grace period with
`:shutdown-timeout-ms`, `DATAHIKE_SHUTDOWN_TIMEOUT_MS`, or
`--shutdown-timeout-ms`; zero requests an immediate stop. Set an orchestrator's
termination grace period above the server timeout.

Calling `stop-server` provides the same ordered drain for programmatic server
instances and is idempotent with respect to Datahike-owned resources. An
embedding host using `datahike.http.routes/handler` still owns its HTTP and JVM
shutdown lifecycle and should call `release-all!` after its adapter drains.

### Health checks

The server exposes two unauthenticated, plain-text health endpoints for
orchestrators and load balancers:

- `GET /health/live` returns 200 while the HTTP process can answer requests.
- `GET /health/ready` returns 200 only when every database store currently held
  by the server, plus its configured `:system-db`, accepts a non-mutating Konserve
  read. It returns 503 otherwise.

Both responses intentionally contain only `live`, `ready`, or `not ready`. The
server log identifies a failed store and the exception class/type without
copying exception messages or data that might contain credentials. Data
databases are supplied dynamically by API clients today, so readiness covers
the live connections the server knows about. A later listener/reconnection
stage will open catalog entries at startup so readiness can cover those before
the first API call too.

`GET /version` returns build and runtime metadata: the Datahike version and Git
SHA, Konserve version, registered Konserve backends, and the server
configuration with credential-valued fields replaced by `REDACTED`. Unlike the
health endpoints, it requires the server's normal authentication because even
redacted configuration exposes deployment topology. Send an `Accept` header
such as `application/json` or `application/edn` to choose the response format.

The standalone server artifact registers the built-in file, memory, and tiered
stores plus the official S3, JDBC, DynamoDB, and Redis backends. It includes the
PostgreSQL JDBC driver. GCS remains an opt-in dependency because of its large
SDK graph; RocksDB and LMDB remain opt-in because they impose native and
platform-specific runtime requirements. Embedding applications can load any
additional Konserve backend, and `/version` reports it automatically.

`datahike.http.server/start-server` and `stop-server` do the same from a
REPL; `stop-server` releases every database the server opened, the auth
database included. `app` takes the config and a connections atom, for hosts
that want the server's exact handler (Swagger, CORS, permissions) inside
their own Jetty.

## Migrating from the standalone server

Nothing changes for clients: `:url` now points at your prefix. On the server
side, replace `start-server` with `handler` mounted in your app, pass your
atom, and bind it where the host connects. The server's `app` now takes
`(app config connections)`; `start-server config` is unchanged.

## Troubleshooting

- **401 on everything** — no `:token`, `:validator`, `:dev-mode` or
  `:auth :upstream` in the config. The client sends
  `authorization: token <token>`.
- **403** — the call reaches a database the principal has no permission on;
  the ex-data names the op and the databases. Grant, or check the principal
  your validator returns.
- **404 under the prefix** — the prefix is normalized, but the *host* must
  route the prefixed paths to the handler; check the host's dispatch.
- **413** — raise `:max-body-bytes`, or transact in smaller batches.
- **A permission does not take effect** — objects are keyed by exact `:sub`
  and store id; check `POST /permissions/relationships` as root.
- **The host does not see what a client wrote** — the host connected outside
  `(binding [*connections* connections] …)`; two connections, two views.
- **A client gets "not available over the :datahike-server writer"** — it
  called a writer-side operation the HTTP writer does not carry; run it in the
  service.

Credit: the embedding mode, its documentation shape and the security
checklist grew out of Alex Oloo's proposal in
[#755](https://github.com/replikativ/datahike/pull/755).
