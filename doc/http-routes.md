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
config map (`:token`, `:dev-mode`, `:max-body-bytes`); `opts`:

| option | default | meaning |
|---|---|---|
| `:prefix` | none (mount at `/`) | Path every route is nested under. `"/datahike"`, `"datahike/"` and `"/datahike/"` all mean `/datahike`. |
| `:connections` | a fresh atom | The connection registry the routes use. Pass your own to share databases with the host (below). |
| `:extra-routes` | none | Your own reitit routes on the same router, under the same prefix; the server adds `/swagger.json` this way. Mark a route `:public? true` to exempt it from the token gate. |

Requests the router does not match fall through with a 404 from
`reitit.ring/create-default-handler`. CORS, static files, TLS and the rest of
your application are yours — the handler adds no middleware beyond what the
API itself needs.

### The request contract

Every request the router matches goes through `routes/wrap-api`, in this order:

1. **The gate, before any decoding.** A public route passes. Every other
   route requires the token *here*: `authorization: token <token>` (also the
   `token` header), or `:dev-mode true` in the config. A request without it
   is answered `401` without its body ever being parsed. Then the body is
   capped at `:max-body-bytes` (default 64 MiB) — by `Content-Length` and by
   the stream itself, so a chunked body cannot skip the check — with `413`.
2. **The registry.** `datahike.connections/*connections*` is bound to your
   atom for the *whole* request, decoding included. Every route — `connect`,
   `q`, `release`, the writer routes, a database handle inside a body —
   resolves connections in that atom, never in the process-wide default.

A handler with no `:token` and no `:dev-mode` admits nobody; that is the
secure default, not a bug.

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

- **Leases.** The writer routes hold *one* connection per database in the
  atom, opened on first use, and never release it per request; ordinary
  `connect`/`release` routes count leases exactly as the API does for a local
  caller. Call `(routes/release-all! connections)` when your service shuts
  down.
- **Deletion is process-wide.** `delete-database` through the API releases
  and invalidates *every* connection to that database in the atom, the host's
  included — as it would if the host called it itself. If your service must
  keep a database, do not hand out the token that can delete it.

### What the writer routes accept

A `:datahike-server` writer sends `create-database`, `delete-database` and
`transact!` to your service. Other writer-side operations
(`load-entities`, `merge-db!`, `gc-storage!`, `publish-built-db!`) are not
available over this writer; a client calling them gets an error naming the
operation. Run those where the writer runs — in your service — or give that
client a `:self` writer.

## Authentication and security

- The token is a shared secret; put it in an environment variable or a
  secrets manager, never in the config file you commit. Use different tokens
  per environment and rotate them.
- Tokens travel in a header, never in the body: the writer client strips its
  own credentials from what it sends, and the server strips `:writer` and
  `:remote-peer` from any config it receives before using it.
- Error bodies (500) carry the exception's message and `ex-data` with
  credential-valued keys (`:token`, `:secret`, `:access-key`, `:secret-key`,
  `:password`) redacted.
- TLS is your reverse proxy's job (nginx, Caddy, a cloud load balancer). Do
  not send tokens over plain HTTP outside a private network.
- `:dev-mode true` disables authentication entirely. Never deploy with it.
- For anything beyond a shared token — OAuth, JWT, mTLS, per-user
  authorization — wrap the handler in your own middleware and run Datahike
  with `:dev-mode true` *behind* it, so that only your middleware decides who
  gets in. The API has no notion of users; authorization is per token.

Checklist: token set · `:dev-mode` false · token in env/secret store · TLS at
the edge · `release-all!` on shutdown · deletion token held only by whoever
may delete.

## Running the standalone server

The server is the same routes with Swagger UI at `/`, `/swagger.json`, CORS
and Jetty around them:

```bash
clojure -M:http-server -m datahike.http.server config.edn
```

```clojure
;; config.edn
{:port     4444
 :join?    false
 :token    "securerandompassword"
 :dev-mode false
 :level    :info}
```

`datahike.http.server/start-server` and `stop-server` do the same from a
REPL; `stop-server` releases every database the server opened. `app` takes
the config and a connections atom, for hosts that want the server's exact
handler (Swagger included) inside their own Jetty.

## Migrating from the standalone server

Nothing changes for clients: `:url` now points at your prefix. On the server
side, replace `start-server` with `handler` mounted in your app, pass your
atom, and bind it where the host connects. The server's `app` now takes
`(app config connections)`; `start-server config` is unchanged.

## Troubleshooting

- **401 on everything** — no `:token` in the config and no `:dev-mode`. Set
  the token; the client must send `authorization: token <token>`.
- **404 under the prefix** — the prefix is normalized, but the *host* must
  route the prefixed paths to the handler; check the host's dispatch.
- **413** — raise `:max-body-bytes`, or transact in smaller batches.
- **The host does not see what a client wrote** — the host connected outside
  `(binding [*connections* connections] …)`; two connections, two views.
- **A client gets "not available over the :datahike-server writer"** — it
  called a writer-side operation the HTTP writer does not carry; run it in the
  service.

Credit: the embedding mode, its documentation shape and the security
checklist grew out of Alex Oloo's proposal in
[#755](https://github.com/replikativ/datahike/pull/755).
