# Thin client: the Datahike API against a server, no engine in the page

**Status: Beta.** Tested on Node.js and the JVM against the standalone server;
the browser build is the same code over `fetch`.

The npm entry `datahike/remote` is the Datahike API with nothing behind it:
every call is a request to a Datahike server, and connections, databases and
entities are opaque handles that go back to the server as they came. The
bundle carries no database engine, which is what makes it a fit for a web
page that should not ship one: about 76 KiB gzipped, against 440 KiB for the
full runtime.

This is the same mode the `dthk` binary and the Clojure client use: a
configuration whose `:remote-peer` names the server. Three modes, one API,
chosen by configuration:

| mode | npm entry | data lives | queries run | writes go |
|---|---|---|---|---|
| embedded | `datahike` | in the page or the Node process | locally | locally |
| replica | `datahike/kabel` | in a local replica of a server database | locally | to the server, replicated back |
| thin | `datahike/remote` | on the server | on the server | to the server |

See [browser-replicas.md](browser-replicas.md) for the replica and
[http-routes.md](http-routes.md) for the server.

## Use

```typescript
import * as d from 'datahike/remote';

const config = {
  store: { backend: ':memory', id: d.randomUuid() },
  'schema-flexibility': ':read',
  'remote-peer': { backend: ':datahike-server', url: 'https://data.example.com', token: process.env.TOKEN }
};

await d.createDatabase(config);           // or an admin created it for you
const conn = await d.connect(config);
await d.transact(conn, [{ ':person/name': 'Ada' }]);
const db = await d.db(conn);
await d.q('[:find ?n :where [?e :person/name ?n]]', db);   // => [['Ada']]
await d.release(conn);
```

`connect`, `db` and the rest return handles: pass them back, do not look
inside. A transaction report is a JavaScript object whose `db-after` is such
a handle. Values convert at the boundary the way the main entry converts
them: keywords as `':name'` strings, uuids made with `d.uuid` or
`d.randomUuid`, results as plain arrays and objects. Every function returns a
Promise. The functions are exactly the specification's remote-capable ones;
`remote.d.ts` lists them.

Authentication is the HTTP API's: the shared `token`, or a JWT once the
server has a `:validator` ([http-routes.md](http-routes.md#authentication-who-is-calling)).
What a caller may do is the server's `:authorize` policy and the permission
graph, the same as for every other client. Under the server, query
functions resolve only to the curated safe set and what the server
registered ([http-routes.md](http-routes.md#query-functions)).

## On the wire

CBOR, through boring, in both directions. A read (`q`, `pull`, `datoms`, …)
is a GET whose arguments travel base64url-encoded in the URL while they fit
in 2 KiB, so the response is cacheable by URL and the server sends the cache
headers its `:cache` configuration asks for; larger arguments go by POST,
which is never cached. A write is a POST. A database handle carries the
snapshot's commit id, so a read against one is the same result for as long
as the handle is used.

## Change notification

A thin client holds no replica, so it learns about other writers' changes
only by asking. The server broadcasts every transaction report per store
over Kabel; a page that wants to be told adds the `datahike/kabel` entry's
peer and subscribes to the store's report topic. That is the replica's
transport and adds its bundle; there is no lighter notification lane.

## What is not there

- No offline operation: every call needs the server.
- No streaming results: a query result arrives whole. Use `:limit` and
  `:offset`, or narrow the query.
- No client-side result cache yet. A read against a database handle is
  immutable for that snapshot, so one is possible; the HTTP cache covers the
  GET path meanwhile.

## Reference

`npm-package/remote.d.ts` is generated from the API specification. The
ClojureScript namespace behind the entry is `datahike.http.client`, the
same one the JVM uses; `datahike.js.remote` is the JavaScript boundary.
