# Thin client: the Datahike API against a server, no engine in the page

**Status: Beta.** Tested on Node.js and the JVM against the standalone server;
the JavaScript browser client is the same TypeScript code over `fetch`.

The npm entry `datahike/remote` is the Datahike API with nothing behind it:
every call is a request to a Datahike server, and connections, databases and
entities are opaque handles that go back to the server as they came. The
bundle carries no database engine, which is what makes it a fit for a web
page that should not ship one: about 4 KiB gzipped, against 69 KiB for the
former ClojureScript thin-client bundle and about 440 KiB for the full runtime.

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

For JavaScript and TypeScript consumers this entry is a hand-written
TypeScript client. `connect`, `db` and the rest return handles: pass them back,
do not look inside. A transaction report is a JavaScript object whose
`db-after` is such a handle. Values convert at the boundary the way the main
entry converts them: keywords as `':name'` strings, uuids made with `d.uuid`
or `d.randomUuid`, results as plain arrays and objects. Every function returns
a Promise. The functions are exactly the specification's remote-capable ones;
`remote/index.d.ts` lists them. ClojureScript consumers instead require
`datahike.http.client` from the Clojure artifact; that API and its `listen`
implementation are unchanged.

Authentication is the HTTP API's: the shared `token`, or a JWT once the
server has a `:validator` ([http-routes.md](http-routes.md#authentication-who-is-calling)).
What a caller may do is the server's `:authorize` policy and the permission
graph, the same as for every other client. Under the server, query
functions resolve only to the curated safe set and what the server
registered ([http-routes.md](http-routes.md#query-functions)).

## On the wire

JSON in both directions, using the server's tagged arrays for keywords,
symbols, sets, UUIDs, dates, datoms, connections, databases, entities and
transaction reports. A read (`q`, `pull`, `datoms`, …) is a GET whose JSON
argument vector travels base64url-encoded in the URL while the encoded bytes
fit in 2 KiB, so the response is cacheable by URL and the server sends the
cache headers its `:cache` configuration asks for; larger arguments go by
POST with a JSON body, which is never cached. A write is always a POST. A
database handle carries the snapshot's commit id, so a read against one is
the same result for as long as the handle is used.

The TypeScript client keeps up to 128 snapshot-read results per client instance
in an in-memory least-recently-used cache. `configureCache({maxEntries})`
changes that bound (zero disables it), and `clearCache()` empties it. The cache
dies with the page and uses neither localStorage nor IndexedDB. Persistence
across sessions is the browser's HTTP cache; offline reads are the replica's
job, not this client's.

## Change notification

A thin client can follow the connected database without carrying a replica.
In JavaScript, `listen` returns a key immediately and calls back with plain
JavaScript reports; pass that key to `unlisten`:

```javascript
const listener = d.listen(conn, report => {
  if (report.error) {
    console.error(`listen failed with HTTP ${report.status}`, report.error);
    return;
  }
  if (report.deleted) return;
  renderFrom(report['db-after']);
});

d.unlisten(conn, listener);
```

The ClojureScript API also accepts an explicit listener key:

```clojure
(def listener (client/listen conn ::ui refresh!))
(client/unlisten conn listener)
```

The first callback is a resync report with `:resync true` and a `:db-after`
handle. Later transaction reports contain `:db-after`, `:db-before nil`,
`:tempids`, and `:commit-id`. Reports of at most 500 datoms also contain
`:tx-data`; larger reports instead contain `:truncated true`. A slow consumer
can receive another resync report after the server coalesces changes. Database
deletion produces the terminal `:deleted true` report.

The client reconnects after a stream error or clean disconnect, starting at
500 ms and doubling the delay up to 30 seconds. It sends the last observed
commit id when reconnecting; the server resyncs when that id is no longer the
head. `unlisten` aborts the open request and cancels reconnects. Deletion does
not reconnect. HTTP 401, 403, and 404 responses are terminal too: the callback
receives `{:error <ex-info> :status <integer>}` in ClojureScript, or an object
with `error` and `status` in JavaScript, and the listener stops.

## What is not there

- No offline operation: every call needs the server.
- No streaming results: a query result arrives whole. Use `:limit` and
  `:offset`, or narrow the query.

## Reference

`ts-client/src/api.generated.ts` is generated from the API specification and
the hand-written transport and codec live beside it. ClojureScript consumers
use `datahike.http.client` from the Clojure artifact, the same namespace the
JVM client uses.
