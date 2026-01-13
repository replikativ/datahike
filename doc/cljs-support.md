# ClojureScript Support

**This feature is in beta. Please try it out and provide feedback.**

Datahike runs in ClojureScript on both Node.js and browser environments. The
core query engine, pull API, and entity API work identically to Clojure. The
main differences are in storage backends and async operation.

## Async Storage Model

ClojureScript environments often require async I/O (IndexedDB, network). To
support this, Datahike's persistent-sorted-set indices use *partial
continuation-passing style* (partial-cps): storage operations return channels
instead of values, while in-memory operations remain synchronous.

This is transparent when using `datahike.api` - async backends return channels
from `create-database`, `connect`, `transact!`, etc.

## Node.js

Node.js supports the file backend via
[konserve-node-filestore](https://github.com/replikativ/konserve):

```clojure
(ns my-app.core
  (:require [datahike.api :as d]
            [datahike.nodejs]  ;; Registers :file backend
            [cljs.core.async :refer [<!] :refer-macros [go]]))

(go
  (let [config {:store {:backend :file
                        :path "./my-database"
                        :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
                :schema-flexibility :write}]
    (<! (d/create-database config))
    (let [conn (<! (d/connect config))]
      (<! (d/transact! conn [{:name "Alice"}]))
      (println (d/q '[:find ?n :where [?e :name ?n]] (d/db conn))))))
```

## Browser

Browsers cannot use file storage directly. Datahike provides two browser-compatible backends:

### Memory backend

Fast but not persistent across page reloads:

```clojure
{:store {:backend :memory :id #uuid "550e8400-e29b-41d4-a716-446655440000"}}
```

### IndexedDB backend

Persistent browser storage via
[konserve-indexeddb](https://github.com/replikativ/konserve):

```clojure
{:store {:backend :indexeddb :id #uuid "550e8400-e29b-41d4-a716-446655440000"}}
```

IndexedDB is async, so all operations return channels.

### TieredStore

For optimal performance, use TieredStore which combines a fast memory frontend
with a persistent IndexedDB backend:

```clojure
{:store {:backend :tiered
         :id #uuid "550e8400-e29b-41d4-a716-446655440000"
         :frontend-config {:backend :memory
                          :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
         :backend-config {:backend :indexeddb
                         :id #uuid "550e8400-e29b-41d4-a716-446655440000"}}}
```

Writes go to both stores (write-through). Reads come from memory. On
reconnection, TieredStore syncs cached data from IndexedDB into memory before
the sync handshake, so only keys newer than cached timestamps are transferred.

## Distributed Setup with KabelWriter

For browser applications that need a persistent backend, use the KabelWriter to
connect to a JVM server. The server owns the database (using file storage) and
streams updates to browser clients via WebSockets. See [Streaming writer
(Kabel)](distributed.md#streaming-writer-kabel) for setup instructions.

This architecture means:
- Transactions are sent to the server via RPC
- The server writes to its file store
- Updates are replicated to the client's TieredStore via konserve-sync
- Queries run locally against the synchronized in-memory store

## JavaScript API

For JavaScript/TypeScript applications, Datahike provides a Promise-based API.
See [JavaScript API](javascript-api.md) for documentation.

```javascript
const d = require('datahike');
const crypto = require('crypto');

async function example() {
  const config = {
    store: {
      backend: ':memory',
      id: crypto.randomUUID()
    }
  };

  await d.createDatabase(config);
  const conn = await d.connect(config);
  await d.transact(conn, [{ name: 'Alice' }]);
  const db = d.db(conn);
  console.log(await d.q('[:find ?n :where [?e :name ?n]]', db));
}
```

## Limitations

- **History queries**: Work but may be slower due to async index traversal
- **Large datasets**: Browser memory constraints apply; consider server-side
  queries for large result sets
- **Concurrent tabs**: Each tab has its own connection; use KabelWriter for
  cross-tab consistency
