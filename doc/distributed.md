# Distributed Architecture

Datahike's architecture is built on **immutable persistent data structures** that enable efficient distribution and collaboration. The database is fundamentally designed around two complementary approaches:

1. **Distributed Index Space (DIS)**: Share persistent indices across processes—readers access data directly without database connections
2. **Remote Procedure Calls (RPC)**: Centralize computation on a server for shared caching and simplified deployment

![Network topology](assets/network_topology.svg)

# Distributed Index Space (DIS)

**Distributed Index Space is Datahike's key architectural advantage.** It enables massive read scalability and powers collaborative systems by treating database snapshots as immutable values that can be shared like files.

## How it works

Datahike builds on **copy-on-write persistent data structures** where changes create new structure sharing most data with previous versions. When you transact to a database:

1. New index nodes are written to the shared [storage backend](storage-backends.md) (S3, JDBC, file, etc.)
2. A new root pointer is published atomically
3. Readers pick up the new snapshot on next access—no active connections needed

This is similar to [Datomic](https://datomic.com), but **Datahike connections are lightweight and require no communication by default**. If you only need to read from a database (e.g., a dataset provided by a third party), you just need read access to the storage—no server setup required.

## Scaling and collaboration

The DIS model provides fundamental advantages for distributed systems:

- **Massive read scaling**: Add readers without coordination—they access persistent indices directly
- **Zero connection overhead**: No connection pooling, no network round-trips for reads
- **Snapshot isolation**: Each reader sees a consistent point-in-time view
- **Efficient sharding**: Create one database per logical unit (e.g., per customer, per project)—readers can join across databases locally
- **Offline-first capable**: Readers can cache indices locally and sync differentially when online

This architecture enables collaborative systems where multiple processes share access to evolving datasets without centralized coordination. The same design principles that enable DIS (immutability, structural sharing) also support more advanced distribution patterns including CRDT-based merge strategies (see [replikativ](https://github.com/replikativ/replikativ)) and peer-to-peer synchronization (demonstrated with [dat-sync](https://github.com/replikativ/dat-sync)).

These capabilities are valuable even in centralized production environments: differential sync reduces bandwidth, immutable snapshots simplify caching and recovery, and the architecture naturally handles network partitions.

## Single writer model

Datahike uses a **single-writer, multiple-reader** model—the same architectural choice as Datomic, Datalevin, and XTDB. While multiple readers can access indices concurrently via DIS, write operations are serialized through a single writer process to ensure strong consistency and linearizable transactions.

To provide distributed write access, you configure a writer endpoint (HTTP server or Kabel WebSocket). The writer:
- Serializes all transactions for strong consistency guarantees
- Publishes new index snapshots to the shared storage backend
- Allows unlimited readers to access the updated indices via DIS

**All readers continue to access data locally** from the distributed storage (shared filesystem, JDBC, S3, etc.) without connecting to the writer—they only contact it to submit transactions. This model is supported by all Datahike clients: JVM, Node.js, browser, CLI, Babashka pod, and libdatahike.

The client setup is simple, you just add a `:writer` entry in the configuration
for your database, e.g.

```clojure
{:store  {:backend :file
          :id #uuid "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
          :path "/shared/filesystem/store"}
 :keep-history?      true
 :schema-flexibility :read
 :writer             {:backend :datahike-server
                      :url     "http://localhost:4444"
                      :token   "securerandompassword"}}
```

You can now use the normal `datahike.api` as usual and all operations changing a
database, e.g. `create-database`, `delete-database` and `transact` are sent to
the server while all other calls are executed locally.

### AWS lambda

An example setup to run Datahike distributed in AWS lambda without a server can
be found [here](https://github.com/viesti/clj-lambda-datahike). It configures a
singleton lambda for write operations while reader lambdas can be run multiple
times and scale out. This setup can be upgraded later to use dedicated servers
through EC2 instances.

### Streaming writer (Kabel)

**Beta feature - please try it out and provide feedback.**

The Kabel writer provides **real-time reactive updates** via WebSockets, complementing the HTTP server's REST API. Where HTTP server is ideal for conventional REST integrations (including non-Clojure clients), Kabel enables live synchronization where clients receive database updates as they happen, without polling.

The stack consists of:

- [kabel](https://github.com/replikativ/kabel) - WebSocket transport with middleware support
- [distributed-scope](https://github.com/replikativ/distributed-scope) - Remote function invocation with Clojure semantics
- [konserve-sync](https://github.com/replikativ/konserve-sync) - Differential store synchronization (only transmits changed data)

This setup is particularly useful for browser clients where storage backends cannot be shared directly, and for applications requiring reactive UIs that update automatically when data changes on the server (see [JavaScript API](javascript-api.md)).

#### Server setup

The server owns the database and handles all write operations. It uses a file
backend and broadcasts updates to connected clients via konserve-sync.

```clojure
(ns my-app.server
  (:require [datahike.api :as d]
            [datahike.kabel.handlers :as handlers]
            [datahike.kabel.fressian-handlers :as fh]
            [kabel.peer :as peer]
            [kabel.http-kit :refer [create-http-kit-handler!]]
            [konserve-sync.core :as sync]
            [is.simm.distributed-scope :refer [remote-middleware invoke-on-peer]]
            [superv.async :refer [S go-try <?]]
            [clojure.core.async :refer [<!!]]))

(def server-id #uuid "aaaaaaaa-0000-0000-0000-000000000001")
(def server-url "ws://localhost:47296")

;; Fressian middleware with Datahike type handlers for serialization
(defn datahike-fressian-middleware [peer-config]
  (kabel.middleware.fressian/fressian
   (atom fh/read-handlers)
   (atom fh/write-handlers)
   peer-config))

;; Store config factory - maps client store UUID to server-side file store
;; Browsers use TieredStore (memory + IndexedDB), but the server uses file backend
;; The store-id parameter is the UUID from the client's :store :id field
(defn store-config-fn [store-id _client-config]
  {:backend :file
   :path (str "/var/data/datahike/" store-id)
   :id store-id})

(defn start-server! []
  (let [;; Create kabel server peer with middleware stack:
        ;; - sync/server-middleware: handles konserve-sync replication
        ;; - remote-middleware: handles distributed-scope RPC
        ;; - datahike-fressian-middleware: serializes Datahike types
        server (peer/server-peer
                S
                (create-http-kit-handler! S server-url server-id)
                server-id
                (comp (sync/server-middleware) remote-middleware)
                datahike-fressian-middleware)]

    ;; Start server and enable remote function invocation
    (<!! (peer/start server))
    (invoke-on-peer server)

    ;; Register global Datahike handlers for create-database, delete-database, transact
    ;; The :store-config-fn translates client config to server-side store config
    (handlers/register-global-handlers! server {:store-config-fn store-config-fn})

    server))
```

#### Browser client setup

Browser clients use a TieredStore combining fast in-memory access with
persistent IndexedDB storage. The KabelWriter sends transactions to the server,
and konserve-sync replicates updates back to the client's store.

**Store IDs**: Store IDs should be UUIDs for distributed coordination. Use a
fixed UUID when multiple clients need to share the same database, or generate a
unique UUID with `(random-uuid)` for ephemeral/test databases.

```clojure
(ns my-app.client
  (:require [cljs.core.async :refer [<! timeout alts!] :refer-macros [go]]
            [datahike.api :as d]
            [datahike.kabel.fressian-handlers :refer [datahike-fressian-middleware]]
            [is.simm.distributed-scope :as ds]
            [kabel.peer :as peer]
            [konserve-sync.core :as sync]
            [superv.async :refer [S] :refer-macros [go-try <?]]))

(def server-url "ws://localhost:47296")
(def server-id #uuid "aaaaaaaa-0000-0000-0000-000000000001")
(def client-id #uuid "bbbbbbbb-0000-0000-0000-000000000002")

(defonce client-peer (atom nil))

(defn init-peer! []
  ;; Create client peer with middleware stack (innermost runs first):
  ;; - ds/remote-middleware: handles distributed-scope RPC responses
  ;; - sync/client-middleware: handles konserve-sync replication
  (let [peer-atom (peer/client-peer
                   S
                   client-id
                   (comp ds/remote-middleware (sync/client-middleware))
                   datahike-fressian-middleware)]
    ;; Start invocation loop for handling remote calls
    (ds/invoke-on-peer peer-atom)
    (reset! client-peer peer-atom)))

(defn example []
  ;; go-try/<?  from superv.async propagate errors through async channels
  ;; Use go/<! if you prefer manual error handling
  (go-try S
    ;; Connect to server via distributed-scope
    (<? S (ds/connect-distributed-scope S @client-peer server-url))

    (let [store-id (random-uuid)
          db-name (str "db-" store-id)
          ;; TieredStore: memory frontend for fast reads, IndexedDB for persistence
          ;; The server uses file backend - store-config-fn handles this translation
          ;; Note: All :id values must match for konserve validation
          config {:store {:backend :tiered
                          :frontend-config {:backend :memory :id store-id}
                          :backend-config {:backend :indexeddb :name db-name :id store-id}
                          :id store-id}
                  :writer {:backend :kabel
                           :peer-id server-id
                           :local-peer @client-peer}
                  :schema-flexibility :write
                  :keep-history? false}]

      ;; Create database on server (transmitted via distributed-scope RPC)
      (<? S (d/create-database config))

      ;; Connect locally - syncs initial state from server via konserve-sync
      ;; TieredStore caches data from IndexedDB into memory before subscribing
      ;; so the sync handshake only requests keys newer than cached timestamps
      (let [conn (<? S (d/connect config {:sync? false}))]

        ;; Transact schema - sent to server, then synced back to local store
        (<? S (d/transact! conn [{:db/ident :name
                                  :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}]))

        ;; Transact data
        (<? S (d/transact! conn [{:name "Alice"} {:name "Bob"}]))

        ;; Query locally - no network round-trip needed
        (let [db (d/db conn)
              results (d/q '[:find ?name :where [?e :name ?name]] db)]
          (println "Found:" results))  ;; => #{["Alice"] ["Bob"]}

        ;; Clean up
        (d/release conn)
        (<? S (d/delete-database config))))))
```

# Remote Procedure Calls (RPC)

In addition to DIS, Datahike supports **remote procedure calls** where all operations (reads and writes) are executed on a server. This approach is complementary to DIS:

**Use RPC when:**
- You want simplified deployment (thin clients, all logic on server)
- Shared server-side caching benefits multiple clients
- Clients are resource-constrained (mobile, embedded)
- You need conventional REST integration with non-Clojure clients

**Use DIS when:**
- Read scalability is critical (unlimited readers without server load)
- You want offline-capable or low-latency reads
- Clients need to run custom queries with local functions/closures
- Network bandwidth or availability is a concern

The remote API has the same call signatures as `datahike.api` and is located in `datahike.api.client`. All functionality except `listen!` and `with` is supported. To use it, add `:remote-peer` to your config:

```clojure
{:store  {:backend :memory :id "distributed-datahike"}
 :keep-history?      true
 :schema-flexibility :read
 :remote-peer        {:backend :datahike-server
                      :url     "http://localhost:4444"
                      :token   "securerandompassword"}}
```

The API will return lightweight remote pointers that follow the same semantics
as `datahike.api`, but do not support any of Datahike's local functionality,
i.e. you can only use them with this API.

# Hybrid Architecture

You can combine DIS and RPC in the same deployment. For example:
- A set of application servers access a shared database via DIS (direct index access)
- These servers expose RPC/REST APIs to external clients
- Internal servers benefit from DIS scalability and local query execution
- External clients get a simple REST interface without needing Datahike dependencies

This pattern is common in production systems where internal services need high-performance data access while external integrations require conventional APIs.

# HTTP Server Setup

The HTTP server provides a **REST/RPC interface** for conventional integrations with any language or tool that speaks HTTP. Use this when you need request/response semantics rather than reactive updates (for reactive updates, see Kabel above).

To build locally, clone the repository and run `bb http-server-uber` to create the jar. Run the server with:

```bash
java -jar datahike-http-server-VERSION.jar path/to/config.edn
```

The edn configuration file looks like:

```clojure
{:port     4444
 :level    :debug
 :dev-mode true
 :token    "securerandompassword"}
```

Port sets the `port` to run the HTTP server under, `level` sets the log-level.
`dev-mode` deactivates authentication during development and if `token` is
provided then you need to send this token as the HTTP header "token" to
authenticate.

The server exports a swagger interface on the port and can serialize requests in
`transit-json`, `edn` and `JSON` with
[jsonista](https://github.com/metosin/jsonista) tagged literals. The server
exposes all referentially transparent calls (that don't change given their
arguments) as GET requests and all requests that depend on input information as
POST requests. All arguments in both cases are sent as a list *in the request
body*.

### Extended configuration

CORS headers can be set, e.g. with adding
```clojure
 :access-control-allow-origin [#"http://localhost" #"http://localhost:8080"]
```

The server also experimentally supports HTTP caching for GET requests, e.g. by adding
```clojure
 :cache {:get {:max-age 3600}}
```

This should be beneficially in case your HTTP client or proxy supports efficient
caching and you often run the same queries many times on different queries (e.g.
to retrieve a daily context in an app against a database only changes with low
frequency.)

# JSON Support (HTTP Server)

The HTTP server supports JSON with embedded [tagged literals](https://github.com/metosin/jsonista#tagged-json) for language-agnostic integration. This allows non-Clojure clients (JavaScript, Python, etc.) to interact with Datahike using familiar JSON syntax.

When sending HTTP requests to the datahike-server, you can use JSON argument arrays in each method body. Include the "token" header if authentication is enabled.

`POST` to "/create-database"
```javascript
["{:schema-flexibility :read}"]
```
Note that here you can pass the configuration as an `edn` string, which is more concise. If you want to speak JSON directly you would pass
```
[{"schema-flexibility": ["!kw", "read"]}]
```

"!kw" annotates a tagged literal here and encodes that "read" is an `edn`
keyword.

The resulting configuration will look like (with random DB name):
```javascript
cfg = {
  "keep-history?": true,
  "search-cache-size": 10000,
  "index": [
    "!kw",
    "datahike.index/persistent-set"
  ],
  "store": {
    "id": "wiggly-field-vole",
    "backend": [
      "!kw",
      "memory"
    ]
  },
  "store-cache-size": 1000,
  "attribute-refs?": false,
  "writer": {
    "backend": [
      "!kw",
      "self"
    ]
  },
  "crypto-hash?": false,
  "remote-peer": null,
  "schema-flexibility": [
    "!kw",
    "read"
  ],
  "branch": [
    "!kw",
    "db"
  ]
}
```

You can now use this cfg to connect to this database:

`POST` to "/connect"
```javascript
[cfg]
```

The result will look like:

```javascript
conn = ["!datahike/Connection",[[["!kw","memory"],"wiggly-field-vole"],["!kw","db"]]]
```

Finally let's add some data to the database:

`POST` to "/transact"
```javascript
[conn, [{"name": "Peter", "age": 42}]]
```

The result is a comprehensive transaction record (feel free to ignore the details):

```javascript
[
  "!datahike/TxReport",
  {
    "db-before": [
      "!datahike/DB",
      {
        "store-id": [
          [
            [
              "!kw",
              "memory"
            ],
            "wiggly-field-vole"
          ],
          [
            "!kw",
            "db"
          ]
        ],
        "commit-id": [
          "!uuid",
          "2c8f71f9-a3c6-4189-ba0c-e183cc29c672"
        ],
        "max-eid": 1,
        "max-tx": 536870913
      }
    ],
    "db-after": [
      "!datahike/DB",
      {
        "store-id": [
          [
            [
              "!kw",
              "memory"
            ],
            "wiggly-field-vole"
          ],
          [
            "!kw",
            "db"
          ]
        ],
        "commit-id": [
          "!uuid",
          "6ebf8979-cdf0-41f4-b615-30ff81830b0c"
        ],
        "max-eid": 2,
        "max-tx": 536870914
      }
    ],
    "tx-data": [
      [
        "!datahike/Datom",
        [
          536870914,
          [
            "!kw",
            "db/txInstant"
          ],
          [
            "!date",
            "1695952443102"
          ],
          536870914,
          true
        ]
      ],
      [
        "!datahike/Datom",
        [
          2,
          [
            "!kw",
            "age"
          ],
          42,
          536870914,
          true
        ]
      ],
      [
        "!datahike/Datom",
        [
          2,
          [
            "!kw",
            "name"
          ],
          "Peter",
          536870914,
          true
        ]
      ]
    ],
    "tempids": {
      "db/current-tx": 536870914
    },
    "tx-meta": {
      "db/txInstant": [
        "!date",
        "1695952443102"
      ],
      "db/commitId": [
        "!uuid",
        "6ebf8979-cdf0-41f4-b615-30ff81830b0c"
      ]
    }
  }
]
```

Note that you can extract the snapshots of the database `db-before` and `db-after` the commit as well as the facts added to the database as `tx-data`.

To retrieve the current database for your connection use

`POST` to "/db"
```javascript
[conn]
```

The result looks like:

```javascript
db = [
  "!datahike/DB",
  {
    "store-id": [
      [
        [
          "!kw",
          "mem"
        ],
        "127.0.1.1",
        "wiggly-field-vole"
      ],
      [
        "!kw",
        "db"
      ]
    ],
    "commit-id": [
      "!uuid",
      "6ebf8979-cdf0-41f4-b615-30ff81830b0c"
    ],
    "max-eid": 2,
    "max-tx": 536870914
  }
]
```

You can query this database with the query endpoint. We recommend again using a string to denote the query DSL instead of direct JSON encoding unless you want to manipulate the queries in JSON programmatically.

`GET` from "/q"
```javascript
["[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]", db]
```

The result set is

```javascript
["!set",[["Peter",42]]]
```

You can also pass strings for pull expressions and to pass configurations to `delete-database` and `database-exists`.
