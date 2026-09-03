# Browser replicas: Datahike in the browser, writing through a server

**Status: Beta.** The API is tested on Chrome, Node and the JVM; the limits
at the end are real and stated.

A Datahike database can live in the browser, in IndexedDB, and stay a replica
of a database a server owns. Queries run locally against the replica. Writes
go to the server, which applies them and streams the changed storage back, so
every replica converges on the same database value. The server validates a
JWT per connection and decides per database who may read and who may write.

This page is the TypeScript view of that setup. The Clojure and
ClojureScript equivalents are in [distributed.md](distributed.md); the HTTP
API and its authorization in [http-routes.md](http-routes.md).

## 1. Run a server

The standalone server ships as a container. Give it a configuration file with
the Kabel listener and a system database, the database that holds the
catalog and the permissions:

```clojure
;; server.edn
{:host "0.0.0.0"
 :port 4444
 :token "replace-with-a-long-random-token"          ; the break-glass identity
 :system-db {:store {:backend :file :path "/var/lib/datahike/system"}}
 :kabel {:host "0.0.0.0"
         :port 47296
         :jwt {:alg :HS256                            ; or :RS256 with :public-key, or an :issuers registry
               :secret "the-secret-your-app-signs-tokens-with"
               :required-claims {:iss "my-app" :aud "datahike"}}
         :store {:backend :file :path "/var/lib/datahike/browser-databases"}}}
```

```bash
docker run --name datahike --detach \
  --publish 4444:4444 --publish 47296:47296 \
  --mount type=volume,source=datahike-data,target=/var/lib/datahike \
  --mount type=bind,source="$PWD/server.edn",target=/run/secrets/server.edn,readonly \
  ghcr.io/replikativ/datahike-server:latest --config /run/secrets/server.edn
```

Port 4444 is the HTTP API, used below to manage permissions. Port 47296 is
the WebSocket the browser connects to; put TLS in front of it and use `wss://`
anywhere but on a loopback development setup. The server never lets a client
choose a filesystem path: every browser database is stored below the
listener's `:store :path` in a directory named for its id.

The server validates tokens; it does not issue them. Your application does,
with the algorithm and key above, and the token's `sub` is the user. Any
issuer whose tokens `kabel.auth.jwt` can verify works, including JWKS-backed
providers.

## 2. Connect from the browser

```typescript
import * as d from 'datahike/kabel';

const storeId  = d.uuid('550e8400-e29b-41d4-a716-446655440000'); // the database's identity, chosen by you
const serverId = d.uuid('aaaaaaaa-0000-0000-0000-000000000001'); // the listener's default :peer-id

const peer = d.createKabelPeer(d.randomUuid(), {
  token: () => getAccessToken(),        // read at every connection and before the token expires
  onError: (e) => console.warn('auth', e)
});
d.maintainKabelPeer(peer, 'wss://data.example.com', {
  onStatus: (e) => setConnectionState(e.status) // connecting | connected | authenticated | disconnected | failed | stopped
});

const conn = await d.connect({
  store: {
    backend: ':tiered',
    id: storeId,
    'frontend-config': { backend: ':memory', id: storeId },
    'backend-config':  { backend: ':indexeddb', id: storeId, name: 'my-app' },
    'write-policy': ':write-through'
  },
  writer: { backend: ':kabel', 'peer-id': serverId, 'local-peer': peer }
}, { 'sync?': false });

await d.transact(conn, [{ ':person/name': 'Alice' }]);       // goes to the server, comes back through sync
const db = await d.db(conn);
await d.q('[:find ?n :where [?e :person/name ?n]]', db);      // runs locally
```

Four things happen here. `createKabelPeer` builds the connection with your
token source and starts serving remote functions on the page.
`maintainKabelPeer` keeps it connected, reconnecting with backoff and
reporting every transition. `connect` opens the local replica, subscribes to
the database's storage and waits for the initial sync to land before it
returns. `transact` sends the write to the server and resolves once the
resulting storage has replicated back, so the returned report already
reflects the server's decision.

The database is created the same way, with `d.createDatabase(config)` on the
same configuration, which asks the server to create and register it. Who may
do that is the next section.

## 3. Decide who may do what

Every write and every subscription is authorized by the server against its
permission graph, the same one the HTTP API enforces. The graph knows
`user`s by their token subject, one `server` with `admin`s, and per
`database` the relations `owner`, `writer` and `reader`:

| may | reader | writer | owner | admin |
|---|---|---|---|---|
| read, subscribe | ✓ | ✓ | ✓ | ✓ |
| transact | | ✓ | ✓ | ✓ |
| delete, grant | | | ✓ | ✓ |
| create a database | | | | ✓ |

Grants go through the HTTP API, by an admin or by an owner of the database,
typically from your backend when it provisions a tenant:

```typescript
await fetch('https://api.example.com/permissions/relationships!', {
  method: 'POST',
  headers: { authorization: `Bearer ${adminToken}`, 'content-type': 'application/json' },
  body: JSON.stringify([{
    operation: 'touch',                             // touch | create | delete
    relationship: { subject:  { type: 'user', id: 'alice' },
                    relation: 'writer',
                    resource: { type: 'database', id: storeId } }
  }])
});
```

`POST /permissions/check` with `{subject, permission, resource}` answers
`{allowed}`, and `POST /permissions/relationships` with `{resource}` lists a
database's relationships. A user cannot list databases it cannot read.

For a multi-tenant application the natural unit is one database per tenant
or per user: databases are cheap, the graph keeps them apart, and every
replica of one database sees the same facts.

## 4. Put your own operations in front

Browsers may transact directly, but many applications want domain
operations instead: validated, run under the caller's identity, free to touch
several databases. The server serves functions you register on it, and a page
can call them by name:

```typescript
const receipt = await d.invokeRemote(peer, serverId, 'shop/place-order', { items });
```

The server side is Clojure, embedded in your own process next to the
listener:

```clojure
(require '[kabel.remote :as remote])

(remote/register! 'shop/place-order
  (fn [{:keys [kabel/principal items]}]      ; the JWT claims, stamped by the server
    (place-order! (:sub principal) items)))
```

The policy is asked about such a function as `{:op :invoke :fn-name …}`.
Without a policy of your own only server admins may invoke it; with one,
composed over the graph, you decide per function, and Datahike's own writes
stay gated by the graph, so a client that skips your domain API still cannot
write what it was not granted. The page can also serve functions the server
calls back into, with `d.registerRemoteFn(name, fn)`.

## 5. What is not there yet

- **No durable offline write queue.** A write issued while disconnected waits
  in memory for the connection and is lost on a page reload.
- **No multi-tab coordination.** Each tab is its own replica and connection.
- **Subscriptions are per connection.** After a reconnection the peer
  authenticates again and remote functions are announced again, but a store
  subscription made by `connect` is not remade; reconnect the database when
  `onStatus` reports `connected`.
- **Permissions are per database.** Row- or attribute-level rules are not
  modelled in the graph; a custom `:authorize` policy on the server sees the
  transaction data and can refuse a write by its content.

## Reference

| npm `datahike/kabel` | |
|---|---|
| `createKabelPeer(clientId, {token, onAuth, onError})` | the peer; `token` is a string or a function returning one, or a promise of one |
| `connectKabelPeer(peer, url)` | connect once; resolves with the server's peer id |
| `maintainKabelPeer(peer, url, {onStatus, backoff, maxAttempts})` | keep connected; returns `{stop, done}` |
| `refreshKabelToken(peer, token?)` | replace the token on the live connection |
| `invokeRemote(peer, remoteId, 'ns/name', args)` | call a function the server serves |
| `registerRemoteFn('ns/name', fn)` | serve a function from the page |
| `stopKabelPeer(peer)` | stop the peer |

Errors from `invokeRemote` carry a `type`: `kabel.remote/not-authorized`,
`kabel.remote/authentication-required`, `kabel.remote/unknown-function`,
`kabel.remote/disconnected` when the connection dropped mid-call, or the type
of the exception the function threw.
