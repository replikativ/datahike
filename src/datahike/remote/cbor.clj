(ns datahike.remote.cbor
  "The CBOR codec for datahike's REMOTE PROTOCOL — the one `datahike.remote`
  and `datahike.transit` already speak in transit, EDN and JSON.

  Nothing here is HTTP. The protocol is defined by what crosses the wire — a
  handle rather than a value, resolved at the far end through the connection
  registry — and HTTP is merely the transport that carries it today; a
  websocket or a raw socket would use this unchanged. So this sits beside
  `datahike.remote` in `src/`, exactly as `datahike.transit` does, and the
  `application/cbor` content-type negotiation lives with the server in
  `http-server/`. Handler sets are the protocol; media types are the transport.

  Distinct from `datahike.cbor`, which is the codec for the kabel wire and for
  konserve blobs. The two carry the same Datom, but a DB is a genuinely
  different wire type in each: over kabel it goes as `db->stored`, with index
  roots the far peer resolves through the shared storage registry, while here
  it goes as a HANDLE — store-id, commit-id, max-tx, max-eid — that the far end
  turns back into a database by looking up a live connection. A remote peer
  holds no store, so `datahike.cbor`'s DB handlers are not merely unnecessary
  here, they are wrong. They are deliberately not installed.

  ## Both halves live here, on purpose

  The wire has two ends and they are not symmetric:

  | | client sends | server sends |
  |---|---|---|
  | connection | `remote.RemoteConnection` | `connector.Connection` |
  | database   | `remote.RemoteDB`         | `db.DB` |
  | entity     | `remote.RemoteEntity`     | `impl.entity.Entity` |

  One logical type, two implementation types, one wire name. The transit codec
  splits these halves across `datahike.remote` (client) and `datahike.transit`
  (server), and they drifted: every handle but one strips `:remote-peer` before
  writing, and `RemoteEntity` did not — so it put the peer's bearer token in
  the request body. Keeping the two columns adjacent is what makes that kind of
  omission visible.

  ## Wire names

  The seven handle names are transit's, unchanged: `datahike/Connection`,
  `datahike/DB`, `datahike/HistoricalDB`, `datahike/SinceDB`,
  `datahike/AsOfDB`, `datahike/Entity`, `datahike/TxReport`. The protocol
  already publishes this vocabulary in three formats; a fourth spelling would
  buy nothing and would cost a client switching between them.

  A Datom is the exception: it keeps `datahike.datom/Datom` from
  `datahike.cbor`, NOT transit's `datahike/Datom`. Unlike the handles, a Datom
  is the same type with the same five-value payload at both ends and on every
  other datahike wire, so it gets one name everywhere — which is the point of
  naming types in an IETF format rather than a private one. Transit's spelling
  is its own legacy and is left alone.

  ## Registration is not optional here

  boring emits an unregistered defrecord NATIVELY, as tag 27 under its derived
  `ns/Record` name. That is a good default and a trap for this namespace: an
  unregistered `RemoteConnection` goes out as `datahike.remote/RemoteConnection`
  **with its `:remote-peer` field, token and all** — measured at 170 bytes
  against the ~30 it should cost. EDN is protected from this by its
  `print-method` overrides and transit/JSON have no default for records at all,
  so CBOR is the only format here that fails OPEN.

  It bites the defrecords only. `connector.Connection` and
  `impl.entity.Entity` are deftypes, which boring refuses rather than guesses
  at — those fail closed. The exposed set is the six `Remote*` records on the
  client and `DB`/`TxReport`/`HistoricalDB`/`SinceDB`/`AsOfDB` on the server.

  So every record type on either end must be registered, and
  `datahike.test.remote-cbor-test` asserts that by enumerating the record types
  in those namespaces rather than repeating a list that can go stale."
  (:require [boring.core :as boring]
            [datahike.cbor :as dcbor]
            [datahike.db]
            [datahike.readers :as readers]
            [datahike.remote :as remote]
            [datahike.connector]
            [datahike.impl.entity]
            [datahike.transit :refer [db->map config->store-id]]))

;; ---------------------------------------------------------------------------
;; Wire names
;; ---------------------------------------------------------------------------

(def ^:const connection-name "datahike/Connection")
(def ^:const db-name         "datahike/DB")
(def ^:const historical-name "datahike/HistoricalDB")
(def ^:const since-name      "datahike/SinceDB")
(def ^:const as-of-name      "datahike/AsOfDB")
(def ^:const entity-name     "datahike/Entity")
(def ^:const tx-report-name  "datahike/TxReport")

(def wire-names
  "Every name this codec speaks, including the Datom name `datahike.cbor` owns.
  Both registries must cover exactly this set."
  #{connection-name db-name historical-name since-name as-of-name
    entity-name tx-report-name dcbor/datom-name})

;; ---------------------------------------------------------------------------
;; Decode options
;;
;; `:on-unknown-record :error`, not boring's `:fallback` default. A fallback is
;; right for a dump, which a stranger should be able to read partially; it is
;; wrong for an RPC, where a tag-27 name neither end registered means the peers
;; disagree about the protocol, and degrading to a tagged literal defers that
;; into an unrelated failure later.
;;
;; It is also what keeps the native image honest. boring's only reflective
;; paths are `:auto-construct-records?` (off by default, unused here) and
;; `records/auto-registry` (unused here); both fail SILENTLY under a
;; closed-world image, returning a carrier or an empty registry. Explicit
;; registration plus `:error` means a native binary either works or says so.
;; ---------------------------------------------------------------------------

(defn decode-opts [registry]
  {:registry registry :on-unknown-record :error})

;; ---------------------------------------------------------------------------
;; Client side: writes handles, reads handles
;; ---------------------------------------------------------------------------

(defn install-client
  "Handlers for a peer that holds no store: the `Remote*` records go out as
  bare handles and come back reattached to `*remote-peer*`.

  Every writer strips `:remote-peer`. It is a client-side back-reference
  carrying the peer's url and auth token, and it has no meaning at the far
  end."
  [reg]
  (-> reg
      dcbor/install-element-handlers

      (boring/register-tag 27 datahike.remote.RemoteConnection
                           (fn [c] [connection-name (:store-id c)]) nil)
      (boring/register-record connection-name remote/remote-connection)

      (boring/register-tag 27 datahike.remote.RemoteDB
                           (fn [db] [db-name (remote/map-without-remote db)]) nil)
      (boring/register-record db-name remote/remote-db)

      (boring/register-tag 27 datahike.remote.RemoteHistoricalDB
                           (fn [db] [historical-name (remote/map-without-remote db)]) nil)
      (boring/register-record historical-name remote/remote-historical-db)

      (boring/register-tag 27 datahike.remote.RemoteSinceDB
                           (fn [db] [since-name (remote/map-without-remote db)]) nil)
      (boring/register-record since-name remote/remote-since-db)

      (boring/register-tag 27 datahike.remote.RemoteAsOfDB
                           (fn [db] [as-of-name (remote/map-without-remote db)]) nil)
      (boring/register-record as-of-name remote/remote-as-of-db)

      (boring/register-tag 27 datahike.remote.RemoteEntity
                           (fn [e] [entity-name (remote/map-without-remote e)]) nil)
      (boring/register-record entity-name remote/remote-entity)

      ;; A TxReport has no :remote-peer field, and its :db-before/:db-after are
      ;; RemoteDBs written by the handler above, nested. The client only ever
      ;; reads one, but registering the write side keeps a client-held report
      ;; from falling through to boring's native record emission.
      (boring/register-tag 27 datahike.db.TxReport
                           (fn [r] [tx-report-name (into {} r)]) nil)
      (boring/register-record tx-report-name datahike.db/map->TxReport)))

;; ---------------------------------------------------------------------------
;; Server side: writes handles, reads real objects
;; ---------------------------------------------------------------------------

(defn install-server
  "Handlers for the peer that holds the store. Reads resolve a handle into a
  live object through `datahike.readers`, which is where the connection
  registry lookup lives; writes project a live object back down to a handle.

  `DB` and the three time-travel DBs are registered as much to SUPPRESS
  boring's native record emission as to enable the handle: unregistered, a DB
  would go out as its whole in-memory record, index roots included."
  [reg]
  (-> reg
      dcbor/install-element-handlers

      (boring/register-tag 27 datahike.connector.Connection
                           (fn [c] [connection-name
                                    (config->store-id (:config @(:wrapped-atom c)))]) nil)
      (boring/register-record connection-name readers/connection-from-reader)

      (boring/register-tag 27 datahike.db.DB
                           (fn [db] [db-name (db->map db)]) nil)
      (boring/register-record db-name readers/db-from-reader)

      (boring/register-tag 27 datahike.db.HistoricalDB
                           (fn [{:keys [origin-db]}]
                             [historical-name {:origin origin-db}]) nil)
      (boring/register-record historical-name readers/history-from-reader)

      (boring/register-tag 27 datahike.db.SinceDB
                           (fn [{:keys [origin-db time-point]}]
                             [since-name {:origin origin-db :time-point time-point}]) nil)
      (boring/register-record since-name readers/since-from-reader)

      (boring/register-tag 27 datahike.db.AsOfDB
                           (fn [{:keys [origin-db time-point]}]
                             [as-of-name {:origin origin-db :time-point time-point}]) nil)
      (boring/register-record as-of-name readers/as-of-from-reader)

      (boring/register-tag 27 datahike.impl.entity.Entity
                           (fn [^datahike.impl.entity.Entity e]
                             [entity-name (assoc (into {} e)
                                                 :db (.-db e)
                                                 :eid (.-eid e))]) nil)
      (boring/register-record entity-name readers/entity-from-reader)

      (boring/register-tag 27 datahike.db.TxReport
                           (fn [r] [tx-report-name (into {} r)]) nil)
      (boring/register-record tx-report-name datahike.db/map->TxReport)))

;; ---------------------------------------------------------------------------
;; Convenience
;; ---------------------------------------------------------------------------

(defn client-registry [] (install-client (boring/tag-registry)))
(defn server-registry [] (install-server (boring/tag-registry)))
