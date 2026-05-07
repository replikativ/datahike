# Optimistic Overlay (`datahike.optimistic`)

**Status: Experimental** — The API may still change. Stable enough for
client-side prototyping; not yet exercised under multi-writer or
concurrent-peer load.

The `datahike.optimistic` namespace adds a thin overlay on top of a
Datahike connection that lets UIs render a transaction's effect
immediately, before the underlying writer has confirmed it. When the
write lands (or fails), listeners re-fire with the new effective
database.

The primitive is small (one namespace, no wire-protocol changes) and
works wherever Datahike runs — JVM, ClojureScript on Node, ClojureScript
in the browser. It is most useful when the conn's writer is remote
(e.g. the Kabel writer over a WebSocket) and the round-trip is visible
to the user.

## Motivation

A write through a streaming writer like Kabel goes:

1. `d/transact!` dispatches the tx to the server.
2. Server applies the tx and replies.
3. `konserve-sync` echoes the new `:db` key back to the client.
4. The client's `@conn` advances; the UI re-renders.

That whole loop is on the user's interaction path. For chat sends,
block edits, page creation, the lag is perceptible.

The overlay lets the UI render the post-tx state at step 1 — locally,
via Datahike's `d/with` — and reconcile when the durable update lands.

## Usage

```clojure
(require '[datahike.api :as d]
         '[datahike.optimistic :as opt]
         '[clojure.core.async :refer [<!]])

;; one-time wiring per conn
(opt/register! conn)

;; subscribe — fires on overlay add, overlay drop, and @conn advance,
;; with the *effective* db (overlay applied on top of @conn)
(opt/listen! conn ::ui (fn [eff-db]
                         (rerender-ui! eff-db)))

;; submit an optimistic write
(let [{:keys [ov-id result]}
      (opt/transact! conn [{:entity/uuid (random-uuid)
                            :S.Page/title "Untitled"}])]
  ;; result is a 1-buffer chan; takes deliver the server tx-report on
  ;; success, or a Throwable / js/Error on failure.
  (let [reply (<! result)]
    (when (instance? Throwable reply)
      (notify-user! "Save failed:" reply))))

;; teardown when the conn is no longer needed
(opt/unlisten!  conn ::ui)
(opt/unregister! conn)
```

`(opt/effective-db conn)` returns the overlay-applied db value
synchronously, useful for one-shot reads outside a listener context.
`(opt/pending conn)` returns the current pending entries.

## How it works

Each registered conn carries an *overlay* — a vector of pending
entries — and a set of listeners.

**Append.** `transact!` first runs `d/with` on the current effective
db. If the tx-data is malformed (schema violation, etc.), the call
throws synchronously and nothing is added. Otherwise an entry
`{:ov-id, :tx-data, :status :pending, :submitted-at}` is appended and
listeners fire with the new effective db.

**Effective db.** `(reduce d/db-with @conn @overlay)` — entries marked
`:transacting?` are skipped (see below).

**Dispatch.** The transact wrapper hands the tx to the writer
(`d/transact!`) by default. A `:dispatch-fn` opt lets the caller
substitute their own RPC (e.g. an `invoke-remote` that does
server-side bootstrap beyond a plain Datahike transact); whatever it
yields gets put on the `:result` channel.

**Removal.** Entries are matched and removed by `:ov-id`:

  - On dispatch success, `transact!` drops the entry; the conn-watcher
    has already fired listeners with the post-write effective db.
  - On dispatch failure, `transact!` drops the entry, fires listeners
    (rolling back the optimistic visibility), and puts the error on
    `:result`.
  - `unregister!` drops every entry tied to that conn.

The conn-watcher itself does not remove entries — its only job is to
fire listeners when `@conn` advances, so UI consumers see the new
effective db whether the advance came from our own writer or from a
`konserve-sync` echo of someone else's write.

**The `:transacting?` marker.** Just before dispatch, `transact!`
marks the entry as `:transacting?`. The conn-watcher fires during
`d/transact!`'s own `swap!`; without the marker, the listener would
re-apply the entry's tx-data via `d/with` on top of a `@conn` that
already has those datoms, briefly double-applying them. The marker
makes `effective-db*` skip that entry while it's mid-flight.

## Identity assumption

Tempid resolution may differ between the overlay's `d/with` and the
underlying transact (different numeric EIDs for the same logical
entity). Applications should identify entities by a stable attribute
— e.g. `:entity/uuid` minted client-side — rather than by EID.
Overlay applies with one EID, the writer applies with another, both
resolve the same `:entity/uuid` lookups.

## Relation to the Kabel writer

The overlay sits *in front of* the writer; it does not replace it.
With a Kabel-backed conn:

```clojure
{:store  {:backend :file :path "/data" :id #uuid "..."}
 :writer {:backend :kabel :peer-id #uuid "..." :local-peer kabel-peer}}
```

`(opt/transact! conn tx-data)` will:

1. Append an overlay entry, fire listeners (UI shows the change).
2. Dispatch via `d/transact!` → KabelWriter → server transact →
   konserve-sync echo → `@conn` advances.
3. Drop the entry; conn-watcher fires listeners with the durable
   effective db.

If your server side does more than a plain `d/transact` (e.g. creates
related entities, runs custom validation), pass `:dispatch-fn` so the
overlay is decoupled from the wire path:

```clojure
(opt/transact! conn tx-data
               {:dispatch-fn #(my-app/create-page-rpc! page-uuid title)})
```

## Limits in v1

- **Single-writer-per-conn assumption.** Overlay entry removal is
  driven by your own `transact!` calls. Concurrent peers writing to
  the same store are fine for sync but unstudied for the overlay's
  reconciliation behaviour.
- **No IndexedDB persistence of pending entries.** Overlay state is
  in-memory; a page reload drops in-flight optimistic entries. (Linear
  and InstantDB persist their pending queues; this can be added later.)
- **No global rejection broadcaster.** A failed transact rejects via
  the `:result` channel of the caller. There is no separate
  subscription for "tell me about all failed mutations." If you have a
  cross-cutting toast layer, wire it from the caller side for now.
- **Listener fires twice per successful transact** (once on append,
  once on conn advance). Both events show the correct effective db —
  no incorrect intermediate state — but it's redundant. Consumers can
  dedupe by comparing the `:max-tx` if they care.
- **Eager validation throws synchronously**; server-side errors arrive
  on the result channel. Two failure paths, two mental models — kept
  distinct because programmer errors in tx-data are categorically
  different from runtime server rejection.

## See also

- [Distributed Architecture](./distributed.md) — Kabel writer, the
  most common reason to want this primitive.
- [ClojureScript Support](./cljs-support.md) — backends and async
  caveats for browser/Node.
