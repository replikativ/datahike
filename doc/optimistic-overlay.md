# Optimistic Overlay (`datahike.optimistic`)

**Status: Experimental** — Stable enough for client-side prototyping.
Exercised under same-conn concurrency (concurrent `opt/transact!`s and
interleaved direct `d/transact!`s, including the unique-collision
Case J path); not yet exercised across multiple peers writing to the
same store over a real Kabel connection.

The `datahike.optimistic` namespace adds a thin overlay on top of a
Datahike connection that lets UIs render a transaction's effect
immediately, before the underlying writer has confirmed it. The
durable update lands later; the overlay reconciles automatically.

The primitive is small (one namespace, no wire-protocol changes) and
works wherever Datahike runs — JVM, ClojureScript on Node, ClojureScript
in the browser. It is most useful when the conn's writer is remote
(e.g. the Kabel writer over a WebSocket) and the round-trip is visible
to the user.

## Motivation

A write through a streaming writer like Kabel goes:

1. `d/transact!` dispatches the tx to the server.
2. Server applies the tx and replies with a tx-report.
3. konserve-sync echoes the new `:db` key back to the client.
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
(opt/register! conn
  {:ttl-ms 30000                            ;; per-entry timeout, default 30s
   :on-conflict (fn [conflicts]             ;; see "Conflicts" below
                  (toast "your edit may not save"))})

;; subscribe — fires on overlay add, overlay drop, and @conn advance,
;; with the *effective* db (overlay applied on top of @conn).
(opt/listen! conn ::ui (fn [eff-db] (rerender-ui! eff-db)))

;; or subscribe to tx-reports if you have a differential UI layer
;; (posh-style materialized views, redux-style reducers, etc.) — see
;; "Tx-report listeners" below.
(opt/listen-tx! conn ::ui
                (fn [{:keys [db-before db-after tx-data origin ov-id]}]
                  (apply-deltas-to-ui-state! tx-data)))

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
`(opt/pending conn)` returns the current pending entries (with
`:conflicting?` and `:last-conflict-error` flags surfaced).

## Consistency Model

The overlay maintains one invariant:

> **An entry is visible from `transact!` return until `@conn` has
> demonstrably reflected its effect, the dispatch failed, or the
> entry's TTL expired.**

Three things make this hold against all the interference modes we've
tested.

### 1. `max-tx` is a sound watermark

When the dispatch resolves successfully, the reply carries the durable
write's `:max-tx`. The overlay stamps the entry with that value as
`:expected-max-tx`. A `@conn` watcher drops the entry the first time
`(:max-tx @conn) >= :expected-max-tx`.

That's not a property of `:max-tx` as a number; it rests on three
structural facts of Datahike:

- **Monotonic assignment.** The writer assigns `:max-tx` to each
  commit in strictly increasing order; no two commits get the same
  value.
- **Snapshot completeness.** Each `:db` value the writer publishes is
  a fully indexed, closed snapshot — the result of applying every
  commit ≤ its own `:max-tx`. The chain of `db_N` values is totally
  ordered, and each is a superset (in history) of all earlier ones.
- **Atomic `@conn` update.** konserve-sync's `on-db-sync!` (kabel) and
  the `:self` writer's commit loop both `reset!` the conn atom with a
  fully-materialized `:db` value. There is no window where `:max-tx`
  has advanced but the indexes haven't.

So when we see `(:max-tx @conn) >= M`, the local `@conn` *is* a
snapshot `db_N` with `N >= M`, fully materialized, containing every
commit ≤ M — including ours.

If our entry's datoms have since been retracted by a later commit,
`@conn`'s current view doesn't have them, and that is the right answer
— the overlay's job is to predict ahead, not to override the durable
truth.

### 2. The default writer chain already gates `@conn` advance

Both shipped writers wait for `@conn` to advance before returning the
tx-report:

- **`:self`** does `(reset! connection commit-db)` *before*
  `(>! callback tx-report)` (`writer.cljc:128`).
- **`:kabel`** registers a waiter keyed by `expected-max-tx` and only
  resolves the dispatch once `on-sync-update!` observes `@conn` past
  it (`kabel/writer.cljc:99-131`).

The overlay's watermark logic is the structural belt-and-suspenders.
With these writers, the sync-check immediately after setting
`:expected-max-tx` finds `@conn` already caught up and drops the entry
right there.

### 3. Idempotent upsert covers the window

Between `transact!` and the dispatch resolving, multiple things can
fire the watcher (a concurrent peer write, a direct `d/transact!`, our
own writer's commit). Each fire recomputes the effective-db by folding
all overlay entries through `d/with` on top of `@conn`. Per the
identity assumption (entities keyed by a stable attribute like
`:entity/uuid`), re-applying an entry whose datoms already landed in
`@conn` is an idempotent upsert — no double application, no flicker.

## The `:dispatch-fn` Contract

By default, `transact!` routes through the conn's writer
(`d/transact!`) and extracts `:max-tx` from `(:max-tx (:db-after
tx-report))`. For app-level RPCs, pass `:dispatch-fn` with this
contract:

- Takes no arguments.
- Returns a **`core.async` channel** that yields a single value
  (typically a `promise-chan` or the channel of an `a/go`/`a/thread`
  block). Datahike's own `throwable-promise` returned by
  `d/transact!` implements `ReadPort` and qualifies.
- On success: yields `{:reply X :max-tx N}` where `N` is the
  `:max-tx` of the durable commit your RPC produced. `X` is what
  gets put on `:result`.
- On failure: throws (CLJ) or yields a `Throwable` / `js/Error` on
  the channel. The wrapper normalizes both onto the same failure
  path — the entry drops, listeners re-fire, the error lands on
  `:result`.

```clojure
(require '[clojure.core.async :as a])

(opt/transact! conn tx-data
  {:dispatch-fn
   (fn []
     (let [out (a/chan 1)]
       (a/go
         (let [report (a/<! (d/transact! conn enriched-tx-data))]
           (a/put! out {:reply  report
                        :max-tx (:max-tx (:db-after report))})))
       out))})
```

`d/transact!` returns a `throwable-promise` which itself implements
`core.async/ReadPort` (and `put!`s the Throwable as a value on
failure — no re-throw), so `<!` reads the reply uniformly on both
platforms.

## Conflicts (Case J)

A pending overlay entry can become un-applicable on top of `@conn`:
a concurrent peer or direct `d/transact!` may have written something
the entry's `d/with` would now reject (e.g. a `:db.unique/value`
collision). When this happens:

- The entry stays in the overlay — we don't yet know the server's
  verdict.
- It is **excluded** from `effective-db` — re-applying it would throw.
- It is **marked** `:conflicting? true` and carries
  `:last-conflict-error`.
- The `:on-conflict` callback (if registered) fires whenever the set
  of conflicting entries changes.

The dispatch is still in flight. When it resolves:

- **Success** (the server reconciled or our timing missed): the entry
  was actually fine. `@conn` advances with our datoms; the entry's
  watermark drops it. The view converges.
- **Failure** (server rejection): the conflict was real. The entry
  drops, listeners re-fire with the rolled-back view, the throwable
  lands on `:result`.

The `:on-conflict` callback gives the UI a place to warn the user
("your edit may not save") *before* the server's verdict arrives —
useful when the server response is slow.

## Tx-report listeners

For UI layers that maintain a materialized derived view (posh-style
query subscriptions, redux-style reducers, anything keyed by
`[entity attribute value]`), the eff-db listener forces a full
recompute on every event. `listen-tx!` instead delivers Datahike-style
**tx-reports** so the UI can apply incremental deltas:

```clojure
(opt/listen-tx! conn :my-key
  (fn [{:keys [db-before db-after tx-data origin ov-id]}]
    ;; tx-data is a vector of #datahike/Datom [e a v t added?]
    (doseq [d tx-data]
      (if (dd/datom-added d)
        (assert-datom! ui-state [(.-e d) (.-a d) (.-v d)])
        (retract-datom! ui-state [(.-e d) (.-a d) (.-v d)])))))
```

### Shape compatibility with `d/transact!`

The tx-report is a plain map (not a `TxReport` record), but its core
keys (`:db-before`, `:db-after`, `:tx-data`) match Datahike's
`TxReport` exactly — same names, same value types (the datoms in
`:tx-data` are real `#datahike/Datom` records). On `:overlay-add` and
`:conn-advance` events the report also carries the originating
`:tempids` and `:tx-meta`, so a consumer that destructures
`{:keys [db-before db-after tx-data tempids tx-meta]}` works on both
optimistic events and Datahike's native tx-reports. On derived events
(`:overlay-drop`, `:overlay-conflict`, `:overlay-resolve`, `:ttl`,
`:overlay-realized`) those two keys are absent — there's no underlying
transact to source them from.

The map additionally carries `:origin` (always) and `:ov-id` (on
entry-scoped events) so consumers can distinguish event sources.

### Event types (`:origin`)

| Origin | When it fires | `:tx-data` contains |
|---|---|---|
| `:overlay-add` | `opt/transact!` accepted, entry added | The predicted datoms from eager `d/with` |
| `:conn-advance` | Any `d/transact!` through this conn (your own or a direct call) | The durable writer's `:tx-data` |
| `:overlay-realized` | Your own write's `:expected-max-tx` was reached, entry dropped | Retracts of any *stale predictions* whose EIDs differed from the writer's (empty when EIDs matched) |
| `:overlay-conflict` | An entry's prediction becomes un-applicable on top of current `@conn` (Case J) | Retracts of the entry's prediction |
| `:overlay-resolve` | A previously-conflicting entry becomes applicable again | Re-adds of the entry's prediction |
| `:overlay-drop` | Dispatch failed for a non-conflicting entry | Retracts of the entry's prediction |
| `:ttl` | Entry's TTL elapsed | Retracts of the entry's prediction |

`:ov-id` is present on entry-scoped events (`:overlay-add`,
`:overlay-realized`, `:overlay-drop`, `:overlay-conflict`,
`:overlay-resolve`, `:ttl`). The `:conn-advance` from the writer
itself has no `:ov-id`.

### Convergence guarantee

Apply the `:tx-data` of every event in order to your derived view
(asserting added datoms, retracting non-added ones). The result is
**always equivalent to `:db-after`** — the consumer's incremental
view stays in sync with `effective-db`, regardless of what happened:

- happy path: `+predicted` then `+writer + retract-stale-predictions`,
- failure: `+predicted` then `-predicted`,
- TTL: `+predicted` then `-predicted`,
- concurrent peer write makes us conflict: `+predicted` then
  `-predicted`, then `+peer-writes` from `:conn-advance`,
- concurrent peer's retraction unblocks our entry: `+peer-retract`
  then `+predicted`.

The EID-shift case (`:db.cardinality/many` with tempids; client- and
server-assigned EIDs differ) is handled by emitting *retracts of
stale predictions* alongside the writer's tx-data. Consumers see the
EID swap explicitly. Keying by stable attributes (`:entity/uuid`) is
strongly recommended.

### v1 limits on tx-reports

- **Custom `:dispatch-fn` that bypasses `d/transact!`** (e.g., a
  remote HTTP POST whose echo comes in via a separate mechanism): no
  `:conn-advance` fires for the durable write. The dispatch path's
  `:overlay-realized` still emits stale-retract cleanup; for the
  durable adds, route them through `d/transact!` (or a wrapper) so the
  writer-listener fires.
- **Foreign-peer writes via konserve-sync** (multi-peer Kabel
  scenarios where another peer's write echoes in via `on-db-sync!` but
  doesn't go through `d/transact!`): no `:conn-advance` fires.
  Standard `listen!` consumers still see `effective-db` updates from
  these writes; only `listen-tx!` consumers miss the delta.

## TTL

Each entry carries an `:expires-at` (default `now + 30000ms`). A
per-conn heartbeat ticks once a second and reaps expired entries:

- The entry's `:result` chan receives an `ex-info` tagged
  `{:type :optimistic/timeout}`.
- The entry is dropped from the overlay and listeners re-fire.
- If the server then *eventually* succeeds, the late reply is silently
  discarded — `:result` is exactly-once-delivery (the `compare-and-set!`
  on `:result-delivered?` is the gate). The durable effect surfaces
  via `@conn` advancing through normal sync.

Override the default at registration (`:ttl-ms 60000`, `:ttl-ms nil`
to disable) or per call (`(opt/transact! conn data {:ttl-ms 60000})`).

## Identity Assumption

Tempid resolution may differ between the overlay's `d/with` and the
underlying writer (different numeric EIDs for the same logical
entity). Applications should identify entities by a stable attribute
— e.g. `:entity/uuid` minted client-side — rather than by EID. The
overlay applies with one EID, the writer applies with another, both
resolve the same `:entity/uuid` lookups, and re-application on top of
`@conn` after echo is an idempotent upsert.

## Relation to the Kabel Writer

The overlay sits *in front of* the writer; it does not replace it.
With a Kabel-backed conn:

```clojure
{:store  {:backend :file :path "/data" :id #uuid "..."}
 :writer {:backend :kabel :peer-id #uuid "..." :local-peer kabel-peer}}
```

`(opt/transact! conn tx-data)` will:

1. Append an overlay entry, fire listeners (UI shows the change).
2. Dispatch via `d/transact!` → KabelWriter → server transact →
   konserve-sync echo → `@conn` advance → KabelWriter's wait-ch
   resolves → tx-report delivered.
3. Stamp the entry with `:expected-max-tx` from the tx-report.
4. Sync-check: `@conn` is already caught up (Kabel gated on it). Drop.
5. Deliver the tx-report on `:result`.

If your server side does more than a plain `d/transact` (creates
related entities, runs custom validation, etc.), pass `:dispatch-fn`
so the overlay is decoupled from the wire path. Remember to include
`:max-tx` in your success shape.

## V1 Limits

- **Conn-bound branches are stable.** Switching a conn between
  branches is not supported in Datahike (each branch is its own
  conn) — so the overlay assumes a stable branch per conn for the
  lifetime of `register!`.
- **Writer queue durability is the writer's problem.** If the Kabel
  WebSocket drops between dispatch and echo, the dispatch fails or
  the entry's TTL expires. Restoring a pending queue on reconnect
  belongs in the Kabel writer, not the overlay. Open follow-up.
- **Cross-peer scenarios are unstudied.** Same-conn concurrency
  (concurrent `opt/transact!`s on one conn, interleaved direct
  `d/transact!`s, Case J unique-collision) is exercised by the smoke
  tests. *Multiple* peers writing to the same store over a real
  Kabel connection — where `@conn` may advance via `on-db-sync!` from
  a write none of your dispatches knows about — is structurally
  supported by the `:max-tx` watermark argument but hasn't been put
  on the bench yet.
- **No IndexedDB persistence of pending entries.** Overlay state is
  in-memory; a page reload drops in-flight optimistic entries.
- **Eager validation throws synchronously**; server-side errors arrive
  on the result channel. Two failure paths, two mental models —
  programmer errors in tx-data are categorically different from
  runtime server rejection.
- **Listener fires twice per successful `transact!`** (once on
  overlay add, once when `@conn` advances with the durable datoms).
  Both events show the correct effective db — no incorrect
  intermediate state — but it's redundant. Consumers can dedupe by
  comparing `:max-tx` if they care.

## See also

- [Distributed Architecture](./distributed.md) — Kabel writer, the
  most common reason to want this primitive.
- [ClojureScript Support](./cljs-support.md) — backends and async
  caveats for browser/Node.
