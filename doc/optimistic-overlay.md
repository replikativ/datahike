# Optimistic Overlay (`datahike.optimistic`)

**Status: Experimental** — Stable enough for client-side prototyping.
Exercised under same-conn concurrency (concurrent `opt/transact!`s,
interleaved direct `d/transact!`s, writer batching, Case J unique-
collisions); not yet exercised across multiple peers writing to the
same store over a real Kabel connection.

## What it does

A thin overlay over a Datahike connection that lets UIs render a
transaction's effect **immediately** — locally, via `d/with` — while
the durable writer commits in the background. When the durable
update lands the overlay reconciles automatically; if the writer
rejects, the optimistic effect rolls back.

The primitive works wherever Datahike runs (JVM, ClojureScript on
Node, browser) and on top of any writer that conforms to Datahike's
standard `dispatch!` protocol — `:self` (default), `:kabel`
(WebSocket streaming), `:datahike-server` (HTTP). It's most useful
when the writer is remote and the round-trip is on the user's
interaction path.

The invariant is one sentence:

> **An entry is visible from `transact!` return until `@conn` has
> demonstrably reflected its effect, the dispatch failed, or the
> entry's TTL expired.**

See [How it works](#how-it-works) for why that holds.

## When to use this

**Yes** — your UI re-renders from `@conn` (or a derived view) and the
writer round-trip is perceptible. Chat sends, block edits, page
creation, anywhere users see lag between click and screen update.
The overlay shrinks the perceived latency to zero while the durable
write proceeds in the background.

**No** — your domain needs *server-authoritative* conflict
resolution beyond what eager `d/with` validation gives you. The
overlay is not a CRDT: optimistic writes go through your normal
durable writer, with the writer's normal conflict semantics
(uniqueness, schema validation, etc.). If the server rejects, you
get the rejection through `:result` and the overlay rolls back —
but the overlay won't *invent* a merged state.

## Quick start

```clojure
(require '[datahike.api :as d]
         '[datahike.optimistic :as opt]
         '[clojure.core.async :refer [<!]])

;; one-time wiring per conn
(opt/register! conn
  {:ttl-ms 30000                                  ;; default per-entry timeout
   :on-conflict (fn [conflicts]                   ;; see Conflicts below
                  (toast "your edit may not save"))})

;; subscribe — fires on every effective-db change with the new db
(opt/listen! conn ::ui
             (fn [eff-db] (rerender-ui! eff-db)))

;; or, for differential UI layers (posh-style materialized views,
;; redux-style reducers), subscribe to tx-reports instead
(opt/listen-tx! conn ::ui
                (fn [{:keys [db-before db-after tx-data origin ov-id]}]
                  (apply-deltas-to-ui-state! tx-data)))

;; submit an optimistic write
(let [{:keys [ov-id result]} (opt/transact! conn [{:entity/uuid (random-uuid)
                                                    :S.Page/title "Untitled"}])]
  ;; result is a 1-buffer channel yielding the server reply on success
  ;; or a Throwable / js/Error on failure (validation, server rejection,
  ;; TTL timeout, or unregister! while pending).
  (let [reply (<! result)]
    (when (instance? Throwable reply)
      (notify-user! "Save failed:" reply))))

;; teardown
(opt/unlisten!  conn ::ui)
(opt/unregister! conn)
```

## API reference

### `(register! conn opts?)` / `(unregister! conn)`

Attach / detach the overlay state. Idempotent. `opts`:

- `:ttl-ms` (default `30000`; `nil` to disable) — per-entry timeout.
  An entry whose dispatch hasn't resolved by then is dropped, with
  a `:result` of `(ex-info "Optimistic transaction timed out"
  {:type :optimistic/timeout})`.
- `:on-conflict` (function, optional) — `(fn [conflicts])`, fires
  when the set of conflicting entries changes. See
  [Conflicts (Case J)](#conflicts-case-j).

`unregister!` cancels in-flight entries with an
`:optimistic/cancelled` error on their `:result`.

### `(listen! conn k f)` / `(unlisten! conn k)`

Register `(fn [effective-db])` under key `k`. Fires whenever the
overlay or `@conn` changes. The argument is the *effective* db —
`@conn` with all applicable overlay entries layered on top.
Conflicting entries (see Case J) are excluded from the view.

### `(listen-tx! conn k f)` / `(unlisten-tx! conn k)`

Register `(fn [tx-report])` under key `k`. Fires once per logical
change with a Datahike-style tx-report carrying the delta a consumer
should apply to their derived view. See
[Tx-report events](#tx-report-events).

### `(on-conflict! conn k f)` / `(off-conflict! conn k)`

Additional conflict listeners beyond the one registerable at
`register!` time. Same signature, same semantics.

### `(transact! conn tx-data opts?)`

Submit an optimistic write. Returns `{:ov-id uuid :result <chan>}`.
Eagerly validates `tx-data` via `d/with` against the current
effective-db; on validation failure throws synchronously and the
overlay is untouched.

`opts`:

- `:dispatch-fn` — substitute the conn's default writer with your own
  RPC. See [The `:dispatch-fn` contract](#the-dispatch-fn-contract).
- `:ttl-ms` — override the conn's default TTL for this call;
  `nil` to disable.

### `(effective-db conn)` / `(pending conn)`

`effective-db` returns the current effective db value (overlay
applied on top of `@conn`) synchronously, useful outside listener
contexts. `pending` returns current overlay entries with internal
fields stripped — `:ov-id`, `:tx-data`, `:predicted-tx-data`,
`:submitted-at`, `:expires-at`, `:expected-max-tx`, `:conflicting?`,
`:last-conflict-error`.

### The `:dispatch-fn` contract

By default, `transact!` routes through the conn's writer (calls
`d/transact!` internally). For app-level RPCs or server-side
bootstrap (creating related entities, custom validation, etc.),
pass a `:dispatch-fn`:

- Takes no arguments.
- Returns a `core.async` channel that yields a single value: either
  `{:reply X :max-tx N}` on success — where `N` is the `:max-tx` of
  the durable commit your RPC produced — or a `Throwable` /
  `js/Error` on failure. A thrown exception during the call is also
  accepted; the wrapper normalizes throw and yield-an-error onto the
  same failure path.
- `X` is what gets put on `:result`.

```clojure
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
`core.async/ReadPort` (and puts the Throwable as a value on failure
— no re-throw), so `<!` works uniformly on both platforms.

For tx-report consumers (`listen-tx!`) the dispatch-fn affects
delta-event quality — see [v1 limits](#v1-limits).

## Tx-report events

`listen-tx!` callbacks receive a Datahike-style tx-report:

```clojure
{:db-before <effective-db at the start of the event>
 :db-after  <effective-db at the end>
 :tx-data   <vector of #datahike/Datom [e a v t added?]>
 :tempids   <map; present on :overlay-add and :conn-advance>
 :tx-meta   <map;  present on :overlay-add and :conn-advance>
 :origin    <see below>
 :ov-id     <uuid, present on entry-scoped origins>}
```

The core keys (`:db-before`, `:db-after`, `:tx-data`) match
`datahike.db/TxReport` exactly — the same destructure works on both
optimistic events and Datahike's native tx-reports. `:tempids` and
`:tx-meta` are passed through on the two origins that have a
real source for them (eager `d/with` for `:overlay-add`; the
writer's tx-report for `:conn-advance`).

### Event types (`:origin`)

| Origin              | When                                                                      | `:tx-data`                                                              |
|---------------------|---------------------------------------------------------------------------|-------------------------------------------------------------------------|
| `:overlay-add`      | `opt/transact!` accepted, entry added                                     | Predicted datoms from eager `d/with`                                    |
| `:conn-advance`     | Any `d/transact!` through this conn (own *or* a direct call by other code) | The durable writer's `:tx-data`                                         |
| `:overlay-realized` | Own entry's `:expected-max-tx` was reached; the entry was dropped         | Retracts of any *stale* predictions (EID-shift cleanup; empty when not needed) |
| `:overlay-conflict` | Entry became un-applicable on top of current `@conn` (Case J)             | Retracts of the entry's prediction                                      |
| `:overlay-resolve`  | Previously-conflicting entry became applicable again                      | Re-adds of the entry's prediction                                       |
| `:overlay-drop`     | Dispatch failed for a non-conflicting entry                               | Retracts of the entry's prediction                                      |
| `:ttl`              | Entry's TTL elapsed                                                       | Retracts of the entry's prediction                                      |

### Convergence guarantee

Apply each event's `:tx-data` in order to your derived view
(asserting added datoms, retracting non-added). **The result is
always equivalent to `:db-after`.** This is the property the
internals are designed to maintain — see
[Soundness](#soundness-ov-id-correlates-max-tx-watermarks).

## How it works

### The three identifiers

Each overlay entry has three distinct identifiers, each with a
single responsibility:

| Field               | Set when                                                                              | Used for                                                                                                  |
|---------------------|---------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `:ov-id` (uuid)     | At `opt/transact!` submit, client-minted                                              | **Correlation key** — identifies the entry across the whole lifecycle (overlay mutations, listener events, writer-tx cache lookup). |
| `:expected-max-tx`  | After dispatch resolves with `{:reply :max-tx N}`                                     | **Watermark** — "drop the entry when `(:max-tx @conn) >= N`."                                            |
| `:predicted-tx-data` | At submit, from eager `d/with`'s tx-report                                            | Datoms shipped on `:overlay-add`; the input to `retract-stale` on `:overlay-realized`.                    |

`:max-tx` is the watermark; `:ov-id` is the correlation key.
Mixing the two leads to subtle bugs under writer batching (see
[Correlating by ov-id](#correlating-by-ov-id-handles-batching)
below).

### Lifecycle of one optimistic write

```
USER
 │
 │  (opt/transact! conn [{:name "alice"}])
 │     1. eager d/with → predicted-tx-data
 │     2. append entry to overlay (with :ov-id, no :expected-max-tx yet)
 │     3. emit :overlay-add tx-report
 │     4. spawn dispatch go-block
 ▼
WRITER  (the conn's `d/transact!` flow — :self / :kabel / :datahike-server)
 │
 │  default-dispatch injects {::ov-id ov-id} into tx-meta and calls d/transact!
 │  → writer commits (possibly batching with other in-flight transacts)
 │  → reset! @conn  ────────────────────────────────────────────────────────┐
 │                                                                          │
 │                                                                          ▼
 │                                                                 ┌──────────────────┐
 │                                                                 │ add-watch fires  │
 │                                                                 │ (drop-caught-up: │
 │                                                                 │  expected-max-tx │
 │                                                                 │  still nil →     │
 │                                                                 │  no drop;        │
 │                                                                 │  fire-listeners) │
 │                                                                 └──────────────────┘
 │                                                                          │
 │  writer's go-block delivers tx-report to the callback ───────────────────┤
 │  → conn-listener fires (our writer-listener hook):                       │
 │      • caches writer's :tx-data keyed by ov-id (from :tx-meta)           │
 │      • emits :conn-advance tx-report                                     │
 │                                                                          ▼
 │                                                                ┌───────────────────┐
 │  promise resolves → default-dispatch's go-block reads result   │ dispatch go-block │
 │  → optimistic-transact's go-block resumes:                     │ resumes:          │
 │      • set :expected-max-tx                                    │   - dropping? T   │
 │      • dropping? = (:max-tx @conn) >= max-tx ⇒ true            │   - sync-drop     │
 │      • sync-drop entry                                         │   - retract-stale │
 │      • take writer-tx-cache by ov-id; compute retract-stale    │   - emit          │
 │      • emit :overlay-realized (empty when EIDs match)          │   - deliver result│
 │      • put reply on :result                                    └───────────────────┘
 ▼
:result chan yields the tx-report
```

For the typical happy path (default-dispatch + matching EIDs)
the events to a tx-report consumer are: `:overlay-add` →
`:conn-advance` → (no `:overlay-realized` — `retract-stale` returns
empty when prediction EIDs already match the writer's). For the
EID-shift case (tempids), a final `:overlay-realized` cleans up the
stale local-EID prediction.

### Soundness: `:ov-id` correlates, `:max-tx` watermarks

**Watermark.** When `(:max-tx @conn) >= M` and `M` is the `:max-tx`
the writer assigned to *our* commit, our datoms are necessarily in
`@conn`. This is **not** a property of `:max-tx` as a number — it's
a property of the snapshot family that `:max-tx` labels into. Three
structural facts of Datahike make it sound:

1. **Monotonic assignment.** The writer assigns `:max-tx` to each
   commit in strictly increasing order; no two commits share a value.
2. **Snapshot completeness.** Each `:db` value the writer publishes
   is a closed snapshot — the result of applying every commit up to
   and including its own `:max-tx`. The chain of `db_N` values is
   totally ordered, each a superset (in history) of all earlier ones.
3. **Atomic `@conn` update.** `on-db-sync!` (kabel) and the `:self`
   writer's commit loop both `reset!` the conn atom with a fully-
   materialized `:db` value. No window where `:max-tx` has advanced
   but the indexes haven't caught up.

So observing `(:max-tx @conn) >= M` means `@conn` *is* some `db_N`
with `N >= M`, fully materialized — and by snapshot completeness
that snapshot contains every commit ≤ M, including ours.

**Correlation.** Watermark alone doesn't say *which entry* belongs
to which writer tx-report — and that matters because the writer
can batch (see below). For correlation we use `:ov-id`, threaded
through `:tx-meta` on the durable transact.

### Correlating by `:ov-id` (handles batching)

Datahike's writer drains its commit-queue greedily — `writer.cljc`'s
commit-loop polls for queued transactions and commits them as one
batch. Inside the batch the writer overwrites each tx-report's
`:db-after` with the batch's commit-db before delivering to the
callback. So **N batched transactions report the same `:max-tx`** —
the watermark works for *all* of them simultaneously, but it can't
tell them apart.

If we keyed the writer-tx cache by `:max-tx`, the N writer-listener
calls would all write the same slot and overwrite each other; the
sync-drop paths would read the *last* tx-data for all N entries and
emit nonsense `retract-stale` events.

`default-dispatch` injects `{::ov-id ov-id}` into the `d/transact!`
arg-map's `:tx-meta`. The writer only augments `:tx-meta` (it adds
`:db/commitId`) — it preserves whatever else is there. Each batched
tx-report carries its own original `:ov-id`. The writer-listener
extracts it, caches by ov-id, and each entry's sync-drop reads its
own writer tx-data back. Cache collisions disappear.

Foreign `d/transact!`s on the same conn (no `::ov-id` tag) still
emit `:conn-advance` to the eff-db listeners — they just don't go
through the cache (there's nothing to correlate).

### Conflicts (Case J)

An overlay entry can become *un-applicable* on top of `@conn` — for
example, a concurrent peer or direct `d/transact!` commits a value
that violates a `:db.unique/value` constraint our entry was about to
claim. When this happens:

- The entry stays in the overlay (we don't yet know the server's
  verdict).
- It's **excluded** from `effective-db` — re-applying it would throw.
- It's **marked** `:conflicting? true` with `:last-conflict-error`.
- `:on-conflict` callbacks fire whenever the set of conflicting
  entries changes.
- A tx-report consumer gets an `:overlay-conflict` event with
  retracts of the entry's prediction; the consumer's incremental
  view excludes the conflicting entry.

The dispatch is still in flight. When it resolves:

- **Success** (the conflict was transient — concurrent peer
  retracted, etc.): the entry was fine after all. `@conn` advances
  with our datoms; the entry's watermark drops it.
  `retract-stale` cleans up any stale-EID prediction. A previously
  conflicting entry that becomes applicable again first emits
  `:overlay-resolve` (re-adding the prediction), then the
  normal `:overlay-realized` cleanup.
- **Failure** (the conflict was real): `:result` yields the error,
  the entry drops; the consumer's view already reflects the
  rollback via the earlier `:overlay-conflict` event.

### TTL

Each entry carries an `:expires-at` (default `now + 30000ms`). A
per-conn heartbeat ticks once a second and reaps expired entries:

- The entry's `:result` chan receives an `ex-info` tagged
  `{:type :optimistic/timeout}`.
- The entry is dropped from the overlay and a `:ttl` tx-report
  fires with retracts of the prediction.
- If the dispatch *does* eventually resolve after the TTL fired,
  the late reply is silently discarded — `:result` is exactly-once-
  delivery (`compare-and-set!` on a per-entry flag). The durable
  effect, if any, still surfaces via `@conn` advancing through
  normal sync (and a `:conn-advance` to tx-listeners).

Override per-conn at `register!` (`:ttl-ms 60000`, `:ttl-ms nil` to
disable) or per call (`(opt/transact! conn data {:ttl-ms 60000})`).

## Identity assumption

The overlay assumes entities are identified by a **stable attribute**
(e.g., `:entity/uuid` minted client-side), not by EID. Tempid
resolution may differ between the overlay's `d/with` and the durable
writer — local EID 42 might land at server EID 17. When that
happens:

- `:overlay-add` ships predicted datoms with local EIDs.
- `:conn-advance` ships writer datoms with server EIDs.
- `:overlay-realized` ships retracts of any predicted datoms whose
  `[e a v]` isn't in the writer's tx-data — cleaning up the stale
  local-EID version.

A `listen-tx!` consumer that keys by `:entity/uuid` sees this as
"same logical entity, EID changed." A consumer keyed by EID sees
two entities momentarily and then the local EID retracted. Use
uuid-keyed views and you never see the flicker.

For non-tempid transactions (using existing EIDs or upserting via
lookup-refs against pre-existing uuid entities), local and writer
EIDs match — `retract-stale` returns empty and no
`:overlay-realized` event fires.

## v1 limits

- **Cross-peer scenarios are unstudied.** Same-conn concurrency
  (concurrent `opt/transact!`s, interleaved direct `d/transact!`s,
  Case J unique-collisions, writer batching) is exercised by the
  smoke tests. *Multiple* peers writing to the same store over a
  real Kabel connection — where `@conn` may advance via
  `on-db-sync!` from a write none of your dispatches knows about —
  is structurally supported by the `:max-tx` watermark argument but
  hasn't been put on the bench yet.
- **`:conn-advance` only fires for writes routed through
  `d/transact!`.** The writer-listener hook is registered via
  `d/listen`, which only triggers when `d/transact!` completes (the
  writer walks `(:listeners (meta connection))` after each successful
  commit). Two scenarios advance `@conn` without going through that
  path, and tx-listeners miss the delta in both:
    - A custom `:dispatch-fn` that never calls `d/transact!` (e.g.,
      fires off an HTTP POST against a non-Datahike server and the
      durable state lives only on the remote side). The overlay's
      `:overlay-add` still fires, but if `@conn` never gets the
      durable change there's no `:conn-advance` to follow it.
      Mitigation: route the durable write through `d/transact!`
      somewhere — even just to mirror server state into the local
      conn.
    - A foreign Kabel-peer write that echoes in via `on-db-sync!`
      and `reset!`s the atom directly. `add-watch` fires (so eff-db
      `listen!` consumers see it), but `d/listen` does not.
      Mitigation: none in v1; an open follow-up would emit a
      `:conn-advance` from `on-db-sync!`.
- **Stale-prediction cleanup (`:overlay-realized`) only fires for
  default-dispatch and for custom dispatches that route through
  `d/transact!`.** A custom `:dispatch-fn` that bypasses `d/transact!`
  doesn't get the `::ov-id` round-trip through `:tx-meta`, so the
  writer-tx-cache has nothing to correlate against. The consumer's
  incremental view still rolls back via the watcher-drop path (with
  full-negate of the prediction) when `@conn` eventually catches up,
  but the EID-shift refinement is skipped.
- **Pending entries are not persisted.** Overlay state is in-memory;
  a page reload drops in-flight optimistic entries. Persistence
  (IndexedDB on browser, file on Node) is an open follow-up — see
  the Kabel writer queue durability work.
- **Eager validation throws synchronously**; server-side errors arrive
  on `:result`. Two failure paths, two mental models — programmer
  errors in tx-data are categorically different from runtime server
  rejection.
- **Listener fires twice per successful `transact!`** (once on
  overlay-add, once when `@conn` advances with the durable datoms).
  Both events show the correct effective db — no incorrect
  intermediate state — but it's redundant. Consumers can dedupe by
  comparing `:max-tx` if they care.

## See also

- [Distributed Architecture](./distributed.md) — Kabel writer, the
  most common reason to want this primitive.
- [ClojureScript Support](./cljs-support.md) — backends and async
  caveats for browser/Node.
