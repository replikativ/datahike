# Optimistic overlays (`datahike.optimistic`)

> **Status: Beta.** The lifecycle and event contracts are ready for application
> integration, but the public API may still evolve before it is declared stable.

An optimistic overlay is an explicit, closeable view over one Datahike
connection. It owns an ordered set of pending transactions and exposes the
database obtained by replaying those transactions over the latest durable
connection value.

The overlay is designed for user interfaces that need immediate local
visibility without confusing local prediction, remote acceptance, and durable
replica synchronization.

## Quick start

```clojure
(require '[clojure.core.async :refer [<! go]])
(require '[datahike.optimistic :as opt])

(def overlay (opt/open conn))

(opt/listen! overlay ::render
  (fn [{:keys [db-after]}]
    (render! db-after)))

(go
  (let [{:keys [result]}
        (opt/transact! overlay [{:item/id id :item/title "New title"}])
        reply (<! result)]
    (case (:status reply)
      :committed (println "committed")
      :rejected  (report! (:error reply)))))

;; Component/application teardown:
(opt/close! overlay)
```

`opt/db` is an O(1) read of the last committed effective snapshot. Do not read
`@conn` when rendering optimistic state.

## Ownership and serialization

`open` installs one watch on the supplied connection. Every state-affecting
input—submission, connection replacement, writer completion, prediction
acknowledgment/rejection, timeout, listener registration, and close—is ordered
through one portable CAS queue and one inline drainer.

Redundant connection wake-ups and timeout ticks are coalesced while queued, so
a slow listener cannot create an unbounded internal-event backlog.

Consequences of this model:

- revisions are strictly increasing within an overlay;
- an event's `:db-before` is the preceding event's `:db-after`;
- listener callbacks run in order and never concurrently;
- submitting or changing listeners from a listener is safe and non-blocking;
- a slow listener delays later transitions;
- synchronous validation is intentionally not part of `transact!`—validation
  failures arrive as tagged results;
- pending transaction data must be deterministic and safe to evaluate with
  `d/with`. Base changes replay pending entries, so transaction functions must
  not perform side effects.

The default limits are 1,024 visible entries and 1,024 queued user submissions.
Configure them with `:max-pending` and `:max-queue` in `open`.

## Snapshot-transition events

`listen!` receives maps of this shape:

```clojure
{:revision    7
 :db-before   before
 :db-after    after
 :base-max-tx 536870920
 :cause       {:type :overlay-added ...}
 :changes     {:added [...] :removed [...]}}
```

These are **not Datahike `TxReport`s**. `:db-after` is authoritative.

Whenever `:changes` is non-nil, it is the exact current-EAV membership
difference between the two snapshots. For overlay-only transitions it is
computed from snapshot membership,
using the old and newly replayed transaction datoms only to bound the candidate
keys. It is therefore safe for cardinality-one replacements, removal of an
early entry followed by replay of later entries, and bases that moved under a
prediction.

`nil` always means “invalidate incremental state and re-read `:db-after`.” It is
used for connection replacement and as a safety fallback if an index cannot
serve the bounded membership lookup. A Kabel/store-sync update
does not necessarily carry an honest aggregate transaction delta. The event still
represents the entire base replacement once, including a batched writer reset.

Typical `:cause :type` values are `:overlay-added`, `:overlay-removed`,
`:base-advanced`, and `:overlay-closed`. Cause metadata is diagnostic; consumers
should drive their view from the snapshots and revision.

```clojure
(opt/listen! overlay key callback)
(opt/unlisten! overlay key)
```

Listener callbacks run inside the overlay's serialized event turn. Lifecycle
calls made reentrantly from a callback are queued and take effect after the
current notification returns; an `opt/db` read in that same callback still sees
the snapshot being notified.

## Writer-backed transactions

```clojure
(def handle (opt/transact! overlay tx-data))
;; => {:ov-id uuid :result promise-channel}
```

The transaction becomes visible before durable dispatch begins. Its result is
one tagged map and the promise channel then closes:

- `{:status :committed :tx-report report}`—the writer committed the operation;
- `{:status :rejected :error error}`—local validation or the writer rejected it;
- `{:status :unknown :reason :overlay-closed}`—the overlay closed while dispatch
  was still unresolved.

There is no visibility TTL for writer-owned work. A slow writer cannot make a
write disappear and later commit behind the user's back. Once the writer report
provides a `:max-tx` watermark, the entry stays visible until the connection base
reaches that watermark.

Writer-backed transactions that create entities must include stable identity
attributes and use lookup refs for later references. An anonymous tempid has no
identity that can correlate the optimistic entity with the durable echo; replay
may therefore show both allocations briefly. This is a protocol constraint, not
something an overlay can infer safely.

## Externally owned predictions

Use `predict!` when another RPC or subsystem owns the durable operation:

```clojure
(let [{:keys [ov-id result]}
      (opt/predict!
       overlay
       [[:db/add [:item/id id] :item/title title]]
       (fn [durable-db]
         (= title (:item/title (d/entity durable-db [:item/id id])))))]
  (go
    (try
      (let [receipt (<! (send-rename-rpc! id title))]
        (opt/ack! overlay ov-id receipt))
      (catch #?(:clj Throwable :cljs :default) error
        (opt/reject! overlay ov-id error)))))
```

The required reconciliation predicate must be deterministic, side-effect-free,
and synchronous. It runs against new durable base snapshots. Lifecycle commands:

- `ack!` means the external owner accepted the operation. It resolves the
  result as `{:status :accepted :receipt ...}`, disables pre-acceptance expiry,
  and keeps the prediction visible until the base matches.
- before acceptance, `reject!` immediately retracts the prediction and resolves
  it as rejected. A late `reject!` after `ack!` is ignored;
- `abandon!` explicitly retracts work the owner no longer wants to reconcile.
- a matching base retracts only the overlay layer and emits status
  `:reconciled`; the effective view does not flicker.

An unacknowledged prediction expires after `:prediction-timeout-ms` (30 seconds
by default), resolves as `{:status :expired :outcome :unknown}`, and is
retracted. Set the timeout to `nil` globally or per prediction to disable it.

Acknowledged work is never retracted by a timer. If the base has not caught up
after `:reconciliation-timeout-ms` (30 seconds by default), the overlay emits a
single `:reconciliation-stalled` status and keeps the entry. This reports a sync
problem without falsely converting an accepted operation into a failure.

## Status stream and inspection

```clojure
(opt/listen-status! overlay ::telemetry callback)
(opt/unlisten-status! overlay ::telemetry)
(opt/pending overlay)
(opt/error? reply)
```

Status events carry `:revision`, `:ov-id`, `:kind`, and `:status`, plus relevant
fields such as `:error`, `:receipt`, or `:max-tx`. Statuses include `:visible`,
`:acknowledged`, `:committed`, `:reconciled`, `:rejected`, `:expired`,
`:abandoned`, `:conflicting`, `:applicable`, `:reconciliation-error`, and
`:reconciliation-stalled`. Closing can additionally emit `:unknown` or
`:detached` when durable work can no longer be observed by the overlay.

`pending` returns sanitized entry metadata. Transaction data, callbacks,
channels, reconciliation predicates, and replay internals are not exposed.

## Conflicts

New entries are applied once to the current effective snapshot. Base changes
and entry removal replay the remaining pending entries in order. If `d/with`
rejects an entry during replay, that entry remains pending but contributes no
datoms. Later entries are replayed against the last applicable snapshot. The
status stream emits `:conflicting` when an entry first becomes inapplicable and
`:applicable` if a later base transition resolves it.

## Closing

`close!` removes the connection watch, stops the heartbeat, emits one final
snapshot transition when pending state is retracted, settles unresolved result
channels, rejects submissions ordered after close, clears listeners, and is
idempotent. It does **not** claim to cancel a durable writer operation already
in flight.

Always close overlays before releasing their connections.

## JavaScript and TypeScript

The JavaScript binding uses an explicit surface because overlay reads are
synchronous while operation results are asynchronous:

```typescript
const overlay = d.openOptimistic(conn);
const unsubscribe = d.optimisticListen(overlay, event => render(event['db-after']));

const { ovId, result } = d.optimisticTransact(overlay, txData);
const outcome = await result; // tagged result; rejection is not a thrown Promise

unsubscribe();
d.closeOptimistic(overlay);
```

Writer and prediction submissions return `{ovId, result: Promise}`. Rejected
operations resolve to a tagged `':rejected'` result rather than rejecting the
Promise, preserving the distinction from transport-level Promise failures.
`optimisticPredict` reconciliation callbacks must return a boolean
synchronously. The generated `index.d.ts` declares the complete surface.

## Current boundary

The optional `:branch` submit key is reserved and appears in pending metadata,
but this overlay does not route writes to a Datahike branch. Applications must
keep branch-specific writes on their branch-aware durable path.

Persisting optimistic entries across process or page reload is deliberately out
of scope. That requires an idempotency and replay protocol, especially for
non-idempotent transaction forms such as `:db/retractEntity`.
