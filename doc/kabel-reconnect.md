# Reconnecting an existing Kabel replica

Experimental: applications that own an authenticated transport can resubscribe
already-open replicas without replacing their connection or optimistic overlay.
This is separate from asynchronous query execution or on-demand index access.
The optional Kabel aliases use Kabel 0.3.141 or newer, whose owned-unsubscribe
guard is required to retire a strategy without cancelling another subscriber.

```clojure
(require '[datahike.kabel.connector :as kabel])

;; After transport authentication AND server-side authorization/store setup:
(let [result (<! (kabel/resubscribe! conn {:epoch authenticated-socket-id}))]
  (if (= :ready (:status result))
    ;; Existing connection/listeners now observe the confirmed server baseline.
    (show-connected!)
    ;; result is an exception. Keep drafts, show unsynchronized state.
    (show-sync-problem! result)))
```

Use a new epoch for each authenticated physical connection. Calls for one epoch
share a result channel, including a failure; a new explicit epoch may retry.
The default deadline is 120000 ms; `:handshake-timeout-ms` accepts a positive
32-bit integer. A deadline is a synchronization failure, not permission to retry
an uncertain remote transaction.

The subscription inventories only this branch's reachable local keys and asks
the server to confirm the mutable branch head. It does not expose that head
until the complete handshake has applied the missing immutable index nodes.
An absent branch head, refusal, release or timeout is not successful catch-up.
The last known DB remains available on error; the application must not label it
current. No `max-tx` comparison substitutes for authoritative head identity.

Resubscription preserves the exact connection, writer, prepared store, index
registry and listeners. A frontend-only tiered store continues to synchronize
its frontend, not its shared read-only backend. Existing optimistic predictions
stay owned by their overlay and reconcile through its ordinary base-change and
acknowledgement machinery. Reconnect does not wipe IndexedDB or dispatch writes.

The connection owns its physical strategy. A replacement strategy owned by
another subscriber is refused rather than silently unsubscribed. Superseded
callbacks cannot publish a new baseline. If construction of the old subscription
is still running at its deadline, callers receive a timeout but its physical
gate stays held until construction finishes: a late topic-only request must not
interleave with a replacement subscription.

Writer shutdown fences publication synchronously and returns a channel joining
subscription cleanup. In-flight and successful closes share one result; a failed
physical drain can be explicitly retried. Only the attached controller owns a
physical strategy: a manually constructed writer does not unsubscribe whatever
currently occupies its topic. Neither writer shutdown nor failed construction
unregisters PSS reconstruction storage by UUID, because branches share that
registration. As with generic release, the bounded entry remains until reconnect
overwrites it; exact last-owner reclamation is a separate concern.

Failed construction fences initial callbacks and joins its captured strategy's
guarded drain **before** releasing the store. `cleanup-confirmed?` reports whether
its local exception positively confirms completed cleanup. On uncertainty,
`recover-connect!` retries only the exact captured cleanup, never construction or
writes. Failed-connect exceptions can carry native retry functions: keep them
local, do not serialize them or treat them as transport data. Unknown failures
do not prove clean completion. Callers must serialize constructors and retirement
for each physical local store; this API does not make overlapping constructors
safe. Initial connection no longer steals an existing topic as a recovery step.

Existing generic `d/release` does **not** await asynchronous
writer shutdown before closing a store. Callers needing a physical drain barrier
must join `datahike.writer/shutdown` before release; this experimental API does
not repair that separate generic release seam or provide arbitrary-backend
store-close completion. Late publication also uses a
compare-and-set guard so it cannot resurrect a released connection.

A different principal or peer is a new session, not a resubscription. Prepare
only authorized already-open stores after reconnect; do not eagerly open every
accessible database. Transport authentication alone is not replica readiness.
