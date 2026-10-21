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
currently occupies its topic. Each constructor owns a reconstruction registration
lease, rather than unregistering by UUID. Retirement restores the newest remaining
live sibling registration; the last lease removes its entry. Explicit unmanaged
`register-store!` registrations still require explicit `unregister-store!`, and
are preserved even when re-registering the same store object. Direct PSS registry
mutation is outside this ownership protocol: a different replacement is preserved,
but same-object re-registration must use the Datahike API to convey new ownership.

Failed construction fences initial callbacks and joins its captured strategy's
guarded drain **before** releasing the store. `cleanup-confirmed?` reports whether
its local exception positively confirms completed cleanup. On uncertainty,
`recover-connect!` retries only the exact captured cleanup, never construction or
writes. Failed-connect exceptions can carry native retry functions: keep them
local, do not serialize them or treat them as transport data. Unknown failures
do not prove clean completion. Callers must serialize constructors and retirement
for each physical local store; this API does not make overlapping constructors
safe. Initial connection no longer steals an existing topic as a recovery step.

For Kabel connections, `d/release` synchronously marks the connection released
and starts its connection-owned retirement: fence publication, join the exact
subscription drain, release its reconstruction lease, then close the native store.
Deletion/invalidation invokes the same captured owner. The public release call
still returns nil. Before reusing the local store, join the complete lifecycle:

```clojure
(d/release conn)
(let [result (<! (kabel/await-release! conn))]
  ;; Only literal true confirms that native resources are retired.
  ...)
```

Pending and successful retirement share one result. An explicit later
`await-release!` retries unfinished phases after failure, without replaying
construction or writes. Failed drain retains the registration and native handle;
failed native close does not drain an already retired subscription again. These
capabilities remain in connection metadata after invalidation. This ordering is
specific to owned Kabel replicas, not a new arbitrary-backend close guarantee.
Late publication also uses a compare-and-set guard so it cannot resurrect a
released connection.

A different principal or peer is a new session, not a resubscription. Prepare
only authorized already-open stores after reconnect; do not eagerly open every
accessible database. Transport authentication alone is not replica readiness.
