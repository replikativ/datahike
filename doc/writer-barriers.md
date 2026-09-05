# Writer barriers

`writer-barrier!` waits for preceding accepted transaction work on a writer
and returns an immutable, durable database value. On the JVM, `writer-barrier`
blocks for the same result. ClojureScript uses the asynchronous channel API;
JavaScript exposes it as `await writerBarrier(conn)`.

For an accepted queue `A, B, barrier, C`, A and B settle before the barrier
returns. Shared-writer head conflicts must finish retrying before C can cross
this boundary. Settlement does not mean every transaction succeeded: callers
must still inspect each transaction's result. Fatal writer failures fail pending
barriers rather than leaving callers waiting indefinitely.

The barrier creates no transaction and invokes no transaction predicates or
commit listeners. It neither locks the branch nor promises that another writer
has not advanced it. Later commits cannot change the returned snapshot. It does
not join independent background jobs merely because they were launched earlier.

Do not block on a barrier inside a transaction function or durable commit
listener: these execute inside the writer whose progress you would be waiting
for. Queue an asynchronous barrier and consume its result outside that callback.

## Kabel replicas

Remote completion and local replication are separate steps. A Kabel barrier
waits for synchronization of the exact returned commit ID. It remembers commits
synchronized while the RPC is in flight, even if the local head advances again
before the response arrives. A larger `max-tx` is not evidence that this snapshot
was synchronized: divergent histories may reuse transaction numbers.

If the requested commit is never observed as synchronized, the barrier fails
with `:kabel/sync-timeout` after the synchronization timeout. This includes a
replication path that skips that head entirely; it does not infer availability
from a later head or silently substitute a different snapshot. Fetching and
verifying a skipped snapshot's complete reachable storage is not implemented by
this API. Ordinary transaction replication waiters retain their existing
`max-tx` behavior; this change hardens barriers, not that separate protocol.
