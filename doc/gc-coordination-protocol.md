# Durable collection and remembered reachability (draft)

Status: protocol proposal and executable schedule model, 2026-09-30. This PR
does not enable `:mode :coordinated`, change publication, or persist mark data.
The mode remains rejected. The [ownership audit](gc-memory-model.md) describes
the current implementation; [#960](https://github.com/replikativ/datahike/issues/960)
tracks the old-object promotion hazard. The model tests a small abstract store,
not backend failure behavior or production integration.

## Safety boundary

A collection may delete an object only when no accepted root names it. Roots
include branch heads, retained history, durable pins/checkpoints, current and
temporal primary indices, embedded secondary generations, and store-ref payloads.
The collector owns embedded Stratum and Scriptum objects in its Konserve store.
An external Proximum generation is a retained root for a separate owner; discovering
it does not authorize Datahike to sweep that owner's store.

The bounded collector's final root read and age cutoff do not enforce this rule
for old objects promoted after marking. Head CAS protects head updates, not
payload deletion. A distributed lease that expires while an unconditional delete
is suspended also fails: its former holder can resume after a publisher proceeds.

## Conservative first protocol

Use one durable, non-expiring CAS gate per store incarnation. The gate stores
`{:format 1 :incarnation id :epoch n :holder nil-or-token :operation kind}`.
Only the exact current owner can release it. Gate acquisition, release, and epoch
advance must use backend conditional writes; unknown or insufficient fencing
domains refuse coordinated mode. A process-local CAS backend can only promise
coordination within that process. A globally fenced backend can support the
distributed protocol once every publisher participates.

1. A publisher acquires the gate before creating private objects or selecting an
   old snapshot for promotion. It holds the gate through durable pointer publication.
   Timed-out writes, especially head/registry CAS, must settle before release:
   a pointer request with an unknown outcome might still land after collection
   begins. Cancellation does not establish that a dispatched request has stopped.
2. Under the gate, the publisher validates the complete proposed closure, including
   store-ref payloads and adapter-owned generation requirements. All required
   objects must exist or be supplied durably in the same operation. Inlined roots
   are validated from the record, rather than required as separate store objects.
   Existing pins/heads are re-read under the gate. Missing objects reject publication.
3. A collector acquires the same gate before reading roots, marks all retained
   closures, then holds it until every dispatched deletion has returned. Batch
   failure, cancellation and unknown outcomes must drain outstanding work before
   release. A crashed collector leaves the gate held.
4. The collector keeps the gate and its incarnation metadata reachable. A publisher
   cannot remove or arbitrarily rewrite these coordination objects; only
   protocol-authorized CAS acquisition, release and epoch transitions may update
   the gate. Store deletion/recreation
   is an administrative operation with a new incarnation and separately fenced
   participants, not a normal publication.

Step 2 is necessary even with a perfect mutex: a collector can delete an unpinned
old snapshot, release the gate, and then admit a publisher that still holds that
snapshot in memory. Serializing those operations does not resurrect its objects.

A stopped owner does not lose ownership with time. Crash recovery requires proof
that its process and all outstanding backend requests can no longer execute.
An operator terminating a process may still have requests in flight; process death
alone is insufficient proof. Without backend fencing of the deletes themselves,
uncertain request completion means the gate remains held. There is no automatic
TTL takeover, `force-unlock`, or token check immediately before an unconditional
delete. These restrictions trade availability for safety explicitly.

The executable model explores publisher/collector interleavings and demonstrates
two counterexamples: expiring ownership while a collector is frozen, and serial
publication without closure validation. The non-expiring protocol preserves root
closure in the modeled schedules; it deliberately cannot make progress after a
crashed owner. This is evidence about the state machine, not proof about S3 or
all implementation paths.

## Publication integration required before enabling the mode

| Path | Required participation |
| --- | --- |
| `writing/commit!` and initial database creation | Gate covers node/schema/secondary writes through head and registry publication; validate rebased proposal before publishing |
| `versioning/branch!`, forced head replacement, branch registry changes | Gate covers source validation and pointer publication; an old snapshot must still be complete |
| `gc-roots/root!`, `set-record!`, renew/release/reap | Gate covers registry and record changes, including checkpoint rewrites; avoid nested gate acquisition |
| Import and migration | Gate covers destination publication; staged private data has explicit ownership until promotion |
| Secondary backfill | Retain base snapshot and private-generation ownership across checkpoints; do not hold the global gate for an hours-long build |
| Embedded adapters | Host owns publication and sweep; adapters cannot independently sweep the shared store |
| External Proximum | Separate generation-owner barrier and acknowledgment of the exact retained root set before its collector sweeps |
| Raw Konserve callers and earlier Datahike versions | Outside the protocol; coordinated activation requires quiescence and upgraded participating writers |

Activate only after quiescing existing publishers and installing the incarnation
and gate. Every connection must discover the active protocol and either participate
or refuse writes. Caching an absent gate permanently is unsafe during activation.
Mixed old/new writers cannot be silently supported. Publication validation should
use an acyclic shared graph module rather than making gc-roots depend on gc.
Current code has not yet implemented these integration points.

## Persistent remembered nodes

Keep *immutable adjacency* separate from a cycle's *live mark set*. A persistent
sorted set can index entries by `[incarnation owner codec-version address digest]`,
with complete child addresses and payload dependencies. An address alone is not
a valid key when online GC or diff buffering permits recycling or several logical
views at one durable anchor. Such indices use their own versioned identity or
fall back to the full walk. Root fusion is an explicit representation case.

Start every cycle with an empty live set. For each current root, read or rebuild
its adjacency entry, add that node to the cycle's live set, and visit every child.
A cache hit avoids fetching/decoding an immutable node; it does not treat the
previous cycle's live set as current liveness. Store-ref values and secondary
dependencies must be re-evaluated or represented in complete versioned adjacency.
The executable graph model checks changed roots, shared descendants and a changed
store incarnation against an uncached full-walk oracle.

Persist only completed entries. Write immutable index nodes first and publish a
versioned manifest with CAS after those nodes are durable. A manifest contains
incarnation, owner/codec versions, entry count and integrity information. A failed
walk cannot publish an entry. Missing, truncated, malformed, mismatched or
unverified entries are misses and trigger a full walk; they never authorize a
partial whitelist. A corrupted cache can delete live data if accepted as complete,
so integrity and structural validation are part of the recovery contract.

The cache's objects belong to a separate derived-metadata owner. Its active
   manifest and reachable PSS nodes are protected during host collection; obsolete
generations are collectible only after the new manifest is durable and retained.
Do not keep every cache generation alive in the host mark set. Bound entries by
an explicit byte/node budget, evict stale entries, and allow rebuilding the entire
index. Failure or eviction costs traversal work, never database liveness.

Do not reuse a complete cached closure after its objects might have been deleted
and later promoted without validation. A cache is not a replacement for the
publication protocol. User-visible activation must follow barrier integration.

## Acceptance checks and measurement

Before enabling coordinated mode, test independent clients/processes with frozen
collectors, owner death, unknown deletion completion, rejected takeover, old root
promotion after collection, checkpoint replacement, late pins, retained history,
store-ref payloads and adapter generations. Add fault injection at every durable
gate/manifest write. Run these against each supported fencing domain, including
S3/MinIO, not only the abstract model. Verify mixed-version activation refuses.

Before enabling persistent reuse, compare every result with a full-walk oracle on
shared histories and changing roots. Test missing/corrupt cache nodes, torn manifest
publication, incarnation changes, address recycling, diff buffering and root fusion.
Measure node fetches/decodes, cache bytes and writes, request counts, peak retained
metadata and elapsed time with recorded commands and raw results. Model cache-hit
counts demonstrate the mechanism; they do not establish production speedups.

Batch sweeps reduce requests independently of this protocol. They preserve the
same ownership requirement and must report partial failures honestly. No backend
request optimization should implicitly enable a stronger collection mode.
