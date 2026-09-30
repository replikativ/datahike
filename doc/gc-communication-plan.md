# Shared history article and BranchBench outreach drafts

These are working drafts accompanying the [GC audit](gc-memory-model.md), not published copy. The site destination is datahike.io after technical review and reproducible measurements. The outreach draft has not been sent.

## Article draft

### Collecting shared history without deleting live branches

A branch can share most of its storage with the database it came from. That makes experiments cheaper to create, but it changes what cleanup must prove. Removing one experiment does not make every node it touched disposable. Another branch, a historical snapshot, or a long-running reader may still need those nodes.

Datahike stores immutable database values in persistent trees. Updating a tree creates new nodes along the changed paths while preserving unchanged nodes. Its columnar, text and vector integrations add another layer: a database commit names the exact secondary generation needed to answer queries against that version. Cleanup must preserve this complete graph, rather than just the latest primary index.

There are also several kinds of cleanup. The JVM can collect an unreachable database object in heap. A cache can evict a restored tree node. A Lucene reader or mmap view can release local resources. Durable garbage collection decides whether stored bytes can disappear permanently. These operations have different owners and lifetimes. A database value kept in heap is not automatically a durable pin: lazy traversal may still need nodes that have never been loaded.

The ordinary durable collector marks objects reachable from retained roots and sweeps old objects outside that set. Datahike's roots include branches, retained commits, durable pins and checkpoints, explicit retained keys, and integrated secondary generations. Stratum and Scriptum generations participate in the primary-store mark. Proximum generations live in an external store; Datahike can discover the retained generation IDs, but that discovery alone does not authorize deleting the rest of the external store.

Writers introduce a timing problem. They write values before publishing the pointer that makes those values reachable. Within a process, a guard protects this interval. The collector reads the guard before marking and uses a cutoff that spares protected writes. Shared and remote writers also receive an age allowance. That allowance helps with ordinary timing and clock differences; it does not provide a distributed snapshot of all roots.

Consider a collector that marks the store and then freezes. Another process publishes a branch pointing at an old generation that the mark did not include. When the collector resumes, that generation is both old enough to delete and absent from its remembered live set. Protecting newly written objects by age does not protect these old objects. A branch-head compare-and-swap prevents a stale writer from overwriting a newer head, but it does not prevent a stale collector from deleting payloads.

This is why the collection contract must say which processes can publish roots while deletion runs. A local exclusive mode can serialize the operations under one owner. A distributed mode needs durable coordination that remains enforceable after suspension or restart. A renewable lease is not sufficient if an expired collector can still send unconditional delete requests. The same contract must cover branch creation, reader pins, imports, backfills and external secondary publication.

Correctness also shapes the optimization. Shared immutable trees are promising candidates for remembered traversal work. Within one collection, expanding the same shared subtree once can avoid repeated work across roots. Across collections, a persistent index can remember complete edges from an immutable node to its children and payloads. The next pass can reuse those edges instead of fetching and decoding the same node again.

Remembered edges and current liveness are different facts. A node that was live yesterday may be dead today. Skipping a previously seen parent must not forget descendants that are still live. The index therefore needs complete entries, explicit collection epochs, recovery rules and protection against recycled addresses. A missing entry requires recomputation. A failed traversal must stop deletion.

The storage backend changes the cost of sweeping. A remote object store may require paginated listing, metadata reads and one delete request per object. Batched deletion can reduce requests, while bounded enumeration can reduce collector memory. Neither optimization fixes stale authority. We need to measure marking and sweeping separately, including requests, bytes, memory and foreground latency.

BranchBench provides useful workloads for the next stage of this work. Our current Clojure adaptation covers sequential failure reproduction and several evaluation queries. It does not yet measure the deep, concurrent branching needed to evaluate the complete collection design. We plan to add those workloads with explicit retention and GC policies, so branch latency is reported alongside the later cost of preserving and reclaiming shared history.

The current implementation and proposed changes should remain visibly separate in the published article. Link the mode table, deterministic race tests and raw benchmark artifacts as they become available. Until then, this explanation supports the architecture and its present boundaries, not a performance comparison or a claim that distributed collection is solved.

Sources for review: [Datahike collection contract](gc.md), [secondary storage ownership](secondary-indices.md), [issue #960](https://github.com/replikativ/datahike/issues/960), [issue #951](https://github.com/replikativ/datahike/issues/951), [Clojure harness](https://github.com/replikativ/branchbench-clj), and [BranchBench paper](https://arxiv.org/abs/2604.17180).

## Outreach draft

Suggested recipient: Elaine Ang, with the other authors included according to their preferred contact channel. Confirm the channel before sending. The initial message can precede new measurements because it asks about methodology and collaboration, rather than claiming results.

Subject: Datahike backend and GC measurements for BranchBench

Hi Elaine and BranchBench team,

We have been exploring BranchBench with Datahike, an immutable database with persistent, structurally shared indices. We found your `db-fork` framework useful and built a Clojure adaptation at https://github.com/replikativ/branchbench-clj.

The adaptation currently covers sequential, depth-1 failure reproduction and four evaluation queries. It is not yet a complete implementation of your five workflows. We have also compared against Dolt locally, but are revisiting reproducibility and configuration equivalence before sharing performance claims.

We would like to extend the work to deep and concurrent branching, and explicitly measure retention and garbage collection: which snapshots remain queryable, how shared nodes are reclaimed, collector cost, and its effect on foreground latency. Datahike's columnar, text and vector integrations make secondary-generation retention another part of that workload.

Would you be interested in a Datahike backend contribution? We would appreciate guidance on retention expectations after branch deletion, how GC/background work should be accounted for, and acceptable adaptations for an embedded Datalog engine alongside the SQL backends. We can share exact revisions, commands and raw artifacts as we complete the expanded runs.

Best,
Christian

## Publication evidence to collect

- Freeze database, harness and dependency versions; preserve existing local benchmark edits separately.
- Record dataset generation, schema/index parity, hardware, durability, caches, warmup, concurrency, branch shape and retention policy.
- Validate answers before timing, including aggregates with duplicate-sensitive semantics.
- Report mark expansions and IO separately from sweep listing, metadata reads and deletion requests.
- Measure heap, RSS, mmap/cache files and durable storage independently.
- Provide commands and raw results; distinguish our Dolt comparison from the paper's DoltgreSQL baseline.
- Review the article against the deployed mode contract and link unresolved limitations.
