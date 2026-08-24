# Garbage Collection

Datahike uses persistent data structures that enable structural sharing—each update creates a new version efficiently by reusing unchanged parts. This allows [time-travel queries](./time_variance.md) and [git-like versioning](./versioning.md), but storage grows over time as old snapshots accumulate.

**Garbage collection removes old database snapshots from storage while preserving current branch heads.**

## GC and purging together

Garbage collection and data purging are different operations:

- **Garbage Collection** (this document) reclaims storage by deleting old database *snapshots* that no branch head still points to. Routine maintenance; doesn't touch data on a live snapshot.
- **[Data Purging](./time_variance.md#data-purging)** rewrites the indices to remove specific *data*. Used for privacy compliance (GDPR, HIPAA, CCPA).

The two compose for actual erasure:

1. `purge` produces a **new commit** whose indices no longer reach the targeted datoms.
2. The **pre-purge commit** is now a live intermediate commit and still references the old nodes in storage. Until GC sweeps it, `(d/commit-as-db conn <pre-purge-uuid>)` still sees the purged data.
3. `d/gc-storage` **with a grace-period cutoff** old enough to drop the pre-purge commit physically evicts those nodes from konserve.

So the erasure recipe is **purge + cutoff-GC**, not purge alone. Plain `d/gc-storage` (no cutoff) only reclaims storage from deleted branches and leaves intermediate commits intact — useful as routine maintenance, but not as the eviction step for erasure.

For multi-branch databases, purge on every branch that holds the datom; for how secondary indices participate, see [Secondary indices: purge propagation](./secondary-indices.md#purge-propagation).

## How Garbage Collection Works

GC whitelists all current branches and marks snapshots as reachable based on a grace period. Snapshots older than the grace period are deleted from storage, but **branch heads are always retained** regardless of age.

## Basic Usage

```clojure
(require '[datahike.api :as d]
         '[superv.async :refer [<?? S]])

;; Remove only deleted branches, keep all snapshots
(<?? S (d/gc-storage conn))
;; => #{...} ; set of deleted storage blobs
```

Running without a date removes **only deleted branches**—all snapshots on active branches are preserved. This is safe to run anytime and reclaims storage from old experimental branches.

**Note:** Returns a `core.async` channel. Use `<??` to block, or run without it for background execution. GC never blocks transactions or reads.

## Where to run GC

**Run GC where the writers are.** A commit writes every value the new head references and only *then* flips the head — so for the duration of that sequence those objects exist in the store and nothing yet names them. A collector in the same process knows a commit is in flight and leaves them alone (`datahike.gc-guard`). A collector in another process cannot know: its view of "what is in flight" is empty because *its* heap is idle, not because the store is quiet.

With a single **exclusive** writer (`:writer {:backend :self :writer-ownership :exclusive}`) that is the whole story: `d/gc-storage` is a writer operation, so it already runs in the right place.

With **shared** writers (`:writer-ownership :shared`, the default) several processes may commit to the same branch. Head fencing keeps them from losing each other's commits, but a fence protects the *pointer*, not the *values*: a commit in flight in another process is invisible to the collector, and its objects are on disk reachable from nothing. The same is true of a cron sidecar collecting a store it never writes. In both cases the only protection is the sweep floor:

```clojure
(d/gc-storage conn (java.util.Date. 0) {:min-age-ms (* 15 60 1000)})
```

`:min-age-ms` spares anything written more recently than that, whatever the mark says. Size it above the longest window between "first value written" and "head flipped" any of your writers can have — a writer that awaits its transacts has that window closed when the call returns, so one request's duration is the bound. The price of a generous value is only delayed reclamation; the price of a small one is a dangling head.

## Grace Periods for Distributed Readers

Datahike's [Distributed Index Space](./distributed.md) allows readers to access storage directly without coordination. This is powerful for scalability but means **long-running processes might read from old snapshots for hours**.

Examples of long-running readers:
- **Reporting jobs**: Generate daily/weekly reports by querying yesterday's snapshot
- **Analytics pipelines**: Process historical data over several hours
- **Monitoring dashboards**: Display metrics from recent snapshots
- **Backup processes**: Copy database state while it's being updated

**The grace period ensures these readers don't encounter missing data.** Snapshots created after the grace period date are kept; older ones are deleted.

```clojure
(require '[datahike.api :as d])

;; Keep last 7 days of snapshots
(let [seven-days-ago (java.util.Date. (- (System/currentTimeMillis)
                                         (* 7 24 60 60 1000)))]
  (<?? S (d/gc-storage conn seven-days-ago)))

;; Keep last 30 days (common for compliance)
(let [thirty-days-ago (java.util.Date. (- (System/currentTimeMillis)
                                          (* 30 24 60 60 1000)))]
  (<?? S (d/gc-storage conn thirty-days-ago)))

;; Keep last 24 hours (for fast-moving data)
(let [yesterday (java.util.Date. (- (System/currentTimeMillis)
                                    (* 24 60 60 1000)))]
  (<?? S (d/gc-storage conn yesterday)))
```

**Choosing a grace period:**
- Consider your longest-running reader process
- Add buffer time for safety (if longest job is 2 hours, use 4-6 hours)
- Balance storage costs against reader safety
- Monitor reader patterns before shortening grace periods

**Branch heads are always kept** regardless of the grace period—only intermediate snapshots are removed.

## Online Garbage Collection (Incremental GC)

> ⚠️ **EXPERIMENTAL FEATURE**

Online GC automatically deletes freed index nodes during transaction commits, preventing garbage accumulation during bulk imports and high-write workloads.

> Online GC is currently an experimental feature. While it has been tested extensively in Clojure/JVM and includes safety mechanisms for multi-branch databases, use with caution in production. We recommend:
> - Thorough testing in your specific use case before production deployment
> - Monitoring freed address counts to verify expected behavior
> - Using it primarily for bulk imports and high-write workloads where it's most beneficial
> - **ClojureScript**: Online GC functionality is available in CLJS but has not been tested in big bulk loads yet. JVM testing is more comprehensive.
> - Reporting any issues at https://github.com/replikativ/datahike/issues

### How Online GC Works

> Online GC has **two hard requirements**: a single branch, and `:diff-buf-size 0`.
> Both are enforced — the diff-buf combination is refused at connect, and both are
> skipped inside the GC itself. Use offline GC (`d/gc-storage`) otherwise; it derives
> reachability itself instead of trusting the freed-address hint, so it is unaffected.
>
> Note the multi-branch restriction is **not** because structural sharing is confined to
> branches — it is not. Nodes are shared between any two versions, including two versions
> of the same branch. See *Why diff-buf is excluded* below.

When PSS (Persistent Sorted Set) index trees are modified during transactions, old index nodes become unreachable. Online GC tracks these freed addresses with timestamps and deletes them incrementally:

1. **During transaction** (transient mode): PSS calls `markFreed()` for each replaced index node
2. **At commit time**: Freed addresses older than the grace period are batch-deleted
3. **Multi-branch safety check**: If multiple branches detected, GC is skipped entirely
4. **No full tree walk**: Only freed addresses are deleted, not requiring expensive tree traversal

**Key benefits:**
- **Prevents unbounded storage growth** during bulk imports (single-branch only)
- **Incremental deletion**: Small batches per commit, low overhead
- **Grace period support**: Safe for concurrent readers accessing old snapshots
- **Multi-branch safety**: Automatically disabled to prevent corruption
- **Configurable**: Can be disabled, tuned, or run in background

### Configuration

Enable online GC in your database config:

```clojure
;; For bulk imports (no concurrent readers, single-branch)
;; See "Address Recycling" section below for details
{:online-gc {:enabled? true
             :grace-period-ms 0          ;; Recycle immediately
             :max-batch 10000}           ;; Large batches for efficiency
 :crypto-hash? false}                   ;; Required for address recycling

;; For production (concurrent readers)
{:online-gc {:enabled? true
             :grace-period-ms 300000     ;; 5 minutes
             :max-batch 1000}}           ;; Smaller batches

;; Disabled (default)
{:online-gc {:enabled? false}}
```

**Configuration options:**

- `:enabled?` - Enable/disable online GC (default: `false`)
- `:grace-period-ms` - Minimum age in milliseconds before deletion (default: `60000` = 1 minute)
- `:max-batch` - Maximum addresses to delete per commit (default: `1000`)
- `:sync?` - Synchronous deletion (always `false` inside commits for async operation)

### Background GC Mode

For production systems, run GC in a background thread instead of blocking commits:

```clojure
(require '[datahike.online-gc :as online-gc])

;; Start background GC
(def stop-ch (online-gc/start-background-gc!
               (:store @conn)
               {:grace-period-ms 60000    ;; 1 minute
                :interval-ms 10000        ;; Run every 10 seconds
                :max-batch 1000}))

;; Later, stop background GC
(clojure.core.async/close! stop-ch)
```

**Background mode advantages:**
- Non-blocking: Doesn't slow down commits
- Periodic cleanup: Runs every N milliseconds
- Graceful shutdown: Close channel to stop

### Address Recycling (Bulk Import Optimization)

> ⚠️ **EXPERIMENTAL FEATURE**
>
> Address recycling is an experimental optimization. It has been designed with safety checks (multi-branch detection, grace periods), but should be thoroughly tested in your environment before production use.

Online GC includes **address recycling**—freed addresses are reused for new index nodes instead of being deleted from storage. This optimization is particularly powerful for bulk imports.

**How it works:**
1. When index trees are modified, old root addresses are marked as freed
2. Online GC moves eligible addresses to a freelist (grace period applies)
3. New index nodes reuse addresses from the freelist instead of generating new UUIDs
4. LMDB overwrites the recycled address with new data

**Benefits:**
- **Zero delete operations**: Converts O(freed_nodes) deletes to O(1) freelist append
- **Reduces LMDB fragmentation**: Addresses are reused rather than accumulating
- **Perfect for bulk imports**: With `:grace-period-ms 0`, recycling happens immediately
- **Minimal overhead**: No tree traversal or complex reachability analysis

**Safety limitations:**

**Address recycling is ONLY safe for:**
- **Single-branch databases** (shared nodes across branches would be corrupted)
- **No long-lived readers** (or grace period exceeds reader lifetime)
- **Bulk import scenarios** (write-only, no concurrent queries)

**Online GC is automatically disabled when:**
- `:diff-buf-size` is non-zero (refused at connect; also skipped with a warning inside the
  GC if reached via `:allow-unsafe-config` or a direct `online-gc!` call).
  Reason: see *Why diff-buf is excluded* below
- Multiple branches exist (online GC completely skipped - use offline GC instead)
  Reason: reachability across branches is not established by the freed-address stream
- Using `:crypto-hash? true` with recycling (falls back to deletion mode)

### Why diff-buf is excluded

Online GC reclaims blobs from the `markFreed` stream that persistent-sorted-set emits.
That stream is documented by pss as a **hint, not a reachability claim** — the consumer
must establish for itself that no live version needs an address.

Without diff-buf the hint is reliable in practice: a changed child is always rewritten to a
new address, so the old one belonged solely to the version being superseded.

With diff-buf it is not. A parent no longer says *"child i is the blob at A"*; it says
*"child i is the blob at anchor A **plus** this diff"*. Two versions can therefore name the
same anchor with different diffs, and neither owns it. Storing one of them may **flush**
that child — write it out whole and free the anchor — which is correct for the version
being stored and wrong for the other. The useful distinction is that freeing on
**supersession** is safe while freeing on **re-representation** is not, and a flush is
re-representation: the same elements, written differently.

So the stream is sound exactly when the commit history is **linear** — every version stored
before the next is derived from it. Measured in pss against a backend that acts on the
callback:

| shape | result |
|---|---|
| linear | publication closure held in 432/432 cells, every budget |
| ancestor then descendant (descendant derived while ancestor unstored) | 25 read failures / 768 trials at budget ≤ 4; clean at ≥ 8 |
| `:diff-buf-size 0` | 864/864 clean, no premature free in any shape |

Two mitigations that do **not** work, so they are not attempted: retaining "the last N
images" fails for every N, because the freed blob is not reachable from the immediately
preceding image and the required depth grows without bound; and limiting the number of
branches does not help, because the hazardous shape lives inside a single lineage
(`force-branch!` twice on one branch reproduces it while `(count branches)` stays 1).

Datahike enforces the simpler, mechanically checkable rule — `:diff-buf-size 0` — rather
than asking users to reason about linearity. That is also strictly stronger: every free
that could be premature happens under diff-buf, so excluding it closes the class.

Tracked in [datahike#951](https://github.com/replikativ/datahike/issues/951) — see the issue
for the underlying design question of whether incremental reclamation should derive its
free set from reachability instead of a producer hint. pss's own statement of the contract
is in `IStorage.markFreed`.

### Bulk Import Configuration

For maximum performance during bulk imports where no concurrent readers exist:

```clojure
;; Optimal bulk import configuration
{:online-gc {:enabled? true
             :grace-period-ms 0        ;; Recycle immediately (no readers)
             :max-batch 10000}         ;; Large batch (only for delete fallback)
 :crypto-hash? false                  ;; Required for recycling
 :branch :db}                         ;; Single branch only

;; Example bulk import
(let [cfg {:store {:backend :file :path "/data/bulk-import"}
           :online-gc {:enabled? true :grace-period-ms 0}
           :crypto-hash? false}
      conn (d/connect cfg)]
  ;; Import millions of entities
  (doseq [batch entity-batches]
    (d/transact conn batch))
  ;; Storage stays bounded - addresses are recycled
  (d/release conn))
```

**Bulk import best practices:**
1. Set `:grace-period-ms 0` (no concurrent readers to protect)
2. Use `:crypto-hash? false` (enables address recycling)
3. Stay on single branch (`:branch :db`)
4. Increase `:max-batch` for efficiency (only affects delete fallback)
5. Monitor freed address counts to verify recycling is working

**Verifying address recycling:**
- Check logs for `"Online GC: recycling N addresses to freelist"`
- If you see `"Online GC: skipped (multi-branch detected)"`, ensure single branch
  (multi-branch databases require offline GC instead)
- Freed address counts should drop to zero after each transaction

### Online GC vs Offline GC

**Online GC** (incremental):
- Runs during commits
- Deletes only **freed index nodes** from recent transactions
- Fast: No tree traversal required
- **With recycling**: No delete operations at all, just freelist management
- **ONLY for single-branch databases** - automatically disabled for multi-branch
- Best for: Bulk imports, high-write workloads

**Offline GC** (`d/gc-storage`):
- Runs manually
- Deletes **entire old snapshots** by walking all branches
- Slower: Full tree traversal and marking
- Handles **multi-branch databases** safely through reachability analysis
- **Required for multi-branch databases** and for any database with a non-zero
  `:diff-buf-size` (online GC doesn't work in either case)
- Best for: Periodic maintenance, deleting old branches, multi-branch cleanup

**Use both:** Online GC for incremental cleanup during single-branch writes, offline GC for periodic deep cleaning and all multi-branch scenarios.

## Automatic Garbage Collection

With online GC enabled, garbage collection becomes largely automatic during normal operation. Manual `d/gc-storage` runs are only needed for:
- Deleting old branches
- Periodic deep cleaning (monthly/quarterly)
- Compliance-driven snapshot removal

## When to Run GC

- **After deleting branches**: Immediately reclaim storage
- **Periodic maintenance**: Weekly/monthly based on storage growth
- **Storage alerts**: When approaching capacity limits
- **Version cleanup**: After completing long-running migrations

## What Gets Deleted

GC removes:
- Old database snapshots older than the grace period
- Deleted branches and their snapshots
- Unreachable index nodes from old snapshots

GC preserves:
- All current branch heads (always)
- Snapshots created after the grace period
- All data on retained snapshots (GC doesn't delete data, only snapshots)

**Remember:** Actual erasure (GDPR / HIPAA / CCPA) requires [purging](./time_variance.md#data-purging) followed by a cutoff `d/gc-storage` sweep. Purge alone leaves the pre-purge commit reachable; GC alone doesn't delete data on a live snapshot.
