# Garbage Collection

Datahike uses persistent data structures that enable structural sharing—each update creates a new version efficiently by reusing unchanged parts. This allows [time-travel queries](./time_variance.md) and [git-like versioning](./versioning.md), but storage grows over time as old snapshots accumulate.

**Garbage collection removes old database snapshots from storage while preserving current branch heads.**

## GC vs Purging

Don't confuse garbage collection with data purging:

- **Garbage Collection** (this document): Removes old database *snapshots* to reclaim storage. Used for routine storage maintenance.
- **[Data Purging](./time_variance.md#data-purging)**: Permanently deletes specific *data* for privacy compliance (GDPR, HIPAA, CCPA). Used only when legally required.

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

**Note:** Returns a `core.async` channel. Use `<??` to block, or run without it for background execution. GC requires no coordination and won't slow down transactions or reads.

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

> ⚠️ **EXPERIMENTAL FEATURE** (Datahike 0.8.0+)
>
> Online GC is currently an experimental feature. While it has been tested extensively in Clojure/JVM and includes safety mechanisms for multi-branch databases, use with caution in production. We recommend:
> - Thorough testing in your specific use case before production deployment
> - Monitoring freed address counts to verify expected behavior
> - Using it primarily for bulk imports and high-write workloads where it's most beneficial
> - **ClojureScript**: Online GC functionality is available in CLJS but has limited test coverage due to async complexity. JVM testing is more comprehensive.
> - Reporting any issues at https://github.com/replikativ/datahike/issues

**Available starting with Datahike 0.8.0**: Online GC automatically deletes freed index nodes during transaction commits, preventing garbage accumulation during bulk imports and high-write workloads.

### How Online GC Works

When PSS (Persistent Sorted Set) index trees are modified during transactions, old index nodes become unreachable. Online GC tracks these freed addresses with timestamps and deletes them incrementally:

1. **During transaction** (transient mode): PSS calls `markFreed()` for each replaced index node
2. **At commit time**: Freed addresses older than the grace period are batch-deleted
3. **No full tree walk**: Only freed addresses are deleted, not requiring expensive tree traversal

**Key benefits:**
- **Prevents unbounded storage growth** during bulk imports
- **Incremental deletion**: Small batches per commit, low overhead
- **Grace period support**: Safe for concurrent readers accessing old snapshots
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

### Freed Address Counts

Understanding how many addresses are freed helps detect memory issues:

**Pattern:** `3 + 2n` addresses for n data transactions
- **Schema transaction**: 3 addresses (EAVT, AEVT, AVET index roots)
- **Data transaction**: 2 addresses per transaction (EAVT, AEVT roots)
  - AVET only changes for **indexed** attributes (`:db/index true`)
  - Non-indexed attributes don't update AVET, so only 2 freed per tx

**Examples:**
- Schema + 1 data tx: 3 + 2 = 5 freed addresses
- Schema + 10 data txs: 3 + 20 = 23 freed addresses
- Schema + 1000 data txs: 3 + 2000 = 2003 freed addresses

**Why this matters:** If freed counts deviate from this pattern, it may indicate:
- Double-freeing (too many addresses)
- Missing frees (too few addresses)
- Index structure changes
- Memory leaks

### Address Recycling (Bulk Import Optimization)

> ⚠️ **EXPERIMENTAL FEATURE**
>
> Address recycling is an experimental optimization. It has been designed with safety checks (multi-branch detection, grace periods), but should be thoroughly tested in your environment before production use.

Starting with Datahike 0.8.0, online GC includes **address recycling**—freed addresses are reused for new index nodes instead of being deleted from storage. This optimization is particularly powerful for bulk imports.

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

⚠️ **Address recycling is ONLY safe for:**
- **Single-branch databases** (shared nodes across branches would be corrupted)
- **No long-lived readers** (or grace period exceeds reader lifetime)
- **Bulk import scenarios** (write-only, no concurrent queries)

⚠️ **Address recycling is automatically disabled when:**
- Multiple branches exist (falls back to deletion mode)
- Using `:crypto-hash? true` (content-addressing requires fresh UUIDs)

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
- If you see `"multi-branch detected, using deletion mode"`, ensure single branch
- Freed address counts should drop to zero after each transaction

### Online GC vs Offline GC

**Online GC** (incremental):
- Runs during commits
- Deletes only **freed index nodes** from recent transactions
- Fast: No tree traversal required
- **With recycling**: No delete operations at all, just freelist management
- Best for: Bulk imports, high-write workloads, single-branch databases

**Offline GC** (`d/gc-storage`):
- Runs manually
- Deletes **entire old snapshots** by walking all branches
- Slower: Full tree traversal and marking
- Handles **multi-branch databases** safely through reachability analysis
- Best for: Periodic maintenance, deleting old branches, multi-branch cleanup

**Use both:** Online GC for incremental cleanup during writes, offline GC for periodic deep cleaning and multi-branch scenarios.

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

**Remember:** For deleting specific data (GDPR compliance), use [data purging](./time_variance.md#data-purging), not garbage collection.
