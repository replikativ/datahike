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
;; For bulk imports (no concurrent readers)
{:online-gc {:enabled? true
             :grace-period-ms 0          ;; Delete immediately
             :max-batch 10000}}          ;; Large batches for efficiency

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

### Online GC vs Offline GC

**Online GC** (incremental):
- Runs during commits
- Deletes only **freed index nodes** from recent transactions
- Fast: No tree traversal required
- Best for: Bulk imports, high-write workloads

**Offline GC** (`d/gc-storage`):
- Runs manually
- Deletes **entire old snapshots** by walking all branches
- Slower: Full tree traversal and marking
- Best for: Periodic maintenance, deleting old branches

**Use both:** Online GC for incremental cleanup, offline GC for periodic deep cleaning.

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
