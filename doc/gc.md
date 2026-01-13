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

## Automatic Garbage Collection

**Coming soon:** Datahike will support automatic GC with configurable grace periods, eliminating manual maintenance.

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
