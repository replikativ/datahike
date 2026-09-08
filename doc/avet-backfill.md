# Background attribute indexing

On the JVM, Datahike can build an attribute's AVET index in the background and
enable it in a separate atomic commit. The same operation can add a uniqueness
constraint. Reads and writes continue under the old schema while the build runs.
This API is experimental.

## Choose a migration mode

Use an ordinary schema transaction when the caller needs the migration to
finish before proceeding. It remains synchronous: successful completion means
the schema change and its required index construction and validation committed
together. The transaction-local `:allow-index-backfill?` option permits that
work; it does not start a background job. This path can hold up other writes
and can require substantial memory for large attributes.

Choose `begin-avet-build!` explicitly for online maintenance when writes need
to continue during construction. It is not the default and does not replace
ordinary schema transactions. Its promise acknowledges the request, not a
completed migration. The caller must monitor that generation to a terminal
result before relying on the new index or uniqueness constraint. Background
construction still consumes CPU, I/O, and scratch space and can affect write
latency or reject writes when journal limits are reached.

## Deployment requirements

**Upgrade every garbage collector sharing the store before using this API.**
Older Datahike versions do not recognize the AVET build marker and can delete
unpublished index nodes even while the source pin remains live. Alternatively,
keep garbage collection disabled in older processes until all builds and their
cleanup have finished. Fencing does not make mixed-version collection safe.

Use a local shared writer with an explicit fencing requirement, the
persistent-set index, and non-cryptographic index addresses:

```clojure
{:index :datahike.index/persistent-set
 :crypto-hash? false
 :writer {:backend :self
          :writer-ownership :shared
          :require-fencing :process}}
```

The store must support the requested fencing domain. Select a domain that covers
your deployment; `:process` is not a cross-process guarantee. Diff buffers,
exclusive writers, remote writers, and ClojureScript builds are not supported.
Existing databases must already use the supported index configuration; changing
configuration is not an index migration.

## Start, inspect, and cancel a build

The request maps existing attribute idents to the properties to enable:

```clojure
(require '[datahike.api :as d])

(def accepted
  @(d/begin-avet-build! conn
     {:customer/email {:db/unique :db.unique/value}
      :customer/joined-at {:db/index true}}))

(def generation (:avet-build-id accepted))

;; Read a fresh snapshot when checking progress.
(d/avet-build-status @conn)
;; => {:id ..., :status :building, :patch {...}}
;; Later: {:id ..., :status :ready}

;; Cancel only this generation, not a newer replacement.
@(d/cancel-avet-build! conn generation)
```

The start promise resolves when the request is committed, **not when the index
is ready**. The cancellation promise resolves when cancellation is committed;
worker cleanup may finish afterward. A completed or replaced generation cannot
be canceled using its old ID.

One build may be active per branch. A request can enable `:db/index true` and
`:db/unique :db.unique/value` or `:db.unique/identity` on up to 256 existing
attributes. It cannot create attributes, disable properties, change types or
cardinality, or include arbitrary transaction data. Uniqueness implies indexing.

`avet-build-status` returns the active request or the most recent result:
`:building`, `:ready`, `:failed`, or `:canceled`. It returns nil before the first
request. Results include their generation ID; failed or canceled results also
include a reason. This is snapshot state, not a worker-liveness check or a
complete job history. Keep the request report's `:avet-build-id` when monitoring
a request: reports in one batch share its final `:db-after`, which may already
describe a later generation.

Treat only `:ready` for the saved generation as successful completion.
`:failed` and `:canceled` are unsuccessful outcomes, not completed migrations.
A different generation's status does not establish the outcome of your
request; retain its observed result before starting another build if that
outcome must be recorded. A monitoring timeout does not cancel the build.
Cancellation must be requested explicitly and can race with activation;
inspect a fresh snapshot to determine which operation committed.

Ordinary `listen` callbacks receive the public start and cancel reports, not the
worker's internal activation report. Use `listen-commits` for commit notifications
or inspect fresh snapshots for readiness, matching the saved generation ID.

## Activation and uniqueness

The build includes current data, retained history, and changes committed while
it runs. The schema and completed AVET roots become visible together. No reader
sees an enabled attribute backed by a partially built index.

A uniqueness build checks the existing values outside the serialized writer,
then checks affected values after each complete replayed transaction. Activation
also validates values changed by the schema transaction, including derived
tuples. It does not skip uniqueness validation. Ordinary per-datom uniqueness
checks still apply; intermediate conflicts in derived tuples can reject an
activation even if its proposed final values would be distinct.

Duplicates encountered during the build fail the request without enabling the
constraint or undoing user transactions. Repair the data and submit a new
request. Until activation commits, the old schema still permits duplicates.

## Resource limits

The builder uses disk-backed sorting and bounded private node buffers. It does
not retain the full change journal or all values being checked for uniqueness
in heap. The final writer step admits only a bounded journal tail; a larger tail
is sent back to the worker for another catch-up pass.

Configure limits under the writer's `:avet-backfill` key:

```clojure
:avet-backfill
{:runtime {:journal {:directory "/path/to/scratch"
                     :max-frame-bytes 1048576
                     :max-bytes 268435456}
           :max-contexts 8
           :max-readers 8}
 :build {:sort {:directory "/path/to/scratch"
                :window-bytes 16777216
                :window-records 65536
                :max-record-bytes 1048576
                :fan-in 16
                :max-bytes 4294967296}
         :storage {:pending-node-limit 64
                   :pending-weight-limit 16777216
                   :cache-node-limit 64
                   :cache-weight-limit 16777216}}
 :tail-bytes 1048576
 :tail-transactions 128
 :max-jobs 8}
```

The shown values are defaults except for the directories, which default to the
JVM temporary directory. A configured directory must already exist. Limits are
per owned runtime or build, not a global memory or disk budget. Weight limits
are conservative accounting units, not exact retained heap measurements.
Backend node decoding, the index builder's frontier, the database's ordinary
caches, and user transaction inputs remain outside these buffer budgets.
Large values or nodes can exceed an individual-record or node limit.

A journal limit or append failure rejects the affected user transaction without
advancing its database state. Cancel the build or wait for it to finish before
retrying. Sort or private-storage failures fail the background request. Very
high write rates can delay activation; bounded catch-up does not guarantee a
completion deadline or a fixed write-pause duration.

If build ownership changes between predicate validation and journal staging,
the in-flight transaction is rejected with `:retryable? true` rather than
publishing a state its predicates did not validate. Retry against a fresh head.

## Recovery and garbage collection

Schema changes invalidate an active build. A different writer taking over its
branch cannot continue the original writer's local journal: its next accepted
write retires that generation. The write itself can still commit normally.
After a restart, submit a fresh request or cancel the durable old generation;
scratch is not a resumable checkpoint. Transaction predicates also apply to
build requests, activation, and cancellation.

The source snapshot is pinned while the build runs. Active durable build markers
defer garbage collection's sweep so that unpublished index nodes are not
reclaimed. Lost pins prevent activation. A marker left after a crash can keep
sweeping deferred until a successful cancellation, replacement, or ordinary
write retires it.

All processes collecting this store must support these build markers. A live
source pin alone does not protect unpublished candidate nodes from an older
collector, and checking that pin cannot detect such deletion.

Normal completion, cancellation, and orderly writer shutdown clean up owned
scratch. A process crash can leave files behind. Use a dedicated scratch
directory and remove abandoned files only after confirming their writer has
stopped. Reclamation of unused immutable store nodes remains the collector's job.
