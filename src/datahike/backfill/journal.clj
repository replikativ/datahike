(ns ^:no-doc datahike.backfill.journal
  "Owned scratch journals for local index builds.

   Descriptors are opaque accepted-prefix handles, not paths, byte offsets,
   durable log addresses, or resumable build checkpoints. Keep them intact.
   Older accepted prefixes remain readable while the owner extends a lineage.
   Appending from an earlier prefix abandons its suffix: callers must first
   ensure that no accepted report or reader still needs that suffix. These
   journals are not branching immutable logs.

   Capture completes after transaction predicates accept and before the writer
   chains/enqueues the report. Only a successful append supplies its replacement
   descriptor. On failure the input prefix remains authoritative; unreachable
   scratch may remain until backend cleanup. Physical truncation is not part of
   this contract. One owner serializes append and disposal, and keeps resources
   alive until all reports/readers needing them have retired.

   This facade is the synchronous JVM adapter. Calls complete or throw; reducers
   are synchronous and honor reduced. Use a blocking-capable execution context.
   A future async adapter must make capture, replay and disposal awaitable at
   their callers, preserve this ordering, bound in-flight work, and coordinate
   shutdown with outstanding operations. Returning a channel from a backend
   alone would not satisfy the current synchronous caller contract. Pure with
   remains I/O-free; scratch is not a database durability source."
  (:require [datahike.backfill.journal.file :as file]))

(defn descriptor-id
  "Stable ownership identity for a descriptor's journal, independent of prefix.
   Consumers may compare this identity but must not interpret descriptor fields."
  [descriptor]
  (file/descriptor-id descriptor))

(defn validate-options!
  "Validate the selected JVM file backend's options without performing I/O.
   Directory and encoded-byte quotas are backend-specific configuration."
  [options]
  (file/validate-options! options))

(defn create!
  "Create owned scratch and return its empty prefix. Completion means the
   backend can accept appends, not that any database state has committed."
  [options]
  (file/create! options))

(defn append!
  "Capture notifications after the supplied prefix and return a new prefix on
   success. Failure does not accept any notification from this call. The owner
   must abandon any existing suffix before appending from an older prefix."
  [descriptor notifications]
  (file/append! descriptor notifications))

(defn reduce-journal
  "Reduce exactly the supplied prefix, releasing read resources on completion,
   reduced or exception. The caller controls retention of the accumulator;
   the file backend decodes one bounded frame at a time."
  [descriptor f init]
  (file/reduce-journal descriptor f init))

(defn start-cursor
  "Return an opaque cursor before the first notification of this journal.
   Cursors support value equality, not field access or numeric arithmetic."
  [descriptor]
  (file/start-cursor descriptor))

(defn end-cursor
  "Return an opaque cursor at this descriptor's accepted end. A cursor is
   valid only while its boundary remains in the owned, retained lineage."
  [descriptor]
  (file/end-cursor descriptor))

(defn compare-cursors
  "Compare two cursor positions within this descriptor's accepted prefix.
   Reject foreign, abandoned or out-of-prefix cursors. Returns negative, zero
   or positive; callers must not infer byte distances from the result."
  [descriptor left right]
  (file/compare-cursors descriptor left right))

(defn range-byte-size
  "Return backend-accounted bytes from left through right. Both cursors remain
   opaque; callers use this only to enforce a resource budget. Reversed,
   foreign, abandoned, or out-of-prefix ranges are rejected."
  [descriptor left right]
  (file/range-byte-size descriptor left right))

(defn reduce-range
  "Reduce from an issued cursor through this descriptor's exact accepted end.
   f receives accumulator, notification and an opaque next cursor. Honors
   reduced; the returned cursor can resume a later call while retained.
   The backend checks cursor identity and validity; callers still own build
   generations, leases and transaction grouping. Synchronous JVM operation."
  [descriptor start f init]
  (file/reduce-range descriptor start f init))

(defn dispose!
  "Release this owner's scratch once no accepted report or reader needs it.
   Idempotent after success; failure may require cleanup to be retried."
  [descriptor]
  (file/dispose! descriptor))
