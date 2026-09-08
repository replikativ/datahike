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

(defn dispose!
  "Release this owner's scratch once no accepted report or reader needs it.
   Idempotent after success; failure may require cleanup to be retried."
  [descriptor]
  (file/dispose! descriptor))
