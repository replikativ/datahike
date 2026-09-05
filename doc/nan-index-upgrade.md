# Upgrading databases containing NaN

> Design draft for the planned 1.0 storage upgrade. The comparator correction
> and safety tests are preserved here for review; the format marker and recovery
> procedure below are proposals, not an approved release or migration policy.
> Coordinate them with any entity-ID/transaction-ID range changes and decide
> whether the unified migration must preserve branches and commit history.

The persistent-set index now orders scalar floating-point NaNs after all other
numbers. NaNs compare equal to one another. The previous numeric comparator
treated NaN as equal to every number, which could suppress writes or leave
persisted index entries in an order incompatible with the corrected comparator.
NaN inside a tuple has the same problem. Primitive float/double arrays use a
separate comparator and are unaffected by this change.

Stop all writers and restart the application when upgrading. Do not hot-reload
this comparator into a process with open databases, and do not run old and new
writers against the same store during migration.

New persistent-set records carry `:pss-comparator-version 1`. Before exposing a
legacy record without this field, Datahike scans all six physical index roots,
including history. A record containing scalar NaN fails with
`:index/comparator-migration-required`. Unknown version markers fail with
`:index/unsupported-comparator-version`. Reads never modify a legacy record;
the next normal commit stamps an unaffected database. Historical commits and
branches are checked independently when loaded. Large legacy databases therefore
incur a full scan on each materialization until a new commit records the format.

For an affected database:

1. Keep a backup of the original store. Using the **old runtime**, export each
   branch/snapshot that needs to survive with
   `(datahike.migrate/export-db db dump-path {:history? true})`.
2. Under the **new runtime**, create a separate, empty database with compatible
   schema/history settings and import the dump:
   `(datahike.migrate/import-db conn dump-path
     {:build-indexes? true :eids :preserve})`.
3. Check the import report's `:verified?`, live and historical datom counts, and
   application queries. Reopen the new database and check indexed NaN lookups
   before redirecting application traffic.

Bulk index import requires persistent-set indexes, no attribute references, and
no secondary-index schema. For configurations outside those requirements, use
the regular import path with `{:eids :preserve}` and verify the result. Import
creates a new commit history; it does not preserve the old branch graph or commit
IDs. Index rebuilding cannot recover writes that the old comparator omitted.

The Hitchhiker Tree backend uses its own persisted key comparator and does not
receive a persistent-set format marker. Its comparison semantics and migration
requirements are separate from this persistent-set upgrade check.
