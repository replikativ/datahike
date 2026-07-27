# Backup & restore

`datahike.migrate/export-db` and `import-db` write and read a portable, type-exact
dump of a database. Use them for backups, moving a database between stores, or
snapshotting before a risky change. For the format details and rationale see
[import-export-design.md](import-export-design.md).

## Export

```clojure
(require '[datahike.migrate :as m])

;; snapshot of the current value (no history)
(m/export-db conn "/backups/mydb")

;; full history — every assertion, retraction, and tx entity
(m/export-db conn "/backups/mydb" {:history? true})
```

A directory target writes the **chunked** format (`manifest.edn` +
`datoms-NNNNNN.edn`); a plain-file target writes the **flat** format. The manifest
is written last and is the commit marker — a dump directory without a
`manifest.edn` is incomplete. Export holds an immutable db value, so it is
consistent even under concurrent writes.

> **Do not run datahike GC during a long export** on a lazy-loading backend. GC
> marks reachability from branch heads only and does not pin a reader-held
> snapshot, so it can sweep index segments the in-flight export still needs.

## Restore

```clojure
(def report (m/import-db fresh-conn "/backups/mydb"))
;; => {:datom-count .. :tx-count .. :max-tx .. :verified? true :finalized? true :errors []}
```

Restore into a **freshly created, empty** database whose config is compatible with
the dump's `:source-config` (the manifest records it). Import:

- runs through `load-entities`, which **remaps** entity/tx ids — a restored
  database is *semantically equivalent*, never id-identical;
- **refuses a non-empty target** (`:import/non-empty-target`). Import is **not
  resumable** (the id-remap is in-memory only); if an import is interrupted,
  delete the target and start over;
- verifies itself against the manifest by default (`:verify? true`) and clears its
  bookkeeping on success (`:finalize? true`).

Options: `:batch-size`, `:verify?`, `:finalize?`, `:on-error :abort|:collect`,
`:progress-fn`. Failures are `ex-info` with a namespaced `:error` (e.g.
`:import/config-mismatch`, `:import/checksum-failed`, `:import/bad-chunk-path`).

Old flat **CBOR** dumps (produced by pre-1.0 datahike) still import through the
legacy path automatically.

## Data protection (PII / right-to-erasure)

**A `:history? true` export resurrects retracted data.** Retraction is not erasure
in an immutable database, so every value ever asserted — including "deleted"
personal data — is written into the dump. If a dump may leave your trust boundary,
export with `:history? false`. The manifest records `:history?` so a receiving
system can tell which kind it holds. Datahike has no history-excision primitive.

## Integrity & signing

Each chunk carries a SHA-256 in the manifest; `import-db`/`verify` recompute them
before touching the database. Exports are deterministic (identical source db ⇒
byte-identical chunks), so a dump can be signed by external tooling (GPG, cosign,
KMS). The manifest's semantic digest is for order-independent content comparison,
not tamper-evidence.
