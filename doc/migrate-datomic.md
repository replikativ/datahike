# Migrating between Datomic and datahike

`datahike.migrate.datomic` moves a database **in either direction** between
Datomic Pro and datahike, over the same record seam dumps use (see
[Backup & restore](./backup.md)). *Experimental.*

```clojure
(require '[datahike.migrate.datomic :as dtm])

(dtm/import-from-datomic! dh-conn datomic-conn)   ; Datomic  -> datahike
(dtm/export-to-datomic!   @dh-conn datomic-conn)  ; datahike -> Datomic
```

## Getting the namespace

datahike takes **no Datomic dependency**. The namespace requires `datomic.api`,
so it loads only when you put the peer jar on your own classpath:

```clojure
{:deps {io.replikativ/datahike {:mvn/version "…"}
        com.datomic/peer       {:mvn/version "1.0.7622"}}}
```

Nothing else in datahike requires it, so if you never add the dependency you
never pay for it.

## Scope: Pro, via the peer API

The peer API (`datomic.api`) is **Pro only**. Datomic Cloud and Datomic Local
speak the *client* API (`datomic.client.api`), which has no `d/log` — and the
transaction log is what the source reads to reproduce history. Supporting them
means a different namespace and a different strategy for history, not a
different `require`.

`datomic:mem://` works, which is what the test suite uses: no licence key, no
transactor, no container.

## What survives, and what cannot

**Values, history and transaction times survive. Entity and transaction ids do
not.** That is not a shortcut; it is arithmetic.

| | Datomic | datahike |
|---|---|---|
| user entity id | ~1.76e13 | `emax` = 2 147 483 647 |
| transaction entity id | ~1.3e13 | `txmax` = 2 147 483 647 |

Datomic's ids are four orders of magnitude above what datahike can represent, so
`{:eids :preserve}` is **refused** rather than silently downgraded — datahike
does not range-check an incoming eid, it simply reallocates, so passing them
through would look like it had worked. `:eids` still accepts a map or a function
if you know how the two id spaces should relate.

In the other direction Datomic refuses explicit entity ids outright
(`:db.error/invalid-entity-id`), so a datahike → Datomic export always
reallocates too.

datahike also assigns its own `t`: a stream is renumbered sequentially in source
order. What is preserved is the transaction **order** and every
`:db/txInstant`, which is what history actually depends on.

## Provenance: which transaction was which

Because the ids do not survive, the correspondence is recorded as data. Each
imported transaction carries two datoms **on the transaction entity**:

| attribute | value |
|---|---|
| `:datomic/t` | the source `t` (e.g. `1004`) |
| `:datomic/tx-eid` | the source transaction entity id |

So the question is a query rather than arithmetic:

```clojure
(d/q '[:find ?tx :in $ ?t :where [?tx :datomic/t ?t]] @conn 1004)
;=> #{[536870917]}
```

Pass `{:provenance? false}` to leave them out.

## Round trips

Both directions are covered by tests that compare against the original rather
than merely checking the result is non-empty.

**Datomic → datahike → Datomic** is datom-for-datom identical to the source,
with exactly one difference: one extra transaction, and its timestamp. Datomic
will not *use* an attribute in the transaction that *installs* it, while
datahike will, so a source transaction carrying both becomes two on the way
back. The schema half is stamped one millisecond before the source instant, so
the two still ascend; its content and ordering are exact, its installation time
is approximate.

**datahike → Datomic → datahike** preserves current values and history.

## Limits worth knowing before you start

**Export into a *fresh* Datomic database.** `:db/txInstant` must be monotonic and
at or after the database basis. A fresh database's basis instant is
`1970-01-01`, so real historical times replay from the first transaction onward.
Appending into a populated database whose basis is newer than your oldest record
cannot work, and Datomic says so with `:db.error/past-tx-instant`.

**Re-asserting a `:db.unique/identity` value upserts.** Exporting into a
non-empty Datomic database merges onto existing entities rather than
duplicating, which may or may not be what you want.

**datahike-only schema is stripped.** Datomic answers `:db/maxLength`,
`:db.valid/from`, `:db.valid/to`, `:db.secondary/*` and datahike's extra value
types with `:db.error/not-an-entity`, which would fail the transaction.
`{:strip-datahike-schema? false}` keeps them and lets Datomic refuse. Unknown
*application* attributes are fine either way.

**Datomic-only vocabulary is dropped**, with a warning: transaction functions
(`:db/fn`, `:db.fn/cas`), `:db/fulltext`, `:db/lang`, and the excision
vocabulary have no datahike equivalent. `:db.type/uri` values arrive as their
string form, losing the type ([#135](https://github.com/replikativ/datahike/issues/135)).

**Excised data cannot be recovered** by any reader — it is absent from the log
by definition.

## Importing into a database that already has data

`{:merge? true}` lifts the empty-target refusal. It is **append-only**:
`transact-entities-directly` does not resolve `:db.unique/identity`, so
importing the same Datomic database twice adds the entities twice rather than
upserting onto the existing ones.

## Memory

Descriptors are `t` *windows*, not records, and `:read` fetches one window at a
time, so neither the descriptor list nor a read holds the log. Measured on 3000
transactions / 12 012 datoms under `-Xmx700m`: 60 descriptors, heap 61 → 63 MB
across the whole import, and 0 MB to build the source.

`:window` (default 100) sets transactions per chunk. A chunk is whole
transactions by construction, so transaction alignment is free.

## Verification

`import-from-datomic!` counts the source by default so the import can be
verified, which costs one extra pass over the log. `{:count? false}` skips it —
and then requires `{:verify? false}`, so an unverified import is always
something you asked for rather than something that quietly happened.

## Running the tests

```
bb test datomic
```

Needs the `:datomic` alias for the peer jar. The tests live on their own source
path (`test-datomic`) precisely because the namespace fails to *load* without
Datomic rather than skipping, so it must be unreachable from every other tier.
