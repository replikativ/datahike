# Valid Time

Datahike tracks a second time axis alongside transaction time: **valid
time**, the time at which a fact is true in the modeled world. While
`:db/txInstant` records *when the database learned* about a fact (see
[Time Variance](time_variance.md)), `:db.valid/from` and
`:db.valid/to` record *when the fact applies*. Together the two axes
make Datahike a bitemporal database.

You opt in per transaction. A tx with no valid-time meta has the same
semantics as today; valid-time only matters for transactions that
explicitly set it.

## Valid-Time Views

| View | Function | Returns | Use Case |
|------|----------|---------|----------|
| **At valid-time** | `(d/valid-at db t)` | State as it applies at `t` | "What was Bob's salary on Feb 15?" |
| **Between** | `(d/valid-between db from to)` | Datoms whose validity window overlaps `[from, to)` | Window queries, audit-by-period |
| **During** | `(d/valid-during db from to)` | Datoms whose validity window is contained by `[from, to)` | "Strictly within this period" |
| **All** | `(d/valid-all db)` | Every datom regardless of validity window | Inspection / reporting |

**Related:** For transaction-time travel, see [Time
Variance](time_variance.md). For column-oriented secondary indices
that push the valid-time filter into a native scan, see [Secondary
Indices](secondary-indices.md).

## Setup for Examples

All examples below use this shared setup:

```clojure
(require '[datahike.api :as d])

(def schema
  [{:db/ident :emp/name
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :emp/salary
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}])

(def cfg {:store {:backend :memory :id #uuid "11111111-0000-0000-0000-000000000001"}
          :initial-tx schema
          :keep-history? true})

(d/create-database cfg)
(def conn (d/connect cfg))
```

`:keep-history? true` is required — `d/valid-at` reads the event
stream behind every fact, and history is what keeps that stream
available.

## Writing a Tx With Valid-Time

Pass `:tx-meta` alongside `:tx-data`. Both bounds are optional:

```clojure
;; Bob earns 100k for the whole of 2026 (no upper bound = open-ended)
(d/transact conn
  {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
   :tx-meta {:db.valid/from #inst "2026-01-01"}})

;; A bounded window — explicit start AND end
(d/transact conn
  {:tx-data [{:emp/name "Alice" :emp/salary 90000}]
   :tx-meta {:db.valid/from #inst "2026-01-01"
             :db.valid/to   #inst "2026-07-01"}})
```

`:db.valid/from` defaults to the tx's `:db/txInstant` when absent.
`:db.valid/to` defaults to open-ended (treated as +∞). The interval
is half-open: a fact is valid at `t` when `vf <= t < vt`.

The transactor rejects a tx whose `:db.valid/from >= :db.valid/to` —
zero-width or reverse windows can never match a `d/valid-at` query.

Equivalent low-level form, when you need the valid-time meta inside a
multi-step tx-data vector:

```clojure
(d/transact conn
  [{:emp/name "Bob" :emp/salary 100000}
   [:db/add "datomic.tx" :db.valid/from #inst "2026-01-01"]])
```

## Querying At a Valid-Time

`d/valid-at` returns a filtered view; queries against it see only
the datoms whose tx's valid-time window covers the query point.

```clojure
(d/transact conn
  {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
   :tx-meta {:db.valid/from #inst "2026-01-01"}})

(d/q '[:find ?s . :where [?e :emp/name "Bob"] [?e :emp/salary ?s]]
     (d/valid-at @conn #inst "2026-06-15"))
;; => 100000     ; Bob's window covers June 15
```

Pair with `d/as-of` for "what did we know at system-time T about
valid-time V?" — useful for replays and audits. **Order matters**:
wrap `d/as-of` first, then `d/valid-at` outermost:

```clojure
(d/q query (d/valid-at (d/as-of @conn audit-system-time) audit-valid-time))
```

`d/as-of` will throw on a valid-time-marked db to catch the wrong
ordering at the call site.

## Back-Corrections

The point of a second time axis: when you learn that yesterday's
assertion was wrong, you can record the correction *and* preserve
the audit trail of what you used to believe.

```clojure
;; Day 1: Bob's salary recorded as 100k from Jan-01 onward
(d/transact conn
  {:tx-data [{:emp/name "Bob" :emp/salary 100000}]
   :tx-meta {:db.valid/from #inst "2026-01-01"}})

;; Day 90 (Apr-01): correction — actual salary was 90k from Apr-01
(d/transact conn
  {:tx-data [{:emp/name "Bob" :emp/salary 90000}]
   :tx-meta {:db.valid/from #inst "2026-04-01"}})
```

Reads via `(d/valid-at (d/history db) t)`. History is the right view
here — it exposes both txes' assertions to the valid-time filter so
the correction can take effect. The 5-position datom pattern with
`?op = true` filters retraction datoms surfaced by the
cardinality-one upsert:

```clojure
(def hist (d/history @conn))

;; Before the correction's vt-from — only the original tx covers Feb-15
(d/q '[:find ?s . :where [?e :emp/name "Bob"] [?e :emp/salary ?s ?tx true]]
     (d/valid-at hist #inst "2026-02-15"))
;; => 100000

;; After the correction's vt-from — both txes cover May-15, but the
;; later tx wins for the same (entity, attribute, value) group.
(d/q '[:find ?s . :where [?e :emp/name "Bob"] [?e :emp/salary ?s ?tx true]]
     (d/valid-at hist #inst "2026-05-15"))
;; => 90000
```

The "later tx wins" rule operates per `(entity, attribute, value)`
triple. When the correction is a cardinality-one upsert (as above),
the prior value's retraction datom carries the new tx's id, so the
filter naturally picks the correction over the original at every
valid-time point covered by both windows.

## Retroactively Closing a Window

A back-correction expressed via cardinality-one upsert handles the
common case. The other case: you want to mark a prior tx's whole
window as "no longer valid past date T" — typically when the
correction creates *new entities* rather than updating existing ones,
so there's no shared `(e, a, v)` for the per-triple rule to dedupe.

Write a new datom on the prior tx-entity, naming the tx-id you want
to close:

```clojure
;; First tx — capture its tx-id from the report
(def r (d/transact conn
         {:tx-data [{:emp/name "Charlie" :emp/salary 80000}]
          :tx-meta {:db.valid/from #inst "2026-01-01"}}))
(def charlie-tx
  (->> (:tx-data r)
       (filter #(= :db/txInstant (.-a %)))
       first
       .-tx))

;; Later — close Charlie's tx-window effective Jul-01 forward
(d/transact conn
  [{:db/id charlie-tx :db.valid/to #inst "2026-07-01"}])
```

After the closing tx:

```clojure
(d/q '[:find ?s . :where [?e :emp/name "Charlie"] [?e :emp/salary ?s]]
     (d/valid-at @conn #inst "2026-06-15"))
;; => 80000    ; June 15 is inside the now-closed window

(d/q '[:find ?s . :where [?e :emp/name "Charlie"] [?e :emp/salary ?s]]
     (d/valid-at @conn #inst "2026-08-15"))
;; => nil      ; August 15 is past the closure
```

The closing tx is a normal commit on the writing tx. The prior tx's
hash is unchanged; the writing tx records the change. Both are
auditable, both verifiable. The transactor enforces a cross-tx
`vf < vt` check so a careless closure cannot silently invalidate the
prior tx's window.

The bare datom form above is the API. If you want a named operation
or to attach a `:reason` attr to the closing tx, do so in your own
namespace — the substrate intentionally stays primitives-first.

## Sequenced UPDATE (SQL:2011 style)

The pattern "for the slice `[Apr-01, Jul-01)` only, set salary =
95k" composes from three vt-windowed commits. The polygon at read
time handles the slice arithmetic:

```clojure
;; Original — vt-window [Jan-01, ∞)
(d/transact conn
  {:tx-data [{:emp/name "Dave" :emp/salary 100000}]
   :tx-meta {:db.valid/from #inst "2026-01-01"}})

;; The slice — vt-window [Apr-01, Jul-01)
(d/transact conn
  {:tx-data [{:emp/name "Dave" :emp/salary 95000}]
   :tx-meta {:db.valid/from #inst "2026-04-01"
             :db.valid/to   #inst "2026-07-01"}})

;; Restore the original value for the post-slice period
(d/transact conn
  {:tx-data [{:emp/name "Dave" :emp/salary 100000}]
   :tx-meta {:db.valid/from #inst "2026-07-01"}})
```

Reads:

| Query date | Result | Why |
|---|---|---|
| `#inst "2026-02-15"` | 100000 | Only the original window covers Feb-15 |
| `#inst "2026-05-15"` | 95000  | Two windows cover, the later tx wins for the same `(e, a, v)` |
| `#inst "2026-08-15"` | 100000 | Two windows cover, again the later tx wins |

The writer's job is to land the three windowed assertions; the
reader gets the slice for free.

## Performance Note

`d/valid-at` filters at the read layer — every result datom is
checked against its tx's vt-window, plus a per-`(e, a, v)` scan to
pick the winner among overlapping windows. The check is correct on
every read path (`d/q`, `d/datoms`, `d/pull`, `d/entity`,
`d/seek-datoms`, `d/index-range`) but does add cost.

For workloads with high vt-query volume, install a valid-time-aware
secondary index (see [Secondary Indices](secondary-indices.md));
those push the filter into a native columnar scan and bypass the
read-layer polygon entirely.

For one-off audits and ordinary tx workloads, the read-layer
filter is the path of least configuration.
