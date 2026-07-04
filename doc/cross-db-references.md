# Cross-database references (`datahike.reference`)

Part of datahike's [distributed architecture](./distributed.md): the
[Distributed Index Space](./distributed.md#distributed-index-space-dis)
shares single databases across peers; this document covers addressing
*between* databases.

Datahike deployments legitimately span many databases: per domain, per
tenant, per permission boundary. Multiple databases are a feature — they
are the unit of replication (the store `:id` names the same logical
database on every peer that syncs it) and the natural unit of selective
sharing. What was missing is a systematic way to *point across* them.

## The reference triple

A cross-database reference can never be an entity id — eids are internal
to one index. It must be value-level:

```
(db-id, selector, temporal)
```

| part | what | datahike primitive |
|---|---|---|
| `db-id` | target database | the store `:id` (mandatory UUID) |
| `selector` | entity inside it | a lookup ref `[attr value]` (unique attr; non-unique falls back to a value query) |
| `temporal` | which version | `nil` = live head; `{:tx n}` / `{:date inst}` = `as-of`; `{:branch "name"}` = branch head |

The temporal distinguishes the **two reference kinds**:

- **Living reference** (no temporal / branch): tracks the head. For
  navigation, mentions, "see also" — follows edits.
- **Record reference** (`:tx` / `:date`): pinned to a snapshot. For
  provenance, citation, audit — immutable, and resolvable forever on
  stores with `:keep-history? true`.

## URI form

For text, hyperlinks, logs, and export:

```
dh://<db-id>/<attr>/<value>[@<temporal>]

dh://96ae43a7-…/entity%2Fuuid/08308437-…                    ; living
dh://96ae43a7-…/S.Page%2Ftitle/str:Roadmap@tx:536871113     ; record
dh://96ae43a7-…/entity%2Fuuid/08308437-…@branch:experiment  ; branch head
```

Attr keywords are URL-encoded whole; values carry an optional type tag
(`str:` `long:` `kw:`; untagged = UUID-or-string). `render` / `parse`
round-trip.

## Reified references (queryable links)

Store an *outgoing* cross-db link as a small entity (`ref-schema`), not a
URI string, so links stay queryable in datalog by predicate and target:

```clojure
{:dh.ref/db    #uuid "96ae43a7-…"
 :dh.ref/attr  :entity/uuid
 :dh.ref/value "08308437-…"
 :dh.ref/type  :derived-from}   ; application vocabulary: :mentions, :cites, …
```

`reference->tx-map` / `tx-map->reference` convert. Keep *within*-database
links as plain `:db.type/ref` attributes — reified references are only
for crossing stores.

## Resolution

Connection acquisition is deployment-specific (peer registry, access
control, branch selection), so it is injected:

```clojure
(require '[datahike.reference :as ref])

(ref/resolve-reference
  (ref/parse "dh://96ae43a7-…/entity%2Fuuid/08308437-…@tx:536871113")
  (fn [db-id {:keys [branch]}]
    (my-registry/connect db-id branch)))   ; conn, db value, or nil
;; => {:db <as-of db> :eid 387}   or nil (dangling)
```

Resolution is **strict by default**: the selector attr must be
`:db/unique` in the target schema, so the *database* — not luck —
guarantees the reference is single-valued (and the lookup is a direct
AVET index slice: datahike's AVET only contains `:db/unique`/`:db/index`
attrs, so the fast path exists exactly where the semantics are sound).
For non-unique selectors, opt in explicitly:

```clojure
(ref/resolve-reference r connect-fn {:ambiguous :first}) ; first match
(ref/resolve-all r connect-fn)                           ; the whole fiber
```

Both are AEVT attribute scans — linear in the attribute's datom count.
The explicit opt-in keeps the relaxed convenience out of the default
contract (nothing to deprecate later) and makes its real cost visible
at the call site. Schema-on-read users can resolve by any attribute —
they just say so.

A `nil` from the connect-fn (unknown or ungranted database) resolves the
reference to `nil` — dangling-but-typed, which is the correct behaviour
at a permission boundary: the link's existence is visible, its content is
not.

## Joins across databases

Datalog's multi-source queries make value-level joins first-class — the
pullback (fiber product) over a shared identity value is one query:

```clojure
(d/q '[:find ?a ?b :in $one $two
       :where [$one ?a :S.Page/title ?t]
              [$two ?b :entity/title  ?t]]
     @db-one @db-two)
```

`datahike.reference` addresses a *single* entity; for set-level joins use
multi-source queries directly.

## Correspondence to RDF / semantic web

Deliberately rhymed with, not adopted: `db-id` ≙ named graph, the
selector ≙ resource IRI, `:dh.ref/type` ≙ predicate, record references ≙
a dataset snapshot. Export is mechanical if ever needed; nothing in the
representation depends on the RDF stack.
