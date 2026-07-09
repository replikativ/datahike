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

A cross-database reference is a triple — the target database, a selector
for the entity inside it, and which version:

```
(db-id, selector, temporal)
```

The selector is either **value-level** (a lookup ref on a `:db/unique`
attribute — portable, survives export/import) or a **bare entity id** —
the cheapest, most general pointer, stable within a logical database but
renumbered by re-materialization (see [Eid vs value selectors](#uri-form)).

| part | what | datahike primitive |
|---|---|---|
| `db-id` | target database | the store `:id` (mandatory UUID) |
| `selector` | entity inside it | a lookup ref `[attr value]` (unique attr; non-unique via explicit opt-in) or a bare **entity id** (`dh://<db-id>/387`) — the cheapest, most general pointer: direct EAVT seek, no AVET, no schema, no unique attr needed |
| `temporal` | which version | `nil` = live head; `{:tx n}` / `{:date inst}` = `as-of`; `{:commit uuid}` = an exact content-addressed commit (via `commit-as-db`); `{:valid inst}` = [valid-time](./valid_time.md) point (combinable with the tx-time pin); `{:branch "name"}` = branch head |

The temporal distinguishes the **two reference kinds**:

- **Living reference** (no temporal / branch): tracks the head. For
  navigation, mentions, "see also" — follows edits.
- **Record reference** (`:tx` / `:date` / `:commit`): pinned to a snapshot.
  For provenance, citation, audit — immutable, and resolvable forever on
  stores with `:keep-history? true`. `:commit` is the most precise form:
  the commit-id fully determines the db value (content-addressed — a
  merkle root under `:crypto-hash?`), independent of branch, so it
  supersedes `:tx`/`:date` when present.

With [bitemporal valid-time](./valid_time.md), `{:valid inst}` pins the
*valid-time* axis (`valid-at`, supersession semantics) and composes with
the tx-time pin — `{:tx n :valid v}` reads "what we believed at tx `n`
about the state valid at `v`", the classic bitemporal citation. Applied
in the documented composition order (`valid-at` outermost over `as-of`).

## URI form

For text, hyperlinks, logs, and export. The temporal is a standard URL
query string (`?key=value&key=value`):

```
dh://<db-id>/<attr>/<value>[?<temporal>]

dh://96ae43a7-…/entity/uuid/08308437-…                    ; living
dh://96ae43a7-…/S.Page/title/str:Roadmap?tx=536871113     ; record (as-of tx)
dh://96ae43a7-…/entity/uuid/08308437-…?commit=1a2b…       ; record (exact commit)
dh://96ae43a7-…/entity/uuid/08308437-…?branch=experiment  ; branch head
dh://96ae43a7-…/entity/uuid/08308437-…?tx=536871113&valid=2026-06-01T00:00:00Z  ; bitemporal
```

A namespaced attr keeps its `/` as a literal path separator, so it reads
hierarchically (`:S.Page/title` → `S.Page/title`, not `S.Page%2Ftitle`);
each part is still url-encoded for any other special char. The value is
always the last, `/`-free path segment (`parse` uses this to recover the
attr, whether it spans one segment or two), and carries a type tag so the
selector round-trips to the exact datahike value. **Every value type is
supported.** Readable tags cover the identity-friendly scalars — untagged
UUID, `str:`, `long:`, `kw:`, `bool:`, `inst:` (ISO-8601); `flt:` and
`b64:` (url-safe base64) carry `float` and `bytes`; and an `edn:` fallback
losslessly encodes the rest — `bigint`, `bigdec` (**scale preserved**),
`double`, `symbol`, and `tuple` — via `pr-str` / `edn/read-string`. A
single all-digit path segment is an entity-id selector. The temporal
qualifiers — `tx` `date` `commit` `valid` `branch` — combine with `&`.
`render` / `parse` round-trip.

**Eid vs value selectors.** An eid is stable within its logical
database — replicas of the same store `:id` share the index, branches
share it copy-on-write, and datahike never reuses eids (a dangling eid
can never silently point at a new entity). What eids do NOT survive is
re-materialization: export/import or migration-by-retransaction
renumbers them, while value selectors survive anything that preserves
values. Eid = physically-bound pointer (cheapest, works for entities
with no unique attribute at all, on schemaless databases); value
selector = semantically-robust pointer. Pick per use; record references
(`?tx=` / `?commit=`) compose with both.

## Reified references (queryable links)

Store an *outgoing* cross-db link as a small entity, not a URI string, so
links stay queryable in datalog by predicate and target:

```clojure
{:dh.ref/db    #uuid "96ae43a7-…"
 :dh.ref/attr  :entity/uuid
 :dh.ref/value "08308437-…"
 :dh.ref/type  :derived-from}   ; application vocabulary: :mentions, :cites, …
```

`reference->tx-map` / `tx-map->reference` convert. Keep *within*-database
links as plain `:db.type/ref` attributes — reified references are only
for crossing stores.

The `:dh.ref/*` attributes are **installed in the system schema** (they
graduated like `:db.valid/*` and `:db.secondary/*`): every database accepts
reified references without any schema declaration, in both
schema-flexibility modes, with `:dh.ref/db` and `:dh.ref/value`
AVET-indexed for reverse lookups ("all references into database X").

`:dh.ref/value` stores the value in the same self-describing tagged
encoding as the URI (`long:42`, `str:…`, `edn:…`; a bare uuid is untagged),
so the selector's **datatype travels inside the string** — there is no
separate value-type attribute to keep in sync, and `tx-map->reference`
recovers the exact typed value. The practical consequence: a reverse lookup
*by value* must query the **encoded** form, most easily obtained via
`reference->tx-map`:

```clojure
;; every reified link pointing at [:item/code 42] in database X
(let [needle (:dh.ref/value (ref/reference->tx-map (ref/reference db-x [:item/code 42])))]
  (d/q '[:find ?r :in $ ?db ?v :where
         [?r :dh.ref/db ?db] [?r :dh.ref/value ?v]]
       db db-x needle))
```

Because the stored form sorts lexically, value queries are **exact-match,
not native range/order**. That is exactly right for a pointer (you resolve
it, you don't range-query it). If you ever need to range- or type-query
reference values, store a native-typed mirror alongside the string — e.g. a
`:db.type/literal` from the experimental RDF support — rather than reaching
for a value-type tag.

## Resolution

Connection acquisition is deployment-specific (peer registry, access
control, branch selection), so it is injected:

```clojure
(require '[datahike.reference :as ref])

(ref/resolve-reference
  (ref/parse "dh://96ae43a7-…/entity/uuid/08308437-…?tx=536871113")
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
