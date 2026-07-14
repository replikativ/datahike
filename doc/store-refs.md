# Blobs and out-of-line values (`:db.type/store-ref`)

*Experimental.*

Some values have no queryable structure — a PDF, an image, model weights, an audio
file. You want them **stored and fetched**, not indexed. `:db.type/store-ref` lets a
datom **name** such an object so the database knows it is still in use.

```clojure
{:db/ident       :issue/attachment
 :db/valueType   :db.type/store-ref     ;; a UUID naming an object
 :db/cardinality :db.cardinality/one}
```

The type adds exactly one thing over `:db.type/uuid`: **the garbage collector marks
it.** That is the whole feature.

## The rule: the database is the root set

> **An object lives iff a datom names it.** Anything else is garbage, by definition —
> including anything you write and never reference.

Retract the datom and the object becomes collectable. Keep history and it stays live,
because an `as-of` read can still reach the datom that names it — a retracted
attachment you can no longer fetch is not history.

## Where the bytes live is *your* choice

`:db.type/store-ref` deliberately does **not** say. There are two deployments, and
the trade-off between them is real.

### In the database's own konserve store

```clojure
(let [key (hasch.core/uuid bytes)]
  (guard/with-unreferenced-writes store-id       ;; see "the write window" below
    (k/bassoc store key bytes {:sync? true})
    (d/transact conn [{:issue/title "crash" :issue/attachment key}])))
```

- ✅ **GC reclaims it for you.** `d/gc-storage` spares it while referenced and deletes
  it once nothing names it.
- ✅ **`delete-database` erases it** with the database. One store, one lifetime.
- ✅ **Portable** — works on every konserve backend, so the local `:file` setup behaves
  exactly like S3.
- ❌ **Bytes transit your process.** konserve frames a binary value as
  `[header][meta][payload]`, so the object at that key is *not* raw. Reads and writes
  must go through `k/bget` / `k/bassoc`. Fine for a 50 KB thumbnail. **Wrong for a
  50 MB upload.**

### Anywhere else — a raw object store, a CDN

The browser `PUT`s straight to `s3://bucket/tenant/{id}/blobs/{uuid}` with a presigned
URL and a `Content-Type`; you transact the uuid. **The bytes never touch your JVM** —
which is the entire point, and at any real size it is the only sane shape.

Datahike cannot delete from there and does not pretend to. Instead it gives you the
**mark**, which is the hard half:

```clojure
(let [live (<?? S (datahike.gc/reachable-store-refs @conn))]
  (doseq [obj (list-objects bucket (str "tenant/" tenant-id "/blobs/"))]
    (when-not (live (blob-id obj))
      (delete-object bucket obj))))
```

`reachable-store-refs` returns every store-ref the database still names — **across all
branches, and through retained history**, honouring `remove-before` exactly as index
nodes do. Working out what is still referenced in an immutable, structurally-shared,
branched database is the difficult part; listing a prefix and deleting the rest is not.

**Same type, same mark, two deployments.** Marking an external id is a harmless no-op
for the local sweep (whitelisting a key the store does not have does nothing), so you
can mix them freely.

## The write window

Writing an object and referencing it in a *later* transaction leaves a gap in which
nothing names it — and an object nothing names is, by the rule above, garbage. If a
collection runs in that gap, it may take it.

Hold the guard across both:

```clojure
(guard/with-unreferenced-writes store-id
  (k/bassoc store key bytes {:sync? true})
  (d/transact conn [{:blob/id key}]))
```

For a browser upload the same problem appears differently: the object exists in S3
before any transaction names it. Two options —

1. **Transact the blob entity on upload**, and link it to the issue later. The object
   is named from the moment it lands. (This also makes an abandoned upload *findable*:
   `:find ?e :where [?e :blob/id] (not [_ :issue/attachment ?e])`.)
2. **Sweep with an age floor** — only delete objects older than your longest
   upload-to-transact gap. Simple, and it is what you would do anyway.

## What this is NOT for

**Structured data.** If you would ever want to filter or join on something *inside*
the value, it is a document, not a blob:

| what it is | where it goes |
|---|---|
| structure you **query** | datoms — `datahike.experimental.unstructured` shreds a nested map into entities with an inferred schema |
| structure you **search** | a secondary index — `:db.secondary/*` |
| bytes you **fetch** | `:db.type/store-ref` |

Note that **datoms are already sparse**: an entity has whatever attributes it has, and
you never had to declare your fields. The schema only pins *types*, cardinality,
uniqueness and indexing. So storing a document as an opaque value buys you **no
flexibility you did not already have** — it only costs you the indices, and the
indices are why the data is here.

## Content addressing

Use [hasch](https://github.com/replikativ/hasch) for the key, as `:db.secondary/only`
already does internally. Then:

- **the same bytes are the same key** — re-uploading is idempotent, and two entities
  referencing identical content share one object;
- **the object is immutable**, so a torn write leaves a collectable orphan, never a
  dangling pointer — the same property the whole storage model rests on.

A random uuid works too, but it means the object is a *mutable cell*, and mutable cells
are precisely what datahike otherwise does not have (the branch head is the only one).
Use one only when you mean it.
