# Experimental snapshot reference values

A `:db.type/store-ref` UUID retains one object key. Naming a commit that way keeps
its record, but does not retain its index nodes, schema, payloads, or secondary
index generations. A snapshot needs a transitive dependency walk.

Attributes of `:db.type/gc-ref` hold versioned typed references. The initial kind
is an exact **same-store** snapshot:

```clojure
(require '[datahike.snapshot-reference :as snapshot])

(d/transact conn [{:db/ident :source/snapshot
                   :db/valueType :db.type/gc-ref
                   :db/cardinality :db.cardinality/one}])

;; Keep this target retained throughout construction and publication.
(def source @conn)
(def reference (snapshot/snapshot-ref source))
(d/transact conn [{:source/snapshot reference}])

;; reference = [:datahike/snapshot 1 <store-id> <commit-id>]
(d/commit-as-db conn (nth reference 3))
```

`snapshot-ref` accepts a committed database value, checks its required stored record, index and generation dependencies, and returns a portable vector. `{:sync? false}` returns a channel. It
rejects uncommitted values and incomplete targets. Legacy store-ref blob keys remain opaque: they may name external objects and
do not assert existence in the primary store. Construction does not create a
pin, and cannot resurrect an already collected snapshot. Transactions validate
referenced targets before publishing their head.

The full collector follows these references from live datoms and retained
history. It keeps each exact commit, its schema, primary and temporal indices,
nested snapshot references, blob keys, and same-store secondary generations.
Exact references do not keep commit ancestors implicitly. Shared targets and
cycles terminate through the traversal's visited set. Missing required targets,
malformed values, unregistered markers, and invalid marker results abort marking
before any sweep.

Retracting the last reference permits collection once no branch, durable root,
retained history, or other snapshot references the target. With history enabled,
a retracted value can remain reachable through temporal datoms. As for store-ref,
`noHistory` and tuple nesting are rejected for reference attributes.

## Operational contract

Use `:crypto-hash? true` and full reachability GC. As with full PSS marking,
ClojureScript needs synchronously readable nodes, such as a warmed tiered
frontend. The async constructor option does not support cold async-only node
reads. These references are rejected
when online freed-address GC is enabled or addresses can be recycled. Every
exact target must satisfy the same policy, including older snapshots.

Before enabling this feature, upgrade every full collector and replication walker
to understand typed references. Older collectors can delete their dependencies
and the pause flag. Then stop and drain every
online or recycling collector, including suspended collectors and other
connections. Commit publication sets a permanent store-level pause flag; later
online collector runs consult it, and full GC preserves it. The flag cannot stop
a collector that already passed its check. It remains set even after reference
attributes are removed, because old snapshots may still need their targets.

The current collector remains `:mode :bounded`. Neither constructor validation,
commit validation, nor its age floor provides a publication barrier for **old**
target objects. Throughout construction and publication, keep the target already
retained under the deployment's existing coordination contract, or externally
exclude GC. A process-local guard does not coordinate other processes or stop a
suspended collector. Do not publish an unretained old snapshot concurrently with
GC. Coordinated mode remains unavailable.

A same-store snapshot may name external blobs or secondary generations. Blob
keys and external secondary root envelopes are discovered transitively; Datahike
cannot retain or delete objects in another physical store. External root discovery
is diagnostic input and grants no sweep authority. Cross-store snapshot values
are rejected. Datom export and retransaction into another store do not reproduce
these exact snapshots; use an appropriate store-level copy that preserves store
identity and dependencies.

## Internal extension contract

`datahike.gc-reference/decode-reference` turns a versioned value into a typed
edge. `expand-reference` reports complete dependencies under `:reachable`,
`:store-refs`, `:external-secondary-roots`, or `:records`. Record requests are exact,
required edges; a marker must validate its own direct object dependencies and
throw on incomplete input. Implementations must be read-only, deterministic, and
complete. They cannot grant retention or external sweep authority. This is an
internal experimental contract, not an API for arbitrary values of `:db.type/any`.

Secondary index key-maps use the same edge expansion boundary while retaining
their registered adapter markers. Structural PSS traversal remains separate: it
marks published node addresses and does not infer application-level references
from arbitrary datom values.
