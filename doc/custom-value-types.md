# Custom value types

Datahike can accept scalar value types supplied by an extension. The extension
registers process-local behavior before creating or connecting a database:

```clojure
(require '[datahike.value-type :as value-type])

(value-type/register!
 {:id :example.type/box
  :type Box
  :valid? box?
  :compare compare-boxes
  :wire {:name "example/box"
         :version 1
         :encode box->portable-value
         :decode portable-value->box}})
```

The decoder receives `[version payload]` as two arguments. Its payload must be
ordinary portable data supported by the surrounding codec. It must retain
support for every version previously written under the same type id and wire
name. An incompatible change to equality or ordering requires a new type id and
a data migration.

The runtime value class owns equality and hashing. Its comparator must be a
deterministic total order with these invariants:

```text
compare(a, b) == 0  iff  a == b
a == b              implies hash(a) == hash(b)
```

Datahike cannot implement those operations only inside its index: Clojure sets,
maps, query grouping, `distinct`, unique attributes and retractions all observe
the host value's equality and hash. Datahike validates the exact runtime type
and the registered predicate on transaction and after decoding.
Built-in scalar runtime types cannot be registered because comparator dispatch
is global; extensions must use a dedicated wrapper type.

Registration is immutable for the lifetime of every connection and store in
the process. Repeating an equal descriptor is idempotent; reusing an id, runtime
type or wire name for a different descriptor throws. `reset-registry!` exists
only for isolated tests and REPL experiments and must never be called while a
connection or store is live.

## Initial support boundary

This API is experimental. The first supported path is:

- `:schema-flexibility :write`;
- JVM and ClojureScript registry/comparison semantics;
- the persistent-set index using its ordinary Fressian-backed store path;
- release/reconnect with the extension registered before connecting;
- explicit failure when the database schema requires an unregistered type.

The following boundaries are not supported yet:

- `:attribute-refs? true` databases;
- preconfigured stores whose serializer is installed by the backend, including
  the current Boring/LMDB path;
- migration export/import;
- Transit and remote CBOR connections;
- custom types nested inside typed tuples.

Each needs an adapter driven by the same descriptor, plus a fail-closed
capability check. Those unsupported boundaries are not all mechanically
rejected yet. Until their adapters and guards land, do not use a custom type
across them or register one after opening a connection.
