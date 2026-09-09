# Snapshot dependency tracking

Dependency tokens let an in-process cache reuse a projection when its selected
database attributes have not changed. They are experimental and available in
Clojure and ClojureScript through `datahike.dependency-tracking`.

Enroll a named selector in a transaction, then capture the token and projection
from the same resulting database value:

```clojure
(require '[datahike.api :as d]
         '[datahike.dependency-tracking :as deps])

(def selector {:namespaces #{"catalog"}
               :exclude-attributes #{:catalog/counter}})

(def report
  (d/transact conn
    {:tx-data []
     :tx-options
     {:track-dependencies
      {::catalog selector}}}))

(def snapshot (:db-after report))
(def cached-token (deps/token snapshot ::catalog selector))
(def cached-value (d/q '[:find ?e ?name
                         :where [?e :catalog/name ?name]] snapshot))

;; Reuse cached-value only when this returns true; otherwise recompute it
;; and capture a new token from the same DB used for recomputation.
(deps/valid? cached-token @conn ::catalog selector)
```

The transaction option also works with `d/with`. Selectors combine exact keyword
`:attributes`, exact string `:namespaces`, and string `:namespace-prefixes`.
`:exclude-attributes` takes precedence over those selections. Schema and ident
changes always invalidate every group, even when excluded. Include every
attribute on which the projection depends, including attributes used only in
filters, joins, or ordering. Tracking a namespace also tracks new attributes in
that namespace. Selecting `"db"` includes `:db/txInstant`, which normally changes
on every transaction.

Named groups belong to each immutable DB value. Ordinary transactions inherit
them; a matching mutation replaces that group's token before any subsequent
transaction function runs. Intermediate transaction values therefore get their
own tokens, and a failed transaction cannot invalidate the original snapshot.
Changing a selector gives it a fresh token. A nil selector removes the group;
omitting a group leaves it unchanged.

Pass the expected selector to `token` and `valid?` when other code can enroll
the same group id. A mismatched selector returns no token, even if that group
is tracked. This prevents a replacement with a narrower selector from silently
weakening the cache's dependencies. The shorter arities omit this check and are
appropriate only when the caller controls the group's enrollment.

There are at most 16 groups per DB, with at most 256 total selector terms per
group. Group ids and selector terms are limited to 512 characters. These bounds
limit work on each mutation; unrelated row writes do not scan the catalog or
allocate replacement tokens.

Tokens are opaque identities, not transaction numbers, equality hashes, or
durability certificates. A changed token can still describe an equal projection;
recomputation is the conservative response. An unchanged token proves only that
no selected mutation occurred along the tracked lineage. It does not prove that
a speculative transaction committed or that a branch still points to that DB.

Tracking is runtime-only: it changes no storage format and is not restored by
reconnecting or loading a stored DB. Wholesale replacement drops tracking.
Filtered and temporal views return nil rather than inheriting a certificate
from their underlying current DB. Nil tokens never validate, including against
another nil token. Re-enroll after loading an untracked DB.

This facility assumes changes go through Datahike's transaction and database
operations. It cannot detect mutation of a Java array or date object held inside
a value, nor edits to DB internals made by application code. Cache only
projections whose input values obey the immutable-value contract. Tokens are
local to the runtime and are not a remote-client cache protocol.
