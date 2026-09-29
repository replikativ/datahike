(ns datahike.query.cancel
  "The ambient cancellation cell of a running query.

   `datahike.query` already threads a `:cancel` IDeref through the query
   context, and `datahike.query.execute` checks it at twenty-odd points.
   Relational algebra cannot see that context: `hash-join` is reached as
   `(reduce rel/hash-join (:rels ctx))`, with no room for an extra
   argument, and `datahike.query.relation` must stay a leaf -- it exists
   to break the cycle between `datahike.query` and
   `datahike.query.execute`.

   So the cell is ambient here, in a namespace with no dependencies,
   which all three can require.

   Checking costs a nil test and, when a query did set a deadline, one
   deref -- free beside building and hashing a tuple, which is what the
   loops that call it are already doing per iteration. Measured on a
   2.25M-row product, runtime with a deadline set is inside the noise of
   the same query without one.

   Where to call it depends on what the loop emits. In `-collect` and
   `cartesian-merge` the loop IS the product, so it is called per
   emitted tuple. In `hash-join` it is called per OUTER tuple and never
   per produced tuple: one outer iteration emits at most |smaller|
   tuples, since the hashed side is always the smaller relation, so the
   overshoot is already bounded by one inner pass. In the sort it is
   every 4096th comparison, which keeps the inner comparison free while
   still giving thousands of checks for any sort large enough to matter.

   JVM only, in practice. On ClojureScript a query runs synchronously,
   so the `js/setTimeout` that arms a `:timeout` cannot fire until the
   query has already returned -- `:timeout` there neither stops a query
   nor reports one. A `:cancel` cell works only if it is already truthy
   when the query starts."
  (:refer-clojure :exclude [check]))

(def ^:dynamic *cancel*
  "An IDeref whose non-nil value ends the query, or nil for no deadline.
   Bound by `datahike.query/run-with-query-timeout`."
  nil)

(defn check!
  "Throw if the query has been canceled or has run past its deadline.
   A no-op when no deadline is set."
  []
  (when-let [c *cancel*]
    (when-let [v #?(:clj (.deref ^clojure.lang.IDeref c) :cljs (deref c))]
      (if (= :datahike/query-timeout (:type v))
        (throw (ex-info "query timed out" v))
        (throw (ex-info "query canceled" {:datahike/canceled true}))))))
