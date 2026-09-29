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

(def ^:private tick-bits
  "Check roughly every 512 units of work by default. A product loop
   constructs and hashes a tuple per unit, so 512 of them is still well
   under a millisecond -- fine for a deadline -- while keeping the cell
   out of the inner loop."
  9)

(defn ticker
  "A stateful counter that checks the deadline every 2^`bits` units,
   512 by default.

   `(tick)` counts one unit; `(tick n)` counts n, for a loop that knows
   it is about to do n units of work in one step. Counting the work
   rather than the iterations is what bounds the gap: `hash-join`'s
   outer loop emits a whole inner pass per iteration, so ticking once an
   iteration would let a wide join run 512 PASSES -- millions of tuples
   -- between checks.

   Pass a larger `bits` where a unit is much cheaper than a tuple; a
   sort comparison is the one case, at 4096.

   Coarse on purpose. Checking every unit is affordable in principle but
   measured ~40% slower on the join's inner loop, and it makes the
   cancel cell a poor proxy for SCAN demand: `query-limit-demand-test`
   counts derefs of its own cell to assert that a bounded query does not
   walk the whole attribute, and a per-tuple check turned a constant
   count into one proportional to the input (133, 233, 433 for n of 100,
   200, 400). The work was always O(n); only the counting was new -- but
   a test that can no longer see the difference is worse than a coarser
   check."
  ([] (ticker tick-bits))
  ([bits]
   (let [mask (dec (bit-shift-left 1 (long bits)))
         n    (volatile! 0)]
     (fn
       ([] (when (zero? (bit-and (vswap! n unchecked-inc) mask))
             (check!)))
       ([units]
        (let [before (bit-shift-right ^long @n (long bits))
              after  (bit-shift-right ^long (vswap! n unchecked-add (long units))
                                      (long bits))]
          (when (not= before after)
            (check!))))))))
