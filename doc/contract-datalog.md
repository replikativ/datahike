# Experimental metered contract queries

The JVM namespace `datahike.experimental.contract` runs a restricted query
against a current Datahike snapshot and a bounded input relation. A host pins
the language and meter identifier `:datahike/contract-v1` at deployment. The
contract has read access to attributes explicitly granted by the host.

```clojure
(require '[datahike.experimental.contract :as contract])

(def window
  '[:find ?id :in $ $delta
    :where [$delta ?lo ?hi ?target]
           [?e :sample/n ?n] [(>= ?n ?lo)] [(<= ?n ?hi)]
           [?e :sample/id ?id] [(= ?n ?target)]])

;; :sample/n is an indexed long. :sample/id is a long on the same entity.
(def reads #{:sample/n :sample/id})
(contract/admit window reads 3 (:schema db))
(contract/execute window reads 3 db [[0 149 149]] 1024)
```

Admission checks syntax with datalog-parser and lowers each clause in written
order. A clause without a supported operator fails admission. The first
pattern consumes signed `$delta` rows in order. Subsequent patterns use EAVT
when the entity is bound or AVET when the attribute value is bound. Indexed
long ranges use the three adjacent clauses shown above, with two bound,
inclusive endpoints. Joins consume each cursor in native index order and
stream each candidate directly to the following operator.

Supported forms include relational projection, variables, wildcards and
scalar constants, equality and inequality, integer comparisons, correlated
`not-join` and existential `or-join` with already-bound join variables. The
result can group by projected variables and compute `sum` and `count` over
distinct projection tuples including the variables specified by `:with`.
Predicate names resolve from a fixed table. Rules, recursion, arbitrary
functions, dynamic attributes, pull, and history views are outside this profile.

### Receipts and limits

| Event | When one unit is spent |
| --- | --- |
| Input | Before consuming a delta row |
| Cursor | Before opening an index cursor |
| Advance | Before requesting its first or next candidate, including the terminal check |
| Predicate | Before evaluating a comparison |
| Branch | Before entering a correlated branch |
| Project | Before projecting a candidate |
| Aggregate | Before updating a group for a distinct projection tuple |

A successful receipt includes the result relation, the total fuel and per-event
counts. Exhaustion stops at the signed limit. A type error, integer overflow,
oversized stored scalar or relation limit returns a refusal and the consumed
fuel. Every refusal omits the result relation. Hosts must accept writes only
on successful execution and satisfaction of their invariant.

The profile admits up to 128 syntax nodes, depth 12, one to eight delta
columns, 32 delta rows and 4,096 fuel. Scalars are signed machine integers,
keywords or strings; keyword spellings and strings are limited to 128 JVM
UTF-16 code units. Result materialization retains up to 128 distinct basis
tuples and at most one aggregate group per tuple. Intermediate join bindings
stream on the stack. A host may set tighter byte, state-size and fuel limits.
Compilation and schema validation have syntax bounds and do not spend fuel.
Index pages, physical bytes and wall-clock time are outside these counters.

### Evidence and review scope

The differential fixture scans an inclusive range of 150 values, follows
each entity to a second attribute, and returns one projected row. Tests
compare its result with ordinary Datahike Datalog, stop during the scan at
several budgets, and exercise the projection limit. Aggregation tests cover
duplicate inputs, reversed input order, `:with`, and exact exhaustion.

This is an experimental execution path separate from `d/q`. Before freezing
the profile, review equivalence over a broader generated query corpus and
define the supported index configurations for consensus replay. A new
execution order or charging schedule requires a distinct profile identifier.
