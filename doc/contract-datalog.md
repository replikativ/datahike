# Experimental metered contract queries

The JVM namespace `datahike.experimental.contract` runs a restricted query
against immutable Datahike snapshots and a bounded input relation. A host pins
the language and meter profile at deployment. The
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

### Queries over a transaction

Use `contract/transition-profile` to read both snapshots in a transaction report.
The host supplies the delta rows and their order. For example, a host can pass
a booking's resolved price changes as four-column rows.

```clojure
(def confirmed-price-change
  '[:find ?booking :in $before $after $delta
    :where [$delta ?booking _ _ :added]
           [$before ?booking :booking/status :confirmed]])

(def snapshots {'$before (:db-before report) '$after (:db-after report)})
(contract/execute contract/transition-profile confirmed-price-change
                  #{:booking/status} 4 snapshots price-changes 32)
```

Every database pattern names its snapshot, including patterns inside joins.
Both snapshots use the v2 operators, the same read allowlist and the same
fuel schedule. Admission checks each read against its source's schema.
The ordinary query engine and the metered evaluator share the Datalog parser
and Datahike index operations. The metered evaluator fixes clause order,
cursor traversal and charging points so replicas can reproduce a receipt
independently of ordinary query planning and result caching.

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
UTF-16 code units. Integer bindings normalize to signed longs as on the EDN
wire. Result materialization retains up to 128 distinct basis
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
The generated corpus also exercises overlapping windows, missing keys, and
grouped counts over duplicate input rows. The differential tests compare boxed
integer inputs against the base and optimized ordinary engines with result
caching disabled, and compare the contract results on boxed and wire inputs.

### Contract v1 language

The following grammar describes the submitted EDN. Braces mean repetition,
angle brackets name grammar elements, and the optional section is marked
with a question mark. Admission also applies the binding and schema rules
below and the ordinary Datahike parser.

```text
query     ::= [:find find {find} (:with var {var})? :in $ $delta :where delta {clause}]
find      ::= var | (sum var) | (count var)
delta     ::= [$delta term {term}]
term      ::= var | _ | scalar
clause    ::= [entity attr term] | [(pred arg arg)] | interval
            | (not-join [var {var}] clause {clause})
            | (or-join [var {var}] branch {branch})
branch    ::= clause | (and clause {clause})
interval  ::= [entity attr var] [(>= var arg)] [(<= var arg)]
entity    ::= var | _ | positive-integer
arg       ::= var | scalar
pred      ::= = | not= | < | <= | > | >=
```

Variables are simple symbols beginning with `?`. Each input row unifies
left to right with the leading delta pattern. A repeated variable requires
equality, a literal must match, and each wildcard ignores its position.
A database attribute is a literal qualified keyword on the host's read
list. Entity lookups use EAVT. A value lookup requires an indexed or unique
attribute. A range requires an unbound value variable, an unbound entity or
wildcard, an indexed long attribute, and two already-known endpoints.
Point lookups support long, string and keyword attributes. Order comparisons
accept integers. Equality and inequality accept all profile scalars.

Each branch starts with exactly its declared, already-bound correlation
variables. Locals do not escape. Negation succeeds on absence. Disjunction
succeeds on the first witness. Each retains its outer row at most once.
Projection yields a set of vectors. Aggregates deduplicate the combined
find-variable and `:with` tuple before grouping by plain find variables.
Each distinct basis tuple incurs one aggregate event even when several
aggregates share it. Sum uses checked signed 64-bit addition.
The contract evaluator constructs groups only when it sees a basis tuple.

### Opt-in contract v2

Pass `contract/version-2` as the first argument to `admit` or `execute`.
The existing arities select v1. A Skuld posting program signs its selected
meter identifier along with the query. V2 adds these forms to the v1 grammar:

```clojure
[:find ?leg ?neighbor :in $ $delta
 :where [$delta ?leg ?account ?commodity]
 (or-join [?leg ?neighbor]
          [(inc ?leg) ?neighbor]
          [(dec ?leg) ?neighbor])]
```

* `[(inc arg) ?out]` and `[(dec arg) ?out]` take one bound argument and a
  fresh output variable. Spend one `:function` unit before checking the type
  and performing checked signed 64-bit arithmetic. Invalid types and overflow
  stop execution with the same refusal semantics as v1.
* An `or-join` with fresh variables in its flat argument vector exports them.
  Argument names must be distinct and every branch must bind every export.
  Each branch receives only the already-bound arguments. Locals stay inside
  their branch. Branches run in written order; each costs one `:branch` unit.
* For each candidate, spend one `:union` unit before testing the tuple of
  declared arguments for membership. Forward the first occurrence per outer
  row and retain at most 128 distinct tuples per active union. A 129th distinct
  tuple stops execution with `:relation-limit`, even when a later predicate
  would discard it. A surrounding existential may stop after the first witness.
* An all-bound disjunction still uses the existential operator. V1 receipts
  retain their original event keys. V2 receipts include `:function` and
  `:union`, including zero counts.

### Extending coverage toward ordinary Datahike Datalog

| Ordinary query family | Contract v1 coverage | Work to admit more |
| --- | --- | --- |
| Patterns and joins | Three-place, fixed-attribute, bound-prefix joins and indexed integer windows | Specify charging for full scans, additional pattern positions, dynamic attributes and additional value types |
| Logical clauses | Correlated existential disjunction and negation | V2 adds flat binding-producing `or-join`; plain `or`, plain `not` and nested required-variable syntax need separate admission rules |
| Inputs and databases | One current snapshot and one bounded relation | Define limits and iteration order for scalar, tuple, collection and additional database inputs or views |
| Results | Relation finds, `:with`, grouped `count` and `sum` | Specify empty-group behavior, scalar/tuple/collection finds, other aggregates, return maps, sorting and pull traversal |
| Predicates and functions | Six named binary comparisons | V2 adds checked unary `inc` and `dec`; specify type, output size and cost for additional built-ins |
| Rules | No rule input | Meter rule expansion first; recursive rules also need bounds for intermediate relations and repeated derivation |

For each extension, compare results with ordinary Datalog, record the exact
charge of a successful run and check interruption before each event. Include
misses and duplicate candidates so output size cannot hide work. This schedule
measures logical work. A native seek can still traverse index pages; bounding
physical work also depends on the supported backend and snapshot size.

Metering ordinary `d/q` would additionally require accounting across both
engines, including planner rewrites, native join tables, intermediate
materialization, caches and result shaping. Cancellation supplies a place to
stop execution; deterministic fuel also needs a versioned definition of what
consumes a unit across validators. A change to the contract execution order
or charging schedule requires a distinct profile identifier.

### An ordinary join through both engines

On the JVM, create 150 entities with `:left i` and `:right i`, for integers
0 through 149, then run the following relation query with the result cache
disabled. Instrumenting the named functions on main at `01c0c9e2` (with the
contract draft loaded) gave 150 result rows in each mode.

```clojure
[:find ?left ?right :where [?left :left ?x] [?right :right ?x]]
```

| Stage | Observed path or operation | Where a work budget would apply |
| --- | --- | --- |
| Plan | One call each to `get-or-create-plan` and `create-plan-via-ir` on a cold optimized run | Bound planning separately and define a cache policy for consensus receipts. |
| Scan and join | `execute-plan-prepared` reached `execute-plan-direct`. The producer added 150 entries via `probe-map-add`; the consumer called `probe-map-get` 150 times. | Charge index traversal, insertion and probes including misses. Charge each matched pair before allocating and filling its output tuple; repeated keys can yield many pairs per probe. |
| Output | The direct path assembles the result list and calls `finalize-direct-result` with a deduplication strategy. | Bound retained slots and charge candidate processing before deduplication. Final row count cannot bound intermediate work. |
| Base engine | Two calls to `collapse-rels` and one to `hash-join`; the same 150 rows. | Cover hash build, outer probes, and each emitted pair on this path too. Existing cancellation polling and retained-slot accounting address separate concerns. |

These are observed call counts for this fixture. They are not a consensus
price. A first engine-wide experiment could expose opt-in work counters for
these stages, exercise one-to-many joins and misses, and compare cold and
warm plans. Contracts would continue to use their pinned operator schedule
while those counters and the cost of index traversal are evaluated.

## Transaction preparation

On the JVM, `datahike.experimental.transaction/prepare` takes a current database,
transaction data and a nonnegative integer limit. It returns the ordinary
transaction report on success. It throws with `:error :transact/expansion-limit`
when the next logical unit exceeds the limit. The input database stays immutable.

Each transactor queue entry costs one unit, including expanded map operations,
component retractions, metadata, tuple flushes and entries revisited on a tempid
retry. Ordinary retractions also spend one unit per index lookup (including a
miss) and one per matching datom as the slice is collected. This includes outgoing
facts and incoming references. The schedule measures these logical operations.
Input decoding, map inspection, uniqueness checks, schema changes, transaction
functions, netting, index maintenance and physical I/O have separate costs which
this API does not bound. Callers admitting untrusted transactions need separate
controls for those paths.

An optional fourth argument to `prepare` receives the number of expansion units
consumed. It runs once as preparation exits, including when a transaction fails.
