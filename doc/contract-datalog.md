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

### Extending coverage toward ordinary Datahike Datalog

| Ordinary query family | Contract v1 coverage | Work to admit more |
| --- | --- | --- |
| Patterns and joins | Three-place, fixed-attribute, bound-prefix joins and indexed integer windows | Specify charging for full scans, additional pattern positions, dynamic attributes and additional value types |
| Logical clauses | Correlated existential disjunction and negation | Specify branch output bindings, deduplication and negation scope for plain `or`, `not` and binding-producing `or-join` |
| Inputs and databases | One current snapshot and one bounded relation | Define limits and iteration order for scalar, tuple, collection and additional database inputs or views |
| Results | Relation finds, `:with`, grouped `count` and `sum` | Specify empty-group behavior, scalar/tuple/collection finds, other aggregates, return maps, sorting and pull traversal |
| Predicates and functions | Six named binary comparisons | Add deterministic built-ins individually with type, result-size and per-call work bounds |
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
