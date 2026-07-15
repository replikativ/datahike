# RDF and RDF-star on Datahike

*Experimental (`datahike.experimental.rdf`). Not a stable API.*

Datahike's data model already covers most of RDF. The node kinds collapse onto it:

| RDF term | Datahike |
|---|---|
| **IRI** | a `:db/unique` entity (`:node/iri`), referenced as `:db.type/ref` |
| **blank node** | an eid / tempid |
| **literal** `(lexical, datatype, lang)` | an **interned entity**, keyed by its canonical term |
| **quoted triple** (RDF-star) | a reified statement entity |

The only piece that needs care is the literal — and the care is *identity*, not a new value type.

## Literals are interned, not a value type

An earlier design made `:db.type/literal` a native value type whose `Comparable` order sorted numerically in AVET. That is **wrong**, and the reason is worth stating because it is a general rule:

> **RDF has two equalities.** *Term* equality — same lexical form ∧ datatype IRI ∧ lang (lang case-insensitive) — is what the index, joins, `DISTINCT` and `COUNT` must key on. *Value* equality — `"1"^^xsd:integer` = `"01"^^xsd:integer` — is computed by `FILTER`/`=` at query time. `"1"` and `"01"` are **distinct terms but equal values.**

A value type makes the index key the value's `compareTo`. To sort numerically it must make `"1"` and `"01"` compare equal — which, in a sorted-set index, **collapses two distinct terms into one slot**: a cardinality-many attribute silently loses a datom, and `COUNT`/`DISTINCT` are wrong.

So a literal is **interned as an entity** keyed by the canonical *term* — the same move a production RDF store makes (Virtuoso's `RO_ID`). Same term ⇒ same entity; distinct term ⇒ distinct entity; the collapse is structurally impossible. Value comparison stays a query-time operation over the interned attributes.

```clojure
(require '[datahike.experimental.rdf :as rdf])

(d/transact conn rdf/literal-schema)

;; intern a term, then reference it
(let [t (rdf/literal "42" {:datatype :xsd/integer})]
  (d/transact conn [(rdf/literal-entity t)])                 ;; upserts by term id
  (d/transact conn [{:node/iri "ex:answer"
                     :ex/value (rdf/literal-ref t)}]))       ;; object is a ref
```

The interned entity:

```clojure
{:literal/id       #uuid "…"      ;; canonical TERM hash — :db.unique/identity
 :literal/lexical  "42"           ;; verbatim
 :literal/datatype :xsd/integer
 :literal/lang     ""
 :literal/num      42M}           ;; :db.type/bigdec — present only for numeric datatypes
```

### Numeric range stays index-native

`:literal/num` is an indexed `:db.type/bigdec` shadow of numeric literals. Because it is *homogeneously typed*, `FILTER(?x >= 10)` is an ordinary AVET range seek — `d/index-range` — with **no RDF code in datahike's core comparator**. (A native value type would still need the query planner to datatype-bound the predicate to seek at all — which is how Virtuoso does it, `sparql2sqltext.c` `RDF_TYPEMIN/TYPEMAX`. The shadow attribute reaches the same acceleration with less machinery.)

```clojure
(d/q '[:find [?lex ...] :where
       [?e :literal/num ?n] [(>= ?n 10M)]
       [?e :literal/lexical ?lex]] @conn)
```

### Canonicalization is the one irreversible decision

`:literal/id` is on-disk identity, so `rdf/literal` canonicalizes in exactly one place, and getting it wrong corrupts data that already exists:

- a **language tag folds to lower-case** — `@FR` = `@fr` (RDF 1.1);
- a **plain literal is `xsd:string`** — `"cat"` ≙ `"cat"^^xsd:string` (RDF 1.1 §3.3);
- the **lexical form is preserved verbatim** — `"1"` and `"01"` stay distinct terms; value canonicalization is query-time, never done on write.

## RDF-star: reified statements

Two things, kept separate:

- **the ground triple** — the real graph, `{subject-entity predicate object-ref}` datoms, typed and queryable;
- **the statement** — a *reification*, a handle for annotating the triple. Its identity is a canonical tuple over the three terms' N-Triples strings, so it is addressable by a static lookup-ref and is term-distinct (`<<s p "9">>` ≠ `<<s p "09">>`).

```clojure
(d/transact conn rdf/statement-schema)   ;; includes the interned-literal schema

;; SPARQL-star annotation `s p o {| :meta/source "crawler-1" |}`:
;; asserts the ground triple AND reifies it.
(d/transact conn (rdf/annotate (rdf/iri "ex:alice") :ex/knows (rdf/iri "ex:bob")
                               {:meta/source "crawler-1"}))

;; the ground triple is real data:
(d/q '[:find ?o :in $ ?s :where [?s :ex/knows ?o]] @conn (rdf/iri-ref (rdf/iri "ex:alice")))

;; the statement is addressable:
(d/pull @conn [:meta/source] (rdf/statement-ref (rdf/iri "ex:alice") :ex/knows (rdf/iri "ex:bob")))
```

`quote-triple` reifies **without** asserting the ground triple (a quoted-only triple). `annotate` does both.

## SPARQL ORDER BY

`rdf-term-compare` gives the SPARQL 1.1 total order over **mixed** query terms — `unbound < bnode < IRI < literal`, numeric-aware within literals. It orders query *results*; it is never an index comparator.

## Named graphs — federation (a deliberate 1.0 scope)

**One Datahike store is one RDF dataset.** Named graphs are a **federation of stores** via [`datahike.reference`](./cross-db-references.md) — `db-id` ≙ graph name.

This fits *graph-scoped* datasets (per-tenant, per-domain, per-source). It is **not** the SPARQL 1.1 *single-dataset-many-graphs* model:

- `GRAPH ?g { ?s ?p ?o }` with `?g` **unbound** is the union over all named graphs — a fan-out + union across stores, not one index scan;
- **cross-graph joins** are cross-*database* joins (the planner's relational fallback), not a merge-join on a shared index;
- `?g` as a per-quad provenance variable has no native representation, because the graph name lives outside the triple.

The alternative is a **quad** — a 4th indexed position in the datom (every index permutation becomes `GSPO`/`GPOS`/…). That is a **storage-format change** and is deliberately deferred past 1.0; it cannot be added afterward without a migration, so it is a decision to make consciously if single-store multi-graph query becomes a requirement.

## Known limitations

- **Predicate IRIs.** A predicate is a datahike attribute keyword. Arbitrary predicate IRIs need an explicit interning scheme (map IRI ↔ keyword); a predicate used as an *object* has no consistent representation yet. Triples *about* a predicate (`rdfs:domain`, `rdfs:subPropertyOf`) work via the attribute entity.
- **Range inside quoted triples.** A quoted statement stores its object's *term string*, so you cannot range-query the object of a `<<s p o>>` statement — query the ground triple for that.
- **Blank-node scoping.** `bnode = eid` is *stronger* than RDF (globally stable). On import, the same `_:b1` label in two different documents must mint **distinct** eids — do not reuse a `(bnode "b1")` tempid across documents.
- **cljs persistence.** The in-memory term API is cross-platform; durable use is exercised on the JVM.
- **No SPARQL engine / RDF I/O.** This is the substrate — term identity, interning, reification, ordering. Turtle/N-Triples import-export and a SPARQL surface layer above it are out of scope here.
