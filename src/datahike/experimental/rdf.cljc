(ns datahike.experimental.rdf
  "EXPERIMENTAL — RDF / RDF-star semantics on datahike (NOT a stable API).

   Datahike's node kinds collapse onto its data model: an RDF **IRI** is a
   `:db/unique` entity (referenced as `:db.type/ref`), a **blank node** is an
   eid/tempid, and a **quoted triple** is a reified statement entity. A **literal**
   `(lexical, datatype, lang)` is INTERNED as an entity keyed by its canonical term —
   the same move a production store makes (Virtuoso's `RO_ID`), and the reason is
   below.

   WHY LITERALS ARE INTERNED, NOT A VALUE TYPE. RDF has two equalities. TERM equality
   — same lexical form ∧ datatype IRI ∧ lang (lang case-insensitive) — is what the
   index, joins, DISTINCT and COUNT must key on. VALUE equality — `\"1\"^^xsd:integer`
   = `\"01\"^^xsd:integer` — is computed by `FILTER`/`=` at query time. These are
   different: `\"1\"` and `\"01\"` are DISTINCT terms but EQUAL values.

   A native `:db.type/literal` value type would make the index key the value's
   `compareTo`. To sort numerically it must make `\"1\"` and `\"01\"` compare equal —
   which, in a sorted-set index, COLLAPSES two distinct terms into one slot: lost
   datoms, wrong COUNT/DISTINCT. An interned entity keyed by the canonical TERM hash
   makes that impossible: same term ⇒ same entity, distinct term ⇒ distinct entity,
   and value comparison stays a query-time operation over the interned attributes.
   Numeric RANGE stays index-native via a `:literal/num` shadow attribute (a
   homogeneously-typed `:db.type/bigdec` key `d/index-range` already seeks) — with no
   RDF code in datahike's core comparator.

   THE IRREVERSIBLE PART is the canonical term id (`literal-term-id`): it becomes
   on-disk identity, so its canonicalization — lang folded to lower-case, plain
   literal ≙ `xsd:string`, lexical form preserved VERBATIM — must be right before any
   data is written. See the `literal` docstring.

   NAMED GRAPHS: one datahike store is one RDF dataset. Named graphs are a FEDERATION
   of stores via `datahike.reference` (`db-id` ≙ graph). This is a deliberate 1.0
   scope: it fits graph-scoped datasets (per-tenant, per-domain) but is NOT the
   SPARQL-1.1 single-dataset-many-graphs model — `GRAPH ?g {…}` with `?g` unbound is a
   fan-out + union across stores, not one index scan, and cross-graph joins are
   cross-database joins. A quad (a 4th indexed datom position) is the alternative and
   is a storage-format change deferred past 1.0.

   `rdf-term-compare` gives the SPARQL ORDER BY total order over mixed query terms
   (unbound < bnode < IRI < literal) for ordering RESULTS — never an index comparator.

   Cross-platform (JVM + cljs): interning is plain datoms + `hasch`, both portable.

   REPL: `(require '[datahike.experimental.rdf :as rdf])` then `(rdf/demo)`."
  (:require [datahike.api :as d]
            [hasch.core :as hasch]
            [clojure.string :as str]))

;; ============================================================================
;; RDF terms — literals
;; ============================================================================
;;
;; RDF 1.1: every literal has a datatype. A plain string is xsd:string; a
;; language-tagged string is rdf:langString and carries a non-empty lang.
;;
;; A literal TERM in memory is a canonical map {:literal/lexical :literal/datatype
;; :literal/lang}. `literal` is the ONLY constructor, so canonicalization happens in
;; exactly one place and the id and the stored fields can never disagree.

(def ^:const xsd-string :xsd/string)
(def ^:const rdf-lang-string :rdf/langString)

(def numeric-datatypes
  "XSD datatypes whose literals also compare NUMERICALLY (value equality), and so
   get a `:literal/num` shadow for index-native range."
  #{:xsd/integer :xsd/decimal :xsd/double :xsd/float :xsd/long :xsd/int
    :xsd/short :xsd/byte :xsd/nonNegativeInteger :xsd/positiveInteger
    :xsd/negativeInteger :xsd/nonPositiveInteger :xsd/unsignedLong
    :xsd/unsignedInt :xsd/unsignedShort :xsd/unsignedByte})

(defn literal
  "A canonical RDF literal term.
     (literal \"cat\")                        => plain (xsd:string)
     (literal \"chat\" {:lang \"FR\"})          => rdf:langString @fr  (lang lower-cased)
     (literal \"42\"  {:datatype :xsd/integer})

   CANONICALIZATION (irreversible — this defines term identity):
     - a language tag folds to lower-case (`@FR` = `@fr`, RDF 1.1);
     - a lang-tagged literal has datatype rdf:langString; a plain one xsd:string
       (so `\"cat\"` ≙ `\"cat\"^^xsd:string`, RDF 1.1 §3.3);
     - the lexical form is preserved VERBATIM — `\"1\"` and `\"01\"` stay distinct
       terms; value canonicalization is a query-time concern, never done on write."
  ([lexical] (literal lexical nil))
  ([lexical {:keys [datatype lang]}]
   (let [lex  (str lexical)
         lang (some-> lang str/lower-case not-empty)
         dt   (cond lang rdf-lang-string
                    datatype datatype
                    :else xsd-string)]
     {:literal/lexical lex :literal/datatype dt :literal/lang (or lang "")})))

(defn literal? [t]
  (and (map? t) (contains? t :literal/lexical) (contains? t :literal/datatype)))

(defn lex      [t] (:literal/lexical t))
(defn datatype [t] (:literal/datatype t))
(defn lang     [t] (let [l (:literal/lang t)] (when (seq l) l)))
(defn plain?   [t] (and (literal? t) (= (datatype t) xsd-string) (nil? (lang t))))

(defn literal-term-id
  "The canonical TERM id of a literal — a `hasch` content-hash uuid over the
   already-canonical (lexical, datatype, lang). Same term ⇒ same id; distinct term
   ⇒ distinct id. This is the interned entity's `:db.unique/identity`."
  [t]
  (hasch/uuid [(:literal/lexical t) (:literal/datatype t) (:literal/lang t)]))

(defn- numeric-value
  "The bigdec value of a numeric literal (for the `:literal/num` range shadow), or
   nil. `\"1\"` and `\"01\"` both yield 1M — that is VALUE equality, and it lives on a
   separate attribute, never on the term id."
  [t]
  (when (contains? numeric-datatypes (:literal/datatype t))
    (try #?(:clj (bigdec (:literal/lexical t)) :cljs (js/parseFloat (:literal/lexical t)))
         (catch #?(:clj Exception :cljs :default) _ nil))))

(defn literal-entity
  "A tx-map for the interned literal entity. `:literal/id` is the term id and is
   `:db.unique/identity`, so re-interning the same term upserts to ONE entity (RDF
   set semantics for literals). Numeric literals also carry `:literal/num` for range."
  [t]
  (let [num (numeric-value t)]
    (cond-> {:literal/id       (literal-term-id t)
             :literal/lexical  (:literal/lexical t)
             :literal/datatype (:literal/datatype t)
             :literal/lang     (:literal/lang t)}
      num (assoc :literal/num num))))

(defn literal-ref
  "A lookup-ref addressing the interned literal term — use where a `:db.type/ref`
   object is expected. Transact `(literal-entity t)` in the same tx (or earlier) so
   the entity exists."
  [t]
  [:literal/id (literal-term-id t)])

;; IRIs and blank nodes are NOT literals — an IRI is a ref entity, a bnode an eid.
;; These tagged forms carry a term through construction and mixed-term ordering.
(defn iri   [s]  [:iri (str s)])
(defn bnode [id] [:bnode (str id)])

(def literal-schema
  "Interned-literal schema. Transact once per store that stores RDF literals."
  [{:db/ident :literal/id :db/valueType :db.type/uuid
    :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}
   {:db/ident :literal/lexical :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   {:db/ident :literal/datatype :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
   {:db/ident :literal/lang :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
   ;; The value shadow: numeric literals index here so FILTER range is an AVET seek
   ;; (d/index-range) on a homogeneously-typed bigdec key. Non-numerics are absent.
   {:db/ident :literal/num :db/valueType :db.type/bigdec
    :db/index true :db/cardinality :db.cardinality/one}])

;; ============================================================================
;; Mixed-term SPARQL ORDER BY (query-result ordering — NEVER an index comparator)
;; ============================================================================

(defn- literal-order-compare
  "Order two literals for ORDER BY: numeric literals numerically (by value), the
   rest by (lexical, datatype, lang). This is a RESULT comparator, so ordering two
   value-equal terms adjacently is fine — SPARQL leaves their relative order
   unspecified. It is NOT the index key, so it cannot collapse terms."
  [a b]
  (let [na (numeric-value a) nb (numeric-value b)]
    (if (and na nb)
      (compare na nb)
      (let [c (compare (:literal/lexical a) (:literal/lexical b))]
        (if-not (zero? c)
          c
          (let [c2 (compare (str (:literal/datatype a)) (str (:literal/datatype b)))]
            (if-not (zero? c2)
              c2
              (compare (:literal/lang a) (:literal/lang b)))))))))

(defn- term-rank [t]
  (cond (nil? t) 0
        (and (vector? t) (= :bnode (first t))) 1
        (and (vector? t) (= :iri (first t)))   2
        :else 3))

(defn rdf-term-compare
  "SPARQL ORDER BY total order over MIXED terms: unbound < bnode < IRI < literal
   (numeric-aware within literals). For ordering query RESULTS."
  [a b]
  (let [ra (term-rank a) rb (term-rank b)]
    (if (not= ra rb)
      (compare ra rb)
      (case ra
        0 0
        1 (compare (second a) (second b))
        2 (compare (second a) (second b))
        3 (literal-order-compare a b)))))

(defn term->nt
  "N-Triples-ish string form of any term (IRI/bnode/literal) — for display and for
   the string form of a reified term."
  [t]
  (cond
    (and (vector? t) (= :iri (first t)))   (str "<" (second t) ">")
    (and (vector? t) (= :bnode (first t))) (str "_:" (second t))
    (literal? t) (str "\"" (:literal/lexical t) "\""
                      (cond (lang t)                        (str "@" (lang t))
                            (not= (datatype t) xsd-string)  (str "^^" (datatype t))
                            :else ""))
    :else (str t)))

;; EDN print/read for the literal term map: #datahike.rdf/literal ["lex" :dt "lang"]
#?(:clj
   (defn literal-from-reader [[lx dt lg]]
     {:literal/lexical lx :literal/datatype dt :literal/lang (or lg "")}))

;; ============================================================================
;; IRI nodes
;; ============================================================================

(def iri-schema
  "An IRI is a unique entity. Reference it by `[:node/iri \"…\"]`."
  [{:db/ident :node/iri :db/valueType :db.type/string
    :db/unique :db.unique/identity :db/cardinality :db.cardinality/one}])

(defn iri-entity [term]
  {:node/iri (second term)})

(defn iri-ref [term]
  [:node/iri (second term)])

(defn- object-ref
  "Resolve an object term to what a `:db.type/ref` object should hold, and the
   entity that must exist for it. Returns [ref entity-or-nil]."
  [o]
  (cond
    (literal? o)                          [(literal-ref o) (literal-entity o)]
    (and (vector? o) (= :iri (first o)))  [(iri-ref o) (iri-entity o)]
    (and (vector? o) (= :bnode (first o))) [o nil]        ;; tempid/eid
    :else                                 [o nil]))

;; ============================================================================
;; Statement reification (RDF-star)
;; ============================================================================
;;
;; Two distinct things, kept separate:
;;
;;   THE GROUND TRIPLE is the real graph — `{subject-entity predicate object-ref}`
;;   datoms, typed and queryable, where the object is a ref to an interned literal
;;   or IRI entity.
;;
;;   THE STATEMENT is a REIFICATION — a handle for talking ABOUT the triple. Its
;;   identity is a canonical tuple over the three TERMS' N-Triples strings, so it is
;;   addressable by a static lookup-ref (resolvable from the terms alone, no db) and
;;   term-distinct: <<s p "9"^^xsd:integer>> and <<s p "09"^^xsd:integer>> are
;;   distinct statements, which is correct term-level RDF-star. (A composite tuple
;;   over the ref attributes would hold EIDS, so it could not be looked up from terms
;;   without first resolving them — hence the string identity here.)
;;
;; The object's TERM string form is stored, so `<<s p "9">>` is term-distinct; range
;; querying INSIDE a quoted triple is not supported (query the ground triple for
;; that). This is the one documented limitation of string-keyed reification.

(def statement-schema
  "RDF-star statement reification, plus the interned-term schema it references.
   Identity is the unique `:rdf/spo` tuple over the (subject, predicate, object)
   N-Triples strings. Annotations are application attributes added to the statement."
  (into literal-schema
        (into iri-schema
              [{:db/ident :rdf/s :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
               {:db/ident :rdf/p :db/valueType :db.type/keyword :db/cardinality :db.cardinality/one}
               {:db/ident :rdf/o :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
               {:db/ident        :rdf/spo
                :db/valueType    :db.type/tuple
                :db/tupleAttrs   [:rdf/s :rdf/p :rdf/o]
                :db/unique       :db.unique/identity
                :db/cardinality  :db.cardinality/one}])))

(defn- subject-ref
  "The ground-triple subject ref + the entity that must exist for it."
  [s]
  (cond
    (and (vector? s) (= :iri (first s)))   [(iri-ref s) (iri-entity s)]
    (and (vector? s) (= :bnode (first s))) [s nil]
    :else                                  [s nil]))

(defn- ground-triple
  "The real graph datom `{subject-entity predicate object-ref}`, plus the term
   entities that must exist. Returns a vector of tx-maps."
  [s p o]
  (let [[sref sent] (subject-ref s)
        [oref oent] (object-ref o)]
    (into [] (remove nil?) [sent oent {:db/id sref p oref}])))

(defn- reification
  "The statement entity `{:rdf/s :rdf/p :rdf/o :rdf/spo + annotations}`, keyed by the
   canonical term strings."
  [s p o annotations]
  (merge {:rdf/s (term->nt s) :rdf/p p :rdf/o (term->nt o)} annotations))

(defn statement-ref
  "Lookup-ref addressing the statement (s p o) — static, resolvable from the terms
   alone. Use as a `:db/id`."
  [s p o]
  [:rdf/spo [(term->nt s) p (term->nt o)]])

(defn quote-triple
  "tx-data reifying (s p o) as a statement entity WITHOUT asserting the ground triple
   — RDF-star's QUOTED triple. Re-quoting upserts to one statement (RDF set
   semantics)."
  [s p o]
  [(reification s p o nil)])

(defn annotate
  "tx-data that ASSERTS the ground triple (s p o) AND reifies it as an annotatable
   statement carrying `annotations` — SPARQL-star's annotation form `s p o {| … |}`:
   the triple is a fact, the statement a handle for talking about it.

   The ground triple asserts `{subject-entity predicate object}`, so the predicate
   keyword must be a schema attribute (a ref-typed one for IRI/bnode/literal objects).
   See the namespace doc on predicate-IRI mapping."
  [s p o annotations]
  (conj (ground-triple s p o)
        (reification s p o annotations)))

;; ============================================================================
;; Showcase
;; ============================================================================

#?(:clj
   (defn- with-mem-db [schema f]
     (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
                :schema-flexibility :write :keep-history? false
                :index :datahike.index/persistent-set}]
       (d/create-database cfg)
       (let [conn (d/connect cfg)]
         (try @(d/transact! conn schema) (f conn)
              (finally (d/release conn) (d/delete-database cfg)))))))

#?(:clj
   (defn demo
     "End-to-end showcase over a throwaway in-memory db: interned literals
      (term dedup + distinct terms for equal values + numeric range via :literal/num
      + LANG/STR), reified statements that assert the ground triple + annotate +
      dedup, and the mixed-term SPARQL order."
     []
     (let [schema (into statement-schema
                        [{:db/ident :rdfs/label :db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/many}
                         {:db/ident :ex/knows :db/valueType :db.type/ref
                          :db/cardinality :db.cardinality/many}
                         {:db/ident :meta/source :db/valueType :db.type/string
                          :db/cardinality :db.cardinality/one}])]
       (with-mem-db
         schema
         (fn [conn]
           ;; intern four literal terms and attach them to an IRI entity
           (let [labels [(literal "chat" {:lang "FR"})     ;; lang folds to @fr
                         (literal "cat"  {:lang "en"})
                         (literal "9"   {:datatype :xsd/integer})
                         (literal "42"  {:datatype :xsd/integer})]]
             @(d/transact! conn (mapv literal-entity labels))
             @(d/transact! conn [{:node/iri "ex:cat"
                                  :rdfs/label (mapv literal-ref labels)}]))
           ;; a statement: assert ex:alice ex:knows ex:bob, annotate its provenance
           (let [s (iri "ex:alice") p :ex/knows o (iri "ex:bob")]
             @(d/transact! conn (annotate s p o {:meta/source "crawler-1"}))
             (let [before (count (d/datoms @conn :eavt))]
               @(d/transact! conn (quote-triple s p o))   ;; re-quote: idempotent
               {:literals
                {:distinct-terms-for-equal-values
                 (mapv (comp :literal/lexical #(d/pull @conn [:literal/lexical] %))
                       (d/q '[:find [?e ...] :where [?e :literal/num 9M]] @conn))
                 :numeric-range-seek
                 (sort (d/q '[:find [?lx ...] :in $ :where
                              [?e :literal/num ?n] [(>= ?n 10M)]
                              [?e :literal/lexical ?lx]] @conn))
                 :lang-folded
                 (d/q '[:find ?lg . :where [?e :literal/lexical "chat"] [?e :literal/lang ?lg]] @conn)}
                :reification
                {:ground-triple-asserted?
                 (boolean (seq (d/q '[:find ?o :in $ ?s
                                      :where [?s :ex/knows ?o]] @conn (iri-ref s))))
                 :statement-annotated
                 (:meta/source (d/pull @conn [:meta/source] (statement-ref s p o)))
                 :requote-idempotent? (= before (count (d/datoms @conn :eavt)))}
                :mixed-term-order
                (mapv term->nt (sort rdf-term-compare
                                     [(literal "10" {:datatype :xsd/integer})
                                      (literal "9" {:datatype :xsd/integer})
                                      (iri "ex:z") (bnode "b1") nil]))})))))))

(comment
  (require '[datahike.experimental.rdf :as rdf])
  (rdf/demo))
