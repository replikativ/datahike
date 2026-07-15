(ns datahike.test.rdf-test
  "RDF / RDF-star on datahike via interned-literal entities (option C).

   The load-bearing tests are `literal-term-identity-*`: the canonical term id is
   ON-DISK IDENTITY, so its four canonicalization rules are irreversible and pinned
   here exactly as the store-ref root-set rule is. The native-value-type approach
   these replace had a real bug — its comparator collapsed `\"1\"^^xsd:integer` and
   `\"01\"^^xsd:integer` into one AVET slot (compareTo 0, equals ≠), losing datoms
   and miscounting. Interning makes that collapse structurally impossible; the tests
   below prove it."
  (:require [clojure.test :refer [deftest is testing]]
            [datahike.api :as d]
            [datahike.experimental.rdf :as rdf]))

(defn- mem-conn [schema]
  (let [cfg {:store {:backend :memory :id (java.util.UUID/randomUUID)}
             :schema-flexibility :write :keep-history? false
             :index :datahike.index/persistent-set}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (d/transact conn schema)
      conn)))

;; ---------------------------------------------------------------------------
;; THE IRREVERSIBLE PART — the four canonicalization rules of the term id.
;; Each becomes on-disk identity, so a wrong rule corrupts data that already exists.

(deftest literal-term-identity-distinct-terms
  (testing "distinct terms get distinct ids, even when their VALUES are equal"
    ;; This is the exact case the native value type collapsed.
    (is (not= (rdf/literal-term-id (rdf/literal "1" {:datatype :xsd/integer}))
              (rdf/literal-term-id (rdf/literal "01" {:datatype :xsd/integer})))
        "\"1\" and \"01\" are distinct TERMS (equal values) → distinct ids")
    (is (not= (rdf/literal-term-id (rdf/literal "1" {:datatype :xsd/integer}))
              (rdf/literal-term-id (rdf/literal "1" {:datatype :xsd/long})))
        "same lexical, different datatype → distinct terms")))

(deftest literal-term-identity-same-terms
  (testing "same term → same id (interning / dedup by construction)"
    (is (= (rdf/literal-term-id (rdf/literal "cat"))
           (rdf/literal-term-id (rdf/literal "cat")))))
  (testing "plain literal is xsd:string (RDF 1.1 §3.3)"
    (is (= (rdf/literal-term-id (rdf/literal "cat"))
           (rdf/literal-term-id (rdf/literal "cat" {:datatype :xsd/string})))
        "\"cat\" and \"cat\"^^xsd:string are ONE term"))
  (testing "language tags are case-insensitive"
    (is (= (rdf/literal-term-id (rdf/literal "chat" {:lang "FR"}))
           (rdf/literal-term-id (rdf/literal "chat" {:lang "fr"})))
        "@FR and @fr are ONE term")))

(deftest literal-lexical-preserved-verbatim
  (testing "the lexical form is never canonicalized on write"
    (is (= "1.0E2" (rdf/lex (rdf/literal "1.0E2" {:datatype :xsd/double})))
        "value canonicalization is a query-time concern, not stored")
    (is (not= (rdf/literal-term-id (rdf/literal "1.0E2" {:datatype :xsd/double}))
              (rdf/literal-term-id (rdf/literal "100" {:datatype :xsd/double})))
        "equal doubles, distinct lexical forms → distinct terms")))

;; ---------------------------------------------------------------------------
;; Interning in the store — the collapse is structurally impossible.

(deftest interning-dedups-and-keeps-distinct-terms
  (let [conn (mem-conn rdf/literal-schema)]
    (testing "the same term interned twice is ONE entity"
      (let [t (rdf/literal "cat" {:lang "en"})]
        (d/transact conn [(rdf/literal-entity t)])
        (d/transact conn [(rdf/literal-entity t)])   ;; re-intern
        (is (= 1 (count (d/q '[:find ?e :where [?e :literal/lexical "cat"]] @conn))))))
    (testing "value-equal but term-distinct literals are TWO entities — no collapse"
      ;; the native value type dropped one of these; here both survive.
      (d/transact conn [(rdf/literal-entity (rdf/literal "1" {:datatype :xsd/integer}))
                        (rdf/literal-entity (rdf/literal "01" {:datatype :xsd/integer}))])
      (is (= 2 (count (d/q '[:find ?e :where [?e :literal/num 1M]] @conn)))
          "both \"1\" and \"01\" are stored; COUNT is correct"))
    (d/release conn)))

(deftest numeric-range-is-an-index-seek
  (testing ":literal/num gives index-native numeric range without a value type"
    (let [conn (mem-conn rdf/literal-schema)]
      (d/transact conn (mapv #(rdf/literal-entity (rdf/literal (str %) {:datatype :xsd/integer}))
                             [1 9 10 42 100]))
      (is (= [10M 42M 100M]
             (sort (d/q '[:find [?n ...] :where [_ :literal/num ?n] [(>= ?n 10M)]] @conn)))
          "FILTER(?x >= 10) is a range over the homogeneous bigdec shadow key")
      ;; and index-range on the indexed attribute seeks rather than scans
      (is (= 2 (count (d/index-range @conn {:attrid :literal/num :start 40M :end 200M})))
          "40..200 → {42, 100}")
      (d/release conn))))

;; ---------------------------------------------------------------------------
;; RDF-star reification: ground triple asserted, statement addressable, idempotent.

(deftest reification-asserts-ground-and-annotates
  (let [conn (mem-conn (into rdf/statement-schema
                             [{:db/ident :ex/knows :db/valueType :db.type/ref
                               :db/cardinality :db.cardinality/many}
                              {:db/ident :meta/source :db/valueType :db.type/string
                               :db/cardinality :db.cardinality/one}]))
        s (rdf/iri "ex:alice") p :ex/knows o (rdf/iri "ex:bob")]
    (d/transact conn (rdf/annotate s p o {:meta/source "crawler-1"}))
    (testing "the GROUND triple is asserted — queryable as real data"
      (is (seq (d/q '[:find ?o :in $ ?s :where [?s :ex/knows ?o]] @conn (rdf/iri-ref s)))
          "after annotate, {s :ex/knows ?o} returns the object (was empty in the old design)"))
    (testing "the statement is addressable by a static lookup-ref and carries the annotation"
      (is (= "crawler-1" (:meta/source (d/pull @conn [:meta/source] (rdf/statement-ref s p o))))))
    (testing "re-quoting the same triple is idempotent (RDF set semantics)"
      (let [before (count (d/datoms @conn :eavt))]
        (d/transact conn (rdf/quote-triple s p o))
        (is (= before (count (d/datoms @conn :eavt))))))
    (d/release conn)))

(deftest reified-object-is-term-distinct
  (testing "<<s p \"9\">> and <<s p \"09\">> are distinct statements (term-level RDF-star)"
    (let [conn (mem-conn rdf/statement-schema)
          s (rdf/iri "ex:x") p :ex/v]
      (d/transact conn (rdf/quote-triple s p (rdf/literal "9" {:datatype :xsd/integer})))
      (d/transact conn (rdf/quote-triple s p (rdf/literal "09" {:datatype :xsd/integer})))
      (is (= 2 (count (d/q '[:find ?e :where [?e :rdf/spo _]] @conn)))
          "two distinct quoted statements")
      (d/release conn))))

;; ---------------------------------------------------------------------------
;; Query-side ordering (never an index comparator).

(deftest mixed-term-order-is-sparql
  (testing "unbound < bnode < IRI < literal, numeric-aware within literals"
    (is (= ["" "_:b" "<ex:z>" "\"9\"^^:xsd/integer" "\"10\"^^:xsd/integer"]
           (mapv rdf/term->nt
                 (sort rdf/rdf-term-compare
                       [(rdf/literal "10" {:datatype :xsd/integer})
                        (rdf/literal "9" {:datatype :xsd/integer})
                        (rdf/iri "ex:z") (rdf/bnode "b") nil])))
        "9 before 10 numerically, not lexically")))

(deftest demo-runs
  (testing "the end-to-end showcase runs and reports the expected shape"
    (let [r (rdf/demo)]
      (is (= true (get-in r [:reification :ground-triple-asserted?])))
      (is (= "crawler-1" (get-in r [:reification :statement-annotated])))
      (is (= true (get-in r [:reification :requote-idempotent?])))
      (is (= "fr" (get-in r [:literals :lang-folded]))))))
