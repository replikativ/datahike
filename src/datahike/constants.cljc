(ns ^:no-doc datahike.constants)

(def ^:const e0 0x00000000)
(def ^:const tx0 0x20000000)
(def ^:const emax 0x7FFFFFFF)
(def ^:const txmax 0x7FFFFFFF)

(def ^:const system-schema
  [{:db/id        tx0
    :db/txInstant #inst"1970-01-01T00:00:00.000-00:00"}
   {:db/id 1
    :db/ident :db/ident
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's or specification's identifier"
    :db/unique :db.unique/value}
   {:db/id 2
    :db/ident :db/valueType
    :db/valueType :db.type/valueType
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's value type"}
   {:db/id 3
    :db/ident :db/cardinality
    :db/valueType :db.type/cardinality
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's cardinality"}
   {:db/id 4
    :db/ident :db/doc
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's documentation"}
   {:db/id 5
    :db/ident :db/index
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's index selection"}
   {:db/id 6
    :db/ident :db/unique
    :db/valueType :db.type/unique
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's unique selection"}
   {:db/id 7
    :db/ident :db/noHistory
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc "An attribute's history selection"}
   {:db/id 8
    :db/ident :db.install/attribute
    :db/valueType :db.type.install/attribute
    :db/cardinality :db.cardinality/one
    :db/doc "Only for interoperability with Datomic"}
   {:db/id 9
    :db/ident :db/txInstant
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc "A transaction's time-point"
    :db/noHistory true
    :db/index true}
   {:db/id 10
    :db/ident :db.cardinality/many}
   {:db/id 11
    :db/ident :db.cardinality/one}
   {:db/id 12
    :db/ident :db.part/sys}
   {:db/id 13
    :db/ident :db.part/tx}
   {:db/id 14
    :db/ident :db.part/user}
   {:db/id 15
    :db/ident :db.type/bigdec}
   {:db/id 16
    :db/ident :db.type/bigint}
   {:db/id 17
    :db/ident :db.type/boolean}
   {:db/id 18
    :db/ident :db.type/double}
   {:db/id 19
    :db/ident :db.type/cardinality}
   {:db/id 20
    :db/ident :db.type/float}
   {:db/id 21
    :db/ident :db.type/number}
   {:db/id 22
    :db/ident :db.type/instant}
   {:db/id 23
    :db/ident :db.type/keyword}
   {:db/id 24
    :db/ident :db.type/long}
   {:db/id 25
    :db/ident :db.type/ref}
   {:db/id 26
    :db/ident :db.type/string}
   {:db/id 27
    :db/ident :db.type/symbol}
   {:db/id 28
    :db/ident :db.type/unique}
   {:db/id 29
    :db/ident :db.type/uuid}
   {:db/id 30
    :db/ident :db.type/valueType}
   {:db/id 31
    :db/ident :db.type.install/attribute}
   {:db/id 32
    :db/ident :db.unique/identity}
   {:db/id 33
    :db/ident :db.unique/value}
   {:db/id 34
    :db/ident :db/isComponent}
   {:db/id 35
    :db/ident :db/tupleType}
   {:db/id 36
    :db/ident :db/tupleTypes}
   {:db/id 37
    :db/ident :db/tupleAttrs}
   {:db/id 38
    :db/ident :db.type/tuple}
   ;; Bitemporal valid-time tx-meta — graduated from kontor-side
   ;; userland into system schema. Tx-attached, half-open
   ;; [from, to). Both AVET-indexed so the query planner can seek
   ;; into them via the `valid-at` / `valid-between` rule rewrites.
   {:db/id 39
    :db/ident :db.valid/from
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc "A transaction's valid-time lower bound (inclusive).
             Every datom in the tx inherits this vt-from. When
             absent the transaction's `:db/txInstant` is used.
             See `doc/valid_time.md` for usage."
    :db/index true}
   {:db/id 40
    :db/ident :db.valid/to
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc "A transaction's valid-time upper bound (exclusive).
             When absent the interval is open-ended (treated as +∞).
             May be written retroactively on a prior tx-entity to
             close that tx's window; the write is a normal commit,
             the prior tx's hash is unchanged, and the transactor
             enforces a cross-tx `vf < vt` check on the combined
             state. See `doc/valid_time.md` for usage."
    :db/index true}
   ;; Secondary-index schema attrs — graduated into the system schema so the
   ;; whole `:db.secondary/*` family has stable entity IDs that align across
   ;; attribute-refs databases (mirrors how `:db.valid/*` was added). Their
   ;; value-types live in `schema/implicit-schema-spec`, like the other
   ;; schema-meta flag attrs (`:db/index`, `:db/isComponent`, …).
   {:db/id 41
    :db/ident :db.secondary/type}
   {:db/id 42
    :db/ident :db.secondary/attrs}
   {:db/id 43
    :db/ident :db.secondary/config}
   {:db/id 44
    :db/ident :db.secondary/status}
   {:db/id 45
    :db/ident :db.secondary/building-since-tx}
   {:db/id 46
    :db/ident :db.secondary/only}
   ;; Cross-database reference attrs (datahike.reference) — graduated into
   ;; the system schema like `:db.valid/*`, so reified `dh://` references
   ;; are transactable on any database without a user schema declaration.
   ;; Plain USER-DATA attrs (not schema-meta): deliberately NOT part of the
   ;; `schema?` datom classification in db.transaction. `:dh.ref/db` and
   ;; `:dh.ref/value` are AVET-indexed for reverse lookups ("all references
   ;; into database X / to value V"). See `doc/cross-db-references.md`.
   {:db/id 47
    :db/ident :dh.ref/db
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/doc "Cross-db reference: target database (store :id)"
    :db/index true}
   {:db/id 48
    :db/ident :dh.ref/attr
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Cross-db reference: unique attribute of the target lookup ref"}
   {:db/id 49
    :db/ident :dh.ref/value
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Cross-db reference: lookup-ref value, canonically encoded"
    :db/index true}
   {:db/id 50
    :db/ident :dh.ref/temporal
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "Cross-db reference: temporal qualifier (tx:/date:/valid:/branch:); absent = live head"}
   {:db/id 51
    :db/ident :dh.ref/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "Cross-db reference: link predicate (application vocabulary)"}])

(def ^:const system-entities
  "Holds the entity IDs of system attributes"
  (set (reduce
        (fn [m {:keys [db/ident db/id] :as attr}]
          (when ident (conj m id)))
        []
        system-schema)))

(def ^:const non-ref-implicit-schema
  ;; Pre-existing system attrs whose schema is "implicit" in non-attribute-
  ;; refs mode (i.e. they're not in the user-installed schema, but the
  ;; transactor + query planner must know their properties). Historically
  ;; only carried the four entries below — but `:db/txInstant` was missing
  ;; `:db/index true` even though it IS indexed both in `ref-implicit-schema`
  ;; and in `implicit-schema-spec` (schema.cljc). The `:db/index` omission
  ;; meant `(dbu/indexing? db :db/txInstant)` returned false, which silently
  ;; broke the planner's AVET pushdown for predicates pivoting on these
  ;; attrs (e.g. `[(<= ?vf ?at)]` after `[?tx :db.valid/from ?vf]` produced
  ;; 0 results because the pushdown bounds were computed but the seek path
  ;; treated the attr as non-indexed).
  ;;
  ;; The fix: declare the `:db/index true` flag here too so rschema is
  ;; consistent across modes. Bitemporal-v1 also adds `:db.valid/from` and
  ;; `:db.valid/to`, which join `:db/txInstant` as AVET-indexed tx-meta.
  ;; Note: `:db/txInstant` is deliberately NOT flagged `:db/index true`
  ;; here even though `system-schema` (above) and `implicit-schema-spec`
  ;; (schema.cljc) declare it as such. Flipping it on would land every
  ;; tx's `:db/txInstant` datom in AVET — a semantic change that ripples
  ;; through existing tests + breaks the existing `:db/txInstant`-by-tx
  ;; lookup path. Bitemporal v1 keeps that as-is and only graduates
  ;; `:db.valid/from` / `:db.valid/to`, which are the new tx-meta attrs
  ;; the planner needs to AVET-seek.
  {:db/ident      {:db/unique :db.unique/identity}
   :db/txInstant  {:db/noHistory true}
   :db.valid/from {:db/index true}
   :db.valid/to   {:db/index true}
   :dh.ref/db     {:db/index true}
   :dh.ref/value  {:db/index true}
   :db.entity/attrs {:db/cardinality :db.cardinality/many}
   :db.entity/preds {:db/cardinality :db.cardinality/many}})

(def ^:const ref-implicit-schema
  "Maps attribute names to the attribute's specification"
  (reduce
   (fn [m {:keys [db/ident] :as attr}]
     (when ident
       (assoc m ident (dissoc attr :db/ident))))
   {}
   system-schema))

(def ^:const ue0 (transduce (comp (map :db/id) (remove #{tx0})) max 0 system-schema))
(def ^:const utx0 tx0)
