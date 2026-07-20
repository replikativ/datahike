(ns ^:no-doc datahike.schema
  (:require [clojure.spec.alpha :as s]
            [datahike.datom]
            ;; cljs bigdec values are fress `Bigdec` (unscaled js/BigInt + scale) —
            ;; the same type konserve round-trips via the Fressian BIGDEC (0xC7)
            ;; handlers. Recognise it so :db.type/bigdec accepts a real decimal in
            ;; the browser instead of rejecting everything (the previous
            ;; `(complement any?)` placeholder). Arrives transitively via konserve
            ;; (≥ the release carrying fress 0.4.317), like `fress.api` already does.
            #?(:cljs [fress.impl.bigdec :as fbd]))
  #?(:clj (:import [datahike.datom Datom])))

(s/def :db.type/id #?(:clj #(or (= (class %) java.lang.Long) string?)
                      :cljs #(or (and (number? %)
                                      (js/Number.isSafeInteger %))
                                 string?)))

;; db types
(s/def :db.type/bigdec #?(:cljs fbd/bigdec?
                          :clj decimal?))
(s/def :db.type/bigint #?(:clj integer?
                          :cljs #(or (integer? %)
                                     (= js/BigInt (type %)))))
(s/def :db.type/boolean boolean?)
(s/def :db.type/bytes #?(:clj bytes?
                         :cljs #(and (some->> (.-buffer %) (instance? js/ArrayBuffer))
                                     (some->> (.-byteLength %) (number?)))))
(s/def :db.type/double double?)
(s/def :db.type/float float?)
(s/def :db.type/number number?)
(s/def :db.type/instant #?(:clj #(= (class %) java.util.Date)
                           :cljs inst?))
(s/def :db.type/keyword keyword?)
(s/def :db.type/long #?(:clj #(= (class %) java.lang.Long)
                        :cljs #(js/Number.isSafeInteger %)))
(s/def :db.type/ref :db.type/id)
(s/def :db.type/string string?)
(s/def :db.type/symbol symbol?)
(s/def :db.type/uuid uuid?)
(s/def :db.type/tuple vector?)

(def builtin-value-types
  #{:db.type/bigdec
    :db.type/bigint
    :db.type/boolean
    :db.type/bytes
    :db.type/double
    :db.type/float
    :db.type/number
    :db.type/instant
    :db.type/keyword
    :db.type/long
    :db.type/ref
    :db.type/string
    :db.type/symbol
    :db.type/uuid
    :db.type/store-ref
    :db.type/value
    :db.type/tuple
    :db.type/cardinality
    :db.type.install/attribute
    :db.type/valueType
    :db.type/unique})

(s/def :db.type/value builtin-value-types)

;; :db.type/store-ref — a UUID that NAMES AN OBJECT: a blob, an out-of-line payload.
;; The ONE thing it adds over :db.type/uuid is that the garbage collector MARKS it,
;; so the object it names is known to be live (`key-bearing-value-types` below).
;;
;; It deliberately does NOT say where the bytes are. They may be in this database's
;; konserve store — then `gc-storage!` spares and reclaims them for you — or in a raw
;; S3 prefix a browser uploads to with a presigned URL, in which case
;; `datahike.gc/reachable-store-refs` hands you the live set and you sweep it. Same
;; type, same mark, two deployments.
;;
;; The id must PIN THE CONTENT (`datahike.blob/blob-id`). A store-ref is dereferenced
;; when you read it — including `as-of` an old transaction — so a mutable pointer
;; would hand an old read whatever is behind it NOW. See doc/store-refs.md.
;;
;; NOT for structured data. A store-ref is for bytes with no queryable structure — a
;; PDF, an image, model weights. If you would ever filter or join on something INSIDE
;; the value it is a document, not a blob: transact it as datoms
;; (`datahike.experimental.unstructured`) or index it with a secondary index. Datoms
;; are already sparse, so you never had to declare your fields — blobbing structured
;; data buys no flexibility you did not already have, and costs you the indices.
(s/def :db.type/store-ref uuid?)

(def key-bearing-value-types
  "Value types whose values NAME AN OBJECT in a store, so the collector must mark
   them or the sweep deletes objects a datom still references.

   The GC mark scans the datoms of attributes declared with one of these — and ONLY
   those, so a database that uses none pays nothing (`datahike.gc/store-refs`). For
   every type here the VALUE IS THE KEY; nothing more elaborate has been needed."
  #{:db.type/store-ref})

;; TODO: add bytes

(s/def :db.type/cardinality #{:db.cardinality/one :db.cardinality/many})
(s/def :db.type/unique #{:db.unique/identity :db.unique/value})

;; only for old datomic compliance, will be part of partioning in the future
(s/def :db.type.install/_attribute #{:db.part/tx :db.part/db :db.part/user})

(s/def ::schema-attribute #{:db/id :db/ident :db/isComponent :db/noHistory :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute :db/doc :db/tupleAttrs  :db/tupleType :db/tupleTypes :db.secondary/only :db/maxLength :db.attr/preds})

(s/def ::secondary-index-attribute #{:db.secondary/type :db.secondary/attrs :db.secondary/config :db.secondary/status :db.secondary/building-since-tx})

(s/def ::entity-spec-attribute #{:db/ensure :db.entity/attrs :db.entity/preds})
;; ADR — bitemporal v1: :db.valid/from and :db.valid/to graduate from
;; userland tx-meta into datahike system schema. They are tx-attached
;; (every datom in a tx inherits its tx's vt window) and AVET-indexed
;; so the query planner can seek into them.
(s/def ::meta-attribute #{:db/txInstant :db/retracted :db/noCommit
                          :db.valid/from :db.valid/to})

(s/def ::schema (s/keys :req [:db/ident :db/valueType :db/cardinality]
                        :opt [:db/id :db/unique :db/index :db.install/_attribute :db/doc :db/noHistory :db/tupleType :db/tupleTypes :db.secondary/only :db/maxLength :db.attr/preds]))

(s/def ::entity-spec (s/keys :opt [:db.entity/attrs :db.entity/preds]))

(s/def ::enum (s/keys :req [:db/ident]))

(def required-keys #{:db/ident :db/valueType :db/cardinality})

(s/def :db.secondary/status-type #{:building :ready :disabled})
(s/def :db.type/any any?)

(def ^:const implicit-schema-spec {:dh.ref/db {:db/valueType   :db.type/uuid
                                               :db/cardinality :db.cardinality/one
                                               :db/index       true}
                                   :dh.ref/attr {:db/valueType   :db.type/keyword
                                                 :db/cardinality :db.cardinality/one}
                                   :dh.ref/value {:db/valueType   :db.type/string
                                                  :db/cardinality :db.cardinality/one
                                                  :db/index       true}
                                   :dh.ref/temporal {:db/valueType   :db.type/string
                                                     :db/cardinality :db.cardinality/one}
                                   :dh.ref/type {:db/valueType   :db.type/keyword
                                                 :db/cardinality :db.cardinality/one}
                                   :db.secondary/type {:db/valueType   :db.type/keyword
                                                       :db/cardinality :db.cardinality/one}
                                   :db.secondary/attrs {:db/valueType   :db.type/tuple
                                                        :db/cardinality :db.cardinality/one}
                                   :db.secondary/config {:db/valueType   :db.type/any
                                                         :db/cardinality :db.cardinality/one}
                                   :db.secondary/status {:db/valueType   :db.type/keyword
                                                         :db/cardinality :db.cardinality/one}
                                   :db.secondary/building-since-tx {:db/valueType   :db.type/long
                                                                    :db/cardinality :db.cardinality/one}
                                   :db/ident {:db/valueType   :db.type/keyword
                                              :db/unique      :db.unique/identity
                                              :db/cardinality :db.cardinality/one}
                                   :db/valueType {:db/valueType   :db.type/value
                                                  :db/unique      :db.unique/identity
                                                  :db/cardinality :db.cardinality/one}
                                   :db/id {:db/valueType   :db.type/id
                                           :db/unique      :db.unique/identity
                                           :db/cardinality :db.cardinality/one}
                                   :db/cardinality {:db/valueType   :db.type/cardinality
                                                    :db/unique      :db.unique/identity
                                                    :db/cardinality :db.cardinality/one}
                                   :db/index {:db/valueType   :db.type/boolean
                                              :db/unique      :db.unique/identity
                                              :db/cardinality :db.cardinality/one}
                                   :db/unique {:db/valueType   :db.type/unique
                                               :db/unique      :db.unique/identity
                                               :db/cardinality :db.cardinality/one}
                                   :db/isComponent {:db/valueType :db.type/boolean
                                                    :db/unique :db.unique/identity
                                                    :db/cardinality :db.cardinality/one}
                                   :db/noHistory {:db/valueType :db.type/boolean
                                                  :db/unique :db.unique/identity
                                                  :db/cardinality :db.cardinality/one}
                                   :db.secondary/only {:db/valueType :db.type/boolean
                                                       :db/cardinality :db.cardinality/one}
                                   ;; Attribute-value constraints (opt-in). :db/maxLength is a
                                   ;; declarative string-length bound; :db.attr/preds names value
                                   ;; predicates (registry keywords or, on clj, resolvable symbols),
                                   ;; many-valued. Both enforced on assertion in transact-add.
                                   :db/maxLength {:db/valueType :db.type/long
                                                  :db/cardinality :db.cardinality/one}
                                   ;; symbol-typed (like :db.entity/preds) so many-valued
                                   ;; preds stay homogeneous — the datom index compares
                                   ;; values directly and can't order keyword-vs-symbol.
                                   :db.attr/preds {:db/valueType :db.type/symbol
                                                   :db/cardinality :db.cardinality/many}
                                   :db/txInstant {:db/valueType :db.type/instant
                                                  :db/unique :db.unique/identity
                                                  :db/index true
                                                  :db/cardinality :db.cardinality/one}
                                   ;; Bitemporal valid-time tx-meta — see ::meta-attribute above.
                                   ;; Half-open interval [from, to). Both AVET-indexed.
                                   :db.valid/from {:db/valueType :db.type/instant
                                                   :db/index true
                                                   :db/cardinality :db.cardinality/one}
                                   :db.valid/to   {:db/valueType :db.type/instant
                                                   :db/index true
                                                   :db/cardinality :db.cardinality/one}
                                   :db/noCommit  {:db/valueType :db.type/boolean
                                                  :db/unique :db.unique/identity
                                                  :db/cardinality :db.cardinality/one}
                                   :db/retracted {:db/valueType :db.type/long
                                                  :db/unique :db.unique/identity
                                                  :db/cardinality :db.cardinality/many}
                                   :db/ensure {:db/valueType :db.type/keyword
                                               :db/cardinality :db.cardinality/one}
                                   :db.entity/attrs {:db/valueType :db.type/keyword
                                                     :db/cardinality :db.cardinality/many}
                                   :db.entity/preds {:db/valueType :db.type/symbol
                                                     :db/cardinality :db.cardinality/many}
                                   :db/doc {:db/valueType :db.type/string
                                            :db/index true
                                            :db/cardinality :db.cardinality/one}
                                   :db.install/_attribute {:db/valueType   :db.type.install/_attribute
                                                           :db/unique      :db.unique/identity
                                                           :db/cardinality :db.cardinality/one}
                                   :db/tupleType {:db/valueType :db.type/value
                                                  :db/cardinality :db.cardinality/one}
                                   :db/tupleTypes {:db/valueType :db.type/tuple
                                                   :db/cardinality :db.cardinality/one}
                                   :db/tupleAttrs {:db/valueType :db.type/tuple
                                                   :db/cardinality :db.cardinality/one}})
(s/def :db/helpers #{:db.install/attribute :db})
(s/def :db.part/types #{:db.part/tx :db.part/sys :db.part/user})

(s/def :db.meta/attributes #{:db/txInstant :db.valid/from :db.valid/to})

(s/def ::sys-idents (s/or :value :db.type/value
                          :cardinality :db.type/cardinality
                          :parts :db.part/types
                          :helpers :db/helpers
                          :meta :db.meta/attributes
                          :unique :db.type/unique))

(def schema-keys #{:db/ident :db/isComponent :db/noHistory :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute :db/doc :db/tupleType :db/tupleTypes :db/tupleAttrs
                   :db.secondary/type :db.secondary/attrs :db.secondary/config :db.secondary/status :db.secondary/building-since-tx
                   :db.secondary/only :db/maxLength :db.attr/preds})

(s/def ::old-schema-val (s/keys :req [:db/valueType :db/cardinality]
                                :opt [:db/ident :db/unique :db/index :db.install/_attribute :db/doc :db/noHistory :db/maxLength :db.attr/preds]))

(s/def ::old-schema-key keyword?)

(s/def ::old-schema (s/nilable (s/map-of ::old-schema-key ::old-schema-val)))

(defn old-schema-valid? [schema]
  (s/valid? ::old-schema schema))

(defn explain-old-schema [schema]
  (s/explain-data ::old-schema schema))

(defn meta-attr? [a-ident]
  (s/valid? ::meta-attribute a-ident))

(defn schema-attr? [a-ident]
  (s/valid? ::schema-attribute a-ident))

(defn sys-ident? [a-ident]
  (s/valid? ::sys-idents a-ident))

(defn entity-spec-attr? [a-ident]
  (s/valid? ::entity-spec-attribute a-ident))

(defn secondary-index-attr? [a-ident]
  (s/valid? ::secondary-index-attribute a-ident))

;; Cross-database reference attrs (datahike.reference) — system-installed
;; USER-DATA attrs: transactable without user schema (see the
;; schema-flexibility gate in db.utils), type-validated against
;; implicit-schema-spec, but NOT schema-entities (no `schema?` datom
;; classification).
(s/def ::reference-attribute #{:dh.ref/db :dh.ref/attr :dh.ref/value
                               :dh.ref/temporal :dh.ref/type})

(defn reference-attr? [a-ident]
  (s/valid? ::reference-attribute a-ident))

(defn value-valid? [a-ident v-ident schema]
  (let [schema (if (or (meta-attr? a-ident) (schema-attr? a-ident) (entity-spec-attr? a-ident)
                       (secondary-index-attr? a-ident) (reference-attr? a-ident))
                 implicit-schema-spec
                 schema)
        value-type (get-in schema [a-ident :db/valueType])]
    (s/valid? value-type v-ident)))

(defn instant? [db ^Datom datom schema]
  (let [a-ident (if (:attribute-refs? (:config db))
                  ((:ref-ident-map db) (.-a datom))
                  (.-a datom))
        schema (if (or (meta-attr? a-ident) (schema-attr? a-ident))
                 implicit-schema-spec
                 schema)]
    (= (get-in schema [a-ident :db/valueType]) :db.type/instant)))

(defn schema-entity? [entity]
  (some #(contains? entity %) schema-keys))

(defn secondary-index-entity? [entity]
  (contains? entity :db.secondary/type))

(defn schema? [schema]
  (s/valid? ::schema schema))

(defn describe-type [schema-type]
  ;; The "Bad entity value ..." error shows the user the enumeration of legal value
  ;; types. `:db.type/value` is now defined by reference to `builtin-value-types`, so
  ;; `s/describe` renders that var rather than the alternatives — build the enum here.
  (if (= schema-type :db.type/value)
    (into (sorted-set) builtin-value-types)
    (s/describe schema-type)))

(defn key-bearing-misuse
  "Reasons this schema entity would let the collector delete live data, or nil.

   A key-bearing value type (`:db.type/store-ref`) NAMES an object in the store. The
   collector keeps such an object alive by scanning the datoms of attributes declared
   with that type. Two shapes defeat that scan, both silently — so they are rejected
   rather than documented:

   TUPLES. A `:db/tupleType` / `:db/tupleTypes` value is a vector on a
   `:db.type/tuple` attribute. The attribute's OWN valueType is the builtin tuple, so
   the mark scans by valueType and the keys nested inside the vector are invisible to
   it: the objects get swept while the datom still names them.

   :db/noHistory. Retracted values of a `:db/noHistory` attribute are not retained
   in the temporal indices. Under `:keep-history? true` an `as-of` read can still
   reach the retracted datom, but the object it names has been collected — a
   dangling reference in history."
  [{:keys [db/valueType db/tupleType db/tupleTypes db/noHistory] :as _entity}]
  (let [key-bearing? (fn [t] (contains? key-bearing-value-types t))
        nested       (cond-> (set tupleTypes)
                       tupleType (conj tupleType))]
    (cond
      (some key-bearing? nested)
      (str "a tuple cannot hold " (pr-str (first (filter key-bearing? nested)))
           ": the collector marks store-naming values by attribute valueType, and a"
           " tuple's valueType is :db.type/tuple — the keys inside it would be"
           " invisible to the mark and their objects would be swept while still"
           " referenced. Give the reference its own attribute.")

      (and noHistory (key-bearing? valueType))
      (str ":db/noHistory cannot be combined with " (pr-str valueType)
           ": retracted values are not retained in the temporal indices, so under"
           " :keep-history? the object a retracted reference names would be"
           " collected while an as-of read can still reach the datom naming it."))))

(defn find-invalid-schema-updates [entity attr-schema]
  (reduce-kv
   (fn [m attr-def new-value]
     (let [old-value (get-in attr-schema [attr-def])]
       (when (not= old-value new-value)
         (case attr-def
           :db/cardinality
           ;; Prohibit update from :db.cardinality/one to :db.cardinality/many, if there is a :db/unique constraint.
           (when (and (= new-value :db.cardinality/many)
                      (#{:db.unique/value :db.unique/identity} (:db/unique attr-schema)))
             (assoc m attr-def [old-value new-value]))

           :db/unique
           (when (or (not (:db/unique attr-schema))
                     (not= :db.cardinality/one (:db/cardinality attr-schema)))
             (assoc m attr-def [old-value new-value]))

           ;; Always allow these attributes to be updated.
           :db/doc nil
           :db/noHistory nil
           :db/isComponent nil

           ;; Attribute-value constraints may be added or changed on an
           ;; existing attribute; they only affect future assertions.
           :db/maxLength nil
           :db.attr/preds nil

           ;; Secondary index: monotonic status transitions only
           ;; :building → :ready → :disabled (no going back)
           :db.secondary/status
           (let [valid-transitions {:building #{:ready :disabled}
                                    :ready    #{:disabled}}]
             (when-not (contains? (get valid-transitions old-value) new-value)
               (assoc m attr-def [old-value new-value])))
           :db.secondary/building-since-tx nil

           (assoc m attr-def [old-value new-value])))))
   {}
   (dissoc entity :db/id)))

(def attr-defining-keys
  "Schema keys that make an entry define an ATTRIBUTE (as opposed to an enum
   ident, a doc-only entity, or an entity-spec). An entry carrying any of
   these must be complete (:db/valueType + :db/cardinality) under
   :schema-flexibility :write."
  #{:db/valueType :db/cardinality :db/unique :db/index :db/noHistory
    :db/tupleAttrs :db/tupleType :db/tupleTypes :db/isComponent})

(def ^:private always-allowed-schema-keys
  "Schema-entry keys whose change never needs a transition rule or data scan.
   The value-constraint keys (:db/maxLength, :db.attr/preds) belong here with
   their sibling :db.entity/preds: they are enforced on assertion only, so
   adding or changing one never invalidates existing (or history) datoms —
   it is a valid post-hoc addition to an already-defined attribute."
  #{:db/doc :db/isComponent :db/noHistory :db/ident
    :db/maxLength :db.attr/preds
    :db.entity/attrs :db.entity/preds :db.secondary/building-since-tx})

(defn assess-schema-transition
  "Assess a schema entry TRANSITION on the RESULTING state: `old-entry` is the
   entry before the transaction, `new-entry` after it (nil = entry removed).
   Pure and data-independent — the caller performs the returned data checks
   against the database.

   This is the state-based successor of `find-invalid-schema-updates`, which
   compares a transacted DELTA against the pre-tx schema and therefore only
   guards the entity-map transaction path in the order the datoms happen to
   arrive. Assessing old→new resulting entries at the end of the transaction
   loop covers raw datom vectors, partial updates, retracts and adversarial
   datom order uniformly (the same chokepoint discipline as
   `key-bearing-misuse` in update-schema and the cross-tx vt-window guard).

   Returns {:invalid {key [old new]}     ;; unconditional violations
            :data-checks #{...}}         ;; checks the caller runs against data:
     :attr-used?       — reject when the attribute has current or history
                         datoms (valueType change, tuple-def change, entry
                         removal, :db/index enablement)
     :single-valued?   — reject when some entity has >1 current value
                         (cardinality many→one)"
  [old-entry new-entry write-flexibility?]
  (let [old (or old-entry {})
        new (or new-entry {})
        changed? (fn [k] (not= (get old k) (get new k)))
        ;; A brand-new entry is a DECLARATION, not a transition: the existing
        ;; create-time semantics (validate-schema, implicit defaults, tuple
        ;; nil-filling for undeclared refs, unique on card-many) stay as they
        ;; are, and datoms co-transacted with the declaration are written
        ;; under it. Transition rules and data checks apply only when the
        ;; entry existed before the transaction.
        transition? (some? old-entry)
        invalid
        (cond-> {}
          ;; Completeness (:write): an attribute-defining entry must carry
          ;; valueType and cardinality once the transaction settles.
          (and write-flexibility?
               (some? new-entry)
               (some #(contains? new %) attr-defining-keys)
               (not (and (:db/valueType new) (:db/cardinality new))))
          (assoc :db/valueType-and-cardinality-required
                 [(select-keys old [:db/valueType :db/cardinality])
                  (select-keys new [:db/valueType :db/cardinality])])

          ;; one→many under a unique constraint
          (and transition?
               (changed? :db/cardinality)
               (= (:db/cardinality new) :db.cardinality/many)
               (#{:db.unique/value :db.unique/identity} (:db/unique new)))
          (assoc :db/cardinality [(:db/cardinality old) (:db/cardinality new)])

          ;; ADDING unique to a cardinality-many attribute. Changing an
          ;; existing unique (identity↔value) is allowed regardless of
          ;; cardinality — the long-standing contract (see schema-test
          ;; "Allow to update :db/unique only if it already exists"). An
          ;; absent cardinality is implicitly one under :schema-flexibility
          ;; :read.
          (and transition?
               (some? new-entry)
               (:db/unique new)
               (not (:db/unique old))
               (some? (:db/cardinality new))
               (not= :db.cardinality/one (:db/cardinality new)))
          (assoc :db/unique [(:db/unique old) (:db/unique new)])

          ;; secondary-index status: monotonic transitions only
          (and (changed? :db.secondary/status)
               (contains? old :db.secondary/status)
               (not (contains? (get {:building #{:ready :disabled}
                                     :ready    #{:disabled}}
                                    (:db.secondary/status old))
                               (:db.secondary/status new))))
          (assoc :db.secondary/status
                 [(:db.secondary/status old) (:db.secondary/status new)])

          ;; any other db-namespaced key change is rejected (mirrors
          ;; find-invalid-schema-updates' strict default)
          transition?
          (merge (into {}
                       (keep (fn [k]
                               (when (and (keyword? k)
                                          (re-matches #"db(\..*)?" (or (namespace k) ""))
                                          (not (contains? always-allowed-schema-keys k))
                                          (not (contains? attr-defining-keys k))
                                          (not (#{:db.secondary/status} k))
                                          (changed? k))
                                 [k [(get old k) (get new k)]])))
                       (into #{} (concat (keys old) (keys new))))))
        data-checks
        (cond-> #{}
          ;; entry removed while the attribute may still have datoms
          (and transition? (nil? new-entry))
          (conj :attr-used?)

          ;; valueType CHANGE poisons existing (incl. history) datoms with
          ;; mixed types — comparators and predicates silently misbehave
          (and transition? (some? new-entry)
               (:db/valueType old) (changed? :db/valueType))
          (conj :attr-used?)

          ;; tuple definition change invalidates existing tuple values
          (and transition? (some? new-entry)
               (some (fn [k] (and (contains? old k) (changed? k)))
                     [:db/tupleAttrs :db/tupleType :db/tupleTypes]))
          (conj :attr-used?)

          ;; enabling :db/index retroactively — AVET was not populated for
          ;; existing datoms, so the index path would return incomplete results
          (and transition? (some? new-entry) (changed? :db/index) (:db/index new))
          (conj :attr-used?)

          ;; many→one with entities holding multiple values leaves state that
          ;; q/pull/entity disagree on (entity crashes). Data-checked (not
          ;; :attr-used?) because card-one enforcement reads EAVT, which
          ;; covers pre-existing datoms.
          (and (some? new-entry)
               (changed? :db/cardinality)
               (= (:db/cardinality old) :db.cardinality/many)
               (= (:db/cardinality new) :db.cardinality/one))
          (conj :single-valued?)

          ;; unique added retroactively is UNENFORCEABLE against existing
          ;; data: validate-datom checks uniqueness via AVET, and datoms
          ;; inserted before the attribute was unique/indexed are not in
          ;; AVET — duplicates of old values would be silently accepted.
          ;; Like :db/index enablement, requires an unused attribute (until
          ;; an index-backfill migration exists).
          (and transition? (some? new-entry)
               (:db/unique new) (not (:db/unique old)))
          (conj :attr-used?))]
    {:invalid invalid
     :data-checks data-checks}))

(defn is-system-keyword? [value]
  (and (or (keyword? value) (string? value))
       (if-let [ns (namespace (keyword value))]
         (= "db" (first (clojure.string/split ns #"\.")))
         false)))

(defn get-user-schema [{:keys [schema] :as db}]
  (into {} (filter #(not (is-system-keyword? (key %))) schema)))
