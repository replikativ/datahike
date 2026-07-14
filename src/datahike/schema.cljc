(ns ^:no-doc datahike.schema
  (:require [clojure.spec.alpha :as s]
            [datahike.datom])
  #?(:clj (:import [datahike.datom Datom])))

(s/def :db.type/id #?(:clj #(or (= (class %) java.lang.Long) string?)
                      :cljs #(or (and (number? %)
                                      (js/Number.isSafeInteger %))
                                 string?)))

;; db types
(s/def :db.type/bigdec #?(:cljs (complement any?) ; feels more appropriate than hiding key -pat
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

(s/def ::schema-attribute #{:db/id :db/ident :db/isComponent :db/noHistory :db/valueType :db/cardinality :db/unique :db/index :db.install/_attribute :db/doc :db/tupleAttrs  :db/tupleType :db/tupleTypes :db.secondary/only})

(s/def ::secondary-index-attribute #{:db.secondary/type :db.secondary/attrs :db.secondary/config :db.secondary/status :db.secondary/building-since-tx})

(s/def ::entity-spec-attribute #{:db/ensure :db.entity/attrs :db.entity/preds})
;; ADR — bitemporal v1: :db.valid/from and :db.valid/to graduate from
;; userland tx-meta into datahike system schema. They are tx-attached
;; (every datom in a tx inherits its tx's vt window) and AVET-indexed
;; so the query planner can seek into them.
(s/def ::meta-attribute #{:db/txInstant :db/retracted :db/noCommit
                          :db.valid/from :db.valid/to})

(s/def ::schema (s/keys :req [:db/ident :db/valueType :db/cardinality]
                        :opt [:db/id :db/unique :db/index :db.install/_attribute :db/doc :db/noHistory :db/tupleType :db/tupleTypes :db.secondary/only]))

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
                   :db.secondary/only})

(s/def ::old-schema-val (s/keys :req [:db/valueType :db/cardinality]
                                :opt [:db/ident :db/unique :db/index :db.install/_attribute :db/doc :db/noHistory]))

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
  ;; `:db.type/value` used to be a set spec, so s/describe rendered the enum of
  ;; legal value types — which is what the "Bad entity value ..." error shows the
  ;; user. Opening it up to registered custom types made it a predicate, and
  ;; s/describe would print the predicate's SOURCE instead of the alternatives.
  ;; Rebuild the enum, now including whatever is registered.
  (if (= schema-type :db.type/value)
    (into (sorted-set) builtin-value-types)
    (s/describe schema-type)))

(defn key-bearing-misuse
  "Reasons this schema entity would let the collector delete live data, or nil.

   A key-bearing value type (`:db.type/store-ref`, or a custom type declaring
   `:reachable-keys`) NAMES an object in the store. The collector keeps such an
   object alive by scanning the datoms of attributes declared with that type. Two
   shapes defeat that scan, both silently — so they are rejected rather than
   documented:

   TUPLES. A `:db/tupleType` / `:db/tupleTypes` value is a vector on a
   `:db.type/tuple` attribute. The attribute's OWN valueType is the builtin tuple,
   so the registry is never consulted and the keys nested inside the vector are
   invisible to the mark: the objects get swept while the datom still names them.

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

(defn is-system-keyword? [value]
  (and (or (keyword? value) (string? value))
       (if-let [ns (namespace (keyword value))]
         (= "db" (first (clojure.string/split ns #"\.")))
         false)))

(defn get-user-schema [{:keys [schema] :as db}]
  (into {} (filter #(not (is-system-keyword? (key %))) schema)))
