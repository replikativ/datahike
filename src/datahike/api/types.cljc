(ns datahike.api.types
  "Malli type schemas for Datahike API.

  This namespace replaces datahike.spec with malli-based schemas.
  Schemas are used for:
  - API specification
  - Runtime validation
  - Code generation (Java, CLI, TypeScript)
  - Documentation generation"
  (:require [malli.core :as m]
            [malli.util :as mu]
            [malli.error :as me]
            [datahike.connector :refer [connection?]]
            [datahike.datom :refer [datom?]]
            [datahike.db.utils :as dbu])
  #?(:clj
     (:import [java.util Date])))

;; =============================================================================
;; Helper Predicates
;; =============================================================================

(defn date? [d]
  #?(:cljs (instance? js/Date d)
     :clj  (instance? Date d)))

;; ============================================================================
;; Core Type Schemas
;; =============================================================================

(def SDB
  "Immutable database value."
  [:fn {:error/message "must be a Datahike database"}
   dbu/db?])

(def SConfig
  "Database configuration.

  Can be:
  - Map with :store key (primary format)
  - URI string (deprecated)"
  [:or
   [:map {:closed false}]
   :string])

(def SConnection
  "Database connection reference."
  [:fn {:error/message "must be a Datahike connection"}
   connection?])

(def SEId
  "Entity identifier.

  Can be:
  - Number (entity ID)
  - Sequential (lookup ref [:attr value])
  - Keyword (for schema entities)"
  [:or :number
   [:sequential :any]
   :keyword])

(def SDatom
  "Single datom (immutable fact)."
  [:fn {:error/message "must be a Datom"}
   datom?])

(def SDatoms
  "Collection of datoms."
  [:sequential SDatom])

(def STxMeta
  "Transaction metadata - optional collection."
  [:maybe :any])

(def STransactionReport
  "Transaction result map.

  Keys:
  - :db-before - Database before transaction
  - :db-after - Database after transaction
  - :tx-data - Datoms added/retracted
  - :tempids - Temp ID to entity ID mapping
  - :tx-meta - Transaction metadata (optional)"
  [:map
   [:db-before SDB]
   [:db-after SDB]
   [:tx-data SDatoms]
   [:tempids :map]
   [:tx-meta {:optional true} STxMeta]])

(def STransactions
  "Transaction data - collection of transaction forms."
  [:sequential :any])

(def SPullOptions
  "Pull pattern options map."
  [:map
   [:selector :vector]
   [:eid SEId]])

(def SQueryArgs
  "Query arguments map."
  [:map
   [:query [:or :string :vector :map]]
   [:args {:optional true} [:sequential :any]]
   [:limit {:optional true} :int]
   [:offset {:optional true} :int]])

(def SWithArgs
  "Arguments for 'with' operation."
  [:map
   [:tx-data STransactions]
   [:tx-meta {:optional true} STxMeta]])

(def SIndexLookupArgs
  "Index lookup arguments."
  [:map
   [:index [:enum :eavt :aevt :avet]]
   [:components {:optional true} [:maybe [:sequential :any]]]])

(def SIndexRangeArgs
  "Index range query arguments."
  [:map
   [:attrid :keyword]
   [:start :any]
   [:end :any]])

(def SSchema
  "Database schema - map of attributes to schema entries."
  [:map-of :any :any])

(def SMetrics
  "Database metrics map."
  [:map
   [:count :int]
   [:avet-count :int]
   [:per-attr-counts :map]
   [:per-entity-counts :map]
   [:temporal-count {:optional true} :int]
   [:temporal-avet-count {:optional true} :int]])

(def time-point?
  "Time point - transaction ID (int) or Date."
  [:or :int
   [:fn {:error/message "must be a Date"}
    date?]])

;; =============================================================================
;; Type Mappings for Code Generation
;; =============================================================================

(def malli->java-type
  "Malli schema to Java type mapping."
  {:SConfig "Map<String, Object>"
   :SConnection "datahike.connector.Connection"
   :SDB "datahike.db.DB"
   :SEId "Object"
   :SDatom "datahike.datom.Datom"
   :SDatoms "List<Datom>"
   :STransactions "List<Object>"
   :STransactionReport "Map<String, Object>"
   :SSchema "Map<Object, Object>"
   :SMetrics "Map<String, Object>"
   :SPullOptions "Map<String, Object>"
   :SQueryArgs "Map<String, Object>"
   :SIndexLookupArgs "Map<String, Object>"
   :SIndexRangeArgs "Map<String, Object>"
   :SWithArgs "Map<String, Object>"
   ;; Primitives
   :boolean "boolean"
   :int "int"
   :long "long"
   :double "double"
   :string "String"
   :keyword "String"
   :any "Object"
   :nil "void"
   ;; Collections
   :vector "List"
   :map "Map"
   :set "Set"
   :sequential "List"})

(def malli->typescript-type
  "Malli schema to TypeScript type mapping."
  {:SConfig "DatabaseConfig"
   :SConnection "Connection"
   :SDB "Database"
   :SEId "number | [string, any] | string"
   :SDatom "Datom"
   :SDatoms "Datom[]"
   :STransactions "Transaction[]"
   :STransactionReport "TransactionReport"
   :SSchema "Record<string, any>"
   :SMetrics "Metrics"
   :SPullOptions "PullOptions"
   :SQueryArgs "QueryArgs"
   :SIndexLookupArgs "IndexLookupArgs"
   :SIndexRangeArgs "IndexRangeArgs"
   :SWithArgs "WithArgs"
   ;; Primitives
   :boolean "boolean"
   :int "number"
   :long "number"
   :double "number"
   :string "string"
   :keyword "string"
   :any "any"
   :nil "null"
   ;; Collections
   :vector "Array<any>"
   :map "Record<string, any>"
   :set "Set<any>"
   :sequential "Array<any>"})

;; =============================================================================
;; Validation Helpers
;; =============================================================================

(defn validate
  "Validate data against schema."
  [schema data]
  (m/validate schema data))

(defn explain
  "Explain validation errors."
  [schema data]
  (m/explain schema data))

(defn humanize
  "Get human-readable validation errors."
  [explanation]
  (me/humanize explanation))
