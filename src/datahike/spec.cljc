(ns ^:no-doc datahike.spec
  (:require [datahike.connector :refer [connection?]]
            [datahike.datom :refer [datom?]]
            [datahike.db.utils :as dbu]
            [spec-tools.data-spec :as ds]
            [clojure.spec.alpha :as s])
  #?(:clj
     (:import [clojure.lang IAtom]
              [java.util Date])))

(defn set-of [pred] (s/every pred :kind set?))

(defn date? [d]
  #?(:cljs (instance? js/Date d)
     :clj  (instance? Date d)))

(def time-point? (s/or :int int? :date date?))

(def SDB dbu/db?)

(def SConfig (s/alt :config :datahike/config
                    :deprecated/map :deprecated/config
                    :deprecated/uri string?))

(def SConnection connection?)

(def SEId (s/or :number number? :coll sequential? :keyword keyword?))

(def SPullOptions
  (ds/spec
   {:name ::pull-options
    :keys-default ds/req
    :spec {:selector coll? ;; TODO: spec more of selector
           :eid SEId}}))

(def SDatom datom?)

(def SDatoms
  (s/coll-of SDatom))

(def STxMeta (s/nilable coll?))

(def STransactionReport
  (ds/spec
   {:name ::transaction-report
    :keys-default ds/req
    :spec {:db-before SDB
           :db-after SDB
           :tx-data SDatoms
           :tempids map?
           :tx-meta STxMeta}}))

(def SQueryArgs
  (ds/spec
   {:name ::query-args
    :keys-default ds/req
    :spec {:query (s/or :vec vector? :map map? :str string?)
           :args (s/coll-of (set-of vector?))
           (ds/opt :limit) int?
           (ds/opt :offset) int?}}))

(def STransactions
  (s/coll-of (s/or :seq coll? :map map? :nil nil?)))

(def SWithArgs
  (ds/spec
   {:name ::with-args
    :keys-default ds/opt
    :spec {:tx-data STransactions
           :tx-meta STxMeta}}))

(def SIndexRangeArgs
  (ds/spec
   {:name ::index-range-args
    :keys-default ds/req
    :spec {:attrid keyword?
           :start any?
           :end any?}}))

(def SIndexLookupArgs
  (ds/spec
   {:name ::index-lookup-args
    :keys-default ds/req
    :spec {:index keyword?
           (ds/opt :components) (s/nilable coll?)}}))

(def SSchemaEntry :datahike.schema/schema)

(def SSchema
  (s/map-of any? SSchemaEntry))

(def SMetrics
  (ds/spec
   {:name ::metrics
    :keys-default ds/req
    :spec {:count int?
           :avet-count int?
           :per-attr-counts map?
           :per-entity-counts map?
           (ds/opt :temporal-count) int?
           (ds/opt :temporal-avet-count) int?}}))
