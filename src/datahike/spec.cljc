(ns datahike.spec
  (:require
    [datahike.db :as db]
    [spec-tools.data-spec :as ds]
    [clojure.spec.alpha :as s])
  #?(:clj
      (:import [java.util Date])))

(def non-neg-int? (s/or :zero zero? :posint pos-int?))

(defn set-of [pred] (s/every pred :kind set?))

(defn date? [d]
  #?(:cljs (instance? js/Date d)
     :clj  (instance? Date d)))

(def time-point? (s/or :int int? :date date?))

;; TODO: there's lots of runtime logic to conform these transactions - it would
;; be nice to spec them accurately, and then conform them at runtime through spec
(def Transactions
  (s/coll-of (s/or :seq coll? :map map?)))

;; TODO: deduplicate this Spec with the one in datahike.config
(def Config
  (ds/spec
    {:name ::config
     :keys-default ds/opt
     :spec {:store map?
            :name string?
            :keep-history? boolean?
            :schema-flexibility (s/spec #{:write :read})
            :initial-tx Transactions}}))

(def ConnectionAtom
  (fn [x]
    (and
      (instance? clojure.lang.IAtom x)
      (s/valid? db/db? @x))))

(def EId (s/or :coll coll? :int non-neg-int?))

(def PullOptions
  (ds/spec
    {:name ::pull-options
     :keys-default ds/req
     :spec {:selector coll? ;; TODO: spec more of selector
            :eid EId}}))

(def Datom
  (fn [x] (instance? datahike.datom.Datom x)))

(def Datoms
  (s/coll-of Datom))

(def TxMeta (s/nilable coll?))

(def TransactionReport
  (ds/spec
    {:name ::transaction-report
     :keys-default ds/req
     :spec {:db-before db/db?
            :db-after db/db?
            :tx-data Datoms
            :tempids map?
            :tx-meta TxMeta}}))

(def QueryArgs
  (ds/spec
    {:name ::query-args
     :keys-default ds/req
     :spec {:query (s/or :vec vector? :map map? :str string? )
            :args (s/coll-of (set-of vector?))
            (ds/opt :limit) int?
            (ds/opt :offset) int?}}))

(def WithArgs
  (ds/spec
    {:name ::with-args
     :keys-default ds/opt
     :spec {:tx-data Transactions
            :tx-meta TxMeta}}))

(def IndexRangeArgs
  (ds/spec
    {:name ::index-range-args
     :keys-default ds/req
     :spec {:attrid keyword?
            :start any?
            :end any?}}))

(def IndexLookupArgs
  (ds/spec
    {:name ::index-lookup-args
     :keys-default ds/req
     :spec {:index keyword?
            (ds/opt :components) (s/coll-of any?)}}))
