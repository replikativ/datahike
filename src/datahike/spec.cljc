(ns datahike.spec
  (:require
    [spec-tools.data-spec :as ds]
    [clojure.spec.alpha :as s])
  #?(:clj
     (:import [datahike.db DB]))

(def non-neg-int? (s/or :zero zero? :posint pos-int?))

(defn set-of [pred] (s/every pred :kind set?))

(def Transactions
  (s/coll-of (s/or :seq coll? :map map?))) ; TODO: there's lots of runtime logic to conform these - it would be nicer to spec them, and then conform them at runtime through spec

(def Config
  (ds/spec
    {:name ::config
     :keys-default ds/opt
     :spec {:store map?
            :name string?
            :keep-history? boolean?
            :schema-flexibility (s/spec #{:write :read})
            :initial-tx Transactions}}))

(def DB
  (ds/spec
    {:name ::db
     :spec {:max-tx pos-int?
            :max-eid non-neg-int?}}))

(def ConnectionAtom
  (fn [x]
    (and
      (instance? clojure.lang.IAtom x)
      (instance? datahike.db.DB @x)
      (s/valid? DB @x))))

(def EId (s/or :coll coll? :int non-neg-int?))

(def PullOptions
  (ds/spec
    {:name ::pull-options
     :keys-default ds/req
     :spec {:selector coll?  ; TODO: spec more of selector?
            :eid EId}}))

(def Datom
  (fn [x] (instance? datahike.datom.Datom x)))

(def TxMeta (s/nilable coll?))

(def TransactionReport
  (ds/spec
    {:name ::transaction-report
     :keys-default ds/req
     :spec {:db-before DB
            :db-after DB
            :tx-data (s/coll-of Datom)
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