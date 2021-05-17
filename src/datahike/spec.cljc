(ns datahike.spec
  (:require
    [spec-tools.data-spec :as ds]
    [clojure.spec.alpha :as s])
  #?(:clj
     (:import [datahike.db DB])))

(defn non-neg-int? [x] (or (zero? x) (pos-int? x)))

(def Transactions
  (s/coll-of (s/or :map map?
                   :pair (s/cat :k keyword? :v any?))))

(def Config
  (ds/spec
    {:name ::config
     :keys-default ds/opt
     :spec {:store map?
            :name string?
            :keep-history? boolean?
            :schema-flexibility (s/spec #{:write :read})
            :initial-tx Transactions}}))

(def Connection
  (ds/spec
    {:name ::connection
     :spec {:max-tx pos-int?
            :max-eid non-neg-int?}}))

(def ConnectionAtom
  (fn [x]
    (and
      (instance? clojure.lang.IAtom x)
      (instance? datahike.db.DB @x)
      (s/valid? Connection @x))))
