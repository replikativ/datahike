(ns datahike.api
  "Public API for datahike. Expanded from api.specification."
  (:refer-clojure :exclude [filter])
  #?(:cljs (:require-macros [datahike.api :refer [emit-api]]))
  (:require [datahike.connector :as dc]
            [datahike.config :as config]
            [datahike.api.specification :refer [api-specification malli-schema->argslist]]
            [datahike.api.impl]
            [datahike.writer :as dw]
            #?(:clj [datahike.http.writer])
            [datahike.writing :as writing]
            [konserve.store]
            [datahike.constants :as const]
            [datahike.core :as dcore]
            [datahike.pull-api :as dp]
            [datahike.query :as dq]
            [datahike.schema :as ds]
            [datahike.tools :as dt]
            [datahike.db :as db #?@(:cljs [:refer [HistoricalDB AsOfDB SinceDB FilteredDB]])]
            [datahike.db.interface :as dbi]
            [datahike.db.transaction :as dbt]
            [datahike.impl.entity :as de])
  #?(:clj
     (:import [clojure.lang Keyword PersistentArrayMap]
              [datahike.db HistoricalDB AsOfDB SinceDB FilteredDB]
              [datahike.impl.entity Entity])))

(defmacro ^:private emit-api []
  `(do
     ~@(reduce
        (fn [acc [n {:keys [args doc impl]}]]
          (conj acc
                `(def
                   ~(with-meta n
                      {:arglists `(malli-schema->argslist (quote ~args))
                       :doc      doc})
                   ~impl)))
        ()
        (into (sorted-map) api-specification))))

(emit-api)