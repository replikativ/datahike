(ns datahike.api
  "Public API for datahike. Expanded from api.specification."
  (:refer-clojure :exclude [filter])
  (:require [datahike.connector :as dc]
            [datahike.config :as config]
            [datahike.api.specification :refer [api-specification spec-args->argslist]]
            [datahike.api.impl]
            [clojure.spec.alpha :as s]
            [datahike.writer :as dw]
            [datahike.writing :as writing]
            [datahike.constants :as const]
            [datahike.core :as dcore]
            [datahike.spec :as spec]
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

(doseq [[n {:keys [args ret fn doc impl]}] api-specification]
  (eval
   `(s/fdef ~n :args ~args :ret ~ret ~@(when fn [:fn fn])))
  (eval
   `(~'def
     ~(with-meta n
        {:arglists `(spec-args->argslist (quote ~args))
         :doc      doc})
     ~impl)))
