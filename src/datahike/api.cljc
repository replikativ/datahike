(ns datahike.api
  "Public API for datahike. Expanded from api.specification."
  (:refer-clojure :exclude [filter])
  #?(:cljs (:require-macros [datahike.api :refer [emit-api]]))
  (:require [datahike.connector :as dc]
            [datahike.config :as config]
            [datahike.api.specification :refer [api-specification malli-schema->argslist]]
            [datahike.api.types :as types]
            [malli.core :as m]
            [malli.util :as mu]
            [datahike.api.impl]
            [datahike.writer :as dw]
            #?(:clj [datahike.http.writer])
            [datahike.writing :as writing]
            [konserve.store]
            [datahike.constants :as const]
            [datahike.core :as dcore]
            [datahike.pull-api :as dp]
            [datahike.query :as dq]

            [datahike.query.resolve :as qr]
            [datahike.schema :as ds]
            [datahike.tools :as dt]
            [datahike.warm]
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

(defn ^:no-doc register-api-schemas!
  "Publish this namespace's malli function schemas so `malli.instrument/instrument!`
   can find them. Idempotent; called once at load.

   REGISTRATION IS NOT INSTRUMENTATION. This only records `[:=> …]` forms in
   malli's function-schema registry — no var is wrapped, nothing is validated,
   and there is no runtime cost. Datahike does NOT instrument itself: users who
   never call malli's instrumenter see exactly the behaviour they always did.

   The point of registering rather than keeping the schemas private is that a
   user who already runs `(malli.instrument/instrument!)` in their own dev setup
   gets datahike's API checked too, for free — `instrument!` instruments every
   registered schema, not just the caller's. Our own test suite opts in the same
   way; see `datahike.test.malli-instrumentation-test`.

   The schemas reference datahike's own type registry (`:datahike/SDB` and
   friends), so they are compiled against it here rather than the default one."
  []
  (let [opts {:registry (merge (m/default-schemas) (mu/schemas) types/registry)}]
    (doseq [[n {:keys [args]}] api-specification]
      ;; EVERY operation, with no exclusion list. NOT wrapped in a try either: a
      ;; schema that fails to compile is a schema that silently checks nothing,
      ;; which is how seven of them stayed wrong — so a new one must break the
      ;; build rather than quietly opt itself out.
      ;;
      ;; There was an exclusion list, holding the four operations whose 2-arity
      ;; accepts either a transaction vector or an arg-map. It is gone because
      ;; the reason for it is: `codegen/java`'s `expand-or-args` renders an
      ;; `[:or …]` argument as separate Java overloads, so those schemas can now
      ;; describe both shapes without costing the binding the `List` overload
      ;; that carries `Util.normalizeCollections`. Do not reintroduce the list —
      ;; an operation that cannot be registered is a schema bug or a codegen
      ;; gap, and both are fixable.
      (m/-register-function-schema! 'datahike.api n (m/schema args opts) {}))
    (count api-specification)))

(emit-api)
(register-api-schemas!)

;; The read-only functions a query may call under every resolver, the
;; server's safe one included: a subquery, a pull, an index walk.
(qr/install-datahike-fns!
 {'datahike.api/q           q
  'datahike.api/pull        pull
  'datahike.api/pull-many   pull-many
  'datahike.api/entity      entity
  'datahike.api/datoms      datoms
  'datahike.api/seek-datoms seek-datoms
  'datahike.api/index-range index-range})
