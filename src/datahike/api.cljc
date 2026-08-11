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

(def ^:no-doc uninstrumentable
  "Operations whose `:args` schema cannot be registered with malli, by name and
   for a reason — never as a silent fallback.

   `with` declares three branches, two of them 2-arity. That is the JAVA
   BINDING's shape: `codegen/java` maps `STransactions` to `List` and
   `SWithArgs` to `Object`, so those two branches emit two distinct overloads,
   and the `List` one marshals through `Util.normalizeCollections` while the
   other does not. malli rejects a `:function` with two branches of the same
   arity, and merging them into `[:or …]` deletes `with(Object, List)` from the
   generated Java — measured against the generated source — taking the
   collection normalisation with it.

   The user-facing binding wins over the check. This is the only operation in
   that position; everything else is registered and enforced."
  #{'with})

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
    (doseq [[n {:keys [args]}] api-specification
            :when (not (contains? uninstrumentable n))]
      ;; NOT wrapped in a try. A schema that fails to compile is a schema that
      ;; silently checks nothing, which is how seven of them stayed wrong — so a
      ;; new one must break the build rather than quietly opt itself out. The
      ;; only permitted exceptions are named in `uninstrumentable`, with reasons.
      (m/-register-function-schema! 'datahike.api n (m/schema args opts) {}))
    (- (count api-specification) (count uninstrumentable))))

(emit-api)
(register-api-schemas!)