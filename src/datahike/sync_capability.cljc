(ns ^:no-doc datahike.sync-capability
  "Empirical store sync-read capability — leaf namespace (only konserve.core)
   so both the index storage layer and the query dispatcher can consult it
   without cycles."
  (:require [konserve.core :as k]))

#?(:cljs
   (def ^:private capability-cache
     "store object → boolean, identity-keyed (stores are long-lived; a WeakMap
      lets released stores be collected)."
     (js/WeakMap.)))

(defn sync-read-capable?
  "Can `store` serve synchronous reads? Determined EMPIRICALLY — one probed
   sync read, memoized per store instance — rather than from a hand-maintained
   backend taxonomy that could drift (a wrong static flag either forbids syncs
   that would work or promises ones that cannot; the store itself is the
   source of truth). Async-only cljs backends (IndexedDB, remote stores)
   throw on {:sync? true} and probe as false; memory stores and tiered stores
   with a sync-capable frontend probe as true.

   On the JVM every konserve backend supports synchronous reads."
  [store]
  #?(:clj true
     :cljs (if (.has capability-cache store)
             (.get capability-cache store)
             (let [capable? (try
                              (k/exists? store ::sync-probe {:sync? true})
                              true
                              (catch :default _ false))]
               (.set capability-cache store capable?)
               capable?))))
