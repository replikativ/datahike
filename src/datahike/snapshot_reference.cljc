(ns datahike.snapshot-reference
  "Experimental same-store snapshot values for attributes of :db.type/gc-ref."
  (:require [datahike.gc-reference :as ref]
            [datahike.reachability :as reach]
            [datahike.store :as ds]
            [konserve.core :as k]
            [clojure.core.async :as async]
            [konserve.utils :refer [#?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]])
  #?(:clj (:import [java.util Date])))

(defn snapshot-ref
  "Return a versioned value naming this exact committed snapshot after checking
   its required same-store record, index and generation dependencies. Default is synchronous; {:sync? false}
   returns a channel. Uncommitted db values and incomplete targets are rejected.

   Construction does not pin the target. Keep it retained until the referencing
   transaction publishes, or externally exclude GC throughout construction and
   publication. Bounded GC's age floor cannot protect old target objects."
  ([db] (snapshot-ref db {:sync? true}))
  ([db opts]
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (reach/require-reference-policy! (:config db))
                (let [store (:store db)
                      cid (get-in db [:meta :datahike/commit-id])
                      record (when cid (<?- (k/get store cid nil opts)))]
                  (when (or (nil? record) (not= (:hash db) (:hash record))
                            (not= (:max-tx db) (:max-tx record)))
                    (throw (ex-info "A snapshot reference requires a durable, committed db value."
                                    {:type :datahike/gc-reference-uncommitted-target :commit-id cid})))
                  (<?- (reach/reachable-in-branch store cid (#?(:clj Date. :cljs js/Date.) 0)
                                                  (:config db) (atom {})
                                                  (assoc opts :include-parents? false :required-root? true)))
                  (ref/snapshot-reference
                   (ds/canonical-store-id store (get-in db [:config :store])) cid))))))
