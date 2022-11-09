(ns datahike.gc
  (:require [clojure.set :as set]
            [datahike.index.interface :refer [-mark]]
            [konserve.gc :refer [sweep!]]
            [taoensso.timbre :refer [debug]]))

(defn gc!
  "Invokes garbage collection on the database erasing all fragments that are not
  reachable and older than the date passed (defaults to now). The garbage
  collector can be run asynchronously in the background or you can synchronize
  around the channel returned. The channel will contain as set of the addresses
  deleted."
  ([db] (gc! db (java.util.Date.)))
  ([db date]
   (let [{:keys [eavt avet aevt temporal-eavt temporal-avet temporal-aevt config store]}
         db
         marked (set/union #{:db} (-mark eavt) (-mark aevt) (-mark avet)
                           (when (:keep-history? config)
                             (set/union
                              (-mark temporal-eavt)
                              (-mark temporal-aevt)
                              (-mark temporal-avet))))]
     (debug "gc has marked: " marked)
     (sweep! store marked date))))
