(ns datahike.experimental.transaction
  (:require [datahike.core :as core]
            [datahike.db.transaction :as tx])
  (:import [datahike.db DB]))

(defn prepare
  "Speculatively apply tx-data on a current DB with a logical expansion limit.
  Returns the usual transaction report. Counts dispatched forms, ordinary
  retraction lookups and their result datoms. The optional on-used function receives the number of units consumed, including
  work before a transactor exception. See doc/contract-datalog.md."
  ([db tx-data limit] (prepare db tx-data limit nil))
  ([db tx-data limit on-used]
   (when-not (and (instance? DB db) (int? limit) (<= 0 limit))
     (throw (ex-info "Expected a current database and a nonnegative integer limit"
                     {:error :transact/invalid-expansion-arguments})))
   (let [remaining (volatile! limit)]
     (binding [tx/*expansion-remaining* remaining]
       (try
         (dissoc (core/with db tx-data) :datahike/tx-ops)
         (finally
           (when on-used (on-used (- limit @remaining)))))))))
