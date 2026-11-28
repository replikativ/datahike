(ns datahike.experimental.transaction
  (:require [datahike.core :as core]
            [datahike.db.transaction :as tx])
  (:import [datahike.db DB]))

(defn prepare
  "Speculatively apply tx-data on a current DB with a logical expansion limit.
  Returns the usual transaction report. Counts dispatched forms, ordinary
  retraction lookups and their result datoms. See doc/contract-datalog.md."
  [db tx-data limit]
  (when-not (and (instance? DB db) (int? limit) (<= 0 limit))
    (throw (ex-info "Expected a current database and a nonnegative integer limit"
                    {:error :transact/invalid-expansion-arguments})))
  (binding [tx/*expansion-remaining* (volatile! limit)]
    (dissoc (core/with db tx-data) :datahike/tx-ops)))
