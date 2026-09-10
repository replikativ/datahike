(ns ^:no-doc datahike.backfill.control
  "Internal cooperative worker cancellation. Never a transaction option.")

(def ^:dynamic *check!* (fn [] nil))

(defn check! [] (*check!*))
