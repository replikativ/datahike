;; Maintenance for databases created before every index branch kept its count.
(ns datahike.index.recount
  (:require [datahike.index.interface :as dii]
            [datahike.writer :as dw]
            [replikativ.logging :as log]))

(def ^:private index-keys
  [:eavt :aevt :avet :temporal-eavt :temporal-aevt :temporal-avet])

(defn census
  "`{:complete? b :indices {index-key {:branches n :unknown m}}}` for `conn`'s
   database: whether it is marked as having every count, and per index how many
   branches it has and how many are stored without their element count.

   Reads every branch of every index and no leaf -- for a large database on a
   remote store that is thousands of requests. Nothing is written."
  [conn]
  (let [db @(:wrapped-atom conn)
        _ (when (= :released db)
            (log/raise "The connection has been released." {:type :connection-has-been-released}))
        index (get-in db [:config :index])]
    {:complete? (dii/counts-complete? db)
     :indices (into {} (for [k index-keys
                             :let [ix (get db k)
                                   c (when ix (dii/count-census index ix))]
                             :when c]
                         [k c]))}))

(defn recount!
  "Store every branch of every index of `conn`'s database with its element count,
   and mark the database as complete. Returns what `census` said before.

   WHO NEEDS IT. A database created before Datahike kept a count on every stored
   index branch (persistent-sorted-set before 0.5.148) can hold branches -- and,
   before 0.5.146, roots -- stored without one (#1108). Nothing is wrong with its
   data and every query answers correctly. But the query planner sizes a pattern by
   counting an index range, and a range that covers an uncounted branch reads that
   branch's children to count it, in every fresh process; where a root has no count
   the planner falls back to heuristics. A connect to such a database logs
   `:datahike/index-counts-incomplete`, once.

   WHAT IT DOES. It rewrites each branch that has no count, and the path above it,
   and commits. The commit carries no transaction: no datom changes, :max-tx stays.
   It then marks the database, so later connects are quiet. It applies to the
   branch this connection is on; run it once per branch. On a database whose
   branches all have their counts it only sets the mark.

   WHAT IT COSTS. It reads every branch of every index, plus the leaves under each
   lowest-level branch that has no count. On a remote store that is one request per
   node. The rewriting step runs on the writer, so transactions wait for it; run it
   at a quiet time. Once done it is done: writes keep every count.

   LIMITS. It needs a connection with a local writer. It refuses a :crypto-hash?
   store (rebuild that one with an export and an import), a connection with online
   GC enabled, and a database with an AVET build running."
  [conn]
  (let [db @(:wrapped-atom conn)
        _ (when (= :released db)
            (log/raise "The connection has been released." {:type :connection-has-been-released}))]
    (when-not (= :self (get-in db [:config :writer :backend] :self))
      (log/raise "recount! needs a connection with a local writer."
                 {:type :recount/needs-local-writer
                  :writer (get-in db [:config :writer :backend])}))
    (when (get-in db [:config :crypto-hash?])
      (log/raise "Index counts cannot be rewritten on a :crypto-hash? store: a node's address does not cover its count. Rebuild the store with an export and an import instead."
                 {:type :recount/crypto-hash}))
    (when (get-in db [:config :online-gc :enabled?])
      (log/raise "Index counts cannot be rewritten while online GC is enabled; connect with :online-gc {:enabled? false} for the recount."
                 {:type :recount/online-gc}))
    ;; The census runs here, outside the writer, and leaves every branch in the
    ;; node cache: the writer's own pass then reads little but the leaves under
    ;; uncounted branches.
    (let [before (census conn)
          report @(dw/recount-indices! conn)]
      (when-not (map? report)
        (throw report))
      before)))
