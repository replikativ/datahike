(ns ^:no-doc datahike.migrate.history
  "Deriving the CURRENT datom set from a full history.

   A `:keep-history? true` database keeps six trees: `temporal-eavt/aevt/avet`
   hold every datom ever asserted or retracted, while `eavt/aevt/avet` hold only
   those currently true. A dump carries the history; a bulk-build import has to
   reconstruct both, which means deciding which datoms are current WITHOUT
   replaying transactions.

   ## The rule is simpler than it looks

   Fold the history in transaction order, keyed by `[e a v]`: an assertion adds
   that exact datom, a retraction removes it.

   That is all. No schema, no cardinality distinction, no special case for
   `retractEntity` or `:db/noHistory`. The reason is that datahike writes an
   EXPLICIT retraction of the old value even for a cardinality-one overwrite —
   `{:db/id e :score 10}` over an existing `1` produces both
   `[e :score 1 t false]` and `[e :score 10 t true]` in the same transaction. So
   there is never an implicit supersede to model.

   Two consequences worth stating, because a hand-written version tends to get
   them wrong in the other direction:

   - **Order within a transaction is imposed by the sort, not assumed.** An
     earlier version of this namespace claimed each `[e a v]` key is touched at
     most once per transaction, so retract and assert commute. That is FALSE:
     `[[:db/retract 100 :tag :x] [:db/add 100 :tag :x]]` in one transaction
     produces both `[100 :tag :x t false]` and `[100 :tag :x t true]`, and the
     fold gave two different answers for the same multiset depending on input
     order. `tx-order` now sorts retraction before assertion, which both makes
     the fold deterministic and matches datahike, where the datom ends up
     present.
   - **Cardinality is irrelevant here.** It matters to the transactor, which
     decides whether to emit the retraction; by the time we see history, that
     decision is already recorded.

   Verified against a database exercising card-one overwrite, card-many
   add/retract, retract-then-reassert of the same value, `retractEntity`, refs to
   retracted entities, schema added mid-history, and `:db/noHistory` — plus
   randomised histories. See `datahike.test.migrate-history-test`.")

(defn- tx-order
  "Sort key: transaction, entity, attribute, value, then op with RETRACTION
   before assertion.

   Attribute and value are stringified because neither is mutually Comparable in
   general; this needs a stable total order, not a meaningful one.

   `op` last is load-bearing. Without it the sort is stable-but-input-ordered for
   records that agree on `[t e a]`, and datahike really does produce two such
   records: transacting `[[:db/retract 100 :tag :x] [:db/add 100 :tag :x]]`
   yields BOTH `[100 :tag :x t false]` and `[100 :tag :x t true]`. Fed in one
   order the fold answered `#{}`, in the other `#{[100 :tag :x]}` — for the same
   multiset. datahike says the datom is present, so retraction must sort first
   and the assertion wins."
  [datom]
  [(nth datom 3) (nth datom 0) (str (nth datom 1)) (str (nth datom 2))
   (if (nth datom 4) 1 0)])

(defn derive-current
  "The set of `[e a v]` triples currently true, given `history-records` as
   `[e a v t added]` tuples.

   NOT MEMORY BOUNDED — it sorts the whole input and accumulates a set of every
   live datom. That is fine for a test or a small database and wrong for the case
   a bulk build exists to serve. `current-from-eavt-sorted` is the streaming
   version; this one stays because it is the obvious, order-independent statement
   of the rule and therefore the right thing to check the streaming one against.

   Returns a SET of triples rather than records: `t` is deliberately dropped,
   because the current set is a question about which facts hold, not about when
   they were asserted. Callers that need the asserting transaction should take it
   from the matching history record."
  [history-records]
  (reduce (fn [acc record]
            (let [k [(nth record 0) (nth record 1) (nth record 2)]]
              (if (nth record 4) (conj acc k) (disj acc k))))
          #{}
          (sort-by tx-order history-records)))

(defn current-from-eavt-sorted
  "STREAMING currentness: given records sorted by `[e a v t]`, return a lazy seq
   of the records that are currently true, in that same order.

   `derive-current` sorts its whole input and accumulates a set, so it is O(n) in
   memory — fine for a test or a small database, useless for the case a bulk
   build exists to serve. This is the version the import path uses.

   The trick is the sort order, not the fold. Sorting by `[e a v t]` puts every
   record for one `[e a v]` ADJACENT and in transaction order, so deciding
   whether that datom is current means looking at the last record of the run and
   nothing else. State is one partial run, not a set of every live datom.

   Requires the input to be sorted by `[e a v t]` with `t` ascending within each
   `[e a v]`. That is an external sort the import already performs to feed the
   index builders, so it costs nothing extra."
  [sorted-records]
  (letfn [(step [rs]
            (lazy-seq
             (when-let [s (seq rs)]
               (let [r (first s)
                     k (fn [x] [(nth x 0) (nth x 1) (nth x 2)])
                     kr (k r)
                     run (cons r (take-while #(= kr (k %)) (rest s)))
                     rest-s (drop (count run) s)
                     ;; last record for this [e a v] wins; current iff it asserted
                     final (last run)]
                 (if (nth final 4)
                   (cons final (step rest-s))
                   (step rest-s))))))]
    (step sorted-records)))

(defn split-current
  "Partition `history-records` into `{:current [...] :history [...]}`.

   `:history` is every record, unchanged — the temporal indexes hold the lot.
   `:current` is the subset whose `[e a v]` is currently true, keeping the record
   that ASSERTED it (the latest assertion, since a later retraction would have
   removed the key).

   NOT MEMORY BOUNDED — it builds on `derive-current` and materialises both
   halves. Kept for tests and small inputs; the import path composes
   `current-from-eavt-sorted` with an external sort instead."
  [history-records]
  (let [current (derive-current history-records)
        ;; latest assertion per key — later transactions overwrite earlier ones
        asserted (reduce (fn [acc record]
                           (let [k [(nth record 0) (nth record 1) (nth record 2)]]
                             (if (and (nth record 4) (contains? current k))
                               (assoc acc k record)
                               acc)))
                         {}
                         (sort-by tx-order history-records))]
    {:current (vec (vals asserted))
     :history (vec history-records)}))
