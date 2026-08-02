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

   - **Order within a transaction does not matter.** Each `[e a v]` key is
     touched at most once per transaction, so the retract and the assert above
     commute. A rule keyed by `[e a]` would NOT commute — processing the assert
     first and the retract second would drop the new value — which is exactly the
     bug that produces a database answering present-tense queries correctly and
     `as-of` wrongly.
   - **Cardinality is irrelevant here.** It matters to the transactor, which
     decides whether to emit the retraction; by the time we see history, that
     decision is already recorded.

   Verified against a database exercising card-one overwrite, card-many
   add/retract, retract-then-reassert of the same value, `retractEntity`, refs to
   retracted entities, schema added mid-history, and `:db/noHistory` — plus
   randomised histories. See `datahike.test.migrate-history-test`.")

(defn- tx-order
  "Sort key: transaction, then entity, then attribute as a string.

   The attribute is stringified because idents are not mutually Comparable in
   general, and this only needs a stable total order, not a meaningful one."
  [datom]
  [(nth datom 3) (nth datom 0) (str (nth datom 1))])

(defn derive-current
  "The set of `[e a v]` triples currently true, given `history-records` as
   `[e a v t added]` tuples.

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

(defn split-current
  "Partition `history-records` into `{:current [...] :history [...]}`.

   `:history` is every record, unchanged — the temporal indexes hold the lot.
   `:current` is the subset whose `[e a v]` is currently true, keeping the record
   that ASSERTED it (the latest assertion, since a later retraction would have
   removed the key).

   This is the shape a bulk build wants: one pass over the dump yields both the
   temporal trees and the current ones, with no second read."
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
