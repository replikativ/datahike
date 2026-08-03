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

(defn- eav-runs
  "Group records sorted by `[e a v t]` into consecutive runs sharing `[e a v]`.

   The one structural fact both streaming functions below rest on: that sort puts
   every record for one `[e a v]` ADJACENT and in transaction order, so each is a
   question about the LAST record of a run and nothing else. Memory is one run,
   not a set of every live datom.

   `partition-by` rather than a hand-rolled `take-while`/`drop`, which walks each
   run twice."
  [sorted-records]
  (partition-by (fn [r] [(nth r 0) (nth r 1) (nth r 2)]) sorted-records))

(defn current-from-eavt-sorted
  "STREAMING currentness: given records sorted by `[e a v t]`, return a lazy seq
   of the records that are currently true, in that same order.

   `derive-current` sorts its whole input and accumulates a set, so it is O(n) in
   memory — fine for a test or a small database, useless for the case a bulk
   build exists to serve. This is the version the import path uses.

   Requires the input to be sorted by `[e a v t]` with `t` ascending within each
   `[e a v]`. That is an external sort the import already performs to feed the
   index builders, so it costs nothing extra."
  [sorted-records]
  (keep (fn [run]
          ;; last record for this [e a v] wins; current iff it asserted
          (let [final (last run)]
            (when (nth final 4) final)))
        (eav-runs sorted-records)))

(defn temporal-from-eavt-sorted
  "STREAMING: given records sorted by `[e a v t]`, the subset the TEMPORAL trees
   hold. `exclude-attr?` names attributes whose LIVE datom never reaches temporal.

   The temporal trees are NOT \"every record\", which is the obvious reading and
   is wrong. Derived from the transactor rather than assumed — see `with-datom`
   and `with-datom-upsert`:

     - card-one goes through the upsert path, whose `temporal-upsert` conj's
       every assertion into temporal INCLUDING the currently-live one;
     - a plain card-many assertion touches only the current trees;
     - a retraction puts both the removed assertion and the retraction marker
       into temporal;
     - a `:db/noHistory` attribute never reaches temporal at all, because
       `keep-history?` is `(and (-keep-history? db) (not (no-history? db a)))`.

   So exactly two classes are missing from temporal: the live datom of a
   `:db/noHistory` attribute and the live datom of a card-many one. That is
   precisely why `dbu/distinct-datoms` forms the history view as temporal UNION
   `(current filtered to no-history? OR multival?)` — the union exists to put
   those two classes back.

   Only the FINAL record of each run is dropped, never every assertion in it.
   The difference is reachable: a card-many value retracted and then re-asserted
   has an EARLIER assertion which datahike does keep in temporal (the retraction
   put it there). Dropping every live-keyed assertion loses it — measured, on a
   database carrying that shape, as one missing datom out of 37."
  [exclude-attr? sorted-records]
  (mapcat (fn [run]
            (let [final (last run)]
              (if (and (nth final 4) (exclude-attr? (nth final 1)))
                (butlast run)
                run)))
          (eav-runs sorted-records)))

(defn split-current
  "Partition `history-records` into `{:current [...] :history [...]}`.

   `:history` is every record, unchanged. NOTE that this is NOT what the temporal
   trees hold — they omit the live datom of every `:db/noHistory` and card-many
   attribute, for which see `temporal-from-eavt-sorted`. This function predates
   that finding and is kept only for tests and small inputs; a bulk build that
   fed `:history` to the temporal trees would over-count them (measured: 55
   records against a real temporal tree of 37).

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
