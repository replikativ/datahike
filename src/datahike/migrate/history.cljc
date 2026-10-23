(ns ^:no-doc datahike.migrate.history
  "Deriving the CURRENT datom set from a full history.

   A `:keep-history? true` database keeps six trees: `temporal-eavt/aevt/avet`
   hold every datom ever asserted or retracted, while `eavt/aevt/avet` hold only
   those currently true. A dump carries the history; a bulk-build import has to
   reconstruct both, which means deciding which datoms are current WITHOUT
   replaying transactions. The streaming import replays them, but has the same
   question to answer for records it cannot order (see
   `datahike.db.transaction/transact-entities-directly`), and answers it with
   `tx-plan` below — so the two paths agree.

   ## The common case is simple

   Fold the history in transaction order, keyed by `[e a v]`: an assertion adds
   that exact datom, a retraction removes it. datahike writes an EXPLICIT
   retraction of the old value even for a cardinality-one overwrite —
   `{:db/id e :score 10}` over `1` records `[e :score 1 t false]` and
   `[e :score 10 t true]` — so there is never an implicit supersede to model.

   ## ...until one transaction touches one `[e a v]` twice

   datahike records an `[e a v]` both ASSERTED and RETRACTED in one transaction:
   `[[:db/retract e :tag :x] [:db/add e :tag :x]]`, `[[:db/add e :tag :x]
   [:db/retract e :tag :x]]`, or a cardinality-one attribute set twice (`25`
   then `21` over `24` records `[24 false] [25 true] [25 false] [21 true]`). The
   history is a SET: it does not say which of the two came first, nor how often
   either happened (`add, retract, add` records the same two datoms as `add,
   retract`). So the outcome is not always recoverable from history alone.

   What the history does determine, and the rule `tx-plan` applies:

   - an `[e a v]` only asserted in the transaction is current after it, and one
     only retracted is not;
   - **cardinality-one:** at most one value is current after a transaction. A
     value only asserted is certainly the survivor (any later assertion of
     another value would have retracted it), so it wins over every value both
     asserted and retracted. Without one, a value present before and both
     retracted and re-asserted is taken to survive; failing that, the LAST value
     both asserted and retracted, in record order (`v` order for a dump).
   - **cardinality-many:** values are independent; one both asserted and
     retracted is current after the transaction.

   The last two cases are genuinely ambiguous, and the choices are the ones the
   import made before this rule existed, where it got them right:
   `[[:db/add e :tag :x] [:db/retract e :tag :x]]` over an entity WITHOUT `:x`
   restores `:x` as present (it is absent in the source), and `[retract :x, add
   :x, retract :x]` over one WITH `:x` restores it as present too. For
   cardinality-one, `25, 21, 25` over `24` and `25, 21, retract 21` over `24`
   record the same set, and this picks `25`; `25, retract 25` over `24` restores
   `25` as current. Every such shape needs an explicit retraction of a value the
   same transaction asserted, or a value revisited within one transaction. The
   history (as a set of datoms) is always restored exactly; only which of those
   datoms are CURRENT can differ, and only in these shapes.

   The real fix belongs in the export: a `:history? true` dump that marks which
   assertions are current would make currentness a read rather than an inference.

   ## Order is not assumed within a transaction

   Every decision here reads a transaction's records for one `[e a]` as a set,
   keyed by `value-key` (byte arrays compare by identity, so a `:db.type/bytes`
   value is keyed by its contents).

   Verified against a database exercising card-one overwrite, card-many
   add/retract, retract-then-reassert of the same value, `retractEntity`, refs to
   retracted entities, schema added mid-history, `:db/noHistory`, the
   same-transaction shapes above, plus randomised histories. See
   `datahike.test.migrate-history-test`.")

(defn value-key
  "A value-equality key for `v`. Byte arrays compare by identity, so two decoded
   copies of one `:db.type/bytes` value would never be recognised as the same
   datom; they are keyed by contents."
  [v]
  #?(:clj (if (bytes? v) (vec v) v)
     :cljs (if (instance? js/Uint8Array v) (vec (js/Array.from v)) v)))

(defn tx-plan
  "The outcome of ONE transaction's `records` for one `[e a]`, as a vector of
   per-value entries in first-appearance order:

     {:k value-key :a assertion-or-nil :r retraction-or-nil
      :before? present-before :after? present-after}

   `present?` answers, for a record, whether its `[e a v]` was current before the
   transaction. See the namespace doc for the rule and its limits."
  [records card-many? present?]
  (let [entries (persistent!
                 (reduce (fn [m r]
                           (let [k (value-key (nth r 2))
                                 e (get m k {:k k})]
                             (assoc! m k (if (nth r 4)
                                           (cond-> e (nil? (:a e)) (assoc :a r))
                                           (cond-> e (nil? (:r e)) (assoc :r r))))))
                         (transient {})
                         records))
        ;; first-appearance order, which `transient` maps do not keep
        order (distinct (map #(value-key (nth % 2)) records))
        entries (mapv (fn [k] (let [e (get entries k)]
                                (assoc e :before? (boolean (present? (or (:a e) (:r e)))))))
                      order)]
    (if card-many?
      (mapv (fn [e] (assoc e :after? (cond (:a e) true
                                           (:r e) false
                                           :else (:before? e))))
            entries)
      (let [only-asserted (filter #(and (:a %) (not (:r %))) entries)
            both (filter #(and (:a %) (:r %)) entries)
            winner (or (last only-asserted)
                       (first (filter :before? both))
                       (last both))
            winner-k (:k winner)]
        (mapv (fn [e] (assoc e :after? (if winner
                                         (= winner-k (:k e))
                                         (and (:before? e) (not (:r e))))))
              entries)))))

(defn- tx-order
  "Sort key: transaction, entity, attribute, value, then op with RETRACTION
   before assertion. Attribute and value are stringified because neither is
   mutually Comparable in general; this needs a stable total order, not a
   meaningful one."
  [datom]
  [(nth datom 3) (nth datom 0) (str (nth datom 1)) (str (value-key (nth datom 2)))
   (if (nth datom 4) 1 0)])

(defn- eav-runs
  "Group records sorted by `[e a v t]` into consecutive runs sharing `[e a v]`.

   The one structural fact the streaming functions below rest on: that sort puts
   every record for one `[e a v]` ADJACENT and in transaction order, so each run
   is folded on its own. Memory is one run, not a set of every live datom.
   Values are compared by `value-key`."
  [sorted-records]
  (partition-by (fn [r] [(nth r 0) (nth r 1) (value-key (nth r 2))]) sorted-records))

(defn- has-pair?
  "Does a run of one `[e a]`, sorted by `[v t]`, hold an `[e a v]` asserted and
   retracted in the same transaction? Such records are adjacent."
  [run]
  (some (fn [[x y]] (and (= (nth x 3) (nth y 3))
                         (not= (boolean (nth x 4)) (boolean (nth y 4)))
                         (= (value-key (nth x 2)) (value-key (nth y 2)))))
        (partition 2 1 run)))

(defn card-one-losers
  "The `[e a value-key t]` of every cardinality-ONE value asserted and retracted
   in transaction `t` that is NOT current after it, because another value of the
   same `[e a]` survived that transaction (see `tx-plan`).

   This is the one decision that cannot be made from a value's own records: it
   needs the attribute's other values in the same transaction, which are adjacent
   only in an order that groups `[e a]` — eavt or aevt, not avet. So it is
   computed once, from such an order, and handed to every family's fold. Only
   `[e a]` runs that contain a same-transaction pair are folded, so the set is as
   large as the number of such values, not the database."
  [card-many? sorted-records]
  (persistent!
   (reduce
    (fn [acc run]
      (let [[e a] (first run)]
        (if (or (card-many? a) (not (has-pair? run)))
          acc
          (loop [groups (sort-by key (group-by #(nth % 3) run)) state #{} acc acc]
            (if-let [[t recs] (first groups)]
              (let [plan (tx-plan recs false #(contains? state (value-key (nth % 2))))]
                (recur (next groups)
                       (reduce (fn [s en] (if (:after? en) (conj s (:k en)) (disj s (:k en))))
                               state plan)
                       (reduce (fn [acc en]
                                 (if (and (:a en) (:r en) (not (:after? en)))
                                   (conj! acc [e a (:k en) t])
                                   acc))
                               acc plan)))
              acc)))))
    (transient #{})
    (partition-by (fn [r] [(nth r 0) (nth r 1)]) sorted-records))))

(defn- run-live
  "`[live-record out-of-temporal?]` for one `[e a v]` run sorted by `t`, or nil
   when the datom is not current.

   Per transaction: assertion only -> current; retraction only -> not; both ->
   current, unless `losers` names it (cardinality-one, see `card-one-losers`).

   `out-of-temporal?` says whether datahike keeps the live datom out of the
   temporal trees, which it does for a cardinality-many assertion that went
   straight to the current index: asserted alone, or retracted and re-asserted.
   One that passed through temporal first (asserted, retracted, asserted) is in
   both. Cardinality-one assertions always reach temporal."
  [losers card-many? run]
  (let [[e a v] (first run)
        k (value-key v)]
    (reduce (fn [state group]
              (let [asserted (some #(when (nth % 4) %) group)
                    retracted? (some #(not (nth % 4)) group)]
                (cond (and asserted retracted?)
                      (if (and (not card-many?) (contains? losers [e a k (nth asserted 3)]))
                        nil
                        [asserted (and card-many? (some? state))])
                      asserted [asserted card-many?]
                      retracted? nil
                      :else state)))
            nil
            (partition-by #(nth % 3) run))))

(defn current-from-eavt-sorted
  "STREAMING currentness: given records sorted by `[e a v t]` within each
   `[e a v]` (any family order keeps that), return a lazy seq of the records that
   are currently true, in that same order.

   `card-many?` answers per attribute; `losers` comes from `card-one-losers`.
   The 1-arity treats every attribute as cardinality-many (the fold without
   cross-value decisions); the 2-arity computes `losers` from the same input,
   which must then also group `[e a]` and is realised twice — for tests."
  ([sorted-records]
   (current-from-eavt-sorted (constantly true) #{} sorted-records))
  ([card-many? sorted-records]
   (current-from-eavt-sorted card-many? (card-one-losers card-many? sorted-records)
                             sorted-records))
  ([card-many? losers sorted-records]
   (keep (fn [run]
           (first (run-live losers (card-many? (nth (first run) 1)) run)))
         (eav-runs sorted-records))))

(defn temporal-from-eavt-sorted
  "STREAMING: given records sorted by `[e a v t]`, the subset the TEMPORAL trees
   hold.

   The temporal trees are NOT \"every record\", which is the obvious reading and
   is wrong. Derived from the transactor rather than assumed — see `with-datom`
   and `with-datom-upsert`:

     - card-one goes through the upsert path, whose `temporal-upsert` conj's
       every assertion into temporal INCLUDING the currently-live one;
     - a plain card-many assertion touches only the current trees;
     - a retraction puts both the removed assertion and the retraction marker
       into temporal;
     - a `:db/noHistory` attribute never reaches temporal at all.

   So the live datom of a `:db/noHistory` attribute is dropped, and the live
   datom of a card-many one when `run-live` says it never passed through
   temporal. Only the LIVE record of a run is ever dropped: a card-many value
   retracted and then re-asserted has an EARLIER assertion datahike keeps in
   temporal.

   The 2-arity is the older interface: `excluded?` attributes drop their live
   datom whenever there is one."
  ([excluded? sorted-records]
   (mapcat (fn [run]
             (let [[live] (run-live #{} true run)]
               (if (and live (excluded? (nth live 1)))
                 (remove #(identical? % live) run)
                 run)))
           (eav-runs sorted-records)))
  ([no-history? card-many? losers sorted-records]
   (mapcat (fn [run]
             (let [a (nth (first run) 1)
                   [live out?] (run-live losers (card-many? a) run)]
               (if (and live (or out? (no-history? a)))
                 (remove #(identical? % live) run)
                 run)))
           (eav-runs sorted-records))))

(defn- sorted-for-fold
  "`history-records` in an order that groups `[e a]` and then `[e a v]`, with
   transactions ascending — what the streaming folds require. Not memory
   bounded."
  [history-records]
  (sort-by (fn [r] [(str (nth r 0)) (str (nth r 1)) (str (value-key (nth r 2)))
                    (nth r 3) (if (nth r 4) 1 0)])
           history-records))

(defn- current-records
  [card-many? history-records]
  (let [sorted (sorted-for-fold history-records)]
    (current-from-eavt-sorted card-many? (card-one-losers card-many? sorted) sorted)))

(defn derive-current
  "The set of `[e a v]` triples currently true, given `history-records` as
   `[e a v t added]` tuples.

   NOT MEMORY BOUNDED — it sorts the whole input. Kept as the obvious statement
   of the rule for tests and small inputs, and to check the streaming version
   against. The 1-arity treats every attribute as cardinality-many.

   Returns a SET of triples rather than records: `t` is deliberately dropped,
   because the current set is a question about which facts hold, not about when
   they were asserted."
  ([history-records] (derive-current (constantly true) history-records))
  ([card-many? history-records]
   (into #{} (map (fn [r] [(nth r 0) (nth r 1) (nth r 2)]))
         (current-records card-many? history-records))))

(defn split-current
  "Partition `history-records` into `{:current [...] :history [...]}`.

   `:history` is every record, unchanged. NOTE that this is NOT what the temporal
   trees hold — see `temporal-from-eavt-sorted`. `:current` holds the record that
   ASSERTED each currently-true datom (its latest assertion).

   NOT MEMORY BOUNDED. Kept for tests and small inputs; the import path composes
   `current-from-eavt-sorted` with an external sort instead. The 1-arity treats
   every attribute as cardinality-many."
  ([history-records] (split-current (constantly true) history-records))
  ([card-many? history-records]
   {:current (vec (current-records card-many? history-records))
    :history (vec history-records)}))
