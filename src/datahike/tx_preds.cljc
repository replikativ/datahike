(ns ^:no-doc datahike.tx-preds
  "EXPERIMENTAL / internal. Store-level, MANDATORY, whole-transaction predicate —
   the `tx`-level member of the predicate family, complementing `:db.attr/preds`
   (per value, on assertion) and `:db.entity/preds` (per entity, opt-in via
   `:db/ensure`).

   A tx-pred is `(fn [tx-report] …)` run on the FULLY-RESOLVED report of every
   committed write to a governed store — `{:db-before :db-after :tx-data
   :datahike/tx-ops …}` with real eids and added/retracted datoms. This qualified
   key is an internal predicate input; it is removed before the report crosses a
   public API boundary. The predicate returns anything on success; a thrown
   Exception (NOT an Error/AssertionError — an Error crashes the writer) makes the
   writer reject the transaction: the error is delivered to the caller, the chain
   does not advance, nothing is persisted.

   Unlike `:db/ensure`, it fires on EVERY write regardless of the transaction's
   shape — the trust-boundary property a governed store needs — and because it
   sees the resolved `:tx-data` (with retract flags) it can also guard destructive
   ops. `:datahike/tx-ops` is the set of operation keywords actually interpreted
   by the transactor, including operations expanded from transaction functions.
   It is distinct from the datom delta: a history purge may leave no retraction
   datom, so a predicate can use it to decide when a broader before/after audit
   is required. From the report a consumer can reconstruct the four invariant
   sources it needs ($before/$after/$empty+txs/$txs), post-resolution.

   Referenced OUT OF BAND — keyed by store-id in a process-local registry, never
   placed in the (serialized) config. Register on the writer process after connect
   (same discipline as `datahike.attr-preds`):

     (register-tx-pred! store-id (fn [report] … throw to reject))

   Predicates are registered by name so independent consumers can govern the
   same store without replacing one another. The two-argument registration API
   retains the original unnamed/default slot for compatibility.")

(defonce ^:private registry (atom {}))

(def ^:private default-pred-id ::default)

(defn register-tx-pred!
  "Register tx-pred `f` = `(fn [tx-report] …)` for `store-id`. Runs on every
   committed write to that store; throw an Exception to reject. The three-arg
   form registers an independently removable named predicate. Returns store-id."
  ([store-id f]
   (register-tx-pred! store-id default-pred-id f))
  ([store-id pred-id f]
   (assert (ifn? f) "tx-pred must be a function")
   (swap! registry assoc-in [store-id pred-id] f)
   store-id))

(defn ensure-tx-pred!
  "Install the named predicate only when its slot is empty. Returns :installed
   or :present when the identical function is already registered. A different
   function at the same id is an error; callers cannot replace another guard
   while establishing their own writer invariant."
  [store-id pred-id f]
  (assert (ifn? f) "tx-pred must be a function")
  (loop []
    (let [registered @registry
          existing (get-in registered [store-id pred-id])]
      (cond
        (identical? existing f) :present
        (some? existing)
        (throw (ex-info "A different transaction predicate is registered at this id"
                        {:type :tx-pred/id-collision
                         :store-id store-id :pred-id pred-id}))
        (compare-and-set! registry registered
                          (assoc-in registered [store-id pred-id] f))
        :installed
        :else (recur)))))

(defn unregister-tx-pred!
  ([store-id]
   (unregister-tx-pred! store-id default-pred-id))
  ([store-id pred-id]
   (swap! registry
          (fn [registered]
            (let [remaining (dissoc (get registered store-id) pred-id)]
              (if (seq remaining)
                (assoc registered store-id remaining)
                (dissoc registered store-id)))))
   nil))

(defn tx-pred-for
  "The tx-pred fn registered for `store-id`, or nil."
  [store-id]
  (get-in @registry [store-id default-pred-id]))

(defn tx-preds-for
  "The named transaction predicates registered for `store-id`."
  [store-id]
  (get @registry store-id {}))

(defn check-report
  "Run the store's tx-pred (if any) on `tx-report`. Throws to reject; returns the
   public report on pass. store-id is read from the report's db-after config,
   so it applies to both local and remote (kabel) writers. The provenance set is
   bounded by the small operation vocabulary; an ungoverned store still pays
   only that collection plus a single registry lookup."
  [tx-report]
  (doseq [[_ f] (sort-by (comp pr-str key)
                         (tx-preds-for
                          (get-in tx-report [:db-after :config :store :id])))]
    (f tx-report))
  ;; Operation provenance is a writer-side governance aid, not part of the
  ;; Datomic-shaped transaction report contract. Strip it once, after the
  ;; predicate has inspected the fully resolved report and before callbacks,
  ;; listeners, remote transports, or callers can observe it.
  (dissoc tx-report :datahike/tx-ops))
