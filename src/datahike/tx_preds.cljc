(ns ^:no-doc datahike.tx-preds
  "EXPERIMENTAL / internal. Store-level, MANDATORY, whole-transaction predicate —
   the `tx`-level member of the predicate family, complementing `:db.attr/preds`
   (per value, on assertion) and `:db.entity/preds` (per entity, opt-in via
   `:db/ensure`).

   A tx-pred is `(fn [tx-report] …)` run on the FULLY-RESOLVED report of every
   committed write to a governed store — `{:db-before :db-after :tx-data …}` with
   real eids and added/retracted datoms. It returns anything on success; a thrown
   Exception (NOT an Error/AssertionError — an Error crashes the writer) makes the
   writer reject the transaction: the error is delivered to the caller, the chain
   does not advance, nothing is persisted.

   Unlike `:db/ensure`, it fires on EVERY write regardless of the transaction's
   shape — the trust-boundary property a governed store needs — and because it
   sees the resolved `:tx-data` (with retract flags) it can also guard destructive
   ops. From the report a consumer can reconstruct the four invariant sources it
   needs ($before/$after/$empty+txs/$txs), post-resolution.

   Referenced OUT OF BAND — keyed by store-id in a process-local registry, never
   placed in the (serialized) config. Register on the writer process after connect
   (same discipline as `datahike.attr-preds`):

     (register-tx-pred! store-id (fn [report] … throw to reject))

   Signature is intentionally minimal (one fn per store) and NOT a stable public
   API yet; a named-predicate list and a symbol-in-data wiring are possible later.")

(defonce ^:private registry (atom {}))

(defn register-tx-pred!
  "Register tx-pred `f` = `(fn [tx-report] …)` for `store-id`. Runs on every
   committed write to that store; throw an Exception to reject. Returns store-id."
  [store-id f]
  (assert (ifn? f) "tx-pred must be a function")
  (swap! registry assoc store-id f)
  store-id)

(defn unregister-tx-pred!
  [store-id]
  (swap! registry dissoc store-id)
  nil)

(defn tx-pred-for
  "The tx-pred fn registered for `store-id`, or nil."
  [store-id]
  (get @registry store-id))

(defn check-report
  "Run the store's tx-pred (if any) on `tx-report`. Throws to reject; returns the
   report unchanged on pass. store-id is read from the report's db-after config,
   so it applies to both local and remote (kabel) writers. A nil lookup (no
   tx-pred registered) is a single map read — ungoverned stores pay nothing."
  [tx-report]
  (when-let [f (tx-pred-for (get-in tx-report [:db-after :config :store :id]))]
    (f tx-report))
  tx-report)
