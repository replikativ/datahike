(ns datahike.attr-preds
  "Runtime registry + resolution for `:db.attr/preds` attribute-value predicates.

   A predicate is `(fn [value] -> truthy)` run on every assertion of the
   attribute; a non-`true` return rejects the transaction. Predicates are
   referenced from schema by **symbol** (`:db.attr/preds` is symbol-typed,
   like `:db.entity/preds`), resolved in this order:

   - the runtime registry — `(register-attr-pred! 'app/valid-sku (fn …))`,
     then `:db.attr/preds ['app/valid-sku]`. This is the cross-platform
     (clj + cljs) path. Registrations are process-local: re-register after
     every fresh `connect` (the reference persists in schema, the fn does not),
     and on the writer process for remote/kabel writers.
   - on clj only, if the symbol is not registered it is `requiring-resolve`d
     as a var (Datomic-style `:db.attr/preds ['my.ns/valid-sku?]`).

   Declarative `:db/maxLength` needs no registration — it is a built-in.")

(defonce ^:private registry (atom {}))

(defn register-attr-pred!
  "Register predicate fn `f` under key `k` so a schema `:db.attr/preds [k]`
   resolves to `f`. Returns `k`. `:db.attr/preds` is `:db.type/symbol`, so for a
   SCHEMA reference `k` must be a SYMBOL (e.g. `'app/valid-sku`) — that is the
   only shape usable as a schema value. A keyword key resolves from the registry
   too, but cannot be placed in `:db.attr/preds`; prefer symbols."
  [k f]
  (assert (ifn? f) "attribute predicate must be a function")
  (swap! registry assoc k f)
  k)

(defn unregister-attr-pred!
  "Remove the predicate registered under `k`."
  [k]
  (swap! registry dissoc k)
  nil)

(defn registered
  "Snapshot of the current predicate registry (for inspection)."
  []
  @registry)

(defn resolve-pred
  "Resolve a `:db.attr/preds` reference to a predicate fn, or `nil` if it
   cannot be resolved. `fn` -> itself; keyword -> registry; symbol ->
   registry, else (clj only) `requiring-resolve`."
  [p]
  (cond
    (fn? p)      p
    (keyword? p) (get @registry p)
    (symbol? p)  (or (get @registry p)
                     #?(:clj  (try (some-> (requiring-resolve p) deref)
                                   (catch #?(:clj Throwable :cljs :default) _ nil))
                        :cljs nil))
    :else        nil))
