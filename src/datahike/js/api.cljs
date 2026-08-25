(ns datahike.js.api
  "JavaScript API for Datahike with Promise conversion and data transformation"
  (:refer-clojure :exclude [filter uuid])
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.api.impl]
            [datahike.connector]
            [datahike.optimistic :as optimistic]
            [datahike.store] ;; Register :mem backend
            [datahike.db.interface]
            [datahike.datom]
            [cljs.core.async :refer [<!]]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [goog.object :as gobj])
  (:require-macros [cljs.core.async.macros :refer [go]]
                   [datahike.js.api-macros :refer [emit-js-api]]))

;; Register Node.js file backend - conditional require
;; For Node.js: konserve.node-filestore is added to shadow-cljs :entries
;; For browser: module is excluded from build
(when (and (exists? js/require)
           (fn? js/require))
  (try
    (js/require "./konserve.node_filestore")
    (catch :default _ nil)))

;; =============================================================================
;; Data Conversion Helpers
;;
;; Universal EDN Conversion Rules (consistent across Python, JavaScript, Java):
;; - Keys: always keywordized
;; - Values: ":" prefix = keyword, else literal
;; - Escape: "\\:" for literal colon strings
;; - UUIDs: no auto-detection; use d.uuid(str) or d.randomUuid() explicitly
;; =============================================================================

(defn- convert-string
  "Convert a string to appropriate Clojure type following universal EDN rules:
  - '\\:literal' -> ':literal' (escaped colon becomes literal string with :)
  - ':keyword' -> keyword
  - otherwise -> string (use d.uuid() / d.randomUuid() for UUID values)"
  [s]
  (cond
    ;; Escaped colon - strip backslash and return literal string
    (str/starts-with? s "\\:") (subs s 1)
    ;; Colon prefix - convert to keyword
    (str/starts-with? s ":") (keyword (subs s 1))
    ;; Regular string - pass through unchanged
    :else s))

(declare clj->js-recursive)

(defn js->clj-recursive
  "Recursively convert JS objects to Clojure data with keyword keys.
  Converts strings like ':keyword' to keywords.
  UUID values must be created explicitly with d.uuid() or d.randomUuid()."
  [x]
  (cond
    ;; Dates are API values (temporal queries and GC cutoffs), not option maps.
    ;; Preserve them before the generic JavaScript-object conversion below.
    (instance? js/Date x)
    x

    ;; Check for JS object first (but not arrays, functions, or null)
    (and (object? x)
         (not (array? x))
         (not (fn? x))
         (not (nil? x)))
    (into {} (for [k (js-keys x)]
               (let [v (gobj/get x k)
                     ;; Convert keys to keywords, stripping leading : if present
                     k-kw (if (str/starts-with? k ":")
                            (keyword (subs k 1))
                            (keyword k))]
                 [k-kw (js->clj-recursive v)])))

    ;; Arrays become vectors
    (array? x)
    (mapv js->clj-recursive x)

    ;; JS functions: wrap to convert Clojure args to JS before calling
    ;; This ensures filter/listen callbacks receive plain JS objects, not raw
    ;; ClojureScript data (e.g. Datom records with renamed fields under advanced
    ;; compilation, Clojure persistent maps inaccessible via JS property access).
    (fn? x)
    (fn [& args]
      (apply x (map clj->js-recursive args)))

    ;; Strings: convert keywords or pass through
    (string? x)
    (convert-string x)

    ;; Everything else passes through
    :else x))

(defn clj->js-recursive
  "Recursively convert Clojure data to JS objects.
  Converts keywords to strings with ':' prefix.
  Datahike objects (DB, connections, datoms) pass through unchanged."
  [x]
  (cond
    ;; Datahike DB objects - pass through unchanged
    (satisfies? datahike.db.interface/IDB x)
    x

    ;; Datoms - convert to plain JS objects with stable property names.
    ;; Passing datoms through unchanged causes field renaming under advanced
    ;; compilation (e.g. :v becomes "ca"), breaking JS callers who access .e/.a/.v.
    (= (type x) datahike.datom.Datom)
    (let [^datahike.datom.Datom datom x
          obj (js-obj)]
      (gobj/set obj "e" (.-e datom))
      (gobj/set obj "a" (clj->js-recursive (.-a datom)))
      (gobj/set obj "v" (clj->js-recursive (.-v datom)))
      (gobj/set obj "tx" (.-tx datom))
      (gobj/set obj "added" (.-added datom))
      obj)

    ;; Connections (check for typical connection keys)
    (and (map? x) (:conn-atom x))
    x

    ;; UUID objects become plain strings (round-trip friendly)
    (instance? UUID x)
    (str x)

    ;; Keywords become strings with ":"
    (keyword? x)
    (str x)

    ;; Maps become JS objects
    (map? x)
    (let [obj (js-obj)]
      (doseq [[k v] x]
        (gobj/set obj
                  (if (keyword? k) (subs (str k) 1) (str k))
                  (clj->js-recursive v)))
      obj)

    ;; Sequential collections become arrays
    (sequential? x)
    (into-array (map clj->js-recursive x))

    ;; Sets become arrays
    (set? x)
    (into-array (map clj->js-recursive x))

    ;; Everything else passes through
    :else x))

(defn js->clj-api-args
  "Convert JavaScript arguments and restore collection semantics that plain
  JavaScript cannot express.

  Arrays normally map to vectors because that is the useful representation for
  transactions, query forms, pull selectors, and tuple values. Versioning
  parents are the deliberate exception: the Clojure API models them as a set of
  branch names and/or commit ids. Coerce only those schema positions instead of
  guessing globally and turning unrelated arrays into sets."
  [operation args]
  (let [converted (mapv js->clj-recursive args)]
    (case operation
      "force-branch!" (update converted 2 set)
      "merge-db!" (update converted 1 set)
      converted)))

;; =============================================================================
;; Async/Promise Conversion
;; =============================================================================

(defn maybe-chan->promise
  "Convert a core.async channel to a Promise, or return value directly if not a channel.
  This handles the dynamic async/sync execution in Datahike API.

  Errors returned on the channel (not thrown) are properly rejected by checking
  if the result is a js/Error or ExceptionInfo."
  [x]
  (if (satisfies? cljs.core.async.impl.protocols/Channel x)
    (js/Promise.
     (fn [resolve reject]
       (go
         (try
           (let [result (<! x)]
             ;; Check if result is an error object - reject promise if so
             (if (or (instance? js/Error result)
                     (instance? ExceptionInfo result))
               (reject result)
               (resolve result)))
           (catch :default e
             ;; Catch any exceptions thrown during channel operations
             (reject e))))))
    (js/Promise.resolve x)))

;; =============================================================================
;; Generate All API Functions
;; =============================================================================

(emit-js-api)

;; =============================================================================
;; Additional JS-specific Helpers
;; =============================================================================

(defn ^:export isPromise
  "Check if a value is a Promise."
  [x]
  (instance? js/Promise x))

(defn ^:export uuid
  "Create a Datahike UUID value from a string.
  Use this when transacting or querying :db.type/uuid attributes, and
  for the store config :id field.

  UUID strings are never auto-detected — wrap them explicitly.
  UUID values read back from the database are returned as plain strings.

  Examples:
    // Store config
    { store: { backend: ':memory', id: d.uuid('00000000-0000-0000-0000-000000000001') } }
    // Data attribute
    await d.transact(conn, [{ ':item/id': d.uuid('550e8400-e29b-41d4-a716-446655440000') }])
    // Query returns plain string: '550e8400-e29b-41d4-a716-446655440000'"
  [s]
  (cljs.core/uuid s))

(defn ^:export randomUuid
  "Generate a random UUID value, suitable for use as a store config :id
  or any :db.type/uuid attribute.

  Example:
    { store: { backend: ':memory', id: d.randomUuid() } }"
  []
  (random-uuid))

;; =============================================================================
;; Explicit optimistic overlay API
;; =============================================================================

(defn- overlay-id [x]
  (if (string? x) (uuid x) x))

(defn- result->promise [result-ch]
  (js/Promise.
   (fn [resolve _reject]
     (cljs.core.async/take!
      result-ch
      (fn [result]
        ;; Overlay outcomes are tagged values, including rejection. Keeping
        ;; them on the resolved path prevents JS from losing the distinction
        ;; between a refused operation and an unknown durable outcome.
        (resolve (clj->js-recursive result)))))))

(defn- overlay-handle->js [{:keys [ov-id result]}]
  (let [out (js-obj)]
    (gobj/set out "ovId" (str ov-id))
    (gobj/set out "result" (result->promise result))
    out))

(defn ^:export openOptimistic
  "Open an explicit optimistic overlay. The returned handle is synchronous and
  must eventually be passed to closeOptimistic."
  ([conn] (optimistic/open conn))
  ([conn opts] (optimistic/open conn (js->clj-recursive opts))))

(defn ^:export optimisticDb
  "Return the overlay's current effective Database snapshot synchronously."
  [overlay]
  (optimistic/db overlay))

(defn ^:export optimisticPending
  "Return public metadata for the overlay's pending entries."
  [overlay]
  (clj->js-recursive (optimistic/pending overlay)))

(defn ^:export optimisticTransact
  "Submit a writer-backed optimistic transaction and return {ovId, result}."
  ([overlay tx-data]
   (overlay-handle->js
    (optimistic/transact! overlay (js->clj-recursive tx-data))))
  ([overlay tx-data opts]
   (overlay-handle->js
    (optimistic/transact! overlay
                          (js->clj-recursive tx-data)
                          (js->clj-recursive opts)))))

(defn ^:export optimisticPredict
  "Add an externally-owned prediction and return {ovId, result}.
  reconciled must synchronously return a boolean for a Database snapshot."
  ([overlay tx-data reconciled]
   (optimisticPredict overlay tx-data reconciled nil))
  ([overlay tx-data reconciled opts]
   (let [predicate (when (fn? reconciled)
                     (fn [db]
                       (let [answer (reconciled db)]
                         (when (instance? js/Promise answer)
                           (throw (js/Error.
                                   "optimisticPredict reconciliation callbacks must be synchronous")))
                         (boolean answer))))]
     (overlay-handle->js
      (optimistic/predict! overlay
                           (js->clj-recursive tx-data)
                           predicate
                           (if opts (js->clj-recursive opts) {}))))))

(defn ^:export optimisticAck
  "Mark an external prediction accepted without retracting it."
  ([overlay ov-id] (optimistic/ack! overlay (overlay-id ov-id)))
  ([overlay ov-id receipt]
   (optimistic/ack! overlay (overlay-id ov-id) (js->clj-recursive receipt))))

(defn ^:export optimisticReject
  "Reject and immediately retract an external prediction."
  [overlay ov-id error]
  (optimistic/reject! overlay (overlay-id ov-id) error))

(defn ^:export optimisticAbandon
  "Explicitly retract a prediction whose owner no longer wants to reconcile it."
  ([overlay ov-id] (optimistic/abandon! overlay (overlay-id ov-id)))
  ([overlay ov-id reason]
   (optimistic/abandon! overlay (overlay-id ov-id) (js->clj-recursive reason))))

(defn ^:export optimisticListen
  "Subscribe to ordered snapshot transitions. Returns an unsubscribe function."
  [overlay listener]
  (let [key (random-uuid)]
    (optimistic/listen! overlay key #(listener (clj->js-recursive %)))
    (fn [] (optimistic/unlisten! overlay key))))

(defn ^:export optimisticListenStatus
  "Subscribe to per-entry lifecycle events. Returns an unsubscribe function."
  [overlay listener]
  (let [key (random-uuid)]
    (optimistic/listen-status! overlay key #(listener (clj->js-recursive %)))
    (fn [] (optimistic/unlisten-status! overlay key))))

(defn ^:export closeOptimistic
  "Close an overlay, retract its predictions, and detach its connection watch."
  [overlay]
  (optimistic/close! overlay))
