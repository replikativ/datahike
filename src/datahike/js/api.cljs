(ns datahike.js.api
  "JavaScript API for Datahike with Promise conversion and data transformation"
  (:refer-clojure :exclude [filter uuid])
  (:require [datahike.api.impl]
            [datahike.connector]
            [datahike.optimistic :as optimistic]
            [datahike.store] ;; Register :mem backend
            [datahike.js.convert :as convert]
            [clojure.string :as str]
            [goog.object :as gobj]
            [taoensso.trove :as trove]
            [taoensso.trove.console :as trove-console])
  (:require-macros [cljs.core.async.macros :refer [go]]
                   [datahike.js.api-macros :refer [emit-js-api]]))

;; The Trove console backend deliberately defaults to logging every level in
;; ClojureScript. That is useful during development, but makes the installed npm
;; package print internal trace events during ordinary database operations.
;; Keep the package quiet unless the application explicitly opts into more
;; detail through setLogLevel() or DATAHIKE_LOG_LEVEL.
(def ^:private default-log-level "warn")
(def ^:private supported-log-levels
  #{"off" "trace" "debug" "info" "warn" "error"})

(defn- normalize-log-level [level]
  (when (or (string? level) (keyword? level))
    (-> (name level)
        (str/replace #"^:" "")
        str/lower-case)))

(defn- configure-log-level! [level]
  (let [normalized (normalize-log-level level)]
    (when-not (contains? supported-log-levels normalized)
      (throw (js/Error.
              (str "Unsupported Datahike log level: " level
                   ". Expected one of: off, trace, debug, info, warn, error."))))
    (if (= "off" normalized)
      (trove/set-log-fn! nil)
      (trove/set-log-fn!
       (trove-console/get-log-fn {:min-level (keyword normalized)})))
    normalized))

(defn- environment-log-level []
  (let [process-env (when (exists? js/process)
                      (gobj/get js/process "env"))
        configured (when process-env
                     (gobj/get process-env "DATAHIKE_LOG_LEVEL"))
        normalized (normalize-log-level configured)]
    ;; An unrelated or malformed environment value should never make importing
    ;; a database library fail. The explicit setter remains strict.
    (if (contains? supported-log-levels normalized)
      normalized
      default-log-level)))

(configure-log-level! (environment-log-level))

(defn ^:export setLogLevel
  "Set logging for the JavaScript package.

  Accepted levels are off, trace, debug, info, warn, and error. Returns the
  normalized level name. Node.js applications may set the initial level with
  the DATAHIKE_LOG_LEVEL environment variable before importing Datahike."
  [level]
  (configure-log-level! level))

;; Register Node.js file backend - conditional require
;; For Node.js: konserve.node-filestore is added to shadow-cljs :entries
;; For browser: module is excluded from build
(when (and (exists? js/require)
           (fn? js/require))
  (try
    (js/require "./konserve.node_filestore")
    (catch :default _ nil)))

;; =============================================================================
;; Data Conversion Helpers — in `datahike.js.convert`, shared with the other
;; npm entries; the names stay here for the generated API and `datahike.js.kabel`.
;; =============================================================================

(def js->clj-recursive convert/js->clj-recursive)
(def clj->js-recursive convert/clj->js-recursive)
(def js->clj-api-args convert/js->clj-api-args)
(def maybe-chan->promise convert/maybe-chan->promise)

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
