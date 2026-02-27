(ns datahike.js.api
  "JavaScript API for Datahike with Promise conversion and data transformation"
  (:refer-clojure :exclude [filter])
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.api.impl]
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

(defn js->clj-recursive
  "Recursively convert JS objects to Clojure data with keyword keys.
  Converts strings like ':keyword' to keywords.
  UUID values must be created explicitly with d.uuid() or d.randomUuid()."
  [x]
  (cond
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
    (let [obj (js-obj)]
      (gobj/set obj "e" (.-e x))
      (gobj/set obj "a" (clj->js-recursive (.-a x)))
      (gobj/set obj "v" (clj->js-recursive (.-v x)))
      (gobj/set obj "tx" (.-tx x))
      (gobj/set obj "added" (.-added x))
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
