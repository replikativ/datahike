(ns datahike.js.api
  "JavaScript API for Datahike with Promise conversion and data transformation"
  (:refer-clojure :exclude [filter])
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.api.impl]
            [datahike.store] ;; Register :mem backend
            [konserve.node-filestore] ;; Register :file backend for Node.js
            [datahike.db.interface]
            [datahike.datom]
            [cljs.core.async :refer [<!]]
            [clojure.string :as str]
            [clojure.walk :as walk]
            [goog.object :as gobj])
  (:require-macros [cljs.core.async.macros :refer [go]]
                   [datahike.js.api-macros :refer [emit-js-api]]))

;; =============================================================================
;; Data Conversion Helpers
;; =============================================================================

(def ^:private uuid-regex
  "Regex pattern for UUID strings (8-4-4-4-12 hex digits)"
  #"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

(defn- uuid-string?
  "Check if a string is a valid UUID format"
  [s]
  (and (string? s) (re-matches uuid-regex s)))

(defn- convert-string
  "Convert a string to appropriate Clojure type:
  - ':keyword' -> keyword
  - UUID format -> UUID object
  - otherwise -> string"
  [s]
  (cond
    (str/starts-with? s ":") (keyword (subs s 1))
    (uuid-string? s) (uuid s)
    :else s))

(defn js->clj-recursive
  "Recursively convert JS objects to Clojure data with keyword keys.
  Also converts strings like ':keyword' to keywords and UUID strings to UUIDs."
  [x]
  (cond
    ;; Check for JS object first (but not arrays, functions, or null)
    (and (object? x)
         (not (array? x))
         (not (fn? x))
         (not (nil? x)))
    (into {} (for [k (js-keys x)]
               (let [v (gobj/get x k)
                     ;; Always convert keys to keywords (not just if they start with ":")
                     k-kw (keyword k)]
                 [k-kw (js->clj-recursive v)])))

    ;; Arrays become vectors
    (array? x)
    (mapv js->clj-recursive x)

    ;; Strings: convert keywords, UUIDs, or pass through
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

    ;; Datoms - pass through unchanged
    (= (type x) datahike.datom.Datom)
    x

    ;; Connections (check for typical connection keys)
    (and (map? x) (:conn-atom x))
    x

    ;; Keywords become strings with ":"
    (keyword? x)
    (str x)

    ;; Maps become JS objects
    (map? x)
    (let [obj (js-obj)]
      (doseq [[k v] x]
        (gobj/set obj
                  (if (keyword? k) (name k) (str k))
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
