(ns datahike.js.convert
  "The JavaScript boundary's value conversion, shared by every npm entry:
   the full API, the Kabel replica and the thin HTTP client."
  (:require [datahike.db.interface]
            [datahike.datom]
            [datahike.remote :as remote]
            [cljs.core.async :refer [<!]]
            [clojure.string :as str]
            [goog.object :as gobj])
  (:require-macros [cljs.core.async.macros :refer [go]]))

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
    ;; UUIDs and ClojureScript atoms are opaque API values. In particular, the
    ;; Kabel browser entry returns its peer as an atom which is then placed in
    ;; writer.local-peer. Walking either value as a generic JavaScript object
    ;; corrupts it before the connector sees the config.
    (or (instance? UUID x)
        (satisfies? IDeref x))
    x

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

    ;; Handles of the thin HTTP client (a remote connection, database or
    ;; entity) are opaque to JavaScript: they go back to the server as they
    ;; came. A record is a map, so this has to come before the map clause.
    (some? (remote/remote-peer x))
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

