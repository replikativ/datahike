(ns datahike.codegen.esm
  "Generate ESM browser wrapper from api.specification.

  The browser build of Datahike is an IIFE that attaches the API to
  globalThis['datahike']['js']['api']. This codegen produces an ES module
  wrapper (index.mjs) that re-exports every API function as a named export,
  plus a default export of the full API object.

  Why ESM matters: the previous CJS wrapper (require/module.exports) causes
  bundlers like Vite to inject a `require` shim, which can trick runtime
  environment detection into believing it's running in Node.js, leading to
  fileExistsSync errors in the browser. ESM avoids this entirely."
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.codegen.naming :refer [assert-unique-js-names!
                                             js-skip-list clj-name->js-name
                                             remote-js-exports]]
            [clojure.string :as str]))

;; Additional exports defined in datahike.js.api that are not in the
;; api-specification (manually exported with ^:export metadata).
(def ^:private extra-js-exports
  ["isPromise" "uuid" "randomUuid" "setLogLevel"
   "openOptimistic" "optimisticDb" "optimisticPending"
   "optimisticTransact" "optimisticPredict" "optimisticAck"
   "optimisticReject" "optimisticAbandon" "optimisticListen"
   "optimisticListenStatus" "closeOptimistic"])

(defn generate-esm-wrapper
  "Generate ESM wrapper source that re-exports the IIFE bundle's API.
   Returns the file content as a string."
  []
  (let [clj-exports (for [[fn-name _] (sort-by first api-specification)
                          :when (not (contains? js-skip-list fn-name))]
                      fn-name)
        _ (assert-unique-js-names! clj-exports)
        spec-exports (map clj-name->js-name clj-exports)
        all-exports (concat spec-exports extra-js-exports)
        lines (concat
               ["// Auto-generated ESM wrapper for browser bundlers (vite, rollup, esbuild)."
                "// DO NOT EDIT - Generated from datahike.api.specification"
                "//"
                "// Using ESM avoids CJS require() shims that confuse runtime environment"
                "// detection based on the presence of `require`."
                ""
                "import './datahike.js';"
                ""
                "var _api = (typeof self !== 'undefined' ? self : globalThis)['datahike']['js']['api'];"
                ""
                "export default _api;"]
               (for [name all-exports]
                 (str "export var " name " = _api." name ";"))
               [""])]
    (str/join "\n" lines)))

(defn write-esm-wrapper!
  "Write ESM wrapper to a file."
  ([]
   (write-esm-wrapper! "npm-package/browser/index.mjs"))
  ([output-path]
   (spit output-path (generate-esm-wrapper))
   (println "ESM browser wrapper written to:" output-path)))

(def ^:private kabel-js-exports
  ["createKabelPeer" "connectKabelPeer" "maintainKabelPeer" "refreshKabelToken"
   "invokeRemote" "registerRemoteFn" "unregisterRemoteFn" "stopKabelPeer"])

(defn generate-kabel-esm-wrapper
  "Generate the ESM wrapper for the opt-in replicated browser client."
  []
  (let [base (generate-esm-wrapper)
        ;; The regular generator owns the complete core export list. Replace
        ;; only its default export, then append Kabel's deliberately small API.
        base (str/replace base "export default _api;"
                          "var _kabel = (typeof self !== 'undefined' ? self : globalThis)['datahike']['js']['kabel'];\nvar _datahikeKabel = Object.assign({}, _api, _kabel);\nexport default _datahikeKabel;")]
    (str base
         (str/join "\n" (for [name kabel-js-exports]
                          (str "export var " name " = _kabel." name ";")))
         "\n")))

(defn write-kabel-esm-wrapper! [output-path]
  (spit output-path (generate-kabel-esm-wrapper))
  (println "Kabel ESM browser wrapper written to:" output-path))

(comment
  ;; Preview generated wrapper
  (println (generate-esm-wrapper))

  ;; Write to file
  (write-esm-wrapper!))

(defn generate-remote-esm-wrapper
  "The ESM wrapper of the thin HTTP client: the remote-capable API functions
   and the value helpers, from the `datahike.js.remote` bundle."
  []
  (let [clj-exports (remote-js-exports api-specification)
        _ (assert-unique-js-names! clj-exports)
        all-exports (concat (map clj-name->js-name clj-exports) ["isPromise" "uuid" "randomUuid"])
        lines (concat
               ["// Auto-generated ESM wrapper for the thin HTTP client (datahike/remote)."
                "// DO NOT EDIT - Generated from datahike.api.specification"
                ""
                "import './datahike.js';"
                ""
                "var _api = (typeof self !== 'undefined' ? self : globalThis)['datahike']['js']['remote'];"
                ""
                "export default _api;"]
               (for [name all-exports]
                 (str "export var " name " = _api." name ";"))
               [""])]
    (str/join "\n" lines)))

(defn write-remote-esm-wrapper! [output-path]
  (spit output-path (generate-remote-esm-wrapper))
  (println "Thin-client ESM wrapper written to:" output-path))
