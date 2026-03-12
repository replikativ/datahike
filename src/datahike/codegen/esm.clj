(ns datahike.codegen.esm
  "Generate ESM browser wrapper from api.specification.

  The browser build of Datahike is an IIFE that attaches the API to
  globalThis['datahike']['js']['api']. This codegen produces an ES module
  wrapper (index.mjs) that re-exports every API function as a named export,
  plus a default export of the full API object.

  Why ESM matters: the previous CJS wrapper (require/module.exports) causes
  bundlers like Vite to inject a `require` shim, which tricks environ's
  runtime detection into believing it's running in Node.js, leading to
  fileExistsSync errors in the browser. ESM avoids this entirely."
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.codegen.naming :refer [js-skip-list clj-name->js-name]]
            [clojure.string :as str]))

;; Additional exports defined in datahike.js.api that are not in the
;; api-specification (manually exported with ^:export metadata).
(def ^:private extra-js-exports
  ["isPromise" "uuid" "randomUuid"])

(defn generate-esm-wrapper
  "Generate ESM wrapper source that re-exports the IIFE bundle's API.
   Returns the file content as a string."
  []
  (let [spec-exports (for [[fn-name _] (sort-by first api-specification)
                           :when (not (contains? js-skip-list fn-name))]
                       (clj-name->js-name fn-name))
        all-exports (concat spec-exports extra-js-exports)
        lines (concat
               ["// Auto-generated ESM wrapper for browser bundlers (vite, rollup, esbuild)."
                "// DO NOT EDIT - Generated from datahike.api.specification"
                "//"
                "// Using ESM avoids CJS require() shims that confuse runtime environment"
                "// detection (e.g. environ checking for `require` to detect Node.js)."
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

(comment
  ;; Preview generated wrapper
  (println (generate-esm-wrapper))

  ;; Write to file
  (write-esm-wrapper!))
