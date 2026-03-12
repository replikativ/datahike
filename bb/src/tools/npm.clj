(ns tools.npm
  "Build and version management for npm package."
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [cheshire.core :as json]
            [clojure.string :as str]
            [tools.version :as version]))

(defn clean-npm-package!
  "Remove compiled JS files from npm package directory (both Node and browser builds)"
  [npm-package-path]
  (println "Cleaning npm package directory...")
  (let [js-files (fs/glob npm-package-path "*.js")
        js-map-files (fs/glob npm-package-path "*.js.map")
        all-files (concat js-files js-map-files)
        files-to-keep #{"test.js" "test-final.js" "test-config-keys.js" "test-key-duplication.js"}
        files-to-delete (remove #(contains? files-to-keep (str (fs/file-name %))) all-files)]
    (doseq [file files-to-delete]
      (fs/delete file))
    (println (str "Removed " (count files-to-delete) " compiled files from " npm-package-path))
    (when (fs/exists? (str npm-package-path "/browser"))
      (fs/delete-tree (str npm-package-path "/browser"))
      (println "Removed browser build directory"))))

(defn update-package-json-version!
  "Generate npm package.json from template with version from config.edn"
  [config npm-package-path]
  (let [version-str (version/string config)
        package-json-path (str npm-package-path "/package.json")
        template-path (str npm-package-path "/package.template.json")
        template-content (slurp template-path)
        generated-content (str/replace template-content "{{VERSION}}" version-str)]
    (spit package-json-path generated-content)
    (println (str "Generated " package-json-path " from template with version: " version-str))))

(defn generate-typescript-definitions!
  "Generate TypeScript definitions for npm package"
  [output-path]
  (println "Generating TypeScript definitions...")
  (let [clj-code (str "(require '[datahike.codegen.typescript :as ts]) "
                      "(ts/write-type-definitions! \"" output-path "\")")
        result (p/shell {:out :string
                         :err :string}
                        "clojure" "-M" "-e" clj-code)]
    (when-not (zero? (:exit result))
      (println "Error generating TypeScript definitions:")
      (println (:err result))
      (throw (ex-info "TypeScript generation failed" result)))
    (println (str "TypeScript definitions written to: " output-path))))

(defn generate-esm-wrapper!
  "Generate ESM browser wrapper from api-specification via codegen.
   Modern bundlers (vite, rollup, esbuild) resolve the 'browser' exports
   condition to this file, avoiding CJS require() shims that confuse
   environ's runtime detection."
  [output-path]
  (println "Generating ESM browser wrapper...")
  (let [tmp-file (str (fs/create-temp-file {:prefix "esm-codegen-" :suffix ".clj"}))
        _ (spit tmp-file (str "(require '[datahike.codegen.esm :as esm])\n"
                              "(esm/write-esm-wrapper! \"" output-path "\")\n"))
        result (p/shell {:out :string
                         :err :string}
                        "clojure" "-M" tmp-file)]
    (fs/delete tmp-file)
    (when-not (zero? (:exit result))
      (println "Error generating ESM wrapper:")
      (println (:err result))
      (throw (ex-info "ESM wrapper generation failed" result)))
    (println (str "ESM browser wrapper written to: " output-path))))

(defn write-browser-index!
  "Write browser entry points: ESM (index.mjs) via codegen, CJS (index.js) as fallback.
   The ESM wrapper is generated from api-specification to stay in sync with
   the API automatically. The CJS wrapper is kept for backwards compatibility."
  [npm-package-path]
  (generate-esm-wrapper! (str npm-package-path "/browser/index.mjs"))
  (let [cjs-path (str npm-package-path "/browser/index.js")
        cjs-content (str "// CJS wrapper for legacy bundlers.\n"
                         "// Modern bundlers should resolve to index.mjs via the exports field.\n"
                         "require('./datahike.js');\n"
                         "module.exports = (typeof self !== 'undefined' ? self : global)"
                         "['datahike']['js']['api'];\n")]
    (spit cjs-path cjs-content)
    (println (str "Wrote " cjs-path))))

(defn build-npm-package!
  "Build npm package: clean, update version, generate types, compile ClojureScript for Node and Browser"
  [config npm-package-path]
  (println "Building npm package...")
  (println "")

  (println "Step 1/6: Cleaning old compiled files")
  (clean-npm-package! npm-package-path)
  (println "")

  (println "Step 2/6: Updating package.json version")
  (update-package-json-version! config npm-package-path)
  (println "")

  (println "Step 3/6: Generating TypeScript definitions")
  (generate-typescript-definitions! (str npm-package-path "/index.d.ts"))
  (println "")

  (println "Step 4/6: Releasing Node.js build with shadow-cljs")
  (let [result (p/shell {:out :inherit
                         :err :inherit}
                        "npx shadow-cljs release npm-release")]
    (when-not (zero? (:exit result))
      (throw (ex-info "Shadow-cljs Node.js release failed" result)))
    (println ""))

  (println "Step 5/6: Releasing Browser build with shadow-cljs")
  (let [result (p/shell {:out :inherit
                         :err :inherit}
                        "npx shadow-cljs release browser-release")]
    (when-not (zero? (:exit result))
      (throw (ex-info "Shadow-cljs browser release failed" result)))
    (write-browser-index! npm-package-path)
    (println ""))

  (println "Step 6/6: Running npm package tests")
  (let [test-result (p/shell {:dir npm-package-path
                              :out :inherit
                              :err :inherit}
                             "node test.js")]
    (when-not (zero? (:exit test-result))
      (throw (ex-info "npm package tests failed" test-result)))
    (println "")
    (println "✓ npm package build complete!")
    (println (str "  Version: " (version/string config)))
    (println (str "  Node.js:  " npm-package-path "/datahike.js.api.js  (CJS, includes file backend)"))
    (println (str "  Browser:  " npm-package-path "/browser/datahike.js  (<script> tag / CDN)"))
    (println (str "  Bundlers: " npm-package-path "/browser/index.mjs    (vite/rollup/esbuild, ESM)"))
    (println (str "           " npm-package-path "/browser/index.js     (webpack/legacy, CJS)"))
    (println "")
    (println "Next steps:")
    (println (str "  1. Verify: cd " npm-package-path " && npm pack --dry-run"))
    (println "  2. Publish: npm publish")))
