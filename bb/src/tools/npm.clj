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

(defn write-browser-index!
  "Write browser/index.js — a thin CJS wrapper around the IIFE bundle.
   Bundlers (webpack, vite, rollup) resolve the 'browser' exports condition
   to this file and get proper require()/module.exports semantics.
   The IIFE attaches the API to globalThis; we re-export it as a CJS module.
   The dynamic require of konserve.node_filestore in api.cljs is DCE'd by
   the :target :browser advanced build, so no Node built-ins leak in."
  [npm-package-path]
  (let [path (str npm-package-path "/browser/index.js")
        content (str "// CJS wrapper for browser bundlers (webpack, vite, rollup).\n"
                     "// Loads the self-contained IIFE bundle then re-exports the API.\n"
                     "require('./datahike.js');\n"
                     "module.exports = (typeof self !== 'undefined' ? self : global)"
                     "['datahike']['js']['api'];\n")]
    (spit path content)
    (println (str "Wrote " path))))

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
    (println (str "  Bundlers: " npm-package-path "/browser/index.js     (webpack/vite/rollup)"))
    (println "")
    (println "Next steps:")
    (println (str "  1. Verify: cd " npm-package-path " && npm pack --dry-run"))
    (println "  2. Publish: npm publish")))
