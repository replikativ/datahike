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
        files-to-keep #{"test.js" "test-log-level-env.js" "test-final.js"
                        "test-config-keys.js" "test-key-duplication.js"}
        files-to-delete (remove #(contains? files-to-keep (str (fs/file-name %))) all-files)]
    (doseq [file files-to-delete]
      (fs/delete file))
    (println (str "Removed " (count files-to-delete) " compiled files from " npm-package-path))
    (when (fs/exists? (str npm-package-path "/browser"))
      (fs/delete-tree (str npm-package-path "/browser"))
      (println "Removed browser build directory"))
    (when (fs/exists? (str npm-package-path "/s3"))
      (fs/delete-tree (str npm-package-path "/s3"))
      (println "Removed S3 browser build directory"))
    (when (fs/exists? (str npm-package-path "/kabel"))
      (fs/delete-tree (str npm-package-path "/kabel"))
      (println "Removed Kabel browser build directory"))))

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

(defn generate-kabel-typescript-definitions! [output-path]
  (let [clj-code (str "(require '[datahike.codegen.typescript :as ts]) "
                      "(ts/write-kabel-type-definitions! \"" output-path "\")")
        result (p/shell {:out :string :err :string}
                        "clojure" "-M" "-e" clj-code)]
    (when-not (zero? (:exit result))
      (throw (ex-info "Kabel TypeScript generation failed" result)))
    (println (str "Kabel TypeScript definitions written to: " output-path))))

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
  [browser-path]
  (generate-esm-wrapper! (str browser-path "/index.mjs"))
  (let [cjs-path (str browser-path "/index.js")
        cjs-content (str "// CJS wrapper for legacy bundlers.\n"
                         "// Modern bundlers should resolve to index.mjs via the exports field.\n"
                         "require('./datahike.js');\n"
                         "module.exports = (typeof self !== 'undefined' ? self : global)"
                         "['datahike']['js']['api'];\n")]
    (spit cjs-path cjs-content)
    (println (str "Wrote " cjs-path))))

(defn write-kabel-index! [browser-path]
  (let [esm-path (str browser-path "/index.mjs")
        tmp-file (str (fs/create-temp-file {:prefix "kabel-esm-codegen-" :suffix ".clj"}))
        _ (spit tmp-file (str "(require '[datahike.codegen.esm :as esm])\n"
                              "(esm/write-kabel-esm-wrapper! \"" esm-path "\")\n"))
        result (p/shell {:out :string :err :string}
                        "clojure" "-M" tmp-file)]
    (fs/delete tmp-file)
    (when-not (zero? (:exit result))
      (throw (ex-info "Kabel ESM wrapper generation failed" result)))
    (spit (str browser-path "/index.js")
          (str "require('./datahike.js');\n"
               "var root = (typeof self !== 'undefined' ? self : global);\n"
               "module.exports = Object.assign({}, root['datahike']['js']['api'], root['datahike']['js']['kabel']);\n"))))

(defn- run-package-command!
  [npm-package-path description & command]
  (let [result (apply p/shell {:dir npm-package-path
                               :out :inherit
                               :err :inherit}
                      command)]
    (when-not (zero? (:exit result))
      (throw (ex-info (str description " failed") result)))))

(defn- validate-package-manifest!
  [npm-package-path]
  (let [result (p/shell {:dir npm-package-path :out :string :err :string}
                        "npm" "pack" "--dry-run" "--json"
                        "--cache" "../target/npm-cache")]
    (when-not (zero? (:exit result))
      (throw (ex-info "npm pack dry-run failed" result)))
    (let [parsed (json/parse-string (:out result) true)
          ;; npm 11 returned a vector; npm 12 returns an object keyed by the
          ;; package name. Accept both so the release gate checks the tarball
          ;; rather than silently treating a new CLI shape as an empty report.
          _ (when (and (map? parsed) (:error parsed))
              (throw (ex-info "npm pack dry-run reported an error"
                              {:error (:error parsed) :stderr (:err result)})))
          report (cond
                   (vector? parsed) (first parsed)
                   (:files parsed) parsed
                   (map? parsed) (let [v (first (vals parsed))]
                                   (if (vector? v) (first v) v)))
          files (into #{} (map :path) (:files report))
          required #{"LICENSE" "THIRD_PARTY_LICENSES.md"
                     "README.md" "package.json" "index.d.ts"
                     "datahike.js.api.js" "browser/datahike.js"
                     "browser/index.js" "browser/index.mjs"
                     "s3/datahike.js" "s3/index.js" "s3/index.mjs"
                     "kabel/datahike.js" "kabel/index.js" "kabel/index.mjs"
                     "kabel.d.ts"}
          missing (remove files required)
          forbidden (filter #(or (and (str/ends-with? % ".ts")
                                      (not (str/ends-with? % ".d.ts")))
                                 (re-matches #"test.*\\.js" %)
                                 (= % "PUBLISHING.md")
                                 (= % "package.template.json"))
                            files)]
      (when (seq missing)
        (throw (ex-info "npm package is missing required files"
                        {:missing (vec missing)})))
      (when (seq forbidden)
        (throw (ex-info "npm package contains development-only files"
                        {:forbidden (vec forbidden)})))
      (println (format "Validated npm tarball: %d files, %.1f KiB packed"
                       (count files) (/ (:size report) 1024.0))))))

(defn verify-npm-package!
  "Exercise both public entry points, compile the declaration contract, and
  audit the exact tarball before it can be published."
  [npm-package-path]
  (run-package-command! npm-package-path "Default npm logging test"
                        "node" "test-log-level-env.js" "default")
  (run-package-command! npm-package-path "Environment npm logging test"
                        "node" "test-log-level-env.js" "trace")
  (run-package-command! npm-package-path "CommonJS API test" "node" "test.js")
  (run-package-command! npm-package-path "ESM wrapper syntax check"
                        "node" "--check" "browser/index.mjs")
  (run-package-command! npm-package-path "TypeScript declaration test"
                        "npx" "tsc" "--noEmit" "--project" "tsconfig.json")
  (validate-package-manifest! npm-package-path))

(defn build-npm-package!
  "Build and fully verify the npm package."
  [config npm-package-path]
  (println "Building npm package...")
  (println "")

  (println "Step 1/9: Cleaning old compiled files")
  (clean-npm-package! npm-package-path)
  (fs/copy "LICENSE" (str npm-package-path "/LICENSE") {:replace-existing true})
  (println "")

  (println "Step 2/9: Updating package.json version")
  (update-package-json-version! config npm-package-path)
  (println "")

  (println "Step 3/9: Generating TypeScript definitions")
  (generate-typescript-definitions! (str npm-package-path "/index.d.ts"))
  (generate-kabel-typescript-definitions! (str npm-package-path "/kabel.d.ts"))
  (println "")

  (println "Step 4/9: Releasing Node.js build with shadow-cljs")
  (let [result (p/shell {:out :inherit
                         :err :inherit}
                        "npx shadow-cljs release npm-release")]
    (when-not (zero? (:exit result))
      (throw (ex-info "Shadow-cljs Node.js release failed" result)))
    (println ""))

  (println "Step 5/9: Releasing Browser build with shadow-cljs")
  (let [result (p/shell {:out :inherit
                         :err :inherit}
                        "npx shadow-cljs release browser-release")]
    (when-not (zero? (:exit result))
      (throw (ex-info "Shadow-cljs browser release failed" result)))
    (write-browser-index! (str npm-package-path "/browser"))
    (println ""))

  (println "Step 6/9: Releasing optional S3 browser build")
  (let [result (p/shell {:out :inherit
                         :err :inherit}
                        "npx shadow-cljs release browser-s3-release")]
    (when-not (zero? (:exit result))
      (throw (ex-info "Shadow-cljs S3 browser release failed" result)))
    (write-browser-index! (str npm-package-path "/s3"))
    (println ""))

  (println "Step 7/9: Releasing optional Kabel browser build")
  (let [result (p/shell {:out :inherit
                         :err :inherit}
                        "npx shadow-cljs release browser-kabel-release")]
    (when-not (zero? (:exit result))
      (throw (ex-info "Shadow-cljs Kabel browser release failed" result)))
    (write-kabel-index! (str npm-package-path "/kabel"))
    (println ""))

  (println "Step 8/9: Verifying runtime, types, and tarball")
  (verify-npm-package! npm-package-path)

  (println "Step 9/9: Build summary")
  (println "")
  (println "✓ npm package build complete!")
  (println (str "  Version: " (version/string config)))
  (println (str "  Node.js:  " npm-package-path "/datahike.js.api.js  (CJS, includes file backend)"))
  (println (str "  Browser:  " npm-package-path "/browser/datahike.js  (<script> tag / CDN)"))
  (println (str "  Bundlers: " npm-package-path "/browser/index.mjs    (vite/rollup/esbuild, ESM)"))
  (println (str "           " npm-package-path "/browser/index.js     (webpack/legacy, CJS)"))
  (println (str "  S3:      " npm-package-path "/s3/index.mjs        (optional browser build)"))
  (println (str "  Kabel:   " npm-package-path "/kabel/index.mjs     (IndexedDB + replicated writer)"))
  (println "")
  (println "The main-branch release workflow publishes this verified artifact."))
