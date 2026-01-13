(ns tools.npm
  "Build and version management for npm package."
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [cheshire.core :as json]
            [clojure.string :as str]
            [tools.version :as version]))

(defn clean-npm-package!
  "Remove compiled JS files from npm package directory"
  [npm-package-path]
  (println "Cleaning npm package directory...")
  (let [js-files (fs/glob npm-package-path "*.js")
        js-map-files (fs/glob npm-package-path "*.js.map")
        all-files (concat js-files js-map-files)
        ;; Keep test files and package.json
        files-to-keep #{"test.js" "test-final.js" "test-config-keys.js" "test-key-duplication.js"}
        files-to-delete (remove #(contains? files-to-keep (str (fs/file-name %))) all-files)]
    (doseq [file files-to-delete]
      (fs/delete file))
    (println (str "Removed " (count files-to-delete) " compiled files from " npm-package-path))))

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
  (let [clj-code (str "(require '[datahike.js.typescript :as ts]) "
                      "(ts/write-type-definitions! \"" output-path "\")")
        result (p/shell {:out :string
                         :err :string}
                        "clojure" "-M" "-e" clj-code)]
    (when-not (zero? (:exit result))
      (println "Error generating TypeScript definitions:")
      (println (:err result))
      (throw (ex-info "TypeScript generation failed" result)))
    (println (str "TypeScript definitions written to: " output-path))))

(defn build-npm-package!
  "Build npm package: clean, update version, generate types, compile ClojureScript"
  [config npm-package-path]
  (println "Building npm package...")
  (println "")

  ;; Step 1: Clean old compiled files
  (println "Step 1/5: Cleaning old compiled files")
  (clean-npm-package! npm-package-path)
  (println "")

  ;; Step 2: Update package.json version
  (println "Step 2/5: Updating package.json version")
  (update-package-json-version! config npm-package-path)
  (println "")

  ;; Step 3: Generate TypeScript definitions
  (println "Step 3/5: Generating TypeScript definitions")
  (generate-typescript-definitions! (str npm-package-path "/index.d.ts"))
  (println "")

  ;; Step 4: Compile ClojureScript
  (println "Step 4/5: Compiling ClojureScript with shadow-cljs")
  (let [result (p/shell {:out :inherit
                         :err :inherit}
                        "npx shadow-cljs compile npm-release")]
    (when-not (zero? (:exit result))
      (throw (ex-info "Shadow-cljs compilation failed" result)))
    (println "")

    ;; Step 5: Run tests
    (println "Step 5/5: Running npm package tests")
    (let [test-result (p/shell {:dir npm-package-path
                                :out :inherit
                                :err :inherit}
                               "node test.js")]
      (when-not (zero? (:exit test-result))
        (throw (ex-info "npm package tests failed" test-result)))
      (println "")
      (println "✓ npm package build complete!")
      (println (str "  Version: " (version/string config)))
      (println (str "  Output: " npm-package-path))
      (println "")
      (println "Next steps:")
      (println (str "  1. Verify: cd " npm-package-path " && npm pack --dry-run"))
      (println "  2. Publish: npm publish"))))
