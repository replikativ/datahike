(ns datahike.codegen.clj-kondo
  "Generate clj-kondo export config from api.specification.

  This namespace provides tooling to generate clj-kondo configuration that:
  - Exports all dynamically generated API functions
  - Provides proper arglists for each function
  - Includes documentation strings
  - Provides type information for :type-mismatch linter
  - Enables IDE autocomplete, signature hints, and type checking

  The generated config allows clj-kondo to understand the dynamically
  expanded API functions from the emit-api macro, eliminating yellow
  squiggles and providing full IDE support."
  (:require [datahike.api.specification :refer [api-specification malli-schema->argslist]]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]))

;; =============================================================================
;; Malli Schema to clj-kondo Type Conversion
;; =============================================================================

(defn- malli->clj-kondo-type
  "Convert a Malli type to a clj-kondo type keyword.

  Examples:
    :int -> :int
    :string -> :string
    :boolean -> :boolean
    :datahike/SDB -> :any (custom types become :any)
    [:vector :int] -> :vector
    [:map ...] -> :map"
  [schema]
  (cond
    ;; Primitive types that clj-kondo understands natively
    (#{:int :integer :double :string :boolean :keyword :symbol :nil :any} schema)
    schema

    ;; Collection types
    (and (vector? schema) (= :vector (first schema)))
    :vector

    (and (vector? schema) (= :sequential (first schema)))
    :seqable

    (and (vector? schema) (= :map (first schema)))
    :map

    (and (vector? schema) (= :set (first schema)))
    :set

    ;; Function schemas - treat as :ifn
    (and (vector? schema) (#{:=> :function} (first schema)))
    :ifn

    ;; Or/alt schemas - use :any for now (could be smarter)
    (and (vector? schema) (#{:or :alt} (first schema)))
    :any

    ;; Maybe/optional - extract inner type
    (and (vector? schema) (= :maybe (first schema)))
    (if-let [inner (second schema)]
      (malli->clj-kondo-type inner)
      :any)

    ;; Custom types (namespaced keywords like :datahike/SDB) -> :any
    (keyword? schema)
    :any

    ;; Symbols (type references) -> :any
    (symbol? schema)
    :any

    ;; Fn predicates [:fn pred] -> :any
    (and (vector? schema) (= :fn (first schema)))
    :any

    ;; Default
    :else
    :any))

(defn- extract-arity-info
  "Extract arity information from a Malli function schema.

  Input: [:=> [:cat Type1 Type2] RetType]
  Output: {:args [:type1 :type2] :ret :rettype}

  Input: [:function [:=> [:cat Type1] RetType] [:=> [:cat Type1 Type2] RetType]]
  Output: [{:args [:type1] :ret :rettype}
           {:args [:type1 :type2] :ret :rettype}]"
  [schema]
  (cond
    ;; Single arity: [:=> [:cat Type1 Type2] RetType]
    (and (vector? schema) (= :=> (first schema)))
    (let [[_ input-schema ret-schema] schema]
      (if (and (vector? input-schema) (= :cat (first input-schema)))
        [{:args (mapv malli->clj-kondo-type (rest input-schema))
          :ret (malli->clj-kondo-type ret-schema)}]
        ;; No :cat - assume no args
        [{:args []
          :ret (malli->clj-kondo-type ret-schema)}]))

    ;; Multi-arity: [:function [:=> ...] [:=> ...]]
    (and (vector? schema) (= :function (first schema)))
    (vec (for [arity-schema (rest schema)]
           (when (and (vector? arity-schema) (= :=> (first arity-schema)))
             (let [[_ input-schema ret-schema] arity-schema]
               (if (and (vector? input-schema) (= :cat (first input-schema)))
                 {:args (mapv malli->clj-kondo-type (rest input-schema))
                  :ret (malli->clj-kondo-type ret-schema)}
                 ;; No :cat - assume no args
                 {:args []
                  :ret (malli->clj-kondo-type ret-schema)})))))

    ;; Fallback
    :else
    nil))

(defn- generate-type-definition
  "Generate clj-kondo type definition for a single API function.

  Returns a map with :arities for :type-mismatch linter."
  [fn-name {:keys [args]}]
  (when-let [arities (extract-arity-info args)]
    (let [;; Convert arities to clj-kondo format: {arity {:args [...] :ret ...}}
          arities-map (into {}
                            (map-indexed
                              (fn [idx {:keys [args ret]}]
                                [(count args) {:args args :ret ret}])
                              arities))]
      {:arities arities-map})))

;; =============================================================================
;; Config Generation
;; =============================================================================

(defn- generate-var-definition
  "Generate clj-kondo var definition for a single API function.

  Returns a map with :arglists and :doc for the function."
  [fn-name {:keys [args doc]}]
  (let [arglists (malli-schema->argslist args)
        ;; Ensure arglists is a proper list (not lazy seq or vector)
        arglists-list (if (list? arglists)
                        arglists
                        (apply list arglists))]
    ;; Use a quoted form (via syntax-quote) so it prints as '(...) not (quote (...))
    {:arglists `'~arglists-list
     :doc doc}))

(defn generate-config
  "Generate complete clj-kondo export configuration.

  Returns an EDN map with:
  - :var-definitions - Basic function signatures for IDE support
  - :linters :type-mismatch - Type information for type checking"
  []
  (let [var-defs (into {}
                       (map (fn [[fn-name spec]]
                              [fn-name (generate-var-definition fn-name spec)])
                            api-specification))
        type-defs (into {}
                        (keep (fn [[fn-name spec]]
                                (when-let [type-def (generate-type-definition fn-name spec)]
                                  [fn-name type-def]))
                              api-specification))]
    {:var-definitions
     {'datahike.api var-defs}

     :linters
     {:type-mismatch
      {:namespaces
       {'datahike.api type-defs}}}}))

(defn write-config!
  "Write clj-kondo export configuration to file.

  This should be the resources/ location so it gets packaged in the jar
  and auto-imported by consuming projects.

  Args:
    output-path - Path to write config.edn (usually resources/clj-kondo.exports/io.replikativ/datahike/config.edn)"
  [output-path]
  (let [config (generate-config)
        output-file (io/file output-path)]

    ;; Ensure parent directories exist
    (.mkdirs (.getParentFile output-file))

    ;; Write export config (using prn for now to avoid pprint issues with quoted forms)
    (with-open [writer (io/writer output-file)]
      (binding [*out* writer]
        (println ";; clj-kondo export configuration for Datahike")
        (println ";; AUTO-GENERATED from api-specification")
        (println ";; To regenerate: bb codegen-clj-kondo")
        (println)
        (prn config)))

    (let [var-count (count (get-in config [:var-definitions 'datahike.api]))
          type-count (count (get-in config [:linters :type-mismatch :namespaces 'datahike.api]))]
      (println (str "Generated clj-kondo export config: " output-path))
      (println (str "  Exported " var-count " API functions with signatures"))
      (println (str "  Generated " type-count " type definitions for type checking"))
      output-path)))

(defn copy-export-to-local!
  "Copy export config to .clj-kondo directory for local development.

  The export config in resources/ gets packaged in the jar. For local
  development, we also need it in .clj-kondo/datahike/datahike/ which
  is referenced via :config-paths.

  Args:
    export-path - Source path (resources/clj-kondo.exports/io.replikativ/datahike/config.edn)
    local-path - Destination path (.clj-kondo/datahike/datahike/config.edn)"
  [export-path local-path]
  (let [export-file (io/file export-path)
        local-file (io/file local-path)]

    ;; Ensure parent directories exist
    (.mkdirs (.getParentFile local-file))

    ;; Copy file
    (io/copy export-file local-file)

    (println (str "Copied to local config: " local-path))
    local-path))

(defn update-main-config!
  "Update main .clj-kondo/config.edn with type-mismatch linter.

  Reads existing config, merges in type definitions, and writes back.
  This enables type checking for Datahike's own project (export config
  is for consuming libraries).

  Args:
    main-config-path - Path to main config (usually .clj-kondo/config.edn)"
  [main-config-path]
  (let [config (generate-config)
        type-defs (get-in config [:linters :type-mismatch :namespaces 'datahike.api])
        main-config-file (io/file main-config-path)
        existing-config (when (.exists main-config-file)
                          (read-string (slurp main-config-file)))

        ;; Merge type-mismatch into existing linters
        updated-config (update-in existing-config
                                  [:linters :type-mismatch :namespaces]
                                  (fn [existing]
                                    (merge existing {'datahike.api type-defs})))]

    ;; Write updated config with pretty printing
    (with-open [writer (io/writer main-config-file)]
      (binding [*out* writer]
        (println ";; clj-kondo configuration for Datahike")
        (println ";; Type definitions for datahike.api are AUTO-GENERATED")
        (println ";; To regenerate: bb codegen-clj-kondo")
        (println)
        (pprint/pprint updated-config)))

    (println (str "Updated main config: " main-config-path))
    (println (str "  Added type checking for " (count type-defs) " API functions"))
    main-config-path))

;; =============================================================================
;; Verification
;; =============================================================================

(defn verify-config
  "Verify that generated config is up-to-date.

  Reads existing config and compares to what would be generated.
  Returns true if up-to-date, false otherwise.

  Useful for CI checks to ensure codegen was run."
  [config-path]
  (try
    (let [expected (generate-config)
          actual (read-string (slurp config-path))]
      (= expected actual))
    (catch Exception e
      (println (str "Error verifying config: " (.getMessage e)))
      false)))

(comment
  ;; Generate config
  (write-config! ".clj-kondo/datahike/datahike/config.edn")

  ;; Verify config is up-to-date
  (verify-config ".clj-kondo/datahike/datahike/config.edn")

  ;; Preview what would be generated
  (clojure.pprint/pprint (generate-config)))
