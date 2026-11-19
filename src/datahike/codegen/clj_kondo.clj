(ns datahike.codegen.clj-kondo
  "Generate clj-kondo export config from api.specification.

  Writes a :macroexpand hook for datahike.api/emit-api that defines each API function with its docstring and arglists.
  Writes :type-mismatch types for each API function."
  (:require [datahike.api.specification :refer [api-specification malli-schema->argslist]]
            [clojure.java.io :as io]
            [clojure.string :as str]))

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

(defn- plain-param
  [i param]
  (cond
    (symbol? param) param
    (and (map? param) (symbol? (:as param))) (:as param)
    :else (symbol (str "arg" i))))

(defn- plain-arglist
  [arglist]
  (vec (map-indexed plain-param arglist)))

(defn- impl-arglists
  [impl]
  (require (symbol (namespace impl)))
  (:arglists (meta (resolve impl))))

(defn- hook-arity
  [impl arglist]
  (let [[fixed [_ rest-param]] (split-with #(not= '& %) arglist)]
    (list arglist
          (if rest-param
            `(~'apply ~impl ~@fixed ~rest-param)
            `(~impl ~@fixed)))))

(defn- hook-defn
  [fn-name {:keys [args doc impl]}]
  (let [arglists (or (impl-arglists impl)
                     [(-> (apply min-key count (malli-schema->argslist args))
                          (conj '& 'args))])]
    `(~'defn ~fn-name ~doc
      ~@(map #(hook-arity impl %) (distinct (map plain-arglist arglists))))))

(defn generate-hook-source
  "Returns the source of a clj-kondo macroexpand hook for datahike.api/emit-api.
  The hook expands to one defn per API operation, with the arglists of its implementation."
  []
  (let [defns (map (fn [[fn-name spec]] (hook-defn fn-name spec))
                   (into (sorted-map) api-specification))]
    (with-out-str
      (println ";; AUTO-GENERATED from api-specification by bb codegen-clj-kondo")
      (println "(ns datahike.api)")
      (println)
      (println "(defmacro emit-api []")
      (println (str "  '(do\n"
                    (str/join "\n" (map #(str "     " (pr-str %)) defns))
                    "))")))))

(defn generate-config
  "Returns the clj-kondo export configuration.
  It registers the emit-api hook and the :type-mismatch types of datahike.api."
  []
  (let [type-defs (into {}
                        (keep (fn [[fn-name spec]]
                                (when-let [type-def (generate-type-definition fn-name spec)]
                                  [fn-name type-def]))
                              api-specification))]
    {:hooks
     {:macroexpand {'datahike.api/emit-api 'datahike.api/emit-api}}

     :linters
     {:type-mismatch
      {:namespaces
       {'datahike.api type-defs}}}}))

(defn- hook-path
  [config-path]
  (io/file (.getParentFile (io/file config-path)) "datahike" "api.clj"))

(defn write-config!
  "Writes the clj-kondo export configuration to output-path and the emit-api hook next to it.
  output-path is usually resources/clj-kondo.exports/io.replikativ/datahike/config.edn."
  [output-path]
  (let [config (generate-config)
        output-file (io/file output-path)
        hook-file (hook-path output-path)]
    (io/make-parents output-file)
    (io/make-parents hook-file)
    (with-open [writer (io/writer output-file)]
      (binding [*out* writer]
        (println ";; clj-kondo export configuration for Datahike")
        (println ";; AUTO-GENERATED from api-specification")
        (println ";; To regenerate: bb codegen-clj-kondo")
        (println)
        (prn config)))
    (spit hook-file (generate-hook-source))
    (let [type-count (count (get-in config [:linters :type-mismatch :namespaces 'datahike.api]))]
      (println (str "Generated clj-kondo export config: " output-path))
      (println (str "  Generated emit-api hook for " (count api-specification) " API functions: " hook-file))
      (println (str "  Generated " type-count " type definitions for type checking"))
      output-path)))

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
  (write-config! "resources/clj-kondo.exports/io.replikativ/datahike/config.edn")

  ;; Verify config is up-to-date
  (verify-config "resources/clj-kondo.exports/io.replikativ/datahike/config.edn")

  ;; Preview what would be generated
  (generate-config))
