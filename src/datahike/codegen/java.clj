(ns datahike.codegen.java
  "Generate Java source code from API specification.

   This namespace generates DatahikeGenerated.java containing all
   static method bindings from the universal API specification."
  (:require [datahike.api.specification :refer [api-specification]]
            [clojure.string :as str]
            [clojure.java.io :as io]))

;; =============================================================================
;; Type Mapping: Malli Schema -> Java
;; =============================================================================

(defn malli->java-type
  "Map malli schema to Java type string.
   Uses strongest Java collection interface we can safely cast to."
  [schema]
  (cond
    ;; Keyword schemas (primitives and basic types)
    (keyword? schema)
    (case schema
      :any "Object"
      :nil "void"
      :boolean "boolean"
      :int "int"
      :long "long"
      :double "double"
      :string "String"
      :keyword "Object"  ; clojure.lang.Keyword
      :symbol "Object"
      :map "Map<?,?>"
      :vector "List<?>"
      :sequential "Iterable<?>"
      :set "Set<?>"
      "Object")

    ;; Symbol schemas (type references)
    (symbol? schema)
    (let [schema-name (name schema)]
      (case schema-name
        ;; Semantic Datahike types
        "SConnection" "Object"
        "SDB" "Object"
        "SEntity" "Object"
        "STransactionReport" "Map<String,Object>"
        "SSchema" "Map<Object,Object>"
        "SMetrics" "Map<String,Object>"
        "SDatoms" "Iterable<?>"
        "SEId" "Object"
        "SPullPattern" "String"
        "SConfig" "Map<String,Object>"
        "STransactions" "List"
        "SQueryArgs" "Object"
        ;; Default
        "Object"))

    ;; Vector schemas (compound types)
    (vector? schema)
    (let [[op & args] schema]
      (case op
        ;; [:or Type1 Type2] -> Object (can't determine at compile time)
        :or "Object"

        ;; [:maybe Type] -> Object (nullable)
        :maybe (let [inner (malli->java-type (first args))]
                 ;; Box primitives for nullable
                 (case inner
                   "boolean" "Boolean"
                   "int" "Integer"
                   "long" "Long"
                   "double" "Double"
                   inner))

        ;; [:sequential Type] -> Iterable<?>
        :sequential "Iterable<?>"

        ;; [:vector Type] -> List<Type>
        :vector
        (str "List<?>")

        ;; [:set Type] -> Set<Type>
        :set
        (str "Set<?>")

        ;; [:map ...] -> Map
        :map "Map<?,?>"

        ;; [:function ...] or [:=> ...] - extract return type
        (:function :=>)
        (if (= op :=>)
          (malli->java-type (nth schema 2))  ; [:=> input output]
          (malli->java-type (second schema)))  ; [:function [:=> ...]]

        ;; [:cat ...] - args tuple, not a return type
        :cat "Object"

        ;; Default
        "Object"))

    ;; inst? predicate -> Date
    (= schema 'inst?) "Date"

    ;; Default
    :else "Object"))

(defn param-type->java
  "Map parameter type to Java type string."
  [schema]
  (cond
    ;; Keyword type references with datahike/ prefix
    (and (keyword? schema)
         (= (namespace schema) "datahike"))
    (let [type-name (name schema)]
      (case type-name
        "SConfig" "Map<String,Object>"
        "SConnection" "Object"
        "SDB" "Object"
        "STransactions" "List"
        "SPullPattern" "String"
        "SEId" "Object"
        "Object"))

    ;; inst? predicate
    (= schema 'inst?) "Date"

    ;; Default to general type mapping
    :else (malli->java-type schema)))

;; =============================================================================
;; Name Conversion
;; =============================================================================

(defn clj-name->java-method
  "Convert Clojure operation name to Java method name.
   Examples: database-exists? -> databaseExists
             gc-storage -> gcStorage
             transact! -> transactAsync"
  [op-name]
  (let [s (name op-name)
        ;; Handle ! suffix -> Async
        has-bang? (str/ends-with? s "!")
        ;; Remove trailing ! and ?
        clean (str/replace s #"[!?]$" "")
        ;; Split on hyphens
        parts (str/split clean #"-")
        ;; camelCase
        base (apply str (first parts)
                    (map str/capitalize (rest parts)))]
    ;; Add Async suffix if had !
    (if has-bang?
      (str base "Async")
      base)))

;; =============================================================================
;; Parameter Extraction
;; =============================================================================

(defn extract-params-from-schema
  "Extract parameter list from malli function schema.
   Returns vector of {:name :type} maps."
  [args-schema]
  (cond
    ;; [:=> [:cat Type1 Type2] Return]
    (and (vector? args-schema)
         (= :=> (first args-schema)))
    (let [[_ input-schema _] args-schema]
      (when (and (vector? input-schema)
                 (= :cat (first input-schema)))
        (vec
         (map-indexed
          (fn [idx param-schema]
            {:name (str "arg" idx)
             :type (param-type->java param-schema)})
          (rest input-schema)))))

    ;; [:function [:=> ...] [:=> ...]] - multi-arity, return all arities
    (and (vector? args-schema)
         (= :function (first args-schema)))
    :multi-arity  ; Signal to caller

    ;; Default
    :else []))

(defn extract-multi-arity-params
  "Extract all arities from multi-arity function schema.
   Returns vector of param-lists, each being a vector of {:name :type} maps."
  [args-schema]
  (when (and (vector? args-schema)
             (= :function (first args-schema)))
    (vec
     (map extract-params-from-schema (rest args-schema)))))

;; =============================================================================
;; IFn Declaration Generation
;; =============================================================================

(defn generate-ifn-declarations
  "Generate static IFn field declarations for all operations."
  []
  (str/join "\n"
            (for [[op-name _] (sort-by first api-specification)]
              (let [fn-var-name (str (clj-name->java-method op-name) "Fn")
                    clj-op-name (name op-name)]
                (str "    protected static final IFn " fn-var-name
                     " = Clojure.var(\"datahike.api\", \"" clj-op-name "\");")))))

;; =============================================================================
;; Javadoc Generation
;; =============================================================================

(defn escape-javadoc
  "Escape special characters for javadoc."
  [s]
  (-> s
      (str/replace #"<" "&lt;")
      (str/replace #">" "&gt;")
      (str/replace #"&" "&amp;")
      (str/replace #"@" "{@literal @}")))

(defn format-javadoc
  "Format a docstring and examples as javadoc comment."
  [doc examples]
  (when doc
    (let [lines (str/split-lines doc)
          doc-lines (map #(str "     * " (escape-javadoc %)) lines)
          ;; Add examples if available
          example-lines (when (seq examples)
                          (concat
                           ["     * "
                            "     * <h3>Examples:</h3>"
                            "     * <pre>{@code"]
                           (mapcat
                            (fn [{:keys [desc code]}]
                              (concat
                               [(str "     * // " (escape-javadoc desc))]
                               (map #(str "     * " (escape-javadoc %))
                                    (str/split-lines code))))
                            (take 2 examples))
                           ["     * }</pre>"]))]
      (str "    /**\n"
           (str/join "\n" (concat doc-lines example-lines))
           "\n     */"))))

;; =============================================================================
;; Method Body Generation
;; =============================================================================

(defn generate-method-body
  "Generate method body that invokes Clojure function."
  [op-name params return-type]
  (let [fn-var-name (str (clj-name->java-method op-name) "Fn")
        ;; Determine if we need varargs handling
        has-varargs? (some #(str/includes? (:name %) "...") params)
        ;; Build argument list
        arg-names (map :name params)]
    (cond
      ;; Void return
      (= return-type "void")
      (str "        " fn-var-name ".invoke("
           (str/join ", " arg-names) ");\n")

      ;; Config parameter needs conversion
      (and (seq params)
           (= (:type (first params)) "Map<String,Object>"))
      (let [converted-first "Util.mapToPersistentMap(arg0)"
            rest-args (rest arg-names)
            all-args (cons converted-first rest-args)]
        (str "        return (" return-type ") " fn-var-name ".invoke("
             (str/join ", " all-args) ");\n"))

      ;; Transaction report needs conversion
      (= return-type "Map<String,Object>")
      (str "        APersistentMap result = (APersistentMap) " fn-var-name ".invoke("
           (str/join ", " arg-names) ");\n"
           "        return Util.clojureMapToJavaMap(result);\n")

      ;; Varargs handling (like q, datoms)
      has-varargs?
      (str "        List<Object> args = new ArrayList<>();\n"
           (str/join "\n" (map #(str "        args.add(" % ");") arg-names))
           "\n"
           "        return (" return-type ") " fn-var-name ".applyTo(RT.seq(args));\n")

      ;; Simple return with cast
      :else
      (str "        return (" return-type ") " fn-var-name ".invoke("
           (str/join ", " arg-names) ");\n"))))

;; =============================================================================
;; Method Generation
;; =============================================================================

(defn generate-method
  "Generate a single static method from spec entry."
  [[op-name {:keys [args ret doc examples]}]]
  (let [method-name (clj-name->java-method op-name)
        return-type (malli->java-type ret)
        javadoc (format-javadoc doc examples)]

    ;; Check for multi-arity
    (if (= :multi-arity (extract-params-from-schema args))
      ;; Generate multiple overloads
      (let [arities (extract-multi-arity-params args)]
        (str/join "\n\n"
                  (for [params arities]
                    (let [param-str (str/join ", "
                                              (map-indexed
                                               (fn [idx {:keys [type name]}]
                                                 (str type " " name))
                                               params))]
                      (str (when javadoc (str javadoc "\n"))
                           "    public static " return-type " " method-name
                           "(" param-str ") {\n"
                           (generate-method-body op-name params return-type)
                           "    }")))))

      ;; Single arity
      (let [params (extract-params-from-schema args)
            param-str (str/join ", "
                                (map-indexed
                                 (fn [idx {:keys [type name]}]
                                   (str type " " name))
                                 params))]
        (str (when javadoc (str javadoc "\n"))
             "    public static " return-type " " method-name
             "(" param-str ") {\n"
             (generate-method-body op-name params return-type)
             "    }")))))

;; =============================================================================
;; Full Class Generation
;; =============================================================================

(defn generate-java-class
  "Generate complete DatahikeGenerated.java source."
  []
  (str
   "package datahike.java;\n\n"
   "import clojure.java.api.Clojure;\n"
   "import clojure.lang.IFn;\n"
   "import clojure.lang.APersistentMap;\n"
   "import clojure.lang.RT;\n"
   "import java.util.*;\n\n"
   "/**\n"
   " * Generated Datahike API bindings.\n"
   " * DO NOT EDIT - Generated from datahike.api.specification\n"
   " *\n"
   " * This class is package-private. Use the public Datahike facade instead.\n"
   " */\n"
   "class DatahikeGenerated {\n\n"
   "    // ===== Generated IFn Static Fields =====\n\n"
   (generate-ifn-declarations)
   "\n\n"
   "    // ===== Static Initialization =====\n\n"
   "    static {\n"
   "        IFn require = Clojure.var(\"clojure.core\", \"require\");\n"
   "        require.invoke(Clojure.read(\"datahike.api\"));\n"
   "    }\n\n"
   "    // ===== Generated Static Methods =====\n\n"
   (str/join "\n\n" (map generate-method (sort-by first api-specification)))
   "\n}\n"))

;; =============================================================================
;; File Writing
;; =============================================================================

(defn write-generated-source
  "Write generated Java source to file."
  [output-dir]
  (let [package-dir (io/file output-dir "datahike" "java")
        java-file (io/file package-dir "DatahikeGenerated.java")]
    (.mkdirs package-dir)
    (spit java-file (generate-java-class))
    (println "Generated:" (.getPath java-file))
    {:generated-files [(.getPath java-file)]}))

(defn -main
  "CLI entry point for Java source generation."
  [& args]
  (let [output-dir (or (first args) "java/src-generated")]
    (println "Generating Java API from specification...")
    (write-generated-source output-dir)
    (println "Done.")))

(comment
  ;; Test generation
  (println (generate-java-class))

  ;; Generate to file
  (write-generated-source "java/src-generated")

  ;; Test type mapping
  (malli->java-type :boolean)  ; => "boolean"
  (malli->java-type :datahike/SConnection)  ; => "Object"
  (malli->java-type [:sequential :any])  ; => "Iterable<?>"
  )
