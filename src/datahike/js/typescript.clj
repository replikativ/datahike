(ns datahike.js.typescript
  "Generate TypeScript type definitions from api.specification.
  
  This namespace provides tooling to generate .d.ts files that:
  - Map Clojure specs to TypeScript types
  - Provide proper Promise<T> types for async operations
  - Include JSDoc comments with full documentation
  - Support IDE autocompletion and type checking"
  (:require [datahike.api.specification :refer [api-specification spec-args->argslist]]
            [datahike.js.naming :refer [js-skip-list clj-name->js-name]]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]))

;; =============================================================================
;; Spec -> TypeScript Type Mapping
;; =============================================================================

(def spec-type-map
  "Map of Clojure spec predicates to TypeScript types"
  {'boolean? "boolean"
   'string? "string"
   'number? "number"
   'int? "number"
   'keyword? "string"  ;; Keywords represented as strings in JS
   'symbol? "string"
   'map? "object"
   'vector? "Array<any>"
   'coll? "Array<any>"
   'seq? "Array<any>"
   'any? "any"
   'nil? "null"
   'fn? "Function"})

(defn spec->ts-type
  "Convert a Clojure spec to a TypeScript type string.
  This is a simplified conversion - full spec->TS would be more complex."
  [spec-form]
  (cond
    ;; Direct spec predicates
    (contains? spec-type-map spec-form)
    (get spec-type-map spec-form)

    ;; Custom specs (from datahike.spec namespace)
    (and (symbol? spec-form) (str/starts-with? (name spec-form) "S"))
    (case (name spec-form)
      "SConfig" "DatabaseConfig"
      "SConnection" "Connection"
      "SDB" "Database"
      "STransactions" "Transaction[]"
      "STransactionReport" "TransactionReport"
      "SEId" "number | string"
      "SSchema" "Schema"
      "SMetrics" "Metrics"
      "SDatom" "Datom"
      "SDatoms" "Datom[]"
      "SWithArgs" "{ 'tx-data': Transaction[], 'tx-meta'?: any }"
      "SQueryArgs" "{ query: string | any[], args?: any[], limit?: number, offset?: number }"
      "SPullOptions" "{ selector: any[], eid: number | string }"
      "SIndexLookupArgs" "{ index: string, components?: any[] }"
      "SIndexRangeArgs" "{ attrid: string, start: any, end: any }"
      "any")  ;; fallback

    ;; s/alt (union types) - extract the actual types
    (and (seq? spec-form) (= (first spec-form) 's/alt))
    (let [types (take-nth 2 (rest spec-form))
          ts-types (set (map spec->ts-type types))]
      (str/join " | " (remove #{"null"} ts-types)))  ;; Remove null from unions

    ;; s/cat (single type for now, would need tuple support)
    (and (seq? spec-form) (= (first spec-form) 's/cat))
    "any"

    ;; s/or (union types)
    (and (seq? spec-form) (= (first spec-form) 's/or))
    (let [types (take-nth 2 (rest spec-form))
          ts-types (set (map spec->ts-type types))]
      (str/join " | " ts-types))

    ;; s/coll-of (array of type)
    (and (seq? spec-form) (= (first spec-form) 's/coll-of))
    (str "Array<" (spec->ts-type (second spec-form)) ">")

    ;; s/? (optional)
    (and (seq? spec-form) (= (first spec-form) 's/?))
    (str (spec->ts-type (second spec-form)) " | undefined")

    ;; s/* (array)
    (and (seq? spec-form) (= (first spec-form) 's/*))
    (str "Array<" (spec->ts-type (second spec-form)) ">")

    ;; Function predicate
    (and (seq? spec-form) (= (first spec-form) 'fn?))
    "Function"

    ;; Default
    :else "any"))

(defn extract-params-with-types
  "Extract parameter names and types from spec args.
  Returns vector of maps with :name, :type, and :optional keys."
  [args-spec]
  (if-not (seq? args-spec)
    []
    (let [[op & args] args-spec]
      (cond
        ;; s/alt represents optional variants (e.g., with config or without)
        (= op 's/alt)
        (let [variants (partition 2 args)
              ;; Take the most complete variant (usually first non-nil one)
              main-variant (or (first (filter #(not= :nil (first %)) variants))
                               (first variants))]
          (if main-variant
            (extract-params-with-types (second main-variant))
            []))

        ;; s/cat represents a sequence of parameters
        (= op 's/cat)
        (let [params (partition 2 args)]
          (vec
           (for [[param-key param-spec] params
                 :when (not (and (seq? param-spec)
                                 (= (first param-spec) 's/?)
                                 (seq? (second param-spec))
                                 (= (first (second param-spec)) 's/cat)
                                 (= (second (second param-spec)) :k)))] ;; Skip keyword-only params
             (let [optional? (and (seq? param-spec) (= (first param-spec) 's/?))
                   inner-spec (if optional? (second param-spec) param-spec)
                   ts-type (spec->ts-type inner-spec)]
               {:name (clj-name->js-name param-key)  ; Use shared naming function
                :type ts-type
                :optional (or optional? true)}))))  ;; Make all params optional for flexibility

        :else []))))

(defn generate-function-signature
  "Generate TypeScript function signature from specification entry."
  [[fn-name {:keys [args ret doc]}]]
  (let [ts-name (clj-name->js-name fn-name)  ; Use shared naming function
        params (extract-params-with-types args)
        params-str (str/join ", "
                             (map (fn [{:keys [name type optional]}]
                                    (str name (if optional "?" "") ": " type))
                                  params))
        return-type (cond
                      (= ret 'boolean?) "boolean"
                      (= ret 'string?) "string"
                      (= ret 'number?) "number"
                      (symbol? ret) (spec->ts-type ret)
                      (seq? ret) (spec->ts-type ret)
                      :else "any")
        ;; Wrap in Promise if it's an async operation
        final-return (str "Promise<" return-type ">")]
    {:name ts-name
     :signature (str "export function " ts-name "(" params-str "): " final-return ";")
     :doc doc}))

(defn generate-jsdoc
  "Generate JSDoc comment from docstring."
  [doc]
  (when doc
    (let [lines (str/split-lines doc)
          ;; Format multi-line doc
          formatted (if (> (count lines) 1)
                      (str "/**\n"
                           " * " (str/join "\n * " lines)
                           "\n */")
                      (str "/** " (first lines) " */"))]
      formatted)))

(defn generate-type-definitions
  "Generate complete TypeScript type definitions from api-specification."
  []
  (let [header "// Auto-generated TypeScript definitions for Datahike JavaScript API
// DO NOT EDIT - Generated from datahike.api.specification

"
        ;; Core type definitions
        types "
// Core Datahike Types

export interface DatabaseConfig {
  store: {
    backend: string;
    id?: string;
    path?: string;
    [key: string]: any;
  };
  'keep-history'?: boolean;
  'schema-flexibility'?: 'read' | 'write';
  'initial-tx'?: Transaction[];
  name?: string;
  [key: string]: any;
}

export interface Connection {
  [key: string]: any;
}

export interface Database {
  [key: string]: any;
}

export type Transaction = 
  | [':db/add', number | string, string, any]
  | [':db/retract', number | string, string, any]
  | { [key: string]: any };

export interface TransactionReport {
  'db-before': Database;
  'db-after': Database;
  'tx-data': Datom[];
  tempids: { [key: string]: number };
  'tx-meta'?: any;
}

export interface Datom {
  e: number;
  a: string;
  v: any;
  tx: number;
  added: boolean;
}

export interface Schema {
  [key: string]: {
    'db/valueType': string;
    'db/cardinality': string;
    'db/unique'?: string;
    'db/index'?: boolean;
    [key: string]: any;
  };
}

export interface Metrics {
  [key: string]: any;
}

"
        ;; Generate function signatures (filter using shared js-skip-list)
        functions (str/join "\n\n"
                            (for [entry (sort-by first api-specification)
                                  :when (not (contains? js-skip-list (first entry)))
                                  :let [{:keys [name signature doc]} (generate-function-signature entry)
                                        jsdoc (generate-jsdoc doc)]]
                              (str jsdoc "\n" signature)))]
    (str header types "\n// API Functions\n\n" functions "\n")))

(defn write-type-definitions!
  "Write TypeScript definitions to a file."
  ([]
   (write-type-definitions! "npm-package/index.d.ts"))
  ([output-path]
   (spit output-path (generate-type-definitions))
   (println "TypeScript definitions written to:" output-path)))

(comment
  ;; Generate types
  (println (generate-type-definitions))

  ;; Write to file
  (write-type-definitions!))
