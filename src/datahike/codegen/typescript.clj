(ns datahike.codegen.typescript
  "Generate TypeScript type definitions from api.specification.

  This namespace provides tooling to generate .d.ts files that:
  - Map malli schemas to TypeScript types
  - Provide proper Promise<T> types for async operations
  - Include JSDoc comments with full documentation
  - Support IDE autocompletion and type checking"
  (:require [datahike.api.specification :refer [api-specification malli-schema->argslist]]
            [datahike.api.types :as types]
            [datahike.codegen.naming :refer [js-skip-list clj-name->js-name]]
            [clojure.string :as str]))

;; =============================================================================
;; Malli -> TypeScript Type Mapping
;; =============================================================================

(defn malli->ts-type
  "Convert a malli schema to a TypeScript type string."
  [schema]
  (cond
    ;; Keyword schemas (primitives)
    (keyword? schema)
    (case schema
      :boolean "boolean"
      :string "string"
      :int "number"
      :long "number"
      :double "number"
      :number "number"
      :keyword "string"
      :symbol "string"
      :any "any"
      :nil "null"
      :map "object"
      :vector "Array<any>"
      :sequential "Array<any>"
      :set "Set<any>"
      "any")

    ;; Symbols (type references)
    (symbol? schema)
    (let [schema-name (name schema)]
      (cond
        ;; types/SConfig → DatabaseConfig
        (str/starts-with? schema-name "types/")
        (get types/malli->typescript-type
             (keyword (subs schema-name 6))
             "any")

        ;; Direct type names
        :else
        (get types/malli->typescript-type
             (keyword schema-name)
             "any")))

    ;; Vector schemas
    (vector? schema)
    (let [[op & args] schema]
      (case op
        ;; [:or Type1 Type2] → Type1 | Type2
        ;;
        ;; Distinct, because branches that differ in malli need not differ in
        ;; TypeScript: `entity`'s `[:or :datahike/SEId :any]` maps both to `any`
        ;; and emitted the degenerate `any | any`. Same reason `codegen/java`
        ;; deduplicates expanded overloads by signature.
        :or
        (str/join " | " (distinct (map malli->ts-type args)))

        ;; [:maybe Type] → Type | null
        :maybe
        (str (malli->ts-type (first args)) " | null")

        ;; [:sequential Type] → Array<Type>
        :sequential
        (str "Array<" (malli->ts-type (first args)) ">")

        ;; [:vector Type] → Array<Type>
        :vector
        (str "Array<" (malli->ts-type (first args)) ">")

        ;; [:map ...] → object
        :map
        "object"

        ;; [:function ...] or [:=> ...] - extract return type
        (:function :=>)
        "any"

        ;; [:cat ...] - tuple, represent as array for now
        :cat
        "Array<any>"

        ;; [:alt ...] - union
        :alt
        "any"

        ;; [:* Type] - rest params
        :*
        (str "..." (malli->ts-type (first args)) "[]")

        ;; [:enum ...] - union of literals
        :enum
        (str/join " | " (map #(str "'" % "'") args))

        ;; [:fn ...] - function type
        :fn
        "Function"

        ;; Default
        "any"))

    ;; Default
    :else "any"))

(defn extract-params-from-malli
  "Extract parameter information from malli function schema.
   Returns vector of maps with :name, :type, and :optional keys."
  [args-schema]
  (cond
    ;; [:=> [:cat Type1 Type2] Return]
    (and (vector? args-schema) (= :=> (first args-schema)))
    (let [[_ input-schema _] args-schema]
      (when (and (vector? input-schema) (= :cat (first input-schema)))
        (vec
         (map-indexed
          (fn [idx type-schema]
            {:name (str "arg" idx)
             :type (malli->ts-type type-schema)
             :optional false})
          (rest input-schema)))))

    ;; [:function [:=> ...] [:=> ...]] - multi-arity, use first
    (and (vector? args-schema) (= :function (first args-schema)))
    (let [first-arity (second args-schema)]
      (extract-params-from-malli first-arity))

    ;; Default
    :else []))

(defn generate-function-signature
  "Generate TypeScript function signature from specification entry."
  [[fn-name {:keys [args ret doc]}]]
  (let [ts-name (clj-name->js-name fn-name)
        params (extract-params-from-malli args)
        params-str (str/join ", "
                             (map (fn [{:keys [name type optional]}]
                                    (str name (if optional "?" "") ": " type))
                                  params))
        return-type (malli->ts-type ret)
        ;; Wrap in Promise for async operations
        final-return (str "Promise<" return-type ">")]
    {:name ts-name
     :signature (str "export function " ts-name "(" params-str "): " final-return ";")
     :doc doc}))

(defn generate-jsdoc
  "Generate JSDoc comment from docstring and examples."
  [doc examples]
  (when doc
    (let [;; Extract first sentence for summary
          summary (first (str/split doc #"\.\s"))
          ;; Add examples if available
          example-text (when (seq examples)
                         (str "\n *\n * Examples:\n"
                              (str/join "\n"
                                        (map (fn [{:keys [desc code]}]
                                               (str " * - " desc "\n"
                                                    " *   " code))
                                             (take 2 examples)))))]
      (str "/**\n * " summary "."
           example-text
           "\n */"))))

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

export interface OptimisticOverlay {
  readonly __optimisticOverlayBrand: never;
}

export interface OptimisticOptions {
  'max-pending'?: number;
  'max-queue'?: number;
  'prediction-timeout-ms'?: number | null;
  'reconciliation-timeout-ms'?: number | null;
}

export interface OptimisticSubmitOptions {
  branch?: string;
}

export interface OptimisticPredictionOptions extends OptimisticSubmitOptions {
  'timeout-ms'?: number | null;
}

export type OptimisticTransactionInput =
  | Transaction[]
  | { 'tx-data': Transaction[]; 'tx-meta'?: any };

export type OptimisticResultStatus =
  | ':committed'
  | ':accepted'
  | ':reconciled'
  | ':rejected'
  | ':expired'
  | ':abandoned'
  | ':unknown'
  | ':detached';

export interface OptimisticResult {
  status: OptimisticResultStatus;
  error?: any;
  receipt?: any;
  'tx-report'?: TransactionReport;
  outcome?: ':unknown' | ':committed';
  reason?: any;
  'accepted?'?: boolean;
}

export interface OptimisticHandle {
  ovId: string;
  result: Promise<OptimisticResult>;
}

export interface OptimisticPendingEntry {
  'ov-id': string;
  kind: ':writer' | ':prediction';
  'submitted-at': number;
  'expires-at'?: number | null;
  'expected-max-tx'?: number | null;
  branch?: string;
  'acknowledged?': boolean;
  'conflicting?': boolean;
  'last-conflict-error'?: any;
  'reconcile-deadline-at'?: number | null;
  'reconciliation-stalled?'?: boolean;
  [key: string]: any;
}

export interface OptimisticChanges {
  added: Datom[];
  removed: Datom[];
}

export interface OptimisticTransition {
  revision: number;
  'db-before': Database;
  'db-after': Database;
  'base-max-tx': number | null;
  cause: { type: string; [key: string]: any };
  changes: OptimisticChanges | null;
}

export interface OptimisticStatusEvent {
  revision: number;
  'ov-id': string;
  kind: ':writer' | ':prediction';
  status:
    | ':visible'
    | ':acknowledged'
    | ':committed'
    | ':reconciled'
    | ':rejected'
    | ':expired'
    | ':abandoned'
    | ':conflicting'
    | ':applicable'
    | ':reconciliation-error'
    | ':reconciliation-stalled'
    | ':unknown'
    | ':detached';
  [key: string]: any;
}

"
        ;; Generate function signatures
        functions (str/join "\n\n"
                            (for [entry (sort-by first api-specification)
                                  :when (not (contains? js-skip-list (first entry)))
                                  :let [[fn-name spec-data] entry
                                        {:keys [name signature doc]} (generate-function-signature entry)
                                        jsdoc (generate-jsdoc doc (:examples spec-data))]]
                              (str jsdoc "\n" signature)))
        optimistic-functions "

// Explicit optimistic overlay API. Unlike specification-generated functions,
// snapshot reads and lifecycle commands are synchronous.
export function openOptimistic(conn: Connection, opts?: OptimisticOptions): OptimisticOverlay;
export function optimisticDb(overlay: OptimisticOverlay): Database;
export function optimisticPending(overlay: OptimisticOverlay): OptimisticPendingEntry[];
export function optimisticTransact(overlay: OptimisticOverlay, txData: OptimisticTransactionInput, opts?: OptimisticSubmitOptions): OptimisticHandle;
export function optimisticPredict(overlay: OptimisticOverlay, txData: OptimisticTransactionInput, reconciled: (db: Database) => boolean, opts?: OptimisticPredictionOptions): OptimisticHandle;
export function optimisticAck(overlay: OptimisticOverlay, ovId: string, receipt?: any): null;
export function optimisticReject(overlay: OptimisticOverlay, ovId: string, error: any): null;
export function optimisticAbandon(overlay: OptimisticOverlay, ovId: string, reason?: any): null;
export function optimisticListen(overlay: OptimisticOverlay, listener: (event: OptimisticTransition) => void): () => void;
export function optimisticListenStatus(overlay: OptimisticOverlay, listener: (event: OptimisticStatusEvent) => void): () => void;
export function closeOptimistic(overlay: OptimisticOverlay): null;
"]
    (str header types "\n// API Functions\n\n" functions optimistic-functions "\n")))

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
