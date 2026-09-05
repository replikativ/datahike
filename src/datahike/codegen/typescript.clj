(ns datahike.codegen.typescript
  "Generate TypeScript type definitions from api.specification.

  This namespace provides tooling to generate .d.ts files that:
  - Map malli schemas to TypeScript types
  - Provide proper Promise<T> types for async operations
  - Include JSDoc comments with full documentation
  - Support IDE autocompletion and type checking"
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.api.types :as types]
            [datahike.codegen.naming :refer [assert-unique-js-names!
                                             js-skip-list clj-name->js-name
                                             remote-js-exports]]
            [clojure.string :as str]))

;; =============================================================================
;; Malli -> TypeScript Type Mapping
;; =============================================================================

(defn malli->ts-type
  "Convert a Malli schema to the JavaScript representation's TypeScript type."
  [schema]
  (cond
    (keyword? schema)
    (or (get types/malli->typescript-type (keyword (name schema)))
        (case schema :number "number" :symbol "string" "any"))

    (symbol? schema)
    (get types/malli->typescript-type (keyword (name schema)) "any")

    (vector? schema)
    (let [[op & args] schema]
      (case op
        :or (str/join " | " (distinct (map malli->ts-type args)))
        :maybe (str (malli->ts-type (first args)) " | null")
        (:sequential :vector :set :* :+)
        (str "Array<" (malli->ts-type (first args)) ">")
        :map-of (str "Record<string, " (malli->ts-type (second args)) ">")
        :map "Record<string, any>"
        (:function :=> :fn) "Function"
        :cat (str "[" (str/join ", " (map malli->ts-type args)) "]")
        :alt (str/join " | " (distinct (map malli->ts-type args)))
        :enum (str/join " | " (map #(pr-str (if (keyword? %) (str %) %)) args))
        "any"))

    :else "any"))

(def ^:private parameter-names
  {'connect ["config" "opts"]
   'db ["conn"]
   'branches ["conn"]
   'branch! ["conn" "from" "newBranch"]
   'delete-branch! ["conn" "branch"]
   'force-branch! ["db" "branch" "parents"]
   'merge-db! ["conn" "parents" "tx" "txMeta"]
   'commit-id ["db"]
   'parent-commit-ids ["db"]
   'commit-as-db ["source" "commit"]
   'branch-as-db ["source" "branch"]
   'gc-storage ["conn" "removeBefore" "options"]
   'release ["conn" "releaseAll"]
   'transact! ["conn" "tx"]
   'with ["db" "tx" "txMeta"]
   'db-with ["db" "tx"]
   'q ["query" "inputs"]
   'query-stats ["query" "inputs"]
   'pull ["db" "patternOrOptions" "eid"]
   'pull-many ["db" "patternOrOptions" "eids"]
   'entity ["db" "eid"]
   'datoms ["db" "indexOrOptions" "components"]
   'seek-datoms ["db" "indexOrOptions" "components"]
   'rseek-datoms ["db" "indexOrOptions" "components"]
   'index-range ["db" "options"]
   'listen ["conn" "key" "listener"]
   'unlisten ["conn" "key"]
   'listen-commits ["conn" "key" "listener"]
   'unlisten-commits ["conn" "key"]})

(defn- function-arities [schema]
  (if (and (vector? schema) (= :function (first schema)))
    (rest schema)
    [schema]))

(defn- parameter-name [fn-name arity-size idx]
  (or (when (and (#{'listen 'listen-commits} fn-name)
                 (= arity-size 2) (= idx 1))
        "listener")
      (get-in parameter-names [fn-name idx])
      (str "arg" idx)))

(defn- parameter-type [fn-name arity-size idx schema]
  (cond
    (and (= fn-name 'listen-commits)
         (= idx (dec arity-size)))
    "(event: DurableCommitEvent) => void"

    (and (= fn-name 'listen)
         (= idx (dec arity-size)))
    "(report: TransactionReport) => void"

    ;; UUIDs crossing into the CLJS API must be constructed with uuid() or
    ;; randomUuid(). UUIDs crossing out are converted to plain strings.
    (= schema :uuid) "DatahikeUuid"

    ;; The first tempid argument is the Datomic-compatible partition keyword.
    ;; Datahike ignores the partition, but accepting arbitrary values only
    ;; weakens the JavaScript contract.
    (and (= fn-name 'tempid) (= idx 0)) "Keyword"

    (and (= fn-name 'branch!) (= idx 1)) "VersionRef"
    (and (= fn-name 'force-branch!) (= idx 2)) "VersionRef[]"
    (and (= fn-name 'merge-db!) (= idx 1)) "VersionRef[]"
    (and (= fn-name 'merge-db!) (= idx 3)) "unknown"
    (and (#{'commit-as-db 'branch-as-db} fn-name) (= idx 0))
    "Connection | Database"
    (and (= fn-name 'gc-storage) (= idx 2)) "GcOptions"
    (and (= fn-name 'connect) (= idx 1)) "ConnectOptions"

    :else (malli->ts-type schema)))

(defn- render-parameters [fn-name arity]
  (let [[_ input-schema _] arity
        schemas (if (and (vector? input-schema) (= :cat (first input-schema)))
                  (rest input-schema)
                  [])
        arity-size (count schemas)]
    (str/join
     ", "
     (map-indexed
      (fn [idx schema]
        (let [parameter (parameter-name fn-name arity-size idx)]
          (if (and (vector? schema) (#{:* :+} (first schema)))
            (str "..." parameter ": " (parameter-type fn-name arity-size idx schema))
            (str parameter ": " (parameter-type fn-name arity-size idx schema)))))
      schemas))))

(defn- generic-declaration [fn-name]
  (case fn-name
    q "<T = unknown>"
    pull "<T extends Record<string, any> = Record<string, any>>"
    pull-many "<T extends Record<string, any> = Record<string, any>>"
    ""))

(defn- return-type [fn-name arity fallback]
  (case fn-name
    q "T"
    pull "T | null"
    pull-many "T[]"
    transact! "TransactionReport"
    merge-db! "TransactionReport"
    branch! "unknown"
    delete-branch! "unknown"
    parent-commit-ids "UuidValue[] | null"
    gc-storage "unknown[]"
    (malli->ts-type (or (nth arity 2 nil) fallback))))

(defn generate-function-signatures
  "Generate every TypeScript overload from a specification entry."
  [[fn-name {:keys [args ret doc]}]]
  (let [ts-name (clj-name->js-name fn-name)]
    {:name ts-name
     :signatures
     (for [arity (function-arities args)]
       (str "export function " ts-name (generic-declaration fn-name)
            "(" (render-parameters fn-name arity) "): Promise<"
            (return-type fn-name arity ret) ">;"))
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
  "Generate complete TypeScript type definitions from api-specification.
   With `:remote-only? true`, the thin HTTP client's: the remote-capable
   functions and the value helpers, no optimistic overlay, no logging."
  ([] (generate-type-definitions {}))
  ([{:keys [remote-only?]}]
   (let [header "// Auto-generated TypeScript definitions for Datahike JavaScript API
// DO NOT EDIT - Generated from datahike.api.specification

"
        ;; Core type definitions
         types "
// Core Datahike Types

export type Keyword = `:${string}`;
export type Attribute = Keyword;

declare const datahikeUuidBrand: unique symbol;
/** Opaque UUID input returned by uuid() and randomUuid(). */
export interface DatahikeUuid {
  readonly [datahikeUuidBrand]: true;
}
/** UUID values returned by the JavaScript boundary are ordinary strings. */
export type UuidValue = string;

/** A named branch (for example `':db'` or `':feature'`). */
export type BranchName = Keyword;
/** A branch name or an input UUID created with uuid(). */
export type VersionRef = BranchName | DatahikeUuid;

export interface GcOptions {
  /** Keep objects written within this many milliseconds, even if unreachable. */
  'min-age-ms'?: number;
}

export interface StoreConfig {
  backend: Keyword;
  id?: DatahikeUuid;
  path?: string;
  [key: string]: any;
}

/** S3 backend configuration for the opt-in `datahike/s3` browser entry. */
export interface S3StoreConfig extends StoreConfig {
  backend: ':s3';
  id: DatahikeUuid;
  endpoint: string;
  bucket: string;
  'access-key': string;
  secret: string;
  region?: string;
  'session-token'?: string;
  'path-style?'?: boolean;
  config?: {
    'optimistic-locking-retries'?: number;
    [key: string]: any;
  };
}

/**
 * Browser-safe Datahike storage: a complete synchronous memory frontend over
 * an asynchronous, authoritative S3 backend. Bare S3StoreConfig is not a
 * usable Datahike query store in browsers.
 */
export interface TieredS3StoreConfig extends StoreConfig {
  backend: ':tiered';
  id: DatahikeUuid;
  'frontend-config': { backend: ':memory'; id: DatahikeUuid };
  'backend-config': S3StoreConfig;
  'write-policy'?: ':write-through';
  'read-policy'?: ':frontend-first';
}

export interface WriterConfig {
  backend: Keyword;
  [key: string]: any;
}

export interface ConnectOptions {
  /**
   * Preserve the ClojureScript API's execution mode. Memory stores can run
   * synchronously; browser and remote stores use false and resolve a Promise.
   */
  'sync?'?: boolean;
  [key: string]: any;
}

/** Persistent browser backend registered by the `datahike/kabel` entry. */
export interface IndexedDbStoreConfig extends StoreConfig {
  backend: ':indexeddb';
  id: DatahikeUuid;
  name?: string;
}

/** Synchronous memory frontend over an authoritative IndexedDB cache. */
export interface TieredIndexedDbStoreConfig extends StoreConfig {
  backend: ':tiered';
  id: DatahikeUuid;
  'frontend-config': { backend: ':memory'; id: DatahikeUuid };
  'backend-config': IndexedDbStoreConfig;
  'write-policy'?: ':write-through';
  'read-policy'?: ':frontend-first';
}

export interface DatabaseConfig {
  store: StoreConfig;
  writer?: WriterConfig;
  branch?: Keyword;
  'keep-history?'?: boolean;
  'schema-flexibility'?: ':read' | ':write';
  'initial-tx'?: Transaction[];
  name?: string;
  [key: string]: any;
}

declare const connectionBrand: unique symbol;
export interface Connection {
  readonly [connectionBrand]: true;
}

declare const databaseBrand: unique symbol;
export interface Database {
  readonly [databaseBrand]: true;
}

export type EntityId = number | string | [Attribute, any];
export type EntityMap = Record<string, any>;
export type Transaction =
  | [':db/add', EntityId, Attribute, any]
  | [':db/retract', EntityId, Attribute, any]
  | [Keyword, ...any[]]
  | EntityMap;

export interface WithArgs {
  'tx-data': Transaction[];
  'tx-meta'?: any;
}

export interface QueryArgs {
  query: string | any[] | Record<string, any>;
  args?: any[];
  limit?: number;
  offset?: number;
}

export interface PullOptions {
  selector: any[];
  eid: EntityId | EntityId[];
}

export type Index = ':eavt' | ':aevt' | ':avet';
export interface IndexLookupArgs {
  index: Index;
  components?: any[] | null;
}

export interface IndexRangeArgs {
  attrid: Attribute;
  start: any;
  end: any;
}

export interface TransactionReport {
  'db-before': Database;
  'db-after': Database;
  'tx-data': Datom[];
  tempids: { [key: string]: number };
  'tx-meta'?: any;
}

export interface DurableCommitEvent {
  type: ':datahike/commit';
  'store-id': UuidValue;
  branch: Keyword;
  'commit-id': UuidValue;
  'parent-commit-ids': UuidValue[];
  'max-tx': number;
  'tx-count': number;
  'db-before': Database;
  'db-after': Database;
  'tx-reports': TransactionReport[];
}

export interface Datom {
  e: number;
  a: Attribute;
  v: any;
  tx: number;
  added: boolean;
}

export interface Schema {
  [key: string]: {
    'db/valueType': Keyword;
    'db/cardinality': Keyword;
    'db/unique'?: Keyword;
    'db/index'?: boolean;
    [key: string]: any;
  };
}

export interface Metrics {
  count: number;
  'avet-count': number;
  'per-attr-counts': Record<string, number>;
  'per-entity-counts'?: Record<string, number>;
  'temporal-count'?: number;
  'temporal-avet-count'?: number;
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
  branch?: Keyword;
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
         clj-exports (if remote-only?
                       (remote-js-exports api-specification)
                       (for [[fn-name _] (sort-by first api-specification)
                             :when (not (contains? js-skip-list fn-name))]
                         fn-name))
         _ (assert-unique-js-names! clj-exports)
         functions (str/join "\n\n"
                             (for [fn-name clj-exports
                                   :let [spec-data (get api-specification fn-name)
                                         entry [fn-name spec-data]
                                         {:keys [signatures doc]} (generate-function-signatures entry)
                                         jsdoc (generate-jsdoc doc (:examples spec-data))]]
                               (str jsdoc "\n" (str/join "\n" signatures))))
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

// JavaScript-specific value helpers.
export function isPromise(value: any): value is Promise<unknown>;
export function uuid(value: string): DatahikeUuid;
export function randomUuid(): DatahikeUuid;

export type LogLevel = 'off' | 'trace' | 'debug' | 'info' | 'warn' | 'error';
/**
 * Configure logging for the Datahike JavaScript package.
 * The initial level is `warn`, or `DATAHIKE_LOG_LEVEL` in Node.js.
 */
export function setLogLevel(level: LogLevel): LogLevel;
"]
     (if remote-only?
       (str header types "\n// API Functions (the thin HTTP client; every call reaches the server)\n\n" functions "

export interface RemoteReport {
  'db-after'?: Database;
  'db-before'?: null;
  'tx-data'?: Datom[];
  tempids?: { [key: string]: number };
  'commit-id'?: UuidValue;
  resync?: boolean;
  deleted?: boolean;
  truncated?: boolean;
  error?: any;
  status?: number;
}

export function listen(conn: Connection, callback: (report: RemoteReport) => void): string;
export function unlisten(conn: Connection, key: string): void;

// JavaScript-specific value helpers.
export function isPromise(value: any): value is Promise<unknown>;
export function uuid(value: string): DatahikeUuid;
export function randomUuid(): DatahikeUuid;
")
       (str header types "\n// API Functions\n\n" functions optimistic-functions "\n")))))

(defn write-type-definitions!
  "Write TypeScript definitions to a file."
  ([]
   (write-type-definitions! "npm-package/index.d.ts"))
  ([output-path]
   (spit output-path (generate-type-definitions))
   (println "TypeScript definitions written to:" output-path)))

(defn generate-kabel-type-definitions []
  "export * from './index';

export interface KabelPeer {
  readonly __kabelPeerBrand: never;
}

/** A token, or a source read at every connection and for refreshes. */
export type KabelToken = string | (() => string | Promise<string>);

export interface KabelPeerOptions {
  token?: KabelToken;
  onAuth?: (principal: Record<string, any>) => void;
  onError?: (error: any) => void;
}

export type KabelStatus =
  | 'connecting' | 'connected' | 'authenticated' | 'disconnected' | 'failed' | 'stopped';

export interface KabelStatusEvent {
  status: KabelStatus;
  attempt?: number;
  error?: string;
  reason?: string;
  principal?: Record<string, any>;
  [key: string]: unknown;
}

export interface KabelMaintainOptions {
  onStatus?: (event: KabelStatusEvent) => void;
  backoff?: { 'initial-ms'?: number; 'max-ms'?: number; factor?: number; jitter?: number };
  maxAttempts?: number;
}

export interface KabelMaintainHandle {
  /** Stop reconnecting and close the current connection. */
  stop: () => void;
  /** Resolves once the reconnection loop has ended. */
  done: Promise<unknown>;
}

export interface KabelWriterConfig {
  backend: ':kabel';
  'peer-id': import('./index').DatahikeUuid;
  'local-peer': KabelPeer;
  url?: string;
}

export function createKabelPeer(
  clientId: import('./index').DatahikeUuid,
  options?: KabelPeerOptions
): KabelPeer;

/** Connect once; resolves with the server's peer id when remote calls work. */
export function connectKabelPeer(peer: KabelPeer, url: string): Promise<import('./index').DatahikeUuid>;
/** Keep the peer connected across drops, with backoff and status events. */
export function maintainKabelPeer(peer: KabelPeer, url: string, options?: KabelMaintainOptions): KabelMaintainHandle;
/** Replace the token on the live connection; resolves with the accepted principal. */
export function refreshKabelToken(peer: KabelPeer, token?: string): Promise<Record<string, any>>;
/** Invoke a function served by the peer remoteId, by its 'namespace/name'. */
export function invokeRemote<T = unknown>(
  peer: KabelPeer,
  remoteId: import('./index').DatahikeUuid,
  fnName: string,
  args?: Record<string, unknown>
): Promise<T>;
/** Serve a function from this process; the server may call back into it. */
export function registerRemoteFn(
  fnName: string,
  fn: (args: Record<string, any>) => unknown | Promise<unknown>
): string;
export function unregisterRemoteFn(fnName: string): void;
export function stopKabelPeer(peer: KabelPeer): Promise<boolean>;
")

(defn write-remote-type-definitions! [output-path]
  (spit output-path (generate-type-definitions {:remote-only? true}))
  (println "Thin-client TypeScript definitions written to:" output-path))

(defn write-kabel-type-definitions! [output-path]
  (spit output-path (generate-kabel-type-definitions))
  (println "Kabel TypeScript definitions written to:" output-path))

(comment
  ;; Generate types
  (println (generate-type-definitions))

  ;; Write to file
  (write-type-definitions!))
