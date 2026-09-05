// Generated from datahike.api.specification. DO NOT EDIT.
import { request } from "./core.js";
import type { Connection, ConnectOptions, Database, DatabaseConfig, DatahikeUuid, Datom, EntityId, GcOptions, IndexLookupArgs, IndexRangeArgs, Keyword, Metrics, PullOptions, QueryArgs, Schema, Transaction, TransactionReport, UuidValue, VersionRef, WithArgs } from "./core.js";

/**
 * Returns database state at given time point (Date or transaction ID).
 */
export function asOf(arg0: Database, arg1: number | Date): Promise<Database>;
export function asOf(...args: unknown[]): Promise<any> {
  return request("as-of", true, args, false);
}

/**
 * Create a new branch from an existing branch or commit. Secondary indices are CoW-branched automatically.
 */
export function branch(conn: Connection, from: VersionRef, newBranch: Keyword): Promise<unknown>;
export function branch(...args: unknown[]): Promise<any> {
  return request("branch", false, args, false);
}

/**
 * Load the database at a branch head. First argument can be a connection, db value, or raw store.
 */
export function branchAsDb(source: Connection | Database, branch: Keyword): Promise<Database | null>;
export function branchAsDb(...args: unknown[]): Promise<any> {
  return request("branch-as-db", false, args, false);
}

/**
 * List all known branch names. Returns set of keywords.
 */
export function branches(conn: Connection): Promise<Array<Keyword>>;
export function branches(...args: unknown[]): Promise<any> {
  return request("branches", false, args, false);
}

/**
 * Load the database at a specific commit. First argument can be a connection, db value, or raw store.
 */
export function commitAsDb(source: Connection | Database, commit: DatahikeUuid): Promise<Database | null>;
export function commitAsDb(...args: unknown[]): Promise<any> {
  return request("commit-as-db", false, args, false);
}

/**
 * Retrieve the commit-id for this database value.
 */
export function commitId(db: Database): Promise<UuidValue | null>;
export function commitId(...args: unknown[]): Promise<any> {
  return request("commit-id", true, args, false);
}

/**
 * Connects to a Datahike database via configuration map.
 */
export function connect(config: DatabaseConfig): Promise<Connection>;
export function connect(config: DatabaseConfig, opts: ConnectOptions): Promise<Connection>;
export function connect(): Promise<Connection>;
export function connect(...args: unknown[]): Promise<any> {
  return request("connect", false, args, false);
}

/**
 * Creates a database via configuration map.
 */
export function createDatabase(arg0: DatabaseConfig): Promise<DatabaseConfig>;
export function createDatabase(): Promise<DatabaseConfig>;
export function createDatabase(...args: unknown[]): Promise<any> {
  return request("create-database", false, args, true);
}

/**
 * Checks if a database exists via configuration map.
 */
export function databaseExists(arg0: DatabaseConfig): Promise<boolean>;
export function databaseExists(): Promise<boolean>;
export function databaseExists(...args: unknown[]): Promise<any> {
  return request("database-exists", false, args, false);
}

/**
 * Index lookup. Returns sequence of datoms matching index components.
 */
export function datoms(db: Database, indexOrOptions: ":eavt" | ":aevt" | ":avet" | IndexLookupArgs): Promise<Datom[] | null>;
export function datoms(db: Database, indexOrOptions: ":eavt" | ":aevt" | ":avet", ...components: Array<any>): Promise<Datom[] | null>;
export function datoms(...args: unknown[]): Promise<any> {
  return request("datoms", true, args, false);
}

/**
 * Returns the underlying immutable database value from a connection. Prefer using @conn directly.
 */
export function db(conn: Connection): Promise<Database>;
export function db(...args: unknown[]): Promise<any> {
  return request("db", false, args, false);
}

/**
 * Remove a branch. The branch data remains accessible until the next GC.
 */
export function deleteBranch(conn: Connection, branch: Keyword): Promise<unknown>;
export function deleteBranch(...args: unknown[]): Promise<any> {
  return request("delete-branch", false, args, false);
}

/**
 * Deletes a database given via configuration map.
 */
export function deleteDatabase(arg0: DatabaseConfig): Promise<any>;
export function deleteDatabase(): Promise<any>;
export function deleteDatabase(...args: unknown[]): Promise<any> {
  return request("delete-database", false, args, false);
}

/**
 * Retrieves an entity by its id. Returns lazy map-like structure.
 */
export function entity(db: Database, eid: number | [string, any] | string | any): Promise<any>;
export function entity(...args: unknown[]): Promise<any> {
  return request("entity", true, args, false);
}

/**
 * Returns database that entity was created from.
 */
export function entityDb(arg0: any): Promise<Database>;
export function entityDb(...args: unknown[]): Promise<any> {
  return request("entity-db", true, args, false);
}

/**
 * Invokes garbage collection on connection's store. Removes old snapshots before given time point. `:min-age-ms` spares anything written more recently than that, which is what makes collecting from outside the writer process possible — it must exceed the longest values-then-pointer window any writer can have. When omitted it defaults to 0 under an exclusive local writer (`:writer {:backend :self :writer-ownership :exclusive}`) and to 15 minutes under a shared or remote writer, where another process's commit in flight is invisible to this collector. The default is a bound on one awaited request, not a guarantee: size an explicit value above your longest in-flight window plus the largest clock difference between your processes (the stamps it compares against are each writer's own). Pass `{:min-age-ms 0}` explicitly to sweep without a floor.
 */
export function gcStorage(conn: Connection, removeBefore: number | Date, options: GcOptions): Promise<unknown[]>;
export function gcStorage(conn: Connection, removeBefore: number | Date): Promise<unknown[]>;
export function gcStorage(conn: Connection): Promise<unknown[]>;
export function gcStorage(...args: unknown[]): Promise<any> {
  return request("gc-storage", false, args, false);
}

/**
 * Returns full historical state of database including all assertions and retractions.
 */
export function history(arg0: Database): Promise<Database>;
export function history(...args: unknown[]): Promise<any> {
  return request("history", true, args, false);
}

/**
 * Returns part of :avet index between start and end values.
 */
export function indexRange(db: Database, options: IndexRangeArgs): Promise<Datom[]>;
export function indexRange(...args: unknown[]): Promise<any> {
  return request("index-range", true, args, false);
}

/**
 * Load entities directly (bulk load).
 */
export function loadEntities(arg0: Connection, arg1: Transaction[]): Promise<any>;
export function loadEntities(...args: unknown[]): Promise<any> {
  return request("load-entities", false, args, false);
}

/**
 * Create a merge commit combining the current branch with parent branches/commits. The caller provides the merged tx-data. Routed through the writer for serialization. Blocks until committed. WARNING: Do not call from listener callbacks — use merge-db! instead to avoid deadlocks.
 */
export function mergeDb(arg0: Connection, arg1: Array<any>, arg2: Transaction[]): Promise<TransactionReport>;
export function mergeDb(arg0: Connection, arg1: Array<any>, arg2: Transaction[], arg3: any): Promise<TransactionReport>;
export function mergeDb(...args: unknown[]): Promise<any> {
  return request("merge-db", false, args, false);
}

/**
 * Returns database metrics: datom counts overall, per attribute and for the indexed (AVET) attributes, plus the same for history when kept. Computed from the indices' subtree counts — O(#attributes · log n) — so it is cheap on a large database. `{:per-entity-counts? true}` adds `:per-entity-counts`, a walk over every datom with one map entry per entity.
 */
export function metrics(arg0: Database): Promise<Metrics>;
export function metrics(arg0: Database, arg1: Record<string, any>): Promise<Metrics>;
export function metrics(...args: unknown[]): Promise<any> {
  return request("metrics", true, args, false);
}

/**
 * Retrieve parent commit ids from this database value.
 */
export function parentCommitIds(db: Database): Promise<UuidValue[] | null>;
export function parentCommitIds(...args: unknown[]): Promise<any> {
  return request("parent-commit-ids", true, args, false);
}

/**
 * Fetches data using recursive declarative pull pattern.
 */
export function pull<T extends Record<string, any> = Record<string, any>>(db: Database, patternOrOptions: PullOptions): Promise<T | null>;
export function pull<T extends Record<string, any> = Record<string, any>>(db: Database, patternOrOptions: Array<any>, eid: number | [string, any] | string): Promise<T | null>;
export function pull(...args: unknown[]): Promise<any> {
  return request("pull", true, args, false);
}

/**
 * Same as pull, but accepts sequence of ids and returns sequence of maps.
 */
export function pullMany<T extends Record<string, any> = Record<string, any>>(db: Database, patternOrOptions: PullOptions): Promise<T[]>;
export function pullMany<T extends Record<string, any> = Record<string, any>>(db: Database, patternOrOptions: Array<any>, eids: Array<number | [string, any] | string>): Promise<T[]>;
export function pullMany(...args: unknown[]): Promise<any> {
  return request("pull-many", true, args, false);
}

/**
 * Executes a datalog query.
 */
export function q<T = unknown>(query: QueryArgs): Promise<T>;
export function q<T = unknown>(query: Array<any> | Record<string, any> | string, ...inputs: Array<any>): Promise<T>;
export function q(...args: unknown[]): Promise<any> {
  return request("q", true, args, false);
}

/**
 * Executes query and returns execution statistics.
 */
export function queryStats(query: QueryArgs): Promise<Record<string, any>>;
export function queryStats(query: Array<any> | Record<string, any>, ...inputs: Array<any>): Promise<Record<string, any>>;
export function queryStats(...args: unknown[]): Promise<any> {
  return request("query-stats", true, args, false);
}

/**
 * Releases a database connection.
 */
export function release(conn: Connection): Promise<null>;
export function release(conn: Connection, releaseAll: any): Promise<null>;
export function release(...args: unknown[]): Promise<any> {
  return request("release", false, args, false);
}

/**
 * Returns reverse schema definition (attribute id to ident mapping).
 */
export function reverseSchema(arg0: Database): Promise<Record<string, any>>;
export function reverseSchema(...args: unknown[]): Promise<any> {
  return request("reverse-schema", true, args, false);
}

/**
 * Like seek-datoms, but iterates BACKWARDS: datoms <= the given components, descending to the beginning of the index. Lazy on the persistent-sorted-set index — the primitive for windowed backwards pagination (latest-N, N-before-cursor).
 */
export function rseekDatoms(db: Database, indexOrOptions: ":eavt" | ":aevt" | ":avet" | IndexLookupArgs): Promise<Datom[] | null>;
export function rseekDatoms(db: Database, indexOrOptions: ":eavt" | ":aevt" | ":avet", ...components: Array<any>): Promise<Datom[] | null>;
export function rseekDatoms(...args: unknown[]): Promise<any> {
  return request("rseek-datoms", true, args, false);
}

/**
 * Returns current schema definition.
 */
export function schema(arg0: Database): Promise<Schema>;
export function schema(...args: unknown[]): Promise<any> {
  return request("schema", true, args, false);
}

/**
 * Like datoms, but returns datoms starting from specified components through end of index.
 */
export function seekDatoms(db: Database, indexOrOptions: ":eavt" | ":aevt" | ":avet" | IndexLookupArgs): Promise<Datom[] | null>;
export function seekDatoms(db: Database, indexOrOptions: ":eavt" | ":aevt" | ":avet", ...components: Array<any>): Promise<Datom[] | null>;
export function seekDatoms(...args: unknown[]): Promise<any> {
  return request("seek-datoms", true, args, false);
}

/**
 * Returns database state since given time point (Date or transaction ID). Contains only datoms added since that point.
 */
export function since(arg0: Database, arg1: number | Date): Promise<Database>;
export function since(...args: unknown[]): Promise<any> {
  return request("since", true, args, false);
}

/**
 * Applies transaction to the database and updates connection. Blocks until committed. WARNING: Do not call from listener callbacks or transaction functions — use transact! instead to avoid deadlocks.
 */
export function transact(arg0: Connection, arg1: Transaction[] | WithArgs): Promise<TransactionReport>;
export function transact(...args: unknown[]): Promise<any> {
  return request("transact", false, args, false);
}

/**
 * Strip any valid-time markers from `db` so the full vt-history is
 *            visible. Idempotent. Does not unwrap an existing FilteredDB; to
 *            drop an active filter, start from the unwrapped db.
 */
export function validAll(arg0: Database): Promise<Database>;
export function validAll(...args: unknown[]): Promise<any> {
  return request("valid-all", true, args, false);
}

/**
 * Tag `db` with a `:datahike/valid-at` marker so vt-aware secondary
 *            indices push the filter through `-search-at-vt`. Valid-time is a
 *            secondary-index axis; regular datalog patterns still require the
 *            built-in `(valid-at ?tx ?at)` rule to filter by vt explicitly.
 */
export function validAt(arg0: Database, arg1: any): Promise<Database>;
export function validAt(...args: unknown[]): Promise<any> {
  return request("valid-at", true, args, false);
}

/**
 * Filter `db` to datoms whose asserting tx's vt-window overlaps the
 *            half-open interval `[from, to)`. SQL:2011 `FOR VALID_TIME BETWEEN
 *            from AND to` maps to this. Carries
 *            `:datahike/valid-between [from to]` on the result for vt-aware
 *            secondary-index pushdown.
 */
export function validBetween(arg0: Database, arg1: any, arg2: any): Promise<Database>;
export function validBetween(...args: unknown[]): Promise<any> {
  return request("valid-between", true, args, false);
}

/**
 * Filter `db` to datoms whose tx's vt-window is *fully contained*
 *            in `[from, to)`. Stricter than `valid-between` — overlapping
 *            windows that extend past either endpoint are excluded. Useful
 *            for 'corrections wholly within Q2' style queries.
 */
export function validDuring(arg0: Database, arg1: any, arg2: any): Promise<Database>;
export function validDuring(...args: unknown[]): Promise<any> {
  return request("valid-during", true, args, false);
}
