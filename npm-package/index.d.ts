// Auto-generated TypeScript definitions for Datahike JavaScript API
// DO NOT EDIT - Generated from datahike.api.specification


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


// API Functions

/**
 * Returns database state at given time point (Date or transaction ID)..
 *
 * Examples:
 * - Query as of date
 *   (q '[:find ?n :where [_ :name ?n]] (as-of @conn date))
 * - Query as of transaction
 *   (as-of @conn 536870913)
 */
export function asOf(arg0: any, arg1: any): Promise<any>;

/**
 * Connects to a Datahike database via configuration map..
 *
 * Examples:
 * - Connect to default in-memory database
 *   (connect)
 * - Connect to file-based database
 *   (connect {:store {:backend :file :path "/tmp/example"}})
 */
export function connect(arg0: any): Promise<any>;

/**
 * Creates a database via configuration map..
 *
 * Examples:
 * - Create empty database
 *   (create-database {:store {:backend :memory :id "example"}})
 * - Create with schema-flexibility :read
 *   (create-database {:store {:backend :memory :id "example"} :schema-flexibility :read})
 */
export function createDatabase(arg0: any): Promise<any>;

/**
 * Checks if a database exists via configuration map..
 *
 * Examples:
 * - Check if in-memory database exists
 *   (database-exists? {:store {:backend :memory :id "example"}})
 * - Check with default config
 *   (database-exists?)
 */
export function databaseExists(arg0: any): Promise<boolean>;

/**
 * Index lookup.
 *
 * Examples:
 * - Find all datoms for entity
 *   (datoms db {:index :eavt :components [1]})
 * - Find datoms for entity and attribute
 *   (datoms db {:index :eavt :components [1 :likes]})
 */
export function datoms(arg0: any, arg1: any): Promise<any | null>;

/**
 * Returns the underlying immutable database value from a connection.
 *
 * Examples:
 * - Get database from connection
 *   (db conn)
 * - Prefer direct deref
 *   @conn
 */
export function db(arg0: any): Promise<any>;

/**
 * Applies transaction to immutable db value, returns new db.
 *
 * Examples:
 * - Get db after transaction
 *   (db-with @conn [[:db/add 1 :name "Ivan"]])
 */
export function dbWith(arg0: any, arg1: any): Promise<any>;

/**
 * Deletes a database given via configuration map..
 *
 * Examples:
 * - Delete database
 *   (delete-database {:store {:backend :memory :id "example"}})
 */
export function deleteDatabase(arg0: any): Promise<any>;

/**
 * Retrieves an entity by its id.
 *
 * Examples:
 * - Get entity by id
 *   (entity db 1)
 * - Get entity by lookup ref
 *   (entity db [:email "alice@example.com"])
 */
export function entity(arg0: any, arg1: any | any): Promise<any>;

/**
 * Returns database that entity was created from..
 *
 * Examples:
 * - Get entity's database
 *   (entity-db (entity db 1))
 */
export function entityDb(arg0: any): Promise<any>;

/**
 * Returns filtered view over database.
 *
 * Examples:
 * - Filter to recent datoms
 *   (filter db (fn [db datom] (> (:tx datom) recent-tx)))
 */
export function filter(arg0: any, arg1: any): Promise<any>;

/**
 * Invokes garbage collection on connection's store.
 *
 * Examples:
 * - GC all old snapshots
 *   (gc-storage conn)
 * - GC snapshots before date
 *   (gc-storage conn (java.util.Date.))
 */
export function gcStorage(arg0: any, arg1: any): Promise<any>;

/**
 * Returns full historical state of database including all assertions and retractions..
 *
 * Examples:
 * - Query historical data
 *   (q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] (history @conn))
 */
export function history(arg0: any): Promise<any>;

/**
 * Returns part of :avet index between start and end values..
 *
 * Examples:
 * - Find datoms in value range
 *   (index-range db {:attrid :likes :start "a" :end "z"})
 * - Find entities with age in range
 *   (->> (index-range db {:attrid :age :start 18 :end 60}) (map :e))
 */
export function indexRange(arg0: any, arg1: any): Promise<any>;

/**
 * Returns true if database was filtered using filter, false otherwise..
 *
 * Examples:
 * - Check if filtered
 *   (is-filtered db)
 */
export function isFiltered(arg0: any): Promise<boolean>;

/**
 * Listen for changes on connection.
 *
 * Examples:
 * - Listen with callback
 *   (listen conn (fn [tx-report] (println "Transaction:" (:tx-data tx-report))))
 * - Listen with key
 *   (listen conn :my-listener (fn [tx-report] ...))
 */
export function listen(arg0: any, arg1: any): Promise<any>;

/**
 * Load entities directly (bulk load)..
 *
 * Examples:
 * - Bulk load entities
 *   (load-entities conn entities)
 */
export function loadEntities(arg0: any, arg1: any): Promise<any>;

/**
 * Returns database metrics (datom counts, index sizes, etc)..
 *
 * Examples:
 * - Get metrics
 *   (metrics @conn)
 */
export function metrics(arg0: any): Promise<any>;

/**
 * Fetches data using recursive declarative pull pattern..
 *
 * Examples:
 * - Pull with pattern
 *   (pull db [:db/id :name :likes {:friends [:db/id :name]}] 1)
 * - Pull with arg-map
 *   (pull db {:selector [:db/id :name] :eid 1})
 */
export function pull(arg0: any, arg1: any): Promise<object | null>;

/**
 * Same as pull, but accepts sequence of ids and returns sequence of maps..
 *
 * Examples:
 * - Pull multiple entities
 *   (pull-many db [:db/id :name] [1 2 3])
 */
export function pullMany(arg0: any, arg1: any): Promise<Array<object>>;

/**
 * Executes a datalog query..
 *
 * Examples:
 * - Query with vector syntax
 *   (q '[:find ?value :where [_ :likes ?value]] db)
 * - Query with map syntax
 *   (q '{:find [?value] :where [[_ :likes ?value]]} db)
 */
export function q(arg0: any): Promise<any>;

/**
 * Executes query and returns execution statistics..
 *
 * Examples:
 * - Query with stats
 *   (query-stats '[:find ?e :where [?e :name]] db)
 */
export function queryStats(arg0: any): Promise<object>;

/**
 * Releases a database connection..
 *
 * Examples:
 * - Release connection
 *   (release conn)
 */
export function release(arg0: any): Promise<null>;

/**
 * Returns reverse schema definition (attribute id to ident mapping)..
 *
 * Examples:
 * - Get reverse schema
 *   (reverse-schema @conn)
 */
export function reverseSchema(arg0: any): Promise<object>;

/**
 * Returns current schema definition..
 *
 * Examples:
 * - Get schema
 *   (schema @conn)
 */
export function schema(arg0: any): Promise<any>;

/**
 * Like datoms, but returns datoms starting from specified components through end of index..
 *
 * Examples:
 * - Seek from entity
 *   (seek-datoms db {:index :eavt :components [1]})
 */
export function seekDatoms(arg0: any, arg1: any): Promise<any | null>;

/**
 * Returns database state since given time point (Date or transaction ID).
 *
 * Examples:
 * - Query since date
 *   (since @conn (java.util.Date.))
 * - Query since transaction
 *   (since @conn 536870913)
 */
export function since(arg0: any, arg1: any): Promise<any>;

/**
 * Allocates temporary id (negative integer).
 *
 * Examples:
 * - Generate tempid
 *   (tempid :db.part/user)
 * - Prefer direct negative integers
 *   (transact conn [{:db/id -1 :name "Alice"}])
 */
export function tempid(arg0: any): Promise<any | number>;

/**
 * Same as transact, but asynchronously returns a future..
 *
 * Examples:
 * - Async transaction
 *   @(transact! conn [{:db/id -1 :name "Alice"}])
 */
export function transact(arg0: any, arg1: any): Promise<any>;

/**
 * Removes registered listener from connection..
 *
 * Examples:
 * - Remove listener
 *   (unlisten conn :my-listener)
 */
export function unlisten(arg0: any, arg1: any): Promise<object>;

/**
 * Applies transaction to immutable db value.
 *
 * Examples:
 * - Transaction on db value
 *   (with @conn [[:db/add 1 :name "Ivan"]])
 * - With metadata
 *   (with @conn {:tx-data [...] :tx-meta {:source :import}})
 */
export function withDb(arg0: any, arg1: any): Promise<any>;
