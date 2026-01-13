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
 * Returns the database state at given point in time (you may use either java.util.Date or transaction ID as long).
 * 
 * (transact conn {:tx-data [{:db/ident :name
 *                 :db/valueType :db.type/string
 *                 :db/unique :db.unique/identity
 *                 :db/index true
 *                 :db/cardinality :db.cardinality/one}
 *                {:db/ident :age
 *                 :db/valueType :db.type/long
 *                 :db/cardinality :db.cardinality/one}]})
 * 
 * (transact conn {:tx-data [{:name "Alice" :age 25} {:name "Bob" :age 30}]})
 * 
 * (def date (java.util.Date.))
 * 
 * (transact conn {:tx-data [{:db/id [:name "Alice"] :age 35}]})
 * 
 * (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
 *     :args [(as-of @conn date)]}) ; => #{["Alice" 25] ["Bob" 30]}
 * 
 * (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
 *     :args [@conn]}) ; => #{["Alice" 35] ["Bob" 30]}
 */
export function asOf(db?: Database, timePoint?: any): Promise<Database>;

/**
 * Connects to a datahike database via configuration map. For more information on the configuration refer to the [docs](https://github.com/replikativ/datahike/blob/master/doc/config.md).
 * 
 * The configuration for a connection is a subset of the Datahike configuration with only the store necessary: `:store`.
 * 
 * `:store` defines the backend configuration as hash-map with mandatory key: `:backend` and store dependent keys.
 * 
 * Per default Datahike ships with `:memory` and `:file` backend.
 * 
 * The default configuration:
 * `{:store {:backend :memory :id "default"}}`
 * 
 * Usage:
 * 
 * Connect to default in-memory configuration:
 * `(connect)`
 * 
 * Connect to a database with persistent store:
 * `(connect {:store {:backend :file :path "/tmp/example"}})`
 */
export function connect(config?: DatabaseConfig): Promise<Connection>;

/**
 * Creates a database via configuration map. For more information on the configuration refer to the [docs](https://github.com/replikativ/datahike/blob/master/doc/config.md).
 * 
 * The configuration is a hash-map with keys: `:store`, `:initial-tx`, `:keep-history?`, `:schema-flexibility`, `:index`
 * 
 * - `:store` defines the backend configuration as hash-map with mandatory key: `:backend` and store dependent keys.
 * Per default Datahike ships with `:memory` and `:file` backend.
 * - `:initial-tx` defines the first transaction into the database, often setting default data like the schema.
 * - `:keep-history?` is a boolean that toggles whether Datahike keeps historical data.
 * - `:schema-flexibility` can be set to either `:read` or `:write` setting the validation method for the data.
 * - `:read` validates the data when your read data from the database, `:write` validates the data when you transact new data.
 * - `:index` defines the data type of the index. Available are `:datahike.index/hitchhiker-tree`, `:datahike.index/persistent-set` (only available with in-memory storage)
 * - `:name` defines your database name optionally, if not set, a random name is created
 * - `:writer` optionally configures a writer as a hash map. If not set, the default local writer is used.
 * 
 * Default configuration has in-memory store, keeps history with write schema flexibility, and has no initial transaction:
 * `{:store {:backend :memory :id "default"} :keep-history? true :schema-flexibility :write}`
 * 
 * Usage:
 * 
 *     ;; create an empty database:
 *     (create-database {:store {:backend :memory :id "example"} :name "my-favourite-database"})
 * 
 *     ;; Datahike has a strict schema validation (schema-flexibility `:write`) policy by default, that only allows transaction of data that has been pre-defined by a schema.
 *     ;; You may influence this behaviour using the `:schema-flexibility` attribute:
 *     (create-database {:store {:backend :memory :id "example"} :schema-flexibility :read})
 * 
 *     ;; By writing historical data in a separate index, datahike has the capability of querying data from any point in time.
 *     ;; You may control this feature using the `:keep-history?` attribute:
 *     (create-database {:store {:backend :memory :id "example"} :keep-history? false})
 * 
 *     ;; Initial data after creation may be added using the `:initial-tx` attribute, which in this example adds a schema:
 *     (create-database {:store {:backend :memory :id "example"} :initial-tx [{:db/ident :name :db/valueType :db.type/string :db.cardinality/one}]})
 */
export function createDatabase(config?: DatabaseConfig): Promise<any>;

/**
 * Checks if a database exists via configuration map.
 * Usage:
 * 
 *     (database-exists? {:store {:backend :memory :id "example"}})
 */
export function databaseExists(config?: DatabaseConfig): Promise<boolean>;

/**
 * Index lookup. Returns a sequence of datoms (lazy iterator over actual DB index) which components
 * (e, a, v) match passed arguments. Datoms are sorted in index sort order. Possible `index` values
 * are: `:eavt`, `:aevt`, `:avet`.
 * 
 * Accepts db and a map as arguments with the keys `:index` and `:components` provided within the
 * map, or the arguments provided separately.
 * 
 * 
 * Usage:
 * 
 * Set up your database. Beware that for the `:avet` index the index needs to be set to true for
 * the attribute `:likes`.
 * 
 *     (d/transact db [{:db/ident :name
 *                      :db/type :db.type/string
 *                      :db/cardinality :db.cardinality/one}
 *                     {:db/ident :likes
 *                      :db/type :db.type/string
 *                      :db/index true
 *                      :db/cardinality :db.cardinality/many}
 *                     {:db/ident :friends
 *                      :db/type :db.type/ref
 *                      :db/cardinality :db.cardinality/many}]
 * 
 *     (d/transact db [{:db/id 4 :name "Ivan"
 *                     {:db/id 4 :likes "fries"
 *                     {:db/id 4 :likes "pizza"}
 *                     {:db/id 4 :friends 5}])
 * 
 *     (d/transact db [{:db/id 5 :name "Oleg"}
 *                     {:db/id 5 :likes "candy"}
 *                     {:db/id 5 :likes "pie"}
 *                     {:db/id 5 :likes "pizza"}])
 * 
 * Find all datoms for entity id == 1 (any attrs and values) sort by attribute, then value
 * 
 *     (datoms @db {:index :eavt
 *                  :components [1]}) ; => (#datahike/Datom [1 :friends 2]
 *                                    ;     #datahike/Datom [1 :likes "fries"]
 *                                    ;     #datahike/Datom [1 :likes "pizza"]
 *                                    ;     #datahike/Datom [1 :name "Ivan"])
 * 
 * Find all datoms for entity id == 1 and attribute == :likes (any values) sorted by value
 * 
 *     (datoms @db {:index :eavt
 *                  :components [1 :likes]}) ; => (#datahike/Datom [1 :likes "fries"]
 *                                           ;     #datahike/Datom [1 :likes "pizza"])
 * 
 * Find all datoms for entity id == 1, attribute == :likes and value == "pizza"
 * 
 *     (datoms @db {:index :eavt
 *                  :components [1 :likes "pizza"]}) ; => (#datahike/Datom [1 :likes "pizza"])
 * 
 * Find all datoms for attribute == :likes (any entity ids and values) sorted by entity id, then value
 * 
 *     (datoms @db {:index :aevt
 *                  :components [:likes]}) ; => (#datahike/Datom [1 :likes "fries"]
 *                                         ;     #datahike/Datom [1 :likes "pizza"]
 *                                         ;     #datahike/Datom [2 :likes "candy"]
 *                                         ;     #datahike/Datom [2 :likes "pie"]
 *                                         ;     #datahike/Datom [2 :likes "pizza"])
 * 
 * Find all datoms that have attribute == `:likes` and value == `"pizza"` (any entity id)
 * `:likes` must be a unique attr, reference or marked as `:db/index true`
 * 
 *     (datoms @db {:index :avet
 *                  :components [:likes "pizza"]}) ; => (#datahike/Datom [1 :likes "pizza"]
 *                                                   ;     #datahike/Datom [2 :likes "pizza"])
 * 
 * Find all datoms sorted by entity id, then attribute, then value
 * 
 *     (datoms @db {:index :eavt}) ; => (...)
 * 
 * 
 * Useful patterns:
 * 
 * Get all values of :db.cardinality/many attribute
 * 
 *     (->> (datoms @db {:index :eavt
 *                       :components [eid attr]})
 *          (map :v))
 * 
 * Lookup entity ids by attribute value
 * 
 *     (->> (datoms @db {:index :avet
 *                       :components [attr value]})
 *          (map :e))
 * 
 * Find all entities with a specific attribute
 * 
 *     (->> (datoms @db {:index :aevt
 *                       :components [attr]})
 *          (map :e))
 * 
 * Find “singleton” entity by its attr
 * 
 *     (->> (datoms @db {:index :aevt
 *                       :components [attr]})
 *          first
 *          :e)
 * 
 * Find N entities with lowest attr value (e.g. 10 earliest posts)
 * 
 *     (->> (datoms @db {:index :avet
 *                       :components [attr]})
 *          (take N))
 * 
 * Find N entities with highest attr value (e.g. 10 latest posts)
 * 
 *     (->> (datoms @db {:index :avet
 *                       :components [attr]})
 *          (reverse)
 *          (take N))
 * 
 * 
 * Gotchas:
 * 
 * - Index lookup is usually more efficient than doing a query with a single clause.
 * - Resulting iterator is calculated in constant time and small constant memory overhead.
 * - Iterator supports efficient `first`, `next`, `reverse`, `seq` and is itself a sequence.
 * - Will not return datoms that are not part of the index (e.g. attributes with no `:db/index` in schema when querying `:avet` index).
 *   - `:eavt` and `:aevt` contain all datoms.
 *   - `:avet` only contains datoms for references, `:db/unique` and `:db/index` attributes.
 */
export function datoms(db?: Database, args?: { index: string, components?: any[] }): Promise<any>;

/**
 * Returns the underlying immutable database value from a connection.
 * 
 * Exists for Datomic API compatibility. Prefer using `@conn` directly if possible.
 */
export function db(conn?: Connection): Promise<Database>;

/** Applies transaction to an immutable db value, returning new immutable db value. Same as `(:db-after (with db tx-data))`. */
export function dbWith(db?: Database, txData?: Transaction[]): Promise<Database>;

/**
 * Deletes a database given via configuration map. Storage configuration `:store` is mandatory.
 * For more information refer to the [docs](https://github.com/replikativ/datahike/blob/master/doc/config.md)
 */
export function deleteDatabase(config?: DatabaseConfig): Promise<any>;

/**
 * Retrieves an entity by its id from database. Entities are lazy map-like structures to navigate Datahike database content.
 * 
 * For `eid` pass entity id or lookup attr:
 * 
 *     (entity db 1)
 *     (entity db [:unique-attr :value])
 * 
 * If entity does not exist, `nil` is returned:
 * 
 *     (entity db -1) ; => nil
 * 
 * Creating an entity by id is very cheap, almost no-op, as attr access is on-demand:
 * 
 *     (entity db 1) ; => {:db/id 1}
 * 
 * Entity attributes can be lazily accessed through key lookups:
 * 
 *     (:attr (entity db 1)) ; => :value
 *     (get (entity db 1) :attr) ; => :value
 * 
 * Cardinality many attributes are returned sequences:
 * 
 *     (:attrs (entity db 1)) ; => [:v1 :v2 :v3]
 * 
 * Reference attributes are returned as another entities:
 * 
 *     (:ref (entity db 1)) ; => {:db/id 2}
 *     (:ns/ref (entity db 1)) ; => {:db/id 2}
 * 
 * References can be walked backwards by prepending `_` to name part of an attribute:
 * 
 *     (:_ref (entity db 2)) ; => [{:db/id 1}]
 *     (:ns/_ref (entity db 2)) ; => [{:db/id 1}]
 * 
 * Reverse reference lookup returns sequence of entities unless attribute is marked as `:db/component`:
 * 
 *     (:_component-ref (entity db 2)) ; => {:db/id 1}
 * 
 * Entity gotchas:
 * 
 * - Entities print as map, but are not exactly maps (they have compatible get interface though).
 * - Entities are effectively immutable “views” into a particular version of a database.
 * - Entities retain reference to the whole database.
 * - You can't change database through entities, only read.
 * - Creating an entity by id is very cheap, almost no-op (attributes are looked up on demand).
 * - Comparing entities just compares their ids. Be careful when comparing entities taken from different dbs or from different versions of the same db.
 * - Accessed entity attributes are cached on entity itself (except backward references).
 * - When printing, only cached attributes (the ones you have accessed before) are printed. See [[touch]].
 */
export function entity(db?: Database, eid?: any): Promise<any>;

/** Returns a db that entity was created from. */
export function entityDb(entity?: any): Promise<Database>;

/**
 * Returns a view over database that has same interface but only includes datoms for which the `(pred db datom)` is true. Can be applied multiple times.
 * 
 * Filtered DB gotchas:
 * 
 * - All operations on filtered database are proxied to original DB, then filter pred is applied.
 * - Not cached. You pay filter penalty every time.
 * - Supports entities, pull, queries, index access.
 * - Does not support hashing of DB.
 * - Does not support [[with]] and [[db-with]].
 */
export function filter(db?: Database, pred?: any): Promise<any>;

/**
 * Invokes garbage collection on the store of connection by whitelisting currently known branches.
 * All db snapshots on these branches before remove-before date will also be
 * erased (defaults to beginning of time [no erasure]). The branch heads will
 * always be retained. Return the set of removed blobs from the store.
 */
export function gcStorage(conn?: Connection, removeBefore?: any): Promise<any>;

/**
 * Returns the full historical state of the database you may interact with.
 * 
 * 
 * (transact conn {:tx-data [{:db/ident :name
 *                            :db/valueType :db.type/string
 *                            :db/unique :db.unique/identity
 *                            :db/index true
 *                            :db/cardinality :db.cardinality/one}
 *                           {:db/ident :age
 *                            :db/valueType :db.type/long
 *                            :db/cardinality :db.cardinality/one}]})
 * 
 * (transact conn {:tx-data [{:name "Alice" :age 25} {:name "Bob" :age 30}]})
 * 
 * (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
 *     :args [(history @conn)]}) ; => #{["Alice" 25] ["Bob" 30]}
 * 
 * (transact conn {:tx-data [{:db/id [:name "Alice"] :age 35}]})
 * 
 * (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
 *     :args [@conn]}) ; => #{["Alice" 35] ["Bob" 30]}
 * 
 * (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
 *     :args [(history @conn)]}) ; => #{["Alice" 25] ["Bob" 30]}
 */
export function history(db?: Database): Promise<Array<any>>;

/**
 * Returns part of `:avet` index between `[_ attr start]` and `[_ attr end]` in AVET sort order.
 * 
 * Same properties as [[datoms]].
 * 
 * `attr` must be a reference, unique attribute or marked as `:db/index true`.
 * 
 * Usage:
 * 
 * 
 * (transact db {:tx-data [{:db/ident :name
 *                          :db/type :db.type/string
 *                          :db/cardinality :db.cardinality/one}
 *                         {:db/ident :likes
 *                          :db/index true
 *                          :db/type :db.type/string
 *                          :db/cardinality :db.cardinality/many}
 *                         {:db/ident :age
 *                          :db/unique :db.unique/identity
 *                          :db/type :db.type/ref
 *                          :db/cardinality :db.cardinality/many}]})
 * 
 * (transact db {:tx-data [{:name "Ivan"}
 *                         {:age 19}
 *                         {:likes "fries"}
 *                         {:likes "pizza"}
 *                         {:likes "candy"}
 *                         {:likes "pie"}
 *                         {:likes "pizza"}]})
 * 
 * (index-range db {:attrid :likes
 *                  :start  "a"
 *                  :end    "zzzzzzzzz"}) ; => '(#datahike/Datom [2 :likes "candy"]
 *                                          ;      #datahike/Datom [1 :likes "fries"]
 *                                          ;      #datahike/Datom [2 :likes "pie"]
 *                                          ;      #datahike/Datom [1 :likes "pizza"]
 *                                          ;      #datahike/Datom [2 :likes "pizza"])
 * 
 * (index-range db {:attrid :likes
 *                  :start  "egg"
 *                  :end    "pineapple"}) ; => '(#datahike/Datom [1 :likes "fries"]
 *                                          ;      #datahike/Datom [2 :likes "pie"])
 * 
 * Useful patterns:
 * 
 *     ; find all entities with age in a specific range (inclusive)
 *     (->> (index-range db {:attrid :age :start 18 :end 60}) (map :e))
 */
export function indexRange(db?: Database, args?: { attrid: string, start: any, end: any }): Promise<Datom[]>;

/** Returns `true` if this database was filtered using [[filter]], `false` otherwise. */
export function isFiltered(db?: Database): Promise<boolean>;

/**
 * Listen for changes on the given connection. Whenever a transaction is applied to the database via
 * [[transact]], the callback is called with the transaction report. `key` is any opaque unique value.
 * 
 * Idempotent. Calling [[listen]] with the same twice will override old callback with the new value.
 * 
 * Returns the key under which this listener is registered. See also [[unlisten]].
 */
export function listen(conn?: Connection, callback?: Function): Promise<any>;

/** Load entities directly */
export function loadEntities(conn?: Connection, txs?: Transaction[]): Promise<any>;

/** Returns database metrics. */
export function metrics(db?: Database): Promise<Metrics>;

/**
 * Fetches data from database using recursive declarative description. See [docs.datomic.com/on-prem/pull.html](https://docs.datomic.com/on-prem/pull.html).
 * 
 * Unlike [[entity]], returns plain Clojure map (not lazy).
 * 
 * Usage:
 * 
 *     (pull db [:db/id, :name, :likes, {:friends [:db/id :name]}] 1) ; => {:db/id   1,
 *                                                                          :name    "Ivan"
 *                                                                          :likes   [:pizza]
 *                                                                          :friends [{:db/id 2, :name "Oleg"}]}
 * 
 * The arity-2 version takes :selector and :eid in arg-map.
 */
export function pull(db?: Database, opts?: { selector: any[], eid: number | string }): Promise<any>;

/**
 * Same as [[pull]], but accepts sequence of ids and returns sequence of maps.
 * 
 * Usage:
 * 
 *     (pull-many db [:db/id :name] [1 2]) ; => [{:db/id 1, :name "Ivan"}
 *                                               {:db/id 2, :name "Oleg"}]
 */
export function pullMany(db?: Database, opts?: { selector: any[], eid: number | string }): Promise<Array<object>>;

/**
 * Executes a datalog query. See [docs.datomic.com/on-prem/query.html](https://docs.datomic.com/on-prem/query.html).
 * 
 * Usage:
 * 
 * Query as parameter with additional args:
 * 
 *     (q '[:find ?value
 *          :where [_ :likes ?value]]
 *        #{[1 :likes "fries"]
 *          [2 :likes "candy"]
 *          [3 :likes "pie"]
 *          [4 :likes "pizza"]}) ; => #{["fries"] ["candy"] ["pie"] ["pizza"]}
 * 
 * Or query passed in arg-map:
 * 
 *     (q {:query '[:find ?value
 *                  :where [_ :likes ?value]]
 *         :offset 2
 *         :limit 1
 *         :args [#{[1 :likes "fries"]
 *                  [2 :likes "candy"]
 *                  [3 :likes "pie"]
 *                  [4 :likes "pizza"]}]}) ; => #{["fries"] ["candy"] ["pie"] ["pizza"]}
 * 
 * Or query passed as map of vectors:
 * 
 *      (q '{:find [?value] :where [[_ :likes ?value]]}
 *         #{[1 :likes "fries"]
 *           [2 :likes "candy"]
 *           [3 :likes "pie"]
 *           [4 :likes "pizza"]}) ; => #{["fries"] ["candy"] ["pie"] ["pizza"]}
 * 
 * Or query passed as string:
 * 
 *      (q {:query "[:find ?value :where [_ :likes ?value]]"
 *          :args [#{[1 :likes "fries"]
 *                   [2 :likes "candy"]
 *                   [3 :likes "pie"]
 *                   [4 :likes "pizza"]}]})
 * 
 * Query passed as map needs vectors as values. Query can not be passed as list. The 1-arity function takes a map with the arguments :query and :args and optionally the additional keys :offset and :limit.
 */
export function q(map?: { query: string | any[], args?: any[], limit?: number, offset?: number }): Promise<any>;

/**
 * Executes a datalog query and returns the result as well as some execution details.
 * Uses the same arguments as q does.
 */
export function queryStats(map?: { query: string | any[], args?: any[], limit?: number, offset?: number }): Promise<object>;

/** Releases a database connection. You need to release a connection as many times as you connected to it for it to be completely released. Set release-all? to true to force its release. */
export function release(conn?: Connection): Promise<null>;

/** Returns current reverse schema definition. */
export function reverseSchema(db?: Database): Promise<object>;

/** Returns current schema definition. */
export function schema(db?: Database): Promise<Schema>;

/**
 * Similar to [[datoms]], but will return datoms starting from specified components and including rest of the database until the end of the index.
 * 
 *                              If no datom matches passed arguments exactly, iterator will start from first datom that could be considered “greater” in index order.
 * 
 *                              Usage:
 * 
 *                                  (seek-datoms @db {:index :eavt
 *                                                    :components [1]}) ; => (#datahike/Datom [1 :friends 2]
 *                                                                      ;     #datahike/Datom [1 :likes "fries"]
 *                                                                      ;     #datahike/Datom [1 :likes "pizza"]
 *                                                                      ;     #datahike/Datom [1 :name "Ivan"]
 *                                                                      ;     #datahike/Datom [2 :likes "candy"]
 *                                                                      ;     #datahike/Datom [2 :likes "pie"]
 *                                                                      ;     #datahike/Datom [2 :likes "pizza"])
 * 
 *                                  (seek-datoms @db {:index :eavt
 *                                                    :components [1 :name]}) ; => (#datahike/Datom [1 :name "Ivan"]
 *                                                                            ;     #datahike/Datom [2 :likes "candy"]
 *                                                                            ;     #datahike/Datom [2 :likes "pie"]
 *                                                                            ;     #datahike/Datom [2 :likes "pizza"])
 * 
 *                                  (seek-datoms @db {:index :eavt
 *                                                    :components [2]}) ; => (#datahike/Datom [2 :likes "candy"]
 *                                                                      ;     #datahike/Datom [2 :likes "pie"]
 *                                                                      ;     #datahike/Datom [2 :likes "pizza"])
 * 
 *                              No datom `[2 :likes "fish"]`, so starts with one immediately following such in index
 * 
 *                                  (seek-datoms @db {:index :eavt
 *                                                    :components [2 :likes "fish"]}) ; => (#datahike/Datom [2 :likes "pie"]
 *                                                                                      ;     #datahike/Datom [2 :likes "pizza"])
 */
export function seekDatoms(db?: Database, args?: { index: string, components?: any[] }): Promise<any>;

/**
 * Returns the database state since a given point in time (you may use either java.util.Date or a transaction ID as long).
 * Be aware: the database contains only the datoms that were added since the date.
 * 
 * 
 * (transact conn {:tx-data [{:db/ident :name
 *                            :db/valueType :db.type/string
 *                            :db/unique :db.unique/identity
 *                            :db/index true
 *                            :db/cardinality :db.cardinality/one}
 *                           {:db/ident :age
 *                            :db/valueType :db.type/long
 *                            :db/cardinality :db.cardinality/one}]})
 * 
 * (transact conn {:tx-data [{:name "Alice" :age 25} {:name "Bob" :age 30}]})
 * 
 * (def date (java.util.Date.))
 * 
 * (transact conn [{:db/id [:name "Alice"] :age 30}])
 * 
 * (q '[:find ?n ?a
 *      :in $ $since
 *      :where
 *      [$ ?e :name ?n]
 *      [$since ?e :age ?a]]
 *    @conn
 *    (since @conn date)) ; => #{["Alice" 30]}
 * 
 * (q {:query '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]]
 *     :args [@conn]}) ; => #{["Alice" 30] ["Bob" 30]}
 */
export function since(db?: Database, timePoint?: any): Promise<Database>;

/**
 * Allocates and returns a unique temporary id (a negative integer). Ignores `part`. Returns `x` if it is specified.
 * 
 * Exists for Datomic API compatibility. Prefer using negative integers directly if possible.
 */
export function tempid(): Promise<any>;

/** Same as transact, but asynchronously returns a future. */
export function transact(conn?: Connection, txs?: Transaction[]): Promise<any>;

/** Removes registered listener from connection. See also [[listen]]. */
export function unlisten(conn?: Connection, key?: any): Promise<object>;

/**
 * Same as [[transact]]`, but applies to an immutable database value. Returns transaction report (see [[transact]]).
 * 
 *              Accepts tx-data and tx-meta as a map.
 * 
 *                  (with @conn {:tx-data [[:db/add 1 :name "Ivan"]]}) ; => {:db-before #datahike/DB {:max-tx 536870912 :max-eid 0},
 *                                                                       ;     :db-after #datahike/DB {:max-tx 536870913 :max-eid 1},
 *                                                                       ;     :tx-data [#datahike/Datom [1 :name "Ivan" 536870913]],
 *                                                                       ;     :tempids #:db{:current-tx 536870913},
 *                                                                       ;     :tx-meta nil}
 * 
 *                  (with @conn {:tx-data [[:db/add 1 :name "Ivan"]]
 *                               :tx-meta {:foo :bar}}) ; => {:db-before #datahike/DB {:max-tx 536870912 :max-eid 0},
 *                                                      ;     :db-after #datahike/DB {:max-tx 536870913 :max-eid 1},
 *                                                      ;     :tx-data [#datahike/Datom [1 :name "Ivan" 536870913]],
 *                                                      ;     :tempids #:db{:current-tx 536870913},
 *                                                      ;     :tx-meta {:foo :bar}}
 */
export function withDb(db?: Database, argmap?: { 'tx-data': Transaction[], 'tx-meta'?: any }): Promise<TransactionReport>;
