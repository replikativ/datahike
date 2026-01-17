(ns datahike.api.specification
  "Shared specification for different bindings.

  This namespace holds all semantic information such that individual bindings
  (Clojure API, Java API, JavaScript/TypeScript, HTTP routes, CLI) can be
  automatically derived from it.

  Following the Proximum pattern - the spec is purely declarative about semantics,
  not about how each binding should look. Names, routes, and method signatures
  are derived via conventions in the codegen modules.

  Each operation has:
    :args                     - malli function schema [:=> [:cat ...] ret] or [:function [...]]
    :ret                      - malli schema for return value
    :doc                      - documentation string
    :impl                     - symbol pointing to implementation function
    :categories               - semantic grouping tags (vector of keywords)
    :stability                - API maturity (:alpha, :beta, :stable)
    :supports-remote?         - true if can be exposed via HTTP/remote API
    :referentially-transparent? - true if pure (no side effects, deterministic)
    :examples                 - structured usage examples (optional)
    :params                   - detailed parameter documentation (optional)"
  (:require [malli.core :as m]
            [datahike.api.types :as types]))

;; =============================================================================
;; Name Derivation Helpers
;; =============================================================================

(defn ->url
  "Turns an API endpoint name into a URL path segment.
   Removes ? and ! suffixes, uses kebab-case as-is."
  [op-name]
  (-> (str op-name)
      (clojure.string/replace #"[?!]$" "")))

(defn ->cli-command
  "Derives CLI command from operation name.
   Examples:
     database-exists? → db-exists
     create-database → db-create
     transact → transact
     q → query"
  [op-name]
  (-> (str op-name)
      (clojure.string/replace #"^database-" "db-")
      (clojure.string/replace #"[?!]$" "")))

(defn malli-schema->argslist
  "Extract argument list from malli function schema for defn metadata.
   Handles [:=> [:cat ...] ret] and [:function [...]] schemas."
  [schema]
  (let [form (if (m/schema? schema) (m/form schema) schema)]
    (cond
      ;; [:function [:=> [:cat ...] ret] ...] - multi-arity
      (and (vector? form) (= :function (first form)))
      (for [arity-schema (rest form)]
        (when (and (vector? arity-schema) (= :=> (first arity-schema)))
          (let [[_ input-schema _] arity-schema]
            (if (and (vector? input-schema) (= :cat (first input-schema)))
              (vec (map-indexed (fn [i _] (symbol (str "arg" i)))
                                (rest input-schema)))
              []))))

      ;; [:=> [:cat ...] ret] - single arity
      (and (vector? form) (= :=> (first form)))
      (let [[_ input-schema _] form]
        (if (and (vector? input-schema) (= :cat (first input-schema)))
          (list (vec (map-indexed (fn [i _] (symbol (str "arg" i)))
                                  (rest input-schema))))
          '([])))

      :else
      '([& args]))))

;; =============================================================================
;; API Specification
;; =============================================================================

(def api-specification
  "Complete API specification for Datahike.

   Operation names become:
   - Clojure function names (as-is)
   - Java method names (kebab→camelCase, remove !?, via codegen)
   - JavaScript function names (same as Java)
   - HTTP routes (kebab-case path segments)
   - CLI commands (via ->cli-command)"

  '{;; =========================================================================
    ;; Database Lifecycle
    ;; =========================================================================

    database-exists?
    {:args [:function
            [:=> [:cat :datahike/SConfig] :boolean]
            [:=> [:cat] :boolean]]
     :ret :boolean
     :categories [:database :lifecycle :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Checks if a database exists via configuration map."
     :examples [{:desc "Check if in-memory database exists"
                 :code "(database-exists? {:store {:backend :memory :id \"example\"}})"}
                {:desc "Check with default config"
                 :code "(database-exists?)"}]
     :impl datahike.api.impl/database-exists?}

    create-database
    {:args [:function
            [:=> [:cat :datahike/SConfig] :datahike/SConfig]
            [:=> [:cat] :datahike/SConfig]]
     :ret :datahike/SConfig
     :categories [:database :lifecycle :write]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Creates a database via configuration map."
     :examples [{:desc "Create empty database"
                 :code "(create-database {:store {:backend :memory :id \"example\"}})"}
                {:desc "Create with schema-flexibility :read"
                 :code "(create-database {:store {:backend :memory :id \"example\"} :schema-flexibility :read})"}
                {:desc "Create without history"
                 :code "(create-database {:store {:backend :memory :id \"example\"} :keep-history? false})"}
                {:desc "Create with initial schema"
                 :code "(create-database {:store {:backend :memory :id \"example\"}
                                          :initial-tx [{:db/ident :name
                                                        :db/valueType :db.type/string
                                                        :db/cardinality :db.cardinality/one}]})"}]
     :impl datahike.api.impl/create-database}

    delete-database
    {:args [:function
            [:=> [:cat :datahike/SConfig] :any]
            [:=> [:cat] :any]]
     :ret :any
     :categories [:database :lifecycle :write]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Deletes a database given via configuration map."
     :examples [{:desc "Delete database"
                 :code "(delete-database {:store {:backend :memory :id \"example\"}})"}]
     :impl datahike.api.impl/delete-database}

    ;; =========================================================================
    ;; Connection Lifecycle
    ;; =========================================================================

    connect
    {:args [:function
            [:=> [:cat :datahike/SConfig] :datahike/SConnection]
            [:=> [:cat :datahike/SConfig :map] :datahike/SConnection]
            [:=> [:cat] :datahike/SConnection]]
     :ret :datahike/SConnection
     :categories [:connection :lifecycle]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Connects to a Datahike database via configuration map."
     :examples [{:desc "Connect to default in-memory database"
                 :code "(connect)"}
                {:desc "Connect to file-based database"
                 :code "(connect {:store {:backend :file :path \"/tmp/example\"}})"}
                {:desc "Connect with options"
                 :code "(connect {:store {:backend :memory :id \"example\"}} {:validate? true})"}]
     :impl datahike.connector/connect}

    db
    {:args [:=> [:cat :datahike/SConnection] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:connection :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Returns the underlying immutable database value from a connection. Prefer using @conn directly."
     :examples [{:desc "Get database from connection"
                 :code "(db conn)"}
                {:desc "Prefer direct deref"
                 :code "@conn"}]
     :impl datahike.api.impl/db}

    release
    {:args [:=> [:cat :datahike/SConnection] :nil]
     :ret :nil
     :categories [:connection :lifecycle]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Releases a database connection."
     :examples [{:desc "Release connection"
                 :code "(release conn)"}]
     :impl datahike.connector/release}

    ;; =========================================================================
    ;; Transaction Operations
    ;; =========================================================================

    transact
    {:args [:=> [:cat :datahike/SConnection :datahike/STransactions] :datahike/STransactionReport]
     :ret :datahike/STransactionReport
     :categories [:transaction :write]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Applies transaction to the database and updates connection."
     :examples [{:desc "Add single datom"
                 :code "(transact conn [[:db/add 1 :name \"Ivan\"]])"}
                {:desc "Retract datom"
                 :code "(transact conn [[:db/retract 1 :name \"Ivan\"]])"}
                {:desc "Create entity with tempid"
                 :code "(transact conn [[:db/add -1 :name \"Ivan\"]])"}
                {:desc "Create entity (map form)"
                 :code "(transact conn [{:db/id -1 :name \"Ivan\" :likes [\"fries\" \"pizza\"]}])"}
                {:desc "Read from stdin (CLI)"
                 :cli "cat data.edn | dthk transact conn:config.edn -"}]
     :impl datahike.api.impl/transact}

    transact!
    {:args [:=> [:cat :datahike/SConnection :datahike/STransactions] :any]
     :ret :any
     :categories [:transaction :write :async]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? false
     :doc "Same as transact, but asynchronously returns a future."
     :examples [{:desc "Async transaction"
                 :code "@(transact! conn [{:db/id -1 :name \"Alice\"}])"}]
     :impl datahike.api.impl/transact!}

    load-entities
    {:args [:=> [:cat :datahike/SConnection :datahike/STransactions] :any]
     :ret :any
     :categories [:transaction :write :bulk]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Load entities directly (bulk load)."
     :examples [{:desc "Bulk load entities"
                 :code "(load-entities conn entities)"}]
     :impl datahike.writer/load-entities}

    with
    {:args [:function
            [:=> [:cat :datahike/SDB :datahike/SWithArgs] :datahike/STransactionReport]
            [:=> [:cat :datahike/SDB :datahike/STransactions] :datahike/STransactionReport]
            [:=> [:cat :datahike/SDB :datahike/STransactions :datahike/STxMeta] :datahike/STransactionReport]]
     :ret :datahike/STransactionReport
     :categories [:transaction :immutable]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? true
     :doc "Applies transaction to immutable db value. Returns transaction report."
     :examples [{:desc "Transaction on db value"
                 :code "(with @conn [[:db/add 1 :name \"Ivan\"]])"}
                {:desc "With metadata"
                 :code "(with @conn {:tx-data [...] :tx-meta {:source :import}})"}]
     :impl datahike.api.impl/with}

    db-with
    {:args [:=> [:cat :datahike/SDB :datahike/STransactions] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:transaction :immutable]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? true
     :doc "Applies transaction to immutable db value, returns new db. Same as (:db-after (with db tx-data))."
     :examples [{:desc "Get db after transaction"
                 :code "(db-with @conn [[:db/add 1 :name \"Ivan\"]])"}]
     :impl datahike.api.impl/db-with}

    ;; =========================================================================
    ;; Query Operations
    ;; =========================================================================

    q
    {:args [:function
            [:=> [:cat :datahike/SQueryArgs] :any]
            [:=> [:cat [:or [:vector :any] :map :string] [:* :any]] :any]]
     :ret :any
     :categories [:query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Executes a datalog query."
     :examples [{:desc "Query with vector syntax"
                 :code "(q '[:find ?value :where [_ :likes ?value]] db)"}
                {:desc "Query with map syntax"
                 :code "(q '{:find [?value] :where [[_ :likes ?value]]} db)"}
                {:desc "Query with pagination"
                 :code "(q {:query '[:find ?value :where [_ :likes ?value]]
                           :args [db]
                           :offset 2
                           :limit 10})"}]
     :impl datahike.query/q}

    query-stats
    {:args [:function
            [:=> [:cat :datahike/SQueryArgs] :map]
            [:=> [:cat [:or [:vector :any] :map] [:* :any]] :map]]
     :ret :map
     :categories [:query :diagnostics]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Executes query and returns execution statistics."
     :examples [{:desc "Query with stats"
                 :code "(query-stats '[:find ?e :where [?e :name]] db)"}]
     :impl datahike.query/query-stats}

    pull
    {:args [:function
            [:=> [:cat :datahike/SDB :datahike/SPullOptions] [:maybe :map]]
            [:=> [:cat :datahike/SDB [:vector :any] :datahike/SEId] [:maybe :map]]]
     :ret [:maybe :map]
     :categories [:query :pull]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Fetches data using recursive declarative pull pattern."
     :examples [{:desc "Pull with pattern"
                 :code "(pull db [:db/id :name :likes {:friends [:db/id :name]}] 1)"}
                {:desc "Pull with arg-map"
                 :code "(pull db {:selector [:db/id :name] :eid 1})"}]
     :impl datahike.pull-api/pull}

    pull-many
    {:args [:function
            [:=> [:cat :datahike/SDB :datahike/SPullOptions] [:sequential :map]]
            [:=> [:cat :datahike/SDB [:vector :any] :datahike/SEId] [:sequential :map]]]
     :ret [:sequential :map]
     :categories [:query :pull]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Same as pull, but accepts sequence of ids and returns sequence of maps."
     :examples [{:desc "Pull multiple entities"
                 :code "(pull-many db [:db/id :name] [1 2 3])"}]
     :impl datahike.pull-api/pull-many}

    entity
    {:args [:=> [:cat :datahike/SDB [:or :datahike/SEId :any]] :any]
     :ret :any
     :categories [:query :entity]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Retrieves an entity by its id. Returns lazy map-like structure."
     :examples [{:desc "Get entity by id"
                 :code "(entity db 1)"}
                {:desc "Get entity by lookup ref"
                 :code "(entity db [:email \"alice@example.com\"])"}
                {:desc "Navigate entity attributes"
                 :code "(:name (entity db 1))"}]
     :impl datahike.impl.entity/entity}

    entity-db
    {:args [:=> [:cat :any] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:query :entity]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns database that entity was created from."
     :examples [{:desc "Get entity's database"
                 :code "(entity-db (entity db 1))"}]
     :impl datahike.impl.entity/entity-db}

    ;; =========================================================================
    ;; Index Operations
    ;; =========================================================================

    datoms
    {:args [:function
            [:=> [:cat :datahike/SDB :datahike/SIndexLookupArgs] [:maybe :datahike/SDatoms]]
            [:=> [:cat :datahike/SDB :keyword [:* :any]] [:maybe :datahike/SDatoms]]]
     :ret [:maybe :datahike/SDatoms]
     :categories [:query :index :advanced]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Index lookup. Returns sequence of datoms matching index components."
     :examples [{:desc "Find all datoms for entity"
                 :code "(datoms db {:index :eavt :components [1]})"}
                {:desc "Find datoms for entity and attribute"
                 :code "(datoms db {:index :eavt :components [1 :likes]})"}
                {:desc "Find by attribute and value (requires :db/index)"
                 :code "(datoms db {:index :avet :components [:likes \"pizza\"]})"}]
     :impl datahike.api.impl/datoms}

    seek-datoms
    {:args [:function
            [:=> [:cat :datahike/SDB :datahike/SIndexLookupArgs] [:maybe :datahike/SDatoms]]
            [:=> [:cat :datahike/SDB :keyword [:* :any]] [:maybe :datahike/SDatoms]]]
     :ret [:maybe :datahike/SDatoms]
     :categories [:query :index :advanced]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Like datoms, but returns datoms starting from specified components through end of index."
     :examples [{:desc "Seek from entity"
                 :code "(seek-datoms db {:index :eavt :components [1]})"}]
     :impl datahike.api.impl/seek-datoms}

    index-range
    {:args [:=> [:cat :datahike/SDB :datahike/SIndexRangeArgs] :datahike/SDatoms]
     :ret :datahike/SDatoms
     :categories [:query :index :advanced]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns part of :avet index between start and end values."
     :examples [{:desc "Find datoms in value range"
                 :code "(index-range db {:attrid :likes :start \"a\" :end \"z\"})"}
                {:desc "Find entities with age in range"
                 :code "(->> (index-range db {:attrid :age :start 18 :end 60}) (map :e))"}]
     :impl datahike.api.impl/index-range}

    ;; =========================================================================
    ;; Database Filtering
    ;; =========================================================================

    filter
    {:args [:=> [:cat :datahike/SDB :any] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:query :filter]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? true
     :doc "Returns filtered view over database. Only includes datoms where (pred db datom) is true."
     :examples [{:desc "Filter to recent datoms"
                 :code "(filter db (fn [db datom] (> (:tx datom) recent-tx)))"}]
     :impl datahike.core/filter}

    is-filtered
    {:args [:=> [:cat :datahike/SDB] :boolean]
     :ret :boolean
     :categories [:query :filter]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? true
     :doc "Returns true if database was filtered using filter, false otherwise."
     :examples [{:desc "Check if filtered"
                 :code "(is-filtered db)"}]
     :impl datahike.core/is-filtered}

    ;; =========================================================================
    ;; Temporal Queries
    ;; =========================================================================

    history
    {:args [:=> [:cat :datahike/SDB] :any]
     :ret :any
     :categories [:temporal :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns full historical state of database including all assertions and retractions."
     :examples [{:desc "Query historical data"
                 :code "(q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] (history @conn))"}]
     :impl datahike.api.impl/history}

    since
    {:args [:=> [:cat :datahike/SDB types/time-point?] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:temporal :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns database state since given time point (Date or transaction ID). Contains only datoms added since that point."
     :examples [{:desc "Query since date"
                 :code "(since @conn (java.util.Date.))"}
                {:desc "Query since transaction"
                 :code "(since @conn 536870913)"}]
     :impl datahike.api.impl/since}

    as-of
    {:args [:=> [:cat :datahike/SDB types/time-point?] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:temporal :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns database state at given time point (Date or transaction ID)."
     :examples [{:desc "Query as of date"
                 :code "(q '[:find ?n :where [_ :name ?n]] (as-of @conn date))"}
                {:desc "Query as of transaction"
                 :code "(as-of @conn 536870913)"}]
     :impl datahike.api.impl/as-of}

    ;; =========================================================================
    ;; Reactive Operations
    ;; =========================================================================

    listen
    {:args [:function
            [:=> [:cat :datahike/SConnection :any] :any]
            [:=> [:cat :datahike/SConnection :any :any] :any]]
     :ret :any
     :categories [:connection :reactive]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? false
     :doc "Listen for changes on connection. Callback called with transaction report on each transact."
     :examples [{:desc "Listen with callback"
                 :code "(listen conn (fn [tx-report] (println \"Transaction:\" (:tx-data tx-report))))"}
                {:desc "Listen with key"
                 :code "(listen conn :my-listener (fn [tx-report] ...))"}]
     :impl datahike.core/listen!}

    unlisten
    {:args [:=> [:cat :datahike/SConnection :any] :map]
     :ret :map
     :categories [:connection :reactive]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? false
     :doc "Removes registered listener from connection."
     :examples [{:desc "Remove listener"
                 :code "(unlisten conn :my-listener)"}]
     :impl datahike.core/unlisten!}

    ;; =========================================================================
    ;; Schema Operations
    ;; =========================================================================

    schema
    {:args [:=> [:cat :datahike/SDB] :datahike/SSchema]
     :ret :datahike/SSchema
     :categories [:schema :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns current schema definition."
     :examples [{:desc "Get schema"
                 :code "(schema @conn)"}]
     :impl datahike.api.impl/schema}

    reverse-schema
    {:args [:=> [:cat :datahike/SDB] :map]
     :ret :map
     :categories [:schema :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns reverse schema definition (attribute id to ident mapping)."
     :examples [{:desc "Get reverse schema"
                 :code "(reverse-schema @conn)"}]
     :impl datahike.api.impl/reverse-schema}

    ;; =========================================================================
    ;; Diagnostics & Maintenance
    ;; =========================================================================

    metrics
    {:args [:=> [:cat :datahike/SDB] :datahike/SMetrics]
     :ret :datahike/SMetrics
     :categories [:diagnostics :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns database metrics (datom counts, index sizes, etc)."
     :examples [{:desc "Get metrics"
                 :code "(metrics @conn)"}]
     :impl datahike.db/metrics}

    gc-storage
    {:args [:function
            [:=> [:cat :datahike/SConnection types/time-point?] :any]
            [:=> [:cat :datahike/SConnection] :any]]
     :ret :any
     :categories [:maintenance :lifecycle]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Invokes garbage collection on connection's store. Removes old snapshots before given time point."
     :examples [{:desc "GC all old snapshots"
                 :code "(gc-storage conn)"}
                {:desc "GC snapshots before date"
                 :code "(gc-storage conn (java.util.Date.))"}]
     :impl datahike.writer/gc-storage!}

    ;; =========================================================================
    ;; Utility Operations
    ;; =========================================================================

    tempid
    {:args [:function
            [:=> [:cat :any] neg-int?]
            [:=> [:cat :any :int] :int]]
     :ret [:or neg-int? :int]
     :categories [:utility]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? true
     :doc "Allocates temporary id (negative integer). Prefer using negative integers directly."
     :examples [{:desc "Generate tempid"
                 :code "(tempid :db.part/user)"}
                {:desc "Prefer direct negative integers"
                 :code "(transact conn [{:db/id -1 :name \"Alice\"}])"}]
     :impl datahike.core/tempid}})

;; =============================================================================
;; Helper Functions
;; =============================================================================

(defn pure-operations
  "Returns operations that are referentially transparent (pure functions)."
  []
  (filter (fn [[_ spec]] (:referentially-transparent? spec)) api-specification))

(defn io-operations
  "Returns operations with side effects (I/O operations)."
  []
  (remove (fn [[_ spec]] (:referentially-transparent? spec)) api-specification))

(defn remote-operations
  "Returns operations that support remote access (HTTP)."
  []
  (filter (fn [[_ spec]] (:supports-remote? spec)) api-specification))

(defn local-only-operations
  "Returns operations that must run locally."
  []
  (remove (fn [[_ spec]] (:supports-remote? spec)) api-specification))

(defn operations-by-category
  "Returns operations grouped by category."
  [category]
  (filter (fn [[_ spec]]
            (some #(= % category) (:categories spec)))
          api-specification))
