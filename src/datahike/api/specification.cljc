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
    ;; TWO arities: `(release conn)` and `(release conn release-all?)`. The
    ;; second was undeclared, so instrumentation rejected a call the
    ;; implementation has always accepted (`connector/release`).
    {:args [:function
            [:=> [:cat :datahike/SConnection] :nil]
            [:=> [:cat :datahike/SConnection :any] :nil]]
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
    ;; Either a transaction vector or an arg-map `{:tx-data … :tx-meta …}`; the
    ;; arg-map is the form in README.md. Declaring only the vector made a
    ;; CORRECT call fail under `malli.instrument/instrument!`. See `with` above
    ;; for why this is one `[:or …]` parameter rather than two branches, and
    ;; `codegen/java`'s `expand-or-args` for what it emits.
    {:args [:=> [:cat :datahike/SConnection [:or :datahike/STransactions :datahike/SWithArgs]] :datahike/STransactionReport]
     :ret :datahike/STransactionReport
     :categories [:transaction :write]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Applies transaction to the database and updates connection. Blocks until committed. The map form accepts :tx-options {:allow-index-backfill? true} to permit index/uniqueness backfill for this transaction only; it does not change the database config. WARNING: Do not call from listener callbacks or transaction functions — use transact! instead to avoid deadlocks."
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
    {:args [:=> [:cat :datahike/SConnection [:or :datahike/STransactions :datahike/SWithArgs]] :any]
     :ret :any
     :categories [:transaction :write :async]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? false
     :doc "Same as transact, but asynchronously returns a future. Safe to call from listener callbacks and go blocks."
     :examples [{:desc "Async transaction"
                 :code "@(transact! conn [{:db/id -1 :name \"Alice\"}])"}]
     :impl datahike.api.impl/transact!}

    writer-barrier
    {:args [:=> [:cat :datahike/SConnection] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:transaction :read]
     :stability :alpha
     :supports-remote? true
     :referentially-transparent? false
     :doc "Wait for preceding accepted writes to settle and return the durable database. Creates no transaction. Do not call synchronously from a transaction function; use writer-barrier! outside the writer instead."
     :impl datahike.api.impl/writer-barrier}

    writer-barrier!
    {:args [:=> [:cat :datahike/SConnection] :any]
     :ret :any
     :categories [:transaction :read :async]
     :stability :alpha
     :supports-remote? false
     :referentially-transparent? false
     :doc "Asynchronously return the durable database after preceding accepted writes settle. Creates no transaction and fires no transaction listeners or predicates."
     :impl datahike.api.impl/writer-barrier!}

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
    ;; The 2-arity accepts EITHER a transaction vector or an arg-map
    ;; `{:tx-data … :tx-meta …}` — both documented, both in the examples below —
    ;; so it is ONE `[:or …]` parameter. Declaring them as two branches instead
    ;; is what malli rejects (`:malli.core/duplicate-arities`), and it is why
    ;; this operation spent a while excluded from registration entirely.
    ;;
    ;; The Java binding keeps its original overloads (plus the 4-arity):
    ;; `codegen/java`'s `expand-or-args` turns an `[:or …]` argument into one
    ;; overload per distinct Java type, so `STransactions`/`SWithArgs` still
    ;; emit `with(Object, List)` — marshalling through
    ;; `Util.normalizeCollections` — beside `with(Object, Object)`.
    {:args [:function
            [:=> [:cat :datahike/SDB [:or :datahike/STransactions :datahike/SWithArgs]] :datahike/STransactionReport]
            [:=> [:cat :datahike/SDB :datahike/STransactions :datahike/STxMeta] :datahike/STransactionReport]
            [:=> [:cat :datahike/SDB :datahike/STransactions :datahike/STxMeta :datahike/STxOptions] :datahike/STransactionReport]]
     :ret :datahike/STransactionReport
     :categories [:transaction :immutable]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? true
     :doc "Applies transaction to immutable db value. Returns transaction report. Accepts :tx-options in the map form, or as a fourth argument after tx-meta. The only option is :allow-index-backfill? (boolean): true permits index/uniqueness backfill for this transaction without changing the database config."
     :examples [{:desc "Transaction on db value"
                 :code "(with @conn [[:db/add 1 :name \"Ivan\"]])"}
                {:desc "With metadata"
                 :code "(with @conn {:tx-data [...] :tx-meta {:source :import}})"}]
     :impl datahike.api.impl/with}

    db-with
    {:args [:=> [:cat :datahike/SDB [:or :datahike/STransactions :datahike/SWithArgs]] :datahike/SDB]
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

    explain
    {:args [:function
            [:=> [:cat [:or [:vector :any] :map] [:* :any]] :string]]
     :ret :string
     :categories [:query :diagnostics]
     :stability :experimental
     :supports-remote? false
     :referentially-transparent? true
     :doc "Returns a human-readable string explaining the query plan. Shows index selection, scan/merge ordering, recursive rule structure (SCC, base cases, clause versions), and estimated cardinalities. Takes the same arguments as `q`."
     :examples [{:desc "Explain a simple query"
                 :code "(explain '[:find ?e :where [?e :name]] db)"}
                {:desc "Explain a recursive rule"
                 :code "(explain '[:find ?e2 :in $ ?e1 % :where (follow ?e1 ?e2)] db 1 '[[(follow ?e1 ?e2) [?e1 :follow ?e2]] [(follow ?e1 ?e2) [?e1 :follow ?t] (follow ?t ?e2)]])"}]
     :impl #?(:clj datahike.query/explain :cljs nil)}

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
            [:=> [:cat :datahike/SDB [:vector :any] [:sequential :datahike/SEId]] [:sequential :map]]]
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
    ;; Two branches with NON-OVERLAPPING arities. Both forms the impl accepts —
    ;; (f db :eavt component…) and (f db {:index .. :components ..}) — used to be
    ;; declared as two branches that BOTH admitted two arguments; malli's
    ;; `:function` dispatches on arity, so it took the arg-map one and reported
    ;; the canonical `(f db :eavt)` as `:malli.core/invalid-input`.
    ;;
    ;; So the 2-arity branch now accepts EITHER shape via `:or`, and the
    ;; component branch starts at THREE (`:+`, one-or-more components). The
    ;; index keyword is the same enum `SIndexLookupArgs` declares, so a bad
    ;; index is still caught.
    ;;
    ;; The two branches are also load-bearing for CODEGEN, which is why this is
    ;; not collapsed into one variadic `[:* :any]`: `codegen/java` maps each
    ;; `[:cat]` element to one positional Java parameter and has no varargs
    ;; notion, so a single branch emits a single overload and
    ;; `Datahike.datoms(db, kwd(":eavt"))` stops compiling. Two branches, two
    ;; overloads — the arity counts here are the generated Java signatures.
    {:args [:function
            [:=> [:cat :datahike/SDB
                  [:or [:enum :eavt :aevt :avet] :datahike/SIndexLookupArgs]]
             [:maybe :datahike/SDatoms]]
            [:=> [:cat :datahike/SDB [:enum :eavt :aevt :avet] [:+ :any]]
             [:maybe :datahike/SDatoms]]]
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
    ;; Two branches with NON-OVERLAPPING arities. Both forms the impl accepts —
    ;; (f db :eavt component…) and (f db {:index .. :components ..}) — used to be
    ;; declared as two branches that BOTH admitted two arguments; malli's
    ;; `:function` dispatches on arity, so it took the arg-map one and reported
    ;; the canonical `(f db :eavt)` as `:malli.core/invalid-input`.
    ;;
    ;; So the 2-arity branch now accepts EITHER shape via `:or`, and the
    ;; component branch starts at THREE (`:+`, one-or-more components). The
    ;; index keyword is the same enum `SIndexLookupArgs` declares, so a bad
    ;; index is still caught.
    ;;
    ;; The two branches are also load-bearing for CODEGEN, which is why this is
    ;; not collapsed into one variadic `[:* :any]`: `codegen/java` maps each
    ;; `[:cat]` element to one positional Java parameter and has no varargs
    ;; notion, so a single branch emits a single overload and
    ;; `Datahike.datoms(db, kwd(":eavt"))` stops compiling. Two branches, two
    ;; overloads — the arity counts here are the generated Java signatures.
    {:args [:function
            [:=> [:cat :datahike/SDB
                  [:or [:enum :eavt :aevt :avet] :datahike/SIndexLookupArgs]]
             [:maybe :datahike/SDatoms]]
            [:=> [:cat :datahike/SDB [:enum :eavt :aevt :avet] [:+ :any]]
             [:maybe :datahike/SDatoms]]]
     :ret [:maybe :datahike/SDatoms]
     :categories [:query :index :advanced]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Like datoms, but returns datoms starting from specified components through end of index."
     :examples [{:desc "Seek from entity"
                 :code "(seek-datoms db {:index :eavt :components [1]})"}]
     :impl datahike.api.impl/seek-datoms}

    rseek-datoms
    ;; Two branches with NON-OVERLAPPING arities. Both forms the impl accepts —
    ;; (f db :eavt component…) and (f db {:index .. :components ..}) — used to be
    ;; declared as two branches that BOTH admitted two arguments; malli's
    ;; `:function` dispatches on arity, so it took the arg-map one and reported
    ;; the canonical `(f db :eavt)` as `:malli.core/invalid-input`.
    ;;
    ;; So the 2-arity branch now accepts EITHER shape via `:or`, and the
    ;; component branch starts at THREE (`:+`, one-or-more components). The
    ;; index keyword is the same enum `SIndexLookupArgs` declares, so a bad
    ;; index is still caught.
    ;;
    ;; The two branches are also load-bearing for CODEGEN, which is why this is
    ;; not collapsed into one variadic `[:* :any]`: `codegen/java` maps each
    ;; `[:cat]` element to one positional Java parameter and has no varargs
    ;; notion, so a single branch emits a single overload and
    ;; `Datahike.datoms(db, kwd(":eavt"))` stops compiling. Two branches, two
    ;; overloads — the arity counts here are the generated Java signatures.
    {:args [:function
            [:=> [:cat :datahike/SDB
                  [:or [:enum :eavt :aevt :avet] :datahike/SIndexLookupArgs]]
             [:maybe :datahike/SDatoms]]
            [:=> [:cat :datahike/SDB [:enum :eavt :aevt :avet] [:+ :any]]
             [:maybe :datahike/SDatoms]]]
     :ret [:maybe :datahike/SDatoms]
     :categories [:query :index :advanced]
     :stability :experimental
     :supports-remote? true
     :referentially-transparent? true
     :doc "Like seek-datoms, but iterates BACKWARDS: datoms <= the given components, descending to the beginning of the index. Lazy on the persistent-sorted-set index — the primitive for windowed backwards pagination (latest-N, N-before-cursor)."
     :examples [{:desc "Latest room messages, newest first"
                 :code "(take 20 (rseek-datoms db {:index :avet :components [:message/room room-eid]}))"}]
     :impl datahike.api.impl/rseek-datoms}

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
    {:args [:=> [:cat :datahike/SDB] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:temporal :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns full historical state of database including all assertions and retractions."
     :examples [{:desc "Query historical data"
                 :code "(q '[:find ?n ?a :where [?e :name ?n] [?e :age ?a]] (history @conn))"}]
     :impl datahike.api.impl/history}

    since
    {:args [:=> [:cat :datahike/SDB :datahike/time-point?] :datahike/SDB]
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
    {:args [:=> [:cat :datahike/SDB :datahike/time-point?] :datahike/SDB]
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

    valid-at
    {:args [:=> [:cat :datahike/SDB :any] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:temporal :query]
     :stability :experimental
     :supports-remote? true
     :referentially-transparent? true
     :doc "Tag `db` with a `:datahike/valid-at` marker so vt-aware secondary
           indices push the filter through `-search-at-vt`. Valid-time is a
           secondary-index axis; regular datalog patterns still require the
           built-in `(valid-at ?tx ?at)` rule to filter by vt explicitly."
     :examples [{:desc "Query as of valid-time"
                 :code "(q '[:find ?n :where [_ :name ?n]] (valid-at @conn #inst \"2024-04-15\"))"}]
     :impl datahike.api.impl/valid-at}

    valid-between
    {:args [:=> [:cat :datahike/SDB :any :any] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:temporal :query]
     :stability :experimental
     :supports-remote? true
     :referentially-transparent? true
     :doc "Filter `db` to datoms whose asserting tx's vt-window overlaps the
           half-open interval `[from, to)`. SQL:2011 `FOR VALID_TIME BETWEEN
           from AND to` maps to this. Carries
           `:datahike/valid-between [from to]` on the result for vt-aware
           secondary-index pushdown."
     :examples [{:desc "Datoms whose tx vt-window overlaps Q2 2024"
                 :code "(q '[:find ?n :where [_ :name ?n]] (valid-between @conn #inst \"2024-04-01\" #inst \"2024-07-01\"))"}]
     :impl datahike.api.impl/valid-between}

    valid-during
    {:args [:=> [:cat :datahike/SDB :any :any] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:temporal :query]
     :stability :experimental
     :supports-remote? true
     :referentially-transparent? true
     :doc "Filter `db` to datoms whose tx's vt-window is *fully contained*
           in `[from, to)`. Stricter than `valid-between` — overlapping
           windows that extend past either endpoint are excluded. Useful
           for 'corrections wholly within Q2' style queries."
     :examples [{:desc "Corrections whose vt-window was wholly inside Q2"
                 :code "(q '[:find ?e :where [?e :name _]] (valid-during @conn #inst \"2024-04-01\" #inst \"2024-07-01\"))"}]
     :impl datahike.api.impl/valid-during}

    valid-all
    {:args [:=> [:cat :datahike/SDB] :datahike/SDB]
     :ret :datahike/SDB
     :categories [:temporal :query]
     :stability :experimental
     :supports-remote? true
     :referentially-transparent? true
     :doc "Strip any valid-time markers from `db` so the full vt-history is
           visible. Idempotent. Does not unwrap an existing FilteredDB; to
           drop an active filter, start from the unwrapped db."
     :examples [{:desc "Drop vt-marker for full-history query"
                 :code "(q '[:find ?n :where [_ :name ?n]] (valid-all @conn))"}]
     :impl datahike.api.impl/valid-all}

    ;; =========================================================================
    ;; Versioning Operations
    ;; =========================================================================

    branches
    {:args [:=> [:cat :datahike/SConnection] [:set :keyword]]
     :ret [:set :keyword]
     :categories [:versioning :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "List all known branch names. Returns set of keywords."
     :examples [{:desc "List branches"
                 :code "(branches conn)"}]
     :impl datahike.api.impl/branches}

    branch!
    {:args [:=> [:cat :datahike/SConnection :any :keyword] :any]
     :ret :any
     :categories [:versioning :write :async]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Create a new branch from an existing branch or commit. Secondary indices are CoW-branched automatically."
     :examples [{:desc "Branch from main"
                 :code "(branch! conn :db :experiment)"}
                {:desc "Branch from specific commit"
                 :code "(branch! conn #uuid \"...\" :hotfix)"}]
     :impl datahike.api.impl/branch!}

    delete-branch!
    {:args [:=> [:cat :datahike/SConnection :keyword] :any]
     :ret :any
     :categories [:versioning :write :async]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Remove a branch. The branch data remains accessible until the next GC."
     :examples [{:desc "Delete branch"
                 :code "(delete-branch! conn :experiment)"}]
     :impl datahike.api.impl/delete-branch!}

    force-branch!
    {:args [:=> [:cat :datahike/SDB :keyword [:set :any]] :nil]
     :ret :nil
     :categories [:versioning :write :advanced]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? false
     :doc "Force a branch to point to the provided db value. WARNING: This deliberately replaces the branch head, like git reset --hard. On revisioned stores the replacement is conditionally retried so it cannot clobber an update that lands between its read and write. Existing connections to this branch will see stale state and must be released and reconnected."
     :examples [{:desc "Force branch to current db"
                 :code "(force-branch! @conn :experiment #{:db})"}]
     :impl datahike.api.impl/force-branch!}

    merge-db
    {:args [:function
            [:=> [:cat :datahike/SConnection [:set :any] :datahike/STransactions] :datahike/STransactionReport]
            [:=> [:cat :datahike/SConnection [:set :any] :datahike/STransactions :any] :datahike/STransactionReport]]
     :ret :datahike/STransactionReport
     :categories [:versioning :write]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Create a merge commit combining the current branch with parent branches/commits. The caller provides the merged tx-data. Routed through the writer for serialization. Blocks until committed. WARNING: Do not call from listener callbacks — use merge-db! instead to avoid deadlocks."
     :examples [{:desc "Merge feature into main"
                 :code "(d/merge-db conn #{:feature} [{:name \"merged entity\"}])"}
                {:desc "Merge with metadata"
                 :code "(d/merge-db conn #{:feature} [{:name \"merged\"}] {:source :merge})"}]
     :impl datahike.api.impl/merge-db}

    merge-db!
    {:args [:function
            [:=> [:cat :datahike/SConnection [:set :any] :datahike/STransactions] :any]
            [:=> [:cat :datahike/SConnection [:set :any] :datahike/STransactions :any] :any]]
     :ret :any
     :categories [:versioning :write :async]
     :stability :stable
     :supports-remote? false
     :referentially-transparent? false
     :doc "Async version of merge-db. Returns a promise (CLJ) or channel (CLJS). Safe to call from listener callbacks and go blocks."
     :examples [{:desc "Async merge"
                 :code "@(d/merge-db! conn #{:feature} [{:name \"merged\"}])"}]
     :impl datahike.api.impl/merge-db!}

    commit-id
    {:args [:=> [:cat :datahike/SDB] [:maybe :uuid]]
     :ret [:maybe :uuid]
     :categories [:versioning :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Retrieve the commit-id for this database value."
     :examples [{:desc "Get commit id"
                 :code "(commit-id @conn)"}]
     :impl datahike.api.impl/commit-id}

    parent-commit-ids
    {:args [:=> [:cat :datahike/SDB] [:maybe [:set :any]]]
     :ret [:maybe [:set :any]]
     :categories [:versioning :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Retrieve parent commit ids from this database value."
     :examples [{:desc "Get parent commits"
                 :code "(parent-commit-ids @conn)"}]
     :impl datahike.api.impl/parent-commit-ids}

    commit-as-db
    {:args [:=> [:cat :any :uuid] [:maybe :datahike/SDB]]
     :ret [:maybe :datahike/SDB]
     :categories [:versioning :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Load the database at a specific commit. First argument can be a connection, db value, or raw store."
     :examples [{:desc "Load db at commit"
                 :code "(commit-as-db conn #uuid \"...\")"}]
     :impl datahike.api.impl/commit-as-db}

    branch-as-db
    {:args [:=> [:cat :any :keyword] [:maybe :datahike/SDB]]
     :ret [:maybe :datahike/SDB]
     :categories [:versioning :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Load the database at a branch head. First argument can be a connection, db value, or raw store."
     :examples [{:desc "Load db at branch"
                 :code "(branch-as-db conn :experiment)"}]
     :impl datahike.api.impl/branch-as-db}

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
     :doc "Listen for changes on connection. Callback called with transaction report on each transact. WARNING: Inside the callback, use only async operations (transact!, merge-db!) — synchronous writer operations will deadlock."
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
    {:args [:function
            [:=> [:cat :datahike/SDB] :datahike/SMetrics]
            [:=> [:cat :datahike/SDB [:map [:per-entity-counts? {:optional true} :boolean]]] :datahike/SMetrics]]
     :ret :datahike/SMetrics
     :categories [:diagnostics :query]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? true
     :doc "Returns database metrics: datom counts overall, per attribute and for the indexed (AVET) attributes, plus the same for history when kept. Computed from the indices' subtree counts — O(#attributes · log n) — so it is cheap on a large database. `{:per-entity-counts? true}` adds `:per-entity-counts`, a walk over every datom with one map entry per entity."
     :examples [{:desc "Get metrics"
                 :code "(metrics @conn)"}
                {:desc "With datoms per entity (a full walk)"
                 :code "(metrics @conn {:per-entity-counts? true})"}]
     :impl datahike.db/metrics}

    gc-storage
    {:args [:function
            [:=> [:cat :datahike/SConnection :datahike/time-point? [:map [:min-age-ms {:optional true} :int]]] :any]
            [:=> [:cat :datahike/SConnection :datahike/time-point?] :any]
            [:=> [:cat :datahike/SConnection] :any]]
     :ret :any
     :categories [:maintenance :lifecycle]
     :stability :stable
     :supports-remote? true
     :referentially-transparent? false
     :doc "Invokes garbage collection on connection's store. Removes old snapshots before given time point. `:min-age-ms` spares anything written more recently than that, which is what makes collecting from outside the writer process possible — it must exceed the longest values-then-pointer window any writer can have. When omitted it defaults to 0 under an exclusive local writer (`:writer {:backend :self :writer-ownership :exclusive}`) and to 15 minutes under a shared or remote writer, where another process's commit in flight is invisible to this collector. The default is a bound on one awaited request, not a guarantee: size an explicit value above your longest in-flight window plus the largest clock difference between your processes (the stamps it compares against are each writer's own). Pass `{:min-age-ms 0}` explicitly to sweep without a floor."
     :examples [{:desc "GC all old snapshots"
                 :code "(gc-storage conn)"}
                {:desc "GC snapshots before date"
                 :code "(gc-storage conn (java.util.Date.))"}
                {:desc "Collect from a cron job or offline process: spare anything written in the last 24h"
                 :code "(gc-storage conn (java.util.Date. 0) {:min-age-ms 86400000})"}]
     :impl datahike.writer/gc-storage!}

    ;; =========================================================================
    ;; Index Warming (EXPERIMENTAL)
    ;; =========================================================================
    ;;
    ;; A cold reader's wall time is `misses x RTT` with NOTHING overlapping: a
    ;; scan asks for a node, blocks on the GET, and only then learns the next
    ;; address. These three walk the index breadth-first instead, fetching each
    ;; level concurrently — a branch node holds every child address the moment
    ;; it is materialized, so no prediction is involved. See `datahike.warm`.
    ;;
    ;; Three and not more, on purpose. This is experimental surface: adding an
    ;; entry point later is cheap, removing one is breaking. In particular there
    ;; is exactly ONE way to scope a warm to a key range — `warm-datoms`, which
    ;; builds its bounds from datahike's own components->pattern — because a
    ;; hand-built bound in the wrong permutation warms a valid-but-different
    ;; subtree silently.
    ;;
    ;; `:supports-remote? false` deliberately: a warm moves nodes into the node
    ;; cache of the process that holds the index. Over HTTP that would be the
    ;; SERVER's cache, which is a different (and unrequested) operation from the
    ;; one the caller means.
    ;;
    ;; NO trailing `!` on the public names, with the banged fns as `:impl` —
    ;; the same split `gc-storage` -> `datahike.writer/gc-storage!` uses. In this
    ;; specification a trailing `!` means the ASYNC variant of a sibling
    ;; (`transact!`, `merge-db!`, `branch!`), not "has side effects"; every
    ;; synchronous op is unbanged however destructive it is. A warm is not an
    ;; async variant of anything, and it changes no database state at all: the
    ;; db value is immutable and results are identical warm or cold — only
    ;; latency and GET count move. `clj-name->java-method` also maps a trailing
    ;; `!` to an `Async` suffix, so a banged name would generate `warmIndexAsync`
    ;; for a synchronous JVM call.

    warm-index
    {:args [:function
            [:=> [:cat :datahike/SDB :datahike/SWarmIndex] :map]
            [:=> [:cat :datahike/SDB :datahike/SWarmIndex :map] :map]]
     :ret :map
     :categories [:maintenance :index :advanced]
     :stability :experimental
     :supports-remote? false
     :referentially-transparent? false
     :doc "EXPERIMENTAL. Budget-bounded breadth-first warm of ONE whole index into its node cache, fetching each level concurrently instead of discovering it one blocking round trip at a time. Options: :depth (:interior | :with-leaves | integer), :budget, :width, :sync?. Takes no key range on purpose: range scoping has exactly one entry point, warm-datoms, which builds its bounds from datahike's own components->pattern — a hand-built bound in the wrong per-index permutation warms a valid-but-different subtree with no error and no wrong answer, just a warm that misses."
     :examples [{:desc "Warm the interior (branch levels only) of eavt"
                 :code "(warm-index @conn :eavt {:depth :interior :budget 2000})"}
                {:desc "Warm everything, leaves included, with a small budget"
                 :code "(warm-index @conn :avet {:depth :with-leaves :budget 200})"}]
     :impl datahike.warm/warm-index!}

    warm-datoms
    {:args [:function
            [:=> [:cat :datahike/SDB :datahike/SWarmIndex [:vector :any]] :map]
            [:=> [:cat :datahike/SDB :datahike/SWarmIndex [:vector :any] :map] :map]]
     :ret :map
     :categories [:maintenance :index :advanced]
     :stability :experimental
     :supports-remote? false
     :referentially-transparent? false
     :doc "EXPERIMENTAL. Warm exactly the subtree a components-scoped scan will read. Mirrors both component-taking scans: without :unbounded? it corresponds to `datoms` (upper bound = the components pattern, cost proportional to the range), with {:unbounded? true} to `seek-datoms` (upper bound = the end of the index, so it is readahead and :budget is the only bound). `components` is the same [e a v tx] prefix those take, in the index's own component order (:avet -> [a v e tx]), and may be shorter or empty. Bounds are built with datahike's own components->pattern, which permutes components per index and resolves idents — which is why this is the only entry point that scopes a warm to a range."
     :examples [{:desc "Warm what (datoms db :eavt 300) will read"
                 :code "(warm-datoms @conn :eavt [300])"}
                {:desc "Warm an avet range, components in avet order"
                 :code "(warm-datoms @conn :avet [:item/id 300] {:depth :with-leaves})"}
                {:desc "Read ahead from a seek-datoms position"
                 :code "(warm-datoms @conn :eavt [300] {:unbounded? true :budget 64})"}]
     :impl datahike.warm/warm-datoms!}

    warm-db
    {:args [:function
            [:=> [:cat :datahike/SDB] :map]
            [:=> [:cat :datahike/SDB :map] :map]]
     :ret :map
     :categories [:maintenance :index :advanced]
     :stability :experimental
     :supports-remote? false
     :referentially-transparent? false
     :doc "EXPERIMENTAL. Warm every present index of a database, sharing ONE budget round-robin across them so eavt cannot eat it before avet gets any. The connect-time shape, and the one to reach for. Takes warm-index's options plus :indices, the index keys to consider — which covers warming a chosen few, or one. Budget is clamped to 0.8x :store-cache-size, which is entry-counted — warming past it fetches nodes only to evict them. :by-index in the returned report says where the budget went."
     :examples [{:desc "Warm the interior of every index at connect"
                 :code "(warm-db @conn)"}
                {:desc "Spend a fixed budget across all indices, leaves included"
                 :code "(warm-db @conn {:depth :with-leaves :budget 500})"}
                {:desc "Narrow it to the indices a workload actually reads"
                 :code "(warm-db @conn {:indices [:eavt :avet] :budget 500})"}]
     :impl datahike.warm/warm-db!}

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
