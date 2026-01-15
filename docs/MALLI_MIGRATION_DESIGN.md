# Datahike Malli Migration & Specification Enhancement

## Executive Summary

This document outlines the migration from `clojure.spec.alpha` to `malli` for Datahike's API specification system, building on the successful pattern implemented in Proximum. The migration will enable automatic code generation for all bindings (CLI, Java, JavaScript/TypeScript, HTTP), improve IDE integration via clj-kondo, and establish consistency with Proximum's specification approach.

## Goals

1. **Single source of truth** - Enhanced API specification drives all bindings
2. **Malli adoption** - Replace clojure.spec with malli for better tooling
3. **Enhanced metadata** - Add categorization, examples, stability markers
4. **Automatic code generation** - Generate CLI, Java, TypeScript, HTTP routes
5. **Consistency with Proximum** - Align specification patterns across projects
6. **Better IDE support** - clj-kondo integration with full type information
7. **Improved documentation** - Auto-generated docs from examples

## Current State Analysis

### What Works Well

```clojure
;; Current: src/datahike/api/specification.cljc
(def api-specification
  '{database-exists?
    {:args             (s/alt :config (s/cat :config spec/SConfig)
                              :nil (s/cat))
     :ret              boolean?
     :supports-remote? true
     :referentially-transparent? false
     :doc              "Checks if a database exists..."
     :impl             datahike.api.impl/database-exists?}})
```

**Strengths:**
- Purely semantic specification (no binding-specific details)
- Already drives JavaScript/TypeScript and HTTP generation
- Clear separation: spec → implementation
- Convention-based derivation (kebab-case → camelCase)

**Limitations:**
- `clojure.spec` is macro-based (harder to introspect)
- No categorization/grouping metadata
- Examples embedded in doc strings (not structured)
- Java bindings hand-written (70% coverage, manual maintenance)
- CLI not generated from spec
- No stability/versioning markers
- Complex spec forms (`s/alt`, `s/cat`) harder to transform

### Current Bindings

| Binding | Status | Generation Method |
|---------|--------|------------------|
| **Clojure API** | ✓ 100% | Direct from specification |
| **JavaScript** | ✓ 100% | Macro-based (compile-time) |
| **TypeScript** | ✓ 100% | Generated .d.ts from spec |
| **HTTP Server** | ✓ Remote ops | Route generation via `eval` |
| **Java** | ⚠ ~70% | Hand-written (`java/datahike/*.java`) |
| **CLI** | ⚠ Manual | Not generated from spec |

## Design Principles (From Proximum)

### 1. Specification is Semantic Only

**What goes IN the spec:**
- `:args` - malli schema for function arguments
- `:ret` - malli schema for return value
- `:doc` - summary documentation (1-2 sentences)
- `:examples` - structured usage examples (new!)
- `:categories` - semantic grouping tags (new!)
- `:impl` - symbol pointing to implementation
- `:referentially-transparent?` - pure function marker
- `:supports-remote?` - HTTP-exposable marker
- `:stability` - API maturity level (new!)
- `:accepts-stdin?` - CLI stdin support (new!)

**What stays OUT:**
- No `:java {:method "exists"}` - derived via convention
- No `:http {:path "/database/exists"}` - derived via convention
- No `:cli {:command "db-exists"}` - derived via convention
- No duplicate information that can drift

### 2. Names Derived via Conventions

```clojure
;; Clojure operation name (source)
'database-exists?

;; Derived names:
;; Java:       databaseExists()  (kebab→camelCase, remove ?)
;; JavaScript: databaseExists()  (same as Java)
;; HTTP:       /database-exists  (kebab-case, remove ?)
;; CLI:        db-exists         (abbreviate database→db, remove ?)
```

**Override maps** in codegen for special cases:
```clojure
;; In datahike.codegen.java
(def java-name-overrides
  {'q "query"
   'db "database"
   'pull-many "pullBatch"})

;; In datahike.codegen.cli
(def cli-name-overrides
  {'database-exists? "db-exists"
   'create-database "db-create"})
```

### 3. Malli Function Schemas

**Before (clojure.spec):**
```clojure
:args (s/alt :config (s/cat :config spec/SConfig)
             :nil (s/cat))
```

**After (malli):**
```clojure
:args [:=> [:alt
            [:cat SConfig]
            [:cat]]
        :boolean]

;; Or for single arity:
:args [:=> [:cat SConnection STransactions] STransactionReport]
```

**Advantages:**
- Schemas are data (not macros) → runtime introspection
- Better JSON Schema generation → OpenAPI support
- Cleaner transformation to TypeScript/Java types
- Superior error messages via `malli.error/humanize`

## Migration Plan

### Phase 1: Schema Definitions (Week 1-2)

**File:** `src/datahike/schema.cljc` (new)

Convert all `datahike.spec/S*` types to malli schemas:

```clojure
(ns datahike.schema
  "Malli schemas for Datahike data types."
  (:require [malli.core :as m]
            [malli.util :as mu]
            [malli.error :as me]))

;; =============================================================================
;; Core Types
;; =============================================================================

(def SConfig
  "Database configuration map."
  [:map {:closed false}
   [:store [:map
            [:backend [:enum :mem :file]]
            [:id :string]
            [:path {:optional true} :string]]]
   [:keep-history? {:optional true :default true} :boolean]
   [:schema-flexibility {:optional true :default :write} [:enum :read :write]]
   [:index {:optional true :default :datahike.index/hitchhiker-tree}
    [:enum :datahike.index/hitchhiker-tree :datahike.index/persistent-set]]
   [:name {:optional true} :string]
   [:initial-tx {:optional true} :any]  ;; STransactions
   [:attribute-refs? {:optional true :default false} :boolean]
   [:writer {:optional true} :map]])

(def SConnection
  "Database connection reference."
  [:fn {:error/message "must be a Datahike connection"}
   #?(:clj #(instance? clojure.lang.IDeref %)
      :cljs #(satisfies? IDeref %))])

(def SDB
  "Immutable database value."
  [:fn {:error/message "must be a Datahike database"}
   #?(:clj #(satisfies? datahike.db.interface/IDB %)
      :cljs #(satisfies? datahike.db.interface/IDB %))])

(def SEId
  "Entity identifier (number or lookup ref)."
  [:or :int
   [:tuple :keyword :any]])  ;; [:unique-attr value]

(def SDatom
  "Single datom (immutable fact)."
  [:fn {:error/message "must be a Datom"}
   #?(:clj #(instance? datahike.datom.Datom %)
      :cljs #(instance? datahike.datom.Datom %))])

(def STransactions
  "Transaction data - vector or sequence of transaction forms."
  [:or
   [:sequential :any]  ;; Detailed schema possible
   [:vector :any]])

(def STransactionReport
  "Result of a transaction."
  [:map
   [:db-before SDB]
   [:db-after SDB]
   [:tx-data [:sequential SDatom]]
   [:tempids [:map-of :any :int]]
   [:tx-meta {:optional true} :any]])

(def SSchema
  "Database schema definition."
  [:map-of :keyword [:map
                     [:db/valueType {:optional true} :keyword]
                     [:db/cardinality {:optional true} :keyword]
                     [:db/unique {:optional true} :keyword]
                     [:db/index {:optional true} :boolean]
                     [:db/isComponent {:optional true} :boolean]]])

(def SQueryArgs
  "Query argument map."
  [:map
   [:query [:or :string :vector :map]]
   [:args {:optional true} [:sequential :any]]
   [:limit {:optional true} pos-int?]
   [:offset {:optional true} nat-int?]])

(def SPullOptions
  "Pull pattern options."
  [:map
   [:selector :vector]
   [:eid SEId]])

(def SIndexLookupArgs
  "Index lookup arguments."
  [:map
   [:index [:enum :eavt :aevt :avet]]
   [:components {:optional true} [:sequential :any]]])

(def SIndexRangeArgs
  "Index range query arguments."
  [:map
   [:attrid :keyword]
   [:start :any]
   [:end :any]])

;; =============================================================================
;; Type Mappings for Code Generation
;; =============================================================================

(def malli->java-type
  "Mapping from malli schemas to Java types."
  {:SConfig "Map<String, Object>"
   :SConnection "Connection"
   :SDB "Database"
   :SEId "Object"  ;; Long or LookupRef
   :SDatom "Datom"
   :STransactions "List<Object>"
   :STransactionReport "TransactionReport"
   :SSchema "Map<String, Map<String, Object>>"
   :SQueryArgs "Map<String, Object>"
   ;; Primitives
   :boolean "boolean"
   :int "int"
   :long "long"
   :double "double"
   :string "String"
   :keyword "String"
   :any "Object"
   ;; Collections
   :vector "List"
   :map "Map"
   :set "Set"
   :sequential "List"})

(def malli->typescript-type
  "Mapping from malli schemas to TypeScript types."
  {:SConfig "DatabaseConfig"
   :SConnection "Connection"
   :SDB "Database"
   :SEId "number | [string, any]"
   :SDatom "Datom"
   :STransactions "Transaction[]"
   :STransactionReport "TransactionReport"
   ;; Primitives
   :boolean "boolean"
   :int "number"
   :string "string"
   :keyword "string"
   :any "any"
   ;; Collections
   :vector "Array<any>"
   :map "Record<string, any>"
   :set "Set<any>"})
```

**Testing:** Write validation tests for all schemas:
```clojure
(ns datahike.schema-test
  (:require [clojure.test :refer [deftest is]]
            [malli.core :as m]
            [datahike.schema :as schema]))

(deftest config-schema-test
  (is (m/validate schema/SConfig
                  {:store {:backend :mem :id "test"}}))
  (is (m/validate schema/SConfig
                  {:store {:backend :file :id "test" :path "/tmp"}
                   :keep-history? true
                   :schema-flexibility :read}))
  (is (not (m/validate schema/SConfig
                       {:invalid "config"}))))
```

### Phase 2: Enhanced API Specification (Week 2-3)

**File:** `src/datahike/api/specification.cljc` (enhanced)

Add new metadata fields and convert to malli function schemas:

```clojure
(ns datahike.api.specification
  "Enhanced API specification with malli schemas."
  (:require [datahike.schema :as schema]
            [malli.core :as m]))

(def api-specification
  '{;; =========================================================================
    ;; Database Lifecycle
    ;; =========================================================================

    database-exists?
    {:args [:=> [:alt
                 [:cat schema/SConfig]
                 [:cat]]
            :boolean]
     :ret :boolean
     :categories [:database :lifecycle :query]
     :stability :stable
     :accepts-stdin? false
     :supports-remote? true
     :referentially-transparent? false
     :doc "Checks if a database exists via configuration map."
     :examples [{:desc "Check if in-memory database exists"
                 :code "(database-exists? {:store {:backend :mem :id \"example\"}})"
                 :result true}]
     :impl datahike.api.impl/database-exists?}

    create-database
    {:args [:=> [:alt
                 [:cat schema/SConfig]
                 [:cat]]
            schema/SConfig]
     :ret schema/SConfig
     :categories [:database :lifecycle :write]
     :stability :stable
     :accepts-stdin? false
     :supports-remote? true
     :referentially-transparent? false
     :doc "Creates a database via configuration map."
     :params {:config {:desc "Database configuration"
                       :schema schema/SConfig
                       :keys {:store "Backend config (:mem, :file)"
                              :initial-tx "First transaction (schema/data)"
                              :keep-history? "Enable temporal (default: true)"
                              :schema-flexibility "Validation (:read/:write, default: :write)"
                              :index "Index type (default: :hitchhiker-tree)"
                              :name "Optional database name"}
                       :defaults {:store {:backend :mem :id "default"}
                                  :keep-history? true
                                  :schema-flexibility :write}}}
     :examples [{:desc "Create empty database"
                 :code "(create-database {:store {:backend :mem :id \"example\"} :name \"my-db\"})"}
                {:desc "Create with schema-flexibility :read"
                 :code "(create-database {:store {:backend :mem :id \"example\"} :schema-flexibility :read})"}
                {:desc "Create without history"
                 :code "(create-database {:store {:backend :mem :id \"example\"} :keep-history? false})"}
                {:desc "Create with initial schema"
                 :code "(create-database {:store {:backend :mem :id \"example\"}
                                          :initial-tx [{:db/ident :name
                                                        :db/valueType :db.type/string
                                                        :db/cardinality :db.cardinality/one}]})"}]
     :impl datahike.api.impl/create-database}

    ;; =========================================================================
    ;; Transaction Operations
    ;; =========================================================================

    transact
    {:args [:=> [:cat schema/SConnection schema/STransactions]
            schema/STransactionReport]
     :ret schema/STransactionReport
     :categories [:transaction :write]
     :stability :stable
     :accepts-stdin? true  ;; tx-data can come from stdin
     :supports-remote? true
     :referentially-transparent? false
     :doc "Submits a transaction to the database."
     :examples [{:desc "Add single datom"
                 :code "(transact conn [[:db/add 1 :name \"Ivan\"]])"}
                {:desc "Retract datom"
                 :code "(transact conn [[:db/retract 1 :name \"Ivan\"]])"}
                {:desc "Create entity with tempid"
                 :code "(transact conn [[:db/add -1 :name \"Ivan\"]])"}
                {:desc "Create entity (map form)"
                 :code "(transact conn [{:db/id -1 :name \"Ivan\" :likes [\"fries\" \"pizza\"]}])"}
                {:desc "Nested entity creation"
                 :code "(transact conn [{:db/id -1 :name \"Oleg\" :friend {:db/id -2 :name \"Sergey\"}}])"}
                {:desc "Read from stdin (CLI)"
                 :cli "cat data.edn | dthk transact conn:config.edn -"}]
     :impl datahike.api.impl/transact}

    q
    {:args [:=> [:alt
                 [:cat schema/SQueryArgs]
                 [:cat [:or :vector :map :string] [:* :any]]]
            :any]
     :ret :any
     :categories [:query]
     :stability :stable
     :accepts-stdin? false
     :supports-remote? true
     :referentially-transparent? true
     :doc "Executes a datalog query."
     :examples [{:desc "Query with vector syntax"
                 :code "(q '[:find ?value :where [_ :likes ?value]] db)"}
                {:desc "Query with map syntax"
                 :code "(q '{:find [?value] :where [[_ :likes ?value]]} db)"}
                {:desc "Query with arg-map (pagination)"
                 :code "(q {:query '[:find ?value :where [_ :likes ?value]]
                           :args [db]
                           :offset 2
                           :limit 10})"}]
     :impl datahike.query/q}

    ;; ... (all other operations follow same pattern)
})
```

### Phase 3: Code Generation Infrastructure (Week 3-4)

#### 3.1 Clojure API Generation

**File:** `src/datahike/codegen/clojure.clj` (new)

```clojure
(ns datahike.codegen.clojure
  "Generate Clojure API from specification."
  (:require [datahike.api.specification :refer [api-specification]]
            [malli.core :as m]))

(defn malli-schema->argslist
  "Extract argument list from malli function schema."
  [schema]
  (when (and (vector? schema)
             (= :=> (first schema)))
    (let [[_ input-schema _] schema]
      (cond
        ;; [:cat Type1 Type2] → [arg0 arg1]
        (and (vector? input-schema)
             (= :cat (first input-schema)))
        (list (vec (map-indexed (fn [i _] (symbol (str "arg" i)))
                                (rest input-schema))))

        ;; [:alt [:cat ...] [:cat ...]] → multiple arities
        (and (vector? input-schema)
             (= :alt (first input-schema)))
        (for [alt-form (rest input-schema)]
          (when (and (vector? alt-form)
                     (= :cat (first alt-form)))
            (vec (map-indexed (fn [i _] (symbol (str "arg" i)))
                              (rest alt-form)))))

        :else
        '([& args])))))

(defmacro emit-api
  "Generate all API functions from specification at compile time."
  [spec-sym]
  `(do
     ~@(for [[fn-name {:keys [doc impl args]}] (eval spec-sym)]
         `(def ~(with-meta fn-name
                  {:doc doc
                   :arglists (malli-schema->argslist args)})
            ~(symbol (namespace impl) (name impl))))))

;; Usage in datahike.api:
;; (emit-api datahike.api.specification/api-specification)
```

#### 3.2 Java Code Generation

**File:** `src/datahike/codegen/java.clj` (new, based on prox)

```clojure
(ns datahike.codegen.java
  "Generate Java API from specification."
  (:require [datahike.api.specification :refer [api-specification]]
            [datahike.schema :as schema]
            [clojure.string :as str]))

(defn ->java-method
  "Derive Java method name from Clojure operation name."
  [op-name]
  (let [s (name op-name)
        predicate? (str/ends-with? s "?")
        clean (str/replace s #"[!?]$" "")
        parts (str/split clean #"-")
        camel (apply str (first parts)
                     (map str/capitalize (rest parts)))]
    (if predicate?
      (str "is" (str/upper-case (subs camel 0 1)) (subs camel 1))
      camel)))

(def java-name-overrides
  "Java idiom overrides for specific operations."
  {'q "query"
   'db "database"
   'pull-many "pullBatch"
   'entity-db "getEntityDatabase"
   'database-exists? "existsDatabase"
   'create-database "createDatabase"
   'delete-database "deleteDatabase"})

(defn java-method-name [op-name]
  (get java-name-overrides op-name
       (->java-method op-name)))

(defn malli-schema->java-type
  "Convert malli schema to Java type string."
  [schema]
  (cond
    (keyword? schema) (get schema/malli->java-type schema "Object")
    (and (vector? schema) (= :sequential (first schema))) "List<Object>"
    (and (vector? schema) (= :map (first schema))) "Map<String, Object>"
    :else "Object"))

(defn generate-java-class
  "Generate complete Java class source."
  [class-name package-name]
  (str "package " package-name ";\n\n"
       "import java.util.*;\n"
       "import clojure.lang.IFn;\n\n"
       "public class " class-name " {\n"
       (generate-static-methods)
       "\n"
       (generate-instance-methods)
       "}\n"))

;; Implementation continues...
```

#### 3.3 CLI Generation

**File:** `src/datahike/codegen/cli.clj` (new)

```clojure
(ns datahike.codegen.cli
  "Generate CLI from specification."
  (:require [datahike.api.specification :refer [api-specification]]
            [clojure.string :as str]))

(defn ->cli-command
  "Derive CLI command from operation name and categories."
  [op-name categories]
  (let [base-name (name op-name)]
    (cond
      ;; Database operations: database-exists? → db exists
      (and (contains? (set categories) :database)
           (str/starts-with? base-name "database-"))
      ["db" (-> base-name
                (str/replace #"^database-" "")
                (str/replace #"[?!]$" ""))]

      ;; Transaction operations: transact → tx transact
      (contains? (set categories) :transaction)
      ["tx" (str/replace base-name #"[?!]$" "")]

      ;; Query operations stay flat: q, pull, entity
      (contains? (set categories) :query)
      [(str/replace base-name #"[?!]$" "")]

      ;; Default: flat command
      :else
      [(str/replace base-name #"[?!]$" "")])))

(defn generate-cli-dispatch
  "Generate CLI dispatch table from specification."
  []
  (for [[fn-name {:keys [categories impl accepts-stdin?]}] api-specification]
    {:cmds (->cli-command fn-name categories)
     :fn impl
     :stdin? accepts-stdin?}))

;; Implementation continues...
```

### Phase 4: Migration Execution (Week 4-5)

**Step-by-step migration:**

1. **Add malli dependency** to `deps.edn`:
   ```clojure
   {metosin/malli {:mvn/version "0.16.3"}}
   ```

2. **Create new schema namespace** (`datahike.schema`)
3. **Keep both specs** during transition:
   - `datahike.spec` (old clojure.spec)
   - `datahike.schema` (new malli)

4. **Update specification** incrementally:
   - Start with 5-10 core functions
   - Convert their specs to malli
   - Verify all bindings still work
   - Continue with remaining functions

5. **Test each binding** after conversion:
   - Run existing test suite
   - Verify JavaScript generation
   - Verify HTTP routes
   - Test new CLI generation
   - Test new Java generation

6. **Remove old spec** once migration complete

### Phase 5: Documentation & clj-kondo (Week 5-6)

**clj-kondo integration:**

```clojure
;; .clj-kondo/config.edn (generated)
{:linters
 {:unresolved-symbol
  {:exclude [(datahike.api)]}

  :type-mismatch
  {:namespaces
   {datahike.api
    {database-exists? {:arities {1 {:args [:map]
                                     :ret :boolean}}}
     create-database {:arities {1 {:args [:map]
                                    :ret :map}}}
     transact {:arities {2 {:args [:any :vector]
                             :ret :map}}}}}}}}
```

**Auto-generate API docs:**

```clojure
(ns datahike.codegen.docs
  "Generate API documentation from specification.")

(defn generate-markdown-docs
  "Generate markdown API reference from specification."
  [output-file]
  (spit output-file
        (str "# Datahike API Reference\n\n"
             (for [[fn-name spec-data] api-specification]
               (format "## %s\n\n%s\n\n### Examples\n\n%s\n\n"
                       fn-name
                       (:doc spec-data)
                       (render-examples (:examples spec-data)))))))
```

## Breaking Changes & Compatibility

### Non-Breaking (Backward Compatible)

- ✓ Clojure API - identical surface
- ✓ JavaScript API - identical surface
- ✓ HTTP API - identical routes
- ✓ TypeScript types - enhanced but compatible

### Breaking Changes

- ⚠ Java API - method names may change (if generated)
  - Mitigation: Keep hand-written as deprecated wrappers
  - Timeline: One major version deprecation cycle

- ⚠ CLI commands - new structure
  - Mitigation: Support both old and new commands for 1-2 versions
  - Timeline: Deprecate old commands in 0.8.x, remove in 0.9.0

## Success Metrics

1. **100% API coverage** - All operations in specification
2. **Zero hand-written binding code** - CLI, Java, TypeScript fully generated
3. **Consistent with Proximum** - Same patterns, conventions, tooling
4. **Better IDE support** - clj-kondo types for all APIs
5. **Comprehensive examples** - 2-3 examples per operation
6. **Test coverage** - All schemas validated, all codegen tested

## Timeline

| Week | Phase | Deliverables |
|------|-------|--------------|
| 1-2 | Schema migration | `datahike.schema` with all malli schemas |
| 2-3 | Spec enhancement | Enhanced `api-specification` with metadata |
| 3-4 | Codegen infrastructure | CLI + Java generators |
| 4-5 | Migration execution | All 40+ operations migrated |
| 5-6 | Docs & tooling | clj-kondo integration, docs generation |

## Open Questions

1. **:since field** - Use current version (0.7.1624) or leave empty?
   - Recommendation: Leave empty for now, add incrementally going forward

2. **Java API breaking changes** - How to handle?
   - Recommendation: Generate new API, keep old as deprecated wrappers

3. **CLI command structure** - Grouped (`dthk db create`) vs flat (`dthk create-database`)?
   - Recommendation: Grouped (more organized, scales better)

4. **Proximum spec alignment** - Should we make schemas identical?
   - Recommendation: Align conventions, but schemas differ (different domains)

## Next Steps

1. Review and approve this design document
2. Create schema definitions for 5 core operations (POC)
3. Test malli validation and code generation
4. Decide on timeline and prioritization
5. Begin Phase 1 implementation

---

**Authors:** Claude, Christian Weilbach
**Date:** 2026-01-15
**Status:** Draft for Review
