# Java API Code Generation - Design Document

**Status**: Design Phase
**Created**: 2026-01-15
**Authors**: Claude Code Assistant, Christian Weilbach

## Overview

This document describes the design and implementation plan for auto-generating the Datahike Java API from the universal API specification (`datahike.api.specification`).

### Goals

1. **Eliminate manual synchronization** - Generate Java bindings automatically from the specification so changes to the API are reflected in Java without manual updates
2. **Maintain compatibility** - Keep the existing Java API patterns (static methods, Clojure interop)
3. **Improve ergonomics** - Add typed wrapper interfaces (Connection, Database, Entity) for better IDE support
4. **Enable future FFI layers** - Establish patterns for LibDatahike (C FFI) and pydatahike generation

### Non-Goals

- Breaking changes to existing Java API consumers
- Performance optimization (use Clojure types directly, minimal conversion)
- Full type safety for dynamic Datalog queries (queries return flexible types)

---

## Architecture Overview

### Three-Layer Design

```
┌─────────────────────────────────────────────────────────┐
│  Public Java API (Hand-written)                         │
│  - Datahike.java (facade, convenience methods)          │
│  - Connection, Database, Entity (typed interfaces)      │
│  - TransactionReport, QueryResult (wrapper types)       │
│  - Util.java (EDN parsing, Map conversion)              │
└────────────────┬────────────────────────────────────────┘
                 │
                 │ delegates to
                 ▼
┌─────────────────────────────────────────────────────────┐
│  Generated Internal API (Package-private)                │
│  - DatahikeGenerated.java (all IFn vars, static methods)│
│  - Generated from api-specification                      │
│  - ~35 operations, multi-arity overloads                │
└────────────────┬────────────────────────────────────────┘
                 │
                 │ invokes
                 ▼
┌─────────────────────────────────────────────────────────┐
│  Clojure Runtime                                         │
│  - datahike.api namespace                                │
│  - Clojure collections, Datom deftype                    │
└─────────────────────────────────────────────────────────┘
```

### Key Principles

1. **Generate the repetitive** - All static method declarations from spec
2. **Hand-write the ergonomic** - Typed wrappers, builders, convenience methods
3. **No data conversion at boundary** - Use Clojure types directly (APersistentMap, Datom, etc.)
4. **Leverage existing types** - Use `datahike.datom.Datom` (already a Java class from deftype)

---

## Type Mapping: Malli Schema → Java

### Return Type Mapping

```clojure
;; Primitives
:any           → Object
:nil           → void
:boolean       → boolean
:int           → int
:long          → long
:double        → double
:string        → String
:keyword       → Object  ; clojure.lang.Keyword

;; Collections (use strongest interface we can cast to)
:map           → Map<?,?>
:vector        → List<?>
:sequential    → Iterable<?>
:set           → Set<?>

;; Semantic Types (Datahike-specific)
:datahike/SConnection        → Object  ; opaque Clojure connection
:datahike/SDB                → Object  ; opaque Clojure database
:datahike/SEntity            → Object  ; clojure.lang.ILookup
:datahike/STransactionReport → Map<String, Object>  ; converted to Java Map
:datahike/SSchema            → Map<Object, Object>
:datahike/SMetrics           → Map<String, Object>
:datahike/SDatoms            → Iterable<Datom>  ; import datahike.datom.Datom
:datahike/SEId               → Object  ; can be Long, String, Keyword, lookup ref
:datahike/SPullPattern       → String  ; EDN string representation
:datahike/SQueryArgs         → Object  ; varargs in Java

;; Temporal types
inst?          → java.util.Date
```

### Parameter Type Mapping

```clojure
:datahike/SConfig       → Map<String, Object>  ; flexible for DI
:datahike/SConnection   → Object
:datahike/SDB           → Object
:datahike/STransactions → List  ; Java List
:datahike/SPullPattern  → String
:datahike/SEId          → Object
inst?                   → java.util.Date
```

### Collection Strength Rules

Use the strongest Java collection interface we can safely cast to without conversion:

- Spec says `:vector` → `List<?>`
- Spec says `:set` → `Set<?>`
- Spec says `:sequential` → `Iterable<?>`
- Spec says `:map` → `Map<?,?>`
- Unknown/`:any` → `Object`

---

## File Structure

### Generated Files

```
java/src-generated/datahike/java/
└── DatahikeGenerated.java    # All generated static methods (package-private)
```

**Characteristics**:
- Package-private visibility (`class DatahikeGenerated`)
- Contains all IFn static field declarations
- Contains all static method implementations
- Generated from `api-specification` via `datahike.codegen.java`
- Regenerated on spec changes

### Hand-Written Files

```
java/src/datahike/java/
├── Datahike.java             # Public facade (extends DatahikeGenerated)
├── Connection.java           # Typed wrapper interface
├── ConnectionImpl.java       # Implementation (delegates to DatahikeGenerated)
├── Database.java             # Typed wrapper interface
├── DatabaseImpl.java         # Implementation (delegates to DatahikeGenerated)
├── Entity.java               # Typed wrapper interface
├── EntityImpl.java           # Implementation
├── TransactionReport.java    # Wrapper for tx report map
├── QueryResult.java          # Wrapper for query results with type checking
└── Util.java                 # Utilities (EDN parsing, Map conversion, keyword)
```

**Characteristics**:
- Public API visible to users
- Stable across spec changes (minimal updates needed)
- Provide convenience, type safety, IDE support

---

## Generated Code Structure

### DatahikeGenerated.java Template

```java
package datahike.java;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.APersistentMap;
import clojure.lang.RT;
import datahike.datom.Datom;
import java.util.*;

/**
 * Generated Datahike API bindings.
 * DO NOT EDIT - Generated from datahike.api.specification
 *
 * This class is package-private. Use the public Datahike facade instead.
 */
class DatahikeGenerated {

    // ===== Generated IFn Static Fields =====

    protected static final IFn connectFn = Clojure.var("datahike.api", "connect");
    protected static final IFn createDatabaseFn = Clojure.var("datahike.api", "create-database");
    protected static final IFn deleteDatabaseFn = Clojure.var("datahike.api", "delete-database");
    protected static final IFn qFn = Clojure.var("datahike.api", "q");
    // ... ~35 total operations from spec

    // ===== Static Initialization =====

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("datahike.api"));
    }

    // ===== Generated Static Methods =====

    /**
     * <Generated javadoc from spec :doc>
     *
     * <Examples from spec :examples if available>
     *
     * @param <generated from spec :args>
     * @return <generated from spec :ret>
     */
    static <ReturnType> <methodName>(<Parameters>) {
        // Generated implementation
    }

    // ... all operations
}
```

### Method Generation Rules

1. **Method Name**: Use operation name from spec, converting `?` and `!` suffixes
   - `database-exists?` → `databaseExists`
   - `sync!` → `sync`

2. **Multi-Arity**: Generate Java overloads
   ```java
   static Object gcStorage(Object db)
   static Object gcStorage(Object db, Date removeBefore)
   ```

3. **Varargs**: Use Java varargs for operations accepting variable inputs
   ```java
   static Object q(String query, Object... inputs)
   static Iterable<Datom> datoms(Object db, Object index, Object... components)
   ```

4. **Return Types**: Use strongest type we can cast to safely
   ```java
   static void createDatabase(Map<String, Object> config)  // void
   static boolean databaseExists(Map<String, Object> config)  // boolean
   static Object connect(Map<String, Object> config)  // opaque
   static Iterable<Datom> datoms(Object db, Object index, Object... components)  // typed
   ```

5. **Conversions**: Minimal - only when necessary
   ```java
   // Convert Java Map to Clojure map for config params
   createDatabaseFn.invoke(Util.mapToPersistentMap(config));

   // Convert Clojure map to Java map for return values
   return Util.clojureMapToJavaMap((APersistentMap) metricsFn.invoke(db));
   ```

---

## Javadoc Generation

Generated javadoc includes:

1. **Main description** from spec `:doc`
2. **Examples** from spec `:examples` (if present) formatted as code blocks
3. **@param** tags for each parameter
4. **@return** tag describing return value

Example:
```java
/**
 * Returns datoms from an index.
 *
 * <p>Provides raw access to index data matching the specified components.
 * Results are lazy and can be large - use with care.
 *
 * <h3>Examples:</h3>
 * <pre>{@code
 * // All datoms in EAVT index
 * Iterable<Datom> all = db.datoms(Datahike.keyword(":eavt"));
 *
 * // Datoms for specific entity
 * Iterable<Datom> entityDatoms = db.datoms(Datahike.keyword(":eavt"), 123);
 * }</pre>
 *
 * @param db database value
 * @param index index to query - one of :eavt, :aevt, :avet, :vaet
 * @param components optional index components for filtering
 * @return iterable of datoms matching the lookup criteria
 */
static Iterable<Datom> datoms(Object db, Object index, Object... components)
```

---

## Hand-Written API Layer

### Datahike.java - Public Facade

```java
public class Datahike extends DatahikeGenerated {
    private Datahike() {}

    // ===== Expose generated methods publicly =====

    public static void createDatabase(Map<String, Object> config) {
        DatahikeGenerated.createDatabase(config);
    }
    // ... expose all generated methods

    // ===== Convenience methods =====

    /** Parse EDN string to Java Map. */
    public static Map<String, Object> edn(String ednString) {
        return Util.ednToMap(ednString);
    }

    /** Dereference connection to get database value. */
    public static Object deref(Object conn) {
        return Util.derefFn.invoke(conn);
    }

    /** Create keyword from string. */
    public static Keyword keyword(String name) {
        return Util.kwd(name);
    }

    // ===== Typed wrappers =====

    /** Connect to database. Returns typed Connection wrapper. */
    public static Connection connect(Map<String, Object> config) {
        return new ConnectionImpl(DatahikeGenerated.connect(config));
    }

    /** Get database from connection. */
    public static Database db(Connection conn) {
        return new DatabaseImpl(deref(conn.unwrap()));
    }

    /** Execute query. Returns typed QueryResult wrapper. */
    public static QueryResult q(String query, Object... inputs) {
        return QueryResult.of(DatahikeGenerated.q(query, inputs));
    }

    /** Transact. Returns typed TransactionReport wrapper. */
    public static TransactionReport transact(Connection conn, List txData) {
        return new TransactionReport(
            DatahikeGenerated.transact(conn.unwrap(), txData));
    }
}
```

### Connection Interface

```java
public interface Connection {
    /** Get underlying Clojure connection object. */
    Object unwrap();

    /** Get current database value. */
    Database db();

    /** Transact data to database. */
    TransactionReport transact(List txData);

    /** Release connection resources. */
    void release();
}
```

### Database Interface

```java
import datahike.datom.Datom;

public interface Database {
    /** Get underlying Clojure database object. */
    Object unwrap();

    // Entity access
    Entity entity(Object eid);

    // Pull API
    Map<Object, Object> pull(String pattern, Object eid);
    List<Map<Object, Object>> pullMany(String pattern, List eids);

    // Index access
    Iterable<Datom> datoms(Object index, Object... components);
    Iterable<Datom> seekDatoms(Object index, Object... components);

    // Temporal databases
    Database asOf(Date t);
    Database since(Date t);
    Database history();
    Database filter(Predicate<Datom> pred);
    boolean isFiltered();

    // Schema
    Map<Object, Object> schema();
    Map<Object, Object> reverseSchema();
    Map<String, Object> metrics();
}
```

### TransactionReport Wrapper

```java
public class TransactionReport {
    private final Map<String, Object> report;

    public TransactionReport(Map<String, Object> report) {
        this.report = report;
    }

    public Database dbBefore() {
        return new DatabaseImpl(report.get("db-before"));
    }

    public Database dbAfter() {
        return new DatabaseImpl(report.get("db-after"));
    }

    @SuppressWarnings("unchecked")
    public Iterable<Datom> txData() {
        return (Iterable<Datom>) report.get("tx-data");
    }

    @SuppressWarnings("unchecked")
    public Map<Object, Long> tempids() {
        return (Map<Object, Long>) report.get("tempids");
    }

    public Object txMeta() {
        return report.get("tx-meta");
    }

    public Map<String, Object> asMap() {
        return report;
    }
}
```

### QueryResult Wrapper

```java
public class QueryResult {
    private final Object value;

    private QueryResult(Object value) {
        this.value = value;
    }

    public static QueryResult of(Object value) {
        return new QueryResult(value);
    }

    // Type checking
    public boolean isScalar() {
        return !(value instanceof Collection);
    }

    public boolean isTuples() {
        if (!(value instanceof Collection)) return false;
        Collection<?> coll = (Collection<?>) value;
        if (coll.isEmpty()) return false;
        return coll.iterator().next() instanceof List;
    }

    public boolean isNull() {
        return value == null;
    }

    // Safe accessors with runtime checks
    public Object asScalar() {
        if (!isScalar() && !isNull()) {
            throw new ClassCastException("Query result is not a scalar");
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    public Collection<List<Object>> asTuples() {
        if (!isTuples()) {
            throw new ClassCastException("Query result is not tuples");
        }
        return (Collection<List<Object>>) value;
    }

    // Direct access
    public Object get() {
        return value;
    }

    // Generic cast helper
    @SuppressWarnings("unchecked")
    public <T> T as(Class<T> type) {
        return (T) value;
    }
}
```

---

## Code Generator Implementation

### Generator Namespace

**File**: `src/datahike/codegen/java.clj`

```clojure
(ns datahike.codegen.java
  "Generate Java source code from API specification."
  (:require [datahike.api.specification :refer [api-specification]]
            [clojure.string :as str]
            [clojure.java.io :as io]))

(defn malli->java-type
  "Map malli schema to Java type string."
  [schema]
  ;; Implementation based on type mapping rules above
  )

(defn generate-ifn-declarations
  "Generate static IFn field declarations."
  []
  ;; For each operation in api-specification
  ;; Generate: protected static final IFn <op>Fn = Clojure.var("datahike.api", "<op>");
  )

(defn generate-method
  "Generate a single static method from spec entry."
  [[op-name {:keys [args ret doc examples]}]]
  ;; 1. Convert op-name to Java method name
  ;; 2. Map args to Java parameters
  ;; 3. Map ret to Java return type
  ;; 4. Generate javadoc
  ;; 5. Generate method body with Clojure interop
  )

(defn generate-java-class
  "Generate complete DatahikeGenerated.java source."
  []
  (str
    "package datahike.java;\n\n"
    "// Imports\n"
    "import clojure.java.api.Clojure;\n"
    "import clojure.lang.IFn;\n"
    "import datahike.datom.Datom;\n"
    "// ...\n\n"
    "/** Generated from datahike.api.specification */\n"
    "class DatahikeGenerated {\n\n"
    (generate-ifn-declarations)
    "\n\n"
    "static {\n"
    "    IFn require = Clojure.var(\"clojure.core\", \"require\");\n"
    "    require.invoke(Clojure.read(\"datahike.api\"));\n"
    "}\n\n"
    (str/join "\n\n" (map generate-method api-specification))
    "\n}\n"))

(defn write-generated-source
  "Write generated Java source to file."
  [output-dir]
  (let [package-dir (io/file output-dir "datahike" "java")
        java-file (io/file package-dir "DatahikeGenerated.java")]
    (.mkdirs package-dir)
    (spit java-file (generate-java-class))
    (println "Generated:" (.getPath java-file))))

(defn -main [& args]
  (let [output-dir (or (first args) "java/src-generated")]
    (println "Generating Java API from specification...")
    (write-generated-source output-dir)
    (println "Done.")))
```

### Build Integration

**Add to `bb.edn`**:
```clojure
{:tasks
 {:requires ([tools.build])

  codegen-java
  {:doc "Generate Java API from specification"
   :task (clojure "-M -m datahike.codegen.java java/src-generated")}

  compile-java
  {:doc "Compile Java sources"
   :depends [codegen-java]
   :task (tools.build/compile-java)}}}
```

**Usage**:
```bash
bb codegen-java  # Generate Java API
bb compile-java  # Generate and compile
```

---

## Implementation Roadmap

### Phase 1: Core Generator (Week 1)

**Goal**: Generate basic static methods from spec

Tasks:
1. ✅ Create `src/datahike/codegen/java.clj` namespace
2. ✅ Implement type mapping (`malli->java-type`)
3. ✅ Implement IFn declaration generation
4. ✅ Implement basic method generation (single-arity, simple types)
5. ✅ Generate DatahikeGenerated.java skeleton
6. ✅ Test compilation of generated code

**Deliverable**: Compiling `DatahikeGenerated.java` with subset of operations

### Phase 2: Complete Method Generation (Week 1)

**Goal**: Handle all operation types and arities

Tasks:
1. ✅ Implement multi-arity method generation (overloads)
2. ✅ Implement varargs handling (`q`, `datoms`, etc.)
3. ✅ Implement collection type mapping with strength rules
4. ✅ Add parameter conversion logic (Map → APersistentMap)
5. ✅ Add return value conversion logic where needed
6. ✅ Generate all ~35 operations from spec

**Deliverable**: Complete `DatahikeGenerated.java` with all operations

### Phase 3: Javadoc Generation (Week 1)

**Goal**: Rich documentation from spec

Tasks:
1. ✅ Extract doc strings from spec
2. ✅ Format examples from spec as code blocks
3. ✅ Generate @param tags
4. ✅ Generate @return tags
5. ✅ Test javadoc rendering

**Deliverable**: Fully documented generated API

### Phase 4: Hand-Written Wrappers (Week 2)

**Goal**: Ergonomic typed API

Tasks:
1. ✅ Implement `Connection` interface and `ConnectionImpl`
2. ✅ Implement `Database` interface and `DatabaseImpl`
3. ✅ Implement `Entity` interface and `EntityImpl`
4. ✅ Implement `TransactionReport` wrapper
5. ✅ Implement `QueryResult` wrapper
6. ✅ Update `Datahike.java` facade to use generated code
7. ✅ Enhance `Util.java` with EDN parsing, Map conversion

**Deliverable**: Complete typed Java API

### Phase 5: Testing & Documentation (Week 2)

**Goal**: Validate and document the API

Tasks:
1. ✅ Port existing Java tests to use new API
2. ✅ Add tests for typed wrappers
3. ✅ Add tests for QueryResult, TransactionReport
4. ✅ Update Java documentation
5. ✅ Create migration guide for existing users
6. ✅ Add examples to documentation

**Deliverable**: Tested, documented Java API

### Phase 6: Build Integration (Week 2)

**Goal**: Seamless build process

Tasks:
1. ✅ Add `bb codegen-java` task
2. ✅ Integrate into CI/CD
3. ✅ Add pre-commit hook to regenerate on spec changes
4. ✅ Update release process

**Deliverable**: Automated code generation in build pipeline

---

## Future Work (Post-Java API)

### LibDatahike C FFI Generation

After Java API is complete, apply similar patterns to generate LibDatahike C entry points:

```java
@CEntryPoint(name = "database_exists")
public static void database_exists(
    @CEntryPoint.IsolateThreadContext long isolateId,
    @CConst CCharPointer db_config,
    @CConst CCharPointer output_format,
    @CConst OutputReader output_reader) {
    // Generated from spec
}
```

**Note**: LibDatahike has different signature patterns (callbacks, string passing) and we should discuss this separately after Java API is working.

### Pydatahike Generation

After LibDatahike, generate Python ctypes wrappers:

```python
def database_exists(config, output_format="cbor"):
    # Generated from spec
    dll.database_exists(isolatethread, c_char_p(config), ...)
```

---

## Success Criteria

1. ✅ All operations from `api-specification` have Java bindings
2. ✅ Generated code compiles without errors
3. ✅ Existing Java tests pass with new API
4. ✅ New typed wrapper tests pass
5. ✅ Generated javadoc is complete and accurate
6. ✅ Build process regenerates on spec changes
7. ✅ No manual synchronization needed when spec changes
8. ✅ Performance is equivalent to hand-written bindings (no extra conversions)

---

## Open Questions

None - design is finalized and ready for implementation.

---

## Appendix: Usage Examples

### Basic Usage (Raw API)

```java
import datahike.java.*;
import java.util.*;

// Create database
Map<String, Object> config = Map.of(
    "store", Map.of("backend", "mem", "id", "test"),
    "keep-history?", true
);
Datahike.createDatabase(config);

// Connect and query
Object conn = Datahike.connect(config);
Object db = Datahike.deref(conn);

QueryResult result = Datahike.q(
    "[:find ?e ?n :where [?e :name ?n]]", db);
Collection<List<Object>> tuples = result.asTuples();
```

### Typed Wrapper Usage

```java
import datahike.java.*;
import datahike.datom.Datom;

// Connect with typed wrapper
Connection conn = Datahike.connect(config);
Database db = conn.db();

// Transaction
TransactionReport report = conn.transact(List.of(
    Map.of("name", "Alice", "age", 30)
));
Database dbAfter = report.dbAfter();

// Entity access
Entity alice = db.entity(1);
String name = (String) alice.get("name");

// Datoms - no conversion overhead!
Iterable<Datom> datoms = db.datoms(Datahike.keyword(":eavt"));
for (Datom d : datoms) {
    System.out.println(d.e + " " + d.a + " = " + d.v);
}

// Temporal queries
Database histDb = db.history();
Database asOfDb = db.asOf(new Date());
```

### EDN String Usage

```java
// Parse EDN config
Map<String, Object> config = Datahike.edn("""
    {:store {:backend :mem :id "test"}
     :keep-history? true}
    """);

Datahike.createDatabase(config);
```

---

## References

- Existing Datahike.java: `java/src/datahike/java/Datahike.java`
- API Specification: `src/datahike/api/specification.cljc`
- Malli Types: `src/datahike/api/types.cljc`
- TypeScript Codegen (reference): `src/datahike/codegen/typescript.clj`
- Prox Java Codegen (reference): `../prox/src/proximum/codegen/java_source.clj`
