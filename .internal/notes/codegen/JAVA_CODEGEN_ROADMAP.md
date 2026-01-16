# Java API Code Generation - Implementation Roadmap

**Status**: Planning Complete, Ready for Implementation
**Start Date**: 2026-01-15
**Target Completion**: 2 weeks

---

## Overview

This roadmap tracks implementation of auto-generated Java API bindings from the Datahike API specification. See [JAVA_CODEGEN_DESIGN.md](JAVA_CODEGEN_DESIGN.md) for detailed design.

---

## Phase 1: Core Generator (Days 1-2)

**Goal**: Basic code generation infrastructure working

### Tasks

- [ ] **1.1** Create namespace `datahike.codegen.java`
  - File: `src/datahike/codegen/java.clj`
  - Basic structure with `-main` entry point
  - File I/O for writing generated Java source

- [ ] **1.2** Implement type mapping
  - Function: `malli->java-type`
  - Handle primitive types (`:boolean`, `:int`, `:string`, etc.)
  - Handle collection types (`:vector`, `:set`, `:sequential`)
  - Handle semantic types (`:datahike/SConnection`, `:datahike/SDB`, etc.)
  - Test with sample schemas

- [ ] **1.3** Implement IFn declaration generation
  - Function: `generate-ifn-declarations`
  - Input: `api-specification`
  - Output: String with all `protected static final IFn` declarations
  - Format: `protected static final IFn <opName>Fn = Clojure.var("datahike.api", "<op-name>");`

- [ ] **1.4** Implement basic method generation
  - Function: `generate-method` (single-arity only for now)
  - Convert operation name to Java method name (`database-exists?` → `databaseExists`)
  - Extract parameters from malli `:args` schema
  - Map return type from malli `:ret` schema
  - Generate simple method body with `<opName>Fn.invoke(...)`

- [ ] **1.5** Generate skeleton DatahikeGenerated.java
  - Package declaration
  - Imports (Clojure, IFn, APersistentMap, Datom, etc.)
  - Class declaration (package-private)
  - Static initialization block
  - 3-5 sample methods to test compilation

- [ ] **1.6** Test compilation
  - Create `java/src-generated/` directory structure
  - Generate sample `DatahikeGenerated.java`
  - Compile with `javac` to verify syntax
  - Fix any compilation errors

**Deliverable**: Compiling `DatahikeGenerated.java` with 3-5 operations

**Success Criteria**:
- ✅ Generator runs without errors
- ✅ Generated Java code compiles
- ✅ Generated IFn declarations are correct
- ✅ Sample methods have correct signatures

---

## Phase 2: Complete Method Generation (Days 3-4)

**Goal**: Generate all operations with all features

### Tasks

- [ ] **2.1** Implement multi-arity method generation
  - Detect when spec has `:function` schema with multiple arities
  - Generate multiple Java overloads for same operation
  - Example: `gcStorage(Object db)` and `gcStorage(Object db, Date removeBefore)`
  - Test with operations that have optional parameters

- [ ] **2.2** Implement varargs handling
  - Detect when operation accepts variable arguments
  - Generate Java varargs signature: `Object... inputs`
  - Properly construct argument list for `IFn.applyTo`
  - Test with `q`, `datoms`, `seek-datoms`

- [ ] **2.3** Implement collection type mapping with strength rules
  - When spec says `:vector` → use `List<?>`
  - When spec says `:set` → use `Set<?>`
  - When spec says `:sequential` → use `Iterable<?>`
  - When spec says `:datahike/SDatoms` → use `Iterable<Datom>`
  - Add proper casts with `@SuppressWarnings("unchecked")`

- [ ] **2.4** Implement parameter conversion logic
  - For `Map<String, Object>` config parameters → `Util.mapToPersistentMap(config)`
  - For String pull patterns → `Clojure.read(pattern)`
  - For varargs → construct arg list with `RT.seq`
  - Test all parameter conversion paths

- [ ] **2.5** Implement return value conversion logic
  - For transaction reports → `Util.clojureMapToJavaMap(...)`
  - For metrics → `Util.clojureMapToJavaMap(...)`
  - For schema → cast to `Map<Object, Object>` (no conversion)
  - For collections → appropriate cast based on strength rules
  - Minimize conversions - only when necessary

- [ ] **2.6** Generate all operations from spec
  - Iterate through all entries in `api-specification`
  - Generate method for each operation
  - Verify no operations are skipped
  - Count: should have ~35 operations

- [ ] **2.7** Test generated code
  - Compile complete `DatahikeGenerated.java`
  - Verify all methods have correct signatures
  - Test method invocations work (basic smoke tests)
  - Check for compilation warnings

**Deliverable**: Complete `DatahikeGenerated.java` with all ~35 operations

**Success Criteria**:
- ✅ All operations from spec have generated methods
- ✅ Multi-arity operations have multiple overloads
- ✅ Varargs operations compile and work correctly
- ✅ Collection return types use strongest possible interface
- ✅ No compilation errors or warnings
- ✅ Basic smoke tests pass

---

## Phase 3: Javadoc Generation (Day 5)

**Goal**: Comprehensive documentation from spec

### Tasks

- [ ] **3.1** Extract doc strings from spec
  - Read `:doc` field from spec entries
  - Format as javadoc comments
  - Handle multi-line doc strings
  - Escape special characters for javadoc

- [ ] **3.2** Format examples from spec
  - Read `:examples` field if present
  - Format as `<h3>Examples:</h3>` section
  - Wrap in `<pre>{@code ... }</pre>` blocks
  - Indent properly
  - Test javadoc rendering in IDE

- [ ] **3.3** Generate @param tags
  - Extract parameter names from generated signature
  - Generate `@param <name> <description>` for each
  - Derive description from parameter type and position
  - Handle varargs parameters specially

- [ ] **3.4** Generate @return tags
  - Extract return type from generated signature
  - Generate `@return <description>`
  - Describe what the return value represents
  - Note if return is opaque object vs typed

- [ ] **3.5** Test javadoc rendering
  - Generate javadoc HTML with `javadoc` tool
  - View in browser
  - Verify all docs render correctly
  - Check links and formatting

**Deliverable**: Fully documented generated API with rich javadocs

**Success Criteria**:
- ✅ All methods have javadoc comments
- ✅ Doc strings from spec are included
- ✅ Examples render as code blocks
- ✅ @param tags present for all parameters
- ✅ @return tags describe return values
- ✅ Javadoc HTML renders without errors

---

## Phase 4: Hand-Written Wrappers (Days 6-8)

**Goal**: Ergonomic typed API layer

### Tasks

- [ ] **4.1** Implement Connection interface and implementation
  - File: `java/src/datahike/java/Connection.java` (interface)
  - File: `java/src/datahike/java/ConnectionImpl.java` (implementation)
  - Methods: `unwrap()`, `db()`, `transact()`, `release()`
  - Delegate to `DatahikeGenerated` methods
  - Test all methods work

- [ ] **4.2** Implement Database interface and implementation
  - File: `java/src/datahike/java/Database.java` (interface)
  - File: `java/src/datahike/java/DatabaseImpl.java` (implementation)
  - Methods: `unwrap()`, `entity()`, `pull()`, `pullMany()`, `datoms()`, `asOf()`, `since()`, `history()`, `schema()`, etc.
  - Delegate to `DatahikeGenerated` methods
  - Return typed wrappers where appropriate
  - Test all methods work

- [ ] **4.3** Implement Entity interface and implementation
  - File: `java/src/datahike/java/Entity.java` (interface extends Map)
  - File: `java/src/datahike/java/EntityImpl.java` (implementation)
  - Methods: `unwrap()`, `db()`, `get(String)`, `touch()`
  - Implement Map interface methods
  - Convenience: `get(String)` auto-prefixes `:` for keywords
  - Test Map operations work

- [ ] **4.4** Implement TransactionReport wrapper
  - File: `java/src/datahike/java/TransactionReport.java`
  - Constructor: takes `Map<String, Object>`
  - Methods: `dbBefore()`, `dbAfter()`, `txData()`, `tempids()`, `txMeta()`, `asMap()`
  - Return typed wrappers for db values
  - Test all accessor methods

- [ ] **4.5** Implement QueryResult wrapper
  - File: `java/src/datahike/java/QueryResult.java`
  - Type checking: `isScalar()`, `isTuples()`, `isNull()`
  - Safe accessors: `asScalar()`, `asTuples()`
  - Direct access: `get()`, `as(Class)`
  - Test with different query result shapes

- [ ] **4.6** Update Datahike.java facade
  - File: `java/src/datahike/java/Datahike.java`
  - Extend `DatahikeGenerated`
  - Expose generated methods publicly
  - Add convenience methods: `edn()`, `deref()`, `keyword()`
  - Add typed wrapper constructors: `connect()`, `q()`, `transact()`
  - Test facade provides clean public API

- [ ] **4.7** Enhance Util.java
  - File: `java/src/datahike/java/Util.java`
  - Keep existing: `kwd()`, `map()`, `vec()`, `ednFromString()`
  - Add: `mapToPersistentMap()` - convert Java Map to Clojure map
  - Add: `ednToMap()` - parse EDN string to Java Map
  - Add: `clojureMapToJavaMap()` - convert Clojure map to Java Map
  - Handle nested maps recursively
  - Handle keyword key conversions
  - Test all utilities

**Deliverable**: Complete typed Java API with wrappers

**Success Criteria**:
- ✅ All wrapper interfaces defined
- ✅ All implementations delegate correctly
- ✅ Typed wrappers provide better IDE experience
- ✅ Util methods handle conversions correctly
- ✅ Public facade is clean and intuitive
- ✅ No breaking changes to existing API usage

---

## Phase 5: Testing & Documentation (Days 9-10)

**Goal**: Validated and documented API

### Tasks

- [ ] **5.1** Port existing Java tests
  - File: `java/src/datahike/java/DatahikeTest.java`
  - Update to use new generated API
  - Ensure all existing tests pass
  - Fix any breaking changes

- [ ] **5.2** Add tests for typed wrappers
  - Test Connection wrapper methods
  - Test Database wrapper methods
  - Test Entity wrapper and Map interface
  - Verify delegations work correctly

- [ ] **5.3** Add tests for QueryResult
  - Test with scalar query results
  - Test with tuple query results
  - Test with nil results
  - Test type checking methods
  - Test error cases (wrong casts)

- [ ] **5.4** Add tests for TransactionReport
  - Test all accessor methods
  - Test database before/after
  - Test tempid mapping
  - Verify Datom conversion

- [ ] **5.5** Update Java documentation
  - Update `doc/java-api.md` (create if needed)
  - Document new typed wrappers
  - Add usage examples
  - Document configuration options (Map vs EDN)
  - Add migration guide from old API

- [ ] **5.6** Add comprehensive examples
  - Basic CRUD operations
  - Query examples (scalar, tuples, collections)
  - Transaction examples
  - Temporal query examples
  - Entity access examples
  - Schema examples

**Deliverable**: Tested and documented Java API

**Success Criteria**:
- ✅ All tests pass
- ✅ Test coverage for all wrappers
- ✅ Documentation is complete
- ✅ Examples are clear and working
- ✅ Migration guide helps existing users

---

## Phase 6: Build Integration (Days 11-12)

**Goal**: Automated code generation in build

### Tasks

- [ ] **6.1** Add bb task for code generation
  - Update `bb.edn`
  - Add `:codegen-java` task
  - Task runs: `clojure -M -m datahike.codegen.java java/src-generated`
  - Test task execution

- [ ] **6.2** Add bb task for compilation
  - Add `:compile-java` task
  - Depends on `:codegen-java`
  - Compiles all Java sources
  - Test full build pipeline

- [ ] **6.3** Integrate into CI/CD
  - Update `.circleci/config.yml` or CI config
  - Add step to run code generation
  - Add step to verify generated code compiles
  - Add step to run Java tests
  - Test CI pipeline

- [ ] **6.4** Add pre-commit hook (optional)
  - Regenerate Java API on spec changes
  - Detect changes to `api/specification.cljc`
  - Auto-run `bb codegen-java`
  - Commit generated files

- [ ] **6.5** Update release process
  - Document code generation in release checklist
  - Ensure generated files are up-to-date before release
  - Add version metadata to generated files
  - Test release build

- [ ] **6.6** Add developer documentation
  - Document code generation process
  - Explain how to regenerate
  - Explain when regeneration is needed
  - Add troubleshooting guide

**Deliverable**: Automated code generation in build pipeline

**Success Criteria**:
- ✅ `bb codegen-java` works reliably
- ✅ CI runs code generation and tests
- ✅ Build fails if generated code is out of date
- ✅ Release process includes generation step
- ✅ Documentation helps developers

---

## Post-Implementation

### Validation Checklist

Before marking complete, verify:

- [ ] All operations from spec have Java bindings
- [ ] Generated code compiles without warnings
- [ ] All existing Java tests pass
- [ ] New wrapper tests pass
- [ ] Javadoc is complete and renders correctly
- [ ] Build process regenerates automatically
- [ ] Performance is equivalent to hand-written code
- [ ] No extra conversions at boundaries
- [ ] Documentation is complete and accurate
- [ ] Examples work as shown

### Known Limitations

Document these for future work:

1. Query type safety is runtime, not compile-time (by design)
2. Config type safety is limited (uses Map, not typed config object)
3. Keyword handling requires explicit `Datahike.keyword()` calls
4. Some operations return opaque Object (Connection, Database)

### Future Enhancements

Consider for later:

1. Typed config builder (if DI concerns can be addressed)
2. Query DSL for compile-time type safety (major effort)
3. Annotation-based schema definitions
4. Better IDE support with parameter hints

---

## Next Steps After Java API

Once Java API is complete and validated:

1. **LibDatahike Generation** - Apply similar patterns to C FFI layer
2. **Pydatahike Generation** - Generate Python ctypes wrappers
3. **Other Language Bindings** - Expand to additional languages as needed

See design doc for preliminary plans on these.

---

## Progress Tracking

### Current Status: **Planning Complete**

**Completed Phases**: None yet
**Current Phase**: Phase 1 - Core Generator
**Next Milestone**: Compiling DatahikeGenerated.java skeleton

### Time Estimates

- Phase 1: 2 days
- Phase 2: 2 days
- Phase 3: 1 day
- Phase 4: 3 days
- Phase 5: 2 days
- Phase 6: 2 days

**Total**: ~12 working days (~2 calendar weeks)

---

## Questions / Blockers

Track any open questions or blockers here:

- None currently

---

## References

- Design Document: [JAVA_CODEGEN_DESIGN.md](JAVA_CODEGEN_DESIGN.md)
- API Specification: `src/datahike/api/specification.cljc`
- Existing Java API: `java/src/datahike/java/Datahike.java`
- TypeScript Codegen Reference: `src/datahike/codegen/typescript.clj`
- Prox Codegen Reference: `../prox/src/proximum/codegen/java_source.clj`
