# Code Generation Comparison: TypeScript vs Java

## Overview

Datahike now has two code generation systems that create language bindings from the API specification:

1. **TypeScript** - Type definitions for npm package
2. **Java** - Full API implementation for JVM

## Side-by-Side Comparison

| Aspect | TypeScript | Java |
|--------|-----------|------|
| **Generator File** | `src/datahike/codegen/typescript.clj` (263 lines) | `src/datahike/codegen/java.clj` (395 lines) |
| **Output** | Type definitions (.d.ts) | Full implementation (.java) |
| **Output Location** | `npm-package/index.d.ts` | `java/src-generated/datahike/java/DatahikeGenerated.java` |
| **Build Task** | `bb codegen-ts` | `bb codegen-java` |
| **Auto-run** | In `bb npm-build` | In `bb jcompile` |
| **Source** | `api-specification` | `api-specification` |
| **Lines Generated** | ~600 (types + docs) | ~741 (code + docs) |

## Type Mapping

### TypeScript
```clojure
;; In src/datahike/api/types.cljc
(def malli->typescript-type
  {:SConfig "DatabaseConfig"
   :SConnection "Connection"
   :SDB "Database"
   :SEId "number | [string, any] | string"
   :SDatom "Datom"
   :SDatoms "Datom[]"
   ...})

;; In src/datahike/codegen/typescript.clj
(defn malli->ts-type [schema]
  (case schema
    :boolean "boolean"
    :string "string"
    :int "number"
    :long "number"
    ...))
```

### Java
```clojure
;; In src/datahike/codegen/java.clj
(defn malli->java-type [schema]
  (case schema
    :boolean "boolean"
    :string "String"
    :int "int"
    :long "long"
    ...))
```

**Key Difference**: TypeScript mapping is split (primitives in codegen, semantic types in api.types), Java is all in codegen.

## Name Conversion

### TypeScript
```clojure
;; Shared in src/datahike/codegen/naming.cljc
(defn clj-name->js-name [clj-name]
  (let [s (name clj-name)
        s (cond-> s
            (str/ends-with? s "?") (subs 0 (dec (count s)))
            (str/ends-with? s "!") (subs 0 (dec (count s))))
        parts (str/split s #"-")
        base-name (str (first parts)
                      (apply str (map str/capitalize (rest parts))))]
    ;; Handle JS reserved words
    (if (= base-name "with") "withDb" base-name)))
```

### Java
```clojure
;; In src/datahike/codegen/java.clj
(defn clj-name->java-method [op-name]
  (let [s (name op-name)
        has-bang? (str/ends-with? s "!")
        clean (str/replace s #"[!?]$" "")
        parts (str/split clean #"-")
        base (apply str (first parts)
                    (map str/capitalize (rest parts)))]
    ;; Add Async suffix if had !
    (if has-bang? (str base "Async") base)))
```

**Key Differences**:
- TypeScript handles JS reserved words (`with` → `withDb`)
- Java handles `!` suffix (`transact!` → `transactAsync`)
- TypeScript is shared (used by macros + codegen), Java is inline

## What They Generate

### TypeScript Example
```typescript
/**
 * Connects to a Datahike database via configuration map.
 *
 * Examples:
 * - Connect to default in-memory database
 *   (connect)
 * - Connect to file-based database
 *   (connect {:store {:backend :file :path "/tmp/example"}})
 */
export function connect(arg0: any): Promise<any>;
```

**Generates**: Type signature only (implementation is ClojureScript)

### Java Example
```java
/**
 * Connects to a Datahike database via configuration map.
 *
 * <h3>Examples:</h3>
 * <pre>{@code
 * // Connect to default in-memory database
 * (connect)
 * // Connect to file-based database
 * (connect {:store {:backend :file :path "/tmp/example"}})
 * }</pre>
 */
static Object connect(Map<String,Object> arg0) {
    return (Object) connectFn.invoke(Util.mapToPersistentMap(arg0));
}
```

**Generates**: Full implementation with invocation logic

## Multi-Arity Handling

### TypeScript
```clojure
;; Takes first arity only for type signature
(defn extract-params-from-malli [args-schema]
  (when (= :function (first args-schema))
    (let [first-arity (second args-schema)]
      (extract-params-from-malli first-arity))))
```

Result: Single function signature (simplest arity)

### Java
```clojure
;; Generates all arities as overloads
(if (= :multi-arity (extract-params-from-schema args))
  (let [arities (extract-multi-arity-params args)]
    (str/join "\n\n"
              (for [params arities]
                (generate-method-with-params ...)))))
```

Result: Multiple overloaded methods

## Skip Lists

### TypeScript
```clojure
;; In src/datahike/codegen/naming.cljc
(def js-skip-list #{'transact})  ;; Sync version incompatible with ClojureScript
```

### Java
No skip list - all operations are compatible

## Async Handling

### TypeScript
All functions return `Promise<T>` (ClojureScript is async)

### Java
All methods are synchronous (direct Clojure interop)

## Documentation Generation

### TypeScript (JSDoc)
```clojure
(defn generate-jsdoc [doc examples]
  (when doc
    (let [summary (first (str/split doc #"\.\s"))
          example-text (when (seq examples)
                        (str "\n *\n * Examples:\n" ...))]
      (str "/**\n * " summary "."
           example-text
           "\n */"))))
```

### Java (Javadoc)
```clojure
(defn format-javadoc [doc examples]
  (when doc
    (let [lines (str/split-lines doc)
          doc-lines (map #(str "     * " (escape-javadoc %)) lines)
          example-lines (when (seq examples)
                          (concat
                           ["     * <h3>Examples:</h3>"
                            "     * <pre>{@code"]
                           ...))]
      (str "    /**\n"
           (str/join "\n" (concat doc-lines example-lines))
           "\n     */"))))
```

**Similarities**: Both include docstring + examples
**Differences**: HTML escaping, formatting conventions

## Recommendations

### A) Integrate into Unified Codegen Stack?

**YES, with structure:**

```
src/datahike/codegen/
  ├── core.cljc          # Shared utilities
  │   ├── extract-params
  │   ├── format-examples
  │   └── file-writing
  ├── naming.cljc        # Shared naming (move from js/)
  │   ├── clj-name->java-method
  │   ├── clj-name->js-name
  │   └── handle-reserved-words
  ├── java.clj           # Java-specific (current)
  └── typescript.clj     # TS-specific (move from js/)
```

**Benefits:**
- Clear separation: shared vs language-specific
- Easier to add new languages (Python, Kotlin, etc.)
- Consistent task naming
- Shared utilities

**Migration:**
1. Move `src/datahike/codegen/typescript.clj` → `src/datahike/codegen/typescript.clj`
2. Move `src/datahike/codegen/naming.cljc` → `src/datahike/codegen/naming.cljc`
3. Create `src/datahike/codegen/core.cljc` for shared utils
4. Update namespaces and imports

### B) Make Java and JavaScript More Consistent?

**Partially YES:**

#### Consistency Improvements:

**1. Task Naming** ✅ HIGH PRIORITY
```clojure
;; Current
codegen-ts      → Generate TypeScript definitions
codegen-java   → Generate Java bindings

;; Proposed
codegen-ts     → Generate TypeScript definitions
codegen-java   → Generate Java bindings (no change)
codegen-all    → Generate all language bindings
```

**2. File Organization** ✅ MEDIUM PRIORITY
```
# Current
src/datahike/codegen/typescript.clj
src/datahike/codegen/java.clj

# Proposed
src/datahike/codegen/typescript.clj
src/datahike/codegen/java.clj
```

**3. Shared Naming Functions** ✅ LOW PRIORITY
```clojure
;; In src/datahike/codegen/naming.cljc
(defn clj-name->java-method [name] ...)
(defn clj-name->js-name [name] ...)
(defn clj-name->python-name [name] ...)  ;; Future
```

**4. Shared Parameter Extraction** ⚠️ MAYBE
```clojure
;; Could share basic extraction logic
;; But keep language-specific type mapping separate
(defn extract-params [args-schema]
  (map (fn [param] {:name ... :schema ...}) ...))
```

#### Keep Different:

**Type Mappings** ❌ DON'T MERGE
- TypeScript: `malli->ts-type` (types only, Promise wrappers)
- Java: `malli->java-type` (implementation, conversions, primitives)
- **Reason**: Serve fundamentally different purposes

**Multi-Arity Handling** ❌ DON'T MERGE
- TypeScript: Pick first arity (TS doesn't have overloads)
- Java: Generate all overloads
- **Reason**: Language capabilities differ

**Documentation Format** ❌ DON'T MERGE
- TypeScript: JSDoc
- Java: Javadoc with HTML
- **Reason**: Language-specific conventions

## Proposed Action Plan

### Phase 0: Cleanup (Immediate - Zero Risk)
- [ ] Delete `generate-types.clj` from repository root (obsolete script)
- [ ] Verify `bb codegen-ts` and `bb npm-build` still work

### Phase 1: Naming Consistency (Low Risk)
- [ ] Rename `bb codegen-ts` → `bb codegen-ts`
- [ ] Keep `codegen-ts` as alias for compatibility
- [ ] Add `bb codegen-all` task
- [ ] Update documentation

### Phase 2: File Organization (Medium Risk)
- [ ] Move `src/datahike/codegen/typescript.clj` → `src/datahike/codegen/typescript.clj`
- [ ] Move `src/datahike/codegen/naming.cljc` → `src/datahike/codegen/naming.cljc`
- [ ] Update all namespace references
- [ ] Test both generators

### Phase 3: Shared Utilities (Optional)
- [ ] Create `src/datahike/codegen/core.cljc`
- [ ] Extract shared parameter extraction
- [ ] Extract shared documentation formatting
- [ ] Keep type mappings separate

## Benefits of Consolidation

1. **Consistency**: Clear pattern for adding new languages
2. **Discoverability**: All codegen in one place
3. **Maintenance**: Shared utilities reduce duplication
4. **Testing**: Easier to test consistently
5. **Future**: Ready for Python, Kotlin, Go, etc.

## Risks

1. **Breaking Changes**: Moving files requires namespace updates
2. **Complexity**: Too much sharing can create coupling
3. **Testing**: Need to ensure both generators still work

## The generate-types.clj Mystery

### What It Is

There's a standalone script at the repository root: `generate-types.clj`

```clojure
#!/usr/bin/env bb
;; Generate TypeScript definitions for npm package

(require '[datahike.codegen.typescript :as ts])

(println "Generating TypeScript definitions...")
(ts/write-type-definitions! "npm-package/index.d.ts")
(println "TypeScript definitions generated at npm-package/index.d.ts")
```

### The Problem: It's Redundant

This script duplicates functionality that already exists in the build system:

1. **bb.edn task** (line 162-163):
   ```clojure
   codegen-ts {:doc "Generate TypeScript definitions for npm package"
              :task (npm/generate-typescript-definitions! "npm-package/index.d.ts")}
   ```

2. **tools.npm namespace** (bb/src/tools/npm.clj:34-47):
   ```clojure
   (defn generate-typescript-definitions! [output-path]
     (println "Generating TypeScript definitions...")
     (let [clj-code (str "(require '[datahike.codegen.typescript :as ts]) "
                         "(ts/write-type-definitions! \"" output-path "\")")
           result (p/shell {:out :string :err :string}
                          "clojure" "-M" "-e" clj-code)]
       ...))
   ```

3. **npm-build pipeline** - Already includes type generation as Step 3/5

### Why It Exists

Likely created as a quick standalone script before the integrated build task existed. It's now obsolete.

### Recommendation: Remove It

- ❌ **Delete** `generate-types.clj` from repository root
- ✅ **Use** `bb codegen-ts` or `bb npm-build` instead
- 📝 **Document** in README that `bb codegen-ts` is the canonical way to generate types

Benefits of removal:
- One less redundant script to maintain
- Clearer build process (all tasks via bb)
- No confusion about which method to use
- Consistent with Java codegen (no standalone script)

## Recommendation

**Start with Phase 1 (naming) immediately** - low risk, high benefit
**Consider Phase 2 (organization)** - good for future expansion
**Skip Phase 3 for now** - minimal duplication, different enough to keep separate
**Remove generate-types.clj** - obsolete, duplicates bb task
