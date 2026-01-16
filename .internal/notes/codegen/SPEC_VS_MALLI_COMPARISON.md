# clojure.spec vs Malli: Migration Analysis for Datahike

## Executive Summary

Proximum successfully uses malli instead of clojure.spec for API specification and code generation. This document analyzes the differences and what Datahike needs to change for migration.

## Key Findings from Proximum

### 1. No Malli-Specific Findings About Code Generation

The background agent researched Clojure CLI patterns but **did not find information about malli** because they were focused on CLI tool patterns, not the prox codebase itself. The exploration agent filled this gap by directly examining prox's implementation.

### 2. Malli Advantages Over clojure.spec

| Aspect | Malli | clojure.spec |
|--------|-------|--------------|
| **Data vs Macros** | Schemas are plain data (maps/vectors) | Specs are macros (compile-time only) |
| **Runtime Introspection** | Full schema as value, easy to inspect | Limited introspection |
| **Error Messages** | `me/humanize` for readable errors | Standard `explain` output |
| **JSON Schema** | Native generation | Manual implementation needed |
| **Function Schemas** | Clean `:=> [:cat ...]` notation | Complex `s/alt`, `s/cat` nesting |
| **Coercion** | Built-in transformers | Requires custom code |
| **Performance** | Compiled validators (faster) | Slower runtime validation |
| **Tooling** | Growing (reitit, malli-select) | Mature but static |

## What Needs to Change in Datahike

### 1. Schema Definitions

**Current (`datahike.spec`):**
```clojure
(s/def ::SConfig map?)
(s/def ::SConnection #(instance? clojure.lang.Atom %))
(s/def ::SDB #(satisfies? datahike.db.interface/IDB %))
(s/def ::SEId (s/or :eid int? :lookup-ref (s/tuple keyword? any?)))
(s/def ::STransactions (s/or :vec vector? :seq seq?))
```

**New (`datahike.schema`):**
```clojure
(def SConfig
  [:map {:closed false}
   [:store [:map
            [:backend [:enum :mem :file]]
            [:id :string]
            [:path {:optional true} :string]]]
   [:keep-history? {:optional true :default true} :boolean]
   [:schema-flexibility {:optional true :default :write} [:enum :read :write]]])

(def SConnection
  [:fn {:error/message "must be a Datahike connection"}
   #(instance? clojure.lang.IDeref %)])

(def SDB
  [:fn {:error/message "must be a Datahike database"}
   #(satisfies? datahike.db.interface/IDB %)])

(def SEId
  [:or :int
   [:tuple :keyword :any]])  ;; lookup ref

(def STransactions
  [:or [:sequential :any] [:vector :any]])
```

**Changes:**
- `s/def` → `def` (schemas are values, not specs)
- `s/or` → `:or`, `s/tuple` → `:tuple`
- Predicates stay functions but wrapped in `:fn`
- `:closed false` allows extra keys (like spec's open maps)
- Defaults and documentation inline

### 2. Function Argument Specs

**Current (complex `s/alt` nesting):**
```clojure
:args (s/alt :config (s/cat :config spec/SConfig)
             :nil (s/cat))
```

**New (cleaner malli):**
```clojure
:args [:=> [:alt
            [:cat SConfig]
            [:cat]]
        :boolean]
```

**Multi-arity example:**

Current:
```clojure
:args (s/alt :simple (s/cat :db spec/SDB :opts spec/SPullOptions)
             :full (s/cat :db spec/SDB :selector coll? :eid spec/SEId))
```

New:
```clojure
:args [:=> [:alt
            [:cat SDB SPullOptions]
            [:cat SDB :vector SEId]]
        [:map]]
```

**Changes:**
- Top-level `:=>` wraps function schema
- `:alt` for multiple arities (same as spec)
- `:cat` for argument sequence (same as spec)
- Return type as third element in `:=>`

### 3. Arglist Derivation

**Current (complex extraction):**
```clojure
(defn spec-args->argslist [s]
  (if-not (seq? s)
    (if (= :nil s) [] [(symbol (name s))])
    (let [[op & args] s]
      (cond
        (= op 's/cat)
        [(vec (mapcat (fn [[k v]]
                        (if (and (seq? v) (= (first v) 's/*))
                          (vec (concat ['&] (spec-args->argslist k)))
                          (spec-args->argslist k)))
                      (partition 2 args)))]
        ;; ... more cases
        ))))
```

**New (simpler with malli):**
```clojure
(defn malli-schema->argslist [schema]
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
          (vec (map-indexed (fn [i _] (symbol (str "arg" i)))
                            (rest alt-form))))))))
```

**Why simpler:**
- Schemas are data (vectors), not s-expressions
- No macro expansion needed
- Pattern matching on vectors vs symbols

### 4. Type Mapping for Code Generation

**Current (manual in each generator):**
```clojure
;; In datahike.codegen.typescript
(def spec-type-map
  {'boolean? "boolean"
   'string? "string"
   'map? "object"
   ;; ... manual mapping per binding
   })
```

**New (centralized in schema namespace):**
```clojure
;; In datahike.schema
(def malli->java-type
  {:SConfig "Map<String, Object>"
   :SConnection "Connection"
   :SDB "Database"
   :boolean "boolean"
   :int "int"
   ;; ... single source of truth
   })

(def malli->typescript-type
  {:SConfig "DatabaseConfig"
   :SConnection "Connection"
   :SDB "Database"
   :boolean "boolean"
   :int "number"
   ;; ... derived consistently
   })
```

**Advantage:** Change once, applies to all bindings.

### 5. Validation & Error Messages

**Current:**
```clojure
(s/valid? ::SConfig config)
;;=> false

(s/explain ::SConfig config)
;;=> In: [:store :backend]
;;   val: :invalid
;;   fails spec: :datahike.spec/backend
;;   predicate: #{:mem :file}
```

**New:**
```clojure
(m/validate SConfig config)
;;=> false

(m/explain SConfig config)
;;=> #Error{:path [:store :backend]
;;         :in [:store :backend]
;;         :schema [:enum :mem :file]
;;         :value :invalid}

(me/humanize (m/explain SConfig config))
;;=> {:store {:backend "should be either :mem or :file"}}
```

**Advantage:** Better error messages, easier to display to users.

### 6. Code Generation Improvements

**What Proximum generates that Datahike doesn't:**

| Feature | Datahike (current) | Proximum (malli) |
|---------|-------------------|------------------|
| Java source code | ❌ Hand-written | ✅ Fully generated |
| Java builders | ❌ Manual | ✅ Generated from schemas |
| CLI commands | ❌ Manual | ✅ Generated from spec |
| clj-kondo types | ⚠️ Partial | ✅ Full generation |
| Validation sync | ❌ None | ✅ Checks Java vs spec |

**New capabilities from migration:**

1. **Java Builder Pattern:**
   ```java
   // Generated from HnswConfig schema
   Database db = Datahike.builder()
       .store(storeConfig)
       .keepHistory(true)
       .schemaFlexibility(SchemaFlexibility.WRITE)
       .index(IndexType.HITCHHIKER_TREE)
       .build();
   ```

2. **CLI Dispatch Table:**
   ```clojure
   ;; Generated from api-specification
   (def cli-commands
     {"db" {"create" {:fn create-database :stdin? false}
            "delete" {:fn delete-database :stdin? false}
            "exists" {:fn database-exists? :stdin? false}}
      "tx" {"transact" {:fn transact :stdin? true}}})
   ```

3. **Type-Checked API:**
   ```clojure
   ;; clj-kondo knows full type signatures
   (transact conn tx-data)  ;; ✓ correct
   (transact conn 123)      ;; ✗ error: expected vector/seq, got int
   ```

## Migration Strategy

### Recommended Approach: Gradual Migration

1. **Phase 1: Add malli alongside spec** (no breaking changes)
   - Create `datahike.schema` with malli schemas
   - Keep `datahike.spec` for backward compatibility
   - Update spec references incrementally

2. **Phase 2: Dual validation** (validate both)
   - Functions validate with both spec and malli
   - Log discrepancies
   - Build confidence in malli schemas

3. **Phase 3: Switch to malli** (breaking change)
   - Remove clojure.spec dependency
   - Remove `datahike.spec` namespace
   - All validation uses malli

4. **Phase 4: Enhanced codegen** (new features)
   - Generate Java API
   - Generate CLI from spec
   - Full clj-kondo integration

### Compatibility Considerations

**Non-breaking:**
- Clojure API surface identical
- JavaScript API surface identical
- HTTP routes identical
- TypeScript types compatible (may add fields)

**Potentially breaking:**
- Java API method names (if auto-generated)
  - Mitigation: Keep old methods as deprecated wrappers
- CLI command structure (if changed to grouped)
  - Mitigation: Support both old and new for 1-2 versions

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Malli schema bugs | High | Comprehensive test suite, dual validation period |
| Breaking Java API | Medium | Deprecation period, wrapper methods |
| Performance regression | Low | Malli is faster; benchmark anyway |
| Learning curve | Low | Document patterns, similar to spec |
| Ecosystem maturity | Low | Malli widely adopted (reitit, etc.) |

## Recommendations

1. **Start small:** Migrate 5-10 core operations as POC
2. **Test extensively:** Every schema needs validation tests
3. **Keep spec temporarily:** Dual validation during transition
4. **Document patterns:** Clear examples for contributors
5. **Align with Proximum:** Reuse conventions, helper functions
6. **Gradual rollout:** One binding at a time (Clojure → JS → Java → CLI)

## Open Questions

1. **Should we make Datahike and Proximum schemas identical?**
   - No - different domains (database vs vector index)
   - Yes - align conventions and helper functions
   - Recommendation: Shared helpers, domain-specific schemas

2. **Timeline for clojure.spec removal?**
   - Recommendation: 2-3 release cycle
   - Version 0.8: Add malli, dual validation
   - Version 0.9: Malli only, deprecate spec
   - Version 1.0: Remove spec entirely

3. **Java API breaking changes - how to handle?**
   - Option A: Generate new classes (PersistentDatabase, etc.)
   - Option B: Generate + keep old as wrappers
   - Recommendation: Option B (smooth transition)

4. **CLI grouped vs flat commands?**
   - Flat: `dthk create-database` (current style)
   - Grouped: `dthk db create` (like git, more scalable)
   - Recommendation: Grouped (40+ commands benefit from organization)

## Conclusion

Migrating to malli provides:
- ✅ Better error messages
- ✅ Simpler code generation
- ✅ Centralized type mappings
- ✅ Runtime introspection
- ✅ Consistency with Proximum
- ✅ Full Java/CLI generation

The migration is **low-risk** with **high reward**, especially given Proximum's successful implementation of the same pattern.

---

**Status:** Analysis Complete
**Next Step:** Review design document and approve POC implementation
