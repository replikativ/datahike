# Code Generation System Analysis & Recommendations

## Current State

### API Specification Coverage
**Total operations in api-specification:** 32

```
as-of, connect, create-database, database-exists?, datoms, db, db-with,
delete-database, entity, entity-db, filter, gc-storage, history, index-range,
is-filtered, listen, load-entities, metrics, pull, pull-many, q, query-stats,
release, reverse-schema, schema, seek-datoms, since, tempid, transact,
transact!, unlisten, with
```

### Binding Coverage Comparison

| Binding     | Count | Type        | Uses api-spec? | Has Exclusions? | Auto-synced? |
|-------------|-------|-------------|----------------|-----------------|--------------|
| Java        | 32/32 | Overlay     | ✅ Yes         | ❌ No           | ✅ Yes       |
| TypeScript  | 31/32 | Overlay     | ✅ Yes         | ✅ Yes (1 op)   | ✅ Yes       |
| CLI         | 22/32 | Overlay     | ✅ Yes         | ✅ Yes (10 ops) | ✅ Yes       |
| Native (C)  | 15/32 | Independent | ❌ No          | N/A             | ❌ No        |
| Python      | 15/32 | Independent | ❌ No          | N/A             | ❌ No        |

### Detailed Analysis

#### 1. Java Bindings (`codegen/java.clj`) ✅ GOOD
- **Status:** Fully automated overlay
- **Iterates over:** `api-specification`
- **Type inference:** Malli → Java (automated)
- **Docs:** Uses `:doc` and `:examples` from specification
- **Coverage:** 100% (32/32 operations)
- **Verdict:** ✅ Perfect - fully automatic, always in sync

#### 2. TypeScript Bindings (`codegen/typescript.clj`) ✅ GOOD
- **Status:** Fully automated overlay with exclusions
- **Iterates over:** `api-specification`
- **Type inference:** Malli → TypeScript (automated)
- **Docs:** Uses `:doc` and `:examples` from specification
- **Coverage:** 97% (31/32 operations)
- **Exclusions:** `js-skip-list = #{'transact}` (intentional - ClojureScript incompatible)
- **Verdict:** ✅ Excellent - automatic with explicit exclusions

#### 3. CLI Bindings (`codegen/cli.clj`) ✅ GOOD
- **Status:** Fully automated overlay with exclusions
- **Iterates over:** `api-specification`
- **Exclusions:** `cli-excluded-operations = #{10 ops}` with documented reasons:
  - `listen/unlisten`: Require persistent connection with callbacks
  - `release`: Auto-released on CLI exit
  - `db/tempid/entity-db`: Limited utility in CLI context
  - `as-of/since/history/filter`: Return unserializable DB objects
- **Coverage:** 69% (22/32 operations)
- **Verdict:** ✅ Excellent - automatic with well-justified exclusions

#### 4. Native Bindings (`codegen/native.clj`) ⚠️ NEEDS WORK
- **Status:** Independent, manually maintained
- **Iterates over:** `native-operations` (separate map)
- **Type inference:** Manual Java call strings with type casts
- **Docs:** No docs (could pull from specification)
- **Coverage:** 47% (15/32 operations)
- **Missing operations:** 17 operations not exposed
- **Imports but doesn't use:** `api-specification`
- **Verdict:** ⚠️ Manual, no validation, can get out of sync

**Missing from Native:**
```
as-of, connect, db, db-with, is-filtered, listen, load-entities,
query-stats, release, since, tempid, transact!, unlisten, with,
entity-db, filter, history
```

#### 5. Python Bindings (`codegen/python.clj`) ⚠️ NEEDS WORK
- **Status:** Independent, manually maintained
- **Iterates over:** `python-operations` (separate map)
- **Type inference:** Manual Python type annotations
- **Docs:** Manual (could pull from specification)
- **Coverage:** 47% (15/32 operations)
- **Missing operations:** 17 operations not exposed
- **Imports but doesn't use:** `api-specification`
- **Verdict:** ⚠️ Manual, no validation, can get out of sync

**Missing from Python:** (same as Native)

---

## Problems with Current Approach

### 1. Native & Python Don't Use api-specification
Both files import it but never use it:
```clojure
(:require [datahike.api.specification :refer [api-specification]])
;; ... but never reference api-specification in the code
```

### 2. Documentation Duplication
Documentation is duplicated and can drift:
```clojure
;; api.specification.cljc
database-exists?
{:doc "Checks if a database exists via configuration map."
 :examples [...]}

;; python.clj - DUPLICATED & SIMPLIFIED
database-exists?
{:doc "Check if a database exists."}

;; native.clj - NO DOC AT ALL
database-exists?
{:pattern :config-query
 :java-call "..."}
```

### 3. No Validation or Coverage Checking
- Adding new operation to `api-specification` doesn't trigger warnings
- No compile-time check that native/Python cover operations
- Easy to forget updating native/Python bindings

### 4. Manual Type Annotations
Python type annotations are manual, but could be derived from Malli:
```clojure
;; Manual in python.clj
database-exists?
{:return-type "bool"}  ;; Manually specified

;; Could be derived from api.specification
database-exists?
{:ret :boolean}  ;; Malli schema → Python "bool"
```

---

## Recommendations

### Option 1: Make Native & Python Full Overlays (RECOMMENDED)

Transform native & Python to work like Java/TypeScript - iterate over `api-specification` with exclusion lists.

**Benefits:**
- Automatic coverage of new operations
- Docs pulled from specification
- Validation at generation time
- Single source of truth

**Implementation:**
```clojure
;; native.clj
(def native-excluded-operations
  "Operations not supported in native C API.

  Reasons:
  - connect/release/db: Connection lifecycle - internal per-call
  - listen/unlisten: Callbacks across language boundary
  - as-of/since/history/filter: Return DB objects (handled via input_format)
  - with/db-with: Pure functions better done client-side
  - tempid/entity-db/is-filtered: Limited utility in FFI context"
  #{'connect 'release 'db 'listen 'unlisten 'as-of 'since 'history
    'filter 'with 'db-with 'tempid 'entity-db 'is-filtered})

(def native-overlays
  "Native-specific configuration overlay for each operation."
  '{database-exists? {:pattern :config-query
                      :java-call "Datahike.databaseExists(readConfig(db_config))"}
    ;; ... minimal config, docs come from api-specification
    })

(defn generate-all-entry-points []
  (let [supported-ops (remove (comp native-excluded-operations first) api-specification)]
    (for [[op-name spec] supported-ops
          :let [native-config (get native-overlays op-name)
                _ (when-not native-config
                    (throw (ex-info "Missing native overlay config"
                                    {:op op-name})))
                enriched (merge spec native-config)]]  ;; <-- spec provides :doc!
      (generate-entry-point op-name enriched))))
```

### Option 2: Add Coverage Validation (MINIMAL)

Keep current structure but add validation:

```clojure
(defn validate-coverage! []
  (let [all-ops (set (keys api-specification))
        native-ops (set (keys native-operations))
        missing (clojure.set/difference all-ops native-ops native-excluded-operations)]
    (when (seq missing)
      (println "WARNING: Native bindings missing operations:" missing)
      (println "Either implement them or add to native-excluded-operations"))
    missing))

(defn -main [& args]
  (validate-coverage!)  ;; <-- Run at generation time
  (generate-native-bindings))
```

### Option 3: Automated Type Derivation

Derive Python/TypeScript types from Malli schemas:

```clojure
(defn malli->python-type
  "Convert Malli schema to Python type annotation."
  [schema]
  (cond
    (= schema :boolean) "bool"
    (= schema :string) "str"
    (= schema :int) "int"
    (= schema :long) "int"
    (keyword? schema) "Any"

    (vector? schema)
    (let [[op & args] schema]
      (case op
        :maybe (str "Optional[" (malli->python-type (first args)) "]")
        :or (str/join " | " (map malli->python-type args))
        :vector (str "List[" (malli->python-type (first args)) "]")
        :map "Dict[str, Any]"
        "Any"))

    :else "Any"))

;; Use it:
(defn generate-function [op-name]
  (let [spec (get api-specification op-name)
        py-config (get python-operations op-name)
        ;; Derive return type from spec, or use manual override
        return-type (or (:return-type py-config)
                        (malli->python-type (:ret spec)))]
    ...))
```

---

## Specific Suggestions

### 1. Create Unified Coverage Report Tool

Add to `bb.edn`:
```clojure
codegen-report {:doc "Show codegen coverage across all bindings"
                :task (do
                        (require '[datahike.codegen.report])
                        (datahike.codegen.report/print-coverage))}
```

Output:
```
Codegen Coverage Report
=======================

Total operations in api-specification: 32

Java:        32/32 (100%) ✅
TypeScript:  31/32 ( 97%) ✅ 1 excluded
CLI:         22/32 ( 69%) ✅ 10 excluded with reasons
Native:      15/32 ( 47%) ⚠️  17 missing, no exclusion list
Python:      15/32 ( 47%) ⚠️  17 missing, no exclusion list

Missing from Native & Python:
  - as-of, connect, db, db-with, is-filtered, listen,
    load-entities, query-stats, release, since, tempid,
    transact!, unlisten, with, entity-db, filter, history

Action required: Either implement or add to exclusion list
```

### 2. Add Pre-Generation Validation

```clojure
;; In native.clj
(defn -main [& args]
  (validate-coverage!)           ;; Fail if operations missing
  (validate-doc-sync!)           ;; Warn if docs differ from spec
  (generate-native-bindings))
```

### 3. Pull Documentation Automatically

```clojure
;; Instead of manual :doc in python-operations
(defn generate-function [op-name config]
  (let [spec (get api-specification op-name)
        doc (:doc spec)           ;; <-- From specification
        examples (:examples spec)] ;; <-- From specification
    ...))
```

### 4. Create Exclusion Lists for Native & Python

```clojure
;; native.clj
(def native-excluded-operations
  "Operations explicitly excluded from native C API with reasons."
  '{connect "Connection lifecycle managed internally per-call"
    release "Connections auto-released after each operation"
    listen "Requires persistent callbacks across language boundary"
    ;; ... etc
    })
```

### 5. Add CI Check

```yaml
# .github/workflows/codegen-check.yml
- name: Validate codegen coverage
  run: bb codegen-check  # Fails if operations missing without exclusion
```

---

## Migration Path

### Phase 1: Add Validation (Low Risk)
1. Add `validate-coverage!` to native & Python generators
2. Add `bb codegen-report` task
3. Run and document current gaps

### Phase 2: Create Exclusion Lists
1. Create `native-excluded-operations` with reasons
2. Create `python-excluded-operations` with reasons
3. Update validation to check against exclusions

### Phase 3: Auto-Pull Documentation
1. Modify generators to pull `:doc` from `api-specification`
2. Remove duplicate docs from `native-operations` / `python-operations`
3. Keep only binding-specific config (pattern, java-call, return-type)

### Phase 4: Type Derivation (Optional)
1. Implement `malli->python-type` converter
2. Remove manual `:return-type` annotations where possible
3. Keep manual overrides for complex cases

### Phase 5: Full Overlay (Future)
1. Refactor native & Python to iterate over `api-specification`
2. Move to overlay model like Java/TypeScript
3. Remove redundant operation maps

---

## Summary

**Current State:**
- ✅ Java, TypeScript, CLI: Excellent (automated overlays)
- ⚠️  Native, Python: Manual and incomplete (47% coverage)

**Key Issues:**
1. No validation when new operations added
2. Documentation duplication and drift
3. Type annotations could be automated
4. Import `api-specification` but don't use it

**Recommended Actions:**
1. **Immediate:** Add coverage validation and reporting
2. **Short-term:** Create exclusion lists with documented reasons
3. **Medium-term:** Auto-pull docs from specification
4. **Long-term:** Consider full overlay model for native & Python

**Expected Outcome:**
- Single source of truth for all bindings
- Automatic detection of missing operations
- Reduced maintenance burden
- Consistent documentation across languages
