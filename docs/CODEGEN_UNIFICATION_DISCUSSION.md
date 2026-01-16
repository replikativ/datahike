# Code Generation Unification Discussion

## Current State

We have two operational code generation systems:

### TypeScript Generator
- **Location**: `src/datahike/codegen/typescript.clj` + `src/datahike/codegen/naming.cljc`
- **Task**: `bb codegen-ts`
- **Output**: Type definitions only (`.d.ts`)
- **Integration**: Part of `bb npm-build` pipeline
- **Lines**: ~263 lines (generator) + 34 lines (naming)

### Java Generator
- **Location**: `src/datahike/codegen/java.clj`
- **Task**: `bb codegen-java`
- **Output**: Full implementation (`.java`)
- **Integration**: Part of `bb jcompile` pipeline
- **Lines**: ~395 lines

## The Question: Should We Unify?

### Arguments FOR Unification

#### 1. **Consistency & Discoverability**
```
Current:
src/datahike/codegen/typescript.clj      ← Why is this under js/?
src/datahike/codegen/java.clj       ← Why is this under codegen/?

Unified:
src/datahike/codegen/typescript.clj
src/datahike/codegen/java.clj
src/datahike/codegen/python.clj     ← Future languages clear
```

**Benefit**: New contributors immediately understand where language bindings are generated.

#### 2. **Task Naming Consistency**
```clojure
;; Current
bb codegen-ts      ← Not obvious this is code generation
bb codegen-java   ← Clear it's code generation

;; Unified
bb codegen-ts     ← Consistent naming
bb codegen-java   ← No change
bb codegen-python ← Future
bb codegen-all    ← Run all generators
```

**Benefit**: Predictable task naming for all language bindings.

#### 3. **Shared Utilities**
Both generators have similar logic that could be shared:

**Parameter Extraction**:
```clojure
;; TypeScript (src/datahike/codegen/typescript.clj)
(defn extract-params-from-malli [args-schema]
  (when (= :function (first args-schema))
    (let [first-arity (second args-schema)]
      (extract-params-from-malli first-arity))))

;; Java (src/datahike/codegen/java.clj)
(defn extract-params-from-schema [schema]
  (cond
    (vector? schema) (mapv extract-single-param schema)
    (= :multi-arity (first schema)) :multi-arity
    ...))
```

These could share a base implementation in `src/datahike/codegen/core.cljc`.

**Example Formatting**:
Both format examples from the specification, just with different syntax:
- TypeScript: JSDoc format (`* Examples:\n * - ...`)
- Java: Javadoc format (`* <h3>Examples:</h3>\n * <pre>{@code ...}`)

Could extract: `(format-examples examples :style :jsdoc|:javadoc)`

#### 4. **Future Language Support**
Makes adding new languages trivial:

```clojure
;; Add Python support:
;; 1. Create src/datahike/codegen/python.clj
;; 2. Add bb codegen-python task
;; 3. Done! Same pattern as TypeScript and Java
```

#### 5. **Centralized Specification**
All generators work from `datahike.api.specification`, but unification makes this more explicit:

```clojure
(ns datahike.codegen.core
  (:require [datahike.api.specification :as spec]))

(defn generate-all-languages! [output-dirs]
  (doseq [[lang generator] {:typescript typescript/generate!
                            :java java/generate!
                            :python python/generate!}]
    (println "Generating" lang "bindings...")
    (generator spec/api-specification (get output-dirs lang))))
```

### Arguments AGAINST Full Unification

#### 1. **Different Purposes**
```
TypeScript:  Type definitions ONLY (no implementation)
             ↓
             JavaScript runtime uses ClojureScript implementation

Java:        Full implementation + Javadoc
             ↓
             Java runtime invokes Clojure via IFn
```

They're solving fundamentally different problems.

#### 2. **Different Type Systems**
```clojure
;; TypeScript
:int → "number"
:string → "string"
:boolean → "boolean"
:SConnection → "Connection" (interface)
All functions → "Promise<T>" (async)

;; Java
:int → "int" (primitive)
:string → "String" (object)
:boolean → "boolean" (primitive)
Map<String,Object> → APersistentMap (conversion)
All methods → synchronous (direct Clojure call)
```

Type mappings are language-specific and shouldn't be shared.

#### 3. **Multi-Arity Handling**
```clojure
;; TypeScript: Pick first arity (TS has no overloads)
(defn extract-params-from-malli [args-schema]
  (let [first-arity (second args-schema)]
    ...))

;; Java: Generate all arities as overloads
(if (= :multi-arity (extract-params-from-schema args))
  (let [arities (extract-multi-arity-params args)]
    (str/join "\n\n" (for [params arities] ...))))
```

Different language capabilities require different approaches.

#### 4. **Risk of Premature Abstraction**
- Only 2 generators exist currently
- Shared code creates coupling
- Languages differ more than they're similar
- May constrain future generators

#### 5. **Build Integration Differences**
```clojure
;; TypeScript: Part of npm pipeline
bb npm-build → codegen-ts + ClojureScript compilation + tests

;; Java: Part of JVM compilation
bb jcompile → codegen-java + javac compilation
```

Different build contexts may make full unification awkward.

## Recommended Approach: Hybrid

### ✅ DO Unify (High Value, Low Risk)

#### 1. **File Organization**
```
MOVE:
src/datahike/codegen/typescript.clj → src/datahike/codegen/typescript.clj
src/datahike/codegen/naming.cljc    → src/datahike/codegen/naming.cljc

KEEP:
src/datahike/js/macros.clj     ← ClojureScript macros, not codegen
```

#### 2. **Task Naming**
```clojure
;; bb.edn
codegen-ts {:doc "Generate TypeScript definitions"
            :task (codegen-ts/generate! "npm-package/index.d.ts")}

codegen-java {:doc "Generate Java bindings"
              :task (codegen-java/generate! "java/src-generated")}

codegen-all {:doc "Generate all language bindings"
             :depends [codegen-ts codegen-java]}

;; Keep alias for compatibility
codegen-ts {:doc "[DEPRECATED] Use codegen-ts instead"
           :task (codegen-ts/generate! "npm-package/index.d.ts")}
```

#### 3. **Shared Naming Utilities**
```clojure
;; src/datahike/codegen/naming.cljc
(ns datahike.codegen.naming
  (:require [clojure.string :as str]))

(defn clj-name->camel-case [clj-name]
  "Base camelCase conversion used by multiple languages"
  (let [s (name clj-name)
        clean (str/replace s #"[!?]$" "")
        parts (str/split clean #"-")]
    (apply str (first parts)
           (map str/capitalize (rest parts)))))

(defn clj-name->js-name [clj-name]
  "JavaScript naming (handles reserved words)"
  (let [base (clj-name->camel-case clj-name)]
    (if (= base "with") "withDb" base)))

(defn clj-name->java-method [clj-name]
  "Java naming (handles ! suffix for async)"
  (let [has-bang? (str/ends-with? (name clj-name) "!")
        base (clj-name->camel-case clj-name)]
    (if has-bang? (str base "Async") base)))

(defn clj-name->python-name [clj-name]
  "Python naming (snake_case)"
  (str/replace (name clj-name) #"-" "_"))
```

### ❌ DON'T Unify (Low Value, High Risk)

#### 1. **Type Mappings**
Keep separate - they serve different purposes and have no overlap.

```clojure
;; src/datahike/codegen/typescript.clj
(defn malli->ts-type [schema] ...)

;; src/datahike/codegen/java.clj
(defn malli->java-type [schema] ...)

;; No shared code - completely different
```

#### 2. **Multi-Arity Logic**
Keep separate - language-specific capabilities.

#### 3. **Documentation Formatting**
Keep separate - JSDoc vs Javadoc have different conventions.

### 🤔 MAYBE Share (Evaluate After 3rd Language)

#### 1. **Parameter Extraction**
```clojure
;; src/datahike/codegen/core.cljc (if we create it)
(defn extract-params [schema]
  "Extract parameter names and schemas from malli schema"
  ;; Generic extraction logic
  ...)

;; Each generator adds language-specific processing
(defn typescript-params [schema]
  (->> (core/extract-params schema)
       (map #(assoc % :type (malli->ts-type (:schema %))))))
```

Wait until we have Python generator to see if pattern emerges.

## Proposed Incremental Plan

### Step 1: File Organization (This Week)
- [ ] Move `src/datahike/codegen/typescript.clj` → `src/datahike/codegen/typescript.clj`
- [ ] Move `src/datahike/codegen/naming.cljc` → `src/datahike/codegen/naming.cljc`
- [ ] Update all namespace references
- [ ] Run tests to verify nothing broke

### Step 2: Task Naming (This Week)
- [ ] Rename `bb codegen-ts` → `bb codegen-ts` (keep alias)
- [ ] Add `bb codegen-all` task
- [ ] Update documentation

### Step 3: Shared Naming (Optional - Next Sprint)
- [ ] Extract shared `clj-name->camel-case` to `codegen/naming.cljc`
- [ ] Update TypeScript and Java to use shared base function
- [ ] Add Python naming function (prepare for future)

### Step 4: Wait for 3rd Language (Future)
- Only extract shared utilities after we see patterns emerge
- Python or Kotlin bindings will reveal what truly should be shared

## Questions to Resolve

1. **Should `src/datahike/js/macros.clj` move?**
   - No - it's ClojureScript macros, not codegen
   - Keep it under `js/`

2. **What about `src/datahike/api/types.cljc`?**
   - Contains TypeScript type mappings (`:SConnection → "Connection"`)
   - Should these move to `codegen/typescript.clj`?
   - Or keep them in `api/types.cljc` as semantic type definitions?

3. **Should we create `codegen/core.cljc` now or wait?**
   - **Wait** - premature to extract common code with only 2 generators
   - Revisit after Python generator

4. **Integration with npm-build pipeline?**
   - TypeScript generation is Step 3/5 in `npm-build`
   - After moving to `codegen/`, update `tools.npm/build-npm-package!`
   - Should still work seamlessly

## Decision Needed

**Which approach do you prefer?**

**A) Minimal Unification** (File organization + task naming only)
- Low risk, immediate benefit
- Keeps generators independent
- Easy to understand and maintain

**B) Moderate Unification** (+ shared naming utilities)
- Medium risk, medium benefit
- Some code reuse
- Clearer patterns for future languages

**C) Full Unification** (+ core.cljc with shared extraction/formatting)
- Higher risk, unclear benefit
- Might constrain future generators
- Only 2 generators exist - may be premature

**My recommendation: Start with A, evaluate B after completion**
