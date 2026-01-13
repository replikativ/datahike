# Store ID Refactoring - Design & Roadmap

**Status:** In Progress
**Target Version:** 0.7.0
**Breaking Change:** Yes

## Executive Summary

Refactor Datahike's store identity system to use konserve's UUID-based `:id` field instead of backend-specific tuples and the overloaded `:scope` field. This simplifies store identification, enables seamless konserve backend integration, and provides stable identity across distributed replicas.

## Current State Problems

### 1. Fragmented Store Identity

**Datahike's `store-identity` multimethod returns different types:**
```clojure
;; :mem backend
[:mem "hostname" "random-id"]

;; :file backend
[:file "hostname" "/path/to/db"]

;; :indexeddb backend
[:indexeddb "db-name"]

;; :tiered backend (BUG: duplicate backend, should be [frontend backend])
[:tiered [:indexeddb "name"] [:indexeddb "name"]]
```

**Problems:**
- Inconsistent across backends
- Not stable when moving stores (path/hostname changes)
- Can't reliably match store across machines
- Tiered identity is buggy

### 2. Overloaded `:scope` Field

**`:scope` is used for multiple purposes:**

**a) Datahike default configs (hostname):**
```clojure
(defmethod default-config :file [config]
  (merge {:path "..."
          :scope (dt/get-hostname)}  ;; String hostname
         config))
```

**b) Kabel writer (UUID for remote store):**
```clojure
{:writer {:backend :kabel
          :scope-id #uuid "550e8400-..."  ;; UUID
          :peer-id server-id}}
```

**c) Kabel fressian registry (store lookup):**
```clojure
(defn store-identity-for-registry [store-config]
  (select-keys store-config [:backend :scope]))
```

**d) konserve-sync topics:**
```clojure
(keyword (str scope-id))  ;; UUID → keyword
```

**Problems:**
- Overloaded semantics (hostname vs UUID vs identifier)
- Not standardized
- Kabel branch not released yet (can break)

### 3. Duplicate Store Multimethod Infrastructure

**Datahike implements store lifecycle in `datahike.store`:**
```clojure
;; Multimethods dispatching on :backend
(defmulti store-identity :backend)
(defmulti empty-store :backend)
(defmulti delete-store :backend)
(defmulti connect-store :backend)
(defmulti ready-store ...)
(defmulti release-store ...)
(defmulti default-config :backend)
(defmulti config-spec :backend)

;; Per-backend implementations for :mem, :file, :indexeddb, :tiered
```

**Konserve now provides the same via `konserve.store`:**
```clojure
(defmulti -connect-store ...)
(defmulti -create-store ...)
(defmulti -store-exists? ...)
(defmulti -delete-store ...)
(defmulti -release-store ...)

;; All validate :id is UUID and use it for identity
```

**Problems:**
- Duplication of lifecycle management
- External backends (datahike-jdbc, datahike-rocksdb, datahike-lmdb) must implement Datahike multimethods
- Prevents third-party konserve backends from working directly

### 4. External Backend Coupling

**External backends must:**
1. Implement Datahike's store multimethods
2. Maintain separate repos/releases
3. Stay synchronized with Datahike API changes

**With konserve.store:**
- Backends just implement konserve multimethods once
- Work with any konserve client (not just Datahike)
- Already exist: konserve-s3, konserve-dynamodb, konserve-redis, konserve-lmdb, konserve-rocksdb

## Design Decisions

### 1. UUID-Based Store Identity

**Decision:** Use `:id` UUID as sole store identifier.

```clojure
;; User provides :id in config
{:store {:backend :file
         :path "/tmp/db"
         :id #uuid "550e8400-e29b-41d4-a716-446655440000"}}

;; store-identity just returns the :id
(store-identity config) ;; => #uuid "550e8400-..."

;; Connection ID = [store-id branch]
[#uuid "550e8400-..." :db]
```

**Rationale:**
- Stable across machines/paths (critical for distributed setups)
- User controls identity (explicit > implicit)
- Matches konserve's validation
- Simplifies code dramatically

### 2. Remove `:scope` Completely

**Decision:** Eliminate `:scope` from all configs and code.

**Changes:**
- Remove from `default-config` implementations
- Remove from kabel writer (`:scope-id` → use store `:id`)
- Remove from fressian handler registry
- Remove from konserve-sync topic derivation

**Rationale:**
- Kabel branch not released yet (safe to break)
- Overloaded semantics cause confusion
- `:id` serves the identification purpose
- Simpler mental model

### 3. Direct Konserve Integration

**Decision:** Use `konserve.store` multimethods directly, remove Datahike's store infrastructure.

**Keep in `datahike.store`:**
```clojure
(defn add-cache-and-handlers [raw-store config]
  ;; Datahike-specific: add LRU cache and BTSet handlers
  ...)

(defmethod ready-store :tiered [config store]
  ;; Tiered-specific: populate memory from IndexedDB before sync
  ...)
```

**Remove from `datahike.store`:**
- All `empty-store`, `connect-store`, `delete-store`, `release-store` multimethods
- All `default-config` multimethods
- All `config-spec` multimethods
- Backend-specific implementations (:mem, :file, :indexeddb)

**Use konserve directly:**
```clojure
;; In datahike.connector
(require '[konserve.store :as ks])

(ks/create-store (:store config) {:sync? false})
(ks/connect-store (:store config) {:sync? false})
(ks/delete-store (:store config) {:sync? false})
```

**Rationale:**
- Eliminates duplication
- Third-party backends work automatically
- Simpler maintenance
- Konserve handles validation

### 4. Backend Name: `:memory` vs `:mem`

**Decision:** Konserve uses `:memory`, Datahike currently uses `:mem`.

**Options:**
a) Switch to `:memory` (breaking but cleaner)
b) Keep `:mem` with backwards compat in konserve
c) Support both with deprecation path

**Recommendation:** **(a)** - Clean break for 0.7, cleaner going forward.

Users already need to add `:id`, changing `:mem` → `:memory` is minimal friction.

### 5. Config Persistence & Migration

**Decision:** Support loading old databases without `:id`.

**Migration flow:**
```clojure
;; In connector.cljc after loading stored-db
(let [user-config (dc/load-config raw-config)      ;; Has :id (required)
      stored-config (:config stored-db)             ;; Old DB lacks :id

      ;; If stored config has :id, validate it matches user :id
      ;; (Deactivated for now - too brittle with path changes)
      ;; (when (and (:id stored-config)
      ;;            (not= (:id stored-config) (:id user-config)))
      ;;   (throw ...))

      ;; Merge user :id into stored config
      final-config (assoc stored-config :id (get-in user-config [:store :id]))]

  ;; Use final-config
  ;; On next transaction, db->stored will persist :id
  ...)
```

**Rationale:**
- Existing databases continue to work
- User provides :id once, gets stored permanently
- No auto-generation (user controls identity)
- Path to full validation later

### 6. Tiered Store ID Consistency

**Decision:** Enforce frontend/backend `:id` matches tiered config `:id`.

```clojure
;; In konserve.store/-create-store :tiered
(when-not (= (:id config) (:id frontend))
  (throw (ex-info "Frontend :id must match tiered :id" ...)))
(when-not (= (:id config) (:id backend))
  (throw (ex-info "Backend :id must match tiered :id" ...)))
```

**Example valid config:**
```clojure
{:backend :tiered
 :id #uuid "550e8400-e29b-41d4-a716-446655440000"
 :frontend {:backend :memory
            :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
 :backend {:backend :indexeddb
           :name "mydb"
           :id #uuid "550e8400-e29b-41d4-a716-446655440000"}}
```

**Rationale:**
- All sub-stores represent the same logical store
- Prevents configuration errors
- Makes identity explicit

### 7. konserve-sync Topics

**Decision:** Use UUID directly as topic, not `(keyword (str uuid))`.

**Before:**
```clojure
(keyword (str scope-id))  ;; => :550e8400-e29b-41d4-a716-446655440000
```

**After:**
```clojure
(:id store-config)  ;; => #uuid "550e8400-..."
```

**Rationale:**
- Simpler (no conversion)
- Type-safe (UUID vs keyword)
- konserve-sync/kabel support any EDN value as topic
- Not released yet (safe to change)

**Verify:** Check if kabel pubsub requires keywords or accepts UUIDs.

### 8. External Backends

**Decision:** Drop datahike-jdbc, datahike-rocksdb, datahike-lmdb.

**Rationale:**
- All use konserve underneath
- konserve-jdbc, konserve-rocksdb, konserve-lmdb exist
- Users can use konserve backends directly
- Eliminates maintenance burden

**Migration:** Update docs to reference konserve backends.

## Roadmap

### Phase 0: Preparation ✅

- [x] Create this design document
- [x] Compare `datahike.store` vs `konserve.store` for missing specs/features
  - **Finding:** Konserve uses runtime validation only (no specs)
  - **Decision:** Remove all Datahike specs, rely on konserve's `validate-store-config`
  - **Rationale:** Datahike specs are outdated (wrong types, include `:scope`), konserve errors are clear
- [x] Verify kabel pubsub accepts UUID topics (not just keywords)
  - **Finding:** Kabel topics are any EDN value (used as map keys)
  - **Decision:** Use UUID directly as topic, no keyword conversion needed

### Phase 1: Konserve Tiered Store Validation

**Goal:** Enforce ID consistency in tiered stores.

**Files:**
- `konserve/src/konserve/store.cljc`

**Changes:**
1. Add ID validation to `-create-store :tiered`
2. Add ID validation to `-connect-store :tiered`
3. Add test case for mismatched IDs

**Testing:**
```clojure
;; Should throw
{:backend :tiered
 :id #uuid "aaaa..."
 :frontend {:backend :memory :id #uuid "bbbb..."}}  ;; Mismatch!
```

### Phase 2: Datahike Store Cleanup ✅

**Goal:** Remove Datahike multimethods, use konserve directly.

**Status:** COMPLETE (commit 7edeebd)

**Files:**
- `src/datahike/store.cljc`

**Changes:**
1. Compare with `konserve.store` - port any missing specs
2. Remove multimethods: `empty-store`, `connect-store`, `delete-store`, `release-store`, `default-config`, `config-spec`
3. Simplify `store-identity` to single implementation returning `:id`
4. Keep `add-cache-and-handlers` and `ready-store :tiered`
5. Update all callsites to use `konserve.store` directly

**Callsites to update:**
- `src/datahike/connector.cljc`
- `src/datahike/kabel/connector.cljc`
- `src/datahike/writing.cljc`
- Tests

**Testing:**
- Run existing test suite
- Verify `:mem` → `:memory` migration works
- Check error messages when `:id` missing

### Phase 3: Kabel `:scope` Removal

**Goal:** Use store `:id` instead of separate `:scope-id`.

**Files:**
- `src/datahike/kabel/connector.cljc`
- `src/datahike/kabel/writer.cljc`
- `src/datahike/kabel/fressian_handlers.cljc`
- `src/datahike/kabel/handlers.cljc`
- `src/datahike/kabel/tx_broadcast.cljc`

**Changes:**

**connector.cljc:**
```clojure
;; Remove:
{:keys [peer-id scope-id local-peer]} (:writer config)

;; Replace with:
{:keys [peer-id local-peer]} (:writer config)
store-id (get-in config [:store :id])

;; Use UUID directly as topic:
store-topic store-id  ;; Not (keyword (str store-id))
```

**fressian_handlers.cljc:**
```clojure
;; Simplify registry:
(defn store-identity-for-registry [store-config]
  (:id store-config))  ;; Was: (select-keys [:backend :scope])
```

**handlers.cljc:**
```clojure
;; Replace scope-id extraction:
(let [store-id (get-in config [:store :id])]  ;; Was: [:writer :scope-id] or [:store :scope]
  ...)
```

**Testing:**
- Browser integration tests
- JVM integration tests
- Verify sync still works with UUID topics

### Phase 4: Connector Config Migration

**Goal:** Support loading old DBs without `:id`.

**Files:**
- `src/datahike/connector.cljc`

**Changes:**
```clojure
(defn -connect* [raw-config opts]
  (let [user-config (dc/load-config raw-config)
        _ (when-not (get-in user-config [:store :id])
            (throw (ex-info "Store :id is required" ...)))

        ;; Load stored-db from konserve
        stored-db (<?- (k/get store :db))
        stored-config (:config stored-db)

        ;; Merge user :id into stored config if missing
        final-config (cond-> stored-config
                       (nil? (:id (:store stored-config)))
                       (assoc-in [:store :id] (get-in user-config [:store :id])))

        ;; Optional: Validate :id match if both exist
        ;; (Deactivated for now)

        ;; Use final-config - will be persisted on next transaction
        ...])
```

**Testing:**
- Load 0.6 database with new 0.7 code
- Verify :id gets persisted
- Verify subsequent loads work

### Phase 5: Documentation & Migration Guide

**Goal:** Document breaking changes and migration path.

**Files:**
- `CHANGELOG.md`
- `doc/config.md`
- `doc/distributed.md`
- `doc/cljs-support.md`
- `README.md`

**Changes:**
1. Update config examples with `:id`
2. Document `:mem` → `:memory` change
3. Add migration guide for 0.6 → 0.7
4. Remove references to external backends
5. Update kabel writer examples (no `:scope-id`)

**Migration guide sections:**
- Required: Add `:id` to all store configs
- Optional: Change `:mem` → `:memory`
- Kabel: Remove `:scope-id` from writer config
- External backends: Use konserve-jdbc/rocksdb/lmdb directly

### Phase 6: External Backend Cleanup

**Goal:** Remove/deprecate external backend repos.

**Actions:**
1. Archive datahike-jdbc, datahike-rocksdb, datahike-lmdb repos
2. Add README pointing to konserve equivalents
3. Remove from Datahike deps/docs

## Breaking Changes for Users

### Required Changes

**1. Add `:id` to all store configs:**
```clojure
;; Before (0.6)
{:store {:backend :file :path "/tmp/db"}}

;; After (0.7)
{:store {:backend :file :path "/tmp/db" :id #uuid "550e8400-..."}}
```

**2. Change `:mem` → `:memory`:**
```clojure
;; Before
{:store {:backend :mem :id "test"}}

;; After
{:store {:backend :memory :id #uuid "550e8400-..."}}
```

**3. Kabel: Remove `:scope-id`:**
```clojure
;; Before
{:writer {:backend :kabel
          :peer-id server-id
          :scope-id #uuid "550e8400-..."
          :local-peer peer}}

;; After
{:writer {:backend :kabel
          :peer-id server-id
          :local-peer peer}}
;; Uses store :id automatically
```

### Optional Changes

**External backends:**
```clojure
;; Before
{:store {:backend :jdbc :url "..." :scope "..."}}

;; After (use konserve backend directly)
(require '[konserve-jdbc.core])
{:store {:backend :jdbc :url "..." :id #uuid "..."}}
```

## Testing Strategy

### Unit Tests
- [ ] Konserve tiered ID validation
- [ ] store-identity returns UUID
- [ ] Config validation (missing :id)

### Integration Tests
- [ ] Load 0.6 database with 0.7 code
- [ ] Create new database with :id
- [ ] Kabel sync with UUID topics
- [ ] Browser integration test
- [ ] Tiered store with matching IDs
- [ ] Error handling (mismatched IDs, missing :id)

### Regression Tests
- [ ] All existing Datahike tests pass
- [ ] External backend tests (before deprecation)

## Open Questions

### 1. Backend Name: `:mem` vs `:memory`
**Status:** Need decision
**Options:**
- (a) Switch to `:memory` (clean break)
- (b) Support both with deprecation
- (c) Stay with `:mem`

**Recommendation:** (a) - Clean break for 0.7

### 2. konserve-sync UUID Topics
**Status:** Need verification
**Question:** Does kabel pubsub accept UUID topics or require keywords?

**Action:** Test with UUID, verify it works.

### 3. Config Spec Migration
**Status:** Need comparison
**Question:** Does konserve.store have equivalent specs to datahike.store?

**Action:** Compare spec definitions, port any missing ones to konserve.

### 4. ready-store Future
**Status:** Low priority
**Question:** Should `ready-store :tiered` move to konserve or stay in Datahike?

**Decision:** Keep in Datahike for now, can migrate later.

### 5. Backwards Compatibility Window
**Status:** Need policy decision
**Question:** How long to support `:scope` in deprecated form?

**Decision:** No backwards compat - clean break for 0.7 (kabel not released yet).

## Success Criteria

- [x] Design document complete
- [ ] All phases implemented
- [ ] All tests passing
- [ ] Documentation updated
- [ ] Migration guide written
- [ ] Breaking changes clearly communicated
- [ ] konserve tiered ID validation working
- [ ] External backends deprecated/archived

## Timeline

**Target:** Datahike 0.7.0 release
**Dependencies:** konserve store.cljc changes committed first

## References

- konserve store.cljc: `konserve/src/konserve/store.cljc`
- Datahike store.cljc: `src/datahike/store.cljc`
- Kabel connector: `src/datahike/kabel/connector.cljc`
- This branch: `cljs-lean-cps` (not released)
