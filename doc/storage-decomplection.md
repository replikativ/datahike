# Decomplecting datahike's storage + PSS serialization layer

Status: **design / study** (no code changed yet). Goal: streamline datahike's storage
layer — unify the two drifted PSS handler sets onto the canonical codec, remove the
mutable-state-for-storage hack, and decide how to handle the "pending writes" buffer and
the tiered-store bug — **without correctness or perf regressions**.

## 1. What exists today (the map)

### 1a. TWO drifted PSS fressian handler sets

Both live in datahike and share the same tag *strings* but have genuinely drifted.

**In-store** — `src/datahike/index/persistent_set.cljc:420-559` (`di/add-konserve-handlers`):
- tags `datahike.index.PersistentSortedSet` / `.Leaf` / `.Branch` (+ `datahike.datom.Datom`).
- Branch payload `{:level :keys :addresses :subtree-count}`; CLJ read uses the **7-arg**
  `Branch` ctor; Leaf payload `{:level :keys}` (level ignored on read).
- Root read is **eager** → a live `PersistentSortedSet`, storage threaded from a
  closed-over `(atom nil)` `reset!`'d *after* the store is built (the mutable hack, §1b).
- comparator from `(index-type->cmp-quick (:index-type meta) false)`.
- cljs: Leaf write hardcodes `{:level 0}` ("not supported in cljs"); builds `BTSet`.

**Wire / kabel** — `src-kabel/datahike/kabel/fressian_handlers.cljc`:
- **same tag strings** (deliberate reuse).
- Branch payload `{:level :keys :addresses}` — **drops `:subtree-count`**; CLJ read uses the
  **4-arg** `Branch` ctor; rebuilds `Settings` fresh per read (`DEFAULT_BRANCHING_FACTOR`
  redefined locally).
- Root read is **deferred** → a `{:deferred-type :persistent-sorted-set …}` map, later
  rebuilt by `reconstruct-deferred-indexes` resolving storage from a **global
  `store-registry` atom** keyed by `(:id store-config)`, comparator from meta `:index-type`.
- adds `datahike.db.DB` + `datahike.db.TxReport` handlers (the in-store set lacks these).

Drift summary: wire drops subtree-count, uses a different Branch arity, defers root
reconstruction, and resolves storage via a global registry instead of a closed-over atom.
One socket, one serializer (`datahike-fressian-middleware`) already multiplexes RPC +
konserve-sync replication + tx-report pubsub — so the wire handler set is the *shared* one.

### 1b. The mutable-state-for-storage hack (what we want gone)

`persistent_set.cljc:430-435,557-559`: handlers close over `(atom nil)`; `k/assoc-serializers`
builds the store with those handlers; then `(reset! storage (create-storage store config))`.
It breaks a real cycle: a root read handler must hand each set an `IStorage` to lazy-load
children → that storage (`CachedStorage`) wraps `store` → `store` owns the serializer → the
serializer owns these handlers. So storage can't exist when the handler is defined.

The **wire path already shows the alternative**: resolve storage at *read* time from a
registry keyed by an id in the set's `meta`, after the store is fully built and registered.
That is exactly the canonical `root-read-handler {:resolve-storage … :resolve-cmp …}` +
`store-registry` we shipped in persistent-sorted-set for proximum/yggdrasil/stratum.

### 1c. Addresses are serialization-independent (migration is clean)

`gen-address` (`persistent_set.cljc:217-222`) hashes **child addresses** (Branch) / **keys**
(Leaf) under `:crypto-hash?`, or `(squuid)` otherwise — **never the serialized bytes**. So
changing tags/payload does NOT change a node's address. Old nodes keep their addresses and
read fine; new nodes hash identically; structural sharing survives the migration boundary
even under content-addressing. The old payloads are *subsets* of the canonical maps
(`{:keys}` ⊂ canonical leaf; `{:level :keys :addresses :subtree-count}` ⊂ canonical branch;
`{:meta :address :count}` = canonical root), so **canonical handlers can read old data as-is**.
datahike indices carry no measure-ops ⇒ `_measure` is nil ⇒ canonical won't emit `:measure`.

### 1d. Pending operations + the tiered-store bug

`CachedStorage` (`persistent_set.cljc:324`) entangles THREE concerns:
1. **read cache** (LRU of decoded nodes),
2. **write buffer** — `pending-writes` atom; `IStorage.store` appends `[addr node]` instead
   of writing konserve; drained at commit by `writing.cljc/write-pending-kvs!`,
3. **free/GC bookkeeping** — `freed-set` / `freed-addresses` / `freelist`; `markFreed`
   records addresses (no konserve dissoc).

The **tiered store** (`konserve.tiered/TieredStore`: memory frontend + durable backend) routes
every op by a *store-wide* policy. PSS embeds exactly ONE `_storage` and threads it into every
`store`/`markFreed` (`PersistentSortedSet.java`, `Branch.java`), so per-node tier intent can't be
expressed. The observed breakage:
- GC marks against the live (frontend-ish) tree but `sweep!` enumerates `-keys`, which under
  `:frontend-first` returns **backend keys only** → nodes that are only in the frontend
  (write-behind lag / read-populate) are invisible to the sweep set.
- `markFreed` can't target a tier; GC's `-dissoc` deletes from **both** tiers (correct for a
  dead node, but there's no flush barrier guaranteeing both tiers agree first).

Root cause is structural (single storage authority per set). yggdrasil's GC is immune because
it's a pure reachability sweep that never relies on per-node `markFreed` tier routing.

### 1e. konserve extension points (for an overlay)

- One composable-wrapper precedent: `TieredStore` (a `defrecord` wrapping an inner store,
  re-implementing every store protocol; propagates serializers via `-assoc-serializers`).
- `konserve.cache` caches **reads** only; **no** existing write-buffer / dirty-set / deferred
  flush anywhere — a buffered overlay is new ground (favorable; nothing to collide with).
- Primitives a flush would use: `multi-assoc`/`multi-dissoc` (atomic batch, gated by
  `multi-key-capable?`; IndexedDB + memory support it), `PWriteHookStore` (post-write seam),
  per-key `go-locked`. A `:backend :buffered` overlay registers via `defmethod -connect-store`
  exactly like `:tiered`. Everything stays `.cljc` under `async+sync`.

## 2. Target architecture (decomplected)

```
PSS set
 └─ IStorage = CachedStorage         ← read-cache + content-addressing + free-tracking ONLY
      └─ [BufferedStore]   (konserve overlay, optional)   ← write batching + explicit flush!
           └─ [TieredStore] (optional)                    ← memory frontend + durable backend
                └─ DefaultStore (file / indexeddb / …)
```

Serialization: **ONE canonical PSS node+root handler set** (from persistent-sorted-set),
used for BOTH in-store and wire. Storage + comparator resolved per-read via the canonical
`root-read-handler`:
- `:resolve-storage (comp pss-fress/registered-store :store-id)` — store registered under its
  `(:id store-config)` after build; replaces BOTH the closed-over atom and the wire registry.
- `:resolve-cmp (fn [meta] (index-type->cmp-quick (:index-type meta) false))`.
- `meta` gains `:store-id` alongside the existing `:index-type` (stamped at index build).

The DB / TxReport handlers stay datahike-specific (consumer record handlers, not PSS nodes) —
only the NODE + ROOT codec is shared, per the canonical-handlers scope rule.

## 3. Proposed sequencing (two independent, individually-shippable phases)

**Phase 1 — unify the handlers + delete the mutable hack (low risk, well-understood).**
- Point both in-store (`persistent_set.cljc`) and wire (`kabel/fressian_handlers.cljc`) at the
  canonical `write-handlers`/`read-handlers`/`root-write-handlers`/`root-read-handler`.
- Stamp `:store-id` into index meta; register/unregister the storage in `pss-fress/store-registry`
  at store connect/release.
- Dual-tag read: register the canonical handlers ALSO under the legacy
  `datahike.index.PersistentSortedSet[.Leaf/.Branch]` tags (old DBs read unchanged); new writes
  emit `pss/leaf`/`pss/branch`/`pss/set`. Fixes the wire subtree-count drift for free.
- Net: two handler sets → one; closed-over `(atom nil)` + the separate wire registry both gone;
  storage resolution identical in-store and on the wire.
- Verify: full datahike JVM + cljs suites; the kabel integration + browser tests; an
  old-format-read test; a server↔client sync round-trip.

**Phase 2 — pending-ops → konserve overlay + fix the tiered bug (higher risk; design-gated).**
- Extract write-buffering from `CachedStorage` into a konserve `BufferedStore` overlay (dirty
  set + `flush!` via `multi-assoc`, modeled on `TieredStore`). `CachedStorage.store` writes
  through (immediately) to the buffered store; datahike's commit calls one `flush!` barrier.
- The tiered bug then reduces to a **flush-barrier + sweep-enumeration** fix, likely WITHOUT a
  PSS change: GC must run after a full flush (both tiers durable), and the sweep key set must be
  the post-flush backend (or the tier union). To verify whether per-tier `markFreed` routing is
  *actually* needed, or whether a flush barrier + write-through-before-GC suffices.
- This phase is separable; Phase 1 ships and de-risks the bulk of the cleanup regardless.

## 4. Open decisions (need a call before Phase 1 lands)

1. **Storage resolution: global `store-registry` vs per-store `delay`.** The registry unifies
   in-store + wire and matches the canonical design, but adds lifecycle (register on connect,
   unregister on release — leak risk if missed). A per-store `(delay (create-storage …))` keeps
   it local with no global state but does NOT unify with the wire (wire genuinely needs a
   multi-store registry). Leaning registry for the unification the task asks for.
2. **Phase 2 now or later.** Recommend Phase 1 first (clean, shippable), Phase 2 as a
   separately-designed change once the bug repro is pinned and the flush-barrier hypothesis
   tested. Avoids coupling a risky refactor to the straightforward unification.
3. **Tiered-bug fix shape (Phase 2).** Flush-barrier + write-through-before-GC (no PSS change)
   vs threading a tier hint through PSS `IStorage` (lib change, last resort). Determine empirically.
```

---

# FINAL DESIGN (agreed) — canonical PSS codec for storage AND wire, scope-by-store-id

This supersedes the registry sketch in §2–§3 above. The decisions below are settled; the
implementation is staged in the next section.

## D1. Decisions taken

- **Typed handlers, one canonical codec, used for storage AND the kabel wire (Option 2).** Plain
  data on the wire (Option 1) was rejected: the fressian *tags* are exactly what make a nested
  heterogeneous value self-describing (which sub-value is a PSS root vs a datom vs a CRDT) — that
  is the whole reason read/write handlers exist. Dropping types would force out-of-band structural
  knowledge to know where to reconstruct.
- **A root carries `:store-id` in its meta to retain scope.** Storage is a *live, unserializable*
  object, so a deserialized root can only find it by id at runtime. The same id resolves the *whole*
  reconstruction context.
- **Registry value is a Scope, not just storage:** `store-id → {:storage :settings :resolve-cmp}`.
  Settings comes from this one lookup (don't serialize it into the root — that would let the root's
  settings diverge from its nodes'; resolving both from one id keeps them provably identical).
  Settings *data* (`branching-factor`, `diff-buf-size`) lives in the **db config**; functions
  (comparator via `:index-type`, measure-ops in settings) are resolved by id, never serialized.
- **Asymmetry roots vs nodes:** roots carry `:store-id` → resolve full scope. Nodes carry no id and
  never travel *inside a value* (they lazy-load store-locally from the storage the root resolves).
  On raw konserve-sync replication a node *does* cross, but only transiently — it is re-stored
  byte-equivalently (node write is settings-independent) and re-read with the destination store's
  real settings. So a node's wire settings is irrelevant; correctness is always store-local.

## D2. Why this carries any PSS project over one socket (incl. cross-system values)

- konserve-sync ships **deserialized** values (`k/get` → wire → `k/assoc`); kabel has **one
  serializer per peer** (`peer.cljc:78`). So typed values must round-trip through that one
  serializer.
- Because the canonical handlers are **one-per-type and shared**, a single peer carries nodes/roots
  from datahike + yggdrasil + proximum + stratum without tag collisions. A nested composite (e.g. a
  **yggdrasil CRDT containing a datahike PSS root**) deserializes in one recursive pass: the CRDT
  record handler → its element is a datahike root (`pss/set`, `:store-id` = datahike store) →
  `root-read-handler` resolves *that* store's scope → a live, lazy datahike set; the CRDT's own PSS
  parts resolve the yggdrasil scope. Reads later pull nodes lazily from each set's own store.
- **Precondition (documented, honest):** a peer can only reconstruct values for stores it has
  registered, and can only deserialize element types whose handlers it carries. So a multi-system
  peer registers the **union** of participants' element/record handlers (one per type) + their
  scopes. With that, transparency holds; without a participant's handler, its nodes fail with an
  unknown-tag error. PSS exposes a bundle builder so assembling such a peer is a `merge` of element
  maps over the canonical base.

## D3. The PSS API (what enables it)

```clojure
;; scope = {:storage <IStorage> :settings <Settings> :resolve-cmp (fn [meta] -> Comparator)}
(defonce scope-registry (atom {}))                 ; store-id -> scope
(register-scope! [id scope]) (registered-scope [id]) (unregister-scope! [id])
(def ^:const store-id-key :store-id)               ; canonical meta key

write-handlers  (read-handlers settings)           ; node codec — write settings-INDEPENDENT
root-write-handlers                                 ; root -> {:meta :address :count}
(root-read-handler {:resolve-scope <fn meta->scope>}) ; default: registry by (:store-id meta)
;;   back-compat: {:settings :resolve-storage :resolve-cmp} builds a fixed scope (single-store)

(canonical-read-handlers  {:settings :resolve-scope :element-read-handlers})  ; node+root+elements
(canonical-write-handlers {:element-write-handlers})
```

- **Two serializers, each single-purpose — NO registry-first/fixed-fallback hybrid.** Not forcing a
  singleton lives in the *peer* serializer (which genuinely serves many stores), not in a hybrid:
  - **In-store serializer** (per konserve store): only ever reads ITS OWN store's blobs, whose roots
    belong to that same store, so it resolves to **this store, full stop** — `:resolve-scope (fn [_]
    (registered-scope this-store-id))`, where `this-store-id = (:id config)` is captured at attach
    and the scope is `register-scope!`d at connect. No `delay`, no duplicate storage, and it ignores
    the root's `:store-id` — so **old DBs (no store-id) read fine**. Different-settings stores are
    different konserve stores with their own serializers; that's per-store, not a forced singleton.
  - **kabel peer serializer** (assembled at the wire layer): resolves by `:store-id` from the
    registry (`:settings` a default — node settings is transient on the wire; `:element-read-handlers`
    = union of participants'). This is where many stores meet.
- `:store-id` in a root's meta is needed **only for the wire** (so a synced/cross-store root finds the
  receiver's scope). New writes stamp it. The lone edge — an *old* DB (no store-id) that is then
  *synced* — is handled by stamping `:store-id` onto the in-memory roots at connect; in-store reads
  are unaffected either way. Each store `unregister-scope!`s on release.

## D4. Implementation stages (each compiles + tests on its own)

- **Stage 0 — PSS foundation.** Scope registry + `store-id-key`; `root-read-handler` resolves the
  full scope via `:resolve-scope` (registry default; keep `:settings`/`:resolve-storage` as a
  back-compat fixed scope so the already-green consumers don't break mid-flight);
  `canonical-read/write-handlers` bundles. Tests JVM+cljs: fixed-scope round-trip (regression);
  **two scopes in the registry, a composite value with a root from each store** resolving to the
  right storage/settings/cmp + lazy-loading from the right store; a cross-system *shape* (a wrapper
  record holding two roots + a custom element type off one handler set).
- **Stage 1 — proximum + stratum** (local single-store): switch root-read to the `:resolve-scope`
  thunk; keep dual-tag legacy read. Gate: 175 / 1419 green.
- **Stage 2 — yggdrasil** (local + wire): local serializer fixed scope; register scope under
  store-id; stamp `:store-id` into meta of sets that can ride a CRDT value. Gate: JVM + cljs-node.
- **Stage 3 — datahike Phase 1:** config `:branching-factor`/`:diff-buf-size` → `Settings`; stamp
  `:store-id (:id config)` in index meta; `add-konserve-handlers` registers the scope + attaches the
  canonical bundle (fixed scope in-store) and **drops the `(atom nil)`**; release unregisters; kabel
  `fressian_handlers` → canonical wire bundle (registry resolve, default node settings, merged
  Datom/DB/TxReport), **deleting** the deferred-map machinery + the second registry; dual-tag legacy
  read. `gen-address` is serialization-independent ⇒ no data migration. Gate: full JVM + cljs +
  kabel integration + browser + old-format read + server↔client sync.
- **Stage 4 — decisive cross-system test:** a yggdrasil CRDT carrying a datahike root over a real
  kabel ws, both stores registered; each root resolves to its own store + lazy-loads.
- **Stage 5 — release:** `data.fressian`/`fress` `provided` in PSS `template/pom.xml`; release the
  PSS branch; re-pin all four consumers.

**Out of scope for this arc** (separate, repro-gated): the pending-ops → konserve `BufferedStore`
overlay and the tiered-store GC bug (§Phase 2 above). This arc is serialization/scope only.

**Perf:** no regression — same per-node serialization work; an O(1) registry lookup per *root*
only; settings built once at connect.

