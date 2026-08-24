(ns datahike.gc
  (:require [clojure.set :as set]
            [datahike.config :as dc]
            [datahike.constants :as c]
            [datahike.datom :as dd]
            [datahike.gc-guard :as guard]
            [datahike.gc-roots :as roots]
            [datahike.index.interface :refer [-mark -seed-root! -slice with-storage]]
            [datahike.index.secondary :as sec]
            [datahike.schema :as schema]
            [konserve.core :as k]
            [konserve.gc :refer [sweep!]]
            [replikativ.logging :as log]
            ;; `async+sync` and the superv operators are MACROS: ClojureScript
            ;; needs :refer-macros for them, while `S` is an ordinary var needing
            ;; a plain :refer on both. Written as two whole libspecs rather than
            ;; with a reader conditional on the OPTION key, because
            ;; `#?(:clj :refer :cljs :refer-macros)` beside a second `:refer`
            ;; expands to a duplicate `:refer` on the JVM.
            #?(:clj  [konserve.utils :as ku :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :as ku
                      :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            #?(:clj  [superv.async :refer [<? S go-try <<? go-try- <?-]]
               :cljs [superv.async :refer [S]
                      :refer-macros [<? go-try <<? go-try- <?-]])
            ;; go-loop drives start-background-gc!'s scheduler; it's a MACRO, so
            ;; cljs needs it via :require-macros (mirrors datahike.versioning).
            #?(:clj  [clojure.core.async :as async :refer [go-loop]]
               :cljs [clojure.core.async :as async])
            [datahike.schema-cache :as sc])
  #?(:clj  (:import [java.util Date])
     :cljs (:require-macros [clojure.core.async :refer [go-loop go]])))

;; meta-data does not get passed in macros
(defn get-time [d]
  (.getTime ^Date d))

(defn- attr-store-refs
  "The object ids named by the VALUES of `attr` in the AEVT index `aevt`. For a
   key-bearing value type THE VALUE IS THE KEY, so this is just the attribute's
   values.

   Slices exactly the attribute's range, so the cost is O(its datoms), not O(the
   database)."
  [aevt attr]
  (into #{}
        (map :v)
        (-slice aevt
                (dd/datom c/e0 attr nil c/tx0)
                (dd/datom c/emax attr nil c/txmax)
                :aevt)))

(defn- store-refs
  "The object ids this record's datom VALUES name — what a `:db.type/store-ref`
   keeps alive (`datahike.schema/key-bearing-value-types`).

   THE MARK DOES NOT SEE THESE OTHERWISE. It walks the index TREES and collects
   node addresses; it never looks inside a datom's value. So an object named only
   by a value is unreachable from the mark's point of view, and the sweep deletes
   it. That is the whole reason this exists.

   ZERO COST WHEN UNUSED: if the schema declares no attribute of a key-bearing
   type — which is every database that does not use the feature — this returns
   immediately, having sliced nothing.

   Under `:keep-history?` the TEMPORAL index is scanned too: a retracted store-ref
   datom is still readable `as-of` an earlier tx, so the object it names must
   outlive the retraction. (`:db/noHistory` attributes are NOT retained there, which
   is why `schema/key-bearing-misuse` refuses to combine the two.)"
  [config schema ident-ref-map aevt taevt]
  (let [attrs (into []
                    (keep (fn [[ident attr-def]]
                            (when (contains? schema/key-bearing-value-types
                                             (:db/valueType attr-def))
                              ;; With :attribute-refs? the datoms hold the attribute's
                              ;; EID, not its ident — slice by what is actually stored.
                              (if (:attribute-refs? config)
                                (get ident-ref-map ident)
                                ident))))
                    schema)]
    (if (empty? attrs)
      #{}
      (reduce (fn [acc attr]
                (cond-> (set/union acc (attr-store-refs aevt attr))
                  taevt (set/union (attr-store-refs taevt attr))))
              #{} attrs))))

(defn- reachable-in-branch [store branch after-date config schema-cache opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (let [head-cid (<?- (k/get-in store [branch :meta :datahike/commit-id] nil opts))]
      (loop [[to-check & r] [branch]
             visited        #{}
             reachable      #{branch head-cid}
             refs           #{}]
        (if to-check
          (if (visited to-check) ;; skip
            (recur r visited reachable refs)
            (if-let [record (<?- (k/get store to-check nil opts))]
              (let [{:keys                         [eavt-key avet-key aevt-key
                                                    temporal-eavt-key temporal-avet-key temporal-aevt-key
                                                    eavt-root aevt-root avet-root
                                                    temporal-eavt-root temporal-aevt-root temporal-avet-root
                                                    schema-meta-key secondary-index-keys]
                     {:keys [datahike/parents
                             datahike/created-at
                             datahike/updated-at]} :meta}
                    record
                    in-range? (> (get-time (or updated-at created-at))
                                 (get-time after-date))]
                (let [sec-reachable (when (seq secondary-index-keys)
                                      (reduce-kv
                                       (fn [acc _idx-ident key-map]
                                         (set/union acc (sec/mark-from-key-map key-map store)))
                                       #{} secondary-index-keys))
                          ;; Stored roots are storage-detached; bind them to
                          ;; this store's storage so -mark can walk the tree.
                          ;; Root fusion: inlined roots aren't separate konserve
                          ;; objects, so -mark on the lazy index would try to
                          ;; restore the root by address and fail. Seed the
                          ;; inlined root into the with-storage COPY (owned,
                          ;; unpublished) — never into the stored record's
                          ;; index, which may be shared through the store's
                          ;; cache (mirrors stored->db) — so walk-addresses
                          ;; uses it and only its children are fetched.
                          ;;
                          ;; `bind` returns that copy so the SAME seeded instance
                          ;; serves both -mark (walk the tree) and -slice (read the
                          ;; datoms, for store-refs) — seeding a second one would
                          ;; duplicate the work and re-open the shared-record hazard
                          ;; above.
                      bind (fn [idx root]
                             (cond-> (with-storage (:index config) idx (:storage store))
                               root (-seed-root! root)))
                      aevt'  (bind aevt-key aevt-root)
                      taevt' (when (:keep-history? config)
                               (bind temporal-aevt-key temporal-aevt-root))
                            ;; The schema names which attributes can hold store-refs.
                            ;; It is content-addressed and rarely changes, so memoize
                            ;; it across the whole collection rather than re-reading
                            ;; it for every commit in the window.
                      schema-meta (when schema-meta-key
                                    (if-let [cached (get @schema-cache schema-meta-key)]
                                      cached
                                      (let [sm (<?- (k/get store schema-meta-key nil opts))]
                                        (swap! schema-cache assoc schema-meta-key sm)
                                        sm)))
                            ;; Mirror stored->db's schema fallback so gc reads the
                            ;; schema exactly as the db reconstructs it. This does NOT
                            ;; guard store-refs: `(:schema record)` is non-nil only for
                            ;; old inline-schema databases, which predate
                            ;; :db.type/store-ref and so declare no key-bearing
                            ;; attribute (store-refs → #{} regardless).
                      schema (or (:schema schema-meta) (:schema record))
                            ;; Kept SEPARATE from the node addresses, not folded in.
                            ;; A store-ref names an object; it does NOT say where the
                            ;; bytes live. If they are in this konserve store, the
                            ;; sweep protects and reclaims them (gc-storage! unions
                            ;; these in). If they are somewhere else — a raw S3 prefix
                            ;; the browser uploads to directly, a CDN — the sweep here
                            ;; can do nothing with them, but `reachable-store-refs`
                            ;; hands the set to the application, which knows how to
                            ;; delete from wherever it put them.
                      record-refs (if schema
                                    (store-refs config schema
                                                (:ident-ref-map schema-meta) aevt' taevt')
                                    #{})
                      new-reachable (cond-> (set/union reachable #{to-check}
                                                       (when schema-meta-key #{schema-meta-key})
                                                       (-mark (bind eavt-key eavt-root))
                                                       (-mark aevt')
                                                       (-mark (bind avet-key avet-root)))
                                      (:keep-history? config)
                                      (set/union (-mark (bind temporal-eavt-key temporal-eavt-root))
                                                 (-mark taevt')
                                                 (-mark (bind temporal-avet-key temporal-avet-root)))
                                      sec-reachable
                                      (set/union sec-reachable))]
                  (recur (concat r (when in-range? parents))
                         (conj visited to-check)
                         new-reachable
                         (set/union refs record-refs))))
                    ;; Record absent: already swept by an earlier pass with a
                    ;; narrower window, or the store runs :commit-graph? false
                    ;; and never persisted it. Lineage ends here — nothing to
                    ;; mark. (Without this guard the nil destructure NPEs at
                    ;; get-time.)
              (recur r (conj visited to-check) reachable refs)))
          {:reachable reachable :store-refs refs}))))))

(def ^:const DEFAULT_SWEEP_MIN_AGE_MS
  "No floor by default: OFF, so in-process collection behaves exactly as it always
   has.

   Not a hedge — where the writer lives, `safe-point` is EXACT, so a wall-clock
   floor on top of it can only retain garbage the collector was right about. The
   floor is not a safety margin for that case, it is a SUBSTITUTE for information
   that a different process does not have.

   And the substitute cannot have a useful default: the value has to exceed the
   longest values-then-pointer window a deployment's writers can have, which is a
   property of the deployment and not of datahike. A default large enough to be
   meaningful would be wrong for most stores and would silently slow reclamation
   for everyone; a default small enough to be harmless would not protect anyone
   while looking as though it did.

   So it is opt-in, and collecting from outside the writer process without it is
   warned about by name. See [[gc-storage!]]."
  0)

(defn gc-storage!
  "Invokes garbage collection on the database by whitelisting currently known branches.
  All db snapshots on these branches before remove-before date will also be
  erased (defaults to beginning of time [no erasure]). The branch heads will
  always be retained.

  The sweep deletes an unreachable object only if it was written before this
  store's SAFE POINT (`datahike.gc-guard/safe-point`) — NOT before `now`.

  WHY: a commit writes every value the new head references and only THEN flips the
  branch head (`datahike.writing/commit!`; the barrier invariant). For the duration
  of that sequence the commit's objects are on disk and reachable from NOTHING —
  the head still names the previous snapshot — while ALREADY being older than
  `now`. A mark that runs inside the window calls them garbage and a `now` cutoff
  deletes them; the head then lands pointing at deleted objects. The safe point is
  the instant before which that verdict is final, and it is exact: it is `now` when
  nothing is in flight, and the START of the oldest in-flight sequence otherwise.
  `branch!`, `force-branch!` and `create-database` take the same guard, so this
  holds for them too, though they never touch the writer.

  Note the safe point is NOT `remove-before`. `remove-before` is a MARK-side policy
  (how far back the commit graph stays reachable — how much history you keep) and
  makes GC collect MORE. The safe point is a SWEEP-side bound (how recently written
  an object may be and still be judged) and makes it collect LESS.

  PREFER TO RUN IT WHERE THE WRITERS ARE. The safe point is in-process state: it is
  exact for every writer in this JVM and blind to every other. With a single
  exclusive writer (`:writer-ownership :exclusive`) that is the whole picture, and
  `d/gc-storage` is a writer op, so it is already in the right place.

  COLLECTING FROM ANOTHER PROCESS — a cron sidecar, an offline job against the bucket
  — is possible, but ONLY because of `:min-age-ms`, and only if you choose that number
  deliberately. Such a collector cannot observe the writer's in-flight sequences: its
  `safe-point` reports `now` because ITS heap is idle, not because the store is quiet.
  `:min-age-ms` replaces that missing information with a wall-clock bound — spare
  anything written within the last N — so it must EXCEED THE LONGEST VALUES-THEN-POINTER
  WINDOW any writer can have.

  Sizing it: a writer that AWAITS ITS TRANSACTS has no such window open once the call
  returns, so the bound is one request — 15 minutes on AWS Lambda, which makes hours
  orders of magnitude conservative. A writer that dispatches a transaction and returns
  WITHOUT awaiting it breaks that reasoning, and so does a suspended process (see
  issue #960: a frozen Lambda resumes mid-sequence arbitrarily later). The price of a
  generous value is only delayed reclamation.

  A duration and not an instant, deliberately: the cutoff is compared against konserve's
  `:last-write` stamps, which come from its monotonic write clock, and a wall-clock
  `Date` from the caller is not comparable to those in a way anything would detect.

  SHARED WRITERS (`:writer-ownership :shared`, the default since #959) are exactly the
  other-process case: head fencing (#963) keeps two processes from losing each other's
  commits, but a fence guards the pointer, not the values — the objects of a commit in
  flight elsewhere are on disk and reachable from nothing, and this collector cannot
  see them. A collection under shared ownership therefore needs the floor as well.
  Readers are unconstrained."
  ([db] (gc-storage! db (#?(:clj Date. :cljs js/Date.) 0) nil))
  ([db remove-before] (gc-storage! db remove-before nil))
  ([db remove-before {:keys [min-age-ms]}]
   (go-try S
           (let [{:keys [config store]} db
                 store-id (:id (:store config))
                 _ (when (guard/read-in-flight? store-id)
                     (log/raise "Garbage collection is deferred while a secondary index scans an older database snapshot."
                                {:type :gc/read-lease-active
                                 :store-id store-id}))
                 min-age-ms (or min-age-ms DEFAULT_SWEEP_MIN_AGE_MS)
                 ;; Cutoff from konserve's monotonic write clock — the SAME
                 ;; source that stamps :last-write. Strictly increasing, so a
                 ;; cutoff acquired after a write is strictly greater than the
                 ;; write's stamp (no same-ms ties), and a wall-clock retreat
                 ;; cannot classify an in-flight commit's objects as old.
                 now (ku/now)
                 ;; Capture `now` BEFORE reading the guard: a sequence that opened
                 ;; and closed between the two reads has landed its pointer, and the
                 ;; mark (which runs after) sees it. Reading the guard first would
                 ;; miss a sequence that opens in between.
                 ;; The FLOOR: spare anything written within `min-age-ms`
                 ;; regardless of what the guard says. `min` is the safe
                 ;; direction — an earlier cutoff sweeps LESS — so this can only
                 ;; ever retain garbage, never delete a live object. In this
                 ;; process the safe point is the tighter bound and wins; outside
                 ;; it, where the safe point degenerates to `now` because this
                 ;; heap has no in-flight sequences to report, the wall clock is
                 ;; the only bound there is.
                 floor (#?(:clj Date. :cljs js/Date.) (- (get-time now) min-age-ms))
                 cutoff (->> [(guard/safe-point store-id) now floor]
                             (sort-by get-time)
                             first)
                 _ (log/debug :datahike/gc-start {:time now :cutoff cutoff :min-age-ms min-age-ms})
                 ;; The condition that matters is not which writer backend this
                 ;; config names — a second process connecting to the same store
                 ;; gets a `:self` writer by default and looks like the writer —
                 ;; it is whether anything in THIS process has ever written here.
                 ;; If not, `safe-point` is `now` because this heap is idle, not
                 ;; because the store is quiet, and only `min-age-ms` is holding
                 ;; the line.
                 _ (when-not (guard/ever-guarded? store-id)
                     (if (pos? min-age-ms)
                       (log/info :datahike/gc-outside-writer-process
                                 {:min-age-ms min-age-ms
                                  :note "collecting a store this process has never written: the sweep is bounded by :min-age-ms alone"})
                       (log/warn :datahike/gc-outside-writer-process
                                 {:min-age-ms min-age-ms
                                  :note (str "collecting a store this process has never written, and with no sweep floor. "
                                             "Another process's in-flight commit is invisible here — its objects are on disk, "
                                             "reachable from nothing yet, and this sweep can delete them. "
                                             "Set :min-age-ms above the longest values-then-pointer window your writers can have.")})))
                 _ (sc/clear-write-cache (:store config)) ; Clear the schema write cache for this store
                 branches (<? S (k/get store :branches))
                 _ (log/trace :datahike/gc-retain-branches {:branches branches})
                 ;; Durable roots (see `datahike.gc-roots`): reap what has expired
                 ;; FIRST so a dead holder's record is not marked in this cycle,
                 ;; then walk each surviving root's record exactly like a branch
                 ;; head — same seed shape, same walk.
                 live-roots (<? S (roots/reap-expired! store {:sync? false}))
                 root-keys (mapv roots/record-key (keys live-roots))
                 _ (when (seq live-roots)
                     (log/trace :datahike/gc-retain-roots {:root-count (count live-roots)}))
                 ;; shared across branches: the schema is content-addressed, so
                 ;; every commit that did not change it names the SAME object
                 schema-cache (atom {})
                 ;; `{:sync? false}` explicitly: `reachable-in-branch` is
                 ;; `async+sync` now, and `gc-storage!` is an async-only `go-try`,
                 ;; so it takes the channel branch. Passing opts is not optional —
                 ;; omitting it called a 6-arg function with 5 and broke the
                 ;; collector, which is how `background-gc-test` started hanging.
                 walked (->> (concat branches root-keys)
                             (map #(reachable-in-branch store % remove-before config
                                                        schema-cache {:sync? false}))
                             async/merge
                             (<<? S))
                 ;; Store-refs are unioned into the whitelist here. For an object that
                 ;; lives in THIS store that means the sweep spares it (and reclaims it
                 ;; once no datom names it). For an object that lives elsewhere it is a
                 ;; harmless no-op — whitelisting a key the store does not have does
                 ;; nothing — and `reachable-store-refs` is how the application gets at
                 ;; the same set to sweep wherever it actually put the bytes.
                 reachable (-> (apply set/union (map :reachable walked))
                               (set/union (apply set/union (map :store-refs walked)))
                               (conj :branches)
                               (conj roots/registry-key))]
             (log/trace :datahike/gc-reachable {:reachable-count (count reachable)
                                                :cutoff cutoff})
             (<? S (sweep! store reachable cutoff))))))

(defn reachable-store-refs
  "The set of `:db.type/store-ref` values the database still names — its live blob
   ids — across ALL branches, honouring `remove-before` and `:keep-history?`.

   THE MARK, WITHOUT THE SWEEP. A store-ref names an object; it does not say where
   the bytes live, and that is deliberate:

     - IN THIS KONSERVE STORE — `gc-storage!` already spares and reclaims them.
       Portable across every backend, and `delete-database` erases them with the
       database. konserve frames a binary value as [header][meta][payload], so the
       bytes are not raw at that key: reads and writes go through `k/bget`/`k/bassoc`.
       Streamable (e.g. `:file`), so heap stays flat even for large files — but the
       bytes proxy through your JVM (client → server → store) and you forgo S3-native
       range, resumable multipart, and CDN. Fine at moderate size; S3-direct is the
       only thing that scales.

     - ANYWHERE ELSE — a raw S3 prefix the browser PUTs to with a presigned URL and
       a content-type, a CDN, another bucket. The bytes never transit your JVM,
       which is the whole point. Datahike cannot delete from there and does not
       pretend to: it hands you the live set, and you sweep.

   That is the entire contract for external blobs — datahike owns the hard half (what
   is still referenced, including from retained history and other branches); you own
   the easy half (list your prefix, delete what is not in this set):

     (let [live (<?? S (gc/reachable-store-refs @conn))]
       (doseq [obj (list-objects bucket (str \"tenant/\" tid \"/blobs/\"))]
         (when-not (live (blob-id obj))
           (delete-object bucket obj))))

   Retention comes for free: pass `remove-before` and objects named only by commits
   older than it drop out of the set, exactly as index nodes do."
  ([db] (reachable-store-refs db (#?(:clj Date. :cljs js/Date.) 0) {:sync? false}))
  ([db remove-before] (reachable-store-refs db remove-before {:sync? false}))
  ([db remove-before opts]
   ;; `async+sync` so a caller that is ITSELF synchronous — `migrate`'s blob
   ;; export, which has to run on both platforms — can take the value directly
   ;; instead of blocking on a channel. The 1- and 2-arities keep the async
   ;; default, so every existing caller is unaffected.
   ;;
   ;; The branch walks are SEQUENTIAL now, where they used to fan out through
   ;; `async/merge`. One shape has to serve both modes and a channel merge has no
   ;; synchronous counterpart; the result is identical, and the concurrency was
   ;; across BRANCHES, of which a database normally has one. If a many-branch
   ;; collection ever measures slow, the fan-out belongs in the async branch
   ;; explicitly rather than as an accident of how this was first written.
   (async+sync
    (:sync? opts) *default-sync-translation*
    (go-try-
     (let [{:keys [config store]} db
           branches (<?- (k/get store :branches nil opts))
           ;; A blob named only by a rooted commit is live too: the sweep spares
           ;; it, so the set the application sweeps its own storage against must
           ;; include it. Read-only here — reaping is the collector's job.
           root-keys (map roots/record-key (keys (<?- (roots/roots store opts))))
           schema-cache (atom {})]
       (loop [bs (seq (concat branches root-keys)) acc #{}]
         (if (nil? bs)
           acc
           (recur (next bs)
                  (set/union acc
                             (:store-refs (<?- (reachable-in-branch
                                                store (first bs) remove-before
                                                config schema-cache opts))))))))))))

(defn record-store-refs
  "The store-ref blob keys named by the datom VALUES in a SINGLE stored-db record —
   its AEVT, plus temporal AEVT when history is retained. This is the `store-refs`
   slice `gc-storage!` runs, exposed for one branch head.

   WHY IT IS PUBLIC. A reachability walker — the garbage collector here, and
   `konserve-sync`'s replication walker equally — discovers keys by walking the index
   TREES; it never looks inside a datom's value. So a blob named only by a
   `:db.type/store-ref` value is invisible to it: the collector would sweep it (fixed
   by `store-refs`), and the sync walker would never SHIP it — a subscriber ends up
   with a live datom pointing at an object that never replicated. The datahike sync
   walker unions this in per branch head to close that gap, exactly as the mark does.

   PER-RECORD, not per-history: it returns the refs in THIS head's index — precisely
   the set a walker ships for that head (under `:keep-history?` the temporal index
   already carries the retained ones). `reachable-store-refs` is the other shape — it
   walks the commit graph for GC retention. Don't confuse them: sync wants this one.

   Needs only the store and the record — no connection. The record carries its own
   `:config` (`:index`, `:attribute-refs?`), so nothing is inferred: under
   `:attribute-refs?` the datoms hold attribute EIDs and the slice keys off the
   `:ident-ref-map` accordingly. Schema comes from the record's `:schema-meta-key`
   (falling back to an inline `:schema`); the temporal index is sliced when the record
   retains one. `index-type` overrides the record's index if given."
  ([store stored-db] (record-store-refs store stored-db nil))
  ([store stored-db index-type]
   (go-try S
           (let [rec-config    (:config stored-db)
                 index-type    (or index-type (:index rec-config) dc/*default-index*)
                 schema-meta   (when-let [k (:schema-meta-key stored-db)]
                                 (<? S (k/get store k)))
                 schema        (or (:schema schema-meta) (:schema stored-db))
                 ident-ref-map (:ident-ref-map schema-meta)
                 config        {:index index-type
                                :attribute-refs? (:attribute-refs? rec-config)}
                 storage       (:storage store)
                 bind          (fn [idx root]
                                 (cond-> (with-storage index-type idx storage)
                                   root (-seed-root! root)))
                 aevt          (bind (:aevt-key stored-db) (:aevt-root stored-db))
                 taevt         (when (:temporal-aevt-key stored-db)
                                 (bind (:temporal-aevt-key stored-db)
                                       (:temporal-aevt-root stored-db)))]
             (if schema
               (store-refs config schema ident-ref-map aevt taevt)
               #{})))))

(defn start-background-gc!
  "Runs `gc-storage!` on `conn`'s database periodically in the background and
   returns a zero-arg stop function. EXPERIMENTAL.

   Unlike the freed-address online GC (`datahike.online-gc`), this is a full
   reachability collection: it works with MULTIPLE BRANCHES, is insensitive to
   how garbage was produced (it needs no mutation-time tracking), and also
   prunes commit-graph records when a history window is set. Its cost is a walk
   of all reachable nodes per cycle, so prefer the online GC's freed-tracking for
   single-branch bulk imports where write amplification dominates; the two
   compose (this collector reclaims what freed-tracking cannot see).

   It runs CONCURRENTLY with the writer, and is safe to do so because the sweep
   stops at the store's SAFE POINT rather than at `now`: an in-flight commit's
   objects are written after that point and are spared (see `gc-storage!`). Like all
   writer-side maintenance, it belongs in the process the writers run in — which is
   datahike's writer model, not a GC-specific rule (`gc-storage!`).

   Options:
     :interval-ms       — cycle period (default 300000 = 5 min).
     :history-window-ms — when set, commits older than (now - window) are
                          pruned from the commit graph each cycle (ranged GC);
                          default nil keeps all history.

   Errors in a cycle are logged and the loop continues; stop with the
   returned function."
  ([conn] (start-background-gc! conn {}))
  ([conn {:keys [interval-ms history-window-ms]
          :or {interval-ms 300000}}]
   (let [stop-ch (async/chan)]
     (go-loop []
       (let [[_ ch] (async/alts! [stop-ch (async/timeout interval-ms)])]
         (when-not (= ch stop-ch)
           (let [remove-before (if history-window-ms
                                 (#?(:clj Date. :cljs js/Date.)
                                  (- (long (ku/monotonic-now-ms))
                                     (long history-window-ms)))
                                 (#?(:clj Date. :cljs js/Date.) 0))
                 res (async/<! (gc-storage! @conn remove-before))]
             (if (instance? #?(:clj Throwable :cljs js/Error) res)
               (log/warn :datahike/background-gc-error {:error res})
               (log/debug :datahike/background-gc-cycle {:swept (count res)})))
           (recur))))
     (fn stop-background-gc! [] (async/close! stop-ch) :stopped))))
