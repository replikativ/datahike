(ns datahike.versioning
  "Git-like versioning tools for Datahike.
   All operations support both synchronous (CLJ default) and asynchronous modes."
  (:require [konserve.core :as k]
            [datahike.connections :refer [delete-connection!]]
            [datahike.gc-guard :as guard]
            [datahike.store :as ds]
            [datahike.writing :refer [stored->db-read-only db->stored stored-db?
                                      commit! create-commit-id get-and-clear-pending-kvs!
                                      write-pending-kvs!
                                      #?@(:clj [prepare-secondary-generations
                                                release-secondary-generations!])]]
            [datahike.writer]
            ;; cljs: S is a VAR (the supervisor) → :refer; go-try-/<?-/<?/go-loop-try
            ;; are MACROS → :refer-macros. (Was missing <? entirely and putting S
            ;; under :refer-macros, so both were :undeclared-var on cljs.)
            #?(:clj  [superv.async :refer [go-try- <?- <? S go-loop-try]]
               :cljs [superv.async :refer [S] :refer-macros [go-try- <?- <? go-loop-try]])
            [datahike.db.utils :refer [db?]]
            [datahike.tools :as dt]
            [replikativ.logging :as log]
            [konserve.utils :refer [#?(:clj async+sync) multi-key-capable? *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            #?(:clj  [clojure.core.async :as async]
               :cljs [clojure.core.async :refer [<!]]))
  #?(:cljs (:require-macros [clojure.core.async :refer [go]])))

(defn- branch-check [branch]
  (when-not (keyword? branch)
    (log/raise "Branch must be a keyword." {:type :branch-must-be-keyword :branch branch})))

(defn- db-check [db]
  (when-not (db? db)
    (log/raise "You must provide a DB value." {:type :db-value-required :db db})))

(defn- parent-check [parents]
  (when-not (pos? (count parents))
    (log/raise "You must provide at least one parent."
               {:type :must-provide-at-least-one-parent :parents parents})))

(defn- commit-id-check [commit-id]
  (when-not (uuid? commit-id)
    (log/raise "Commit-id must be a uuid."
               {:type :commit-id-must-be-uuid :commit-id commit-id})))

(defn- extract-store
  "Extract konserve store from a connection or db value.

   A connection is detected via `IDeref` rather than the concrete
   `Connection` class: `deftype` recompilation (circular loads, REPL
   reloads) can drift the class identity so `instance? Connection` sees a
   stale class and silently misroutes a live connection to the raw-store
   branch — which surfaced as a konserve `get-lock` NPE from
   `commit-as-db`. A db value is not `IDeref`, so the two stay distinct."
  [conn-or-db]
  (cond
    #?(:clj (instance? clojure.lang.IDeref conn-or-db) :cljs (satisfies? IDeref conn-or-db))
    (:store @conn-or-db)

    (db? conn-or-db)
    (:store conn-or-db)

    :else
    ;; Assume it's a raw store
    conn-or-db))

(defn- revision-mismatch? [e]
  (= :konserve/revision-mismatch (:type (ex-data e))))

#?(:clj
   (defn- take-lifecycle!! [awaitable]
     (let [value (async/<!! awaitable)]
       (if (instance? Throwable value)
         (throw value)
         value))))

(defn- revisioned-read-opts [store opts]
  (cond-> opts
    (k/conditional-write-domain store) (assoc :with-revision? true)))

(defn- unpack-revisioned [store result]
  (if (k/conditional-write-domain store)
    result
    [result nil]))

(defn- update-branches!
  "Apply `f` to the shared branch registry without losing a concurrent branch
   creation/deletion. Conditional stores retry from the new value after a CAS
   miss; stores without revisions retain the historical best-effort update."
  [store f opts]
  (let [fenced? (some? (k/conditional-write-domain store))]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (if-not fenced?
                   (second (<?- (k/update store :branches #(f (set %)) opts)))
                   (loop [attempt 0]
                     (let [raw (<?- (k/get store :branches nil
                                           (revisioned-read-opts store opts)))
                           [branches revision] (unpack-revisioned store raw)
                           updated (f (set branches))
                           ;; A nil revision is a key written before konserve had
                           ;; revisions — an upgraded-in-place database. konserve
                           ;; REFUSES `:expected-revision nil` (loudly, by design),
                           ;; and nothing else ever rewrites `:branches`, so passing
                           ;; it through made every branch operation on a legacy
                           ;; database fail forever, with no write that could heal
                           ;; it. Write unconditionally ONCE instead: the write
                           ;; mints a revision and the registry is fenceable from
                           ;; then on — the same self-healing discipline commit!
                           ;; applies to a legacy branch head.
                           result (try
                                    (<?- (k/assoc store :branches updated
                                                  (cond-> opts
                                                    revision (assoc :expected-revision revision))))
                                    updated
                                    (catch #?(:clj Throwable :cljs :default) e
                                      (if (revision-mismatch? e)
                                        ::retry
                                        (throw e))))]
                       (if (= ::retry result)
                         (if (< attempt 63)
                           (recur (inc attempt))
                           (throw (ex-info "Branch registry kept changing; conditional update did not converge."
                                           {:type :datahike/branch-registry-contention
                                            :attempts (inc attempt)})))
                         result))))))))

(defn- assoc-current!
  "Conditionally replace `key` at the revision observed immediately before the
   write. `retry?` gives force-branch! its deliberate last-writer-wins semantics;
   branch creation instead reports that another creator won."
  [store key value opts retry?]
  (let [fenced? (some? (k/conditional-write-domain store))]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (if-not fenced?
                   (do (<?- (k/assoc store key value opts)) nil)
                   (loop [attempt 0]
                     (let [raw (<?- (k/get store key nil
                                           (revisioned-read-opts store opts)))
                           [_ revision] (unpack-revisioned store raw)
                           ;; nil revision = legacy key (see update-branches!):
                           ;; fence when there is a token, write once
                           ;; unconditionally when there is none. An ABSENT key is
                           ;; not this case — it reads as konserve's `absent`
                           ;; sentinel, which is a real token meaning
                           ;; create-if-absent.
                           result (try
                                    (<?- (k/assoc store key value
                                                  (cond-> opts
                                                    revision (assoc :expected-revision revision))))
                                    :written
                                    (catch #?(:clj Throwable :cljs :default) e
                                      (if (revision-mismatch? e)
                                        ::conflict
                                        (throw e))))]
                       (cond
                         (= :written result) nil
                         (and retry? (< attempt 63)) (recur (inc attempt))
                         retry? (throw (ex-info "Branch head kept changing; forced update did not converge."
                                                {:type :datahike/branch-head-contention
                                                 :branch key
                                                 :attempts (inc attempt)}))
                         :else (throw (ex-info "Branch was created concurrently."
                                               {:type :branch-already-exists
                                                :new-branch key}))))))))))

;; ========================= public API =========================

(defn branches
  "List all known branch names. Returns set of keywords."
  ([conn] (branches conn {:sync? true}))
  ([conn opts]
   (let [store (extract-store conn)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try- (<?- (k/get store :branches nil opts)))))))

(defn branch-history
  "Returns the commit history of the branch of the connection in
  form of all stored db values. Performs backtracking and returns dbs in order.
  Always returns a channel."
  [conn]
  (let [{:keys [store] {:keys [branch]} :config} @conn]
    (go-loop-try S [[to-check & r] [branch]
                    visited #{}
                    reachable []]
                 (if to-check
                   (if (visited to-check) ;; skip
                     (recur r visited reachable)
                     (if-let [raw-db (<? S (k/get store to-check))]
                       (let [{{:keys [datahike/parents]} :meta
                              :as db} (stored->db-read-only raw-db store)]
                         (recur (concat r parents)
                                (conj visited to-check)
                                (conj reachable db)))
                       reachable))
                   reachable))))

(defn branch!
  "Create a new branch from commit-id or existing branch as new-branch.
   Secondary indices copy their immutable generation addresses from the source
   commit; no adapter-owned branch pointer is moved."
  ([conn from new-branch] (branch! conn from new-branch {:sync? true}))
  ([conn from new-branch opts]
   (let [opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  ;; GC GUARD: the new branch's head record is written before
                  ;; `:branches` names it. Until then no published root names that
                  ;; record, so a concurrent collector could sweep it.
                  (let [gc-sid   (ds/canonical-store-id
                                  (:store @conn) (get-in @conn [:config :store]))
                        gc-token (guard/writing! gc-sid)]
                    (try
                      (let [store (:store @conn)
                            existing-branches (<?- (k/get store :branches nil opts))
                            _ (when (and existing-branches (existing-branches new-branch))
                                (log/raise "Branch already exists." {:type :branch-already-exists
                                                                     :new-branch new-branch}))
                            stored-db (<?- (k/get store from nil opts))]
                        (when-not (stored-db? stored-db)
                          (throw (ex-info (if (false? (get (:config @conn) :commit-graph? true))
                                            "From does not point to an existing branch, and this store was created with :commit-graph? false — commit records are not persisted, so branching from a commit-id is unavailable; branch from a branch keyword instead."
                                            "From does not point to an existing branch or commit.")
                                          {:type :from-branch-does-not-point-to-existing-branch-or-commit
                                           :from from
                                           :commit-graph? (get (:config @conn) :commit-graph? true)})))
                  ;; A secondary key-map names an immutable generation. Branching
                  ;; therefore copies addresses from the selected stored commit;
                  ;; it never opens a live adapter or moves a native branch ref.
                        (let [schema-meta (when-let [schema-meta-key
                                                     (:schema-meta-key stored-db)]
                                            (<?- (k/get store schema-meta-key nil opts)))
                              source-schema (or (:schema schema-meta)
                                                (:schema stored-db))
                              building? (fn [ident]
                                          (= :building
                                             (get-in source-schema
                                                     [ident :db.secondary/status])))
                              sec-keys (into {}
                                             (remove (fn [[ident _]] (building? ident)))
                                             (:secondary-index-keys stored-db))
                              ;; Key-maps are opaque immutable addresses. Copying
                              ;; them requires no adapter, so CLJS must preserve
                              ;; generations produced by a JVM writer too.
                              branched-sec-keys (not-empty sec-keys)
                              updated-db (cond-> (-> stored-db
                                                     (assoc-in [:config :branch] new-branch)
                                                     (dissoc :secondary-index-keys))
                                           (seq branched-sec-keys) (assoc :secondary-index-keys branched-sec-keys))]
                          ;; A deleted branch leaves its old head behind until GC,
                          ;; so "must be absent" is too strong here. Fence against
                          ;; whichever state (absent or stale head) we observed;
                          ;; another creator of the same name can then win, but can
                          ;; never be overwritten silently.
                          (<?- (assoc-current! store new-branch updated-db opts false))
                      ;; :branches is the POINTER — written last (barrier invariant)
                          (<?- (update-branches! store #(conj % new-branch) opts))))
                      (finally (guard/done! gc-sid gc-token)))))))))

(defn delete-branch!
  "Removes this branch from set of known branches. The branch will still be
  accessible until the next gc. Remote readers need to release their connections."
  ([conn branch] (delete-branch! conn branch {:sync? true}))
  ([conn branch opts]
   (when (= branch :db)
     (log/raise "Cannot delete main :db branch. Delete database instead."
                {:type :cannot-delete-main-db-branch}))
   (let [opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (let [store (:store @conn)]
                    (<?- (update-branches!
                          store
                          (fn [branches]
                            (when-not (contains? branches branch)
                              (log/raise "Branch does not exist." {:type :branch-does-not-exist
                                                                   :branch branch}))
                            (disj branches branch))
                          opts))
                    (delete-connection!
                     [(ds/store-identity (get-in @conn [:config :store])) branch])))))))

(defn force-branch!
  "Force the branch to point to the provided db value. Branch will be created if
  it does not exist. Parents must point to a set of branches or commits.

  WARNING: This deliberately replaces the branch head, like git reset --hard.
  On revisioned stores the replacement is conditionally retried so it cannot
  clobber an update that lands between its read and write. Existing connections
  to this branch will see stale state and must be released and reconnected. Use
  with care — you can render data inaccessible."
  ([db branch parents] (force-branch! db branch parents {:sync? true}))
  ([db branch parents opts]
   (db-check db)
   (branch-check branch)
   (parent-check parents)
   (let [opts (select-keys opts [:sync?])
         sync? (:sync? opts)]
     (async+sync sync? *default-sync-translation*
                 (go-try-
                  ;; GC GUARD: same values-then-pointer sequence as commit!, but this
                  ;; runs on the CALLER's thread and needs no writer at all — which is
                  ;; exactly why the guard lives in the store rather than in the writer.
                  (let [gc-sid   (ds/canonical-store-id
                                  (:store db) (get-in db [:config :store]))
                        gc-token (guard/writing! gc-sid)
                        store (:store db)
                        attempt-id (random-uuid)
                        preparations* (atom {})
                        primary-commit-id* (atom nil)
                        secondary-index-keys* (atom nil)
                        head-write-issued? (atom false)
                        registry-published? (atom false)
                        branch-was-registered? (atom nil)
                        head-before-registration (atom ::not-read)]
                    (try
                      (let [preparation-result
                            #?(:clj
                               (let [prepared (prepare-secondary-generations
                                               db preparations* attempt-id)]
                                 (if sync?
                                   (take-lifecycle!! prepared)
                                   (<?- prepared)))
                               :cljs {:db db :key-maps {}})
                            prepared-db (:db preparation-result)
                            key-maps (:key-maps preparation-result)
                        ;; Seal secondaries first, then compute the audit-grade
                        ;; cid from the stored form that names those exact
                        ;; generations. Same pattern as writing/commit!.
                            db-with-parents (-> prepared-db
                                                (assoc-in [:config :branch] branch)
                                                (assoc-in [:meta :datahike/parents] parents))
                            [schema-meta-kv-to-write pre-cid-store]
                            (db->stored db-with-parents true key-maps)
                            cid (create-commit-id db-with-parents pre-cid-store)
                            _ (reset! primary-commit-id* cid)
                            _ (reset! secondary-index-keys* key-maps)
                            db-to-store (assoc-in pre-cid-store
                                                  [:meta :datahike/commit-id] cid)
                            pending-kvs (get-and-clear-pending-kvs! store)
                        ;; Same opt-out as datahike.writing/commit!: no
                        ;; commit-graph store → no separate cid record.
                            commit-graph? (get (:config prepared-db) :commit-graph? true)]

                  ;; Register the logical root while the GC guard is held. If
                  ;; the following head write has an ambiguous outcome, GC can
                  ;; resolve whichever head is actually present instead of
                  ;; sweeping a landed generation hidden behind an unregistered
                  ;; branch name.
                        (let [registered (set (<?- (k/get store :branches nil opts)))
                              old-head (<?- (k/get store branch nil opts))]
                          (reset! branch-was-registered? (contains? registered branch))
                          (reset! head-before-registration old-head)
                          (<?- (update-branches! store #(conj % branch) opts))
                          (reset! registry-published? true))

                  ;; Write all data. The branch head is a MUTABLE pointer and goes LAST,
                  ;; after every value it names — the barrier invariant, as in commit!.
                        (if (multi-key-capable? store)
                      ;; ORDERED vec, not a map: konserve applies a [k v] seq in sequence
                      ;; order, and a map batch makes NO ordering promise (konserve.core/
                      ;; multi-assoc). The batch is atomic here, so this is not about torn
                      ;; writes — it is so a konserve-sync subscriber RELAYS the batch in
                      ;; the order it was committed, instead of possibly landing the head
                      ;; on a replica before the nodes it references. Mirrors commit!.
                          (let [fenced? (some? (k/conditional-write-domain store))
                                writes (cond-> (vec pending-kvs)
                                         schema-meta-kv-to-write (conj [(first schema-meta-kv-to-write)
                                                                        (second schema-meta-kv-to-write)])
                                         commit-graph?           (conj [cid db-to-store])
                                         (not fenced?)           (conj [branch db-to-store]))]
                            (when-not fenced?
                              (reset! head-write-issued? true))
                            (when (seq writes)
                              (<?- (k/multi-assoc store writes opts)))
                            ;; Conditional heads cannot live in multi-assoc: its
                            ;; per-key locks cannot make check-all/write-all one
                            ;; atomic operation. Values land first, then the head.
                            (when fenced?
                              (reset! head-write-issued? true)
                              (<?- (assoc-current! store branch db-to-store opts true))))
                          (do
                            (<?- (write-pending-kvs! store pending-kvs sync?))
                            (when schema-meta-kv-to-write
                              (<?- (k/assoc store (first schema-meta-kv-to-write) (second schema-meta-kv-to-write) opts)))
                            (when commit-graph?
                              (<?- (k/assoc store cid db-to-store opts)))
                            (reset! head-write-issued? true)
                            (<?- (assoc-current! store branch db-to-store opts true))))

                        #?(:clj
                           (let [released (release-secondary-generations!
                                           @preparations*
                                           {:status :committed
                                            :attempt-id attempt-id
                                            :branch branch
                                            :store store
                                            :secondary-index-keys key-maps
                                            :primary-commit-id cid})]
                             (if sync?
                               (take-lifecycle!! released)
                               (<?- released))))
                        nil)
                      (catch #?(:clj Throwable :cljs :default) e
                        (let [definitive-abort?
                              (or (not @head-write-issued?)
                                  (= :konserve/revision-mismatch
                                     (:type (ex-data e))))]
                          #?(:clj
                             (let [released
                                   (release-secondary-generations!
                                    @preparations*
                                    (if definitive-abort?
                                      {:status :aborted
                                       :attempt-id attempt-id
                                       :branch branch
                                       :cause e}
                                      {:status :unknown
                                       :attempt-id attempt-id
                                       :branch branch
                                       :store store
                                       :primary-commit-id @primary-commit-id*
                                       :secondary-index-keys
                                       @secondary-index-keys*
                                       :cause e}))]
                               (if sync?
                                 (take-lifecycle!! released)
                                 (<?- released))))
                          ;; Registration precedes the head only to make an
                          ;; ambiguous landed head discoverable to GC. Undo our
                          ;; newly-added registry entry on a definitive failure,
                          ;; but only while the head is still exactly what we
                          ;; observed. A concurrent force that moved it owns the
                          ;; now-live branch and must keep the entry.
                          (when (and definitive-abort?
                                     @registry-published?
                                     (false? @branch-was-registered?)
                                     (= @head-before-registration
                                        (<?- (k/get store branch nil opts))))
                            (<?- (update-branches! store #(disj % branch) opts))))
                        #?(:clj
                           (throw (ex-info (.getMessage ^Throwable e)
                                           (assoc (or (ex-data e) {})
                                                  :datahike/attempt-id attempt-id)
                                           e))
                           :cljs (throw e)))
                      (finally (guard/done! gc-sid gc-token)))))))))

(defn commit-id
  "Retrieve the commit-id for this db."
  [db]
  (db-check db)
  (get-in db [:meta :datahike/commit-id]))

(defn parent-commit-ids
  "Retrieve parent commit ids from db."
  [db]
  (db-check db)
  (get-in db [:meta :datahike/parents]))

(defn commit-as-db
  "Loads the database stored at this commit id.
   First argument can be a connection, db value, or raw konserve store."
  ([conn-or-store cid] (commit-as-db conn-or-store cid {:sync? true}))
  ([conn-or-store cid opts]
   (commit-id-check cid)
   (let [store (extract-store conn-or-store)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (when-let [raw-db (<?- (k/get store cid nil opts))]
                    (stored->db-read-only raw-db store)))))))

(defn branch-as-db
  "Loads the database stored at this branch.
   First argument can be a connection, db value, or raw konserve store."
  ([conn-or-store branch] (branch-as-db conn-or-store branch {:sync? true}))
  ([conn-or-store branch opts]
   (branch-check branch)
   (let [store (extract-store conn-or-store)
         opts (select-keys opts [:sync?])]
     (async+sync (:sync? opts) *default-sync-translation*
                 (go-try-
                  (when-let [raw-db (<?- (k/get store branch nil opts))]
                    (stored->db-read-only raw-db store)))))))

(defn merge!
  "Create a merge commit to the current branch of this connection for parent
  commit uuids or branch keywords. It is the responsibility of the caller to
  make sure that tx-data contains the data to be merged into the branch from
  the parents. This function ensures that the parent commits are properly tracked.

  Routed through the writer for proper serialization with concurrent transactions.
  Returns a tx-report (sync) or promise/channel (async)."
  ([conn parents tx-data]
   (merge! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   @(datahike.writer/merge-db! conn {:parents parents
                                     :tx-data tx-data
                                     :tx-meta tx-meta})))

(defn merge-async!
  "Async version of merge!. Returns a promise (CLJ) or channel (CLJS)."
  ([conn parents tx-data]
   (merge-async! conn parents tx-data nil))
  ([conn parents tx-data tx-meta]
   (parent-check parents)
   (datahike.writer/merge-db! conn {:parents parents
                                    :tx-data tx-data
                                    :tx-meta tx-meta})))
