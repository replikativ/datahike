(ns datahike.gc-roots
  "Durable GC roots: records the collector's mark walks IN ADDITION to branch
   heads, persisted in the store so that a collector in ANY process honours them.

   WHY. `datahike.gc-guard` is exact for the writers in one JVM and blind to
   every other; `gc-storage!`'s `:min-age-ms` floor covers a commit's few
   milliseconds elsewhere but no sane floor covers hours. Two things last
   hours: a long READER of an old record (a secondary-index backfill scanning
   its base snapshot) and a long BUILDER whose partial trees are reachable from
   nothing until they are published (a bulk import, a versioned adapter's
   build generation). Both are protected here by naming what they need in a
   record the mark can walk.

   WHAT A ROOT CAN PROTECT. Only what a record NAMES. The mark derives every
   reachable object from a record's fields — the six index keys, its
   schema-meta, its secondary-index key-maps, the store-refs its datoms hold —
   so a root is a record in exactly that shape:

     :pin        — a verbatim copy of a commit record with its parents removed:
                   this commit, nothing older. The long-reader case.
     :checkpoint — a synthetic record whose fields name PARTIAL state: the
                   trees of a build in progress, or a base commit plus a
                   building index generation under `:secondary-index-keys`.
                   The long-builder case. Republish it as the build advances.
     :ref        — a commit record with its parents kept, so its ancestry is
                   retained under the same `remove-before` gating as a branch.
                   For durable user references; permanent unless given a TTL.

   What a root cannot protect is the millisecond between \"values written\" and
   \"record published\" — there is no record yet. That stays the guard's job
   (in-process) and the floor's (across processes).

   LAYOUT. One registry key, `:datahike/gc-roots`, holding `{id entry}` where
   an entry carries the lease and a pointer to the record, which lives under its
   own immutable key `[:datahike/gc-root id]`. The registry is small and
   rewritten on every renewal; records are written once. Both keys are
   whitelisted by `gc-storage!`, and the record key is the seed of a mark walk
   identical to a branch's — `reachable-in-branch` is reused unchanged.

   LEASES. A root held by a process that dies must not pin forever, and owner
   liveness is undecidable across machines, so time is the signal. A root has
   `:expires-at`; its holder renews at a fraction of the TTL; a collector reaps
   an entry once it is past `:expires-at` by the entry's own TTL again (the
   grace absorbs clock skew and a stalled renewal thread). A holder that finds
   its entry gone at renewal — reaped, or deleted by an OLDER datahike whose
   sweep does not know this key — learns it is `:gc/root-lost` and must not
   publish what the root was protecting. That turns the one failure this design
   cannot prevent (an older collector) from silent into loud.

   Timestamps here are wall-clock instants and are used only for expiry, never
   for the sweep cutoff; konserve's monotone write clock is not involved.

   COMPATIBILITY. With no roots present nothing changes: the registry key is
   absent, the mark walks branches exactly as before. A datahike older than this
   namespace treats `:datahike/gc-roots` and every root record as garbage — the
   same fate the objects they protect already had under such a collector — and
   the loss is detected at the holder's next renewal."
  (:require [datahike.gc-guard :as guard]
            [konserve.core :as k]
            [replikativ.logging :as log]
            #?(:clj  [konserve.utils :as ku :refer [async+sync *default-sync-translation*]]
               :cljs [konserve.utils :as ku
                      :refer [*default-sync-translation*]
                      :refer-macros [async+sync]])
            #?(:clj  [superv.async :refer [go-try- <?-]]
               :cljs [superv.async :refer-macros [go-try- <?-]])
            #?(:clj  [clojure.core.async :refer [go-loop <! alts! timeout chan close!]]
               :cljs [clojure.core.async :refer [<! alts! timeout chan close!]]))
  #?(:clj  (:import [java.util Date])
     :cljs (:require-macros [clojure.core.async :refer [go-loop]])))

(def registry-key
  "The one mutable key: `{id entry}`."
  :datahike/gc-roots)

(defn record-key
  "Where root `id`'s record lives. Immutable once written."
  [id]
  [:datahike/gc-root id])

(def ^:const DEFAULT_TTL_MS
  "An hour. Long enough that minutes of clock skew and a stop-the-world pause
   cannot reap a live holder; short enough that a crashed holder's pin is
   released the same afternoon. Renew at a third of it."
  (* 60 60 1000))

(def kinds #{:pin :checkpoint :ref})

(defn- now-ms []
  #?(:clj (System/currentTimeMillis) :cljs (.getTime (js/Date.))))

(defn- ->date [ms]
  #?(:clj (Date. (long ms)) :cljs (js/Date. ms)))

(defn- date-ms [d]
  #?(:clj (.getTime ^Date d) :cljs (.getTime d)))

(defn- new-id []
  #?(:clj (java.util.UUID/randomUUID) :cljs (random-uuid)))

;; ---------------------------------------------------------------------------
;; Registry updates. The same conditional-write discipline `versioning` applies
;; to `:branches`: CAS with retry where the store fences, one unconditional
;; write to mint a revision on a legacy key, best-effort `k/update` elsewhere.

(defn- revision-mismatch? [e]
  (= :konserve/revision-mismatch (:type (ex-data e))))

(defn- update-registry!
  [store f opts]
  (let [fenced? (some? (k/conditional-write-domain store))]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (if-not fenced?
                   (second (<?- (k/update store registry-key #(f (or % {})) opts)))
                   (loop [attempt 0]
                     (let [raw (<?- (k/get store registry-key nil
                                           (assoc opts :with-revision? true)))
                           [current revision] raw
                           updated (f (or current {}))
                           result (try
                                    (<?- (k/assoc store registry-key updated
                                                  (cond-> opts
                                                    revision (assoc :expected-revision revision))))
                                    updated
                                    (catch #?(:clj Throwable :cljs :default) e
                                      (if (revision-mismatch? e) ::retry (throw e))))]
                       (if (= ::retry result)
                         (if (< attempt 63)
                           (recur (inc attempt))
                           (throw (ex-info "GC root registry kept changing; conditional update did not converge."
                                           {:type :datahike/gc-root-registry-contention
                                            :attempts (inc attempt)})))
                         result))))))))

;; ---------------------------------------------------------------------------
;; Reading

(defn roots
  "The registry: `{id entry}`, or `{}`."
  ([store] (roots store {:sync? false}))
  ([store opts]
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try- (or (<?- (k/get store registry-key nil opts)) {})))))

(defn expired?
  "Past `:expires-at` by the entry's own TTL again. A permanent root
   (`:expires-at nil`) never is."
  ([entry] (expired? entry (now-ms)))
  ([entry now-ms*]
   (boolean
    (when-let [exp (:expires-at entry)]
      (< (+ (date-ms exp) (or (:ttl-ms entry) DEFAULT_TTL_MS)) now-ms*)))))

(defn live-roots
  "The registry without its expired entries — what a reader that does not
   reap (online GC) should treat as roots. Read-only."
  ([store] (live-roots store {:sync? false}))
  ([store opts]
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [now (now-ms)]
                  (into {} (remove (fn [[_ e]] (expired? e now))) (<?- (roots store opts))))))))

(defn reap-expired!
  "Drop expired entries from the registry and return what remains. Run by the
   collector BEFORE it reads the registry for the mark, so a reaped root's
   record and objects are not marked in that cycle. Idempotent."
  ([store opts] (reap-expired! store (now-ms) opts))
  ([store now-ms* opts]
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [current (<?- (roots store opts))
                      dead (into #{} (keep (fn [[id entry]] (when (expired? entry now-ms*) id))) current)]
                  (if (empty? dead)
                    current
                    ;; Decide expiry INSIDE the update, against the registry as
                    ;; it is at write time: a holder that renewed between the
                    ;; read above and this write must keep its root. The set
                    ;; above only tells us a write is worth attempting.
                    (let [remaining (<?- (update-registry!
                                          store
                                          (fn [reg] (into {} (remove (fn [[_ e]] (expired? e now-ms*))) reg))
                                          opts))]
                      (log/info :datahike/gc-roots-reaped
                                {:ids (into #{} (remove #(contains? remaining %)) (keys current))})
                      remaining)))))))

;; ---------------------------------------------------------------------------
;; Records

(defn- store-id [db] (:id (:store (:config db))))

(defn commit-record
  "The stored record for `db`'s own commit: the commit-graph record when the
   store keeps one, else the branch head if it still names that commit. Nil
   when neither resolves — a db that was never committed, or whose commit has
   since been collected."
  ([db] (commit-record db {:sync? false}))
  ([db opts]
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [store (:store db)
                      cid (get-in db [:meta :datahike/commit-id])
                      branch (get-in db [:config :branch] :db)]
                  (when cid
                    (or (<?- (k/get store cid nil opts))
                        (let [head (<?- (k/get store branch nil opts))]
                          (when (= cid (get-in head [:meta :datahike/commit-id]))
                            head)))))))))

(defn- shape-record [record kind]
  (cond-> record
    (not= :ref kind) (update :meta dissoc :datahike/parents)))

;; ---------------------------------------------------------------------------
;; Lifecycle

(defn root!
  "Declare a root. `spec`:
     :kind    :pin | :checkpoint | :ref
     :record  a commit-shaped record (see ns doc); for `:pin` of `db`'s own
              commit, omit it and it is read from the store
     :ttl-ms  lease length; default [[DEFAULT_TTL_MS]]; nil = permanent
     :owner   free-form map naming the holder (diagnostics only)
     :note    free-form string
     :id      optional; generated otherwise
   Writes the record, then publishes it in the registry — values then pointer,
   under the in-process guard like every such sequence. Returns the id."
  ([db spec] (root! db spec {:sync? false}))
  ([db {:keys [kind record owner note id] :as spec} opts]
   (when-not (contains? kinds kind)
     (throw (ex-info "A GC root needs a :kind of :pin, :checkpoint or :ref."
                     {:type :datahike/gc-root-invalid-kind :kind kind})))
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [store (:store db)
                      sid (store-id db)
                      id (or id (new-id))
                      ttl-ms (if (contains? spec :ttl-ms) (:ttl-ms spec) DEFAULT_TTL_MS)
                      record (or record (<?- (commit-record db opts)))
                      _ (when-not (map? record)
                          (throw (ex-info "No record to root: the db has no resolvable commit and none was given."
                                          {:type :datahike/gc-root-no-record
                                           :commit-id (get-in db [:meta :datahike/commit-id])})))
                      record (shape-record record kind)
                      now (now-ms)
                      entry (cond-> {:id id
                                     :kind kind
                                     :record-key (record-key id)
                                     :commit-id (get-in record [:meta :datahike/commit-id])
                                     :owner owner
                                     :note note
                                     :ttl-ms ttl-ms
                                     :created-at (->date now)
                                     :renewed-at (->date now)}
                              ttl-ms (assoc :expires-at (->date (+ now ttl-ms))))
                      token (guard/writing! sid)]
                  (try
                    (<?- (k/assoc store (record-key id) record opts))
                    (<?- (update-registry! store #(assoc % id entry) opts))
                    (finally (guard/done! sid token)))
                  (log/debug :datahike/gc-root-declared {:id id :kind kind :note note})
                  id)))))

(defn pin!
  "Root `db`'s own commit as a `:pin`. Sugar over [[root!]]."
  ([db] (pin! db {} {:sync? false}))
  ([db spec] (pin! db spec {:sync? false}))
  ([db spec opts] (root! db (assoc spec :kind :pin) opts)))

(defn- lost! [id]
  (throw (ex-info (str "GC root " id " is gone: it expired and was reaped, or an older collector deleted the registry. "
                       "Whatever it protected may have been swept; do not publish on top of it.")
                  {:type :gc/root-lost :id id})))

(defn renew!
  "Extend the lease of root `id`. Raises `:gc/root-lost` if the entry is gone —
   the holder must then abandon what the root was protecting. Returns the entry."
  ([db id] (renew! db id {:sync? false}))
  ([db id opts]
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [store (:store db)
                      now (now-ms)
                      updated (<?- (update-registry!
                                    store
                                    (fn [reg]
                                      (if-let [entry (get reg id)]
                                        (assoc reg id
                                               (cond-> (assoc entry :renewed-at (->date now))
                                                 (:ttl-ms entry)
                                                 (assoc :expires-at (->date (+ now (:ttl-ms entry))))))
                                        reg))
                                    opts))]
                  (or (get updated id) (lost! id)))))))

(defn assert-live!
  "The publish-time check: root `id` exists and was renewed within
   `max-staleness-ms`. Raises `:gc/root-lost` otherwise. A holder whose renewal
   stalled long enough to be reaped learns it here, before it publishes."
  ([db id max-staleness-ms] (assert-live! db id max-staleness-ms {:sync? false}))
  ([db id max-staleness-ms opts]
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (let [entry (get (<?- (roots (:store db) opts)) id)]
                  (when-not entry (lost! id))
                  (when (and max-staleness-ms
                             (> (- (now-ms) (date-ms (:renewed-at entry))) max-staleness-ms))
                    (throw (ex-info (str "GC root " id " was last renewed too long ago to trust.")
                                    {:type :gc/root-stale :id id
                                     :renewed-at (:renewed-at entry)})))
                  entry)))))

(defn release!
  "Drop root `id`. Its record becomes ordinary garbage. Idempotent."
  ([db id] (release! db id {:sync? false}))
  ([db id opts]
   (async+sync (:sync? opts) *default-sync-translation*
               (go-try-
                (<?- (update-registry! (:store db) #(dissoc % id) opts))
                (log/debug :datahike/gc-root-released {:id id})
                id))))

(defn start-renewal!
  "Renew root `id` every `interval-ms` — by default a third of the ROOT's own
   TTL, read from its entry — until the returned stop function is called. On
   `:gc/root-lost` (including an entry missing at start) the loop stops and
   calls `on-lost` (if given) with the exception — the consumer must then
   abandon its work. Prefer renewing from the consumer's own loop where it has
   one; this exists for consumers that do not."
  [db id {:keys [interval-ms on-lost]}]
  (let [stop (chan)
        fail! (fn [e]
                (log/warn :datahike/gc-root-renewal-failed {:id id :error e})
                (when on-lost (on-lost e)))]
    (go-loop [interval interval-ms]
      (if (nil? interval)
        ;; First pass: derive the cadence from the entry itself.
        (let [reg (<! (roots (:store db) {:sync? false}))]
          (if (instance? #?(:clj Throwable :cljs js/Error) reg)
            (fail! reg)
            (if-let [entry (get reg id)]
              (recur (quot (or (:ttl-ms entry) DEFAULT_TTL_MS) 3))
              (fail! (ex-info (str "GC root " id " is gone.") {:type :gc/root-lost :id id})))))
        (let [[_ port] (alts! [stop (timeout interval)])]
          (when-not (= port stop)
            (let [res (<! (renew! db id {:sync? false}))]
              (if (instance? #?(:clj Throwable :cljs js/Error) res)
                (fail! res)
                (recur interval)))))))
    (fn stop-renewal! [] (close! stop) nil)))
