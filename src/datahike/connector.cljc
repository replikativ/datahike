(ns ^:no-doc datahike.connector
  (:require [datahike.connections :refer [get-connection add-connection! delete-connection!
                                          acquire-node-cache! abandon-reservation!
                                          *connections*]]
            [datahike.readers]
            [datahike.store :as ds]
            [datahike.index.interface :as dii]
            [datahike.writing :as dsi]
            [datahike.config :as dc]
            [datahike.tools :as dt #?(:clj :refer :cljs :refer-macros) [meta-data]]
            [datahike.writer :as w]
            [konserve.core :as k]
            [konserve.store :as ks]
            [replikativ.logging :as log]
            [clojure.spec.alpha :as s]
            [clojure.data :refer [diff]]
            [konserve.utils :refer [#?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async :refer [go <!] :as async])
  #?(:clj (:import [clojure.lang IDeref IAtom IMeta ILookup IRef])))

;; connection

(declare deref-conn)

(deftype Connection [wrapped-atom]
  IDeref
  (#?(:clj deref :cljs -deref) [conn] (deref-conn conn))
  ;; These interfaces should not be used from the outside, they are here to keep
  ;; the internal interfaces lean and working.
  ILookup
  (#?(:clj valAt :cljs -lookup) [c k] (if (= k :wrapped-atom) wrapped-atom nil))
  IMeta
  (#?(:clj meta :cljs -meta) [_] (meta wrapped-atom))
  #?@(:cljs
      [IAtom
       ISwap
       (-swap! [_ f] (swap! wrapped-atom f))
       (-swap! [_ f arg] (swap! wrapped-atom f arg))
       (-swap! [_ f arg1 arg2] (swap! wrapped-atom f arg1 arg2))
       (-swap! [_ f arg1 arg2 args] (apply swap! wrapped-atom f arg1 arg2 args))
       IReset
       (-reset! [_ newval] (reset! wrapped-atom newval))
       IWatchable ;; TODO This is unofficially supported, it triggers watches on each update, not on commits. For proper listeners use the API.
       (-add-watch [_ key f] (add-watch wrapped-atom key f))
       (-remove-watch [_ key] (remove-watch wrapped-atom key))
       (-notify-watches [_ old new] (-notify-watches wrapped-atom old new))])
  #?@(:clj
      [IAtom
       (swap [_ f] (swap! wrapped-atom f))
       (swap [_ f arg] (swap! wrapped-atom f arg))
       (swap [_ f arg1 arg2] (swap! wrapped-atom f arg1 arg2))
       (swap [_ f arg1 arg2 args] (apply swap! wrapped-atom f arg1 arg2 args))
       (compareAndSet [_ oldv newv] (compare-and-set! wrapped-atom oldv newv))
       (reset [_ newval] (reset! wrapped-atom newval))
       IRef ;; TODO This is unofficially supported, it triggers watches on each update, not on commits. For proper listeners use the API.
       (addWatch [_ key f] (add-watch wrapped-atom key f))
       (removeWatch [_ key] (remove-watch wrapped-atom key))]))

(defn connection? [x]
  (instance? Connection x))

#?(:clj
   (defmethod print-method Connection
     [^Connection conn ^java.io.Writer w]
     (let [config (:config @(:wrapped-atom conn))]
       (.write w "#datahike/Connection")
       (.write w (pr-str [(ds/store-identity (:store config)) (:branch config)])))))

(defn deref-conn [^Connection conn]
  (let [wrapped-atom (.-wrapped-atom conn)]
    (when (= @wrapped-atom :released)
      (throw (ex-info "Connection has been released."
                      {:type :connection-has-been-released})))
    (if (w/refresh-on-deref? (get @wrapped-atom :writer))
      (do
        (log/trace :datahike/db-deref {:branch (:branch (:config @wrapped-atom))})
        ;; Exactly the writer's per-transaction re-read, and deliberately the
        ;; SAME function rather than the same three lines: this used to inline
        ;; them and had dropped the vanished-head check in the copy, so a
        ;; deleted database made `reload-head` compare nil against our cid,
        ;; conclude the head had moved, and build a db out of `{:config …}` —
        ;; nil :max-tx, no index roots — which queries either answer emptily or
        ;; die on much later with a bare IllegalArgumentException. Now it raises
        ;; :branch-head-does-not-exist-in-store like every other reader.
        ;;
        ;; Costs one konserve read, and on an UNMOVED head (cid identity) hands
        ;; back the db we already hold rather than rebuilding it — rebuilding
        ;; would re-run the secondary index restore on EVERY deref, contend with
        ;; a live writer's lock (scriptum) and drop the index. The connection's
        ;; :writer is carried over either way.
        (dsi/reload-branch-head @wrapped-atom))
      @wrapped-atom)))

(defn db-async
  "Return a channel yielding the connection's current immutable database.

   Unlike deref, this may cross an asynchronous storage boundary. It is the
   acquisition API for a shared browser writer backed by a tiered memory/S3
   store: the branch head is read from S3 and the changed persistent-set index
   frontier is read through every frontend tier without listing the bucket. The
   resulting DB remains synchronously queryable. Out-of-line
   `:db.type/store-ref` blobs retain their explicit asynchronous fetch contract;
   they are not needed by the query engine and are not implicitly prefetched.
   Remote/streaming and exclusive writers return their local value."
  [^Connection conn]
  (go-try-
   (let [wrapped-atom (.-wrapped-atom conn)
         current @wrapped-atom]
     (when (= current :released)
       (throw (ex-info "Connection has been released."
                       {:type :connection-has-been-released})))
     (if (w/refresh-on-deref? (:writer current))
       (<?- (dsi/reload-branch-head current false))
       current))))

(defn conn-from-db
  "Creates a mutable reference to a given immutable database. See [[create-conn]]."
  [db]
  (Connection. (atom db :meta {:listeners (atom {})})))

(s/def ::connection #(and (instance? Connection %)
                          (not= @(:wrapped-atom %) :released)))

(defn- version-unknown?
  "A version string we cannot order against another. Either it is absent —
   `get-version` reads Maven `pom.properties`, which a `:local/root` source
   checkout does not carry, so every dependency resolved from a sibling
   checkout reports `nil` — or it is an explicit development build.

   Unknown must never read as OLDER. Comparing `nil`/\"DEVELOPMENT\" against a
   release string made a store written by released konserve unopenable from any
   source checkout of the stack, which is precisely the cross-repo development
   setup this project is built for."
  [v]
  (or (nil? v) (= v "DEVELOPMENT")))

(defn- check-version!
  "Raise when the store was written by a NEWER `label` than the one loaded.
   Skipped whenever either side is unknown (see `version-unknown?`) — one rule
   for all four dependencies, instead of the three different ad-hoc guards this
   replaced (DEVELOPMENT-aware for datahike, nil-safe for hitchhiker-tree,
   neither for persistent-sorted-set and konserve)."
  [label err-type stored now config]
  (when-not (or (version-unknown? stored)
                (version-unknown? now)
                (>= (compare now stored) 0))
    (log/raise (str "Database was written with newer " label " version.")
               {:type err-type
                :stored stored
                :now now
                :config config})))

(defn version-check [{:keys [meta config] :as db}]
  (let [{dh-stored :datahike/version
         hh-stored :hitchhiker.tree/version
         pss-stored :persistent.set/version
         ksv-stored :konserve/version} meta
        {dh-now :datahike/version
         hh-now :hitchhiker.tree/version
         pss-now :persistent.set/version
         ksv-now :konserve/version} (meta-data)]
    (check-version! "Datahike" :db-was-written-with-newer-datahike-version
                    dh-stored dh-now config)
    (check-version! "hitchhiker-tree" :db-was-written-with-newer-hht-version
                    hh-stored hh-now config)
    (check-version! "persistent-sorted-set" :db-was-written-with-newer-pss-version
                    pss-stored pss-now config)
    (check-version! "konserve" :db-was-written-with-newer-konserve-version
                    ksv-stored ksv-now config)))

(defn ensure-stored-config-consistency [config stored-config]
  (let [;; Remove runtime parameters and creation-time parameters. Value-size
        ;; caps are creation-time defaults the connect config may lack (or hold a
        ;; different explicit value for — stored wins), so exclude them here.
        cap-keys (cons :value-caps (keys dc/default-value-caps))
        config (apply dissoc config :name :search-cache-size :store-cache-size cap-keys)
        stored-config (apply dissoc stored-config :initial-tx :name :search-cache-size :store-cache-size cap-keys)
        stored-config (merge {:writer dc/self-writer} stored-config)
        stored-config (if (empty? (:index-config stored-config))
                        (dissoc stored-config :index-config)
                        stored-config)
        ;; Writer ownership is a RUNTIME choice of the connecting process, not a
        ;; property of the stored database. It sits INSIDE :writer, so the flat
        ;; dissoc of runtime keys above does not reach it. Ignore the deprecated
        ;; :streaming? alias too, for databases created while #959 was experimental.
        config        (cond-> config        (:writer config)        (update :writer dissoc :writer-ownership :streaming?))
        stored-config (cond-> stored-config (:writer stored-config) (update :writer dissoc :writer-ownership :streaming?))
        ;; if we connect to remote allow writer to be different
        [config stored-config] (if-not (= dc/self-writer config)
                                 [(dissoc config :writer)
                                  (dissoc stored-config :writer)]
                                 [config stored-config])

        ;; Validate store identities match (prevents connecting to wrong database)
        ;; Store configuration details (backend, path, credentials) can differ
        stored-store-id (get-in stored-config [:store :id])
        connect-store-id (get-in config [:store :id])
        _ (when (and stored-store-id connect-store-id
                     (not= stored-store-id connect-store-id))
            (log/raise "Store identity mismatch: connecting to wrong database."
                       {:type :store-identity-mismatch
                        :stored-id stored-store-id
                        :connect-id connect-store-id
                        :config config
                        :stored-config stored-config}))

        ;; Remove entire :store from comparison (backend, path, credentials can change)
        ;; Only the :id needs to match (checked above)
        config (dissoc config :store)
        stored-config (dissoc stored-config :store)]

    (when-not (= config stored-config)
      (log/raise "Configuration does not match stored configuration. In some cases this check is too restrictive. If you are sure you are loading the right database with the right configuration then you can disable this check by setting :allow-unsafe-config to true in your config."
                 {:type          :config-does-not-match-stored-db
                  :config        config
                  :stored-config stored-config
                  :diff          (diff config stored-config)}))))

(def create-time-fixed-index-keys
  "Sub-keys of :index-config that shape the on-disk index representation and are
   therefore fixed when the database is created. At connect they are adopted from
   the stored config, so a reconnect does not need to re-specify them."
  #{:branching-factor :diff-buf-size})

(def store-fixed-record-keys
  "Top-level config keys that describe how records in the store are laid out
   and are therefore fixed when the database is created: :fuse-index-roots?
   (index roots inlined into the db record) and :commit-graph? (whether each
   commit persists an immutable cid record). Adopted at connect like the
   create-time-fixed :index-config sub-keys."
  #{:fuse-index-roots? :commit-graph?})

(defn- adopt-create-time-fixed
  "Adopt store-fixed settings from the stored config into `config`: the
   create-time-fixed :index-config sub-keys and the store-fixed-record-keys
   (which describe how records in the store are laid out). A key the caller
   did not specify is taken from the store, so reconnects don't need to
   re-specify creation settings; an explicitly conflicting value raises unless
   :allow-unsafe-config is set (then the given value wins). Returns the
   possibly-updated config."
  [config stored-config]
  (let [unsafe?   (:allow-unsafe-config config)
        stored-ic (select-keys (:index-config stored-config) create-time-fixed-index-keys)
        given-ic  (:index-config config)
        conflicts (into (into {}
                              (keep (fn [[k stored-v]]
                                      (when (and (contains? given-ic k)
                                                 (not= (get given-ic k) stored-v))
                                        [k {:given (get given-ic k) :stored stored-v}])))
                              stored-ic)
                        (keep (fn [k]
                                (when (and (contains? stored-config k)
                                           (contains? config k)
                                           (not= (get config k) (get stored-config k)))
                                  [k {:given (get config k)
                                      :stored (get stored-config k)}])))
                        store-fixed-record-keys)]
    (when (and (seq conflicts) (not unsafe?))
      (log/raise "Create-time-fixed index settings differ from the stored configuration."
                 {:type      :create-time-fixed-index-config-mismatch
                  :conflicts conflicts
                  :config    config}))
    (let [ic (if unsafe? (merge stored-ic given-ic) (merge given-ic stored-ic))
          config (if (seq ic)
                   (assoc config :index-config ic)
                   (dissoc config :index-config))]
      (reduce (fn [config k]
                (if (and (contains? stored-config k)
                         (or (not (contains? config k)) (not unsafe?)))
                  (assoc config k (get stored-config k))
                  config))
              config
              store-fixed-record-keys))))

(defn- check-online-gc-compatible
  "Online GC and diff-buf are not compatible; refuse the combination at connect.

   Online GC reclaims blobs from persistent-sorted-set's `markFreed` stream, which pss
   documents as a HINT rather than a reachability claim. Under diff-buf that hint is only
   sound for a LINEAR commit history: a parent's slot names an anchor PLUS a diff, so two
   versions can name the same anchor and neither owns it. Storing one of them may FLUSH that
   child — write it out whole and free the anchor — which is correct for the version being
   stored and wrong for the other. Measured in pss against a backend that acts on the
   callback: 25 read failures / 768 trials at budget <= 4; linear histories clean in 432/432
   cells; budget 0 clean in 864/864.

   Checked HERE because this is the first point where both values are known: `:diff-buf-size`
   is create-time-fixed and has just been adopted from the stored config, while `:online-gc`
   is supplied at connect. The dangerous case is precisely a reconnect — a database created
   long ago with a budget, later connected with online GC switched on.

   Refused rather than silently disabled so the operator learns their GC is not running.
   `:allow-unsafe-config` overrides, consistent with the create-time-fixed conflict above;
   `online-gc!` then skips with a warning rather than acting on the stream.

   Offline GC (`d/gc-storage`) is unaffected — it derives reachability itself instead of
   trusting the hint, and is the supported way to reclaim on a diff-buf database."
  [config]
  (let [dbs (get-in config [:index-config :diff-buf-size] 0)]
    (when (and (get-in config [:online-gc :enabled?])
               (pos? dbs)
               (not (:allow-unsafe-config config)))
      (log/raise "Online GC is not compatible with diff-buf; use offline GC (d/gc-storage)."
                 {:type           :online-gc-incompatible-with-diff-buf
                  :diff-buf-size  dbs
                  :remedy         "Set :online-gc {:enabled? false}, or create the database with :diff-buf-size 0, or run d/gc-storage instead."
                  :config         config})))
  config)

(defn- normalize-config [cfg]
  ;; :index-config and the store-fixed-record-keys are store-fixed and
  ;; adopted on a fresh connect (adopt-create-time-fixed), so an existing
  ;; connection may carry adopted keys the caller's config omits; conflicts
  ;; are guarded on the fresh-connect path, not here. The value-size caps live
  ;; only in the stored config and are ignored for normalization too.
  (apply dissoc cfg :writer :store :store-cache-size :search-cache-size
         :index-config :fuse-index-roots? :commit-graph?
         (cons :value-caps (keys dc/default-value-caps))))

(defn -connect-impl* [config opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [_ (log/debug :datahike/connect {:config (update-in config [:store] dissoc :password)})
                     store-config (:store config)
                     store-id (ds/store-identity store-config)
                     conn-id [store-id (:branch config)]
                     lease* (volatile! nil)]
                 (if-let [conn (get-connection conn-id)]
                   (let [conn-config (:config @(:wrapped-atom conn))
               ;; replace store config with its identity
                         cfg (normalize-config config)
                         conn-cfg (normalize-config conn-config)]
                     (when-not (= cfg conn-cfg)
                       (log/raise "Configuration does not match existing connections."
                                  {:type :config-does-not-match-existing-connections
                                   :config cfg
                                   :existing-connections-config conn-cfg
                                   :diff (diff cfg conn-cfg)}))
                     ;; normalize-config dissocs :writer, so a second connect
                     ;; asking for DIFFERENT ownership gets the cached connection's
                     ;; writer. Say so rather than let the caller believe shared
                     ;; ownership took effect (release the connection to change it).
                     (let [existing (some-> (:writer @(:wrapped-atom conn)) w/writer-ownership)
                           requested (get-in config [:writer :writer-ownership] :shared)]
                       (when (and (some? existing)
                                  (= :self (get-in config [:writer :backend] :self))
                                  (not= requested existing))
                         (log/warn :datahike/writer-ownership-ignored
                                   "Reusing the existing connection and its writer; the requested :writer-ownership is ignored. Release the connection everywhere first if you need different ownership."
                                   {:requested requested
                                    :existing  existing}))
                       ;; A DEMAND, not a preference, so it is checked against the
                       ;; writer this caller actually gets — which on this path is
                       ;; the cached one, whatever it was built with. Checking it
                       ;; only where a writer is created skipped it precisely
                       ;; here: `:require-fencing` exists for deployments with
                       ;; more than one writer, and the second `connect` in a
                       ;; process is the one that comes out of the cache.
                       ;;
                       ;; `existing` rather than the requested ownership: what
                       ;; matters is whether the writer in hand re-reads the head,
                       ;; and the warning above has already said the request is ignored.
                       ;;
                       ;; SCOPED to the :self backend, matching where the option is
                       ;; legal at all: `normalize-writer-config` refuses
                       ;; :require-fencing on a remote writer before any connect
                       ;; path runs, and checking it here against the LOCAL store
                       ;; would be judging a remote writer's guarantee by a store
                       ;; it does not write to — a refusal or a pass, both about
                       ;; the wrong thing.
                       (when (= :self (get-in config [:writer :backend] :self))
                         (w/check-fencing! (get-in config [:writer :require-fencing])
                                           (or existing :shared)
                                           (:store @(:wrapped-atom conn)))))
                     conn)
                   (try
                     (let [raw-store (<?- (ks/connect-store store-config opts))
                           _         (when-not raw-store
                                       (log/raise "Backend does not exist." {:type   :backend-does-not-exist
                                                                             :config store-config}))
                         ;; Reserve this connection's node cache BEFORE building
                         ;; storage, and hand it to storage explicitly. The
                         ;; reservation is what lets a sibling branch connecting
                         ;; concurrently find the same cache instead of building
                         ;; a second one; the `catch` below removes it if
                         ;; this connect never completes.
                           threshold  (:store-cache-size config)
                           [lease node-cache] (acquire-node-cache! conn-id threshold
                                                                   #(dii/make-node-cache threshold))
                           _ (vreset! lease* lease)
                           raw-store  (assoc raw-store dii/node-cache-key node-cache)
                           store     (ds/add-cache-and-handlers raw-store config)
                           _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                           stored-db (<?- (k/get store (:branch config) nil opts))
                           _         (when-not stored-db
                                       (ks/release-store store-config store opts)
                                       (log/raise "Database does not exist." {:type   :db-does-not-exist
                                                                              :config config}))
                           [config store stored-db]
                           (let [intended-index (:index config)
                                 stored-index   (get-in stored-db [:config :index])]
                             (if-not (= intended-index stored-index)
                               (do
                                 (log/warn :datahike/index-mismatch {:stored-index stored-index})
                                 (let [config    (assoc config :index stored-index)
                                       store     (ds/add-cache-and-handlers raw-store config)
                                       _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                                       stored-db (<?- (k/get store (:branch config) nil opts))]
                                   [config store stored-db]))
                               [config store stored-db]))
                         ;; Adopt create-time-fixed index settings (:index-config
                         ;; {:branching-factor :diff-buf-size}) from the stored config.
                         ;; When adoption changes the config, re-derive the store
                         ;; handlers with it (same pattern as the index reconciliation
                         ;; above) so e.g. a legacy store's non-default branching-factor
                         ;; reaches the node read handlers.
                           [config store stored-db]
                           (let [config' (check-online-gc-compatible
                                          (adopt-create-time-fixed config (:config stored-db)))]
                             (if (= config' config)
                               [config store stored-db]
                               (let [store     (ds/add-cache-and-handlers raw-store config')
                                     _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                                     stored-db (<?- (k/get store (:branch config') nil opts))]
                                 [config' store stored-db])))
                         ;; The runtime db is built from the connect-time config (below),
                         ;; but value-size caps live only in the *stored* config (written
                         ;; at create). Merge them in so enforcement sees them and the
                         ;; consistency check matches. Absent in the stored config (older
                         ;; DBs) → nothing merged → unbounded.
                           config (merge config
                                         (select-keys (:config stored-db)
                                                      (keys dc/default-value-caps)))
                           _ (version-check stored-db)
                           _ (when-not (:allow-unsafe-config config)
                               (ensure-stored-config-consistency config (:config stored-db)))
                           conn      (conn-from-db (dsi/stored->db (assoc stored-db :config config) store))]
                       (swap! (:wrapped-atom conn) assoc :writer
                              (w/create-writer (:writer config) conn))
                     ;; Recovery: every :building index is reconstructed from
                     ;; the primary index. Its stored key-map, if any was written
                     ;; by an older release, is deliberately ignored by
                     ;; stored->db because it may describe a partial backfill.
                       #?(:clj
                          (let [db @(:wrapped-atom conn)
                                schema (:schema db)
                                writer (:writer db)]
                            (doseq [[ident entry] schema
                                    :when (and (map? entry)
                                               (:db.secondary/type entry)
                                               (= :building (:db.secondary/status entry)))]
                              (log/trace :datahike/secondary-index-backfill {:ident ident})
                              (go
                              ;; The delta journal that covered transactions
                              ;; after the stored boundary died with the
                              ;; previous process. Re-anchor the boundary at
                              ;; this head first so the scan picks those
                              ;; datoms up from the primary index; only
                              ;; transactions after this point are journaled.
                                (let [reset-result (<! (w/dispatch! writer
                                                                    {:op 'reset-secondary-index-build-boundary!
                                                                     :args [ident]}))
                                      build-result (if (map? reset-result)
                                                     (<! (w/dispatch! writer
                                                                      {:op 'build-secondary-index!
                                                                       :args [ident]}))
                                                     reset-result)]
                                  (if-not (map? build-result)
                                    (log/warn :datahike/secondary-index-recovery-failed
                                              {:ident ident :error build-result})
                                    (let [install-result
                                          (<! (w/dispatch! writer
                                                           {:op 'install-secondary-index!
                                                            :args [build-result]}))]
                                      (when-not (map? install-result)
                                        (log/warn :datahike/secondary-index-recovery-failed
                                                  {:ident ident :error install-result})
                                        (dsi/finish-secondary-index-build!
                                         build-result)))))))))
                       (if (add-connection! conn-id lease conn)
                         conn
                         (do
                           (w/shutdown (:writer @(:wrapped-atom conn)))
                           (reset! conn :released)
                           (log/raise "Database was deleted while connecting."
                                      {:type :database-deleted-during-connect
                                       :config config}))))
                     ;; Any failure between reserving the node cache and
                     ;; registering the connection -- a missing branch, a bad
                     ;; stored config, `ready-store`, writer creation -- would
                     ;; otherwise strand the reservation, and with it a cache
                     ;; nothing could reach to release. A no-op if the connect
                     ;; got as far as `add-connection!`.
                     (catch #?(:clj Exception :cljs :default) e
                       (when-some [lease @lease*]
                         (abandon-reservation! conn-id lease))
                       (throw e))))))))

;; Multimethod dispatch for different writer backends

(defn backend-dispatch [config & _]
  (get-in config [:writer :backend] :self))

(defmulti -connect* #'backend-dispatch)

(defmethod -connect* :self [config opts]
  (-connect-impl* config opts))

;; public API

(defn connect
  "Connect to a Datahike database.
   
   Config can be a map or URI string. Opts map supports:
   - :sync? (default true) - Block and return connection, or return channel for async"
  ([] (connect {} {}))
  ([config] (connect config {}))
  ([config opts]
   (let [opts (merge {:sync? true} opts)
         normalized (cond
                      (string? config) (dc/uri->config config)
                      (map? config) config
                      :else config)
         loaded (dissoc (dc/load-config normalized) :initial-tx :remote-peer :name)]
     (-connect* loaded opts))))

(defn release
  ([connection] (release connection false))
  ([connection release-all?]
   (when-not (= @(:wrapped-atom connection) :released)
     (let [db      @(:wrapped-atom connection)
           _ (log/trace :datahike/release-connection {:backend (get-in db [:config :store :backend])})
           conn-id [(ds/store-identity (get-in db [:config :store]))
                    (get-in db [:config :branch])]]
       (if-not (get @*connections* conn-id)
         (log/trace :datahike/connection-already-released {:conn-id conn-id})
         (let [new-conns (swap! *connections* update-in [conn-id :count] dec)]
           (when (or release-all? (zero? (get-in new-conns [conn-id :count])))
             (delete-connection! conn-id)
             ;; Close secondary index writers to release file locks (e.g., Lucene write.lock)
             #?(:clj
                (doseq [[_ident idx] (:secondary-indices db)]
                  (when (instance? java.io.Closeable idx)
                    (try (.close ^java.io.Closeable idx)
                         (catch Exception e
                           (log/warn :datahike/secondary-index-close-failed {:error (.getMessage e)}))))))
             (w/shutdown (:writer db))
             ;; Release the underlying store to clean up resources (memory registry, etc.).
             ;; NB: we do NOT unregister the PSS storage here — multiple connections (branches)
             ;; share ONE store-id, so releasing one must not drop the storage a sibling still
             ;; uses for root reads. A reconnect overwrites the entry; a never-reconnected store
             ;; leaves one bounded entry (keyed by the stable store UUID). TODO: ref-counted
             ;; unregister on the last release.
             (ks/release-store (get-in db [:config :store]) (:store db))
             nil)))))))
