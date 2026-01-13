(ns ^:no-doc datahike.connector
  (:require [datahike.connections :refer [get-connection add-connection! delete-connection!
                                          *connections*]]
            [datahike.readers]
            [datahike.store :as ds]
            [datahike.writing :as dsi]
            [datahike.config :as dc]
            [datahike.tools :as dt #?(:clj :refer :cljs :refer-macros) [meta-data]]
            [datahike.writer :as w]
            [konserve.core :as k]
            [konserve.store :as ks]
            [taoensso.timbre :as log]
            [clojure.spec.alpha :as s]
            [clojure.data :refer [diff]]
            [konserve.utils :refer [#?(:clj async+sync) *default-sync-translation*]
             #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async :refer [go-try- <?-]]
            [clojure.core.async :refer [go] :as async])
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
    (if (not (w/streaming? (get @wrapped-atom :writer)))
      (let [store  (:store @wrapped-atom)
            stored (k/get store (:branch (:config @wrapped-atom)) nil {:sync? true})]
        (log/trace "Fetched db for deref: " (:config stored))
        (dsi/stored->db stored store))
      @wrapped-atom)))

(defn conn-from-db
  "Creates a mutable reference to a given immutable database. See [[create-conn]]."
  [db]
  (Connection. (atom db :meta {:listeners (atom {})})))

(s/def ::connection #(and (instance? Connection %)
                          (not= @(:wrapped-atom %) :released)))

(defn version-check [{:keys [meta config] :as db}]
  (let [{dh-stored :datahike/version
         hh-stored :hitchhiker.tree/version
         pss-stored :persistent.set/version
         ksv-stored :konserve/version} meta
        {dh-now :datahike/version
         hh-now :hitchhiker.tree/version
         pss-now :persistent.set/version
         ksv-now :konserve/version} (meta-data)]
    (when-not (or (= dh-now "DEVELOPMENT")
                  (= dh-stored "DEVELOPMENT")
                  (>= (compare dh-now dh-stored) 0))
      (dt/raise "Database was written with newer Datahike version."
                {:type :db-was-written-with-newer-datahike-version
                 :stored dh-stored
                 :now dh-now
                 :config config}))
    (when (and hh-stored hh-now
               (not (>= (compare hh-now hh-stored) 0)))
      (dt/raise "Database was written with newer hitchhiker-tree version."
                {:type :db-was-written-with-newer-hht-version
                 :stored hh-stored
                 :now hh-now
                 :config config}))
    (when-not (>= (compare pss-now pss-stored) 0)
      (dt/raise "Database was written with newer persistent-sorted-set version."
                {:type :db-was-written-with-newer-pss-version
                 :stored pss-stored
                 :now pss-now
                 :config config}))
    (when-not (>= (compare ksv-now ksv-stored) 0)
      (dt/raise "Database was written with newer konserve version."
                {:type   :db-was-written-with-newer-konserve-version
                 :stored ksv-stored
                 :now    ksv-now
                 :config config}))))

(defn ensure-stored-config-consistency [config stored-config]
  (let [;; Remove runtime parameters and creation-time parameters
        config (dissoc config :name :search-cache-size :store-cache-size)
        stored-config (dissoc stored-config :initial-tx :name :search-cache-size :store-cache-size)
        stored-config (merge {:writer dc/self-writer} stored-config)
        stored-config (if (empty? (:index-config stored-config))
                        (dissoc stored-config :index-config)
                        stored-config)
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
            (dt/raise "Store identity mismatch: connecting to wrong database."
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
      (dt/raise "Configuration does not match stored configuration. In some cases this check is too restrictive. If you are sure you are loading the right database with the right configuration then you can disable this check by setting :allow-unsafe-config to true in your config."
                {:type          :config-does-not-match-stored-db
                 :config        config
                 :stored-config stored-config
                 :diff          (diff config stored-config)}))))

(defn- normalize-config [cfg]
  (-> cfg
      (dissoc :writer :store :store-cache-size :search-cache-size)))

(defn -connect-impl* [config opts]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try-
               (let [_ (log/debug "Using config " (update-in config [:store] dissoc :password))
                     store-config (:store config)
                     store-id (ds/store-identity store-config)
                     conn-id [store-id (:branch config)]]
                 (if-let [conn (get-connection conn-id)]
                   (let [conn-config (:config @(:wrapped-atom conn))
               ;; replace store config with its identity
                         cfg (normalize-config config)
                         conn-cfg (normalize-config conn-config)]
                     (when-not (= cfg conn-cfg)
                       (dt/raise "Configuration does not match existing connections."
                                 {:type :config-does-not-match-existing-connections
                                  :config cfg
                                  :existing-connections-config conn-cfg
                                  :diff (diff cfg conn-cfg)}))
                     conn)
                   (let [raw-store (<?- (ks/connect-store store-config opts))
                         _         (when-not raw-store
                                     (dt/raise "Backend does not exist." {:type   :backend-does-not-exist
                                                                          :config store-config}))
                         store     (ds/add-cache-and-handlers raw-store config)
                         _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                         stored-db (<?- (k/get store (:branch config) nil opts))
                         _         (when-not stored-db
                                     (ks/release-store store-config store opts)
                                     (dt/raise "Database does not exist." {:type   :db-does-not-exist
                                                                           :config config}))
                         [config store stored-db]
                         (let [intended-index (:index config)
                               stored-index   (get-in stored-db [:config :index])]
                           (if-not (= intended-index stored-index)
                             (do
                               (log/warn (str "Stored index does not match configuration. Please set :index explicitly to " stored-index " in config. The default index is now :datahike/persistent-set. Using stored index setting now, but this might throw an error in the future."))
                               (let [config    (assoc config :index stored-index)
                                     store     (ds/add-cache-and-handlers raw-store config)
                                     _ (<?- (ds/ready-store (assoc store-config :opts opts) store))
                                     stored-db (<?- (k/get store (:branch config) nil opts))]
                                 [config store stored-db]))
                             [config store stored-db]))
                         _ (version-check stored-db)
                         _ (when-not (:allow-unsafe-config config)
                             (ensure-stored-config-consistency config (:config stored-db)))
                         conn      (conn-from-db (dsi/stored->db (assoc stored-db :config config) store))]
                     (swap! (:wrapped-atom conn) assoc :writer
                            (w/create-writer (:writer config) conn))
                     (add-connection! conn-id conn)
                     conn))))))

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
           _ (log/info "Releasing connection, config store backend:"
                       (get-in db [:config :store :backend]))
           conn-id [(ds/store-identity (get-in db [:config :store]))
                    (get-in db [:config :branch])]]
       (if-not (get @*connections* conn-id)
         (log/info "Connection already released." conn-id)
         (let [new-conns (swap! *connections* update-in [conn-id :count] dec)]
           (when (or release-all? (zero? (get-in new-conns [conn-id :count])))
             (delete-connection! conn-id)
             (w/shutdown (:writer db))
             ;; Release the underlying store to clean up resources (memory registry, etc.)
             (ks/release-store (get-in db [:config :store]) (:store db))
             nil)))))))
