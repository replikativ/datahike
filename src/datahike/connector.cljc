(ns datahike.connector
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [datahike.index :as di]
            [datahike.store :as ds]
            [hitchhiker.konserve :as kons]
            [konserve.core :as k]
            [konserve.cache :as kc]
            [superv.async :refer [<?? S]]
            [clojure.spec.alpha :as s]
            [clojure.core.cache :as cache])
  (:import [java.net URI]))

(s/def ::scheme #{"datahike"})
(s/def ::store-scheme #{"mem" "file" "level"})
(s/def ::uri-config (s/cat :meta string?
                           :store-scheme ::store-scheme
                           :path string?))

(s/fdef parse-uri
        :args (s/cat :uri string?)
        :ret ::uri-config)

(defn- parse-uri [uri]
(let [base-uri (URI. uri)
        scheme (.getScheme base-uri)
        sub-uri (URI. (.getSchemeSpecificPart base-uri))
        store-scheme (.getScheme sub-uri)
        path (if (=  store-scheme "pg")
               (.getSchemeSpecificPart sub-uri)
                 (.getPath sub-uri))]
    [scheme store-scheme path ]))

(s/def ::connection #(instance? clojure.lang.Atom %))

(defn connect [uri]
  (let [[scheme store-scheme path] (parse-uri uri)
        store (kons/add-hitchhiker-tree-handlers
               (kc/ensure-cache
                (ds/connect-store store-scheme path)
               (atom (cache/lru-cache-factory {} :threshold 1000))))
        stored-db (<?? S (k/get-in store [:db]))
        ;_ (prn stored-db)
        _ (when-not stored-db
            (ds/release-store store-scheme store)
            (throw (ex-info "Database does not exist." {:type :db-does-not-exist
                                                  :uri uri})))
        {:keys [eavt-key aevt-key avet-key temporal-eavt-key temporal-aevt-key temporal-avet-key schema rschema config max-tx]} stored-db
        empty (db/empty-db nil :datahike.index/hitchhiker-tree :config config)]
    (d/conn-from-db
      (assoc empty
        :max-tx max-tx
        :config config
        :schema schema
        :max-eid (db/init-max-eid eavt-key)
        :eavt eavt-key
        :aevt aevt-key
        :avet avet-key
        :temporal-eavt temporal-eavt-key
        :temporal-aevt temporal-aevt-key
        :temporal-avet temporal-avet-key
        :rschema rschema
        :store store
        :uri uri))))

(defn release [conn]
  (let [[m store-scheme path] (parse-uri (:uri @conn))]
    (ds/release-store store-scheme (:store @conn))))

(defn transact [connection tx-data]
  {:pre [(d/conn? connection)]}
  (future
    (locking connection
      (let [{:keys [db-after] :as tx-report} @(d/transact connection tx-data)
            {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet schema rschema config max-tx]} db-after
            store (:store @connection)
            backend (kons/->KonserveBackend store)
            eavt-flushed (di/-flush eavt backend)
            aevt-flushed (di/-flush aevt backend)
            avet-flushed (di/-flush avet backend)
            temporal-index? (:temporal-index config)
            temporal-eavt-flushed (when temporal-index? (di/-flush temporal-eavt backend))
            temporal-aevt-flushed (when temporal-index? (di/-flush temporal-aevt backend))
            temporal-avet-flushed (when temporal-index? (di/-flush temporal-avet backend))]
        (<?? S (k/assoc-in store [:db]
                           (merge
                            {:schema   schema
                             :rschema  rschema
                             :config   config
                             :max-tx max-tx
                             :eavt-key eavt-flushed
                             :aevt-key aevt-flushed
                             :avet-key avet-flushed}
                            (when temporal-index?
                              {:temporal-eavt-key temporal-eavt-flushed
                               :temporal-aevt-key temporal-aevt-flushed
                               :temporal-avet-key temporal-avet-flushed}))))
        (reset! connection (assoc db-after
                             :eavt eavt-flushed
                             :aevt aevt-flushed
                             :avet avet-flushed
                             :temporal-eavt temporal-eavt-flushed
                             :temporal-aevt temporal-aevt-flushed
                             :temporal-avet temporal-avet-flushed))
        tx-report))))

(defn transact! [connection tx-data]
  (try
    (deref (transact connection tx-data))
    (catch Exception e
      (throw (.getCause e)))))

(defmulti create-database
          "Creates a new database"
          {:arglists '([config])}
          (fn [config] (type config)))

(defmethod create-database String [uri]
  (create-database {:uri uri}))

(defmethod create-database clojure.lang.PersistentArrayMap
  [{:keys [uri initial-tx schema-on-read temporal-index]}]
  (let [[m store-scheme path] (parse-uri uri)
        _ (when-not m
            (throw (ex-info "URI cannot be parsed." {:uri uri})))
         store (kc/ensure-cache
                  (ds/empty-store store-scheme path)
                  (atom (cache/lru-cache-factory {} :threshold 1000)))
        stored-db (<?? S (k/get-in store [:db]))
        _ (when stored-db
            (throw (ex-info "Database already exists." {:type :db-already-exists :uri uri})))
        temporal-index (if (nil? temporal-index)
                                  true
                                  temporal-index)
        config {:schema-on-read (or schema-on-read false)
                :temporal-index temporal-index}
        {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet schema rschema config max-tx]} (db/empty-db {:db/ident {:db/unique :db.unique/identity}} (ds/scheme->index store-scheme) :config config)
        backend (kons/->KonserveBackend store)]
    (<?? S (k/assoc-in store [:db]
                       (merge {:schema   schema
                               :max-tx max-tx
                               :rschema  rschema
                               :config   config
                               :eavt-key (di/-flush eavt backend)
                               :aevt-key (di/-flush aevt backend)
                               :avet-key (di/-flush avet backend)}
                              (when temporal-index
                                {:temporal-eavt-key (di/-flush temporal-eavt backend)
                                 :temporal-aevt-key (di/-flush temporal-aevt backend)
                                 :temporal-avet-key (di/-flush temporal-avet backend)}))))
    (ds/release-store store-scheme store)
    (when initial-tx
      (let [conn (connect uri)]
        (transact! conn initial-tx)
        (release conn)))))

(defn delete-database [uri]
  (let [[m store-scheme path] (parse-uri uri)]
    (ds/delete-store store-scheme path)))
