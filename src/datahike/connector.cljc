(ns datahike.connector
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [datahike.index :as di]
            [hitchhiker.konserve :as kons]
            [konserve.filestore :as fs]
            [konserve-leveldb.core :as kl]
            [konserve.core :as k]
            [konserve.cache :as kc]
            [konserve.memory :as mem]
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
        path (.getPath sub-uri)]
    [scheme store-scheme path]))

(def memory (atom {}))

(defn store-scheme->index [scheme]
  (case scheme
    "mem" :datahike.index/persistent-set
    "file" :datahike.index/hitchhiker-tree
    "level" :datahike.index/hitchhiker-tree
    (throw (ex-info "Unknown datahike store scheme: " scheme))))

(defn connect [uri]
  (let [[scheme store-scheme path] (parse-uri uri)
        _ (when-not (s/valid? ::scheme scheme)
            (throw (ex-info "URI cannot be parsed." {:uri uri})))
        _ (when (= store-scheme "mem")
            (when-not (@memory uri)
              (throw (ex-info
                      (str "Database does not exist at " uri "!")
                      {:type :db-does-not-exist
                       :uri uri}))))
        store (kons/add-hitchhiker-tree-handlers
               (kc/ensure-cache
                (case store-scheme
                 "mem"
                 (@memory uri)
                 "file"
                  (<?? S (fs/new-fs-store path))
                 "level"
                  (<?? S (kl/new-leveldb-store path)))
               (atom (cache/lru-cache-factory {} :threshold 1000))))
        stored-db (<?? S (k/get-in store [:db]))
        ;_ (prn stored-db)
        _ (when-not stored-db
            (case store-scheme
              "level"
              (kl/release store)
              nil)
            (throw (ex-info
                    (str "Database does not exist at " uri "!")
                    {:type :db-does-not-exist :uri uri})))
        {:keys [eavt-key aevt-key avet-key schema rschema config]} stored-db
        empty (db/empty-db)]
    (d/conn-from-db
     (assoc empty
            :config config
            :schema schema
            :max-eid (db/init-max-eid eavt-key)
            :eavt eavt-key
            :aevt aevt-key
            :avet avet-key
            :rschema rschema
            :store store
            :uri uri))))

(defn release [conn]
  (let [[m store-scheme path] (parse-uri (:uri @conn))]
    (case store-scheme
      "mem"
      nil
      "file"
      nil
      "level"
      (kl/release (:store @conn)))))

(defn transact [connection tx-data]
  {:pre [(d/conn? connection)]}
  (future
    (locking connection
      (let [{:keys [db-after] :as tx-report} @(d/transact connection tx-data)
            {:keys [eavt aevt avet schema rschema config]} db-after
            store (:store @connection)
            backend (kons/->KonserveBackend store)
            eavt-flushed (di/-flush eavt backend)
            aevt-flushed (di/-flush aevt backend)
            avet-flushed (di/-flush avet backend)]
        (<?? S (k/assoc-in store [:db]
                           {:schema schema
                            :rschema rschema
                            :config config
                            :eavt-key eavt-flushed
                            :aevt-key aevt-flushed
                            :avet-key avet-flushed}))
        (reset! connection (assoc db-after
                                  :eavt eavt-flushed
                                  :aevt aevt-flushed
                                  :avet avet-flushed))
        tx-report))))

(defn transact! [connection tx-data]
  (try
    (deref (transact connection tx-data))
    (catch Exception e
      (throw (.getCause e)))))

(defn create-database
  ([uri]
   (create-database uri nil))
  ([uri initial-tx]
   (let [[m store-scheme path] (parse-uri uri)
         _ (when-not m
             (throw (ex-info "URI cannot be parsed." {:uri uri})))
         store (kc/ensure-cache
                (case store-scheme
                  "mem"
                  (let [store (<?? S (mem/new-mem-store))]
                    (swap! memory assoc uri store)
                    store)
                  "file"
                  (kons/add-hitchhiker-tree-handlers
                   (<?? S (fs/new-fs-store path)))
                  "level"
                  (kons/add-hitchhiker-tree-handlers
                   (<?? S (kl/new-leveldb-store path))))
                (atom (cache/lru-cache-factory {} :threshold 1000))) ;; TODO: move store to separate ns
         stored-db (<?? S (k/get-in store [:db]))
         _ (when stored-db
             (throw (ex-info "Database already exists." {:type :db-already-exists :uri uri})))
         index-type (store-scheme->index store-scheme)
         config {:schema-on-read false}
         {:keys [eavt aevt avet schema rschema config]} (db/empty-db {} index-type :config config)
         backend (kons/->KonserveBackend store)]
     (<?? S (k/assoc-in store [:db]
                        {:schema schema
                         :rschema rschema
                         :config config
                         :eavt-key (di/-flush eavt backend)
                         :aevt-key (di/-flush aevt backend)
                         :avet-key (di/-flush avet backend)}))
     (case store-scheme
       "level"
       (kl/release store)
       nil)
     (when initial-tx
       (let [conn (connect uri)]
         (transact! conn initial-tx)
         (release conn))))))

(defn delete-database [uri]
  (let [[m store-scheme path] (parse-uri uri)]
    (case store-scheme
      "mem"
      (swap! memory dissoc uri)
      "file"
      (fs/delete-store path)
      "level"
      (kl/delete-store path))))
