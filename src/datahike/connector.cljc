(ns datahike.connector
  (:require [datahike.db :as db]
            [datahike.core :as d]
            [datahike.index :as di]
            [datahike.store :as ds]
            [datahike.config :as dc]
            [datahike.tools :as dt]
            [hitchhiker.tree.bootstrap.konserve :as kons]
            [konserve.core :as k]
            [konserve.cache :as kc]
            [clojure.core.async :as async]
            [hitchhiker.tree.utils.cljs.async :as ha]
            [superv.async :refer [S go-try <?]]
            [taoensso.timbre :as log]
            [clojure.spec.alpha :as s]
            #?(:cljs [konserve.indexeddb :refer [collect-indexeddb-stores]])
            #?(:cljs [cljs.core.async.interop :refer-macros [<p!]])
            #?(:clj [clojure.core.cache :as cache])
            #?(:cljs [cljs.cache :as cache]))
  #?(:clj (:import [java.net URI])))

   (s/def ::connection #(instance? #?(:clj clojure.lang.Atom
                                      :cljs Atom) %))

   (defn update-and-flush-db [connection tx-data update-fn]
     (ha/go-try
      (let [update-fn-result (update-fn connection tx-data)
            fn-take (ha/<? update-fn-result)
            {:keys [db-after] :as tx-report} @fn-take
            {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet schema rschema config max-tx hash]} db-after
            store (:store @connection)
            backend (kons/->KonserveBackend store)
            eavt-flushed (ha/<? (di/-flush eavt backend))
            aevt-flushed (ha/<? (di/-flush aevt backend))
            avet-flushed (ha/<? (di/-flush avet backend))
            keep-history? (:keep-history? config)
            temporal-eavt-flushed (when keep-history? (ha/<? (di/-flush temporal-eavt backend)))
            temporal-aevt-flushed (when keep-history? (ha/<? (di/-flush temporal-aevt backend)))
            temporal-avet-flushed (when keep-history? (ha/<? (di/-flush temporal-avet backend)))]
        (<? S (k/assoc-in store [:db]
                           (merge
                            {:schema   schema
                             :rschema  rschema
                             :config   config
                             :hash hash
                             :max-tx max-tx
                             :eavt-key eavt-flushed
                             :aevt-key aevt-flushed
                             :avet-key avet-flushed}
                            (when keep-history?
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
        tx-report)))
   

   (defn transact!                    ;; TODO: consider this as async on both clj and cljs
     [connection {:keys [tx-data]}]
     {:pre [(d/conn? connection)]}
     #?(:clj
        (future
          (locking connection
            (update-and-flush-db connection tx-data d/transact)))
        :cljs (go-try S
                      (let [l (:lock @connection)]
                        (try
                          (async/<! l)
                          (<? S (update-and-flush-db connection tx-data d/transact))
                          (finally
                            (async/put! l :unlocked)))))))

   (defn transact [connection arg-map]
     (let [arg (cond
                 (and (map? arg-map) (contains? arg-map :tx-data)) arg-map
                 (vector? arg-map) {:tx-data arg-map}
                 (seq? arg-map) {:tx-data arg-map}
                 :else (dt/raise "Bad argument to transact, expected map with :tx-data as key.
                               Vector and sequence are allowed as argument but deprecated."
                                 {:error :transact/syntax :argument arg-map}))
           _ (log/debugf "Transacting with arguments: " arg)]
       #?(:cljs (go-try S
                        (try
                          (<? S (transact! connection arg))
                          (catch js/Error e
                            (log/errorf "Error during transaction %s" (str e))
                            (throw e))))
          :clj (try
                 (deref (transact! connection arg))
                 (catch Exception e
                   (log/errorf "Error during transaction %s" (.getMessage e))
                   (throw (.getCause e)))))))

   (defn load-entities [connection entities]
     #?(:clj (future
               (locking connection
                 (update-and-flush-db connection entities d/load-entities)))
        :cljs (throw (js/Error. "TODO: transact! inside of connector"))))

   (defn release [connection]
     (ds/release-store (get-in @connection [:config :store]) (:store @connection)))
   
   (defn memory-store? [config]
     (= :mem (get-in config [:store :backend])))
   

;; deprecation begin
   (defprotocol IConfiguration
     (-connect [config])
     (-create-database [config #_opts])
     (-delete-database [config])
     (-database-exists? [config]))

   (extend-protocol IConfiguration
     #_String
     #_(-connect [uri]
                 (-connect (dc/uri->config uri)))

     #_(-create-database [uri & opts]
                         (apply -create-database (dc/uri->config uri) opts))

     #_(-delete-database [uri]
                         (-delete-database (dc/uri->config uri)))

     #_(-database-exists? [uri]
                          (-database-exists? (dc/uri->config uri)))

     #?(:clj clojure.lang.IPersistentMap
        :cljs cljs.core/PersistentArrayMap)

     (-database-exists? [config]
       (async/go
         (let [exists? (or (memory-store? config)
                           (contains? (ha/<? (collect-indexeddb-stores))  ;; TODO: ensure that this is our db
                                      (get-in config [:store :id])))]
           (if exists?
             (let [config (dc/load-config config)
                   store-config (:store config)
                   raw-store (ha/<? (ds/connect-store store-config))] 
               (if (not (nil? raw-store))
                 (let [store (kons/add-hitchhiker-tree-handlers
                              (kc/ensure-cache
                               raw-store
                               (atom (cache/lru-cache-factory {} :threshold 1000))))
                       stored-db (<? S (k/get-in store [:db]))]
                   (ds/release-store store-config store)
                   (not (nil? stored-db)))
                 (do
                   (ds/release-store store-config raw-store)
                   false)))
             false))))

     (-connect [config]
       (go-try S
               (if-not (ha/<? (-database-exists? config))
                 (do (println "Database doesn't exist ") nil)  ;; Log this to user
                 (let [config (dc/load-config config)
                       store-config (:store config)
                       raw-store (ha/<? (ds/connect-store store-config))
                       _ (when-not raw-store
                           (dt/raise "Backend does not exist." {:type :backend-does-not-exist
                                                                :config config}))
                       store (kons/add-hitchhiker-tree-handlers
                              (kc/ensure-cache
                               raw-store
                               (atom (cache/lru-cache-factory {} :threshold 1000))))
                       stored-db (<? S (k/get-in store [:db]))
                       _ (when-not stored-db
                               (ds/release-store store-config store)
                               (dt/raise "Database does not exist." {:type :db-does-not-exist
                                                                     :config config}))
                       {:keys [eavt-key aevt-key avet-key temporal-eavt-key temporal-aevt-key temporal-avet-key schema rschema config max-tx hash]} stored-db
                       empty (<? S (db/empty-db nil config))
                       lock-ch (async/chan) ;; TODO: consider reader literals
                       _ (async/put! lock-ch :unlocked)]
                   (d/conn-from-db
                    (assoc empty
                           :max-tx max-tx
                           :config config
                           :schema schema
                           :hash hash
                           :max-eid (<? S (db/init-max-eid eavt-key))
                           :eavt eavt-key
                           :aevt aevt-key
                           :avet avet-key
                           :temporal-eavt temporal-eavt-key
                           :temporal-aevt temporal-aevt-key
                           :temporal-avet temporal-avet-key
                           :rschema rschema
                           :store store
                           :lock lock-ch))))))


     (-create-database [config #_& #_deprecated-config]
       (ha/go-try
        (if (ha/<? (-database-exists? config))
          (do (println "Database already exists ") nil) ;; Log this to user
          (let [{:keys [keep-history? initial-tx] :as config} (dc/load-config config nil #_deprecated-config)
                store-config (:store config)
                store (kc/ensure-cache
                       (ha/<? (ds/empty-store store-config))
                       (atom (cache/lru-cache-factory {} :threshold 1000)))
                stored-db (<? S (k/get-in store [:db]))
                _ (when stored-db
                    (dt/raise "Database already exists." {:type :db-already-exists :config store-config}))
                empty-db-test  (ha/<? (db/empty-db nil config))
                {:keys [eavt aevt avet temporal-eavt temporal-aevt temporal-avet schema rschema config max-tx hash]}
                empty-db-test
                backend (kons/->KonserveBackend store)]
            (<? S (k/assoc-in store [:db]
                              (merge {:schema   schema
                                      :max-tx max-tx
                                      :hash hash
                                      :rschema  rschema
                                      :config   config
                                      :eavt-key (ha/<? (di/-flush eavt backend))
                                      :aevt-key (ha/<? (di/-flush aevt backend))
                                      :avet-key (ha/<? (di/-flush avet backend))}
                                     (when keep-history?
                                       {:temporal-eavt-key (ha/<? (di/-flush temporal-eavt backend))
                                        :temporal-aevt-key (ha/<? (di/-flush temporal-aevt backend))
                                        :temporal-avet-key (ha/<? (di/-flush temporal-avet backend))}))))
            (ds/release-store store-config store)
            (when initial-tx
              (let [conn (<? S (-connect config))]
                (<? S (transact conn initial-tx))
                (release conn)
                (println "Database initialised: " (get-in config [:store :id]))))))))

     (-delete-database [config]
                       (ha/go-try
                        (if (ha/<? (-database-exists? config))
                          (let [config (dc/load-config config {})]
                            (ds/delete-store (:store config)))
                          (do (println "Database doesn't exist") nil)))))

   (defn connect
     ([]
      (-connect {}))
     ([config]
      (-connect config)))
;;deprecation end

   


   (defn create-database
     ([]
      (-create-database {} #_nil))
     ([config #_opts]
      (-create-database config #_opts)))

   (defn delete-database
     ([]
      (-delete-database {}))
  ;;deprecated
     ([config]
   ;; TODO log deprecation notice with #54
      (-delete-database config)))

   (defn database-exists?
     ([]
      (-database-exists? {}))
     ([config]
   ;; TODO log deprecation notice with #54
      (-database-exists? config)))
