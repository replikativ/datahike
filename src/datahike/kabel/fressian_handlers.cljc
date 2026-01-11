(ns datahike.kabel.fressian-handlers
  "Fressian handlers for Datahike types over kabel.

   Provides read/write handlers for:
   - PersistentSortedSet / BTSet
   - Leaf / Branch nodes
   - Datom
   - DB (via db->stored format)
   - TxReport

   These handlers enable Datahike index structures to be serialized over
   kabel (WebSocket) using Fressian, which is the same format used for
   konserve storage - avoiding remarshalling overhead.

   CLJS uses a store registry pattern: register the TieredStore on app start,
   then the read handler looks up storage from the registry to construct BTSets."
  (:require [datahike.datom :as dd :refer [index-type->cmp-quick]]
            [datahike.writing :as dw]
            [kabel.middleware.fressian :refer [fressian]]
            #?(:clj [clojure.data.fressian :as fress]
               :cljs [fress.api :as fress])
            #?(:cljs [me.tonsky.persistent-sorted-set.btset :refer [BTSet]])
            #?(:cljs [me.tonsky.persistent-sorted-set.branch :refer [Branch]])
            #?(:cljs [me.tonsky.persistent-sorted-set.leaf :refer [Leaf]])
            #?(:cljs [datahike.db :refer [DB TxReport]]))
  #?(:clj (:import [org.fressian.handlers WriteHandler ReadHandler]
                   [me.tonsky.persistent_sorted_set PersistentSortedSet Leaf Branch Settings]
                   [datahike.datom Datom]
                   [datahike.db DB TxReport]
                   [java.util List])))

;; ============================================================================
;; Constants
;; ============================================================================

(def ^:const DEFAULT_BRANCHING_FACTOR 512)

;; ============================================================================
;; Store Registry
;; ============================================================================

;; Registry maps store-config to storage for BTSet reconstruction,
;; and store for DB reconstruction via stored->db.
;; Register your store after creation, before receiving DBs over kabel.
(defonce store-registry (atom {}))

(defn store-identity-for-registry
  "Extract identity for registry matching.
   Uses :id which uniquely identifies the store across machines."
  [store-config]
  (:id store-config))

(defn register-store!
  "Register a store in the registry by store-config.
   Call this after creating/connecting the store."
  [store-config store]
  (let [identity (store-identity-for-registry store-config)]
    (swap! store-registry assoc identity store)))

(defn get-store
  "Get store from registry by store-config.
   The store-config should include :id to match stores across
   client/server that are synced via konserve-sync."
  [store-config]
  (get @store-registry (store-identity-for-registry store-config)))

(defn unregister-store!
  "Remove a store from the registry.
   Call this when closing a connection or on error cleanup."
  [store-config]
  (let [identity (store-identity-for-registry store-config)]
    (swap! store-registry dissoc identity)))

(defn clear-registry!
  "Clear all stores from the registry.
   Useful for testing or when resetting application state."
  []
  (reset! store-registry {}))

;; ============================================================================
;; Deferred Index Reconstruction
;; ============================================================================

(defn reconstruct-deferred-index
  "Reconstruct a PersistentSortedSet/BTSet from deferred format.
   Returns nil if not a deferred format map.

   Parameters:
   - deferred-map: {:deferred-type :persistent-sorted-set :meta ... :address ... :count ...}
   - storage: IStorage implementation from prepared store"
  [deferred-map storage]
  (when (and (map? deferred-map)
             (= (:deferred-type deferred-map) :persistent-sorted-set))
    (let [{:keys [meta address count]} deferred-map
          cmp (index-type->cmp-quick (:index-type meta) false)
          #?@(:clj [settings (Settings. (int DEFAULT_BRANCHING_FACTOR) nil)])]
      #?(:clj (PersistentSortedSet. meta cmp address storage nil count settings 0)
         :cljs (BTSet. nil count cmp meta nil storage address {:branching-factor DEFAULT_BRANCHING_FACTOR})))))

(defn reconstruct-deferred-indexes
  "Process a stored-db map, reconstructing any deferred indexes.
   Returns the stored-db with proper PersistentSortedSet/BTSet instances.

   Parameters:
   - stored: The stored-db map (may have deferred index format)
   - storage: IStorage implementation from prepared store"
  [stored storage]
  (let [process (fn [v]
                  (if-let [reconstructed (reconstruct-deferred-index v storage)]
                    reconstructed
                    v))]
    (-> stored
        (update :eavt-key process)
        (update :aevt-key process)
        (update :avet-key process)
        (update :temporal-eavt-key process)
        (update :temporal-aevt-key process)
        (update :temporal-avet-key process))))

;; ============================================================================
;; Read Handlers
;; ============================================================================

(def read-handlers
  "Fressian read handlers for Datahike types.
   CLJS: Constructs BTSet directly using storage from registry.
   CLJ: Returns deferred data (server doesn't need registry pattern)."
  {"datahike.index.PersistentSortedSet"
   ;; PersistentSortedSet is deserialized as deferred data.
   ;; The actual reconstruction happens in stored->db which has access to the store.
   #?(:clj
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (let [{:keys [meta address count]} (.readObject reader)]
            {:deferred-type :persistent-sorted-set
             :meta meta
             :address address
             :count count})))
      :cljs
      (fn [reader _tag _component-count]
        (let [{:keys [meta address count]} (fress/read-object reader)]
          {:deferred-type :persistent-sorted-set
           :meta meta
           :address address
           :count count})))

   "datahike.index.PersistentSortedSet.Leaf"
   #?(:clj
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (let [{:keys [keys _level]} (.readObject reader)
                settings (Settings. (int DEFAULT_BRANCHING_FACTOR) nil)]
            (Leaf. ^List keys settings))))
      :cljs
      (fn [reader _tag _component-count]
        (let [{:keys [keys _level]} (fress/read-object reader)]
          ;; CLJS Leaf deftype: [keys settings]
          (Leaf. (clj->js keys) {:branching-factor DEFAULT_BRANCHING_FACTOR}))))

   "datahike.index.PersistentSortedSet.Branch"
   #?(:clj
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (let [{:keys [keys level addresses]} (.readObject reader)
                settings (Settings. (int DEFAULT_BRANCHING_FACTOR) nil)]
            (Branch. (int level) ^List keys ^List (seq addresses) settings))))
      :cljs
      (fn [reader _tag _component-count]
        (let [{:keys [keys level addresses]} (fress/read-object reader)]
          ;; CLJS Branch deftype: [level keys children addresses settings]
          ;; children=nil for lazy loading
          (Branch. (int level) (clj->js keys) nil (clj->js addresses) {:branching-factor DEFAULT_BRANCHING_FACTOR}))))

   "datahike.datom.Datom"
   #?(:clj
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (dd/datom-from-reader (.readObject reader))))
      :cljs
      (fn [reader _tag _component-count]
        (dd/datom-from-reader (fress/read-object reader))))

   ;; DB is serialized via db->stored format - deserialize via stored->db
   ;; Looks up store by :id from config. Store must be registered with
   ;; matching :id via register-store! before receiving DBs.
   ;; Reconstructs deferred indexes before calling stored->db.
   "datahike.db.DB"
   #?(:clj
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (let [stored (.readObject reader)
                store-config (:store (:config stored))]
            (if-let [store (get-store store-config)]
              (let [storage (:storage store)
                    ;; Reconstruct deferred indexes before stored->db
                    processed (reconstruct-deferred-indexes stored storage)]
                (dw/stored->db processed store))
              ;; Fall back to stored map if no store registered with matching scope
              stored))))
      :cljs
      (fn [reader _tag _component-count]
        (let [stored (fress/read-object reader)
              store-config (:store (:config stored))]
          (if-let [store (get-store store-config)]
            (let [storage (:storage store)
                  ;; Reconstruct deferred indexes before stored->db
                  processed (reconstruct-deferred-indexes stored storage)]
              (dw/stored->db processed store))
            ;; Fall back to stored map if no store registered with matching scope
            stored))))

   ;; TxReport is serialized as plain map with stored db-before/db-after.
   ;; We DON'T reconstruct here because sync hasn't completed yet.
   ;; KabelWriter reconstructs after sync completes.
   "datahike.db.TxReport"
   #?(:clj
      (reify ReadHandler
        (read [_ reader _tag _component-count]
          (.readObject reader)))
      :cljs
      (fn [reader _tag _component-count]
        (fress/read-object reader)))})

;; ============================================================================
;; Write Handlers
;; ============================================================================

(def write-handlers
  "Fressian write handlers for Datahike types.
   CLJ format: nested {Type {\"tag\" handler}} for clojure.data.fressian
   CLJS format: flat {Type handler-fn} for fress library"
  #?(:clj
     ;; CLJ: nested format with tag -> handler mapping
     {me.tonsky.persistent_sorted_set.PersistentSortedSet
      {"datahike.index.PersistentSortedSet"
       (reify WriteHandler
         (write [_ writer pset]
           (when (nil? (.-_address ^PersistentSortedSet pset))
             (throw (ex-info "Must be flushed." {:type :must-be-flushed
                                                 :pset pset})))
           (.writeTag writer "datahike.index.PersistentSortedSet" 1)
           (.writeObject writer {:meta (meta pset)
                                 :address (.-_address ^PersistentSortedSet pset)
                                 :count (count pset)})))}

      me.tonsky.persistent_sorted_set.Leaf
      {"datahike.index.PersistentSortedSet.Leaf"
       (reify WriteHandler
         (write [_ writer leaf]
           (.writeTag writer "datahike.index.PersistentSortedSet.Leaf" 1)
           (.writeObject writer {:level (.level ^Leaf leaf)
                                 :keys (.keys ^Leaf leaf)})))}

      me.tonsky.persistent_sorted_set.Branch
      {"datahike.index.PersistentSortedSet.Branch"
       (reify WriteHandler
         (write [_ writer node]
           (.writeTag writer "datahike.index.PersistentSortedSet.Branch" 1)
           (.writeObject writer {:level (.level ^Branch node)
                                 :keys (.keys ^Branch node)
                                 :addresses (.addresses ^Branch node)})))}

      datahike.datom.Datom
      {"datahike.datom.Datom"
       (reify WriteHandler
         (write [_ writer datom]
           (.writeTag writer "datahike.datom.Datom" 1)
           (.writeObject writer (vec (seq ^Datom datom)))))}

      ;; DB is serialized via db->stored format (plain map with index refs)
      datahike.db.DB
      {"datahike.db.DB"
       (reify WriteHandler
         (write [_ writer db]
           (let [[_schema-meta-kv stored] (dw/db->stored db false)]
             (.writeTag writer "datahike.db.DB" 1)
             (.writeObject writer stored))))}

      ;; TxReport is serialized as plain map with db-before/db-after converted
      datahike.db.TxReport
      {"datahike.db.TxReport"
       (reify WriteHandler
         (write [_ writer tx-report]
           (let [db->stored-map (fn [db]
                                  (when db
                                    (second (dw/db->stored db false))))
                 serializable (-> (into {} tx-report)
                                  (update :db-before db->stored-map)
                                  (update :db-after db->stored-map))]
             (.writeTag writer "datahike.db.TxReport" 1)
             (.writeObject writer serializable))))}}

     :cljs
     ;; CLJS: flat format {Type handler-fn} for fress library
     {BTSet
      (fn [writer pset]
        (when (nil? (.-address ^BTSet pset))
          (throw (ex-info "Must be flushed." {:type :must-be-flushed
                                              :pset pset})))
        (fress/write-tag writer "datahike.index.PersistentSortedSet" 1)
        (fress/write-object writer {:meta (meta pset)
                                    :address (.-address ^BTSet pset)
                                    :count (count pset)}))

      Leaf
      (fn [writer leaf]
        (fress/write-tag writer "datahike.index.PersistentSortedSet.Leaf" 1)
        (fress/write-object writer {:level 0 ;; not supported in cljs
                                    :keys (vec (.-keys ^Leaf leaf))}))

      Branch
      (fn [writer node]
        (fress/write-tag writer "datahike.index.PersistentSortedSet.Branch" 1)
        (fress/write-object writer {:level (.-level ^Branch node)
                                    :keys (vec (.-keys ^Branch node))
                                    :addresses (vec (.-addresses ^Branch node))}))

      dd/Datom
      (fn [writer datom]
        (fress/write-tag writer "datahike.datom.Datom" 1)
        (fress/write-object writer (vec (seq datom))))

      ;; DB is serialized via db->stored format (plain map with index refs)
      DB
      (fn [writer db]
        (let [[_schema-meta-kv stored] (dw/db->stored db false)]
          (fress/write-tag writer "datahike.db.DB" 1)
          (fress/write-object writer stored)))

      ;; TxReport is serialized as plain map with db-before/db-after converted
      TxReport
      (fn [writer tx-report]
        (let [db->stored-map (fn [db]
                               (when db
                                 (second (dw/db->stored db false))))
              serializable (-> (into {} tx-report)
                               (update :db-before db->stored-map)
                               (update :db-after db->stored-map))]
          (fress/write-tag writer "datahike.db.TxReport" 1)
          (fress/write-object writer serializable)))}))

;; ============================================================================
;; Middleware Constructor
;; ============================================================================

(defn datahike-fressian-middleware
  "Create a Fressian middleware configured with Datahike handlers.
   Use this as the serialization middleware for kabel peers.

   Example:
   (peer/server-peer S handler server-id
                     remote-middleware
                     datahike-fressian-middleware)"
  [peer-config]
  (fressian (atom read-handlers)
            (atom write-handlers)
            peer-config))
