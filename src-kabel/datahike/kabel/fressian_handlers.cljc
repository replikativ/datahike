(ns ^:no-doc datahike.kabel.fressian-handlers
  "Fressian handlers for Datahike types over kabel — the SAME canonical PSS codec used for
   konserve storage (org.replikativ.persistent-sorted-set.fressian), so index nodes/roots
   round-trip identically on the wire and in the store.

   PSS nodes (pss/leaf, pss/branch) and the root (pss/set) are the canonical shared handlers;
   datahike adds its element/record handlers (Datom, DB, TxReport). A root is reconstructed
   EAGERLY (no deferred-map indirection): its storage is resolved by the `:pss/storage-id` it carries
   in meta from `pss-fress/storage-registry` — the receiving peer registers its store there before
   receiving DBs (konserve-sync shares the store `:id` across peers, so a synced root resolves to
   the receiver's local store). Legacy `datahike.index.*` tags are dual-registered against the
   canonical handlers (old payloads are subsets; a missing :branching-factor falls back)."
  (:require [datahike.datom :as dd :refer [index-type->cmp-quick]]
            [datahike.writing :as dw]
            [org.replikativ.persistent-sorted-set.fressian :as pss-fress]
            [kabel.middleware.fressian :refer [fressian]]
            #?(:clj [clojure.data.fressian :as fress]
               :cljs [fress.api :as fress])
            #?(:cljs [datahike.db :refer [DB TxReport]]))
  #?(:clj (:import [org.fressian.handlers WriteHandler ReadHandler]
                   [datahike.datom Datom]
                   [datahike.db DB TxReport])))

(def ^:const DEFAULT_BRANCHING_FACTOR 512)

;; ============================================================================
;; Store registry — delegates to the canonical pss-fress/storage-registry. A peer registers its
;; FULL datahike store (it's what stored->db needs); the root resolver derives the IStorage via
;; (:storage store). Keyed by the store-config :id (= the :pss/storage-id a root stamps in its meta).
;; ============================================================================

(defn register-store!   [store-config store] (pss-fress/register-storage! (:id store-config) store))
(defn unregister-store! [store-config] (pss-fress/unregister-storage! (:id store-config)))
(defn get-store         [store-config] (pss-fress/registered-storage (:id store-config)))
(defn clear-registry!   [] (reset! pss-fress/storage-registry {}))

(defn- reconstruct-db
  "A stored-db's index roots are already EAGER (live sets — each resolved its storage by
   :pss/storage-id while being read). Fetch the full store for stored->db from the registry by the
   stored config's store id; fall back to the raw stored map if the store isn't registered."
  [stored]
  (if-let [store (pss-fress/registered-storage (get-in stored [:config :store :id]))]
    (dw/stored->db stored store)
    stored))

;; ============================================================================
;; Read handlers — canonical PSS node/root + datahike element/record handlers.
;; ============================================================================

(def read-handlers
  (let [node-rh (pss-fress/read-handlers {:default-bf DEFAULT_BRANCHING_FACTOR})
        ;; storage resolved by :pss/storage-id from the registered store's :storage; comparator per-index.
        root-rh (pss-fress/root-read-handler
                 {:resolve-storage (fn [m] (some-> (pss-fress/registered-storage (get m pss-fress/storage-id-key)) :storage))
                  :resolve-cmp     (fn [m] (index-type->cmp-quick (:index-type m) false))
                  :default-bf      DEFAULT_BRANCHING_FACTOR})
        datom-rh    #?(:clj  (reify ReadHandler (read [_ r _ _] (dd/datom-from-reader (.readObject r))))
                       :cljs (fn [r _ _] (dd/datom-from-reader (fress/read-object r))))
        ;; DB is db->stored on the wire; reconstruct via stored->db (roots already eager). TxReport
        ;; stays a plain map — KabelWriter reconstructs it after sync completes.
        db-rh       #?(:clj  (reify ReadHandler (read [_ r _ _] (reconstruct-db (.readObject r))))
                       :cljs (fn [r _ _] (reconstruct-db (fress/read-object r))))
        txreport-rh #?(:clj  (reify ReadHandler (read [_ r _ _] (.readObject r)))
                       :cljs (fn [r _ _] (fress/read-object r)))]
    (merge node-rh
           {pss-fress/set-tag root-rh
            ;; BACKWARDS COMPAT: legacy datahike.index.* tags → the canonical handlers.
            "datahike.index.PersistentSortedSet"        root-rh
            "datahike.index.PersistentSortedSet.Leaf"   (get node-rh pss-fress/leaf-tag)
            "datahike.index.PersistentSortedSet.Branch" (get node-rh pss-fress/branch-tag)
            "datahike.datom.Datom" datom-rh
            "datahike.db.DB"       db-rh
            "datahike.db.TxReport" txreport-rh})))

;; ============================================================================
;; Write handlers — canonical PSS node/root + datahike element/record handlers.
;; ============================================================================

(def write-handlers
  (merge
   pss-fress/write-handlers
   pss-fress/root-write-handlers
   #?(:clj
      {datahike.datom.Datom
       {"datahike.datom.Datom"
        (reify WriteHandler (write [_ w d] (.writeTag w "datahike.datom.Datom" 1) (.writeObject w (vec (seq ^Datom d)))))}
       datahike.db.DB
       {"datahike.db.DB"
        (reify WriteHandler (write [_ w db]
                              (let [[_ stored] (dw/db->stored db false)]
                                (.writeTag w "datahike.db.DB" 1) (.writeObject w stored))))}
       datahike.db.TxReport
       {"datahike.db.TxReport"
        (reify WriteHandler (write [_ w tx-report]
                              (let [->stored (fn [db] (when db (second (dw/db->stored db false))))
                                    serializable (-> (into {} tx-report)
                                                     (update :db-before ->stored)
                                                     (update :db-after ->stored))]
                                (.writeTag w "datahike.db.TxReport" 1) (.writeObject w serializable))))}}
      :cljs
      {dd/Datom
       (fn [w d] (fress/write-tag w "datahike.datom.Datom" 1) (fress/write-object w (vec (seq d))))
       DB
       (fn [w db] (let [[_ stored] (dw/db->stored db false)]
                    (fress/write-tag w "datahike.db.DB" 1) (fress/write-object w stored)))
       TxReport
       (fn [w tx-report] (let [->stored (fn [db] (when db (second (dw/db->stored db false))))
                               serializable (-> (into {} tx-report)
                                                (update :db-before ->stored)
                                                (update :db-after ->stored))]
                           (fress/write-tag w "datahike.db.TxReport" 1) (fress/write-object w serializable)))})))

;; ============================================================================
;; Middleware Constructor
;; ============================================================================

(defn datahike-fressian-middleware
  "Fressian serialization middleware configured with datahike's canonical handlers — pass as the
   serialization middleware for kabel peers."
  [peer-config]
  (fressian (atom read-handlers) (atom write-handlers) peer-config))
