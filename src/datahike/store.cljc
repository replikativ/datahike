(ns ^:no-doc datahike.store
  (:require [clojure.spec.alpha :as s]
            #?(:clj [konserve.filestore :as fs])
            #?(:cljs [konserve.indexeddb :as idb])
            [konserve.tiered :as kt]
            [konserve.memory :as mem]
            [environ.core :refer [env]]
            [datahike.index :as di]
            [datahike.tools :as dt]
            [konserve.cache :as kc]
            #?(:clj [clojure.core.cache :as cache]
               :cljs [cljs.cache :as cache])
            [taoensso.timbre :refer [info]]
            [zufall.core :refer [rand-german-mammal]]
            [konserve.utils :refer [#?(:clj async+sync) *default-sync-translation*] #?@(:cljs [:refer-macros [async+sync]])]
            [superv.async #?(:clj :refer :cljs :refer-macros) [go-try- <?-]]
            [clojure.core.async :refer [go] :as async])
  #?(:clj (:import [java.nio.file Paths])))

(defn add-cache-and-handlers [raw-store config]
  (cond->> (kc/ensure-cache
            raw-store
            (atom (cache/lru-cache-factory {} :threshold (:store-cache-size config))))
    (not= :mem (get-in config [:backend :store]))
    (di/add-konserve-handlers config)))

(defmulti store-identity
  "Value that identifies the underlying store."
  {:arglist '([config])}
  :backend)

(defmulti empty-store
  "Creates an empty store"
  {:arglists '([config])}
  :backend)

(defmethod empty-store :default [{:keys [backend]}]
  (throw (#?(:clj IllegalArgumentException. :cljs js/Error.) (str "Can't create a store with scheme: " backend))))

(defmulti delete-store
  "Deletes an existing store"
  {:arglists '([config])}
  :backend)

(defmethod delete-store :default [{:keys [backend]}]
  (throw (#?(:clj IllegalArgumentException. :cljs js/Error.) (str "Can't delete a store with scheme: " backend))))

(defmulti connect-store
  "Makes a connection to an existing store"
  {:arglists '([config])}
  :backend)

(defmethod connect-store :default [{:keys [backend]}]
  (throw (#?(:clj IllegalArgumentException. :cljs js/Error.) (str "Can't connect to store with scheme: " backend))))

(defmulti ready-store
  "Will be notified when the store is ready to use."
  {:arglists '([config store])}
  (fn [{:keys [backend]} _store]
    backend))

(defmethod ready-store :default [{:keys [opts]} _]
  (async+sync (:sync? opts) *default-sync-translation*
              (go-try- true)))

(defmulti release-store
  "Releases the connection to an existing store (optional)."
  {:arglists '([config store])}
  (fn [{:keys [backend]} _store]
    backend))

(defmethod release-store :default [_ _]
  nil)

(defmulti default-config
  "Defines default configuration"
  {:arglists '([config])}
  :backend)

(defmethod default-config :default [{:keys [backend] :as config}]
  (info "No default configuration found for" backend)
  config)

(defmulti config-spec
  "Returns spec for the store configuration"
  {:arglists '([config])}
  :backend)

(defmethod config-spec :default [{:keys [backend]}]
  (info "No configuration spec found for" backend))

;; memory

(def memory (atom {}))

(defmethod store-identity :mem
  [config]
  [:mem (:scope config) (:id config)])

(defmethod empty-store :mem [{:keys [id opts]}]
  (let [opts (or opts {:sync? true})]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (if-let [store (get @memory id)]
                   store
                   (let [store (<?- (mem/new-mem-store (atom {}) opts))]
                     (swap! memory assoc id store)
                     store))))))

(defmethod delete-store :mem [{:keys [id]}]
  (swap! memory dissoc id)
  nil)

(defmethod connect-store :mem [{:keys [id opts]}]
  (let [opts (or opts {:sync? true})]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (get @memory id)))))

(defmethod default-config :mem [config]
  (merge
   {:id (:datahike-store-id env (rand-german-mammal))
    :scope (dt/get-hostname)}
   config))

(s/def :datahike.store.mem/backend #{:mem})
(s/def :datahike.store.mem/id string?)
(s/def ::mem (s/keys :req-un [:datahike.store.mem/backend
                              :datahike.store.mem/id]))

(defmethod config-spec :mem [_config] ::mem)

;; file
#?(:clj
   (defmethod store-identity :file [config]
     [:file (:scope config) (:path config)]))

#?(:clj
   (defmethod empty-store :file [{:keys [path config opts]}]
     (fs/connect-fs-store path :opts (or opts {:sync? true}) :config config)))

#?(:clj
   (defmethod delete-store :file [{:keys [path]}]
     (fs/delete-store path)))

#?(:clj
   (defmethod connect-store :file [{:keys [path config opts]}]
     (fs/connect-fs-store path :opts (or opts {:sync? true}) :config config)))

#?(:clj
   (defn- get-working-dir []
     (.toString (.toAbsolutePath (Paths/get "" (into-array String []))))))

#?(:clj
   (defmethod default-config :file [config]
     (merge
      {:path  (:datahike-store-path env (str (get-working-dir) "/datahike-db-" (rand-german-mammal)))
       :scope (dt/get-hostname)}
      config)))

(s/def :datahike.store.file/path string?)
(s/def :datahike.store.file/backend #{:file})
(s/def :datahike.store.file/scope string?)
(s/def ::file (s/keys :req-un [:datahike.store.file/backend
                               :datahike.store.file/path
                               :datahike.store.file/scope]))

(defmethod config-spec :file [_] ::file)

;; indexeddb
#?(:cljs
   (defmethod store-identity :indexeddb [config]
     [:indexeddb (:name config)]))

#?(:cljs
   (defmethod empty-store :indexeddb [{:keys [name config opts]}]
     (assert (false? (:sync? opts)) "IndexedDB store connections must be async")
     (idb/connect-idb-store name :config config)))

#?(:cljs
   (defmethod delete-store :indexeddb [{:keys [name]}]
     (idb/delete-idb name)))

#?(:cljs
   (defmethod connect-store :indexeddb [{:keys [name config opts]}]
     (assert (false? (:sync? opts)) "IndexedDB store connections must be async")
     (idb/connect-idb-store name :config config)))

#?(:cljs
   (defmethod default-config :indexeddb [config]
     (merge
      {:path  (rand-german-mammal)}
      config)))

(s/def :datahike.store.indexeddb/name string?)
(s/def :datahike.store.indexeddb/backend #{:indexeddb})
(s/def ::indexeddb (s/keys :req-un [:datahike.store.indexeddb/backend
                                    :datahike.store.indexeddb/name]))

;; tiered store for cljs with memory front and file backend
(defmethod store-identity :tiered [config]
  [:tiered (store-identity (:backend-store config)) (store-identity (:backend-store config))])

(defmethod empty-store :tiered [{:keys [backend-store frontend-store opts]}]
  (let [opts (or opts {:sync? true})]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [backend-store (<?- (empty-store (assoc backend-store :opts opts)))
                       frontend-store (<?- (empty-store (assoc frontend-store :opts opts)))]
                   ;; Note: kt/connect-tiered-store expects frontend first, then backend
                   (<?- (kt/connect-tiered-store frontend-store backend-store :opts opts)))))))

(defmethod delete-store :tiered [{:keys [backend-store]}]
  (delete-store backend-store))

(defmethod connect-store :tiered [{:keys [backend-store frontend-store opts]}]
  (async+sync (:sync? (or opts {:sync? true})) *default-sync-translation*
              (go-try-
               (let [opts (or opts {:sync? true})
                     frontend-store (<?- (connect-store (assoc frontend-store :opts opts)))
                     backend-store (<?- (connect-store (assoc backend-store :opts opts)))]
                 (<?- (kt/connect-tiered-store frontend-store backend-store :opts opts))))))

(defmethod ready-store :tiered [{:keys [opts]} store]
  (async+sync (:sync? (or opts {:sync? true})) *default-sync-translation*
              (go-try-
               (<?- (ready-store (:frontend-store opts) (:frontend-store store)))
               (<?- (ready-store (:backend-store opts) (:backend-store store)))
               (<?- (kt/sync-on-connect store kt/populate-missing-strategy opts))
               true)))

(defmethod default-config :tiered [config]
  config)

(s/def :datahike.store.tiered/backend #{:tiered})
(s/def :datahike.store.tiered/backend-store map?)
(s/def ::tiered (s/keys :req-un [:datahike.store.tiered/backend
                                 :datahike.store.tiered/backend-store]))

(defmethod config-spec :tiered [_] ::tiered)