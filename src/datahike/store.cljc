(ns ^:no-doc datahike.store
  (:require [clojure.spec.alpha :as s]
            [konserve.filestore :as fs]
            [konserve.memory :as mem]
            [superv.async :refer [<?? S]]
            [environ.core :refer [env]]
            [datahike.index :as di]
            [konserve.cache :as kc]
            [clojure.core.cache :as cache]))

(defn add-cache-and-handlers [raw-store config]
  (cond->> (kc/ensure-cache
            raw-store
            (atom (cache/lru-cache-factory {} :threshold (:store-cache-size config))))
    (not= :mem (get-in config [:backend :store]))
    (di/add-konserve-handlers config)))

(defmulti empty-store
  "Creates an empty store"
  {:arglists '([config])}
  :backend)

(defmethod empty-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't create a store with scheme: " backend))))

(defmulti delete-store
  "Deletes an existing store"
  {:arglists '([config])}
  :backend)

(defmethod delete-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't delete a store with scheme: " backend))))

(defmulti connect-store
  "Makes a connection to an existing store"
  {:arglists '([config])}
  :backend)

(defmethod connect-store :default [{:keys [backend]}]
  (throw (IllegalArgumentException. (str "Can't connect to store with scheme: " backend))))

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
  (println "INFO: No default configuration found for" backend)
  config)

(defmulti config-spec
  "Returns spec for the store configuration"
  {:arglists '([config])}
  :backend)

(defmethod config-spec :default [{:keys [backend]}]
  (println "INFO: Not configuration spec found for" backend))

;; memory

(def memory (atom {}))

(defmethod empty-store :mem [{:keys [id]}]
  (if-let [store (get @memory id)]
    store
    (let [store (<?? S (mem/new-mem-store))]
      (swap! memory assoc id store)
      store)))

(defmethod delete-store :mem [{:keys [id]}]
  (swap! memory dissoc id))

(defmethod connect-store :mem [{:keys [id]}]
  (@memory id))

(defmethod default-config :mem [config]
  (merge
   {:id (:datahike-store-id env "default")}
   config))

(s/def :datahike.store.mem/backend #{:mem})
(s/def :datahike.store.mem/id string?)
(s/def ::mem (s/keys :req-un [:datahike.store.mem/backend
                              :datahike.store.mem/id]))

(defmethod config-spec :mem [_config] ::mem)

;; file

(defmethod empty-store :file [{:keys [path] :as _config}]
  (<?? S (fs/new-fs-store path)))

(defmethod delete-store :file [{:keys [path]}]
  (fs/delete-store path))

(defmethod connect-store :file [{:keys [path]}]
  (<?? S (fs/new-fs-store path)))

(defmethod default-config :file [config]
  (merge
   {:path (:datahike-store-path env "datahike-db")}
   config))

(s/def :datahike.store.file/path string?)
(s/def :datahike.store.file/backend #{:file})
(s/def ::file (s/keys :req-un {:datahike.store.file/backend
                               :datahike.store.file/path}))

(defmethod config-spec :file [_] ::file)
