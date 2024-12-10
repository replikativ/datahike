(ns ^:no-doc datahike.store
  (:require [clojure.spec.alpha :as s]
            #?(:clj [konserve.filestore :as fs])
            [konserve.memory :as mem]
            [environ.core :refer [env]]
            [datahike.index :as di]
            [datahike.tools :as dt]
            [konserve.cache :as kc]
            #?(:clj [clojure.core.cache :as cache]
               :cljs [cljs.cache :as cache])
            [taoensso.timbre :refer [info]]
            [zufall.core :refer [rand-german-mammal]])
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

(defmethod empty-store :mem [{:keys [id]}]
  (if-let [store (get @memory id)]
    store
    (let [store (mem/new-mem-store (atom {}) {:sync? true})]
      (swap! memory assoc id store)
      store)))

(defmethod delete-store :mem [{:keys [id]}]
  (swap! memory dissoc id)
  nil)

(defmethod connect-store :mem [{:keys [id]}]
  (@memory id))

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
   (defmethod empty-store :file [{:keys [path config]}]
     (fs/connect-fs-store path :opts {:sync? true} :config config)))

#?(:clj
   (defmethod delete-store :file [{:keys [path]}]
     (fs/delete-store path)))

#?(:clj
   (defmethod connect-store :file [{:keys [path config]}]
     (fs/connect-fs-store path :opts {:sync? true} :config config)))

#?(:clj
   (defn- get-working-dir []
     (.toString (.toAbsolutePath (Paths/get "" (into-array String []))))))

#?(:clj
   (defmethod default-config :file [config]
     (merge
      {:path  (:datahike-store-path env (str (get-working-dir) "/datahike-db-" (rand-german-mammal)))
       :scope (dt/get-hostname)}
      config)))

#?(:clj (s/def :datahike.store.file/path string?))
#?(:clj (s/def :datahike.store.file/backend #{:file}))
#?(:clj (s/def :datahike.store.file/scope string?))
#?(:clj (s/def ::file (s/keys :req-un [:datahike.store.file/backend
                                       :datahike.store.file/path
                                       :datahike.store.file/scope])))

#?(:clj  (defmethod config-spec :file [_] ::file))
