(ns datahike.schema-cache
  (:require #?(:clj [clojure.core.cache.wrapped :as cw]
               :cljs [cljs.cache.wrapped :as cw])
            [datahike.config :as dc]
            [datahike.store :as ds]))

;; Shared schema read cache across all stores
(def schema-meta-cache (cw/lru-cache-factory {} :threshold dc/*schema-meta-cache-size*))

;; LRU cache of LRU caches for write operations, one per store
(def schema-write-caches
  (cw/lru-cache-factory {} :threshold dc/*schema-write-cache-max-db-count*))

(defn- get-or-create-write-cache [store-config]
  (let [store-id (ds/store-identity store-config)]
    (if (cw/has? schema-write-caches store-id)
      (cw/lookup schema-write-caches store-id)
      (let [new-cache (cw/lru-cache-factory {} :threshold dc/*schema-meta-cache-size*)]
        (cw/miss schema-write-caches store-id new-cache)
        new-cache))))

(defn cache-has? [schema-meta-key]
  (cw/has? schema-meta-cache schema-meta-key))

(defn cache-lookup [schema-meta-key]
  (cw/lookup schema-meta-cache schema-meta-key))

(defn cache-miss [schema-meta-key schema-meta]
  (cw/miss schema-meta-cache schema-meta-key schema-meta))

(defn write-cache-has? [store-config schema-meta-key]
  (let [write-cache (get-or-create-write-cache store-config)]
    (cw/has? write-cache schema-meta-key)))

(defn add-to-write-cache [store-config schema-meta-key]
  (let [write-cache (get-or-create-write-cache store-config)]
    (cw/miss write-cache schema-meta-key true)))

(defn clear-write-cache [store-config]
  (let [store-id (ds/store-identity store-config)]
    (cw/evict schema-write-caches store-id)))

