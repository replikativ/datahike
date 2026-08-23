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

(defn- get-or-create-write-cache
  "The per-store write cache, created on first use.

   `lookup-or-miss` rather than `has?` then `lookup`, because those are two
   operations on a cache that another thread can change in between — and one
   does, by design: `gc/mark-and-sweep!` calls `clear-write-cache`, an evict,
   while writers are inside `writing/db->stored`. Lose that window and `lookup`
   returns nil, so `(cw/has? nil k)` derefs nil and the WRITER dies with

     NullPointerException: Cannot invoke \"java.util.concurrent.Future.get()\"
     because \"fut\" is null

   which is a background GC killing an unrelated transaction. `add-to-write-cache`
   lost the same race one line over, with `(cw/miss nil …)` and a matching
   \"because \\\"atom\\\" is null\". Reproduced at 5533 failures in 160000
   iterations across 8 threads against a concurrent evictor; zero after.

   The eviction is not only the GC's: `schema-write-caches` is itself an LRU
   bounded by `*schema-write-cache-max-db-count*`, so a process touching more
   stores than that evicts entries on its own.

   `lookup-or-miss` is one atomic `swap!` through the cache and calls the
   factory at most once even under retry, so concurrent first-users share one
   cache rather than racing to install competing ones."
  [store-config]
  (let [store-id (ds/store-identity store-config)]
    (cw/lookup-or-miss schema-write-caches store-id
                       (fn [_] (cw/lru-cache-factory {} :threshold dc/*schema-meta-cache-size*)))))

(defn cache-has? [schema-meta-key]
  (cw/has? schema-meta-cache schema-meta-key))

(defn cache-lookup [schema-meta-key]
  (cw/lookup schema-meta-cache schema-meta-key))

(defn cache-miss [schema-meta-key schema-meta]
  (cw/miss schema-meta-cache schema-meta-key schema-meta))

;; Both tolerate a nil cache rather than assuming one. `lookup-or-miss` gives up
;; after ten retries and returns nil, and while that needs ten consecutive
;; evictions of the entry we just installed, the cost of being wrong is not
;; symmetric: this cache only decides whether to SKIP re-writing schema meta,
;; whose key is `(uuid schema-meta)` — content-addressed, so writing it again is
;; idempotent, same key and same bytes. A miss costs one redundant write; an
;; exception here kills the caller's transaction.

(defn write-cache-has? [store-config schema-meta-key]
  (if-let [write-cache (get-or-create-write-cache store-config)]
    (cw/has? write-cache schema-meta-key)
    false))

(defn add-to-write-cache [store-config schema-meta-key]
  (when-let [write-cache (get-or-create-write-cache store-config)]
    (cw/miss write-cache schema-meta-key true)))

(defn clear-write-cache [store-config]
  (let [store-id (ds/store-identity store-config)]
    (cw/evict schema-write-caches store-id)))

