(ns ^:no-doc datahike.env
  "Runtime defaults read from the finite environment surface Datahike uses.

   Environment variables override legacy Leiningen/Boot files, and JVM system
   properties override environment variables. Unrelated process settings are
   discarded before merging, so their values cannot collide or leak."
  (:require [clojure.string :as str]
            #?(:clj [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            #?(:clj [clojure.java.io :as io])
            #?(:cljs [goog.object :as obj])))

#?(:cljs
   (do
     (def ^:private nodejs? (exists? js/require))
     (def ^:private ^js fs (when nodejs? (js/require "fs")))
     (def ^:private ^js process (when nodejs? (js/require "process")))))

(def ^:private supported-keys
  #{:schema-meta-cache-size
    :schema-write-cache-size
    :datahike-store-backend
    :datahike-index
    :datahike-initial-tx
    :datahike-keep-history
    :datahike-attribute-refs
    :datahike-schema-flexibility
    :datahike-search-cache-size
    :datahike-store-cache-size
    :datahike-index-config
    :datahike-max-db-caches})

(defn normalize-key
  "Normalize an environment or system-property key to Datahike's historical
   keyword shape."
  [key]
  (-> key name str/lower-case (str/replace "_" "-") (str/replace "." "-") keyword))

(defn- normalize-source [source]
  (into {}
        (keep (fn [[key value]]
                (let [key (normalize-key key)]
                  (when (contains? supported-keys key)
                    [key (if (string? value) value (str value))]))))
        source))

(defn merge-sources
  "Merge configuration sources in precedence order after discarding settings
   Datahike never reads."
  [& sources]
  (apply merge (map normalize-source sources)))

(defn- slurp-file [file]
  #?(:clj (when file
            (let [file (io/file file)]
              (when (.exists file)
                (slurp file))))
     :cljs (when (and nodejs? file (.existsSync ^js fs file))
             (str (.readFileSync ^js fs file)))))

(defn- read-env-file [file]
  (some-> file slurp-file edn/read-string))

(defn- system-environment []
  #?(:clj (System/getenv)
     :cljs (if nodejs?
             (zipmap (obj/getKeys (.-env ^js process))
                     (obj/getValues (.-env ^js process)))
             {})))

#?(:clj
   (defn- system-properties []
     (into {} (System/getProperties))))

(defn read-env []
  #?(:clj (merge-sources
           (read-env-file ".lein-env")
           (read-env-file (io/resource ".boot-env"))
           (system-environment)
           (system-properties))
     :cljs (if nodejs?
             (merge-sources (read-env-file ".lein-env")
                            (system-environment))
             {})))

(defonce env (read-env))
