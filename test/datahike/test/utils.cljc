(ns datahike.test.utils
  (:require [datahike.api :as d]
            [datahike.tools :as tools])
  (:import (java.util UUID)))

(defn cfg-template
  "Returning a config template with a random store-id"
  []
  {:store {:backend :mem}
   :keep-history? true
   :schema-flexibility :read})

(defn setup-db
  "Setting up a test-db in memory by default.  Deep-merges the passed config into the defaults."
  ([]
   (setup-db {}))
  ([cfg]
   (setup-db cfg true))
  ([cfg gen-uuid?]
   (let [cfg (cond-> (tools/deep-merge (cfg-template) cfg)
               gen-uuid? (assoc-in [:store :id] (str (UUID/randomUUID))))]
     (d/delete-database cfg)
     (d/create-database cfg)
     (d/connect cfg))))

(defn all-true? [c] (every? true? c))

(defn all-eq? [c1 c2] (all-true? (map = c1 c2)))

(defn sleep [ms]
  #?(:clj (Thread/sleep ms)
     :cljs (js/setTimeout (fn []) ms)))
