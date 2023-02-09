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

(defn setup-default-db [config test-data]
  (let [_ (d/delete-database config)
        _ (d/create-database config)
        conn (d/connect config)]
    (d/transact conn test-data)
    conn))

(defn teardown-db [conn] 
  (d/release conn)
  (d/delete-database (:config @conn)))

(defn with-db
  "Test database fixture"
  ([config f]
   (with-db config [] f))
  ([config test-data f] 
   (let [conn (setup-default-db config test-data)]
     (f)
     (teardown-db conn))))
