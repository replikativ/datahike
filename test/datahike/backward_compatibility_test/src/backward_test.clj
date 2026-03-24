(ns backward-test
  (:require [datahike.api :as d]
            [replikativ.logging :as log]
            [taoensso.trove :as trove]
            [taoensso.trove.console :as trove-console]))

(def schema [{:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])
(def cfg {:store  {:backend :file
                   :path "/tmp/datahike-backward-comp-test"
                   :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
          :keep-history? true
          :schema-flexibility :write
          :initial-tx schema})

(def size 10000)

(defn write [opt]
  (log/info :backward-test/writing "Writing to db using latest released version of datahike")
  (trove/set-log-fn! (trove-console/get-log-fn {:min-level :warn}))

  (d/delete-database cfg)
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn
                (vec (for [i (range size)]
                       [:db/add (inc i) :age i]))))
  (log/info :backward-test/wrote {:count size}))

(defn read [opt]
  (log/info :backward-test/reading "Reading using latest code")
  (let [conn (d/connect cfg)
        res (first (d/q '[:find (count ?a)
                          :in $
                          :where [?e :age ?a]]
                        @conn))]
    (assert (= [size] res))
    (log/info :backward-test/read {:count (first res)})))
