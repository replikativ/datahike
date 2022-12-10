(ns backward-test
  (:refer-clojure :exclude [read])
  (:require [datahike.api :as d]
            [taoensso.timbre :as t]))

(def schema [{:db/ident       :age
              :db/cardinality :db.cardinality/one
              :db/valueType   :db.type/long}])
(def cfg {:store  {:backend :file :path "/tmp/datahike-backward-comp-test"}
          :keep-history? true
          :schema-flexibility :write
          :initial-tx schema})

(def size 10000)

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn write [_opt]
  (t/info "Writing to db using latest released version of datahike ....")
  (t/set-level! :warn)

  (d/delete-database cfg)
  (d/create-database cfg)
  (let [conn (d/connect cfg)]
    (d/transact conn
                (vec (for [i (range size)]
                       [:db/add (inc i) :age i]))))
  (t/info "Wrote " size " entries."))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn read [_opt]
  (t/info "Reading using latest code ....")
  (let [conn (d/connect cfg)
        res (first (d/q '[:find (count ?a)
                          :in $
                          :where [?e :age ?a]]
                        @conn))]
    (assert (= [size] res))
    (t/info "Read " (first res) " entries.")))
