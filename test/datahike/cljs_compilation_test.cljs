(ns datahike.cljs-compilation-test
  (:require [clojure.test :refer [deftest is] :as t]
            [datahike.api :as d]
            datahike.api.impl
            datahike.api.specification
            datahike.array
            datahike.config
            datahike.connector
            datahike.connections
            datahike.constants
            datahike.core
            datahike.datom
            datahike.db
            datahike.db.interface
            datahike.db.search
            datahike.db.transaction
            datahike.db.utils
            datahike.gc
            datahike.impl.entity
            datahike.index
            datahike.index.interface
            datahike.index.persistent-set
            datahike.index.utils
            datahike.integration-test
            datahike.lru
            datahike.middleware.utils
            datahike.pull-api
            datahike.query
            datahike.readers
            datahike.schema
            datahike.schema-cache
            datahike.spec
            datahike.store
            datahike.transit
            datahike.tools
            datahike.writing
            datahike.writer))

(deftest sanity-test
  (is (fn? d/q)))

;clj -M:cljs -m shadow.cljs.devtools.cli compile :comptest && node target/out/comptest.js
(defn -main []
  (let [summary (clojure.test/run-tests)]
    (js/process.exit (if (t/successful? summary) 0 1))))