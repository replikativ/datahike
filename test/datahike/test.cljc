(ns datahike.test
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    #?(:clj [clojure.java.shell :as sh])
    datahike.test.core
    datahike.test.components
    datahike.test.config
    datahike.test.conn
    datahike.test.db
    datahike.test.entity
    datahike.test.explode
    datahike.test.filter
    datahike.test.ident
    datahike.test.index
    datahike.test.listen
    datahike.test.lookup-refs
    datahike.test.lru
    datahike.test.migrate
    datahike.test.pull-api
    datahike.test.purge
    datahike.test.query
    datahike.test.query-aggregates
    datahike.test.query-find-specs
    datahike.test.query-fns
    datahike.test.query-interop
    datahike.test.query-not
    datahike.test.query-or
    datahike.test.query-pull
    datahike.test.query-rules
    datahike.test.query-v3
    datahike.test.schema
    datahike.test.store
    datahike.test.time-variance
    datahike.test.transact
    datahike.test.validation
    datahike.test.upsert))


(defn ^:export test-clj []
  (datahike.test.core/wrap-res #(t/run-all-tests #"datahike\..*")))

(defn ^:export test-cljs []
  (datahike.test.core/wrap-res #(t/run-all-tests #"datahike\..*")))

#?(:clj
   (defn test-node [& args]
     (let [res (apply sh/sh "node" "test_node.js" args)]
       (println (:out res))
       (binding [*out* *err*]
         (println (:err res)))
       (System/exit (:exit res)))))

(comment

  (test-clj))


