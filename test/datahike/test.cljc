(ns datahike.test
  (:require
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
       :clj  [clojure.test :as t :refer        [is are deftest testing]])
    datahike.test.core
   
    datahike.test.btset
    datahike.test.components
    datahike.test.conn
    datahike.test.db
    datahike.test.entity
    datahike.test.explode
    datahike.test.filter
    datahike.test.index
    datahike.test.listen
    datahike.test.lookup-refs
    datahike.test.lru
    datahike.test.parser
    datahike.test.parser-find
    datahike.test.parser-rules
    datahike.test.parser-query
    datahike.test.parser-where
    datahike.test.pull-api
    datahike.test.pull-parser
    datahike.test.query
    datahike.test.query-aggregates
    datahike.test.query-find-specs
    datahike.test.query-fns
    datahike.test.query-not
    datahike.test.query-pull
    datahike.test.query-rules
    datahike.test.query-v3
    datahike.test.serialization
    datahike.test.transact
    datahike.test.validation
    datahike.test.upsert))

(defn ^:export test-most []
  (datahike.test.core/wrap-res #(t/run-all-tests #"datahike\.test\.(?!btset).*")))

(defn ^:export test-btset []
  (datahike.test.core/wrap-res #(t/run-all-tests #"datahike\.test\.btset")))

(defn ^:export test-all []
  (datahike.test.core/wrap-res #(t/run-all-tests #"datahike\..*")))

#?(:clj
(defn test-node [& args]
  (let [res (apply clojure.java.shell/sh "node" "test_node.js" args)]
    (println (:out res))
    (binding [*out* *err*]
      (println (:err res)))
    (System/exit (:exit res)))))
