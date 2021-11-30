(ns datahike.test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing]])
   #?(:clj [clojure.java.shell :as sh])
   datahike.test.core-test
   datahike.test.components-test
   datahike.test.config-test
   datahike.test.db-test
   datahike.test.entity-test
   datahike.test.explode-test
   datahike.test.filter-test
   datahike.test.ident-test
   datahike.test.index-test
   datahike.test.listen-test
   datahike.test.lookup-refs-test
   datahike.test.lru-test
   datahike.test.migrate-test
   datahike.test.pull-api-test
   datahike.test.purge-test
   datahike.test.query-test
   datahike.test.query-aggregates-test
   datahike.test.query-find-specs-test
   datahike.test.query-fns-test
   datahike.test.query-interop-test
   datahike.test.query-not-test
   datahike.test.query-or-test
   datahike.test.query-pull-test
   datahike.test.query-rules-test
   datahike.test.query-v3-test
   datahike.test.schema-test
   datahike.test.store-test
   datahike.test.time-variance-test
   datahike.test.transact-test
   datahike.test.tuples-test
   datahike.test.upsert-test
   datahike.test.upsert-impl-test
   datahike.test.validation-test
   datahike.test.attribute-refs.differences-test
   datahike.test.attribute-refs.entity-test
   datahike.test.attribute-refs.pull-api-test
   datahike.test.attribute-refs.query-test
   datahike.test.attribute-refs.transact-test
   datahike.test.attribute-refs.utils))

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


