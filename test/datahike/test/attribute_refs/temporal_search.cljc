(ns datahike.test.attribute-refs.temporal-search
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is deftest]]
      :clj [clojure.test :refer [is deftest]])
   [datahike.api :as d]))

(deftest q-with-history-should-contain-sys-attributes
  (let [cfg {:store {:backend :memory
                     :id #uuid "00220000-0000-0000-0000-000000000022"}
             :attribute-refs? true
             :keep-history? true
             :schema-flexibility :write}
        conn (do (d/delete-database cfg)
                 (d/create-database cfg)
                 (d/connect cfg))
        query '[:find ?e ?a ?v ?t ?s
                :where [?e ?a ?v ?t ?s]]]
    (is (= (d/q query @conn)
           (d/q query (d/history @conn))))
    (is (< 1
           (count (d/q query (d/history @conn)))))
    (d/release conn)))
