(ns datahike.test.utils
  (:require [datahike.api :as d]))

(defn setup-db [cfg]
  (d/delete-database cfg)
  (d/create-database cfg)
  (d/connect cfg))

(defn with-default-cfg [cfg]
  (merge
   {:store {:backend :mem
            :id "default-test"}
    :attribute-refs? false
    :keep-history? false
    :keep-log? false
    :schema-flexibility :read}
   cfg))


(defn datahike-reset-fixture
  [cfg test-function]
  (d/delete-database cfg)
  (d/create-database cfg)
  (test-function)
  (d/delete-database cfg))