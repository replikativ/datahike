(ns datahike.test.temporal-index
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer        [is are deftest testing use-fixtures]])
   [datahike.api :as d]
   [datahike.test.core :as tdc]))

(defn test-historical-queries
  (let [uri "datahike:mem://test-historical-queries"
        _ (d/delete-database uri)]))
