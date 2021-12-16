(ns datahike.test.query-v3-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [are deftest]]
      :clj  [clojure.test :as t :refer        [are deftest]])
   [datahike.query-v3 :as dq]
   [datahike.test.core-test])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

(deftest test-validation
  (are [q ins msg] (thrown-with-msg? ExceptionInfo msg (apply dq/q q ins))
    '[:find ?a :in $ ?a]          [0]   #"Wrong number of arguments for bindings \[\$ \?a\], 2 required, 1 provided"
    '[:find ?a :in $ :where [?a]] [0 1] #"Wrong number of arguments for bindings \[\$\], 1 required, 2 provided"
    '[:find ?a :where [?a]]       [0 1] #"Wrong number of arguments for bindings \[\$\], 1 required, 2 provided"

    '[:find ?a :where [?a 1]]     [:a]  #"Cannot match by pattern \[\?a 1\] because source is not a collection: :a"))

#_(deftest test-query
    (is (= (dq/q '[:find ?a :where [?a ?a]]
                 [[1 2] [3 3] [4 5] [6 6]])
           #{[3] [6]})))
