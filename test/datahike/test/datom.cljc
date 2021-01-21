(ns datahike.test.datom
  (:require
   #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]]
      :clj  [clojure.test :as t :refer [is are deftest testing]])
   [datahike.datom :refer [datom]]))

(deftest datom-impl
  (let [d (datom 123 :foo/bar "foobar")]
    (is (= [:e 123]
           (find d :e)))
    (is (= {:e 123
            :a :foo/bar
            :v "foobar"}
           (select-keys d [:e :a :v])))
    (is (= 123 (d :e)))))
