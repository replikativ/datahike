(ns datahike.test.insert
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest]]
      :clj  [clojure.test :as t :refer        [is deftest]])
   [datahike.api :as d]))

#?(:cljs
   (def Throwable js/Error))


(deftest cardinality-many-insert
  (let [config {:store {:backend :mem
                        :id      (str "default-" (.toString (java.util.UUID/randomUUID)))}}
        schema [{:db/ident       :block/string
                 :db/valueType   :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident       :block/children
                 :db/valueType   :db.type/ref
                 :db/cardinality :db.cardinality/many}]
        _      (d/create-database config)
        conn   (d/connect config)
        expected (datahike.datom/datom 502 :block/children 501 536870915)]

    (d/transact conn schema)
    (d/transact conn [{:db/id 501 :block/string "one"}])
    (d/transact conn [{:db/id 502 :block/children 501}])
    (d/transact conn [{:db/id 502 :block/children 501}])

    (is (= expected
          (first (d/datoms @conn :eavt 502 :block/children))))
    (is (= expected
          (first (d/datoms @conn :aevt :block/children 502))))
    (is (= expected
          (first (d/datoms @conn :avet :block/children 501))))

    (d/release conn)
    (d/delete-database config)))
