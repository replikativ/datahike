(ns datahike.db-test
  (:import [datahike.db Datom])
  (:require [datahike.db :as dh-db :refer [with-datom]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fdb.core :as fdb]))


(deftest fdb
  "get"
  (let [db                          (dh-db/empty-db)
        {:keys [eavt eavt-durable]} (-> (with-datom db (Datom. 123 :likes "Hans" 0 true))
                                        (with-datom (Datom. 124 :likes "GG" 0 true)))]

    (is (== (nth (fdb/get (:eavt-scalable db)
                          [123 :likes "Hans" 0 true]) 7)
            123))
    (is (== (nth (fdb/get (:eavt-scalable db)
                          [124 :likes "GG" 0 true]) 7)
            124)))

  "range"
  (let [db      (dh-db/empty-db)
        inserts (-> (with-datom db (Datom. 123 :likes "Hans" 0 true))
                    (with-datom (Datom. 124 :likes "GG" 0 true))
                    (with-datom (Datom. 125 :likes "GG" 0 true)))]
    (is (= (fdb/get-range [123 :likes "Hans" 0 true]
                          [125 :likes "GG" 0 true])
            2)))
  )
