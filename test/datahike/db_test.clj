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

  "simple range"
  (let [db      (dh-db/empty-db)
        _  (fdb/clear-all)
        _ (-> (with-datom db (Datom. 123 :likes "Hans" 0 true))
              (with-datom (Datom. 124 :likes "GG" 0 true))
              (with-datom (Datom. 125 :likes "GG" 0 true))
              (with-datom (Datom. 1 :likes "GG" 0 true))
              (with-datom (Datom. 2 :likes "GG" 0 true))
              (with-datom (Datom. 3 :likes "GG" 0 true)))]
    (is (= 2
           (count (fdb/get-range [123 :likes "Hans" 0 true]
                                 [125 :likes "GG" 0 true])))))

  "large range"
  (let [db (dh-db/empty-db)
        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (Datom. %2 :likes "Hans" 0 true)) db (range 100))]
    (is (= 50
           (count (fdb/get-range [1 :likes "Hans" 0 true]
                                 [51 :likes "Hans" 0 true])))))

  "large range"
  ;; TODO: if we don't clear the fdb db then tnere are weird things:
  ;; check as the following might not work for instance.
  ;; Could it be that the same key is allowed to be reinserted multiple
  ;; times and appears multiple times?
  (let [db (dh-db/empty-db)
        ;;        _  (fdb/clear-all)
        _  (reduce #(with-datom %1 (Datom. %2 :likes "Hans" 0 true)) db (range 100))]
    (is (= 2
           (count (fdb/get-range [1 :likes "Hans" 0 true]
                                 [3 :likes "Hans" 0 true]))))))
